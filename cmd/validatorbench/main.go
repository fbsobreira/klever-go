package main

import (
	"context"
	"crypto/ed25519"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"runtime"
	"syscall"
	"time"

	logger "github.com/klever-io/klever-go-logger"
)

// appVersion is injected at build time via -ldflags "-X main.appVersion=...".
var appVersion = "dev"

// internalSHAReExecMarker tells a child process that its parent already
// applied --sha-hardware=off via GODEBUG and there's no need to re-exec
// again. It also flips hwSHADisabled so the report records the right
// state.
const internalSHAReExecMarker = "KLEVER_VBENCH_NO_SHA_HW"

// childOutputMode tells a forked child to emit only JSON on stdout (so
// the parent can parse it for --compare-sha mode).
const childOutputMode = "KLEVER_VBENCH_CHILD_JSON"

func main() {
	cliCfg, configPath, showVersion := parseFlags()
	if showVersion {
		fmt.Printf("klever-validatorbench %s (%s/%s, %s)\n",
			appVersion, runtime.GOOS, runtime.GOARCH, runtime.Version())
		return
	}

	if os.Getenv(internalSHAReExecMarker) == "1" {
		hwSHADisabled = true
	}

	cfg, err := LoadConfig(configPath)
	if err != nil {
		fail("load config: %v", err)
	}
	mergeCLIIntoConfig(&cfg, cliCfg)
	if err := cfg.Validate(); err != nil {
		fail("config: %v", err)
	}

	if !cfg.Verbose {
		_ = logger.SetLogLevel("*:NONE")
	}

	// --compare-sha runs the benchmark twice. We do it via re-exec so
	// the GODEBUG flags actually take effect (GODEBUG is parsed in
	// runtime init and cannot be re-applied later).
	if cfg.CompareSHA {
		if err := runCompareMode(cfg, configPath); err != nil {
			fail("compare-sha: %v", err)
		}
		return
	}

	// If sha-hardware=off and we haven't yet re-execed, do so now with
	// the appropriate GODEBUG flags so the runtime disables SHA-NI.
	if cfg.SHAHardware == "off" && os.Getenv(internalSHAReExecMarker) != "1" {
		if err := reexecWithoutSHA(); err != nil {
			fail("disable HW SHA: %v", err)
		}
		// reexecWithoutSHA replaces this process — control never returns
		// here on a successful exec, but err != nil falls through to a
		// best-effort run with HW SHA still active.
	}

	report, err := runOnce(cfg)
	if err != nil {
		fail("benchmark: %v", err)
	}

	emitOutputs(cfg, report)
}

// runOnce sets up the VM environment, spins up generators, mempool, and
// processor, and returns the resulting Report. Used both for normal and
// compare-sha mode (in the latter, called from a child process).
func runOnce(cfg Config) (*Report, error) {
	hashStats := &HashStats{}
	env, err := NewVMEnv(cfg, hashStats)
	if err != nil {
		return nil, fmt.Errorf("set up VM: %w", err)
	}
	defer env.Close()

	builder := NewTxBuilder(cfg)

	// If the user wrote a tx_mix the engine routes through the "mix"
	// workload so multiple tx types and contracts can run in the same
	// benchmark. Otherwise we honour the simple Workload field.
	wlName := cfg.Workload
	if len(cfg.TxMix) > 0 {
		wlName = "mix"
	}
	wlCfg := cfg
	wlCfg.Workload = wlName
	wl, err := NewWorkload(wlCfg, env, builder)
	if err != nil {
		return nil, err
	}

	// Optional warm-up: pump a small batch of txs to JIT-compile the wasm
	// and populate caches before timing starts.
	if cfg.WarmupTransactions > 0 {
		warmup(env, wl, cfg)
		hashStats.Reset()
	}

	// Mempool capacity: in budget mode we want it permanently overloaded
	// so the processor never waits for txs and we measure its true
	// ceiling. The capacity is the larger of (prefill, block_size * 8).
	mempoolCap := cfg.BlockSize * 8
	if cfg.PrefillMempool > mempoolCap {
		mempoolCap = cfg.PrefillMempool
	}
	mp := NewMempool(mempoolCap)
	metrics := NewMetrics(min(cfg.NumTransactions, 200_000), hashStats)
	proc := NewProcessor(cfg, env, mp, metrics)

	ctx, cancel := contextWithDuration(cfg.Duration)
	defer cancel()

	stopOnSignal(cancel)

	// Prefill the mempool synchronously before timing starts. This is the
	// cleanest way to simulate the "validator is overloaded" state where
	// the per-block ceiling is the only thing limiting throughput.
	if cfg.PrefillMempool > 0 {
		fillCtx, fillCancel := context.WithTimeout(context.Background(), 30*time.Second)
		prefillMempool(fillCtx, cfg, mp, wl, metrics)
		fillCancel()
	}

	if !cfg.Verbose {
		// Even non-verbose runs deserve a heartbeat so it's clear the
		// benchmark hasn't hung — operators tend to leave it alone.
		go progressTicker(ctx, metrics, cfg, os.Stderr)
	}

	metrics.Start()
	startGenerators(ctx, cfg, mp, wl, metrics)

	// Run the processor on the main goroutine. It returns when ctx
	// expires, the tx budget is hit, or the mempool is closed.
	go func() {
		// When generators stop pushing (because ctx canceled or budget
		// hit), the mempool will drain and Drain() will block forever.
		// We rely on ctx cancellation to release it.
	}()
	proc.Run(ctx, cfg.NumTransactions)
	cancel()

	// Drain any remaining txs the generators dropped after ctx died.
	mp.Close()
	metrics.Stop()

	return BuildReport(cfg, metrics, env), nil
}

// prefillMempool synchronously generates Cfg.PrefillMempool transactions
// using the configured workload + concurrency. The processor will only
// start once this returns, so the timed phase begins with the mempool
// full and the validator pipeline immediately under pressure — exactly
// the state we want when measuring "max txs that fit in 500 ms".
func prefillMempool(ctx context.Context, cfg Config, mp *Mempool, wl Workload, ms *Metrics) {
	target := cfg.PrefillMempool
	if target <= 0 {
		return
	}
	produced := make(chan struct{}, target)
	for i := 0; i < cfg.Concurrency; i++ {
		go func(id int) {
			for {
				select {
				case <-ctx.Done():
					return
				default:
				}
				tx := wl.Next(id)
				t0 := time.Now()
				if ed25519.Verify(tx.Sender.Public, tx.SigBody, tx.Signature) {
					tx.Verified = true
				}
				ms.SigVerifyIntakeNs.Add(uint64(time.Since(t0).Nanoseconds()))
				if !mp.Push(ctx, tx) {
					return
				}
				produced <- struct{}{}
			}
		}(i)
	}
	for i := 0; i < target; i++ {
		select {
		case <-produced:
		case <-ctx.Done():
			return
		}
	}
}

// startGenerators launches `cfg.Concurrency` workload producers. Each
// generator owns a slice of the sender pool (sharded modulo workerID)
// to keep nonce contention minimal.
func startGenerators(ctx context.Context, cfg Config, mp *Mempool, wl Workload, ms *Metrics) {
	for i := 0; i < cfg.Concurrency; i++ {
		go generatorLoop(ctx, i, cfg, mp, wl, ms)
	}
}

func generatorLoop(ctx context.Context, id int, cfg Config, mp *Mempool, wl Workload, ms *Metrics) {
	for {
		if ctx.Err() != nil {
			return
		}
		tx := wl.Next(id)
		// Verify ed25519 here, in the generator goroutine, so the cost
		// is amortised across all available cores BEFORE the block
		// budget clock starts. This mirrors a real validator's mempool
		// intake path: signatures are checked when the tx arrives, not
		// when the block builder picks it up. The 500ms slot budget is
		// therefore spent only on execution + state updates.
		t0 := time.Now()
		if ed25519.Verify(tx.Sender.Public, tx.SigBody, tx.Signature) {
			tx.Verified = true
		}
		ms.SigVerifyIntakeNs.Add(uint64(time.Since(t0).Nanoseconds()))
		if !mp.Push(ctx, tx) {
			return
		}
	}
}

// warmup pushes a fixed batch through the VM with timing disabled. It
// JITs the contract, populates account caches, and lets the runtime
// settle into a steady state.
func warmup(env *VMEnv, wl Workload, cfg Config) {
	for i := 0; i < cfg.WarmupTransactions; i++ {
		tx := wl.Next(i % cfg.Concurrency)
		_, _, _ = env.ExecuteTx(tx, &HashStats{})
	}
}

// progressTicker prints a one-line stat snapshot every cfg.ProgressSec.
// It stops when ctx is canceled.
func progressTicker(ctx context.Context, m *Metrics, cfg Config, w io.Writer) {
	if cfg.ProgressSec <= 0 {
		return
	}
	t := time.NewTicker(time.Duration(cfg.ProgressSec) * time.Second)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			tx := m.TxTotal.Load()
			fail := m.TxFailed.Load()
			elapsed := time.Since(m.StartedAt).Seconds()
			tps := float64(tx) / elapsed
			fmt.Fprintf(w, "  ... %.0fs elapsed | tx=%d (failed=%d) | %.0f tx/s\n",
				elapsed, tx, fail, tps)
		}
	}
}

// emitOutputs writes the text report to stderr (so it doesn't pollute
// stdout when piped) and any configured JSON/CSV files. Child processes
// in --compare-sha mode emit pure JSON to stdout instead.
func emitOutputs(cfg Config, r *Report) {
	if os.Getenv(childOutputMode) == "1" {
		_ = json.NewEncoder(os.Stdout).Encode(r)
		return
	}

	PrintTextReport(os.Stdout, r)

	stamp := r.GeneratedAt.Format("20060102-150405")
	if cfg.JSONReport {
		path := filepath.Join(cfg.OutputDir, "report-"+stamp+".json")
		if err := WriteJSON(path, r); err != nil {
			fmt.Fprintf(os.Stderr, "warning: write JSON report: %v\n", err)
		} else {
			fmt.Fprintf(os.Stderr, "JSON report:  %s\n", path)
		}
	}
	if cfg.CSVReport {
		path := filepath.Join(cfg.OutputDir, "report-"+stamp+".csv")
		if err := WriteCSV(path, r); err != nil {
			fmt.Fprintf(os.Stderr, "warning: write CSV report: %v\n", err)
		} else {
			fmt.Fprintf(os.Stderr, "CSV report:   %s\n", path)
		}
	}
}

// runCompareMode forks two child processes — one with HW SHA enabled,
// one without — collects both reports, prints a side-by-side diff, and
// writes the individual JSONs to disk for archival.
func runCompareMode(cfg Config, configPath string) error {
	// Clear the flag so the children run in normal single-shot mode.
	cfg.CompareSHA = false

	args := childArgs(configPath)

	hwReport, err := runChild(args, false)
	if err != nil {
		return fmt.Errorf("HW-on run: %w", err)
	}
	swReport, err := runChild(args, true)
	if err != nil {
		return fmt.Errorf("HW-off run: %w", err)
	}

	stamp := time.Now().Format("20060102-150405")
	if cfg.OutputDir != "" {
		_ = WriteJSON(filepath.Join(cfg.OutputDir, "compare-"+stamp+"-hw.json"), hwReport)
		_ = WriteJSON(filepath.Join(cfg.OutputDir, "compare-"+stamp+"-sw.json"), swReport)
	}

	PrintTextReport(os.Stdout, hwReport)
	PrintTextReport(os.Stdout, swReport)
	CompareReports(os.Stdout, hwReport, swReport)
	return nil
}

func runChild(args []string, disableHW bool) (*Report, error) {
	binPath, err := os.Executable()
	if err != nil {
		return nil, err
	}
	c := exec.Command(binPath, args...)
	c.Env = append(os.Environ(), childOutputMode+"=1")
	if disableHW {
		c.Env = append(c.Env, internalSHAReExecMarker+"=1")
		c.Env = append(c.Env, "GODEBUG=cpu.sha=off,cpu.x86.sha=off,cpu.x86.shaext=off,cpu.arm64.sha2=off")
	}
	c.Stderr = os.Stderr
	out, err := c.Output()
	if err != nil {
		return nil, fmt.Errorf("child exec: %w", err)
	}
	r := &Report{}
	if err := json.Unmarshal(out, r); err != nil {
		return nil, fmt.Errorf("child output: %w", err)
	}
	return r, nil
}

// reexecWithoutSHA replaces the current process with a copy that has
// GODEBUG set to disable HW SHA. On success this call does not return.
func reexecWithoutSHA() error {
	binPath, err := os.Executable()
	if err != nil {
		return err
	}
	env := append(os.Environ(),
		internalSHAReExecMarker+"=1",
		"GODEBUG=cpu.sha=off,cpu.x86.sha=off,cpu.x86.shaext=off,cpu.arm64.sha2=off",
	)
	args := append([]string{binPath}, os.Args[1:]...)
	return syscall.Exec(binPath, args, env)
}

// childArgs strips the --compare-sha flag from os.Args so children don't
// recursively spawn more children, but preserves --config and any other
// user-supplied flag.
func childArgs(configPath string) []string {
	args := []string{}
	if configPath != "" {
		args = append(args, "--config", configPath)
	}
	for i := 1; i < len(os.Args); i++ {
		a := os.Args[i]
		if a == "--compare-sha" || a == "-compare-sha" || a == "--compare-sha=true" {
			continue
		}
		// Skip already-passed --config (we re-add it cleanly above).
		if a == "--config" || a == "-config" {
			i++ // skip its argument
			continue
		}
		args = append(args, a)
	}
	return args
}

// stopOnSignal cancels the context on SIGINT/SIGTERM so an interrupted
// run still emits whatever metrics it has.
func stopOnSignal(cancel context.CancelFunc) {
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-ch
		cancel()
	}()
}

// contextWithDuration returns a context that cancels after d, or one
// that never cancels if d <= 0 (used when NumTransactions sets the
// budget).
func contextWithDuration(d time.Duration) (context.Context, context.CancelFunc) {
	if d <= 0 {
		return context.WithCancel(context.Background())
	}
	return context.WithTimeout(context.Background(), d)
}

func fail(format string, args ...any) {
	fmt.Fprintf(os.Stderr, "error: "+format+"\n", args...)
	os.Exit(1)
}

func min(a, b int) int {
	if a == 0 {
		return b
	}
	if a < b {
		return a
	}
	return b
}

