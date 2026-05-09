package main

import (
	"context"
	"encoding/json"
	"fmt"
	"math/big"
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

// runOnce sets up the BenchNode (production tx pipeline), pre-funds
// accounts, builds the workload, and drives the BenchRunner under
// the configured slot clock + per-block budget. Returns a Report.
//
// Everything in the measured path goes through the production code:
// shardedTxPool, txProcessor, scProcessor, preprocess.transactions.
func runOnce(cfg Config) (*Report, error) {
	bn, err := NewBenchNode(cfg)
	if err != nil {
		return nil, fmt.Errorf("set up bench node: %w", err)
	}
	defer bn.Close()

	// Provision sender accounts. The runner needs at least
	// concurrency*2 senders for the transfer workload (sender +
	// recipient per worker), so size up generously.
	numAccounts := cfg.NumAccounts
	if numAccounts < cfg.Concurrency*4 {
		numAccounts = cfg.Concurrency * 4
	}
	if err := bn.ProvisionAccounts(numAccounts, cfg.InitialBalance); err != nil {
		return nil, fmt.Errorf("provision accounts: %w", err)
	}
	// The owner pays for SC deploys. ProvisionAccounts only funds the
	// sender pool; fund the owner separately so SC workloads don't fail
	// on the deploy fee check.
	if err := bn.FundAccount(bn.Owner().Address[:], cfg.InitialBalance); err != nil {
		return nil, fmt.Errorf("fund owner: %w", err)
	}
	if err := bn.CommitState(); err != nil {
		return nil, fmt.Errorf("commit owner funding: %w", err)
	}

	wl, err := selectProdWorkload(cfg, bn)
	if err != nil {
		return nil, err
	}

	ctx, cancel := contextWithDuration(cfg.Duration)
	defer cancel()
	stopOnSignal(cancel)

	// Slot clock + per-block budget come from config; defaults are
	// klever mainnet (4s / 500ms).
	slot := cfg.BlockTime
	if slot <= 0 {
		slot = 4 * time.Second
	}
	budget := cfg.BlockBudget
	if budget <= 0 {
		budget = 500 * time.Millisecond
	}

	runner := NewBenchRunner(bn, wl, slot, budget, cfg.PrefillMempool, cfg.Concurrency)
	rep, err := runner.Run(ctx, cfg.Duration)
	if err != nil {
		return nil, fmt.Errorf("runner: %w", err)
	}

	return reportFromRunReport(cfg, rep), nil
}

// selectProdWorkload picks a BenchWorkload implementation based on the
// config. tx_mix isn't yet supported by the production path; for now
// the simple workload field selects between transfer and sc-call.
func selectProdWorkload(cfg Config, bn *BenchNode) (BenchWorkload, error) {
	switch cfg.Workload {
	case "transfer":
		return NewProdTransferWorkload(bn, cfg.Concurrency, 1)
	case "sc-call":
		if cfg.ContractPath == "" {
			return nil, fmt.Errorf("sc-call workload needs contract_path")
		}
		// Convert init/call args from the config templates. Production
		// args are []byte; numeric strings become big-endian ints.
		initArgs := encodeProdArgs(cfg.InitArgs)
		callArgs := encodeProdArgs(cfg.CallArgsTpl)
		instances := cfg.NumContracts
		if instances < 1 {
			instances = 1
		}
		return NewProdSCCallWorkload(bn, cfg.ContractPath, initArgs, cfg.CallFunction, callArgs, instances, cfg.GasLimit, cfg.Concurrency)
	default:
		return nil, fmt.Errorf("workload %q not yet supported on production path (use transfer or sc-call)", cfg.Workload)
	}
}

// encodeProdArgs translates the config's argument templates into the
// raw []byte form the production tx-build path expects. Numeric
// strings become big-endian unsigned bigints; everything else is
// treated as raw bytes.
func encodeProdArgs(in []string) [][]byte {
	out := make([][]byte, 0, len(in))
	for _, s := range in {
		if s == "" {
			out = append(out, []byte{})
			continue
		}
		if n, ok := new(big.Int).SetString(s, 10); ok {
			out = append(out, n.Bytes())
			continue
		}
		out = append(out, []byte(s))
	}
	return out
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

