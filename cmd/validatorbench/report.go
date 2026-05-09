package main

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"time"
)

// SystemInfo captures the host environment so a report can be reproduced
// on a different machine without ambiguity.
type SystemInfo struct {
	GOOS      string `json:"goos"`
	GOARCH    string `json:"goarch"`
	GoVersion string `json:"go_version"`
	NumCPU    int    `json:"num_cpu"`
	Hostname  string `json:"hostname"`
}

func currentSystem() SystemInfo {
	host, _ := os.Hostname()
	return SystemInfo{
		GOOS:      runtime.GOOS,
		GOARCH:    runtime.GOARCH,
		GoVersion: runtime.Version(),
		NumCPU:    runtime.NumCPU(),
		Hostname:  host,
	}
}

// Report is the canonical end-of-run summary. It serialises cleanly to
// JSON, but also feeds the human-readable text printer and the CSV
// exporter — keep field types JSON-friendly.
type Report struct {
	GeneratedAt time.Time   `json:"generated_at"`
	Config      Config      `json:"config"`
	System      SystemInfo  `json:"system"`
	Hashing     HashAccelInfo `json:"hashing"`

	DurationSec    float64 `json:"duration_seconds"`
	TxTotal        uint64  `json:"tx_total"`
	TxFailed       uint64  `json:"tx_failed"`
	SCCallsOK      uint64  `json:"sc_calls_ok"`
	SCCallsFailed  uint64  `json:"sc_calls_failed"`
	TransfersOK    uint64  `json:"transfers_ok"`
	TransfersFail  uint64  `json:"transfers_failed"`
	BlocksTotal    uint64  `json:"blocks_total"`

	AvgTPS         float64 `json:"avg_tps"`
	PeakTPS        float64 `json:"peak_tps"`
	AvgSCPS        float64 `json:"avg_scps"`
	PeakSCPS       float64 `json:"peak_scps"`
	AvgTransferPS  float64 `json:"avg_transfer_ps"`

	BlockTimeAvgMs  float64 `json:"block_time_avg_ms"`
	BlockTimePeakMs float64 `json:"block_time_peak_ms"`

	LatencyAvgMs float64 `json:"latency_avg_ms"`
	LatencyP50Ms float64 `json:"latency_p50_ms"`
	LatencyP95Ms float64 `json:"latency_p95_ms"`
	LatencyP99Ms float64 `json:"latency_p99_ms"`
	LatencyMaxMs float64 `json:"latency_max_ms"`

	HashCount      uint64  `json:"hash_count"`
	HashBytes      uint64  `json:"hash_bytes"`
	HashTimeMs     float64 `json:"hash_time_ms"`
	HashPctOfTotal float64 `json:"hash_pct_of_total"`

	SigVerifyMs       float64 `json:"sig_verify_ms"`
	SigVerifyIntakeMs float64 `json:"sig_verify_intake_ms"`
	ExecMs      float64 `json:"exec_ms"`
	FinalizeMs  float64 `json:"finalize_ms"`

	CPUUserSec float64 `json:"cpu_user_sec"`
	CPUSysSec  float64 `json:"cpu_sys_sec"`
	CPUPct     float64 `json:"cpu_pct"`

	HeapBytes uint64 `json:"heap_bytes"`
	SysBytes  uint64 `json:"sys_bytes"`
	NumGC     uint32 `json:"num_gc"`

	Bottleneck      string  `json:"bottleneck"`
	FailureRate     float64 `json:"failure_rate"`
	TimeoutRate     float64 `json:"timeout_rate"`
	ValidatorImpact string  `json:"validator_impact"`

	// Cold-cache costs paid once per process restart per contract.
	// Cold calls are first invocations after deploy, before any caches
	// are warm. Operators size capacity around these numbers because
	// they dominate the worst-case block.
	DeployCount       int     `json:"deploy_count"`
	DeployAvgMs       float64 `json:"deploy_avg_ms"`
	DeployMaxMs       float64 `json:"deploy_max_ms"`
	ColdCallCount     int     `json:"cold_call_count"`
	ColdCallAvgMs     float64 `json:"cold_call_avg_ms"`
	ColdCallMaxMs     float64 `json:"cold_call_max_ms"`
	ColdCallMinMs     float64 `json:"cold_call_min_ms"`

	// Budget-mode results. Populated only when BlockTime + BlockBudget
	// are configured; otherwise EffectiveMaxTPS == 0.
	BudgetMode          bool    `json:"budget_mode"`
	BlockTimeMs         float64 `json:"block_time_ms"`
	BlockBudgetMs       float64 `json:"block_budget_ms"`
	SlotsTotal          int     `json:"slots_total"`
	SlotsEmpty          uint64  `json:"slots_empty"`
	TxPerBlockMin       int     `json:"tx_per_block_min"`
	TxPerBlockAvg       float64 `json:"tx_per_block_avg"`
	TxPerBlockMax       int     `json:"tx_per_block_max"`
	BlockBudgetUsedAvgPct float64 `json:"block_budget_used_avg_pct"`
	BlockBudgetUsedMaxPct float64 `json:"block_budget_used_max_pct"`
	EffectiveMaxTPS     float64 `json:"effective_max_tps"`
}

// (BuildReport, populateColdCalls, populateBudgetMode, identifyBottleneck
// removed in the production-pipeline refactor — runner_report.go now
// builds the Report directly from RunReport, the per-slot observation
// that comes out of the production preprocessor.)

// classifyImpact maps the observed throughput onto a coarse expected-
// behaviour bucket the operator can correlate to capacity planning.
func classifyImpact(r *Report) string {
	switch {
	case r.AvgTPS >= 5000:
		return "Excellent — sustains high-throughput chain workloads"
	case r.AvgTPS >= 1500:
		return "Good — production-ready for normal validator load"
	case r.AvgTPS >= 500:
		return "Acceptable — meets minimum validator requirements"
	case r.AvgTPS >= 100:
		return "Marginal — investigate before promoting to producer node"
	default:
		return "Insufficient — hardware unlikely to keep up with mainnet"
	}
}

// PrintTextReport writes a human-readable summary to w.
func PrintTextReport(w io.Writer, r *Report) {
	fmt.Fprintln(w, "==========================================================")
	fmt.Fprintln(w, " Klever Validator Throughput Benchmark")
	fmt.Fprintln(w, "==========================================================")
	fmt.Fprintf(w, " Generated:    %s\n", r.GeneratedAt.Format(time.RFC3339))
	fmt.Fprintf(w, " Host:         %s (%s/%s, %d CPUs, %s)\n",
		r.System.Hostname, r.System.GOOS, r.System.GOARCH, r.System.NumCPU, r.System.GoVersion)
	fmt.Fprintf(w, " Workload:     %s\n", r.Config.Workload)
	fmt.Fprintf(w, " Contract:     %s\n", r.Config.ContractPath)
	fmt.Fprintf(w, " Hash algo:    %s (HW accel: avail=%t used=%t — %s)\n",
		r.Hashing.Algorithm, r.Hashing.HWAccelAvail, r.Hashing.HWAccelUsed, r.Hashing.Notes)
	fmt.Fprintf(w, " Block size:   %d   Concurrency: %d\n", r.Config.BlockSize, r.Config.Concurrency)
	// Use DurationStr — it round-trips through JSON, unlike Config.Duration
	// (which is tagged `json:"-"` to keep the file form human-readable).
	fmt.Fprintf(w, " Duration:     %s   Wall:        %.2fs\n",
		r.Config.DurationStr, r.DurationSec)
	fmt.Fprintln(w, "----------------------------------------------------------")
	fmt.Fprintln(w, " Throughput")
	fmt.Fprintf(w, "   Transactions:  %d total, %d failed (%.2f%% fail)\n", r.TxTotal, r.TxFailed, r.FailureRate)
	fmt.Fprintf(w, "   Avg TPS:       %.1f       Peak TPS:    %.1f\n", r.AvgTPS, r.PeakTPS)
	fmt.Fprintf(w, "   Avg SC/s:      %.1f       Peak SC/s:   %.1f\n", r.AvgSCPS, r.PeakSCPS)
	if r.TransfersOK+r.TransfersFail > 0 {
		fmt.Fprintf(w, "   Avg Transfer/s: %.1f      Transfers OK/Fail: %d/%d\n",
			r.AvgTransferPS, r.TransfersOK, r.TransfersFail)
	}
	fmt.Fprintf(w, "   Blocks:        %d (avg %.2f ms, peak %.2f ms)\n",
		r.BlocksTotal, r.BlockTimeAvgMs, r.BlockTimePeakMs)
	fmt.Fprintln(w, " Latency")
	fmt.Fprintf(w, "   avg %.3f ms | p50 %.3f | p95 %.3f | p99 %.3f | max %.3f\n",
		r.LatencyAvgMs, r.LatencyP50Ms, r.LatencyP95Ms, r.LatencyP99Ms, r.LatencyMaxMs)
	fmt.Fprintln(w, " Phase breakdown (cumulative across run)")
	fmt.Fprintf(w, "   Intake verify:   %.2f ms (off-budget; mempool path)\n", r.SigVerifyIntakeMs)
	fmt.Fprintln(w, "   --- on the per-block CPU budget ---")
	fmt.Fprintf(w, "   Tx execution:    %.2f ms (%.1f%%)\n",
		r.ExecMs, pct(r.ExecMs, r.ExecMs+r.FinalizeMs))
	fmt.Fprintf(w, "   Block finalize:  %.2f ms (%.1f%%)\n",
		r.FinalizeMs, pct(r.FinalizeMs, r.ExecMs+r.FinalizeMs))
	fmt.Fprintf(w, "   Hashing:       %.2f ms (%.1f%% of total)\n", r.HashTimeMs, r.HashPctOfTotal)
	fmt.Fprintf(w, "   Hashes:        %d (%.2f MB hashed)\n", r.HashCount, float64(r.HashBytes)/(1024*1024))
	fmt.Fprintln(w, " System")
	fmt.Fprintf(w, "   CPU user/sys:  %.2fs / %.2fs (~%.1f%% of single core)\n",
		r.CPUUserSec, r.CPUSysSec, r.CPUPct)
	fmt.Fprintf(w, "   Heap:          %.2f MB    Sys: %.2f MB    GC cycles: %d\n",
		float64(r.HeapBytes)/(1024*1024), float64(r.SysBytes)/(1024*1024), r.NumGC)
	if r.DeployCount > 0 || r.ColdCallCount > 0 {
		fmt.Fprintln(w, "----------------------------------------------------------")
		fmt.Fprintln(w, " Cold-cache costs (paid once per process restart per contract)")
		if r.DeployCount > 0 {
			fmt.Fprintf(w, "   Deploy:            %d contracts, avg %.2f ms, max %.2f ms\n",
				r.DeployCount, r.DeployAvgMs, r.DeployMaxMs)
		}
		if r.ColdCallCount > 0 {
			fmt.Fprintf(w, "   First call:        %d contracts, avg %.3f ms, min %.3f ms, max %.3f ms\n",
				r.ColdCallCount, r.ColdCallAvgMs, r.ColdCallMinMs, r.ColdCallMaxMs)
		}
	}
	if r.BudgetMode {
		fmt.Fprintln(w, "----------------------------------------------------------")
		fmt.Fprintln(w, " Block-budget mode (chain-realistic ceiling)")
		fmt.Fprintf(w, "   Block time:        %.0f ms (slot interval)\n", r.BlockTimeMs)
		fmt.Fprintf(w, "   Block budget:      %.0f ms (max processing time per block)\n", r.BlockBudgetMs)
		fmt.Fprintf(w, "   Slots:             %d total, %d empty (mempool starved)\n",
			r.SlotsTotal, r.SlotsEmpty)
		fmt.Fprintf(w, "   Tx per block:      avg %.1f, min %d, max %d\n",
			r.TxPerBlockAvg, r.TxPerBlockMin, r.TxPerBlockMax)
		fmt.Fprintf(w, "   Budget used:       avg %.1f%%, max %.1f%%\n",
			r.BlockBudgetUsedAvgPct, r.BlockBudgetUsedMaxPct)
		fmt.Fprintf(w, "   EFFECTIVE MAX TPS: %.0f tx/s (%.1f tx/block ÷ %.1fs)\n",
			r.EffectiveMaxTPS, r.TxPerBlockAvg, r.BlockTimeMs/1000)
	}
	fmt.Fprintln(w, "----------------------------------------------------------")
	fmt.Fprintln(w, " Summary")
	fmt.Fprintf(w, "   Bottleneck:        %s\n", r.Bottleneck)
	fmt.Fprintf(w, "   Validator impact:  %s\n", r.ValidatorImpact)
	fmt.Fprintln(w, "==========================================================")
}

func pct(part, total float64) float64 {
	if total <= 0 {
		return 0
	}
	return (part / total) * 100
}

// WriteJSON saves the report to disk.
func WriteJSON(path string, r *Report) error {
	if path == "" {
		return nil
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()
	enc := json.NewEncoder(f)
	enc.SetIndent("", "  ")
	return enc.Encode(r)
}

// WriteCSV saves the headline numbers in a flat CSV row, easy to ingest
// into a spreadsheet or time-series store.
func WriteCSV(path string, r *Report) error {
	if path == "" {
		return nil
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()
	w := csv.NewWriter(f)
	defer w.Flush()

	row := func(k, v string) error { return w.Write([]string{k, v}) }
	if err := w.Write([]string{"metric", "value"}); err != nil {
		return err
	}
	flds := []struct {
		k string
		v string
	}{
		{"generated_at", r.GeneratedAt.Format(time.RFC3339)},
		{"workload", r.Config.Workload},
		{"hash_algo", r.Hashing.Algorithm},
		{"hw_accel_used", strconv.FormatBool(r.Hashing.HWAccelUsed)},
		{"duration_sec", strconv.FormatFloat(r.DurationSec, 'f', 4, 64)},
		{"tx_total", strconv.FormatUint(r.TxTotal, 10)},
		{"tx_failed", strconv.FormatUint(r.TxFailed, 10)},
		{"sc_calls_ok", strconv.FormatUint(r.SCCallsOK, 10)},
		{"avg_tps", strconv.FormatFloat(r.AvgTPS, 'f', 2, 64)},
		{"peak_tps", strconv.FormatFloat(r.PeakTPS, 'f', 2, 64)},
		{"avg_scps", strconv.FormatFloat(r.AvgSCPS, 'f', 2, 64)},
		{"peak_scps", strconv.FormatFloat(r.PeakSCPS, 'f', 2, 64)},
		{"block_time_avg_ms", strconv.FormatFloat(r.BlockTimeAvgMs, 'f', 3, 64)},
		{"block_time_peak_ms", strconv.FormatFloat(r.BlockTimePeakMs, 'f', 3, 64)},
		{"latency_avg_ms", strconv.FormatFloat(r.LatencyAvgMs, 'f', 4, 64)},
		{"latency_p50_ms", strconv.FormatFloat(r.LatencyP50Ms, 'f', 4, 64)},
		{"latency_p95_ms", strconv.FormatFloat(r.LatencyP95Ms, 'f', 4, 64)},
		{"latency_p99_ms", strconv.FormatFloat(r.LatencyP99Ms, 'f', 4, 64)},
		{"latency_max_ms", strconv.FormatFloat(r.LatencyMaxMs, 'f', 4, 64)},
		{"hash_count", strconv.FormatUint(r.HashCount, 10)},
		{"hash_bytes", strconv.FormatUint(r.HashBytes, 10)},
		{"hash_time_ms", strconv.FormatFloat(r.HashTimeMs, 'f', 3, 64)},
		{"hash_pct_of_total", strconv.FormatFloat(r.HashPctOfTotal, 'f', 2, 64)},
		{"sig_verify_ms", strconv.FormatFloat(r.SigVerifyMs, 'f', 3, 64)},
		{"exec_ms", strconv.FormatFloat(r.ExecMs, 'f', 3, 64)},
		{"finalize_ms", strconv.FormatFloat(r.FinalizeMs, 'f', 3, 64)},
		{"cpu_user_sec", strconv.FormatFloat(r.CPUUserSec, 'f', 3, 64)},
		{"cpu_sys_sec", strconv.FormatFloat(r.CPUSysSec, 'f', 3, 64)},
		{"cpu_pct", strconv.FormatFloat(r.CPUPct, 'f', 1, 64)},
		{"heap_bytes", strconv.FormatUint(r.HeapBytes, 10)},
		{"sys_bytes", strconv.FormatUint(r.SysBytes, 10)},
		{"num_gc", strconv.FormatUint(uint64(r.NumGC), 10)},
		{"bottleneck", r.Bottleneck},
		{"validator_impact", r.ValidatorImpact},
		{"failure_rate_pct", strconv.FormatFloat(r.FailureRate, 'f', 3, 64)},
	}
	for _, f := range flds {
		if err := row(f.k, f.v); err != nil {
			return err
		}
	}
	return nil
}

// CompareReports prints a diff between two reports — typically the HW-on
// and HW-off back-to-back runs of --compare-sha. The second report wins
// the "Δ" column.
func CompareReports(w io.Writer, a, b *Report) {
	fmt.Fprintln(w, "==========================================================")
	fmt.Fprintln(w, " SHA Hardware Acceleration — Comparative Summary")
	fmt.Fprintln(w, "==========================================================")
	fmt.Fprintf(w, " A: HW used=%t   B: HW used=%t\n", a.Hashing.HWAccelUsed, b.Hashing.HWAccelUsed)
	fmt.Fprintln(w, "----------------------------------------------------------")
	dlt := func(label string, va, vb float64, unit string, betterUp bool) {
		delta := vb - va
		var arrow string
		if (delta > 0) == betterUp {
			arrow = "+"
		} else {
			arrow = "-"
		}
		_ = arrow
		pct := 0.0
		if va != 0 {
			pct = (delta / va) * 100
		}
		fmt.Fprintf(w, "   %-22s A=%-12.2f B=%-12.2f  Δ=%+.2f %s (%+.1f%%)\n",
			label, va, vb, delta, unit, pct)
	}
	dlt("Avg TPS", a.AvgTPS, b.AvgTPS, "tx/s", true)
	dlt("Peak TPS", a.PeakTPS, b.PeakTPS, "tx/s", true)
	dlt("Hash time", a.HashTimeMs, b.HashTimeMs, "ms", false)
	dlt("Hash pct of total", a.HashPctOfTotal, b.HashPctOfTotal, "%", false)
	dlt("Block avg", a.BlockTimeAvgMs, b.BlockTimeAvgMs, "ms", false)
	dlt("Latency p99", a.LatencyP99Ms, b.LatencyP99Ms, "ms", false)
	dlt("CPU usage", a.CPUPct, b.CPUPct, "%", false)
	fmt.Fprintln(w, "==========================================================")
}
