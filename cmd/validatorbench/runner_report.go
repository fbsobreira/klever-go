package main

// runner_report.go converts a BenchRunner's RunReport (raw per-slot
// observations from the production preprocessor) into the bench's
// existing Report struct, so the text/JSON/CSV exporters can emit
// the same shape we've always emitted.

import (
	"time"
)

// reportFromRunReport projects a RunReport onto the existing Report
// type used by report.go, hashing.go etc. Everything in this projection
// comes from the production-pipeline run — no synthetic numbers.
func reportFromRunReport(cfg Config, rep *RunReport) *Report {
	out := &Report{
		GeneratedAt: time.Now(),
		Config:      cfg,
		System:      currentSystem(),
		Hashing:     DetectHashAccel(cfg.HashAlgo),
	}

	dur := rep.StoppedAt.Sub(rep.StartedAt)
	if dur <= 0 {
		dur = time.Second
	}
	out.DurationSec = dur.Seconds()
	out.TxTotal = rep.TotalTx
	out.TxFailed = rep.TotalFailed
	out.BlocksTotal = uint64(len(rep.Slots))
	out.AvgTPS = float64(rep.TotalTx) / out.DurationSec

	// Budget-mode aggregates.
	out.BudgetMode = true
	out.BlockTimeMs = float64(rep.SlotInterval) / 1e6
	out.BlockBudgetMs = float64(rep.BlockBudget) / 1e6
	out.SlotsTotal = len(rep.Slots)

	if len(rep.Slots) == 0 {
		out.Bottleneck = "no slots executed"
		out.ValidatorImpact = "Insufficient — no measurable activity"
		return out
	}

	var (
		fitSum, usedSum int64
		fitMin, fitMax  int
		usedMax         int64
		emptySlots      uint64
	)
	fitMin = rep.Slots[0].TxFit
	for _, s := range rep.Slots {
		fitSum += int64(s.TxFit)
		usedSum += s.UsedNs
		if s.TxFit < fitMin {
			fitMin = s.TxFit
		}
		if s.TxFit > fitMax {
			fitMax = s.TxFit
		}
		if s.UsedNs > usedMax {
			usedMax = s.UsedNs
		}
		if s.TxFit == 0 {
			emptySlots++
		}
	}
	out.SlotsEmpty = emptySlots
	out.TxPerBlockMin = fitMin
	out.TxPerBlockMax = fitMax
	out.TxPerBlockAvg = float64(fitSum) / float64(len(rep.Slots))
	out.BlockBudgetUsedAvgPct = (float64(usedSum) / float64(len(rep.Slots)) / float64(rep.BlockBudget.Nanoseconds())) * 100
	out.BlockBudgetUsedMaxPct = (float64(usedMax) / float64(rep.BlockBudget.Nanoseconds())) * 100
	out.EffectiveMaxTPS = out.TxPerBlockAvg / rep.SlotInterval.Seconds()

	out.BlockTimeAvgMs = float64(usedSum/int64(len(rep.Slots))) / 1e6
	out.BlockTimePeakMs = float64(usedMax) / 1e6

	out.SigVerifyIntakeMs = float64(rep.IntakeVerifyNs) / 1e6
	out.ExecMs = float64(usedSum) / 1e6
	out.PeakTPS = 0
	if len(rep.Slots) > 0 {
		// Per-slot peak TPS: max(TxFit) / SlotInterval. This is the
		// best single-slot rate we observed.
		out.PeakTPS = float64(fitMax) / rep.SlotInterval.Seconds()
		out.PeakSCPS = out.PeakTPS // distinguished by workload at the
	}

	// Bottleneck heuristic.
	if out.BlockBudgetUsedAvgPct >= 95 {
		out.Bottleneck = "tx execution (production preprocessor saturated within budget)"
	} else if emptySlots > 0 {
		out.Bottleneck = "intake (mempool starved — increase prefill / concurrency)"
	} else {
		out.Bottleneck = "balanced (budget not saturated, mempool not starved)"
	}
	out.ValidatorImpact = classifyImpact(out)

	return out
}
