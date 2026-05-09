package main

import (
	"runtime"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/klever-io/klever-go/vmcommon"
)

// Metrics aggregates everything we measure during a benchmark run.
//
// Counters are atomic so generators, processors, and the progress
// reporter can all read/write without locks. The latency histogram is
// guarded by a mutex because reservoir sampling is rarely-but-not-never
// concurrent (per-block bursts share the same goroutine).
type Metrics struct {
	StartedAt time.Time
	StoppedAt time.Time

	// Tx-level
	TxTotal       atomic.Uint64
	TxFailed      atomic.Uint64
	TxTimeout     atomic.Uint64
	SCCallsOK     atomic.Uint64
	SCCallsFail   atomic.Uint64
	TransfersOK   atomic.Uint64
	TransfersFail atomic.Uint64
	GasUsed       atomic.Uint64

	// Phase-level (nanoseconds)
	SigVerifyNs atomic.Uint64
	ExecNs      atomic.Uint64
	FinalizeNs  atomic.Uint64

	// Block-level
	BlocksProcessed atomic.Uint64
	BlockNsTotal    atomic.Uint64
	BlockNsPeak     atomic.Uint64

	// Latency reservoir for per-tx latency stats
	latMu      sync.Mutex
	latencies  []time.Duration
	maxLatBuck int

	// Hashing — owned externally by the timed hasher.
	hashStats *HashStats

	// Throughput sample buffer for the progress + peak-throughput
	// computation. Each entry is (timestamp, txCount).
	tpsMu    sync.Mutex
	tpsRing  []tpsSample
	tpsHead  int
	peakTPS  float64
	peakSCPS float64

	// CPU sample (sys+user) at start, polled at end.
	cpuStartUserNs uint64
	cpuStartSysNs  uint64
	cpuEndUserNs   uint64
	cpuEndSysNs    uint64

	// Budget-mode bookkeeping. Each entry is one slot.
	budgetMu      sync.Mutex
	budgetTxFit   []int
	budgetUsedNs  []uint64
	emptySlots    atomic.Uint64

	// Memory snapshot at end of run.
	endHeapBytes uint64
	endSysBytes  uint64
	endNumGC     uint32
}

type tpsSample struct {
	when    time.Time
	txCount uint64
}

// NewMetrics creates a Metrics ready to record samples for ~maxLatencies
// transactions. The latency reservoir is a simple ring of recent samples
// so we keep memory bounded even on multi-million tx runs.
func NewMetrics(maxLatencies int, hs *HashStats) *Metrics {
	if maxLatencies <= 0 {
		maxLatencies = 100_000
	}
	return &Metrics{
		latencies:  make([]time.Duration, 0, maxLatencies),
		maxLatBuck: maxLatencies,
		tpsRing:    make([]tpsSample, 64),
		hashStats:  hs,
	}
}

// HashStats exposes the underlying counter so the processor can pass it
// to the VM environment.
func (m *Metrics) HashStats() *HashStats { return m.hashStats }

// Start captures the wall-clock start time and the initial CPU usage.
func (m *Metrics) Start() {
	m.StartedAt = time.Now()
	u, s := readCPUTime()
	m.cpuStartUserNs = u
	m.cpuStartSysNs = s
}

// Stop captures end time, final CPU usage, and a memory snapshot.
func (m *Metrics) Stop() {
	m.StoppedAt = time.Now()
	u, s := readCPUTime()
	m.cpuEndUserNs = u
	m.cpuEndSysNs = s

	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	m.endHeapBytes = ms.HeapAlloc
	m.endSysBytes = ms.Sys
	m.endNumGC = ms.NumGC
}

// RecordTxByKind logs the result of a single transaction, splitting
// successful and failed counts by transaction kind so the report can
// distinguish "raw transfer rate" from "SC call rate" — the two scale
// very differently and reporting one global TPS conflates them.
func (m *Metrics) RecordTxByKind(kind TxKind, latency time.Duration, failed bool, gasProvided uint64, out *vmcommon.VMOutput) {
	m.TxTotal.Add(1)
	if failed {
		m.TxFailed.Add(1)
	}
	switch kind {
	case TxKindSCCall:
		if failed {
			m.SCCallsFail.Add(1)
		} else {
			m.SCCallsOK.Add(1)
		}
	case TxKindTransfer:
		if failed {
			m.TransfersFail.Add(1)
		} else {
			m.TransfersOK.Add(1)
		}
	}
	if out != nil && gasProvided >= out.GasRemaining {
		m.GasUsed.Add(gasProvided - out.GasRemaining)
	}

	m.latMu.Lock()
	if len(m.latencies) < m.maxLatBuck {
		m.latencies = append(m.latencies, latency)
	} else {
		// Reservoir-style replacement: overwrite a random-ish slot using
		// the tx counter so we don't have to seed an RNG.
		idx := int(m.TxTotal.Load() % uint64(m.maxLatBuck))
		m.latencies[idx] = latency
	}
	m.latMu.Unlock()
}

// RecordBlock records block-level timing and updates the peak block time.
func (m *Metrics) RecordBlock(d time.Duration, txCount int) {
	m.BlocksProcessed.Add(1)
	ns := uint64(d.Nanoseconds())
	m.BlockNsTotal.Add(ns)
	for {
		old := m.BlockNsPeak.Load()
		if ns <= old {
			break
		}
		if m.BlockNsPeak.CompareAndSwap(old, ns) {
			break
		}
	}

	// Sample point-in-time TPS for the peak-throughput line. We compare
	// against the previous sample to convert (cumulative tx, time) into
	// instantaneous rate.
	m.tpsMu.Lock()
	now := time.Now()
	tx := m.TxTotal.Load()
	prev := m.tpsRing[m.tpsHead]
	m.tpsHead = (m.tpsHead + 1) % len(m.tpsRing)
	m.tpsRing[m.tpsHead] = tpsSample{when: now, txCount: tx}
	if !prev.when.IsZero() {
		dt := now.Sub(prev.when).Seconds()
		if dt > 0 {
			rate := float64(tx-prev.txCount) / dt
			if rate > m.peakTPS {
				m.peakTPS = rate
			}
			scOK := m.SCCallsOK.Load()
			scRate := float64(scOK) / now.Sub(m.StartedAt).Seconds()
			if scRate > m.peakSCPS {
				m.peakSCPS = scRate
			}
		}
	}
	m.tpsMu.Unlock()
}

// RecordBudgetSlot logs the result of one budget-mode slot: how many
// txs fit and how long the block took. The deadline parameter is kept
// for symmetry with future "missed deadline" alerting.
func (m *Metrics) RecordBudgetSlot(fit int, used time.Duration, _ time.Duration) {
	m.budgetMu.Lock()
	m.budgetTxFit = append(m.budgetTxFit, fit)
	m.budgetUsedNs = append(m.budgetUsedNs, uint64(used.Nanoseconds()))
	m.budgetMu.Unlock()
}

// RecordEmptySlot bumps the counter of slots where the mempool was dry.
// In a real chain that's an idle validator — useful to surface here so
// users tune --prefill or --concurrency until the count is zero.
func (m *Metrics) RecordEmptySlot() { m.emptySlots.Add(1) }

// BudgetSlots returns a copy of the per-slot fit/used vectors so the
// reporter can compute min/avg/max without holding the lock.
func (m *Metrics) BudgetSlots() (fit []int, usedNs []uint64) {
	m.budgetMu.Lock()
	defer m.budgetMu.Unlock()
	fit = make([]int, len(m.budgetTxFit))
	copy(fit, m.budgetTxFit)
	usedNs = make([]uint64, len(m.budgetUsedNs))
	copy(usedNs, m.budgetUsedNs)
	return
}

// EmptySlots returns the count of mempool-starved slots.
func (m *Metrics) EmptySlots() uint64 { return m.emptySlots.Load() }

// LatencyPercentiles returns p50/p95/p99/avg/max latencies in time.Duration.
// It sorts a copy of the reservoir so callers can compute these values
// without disturbing live samples.
func (m *Metrics) LatencyPercentiles() (p50, p95, p99, avg, max time.Duration) {
	m.latMu.Lock()
	if len(m.latencies) == 0 {
		m.latMu.Unlock()
		return
	}
	cp := make([]time.Duration, len(m.latencies))
	copy(cp, m.latencies)
	m.latMu.Unlock()

	sort.Slice(cp, func(i, j int) bool { return cp[i] < cp[j] })
	pick := func(pct float64) time.Duration {
		idx := int(float64(len(cp)) * pct)
		if idx >= len(cp) {
			idx = len(cp) - 1
		}
		return cp[idx]
	}
	p50 = pick(0.50)
	p95 = pick(0.95)
	p99 = pick(0.99)
	max = cp[len(cp)-1]
	var sum time.Duration
	for _, d := range cp {
		sum += d
	}
	avg = sum / time.Duration(len(cp))
	return
}

// CPUUsageSeconds returns user and system CPU time consumed during the run.
func (m *Metrics) CPUUsageSeconds() (user, sys float64) {
	user = float64(m.cpuEndUserNs-m.cpuStartUserNs) / 1e9
	sys = float64(m.cpuEndSysNs-m.cpuStartSysNs) / 1e9
	return
}

// MemorySnapshot returns the heap/sys/num_gc captured at Stop().
func (m *Metrics) MemorySnapshot() (heap, sys uint64, numGC uint32) {
	return m.endHeapBytes, m.endSysBytes, m.endNumGC
}

// PeakThroughput returns the highest sustained TPS observed across the
// rolling window, plus the same for SC calls.
func (m *Metrics) PeakThroughput() (tps, scps float64) {
	m.tpsMu.Lock()
	defer m.tpsMu.Unlock()
	return m.peakTPS, m.peakSCPS
}

// Duration returns total wall-clock duration of the run.
func (m *Metrics) Duration() time.Duration {
	if m.StoppedAt.IsZero() {
		return time.Since(m.StartedAt)
	}
	return m.StoppedAt.Sub(m.StartedAt)
}

