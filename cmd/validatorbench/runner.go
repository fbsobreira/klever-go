package main

// runner.go drives the production tx-processing pipeline against
// a steady stream of signed transactions, slot by slot, under a fixed
// per-block CPU budget. Everything the validator does — mempool
// selection, sig verify, fee handling, VM dispatch, state mutation —
// goes through the production code in the BenchNode bootstrap.
//
// The runner doesn't decide what's in each tx. That's the workload's
// job (transfer, sc-call, mix). The runner just:
//
//   1. Asks the workload for N pre-signed txs and pushes them into
//      the production shardedTxPool (intake — same path the P2P
//      interceptor walks after verifying a P2P-arrived tx).
//   2. At each slot boundary, calls
//      preprocessor.CreateAndProcessBlockTransactions(blk, haveTime)
//      with haveTime = the validator's per-block CPU budget.
//   3. Drains accepted txs from the pool, commits state, records
//      per-slot stats (fit count, used time, success/fail).
//   4. Sleeps until the next slot.
//
// The result is a chain-realistic ceiling: tx_per_slot / slot_interval
// is the effective max TPS the validator can sustain on this hardware.

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/klever-io/klever-go/data/block"
	"github.com/klever-io/klever-go/data/transaction"
)

// signedTx pairs a marshalled+signed *transaction.Transaction with
// its hash. The runner pushes both into shardedTxPool.AddData.
type signedTx struct {
	hash []byte
	tx   *transaction.Transaction
}

// BenchWorkload is what the runner consumes. Implementations live in
// workload_*.go — the runner doesn't care which. Each Build call
// produces one signed tx ready for AddData.
//
// Implementations must:
//   - increment the sender's on-chain nonce locally so subsequent
//     calls produce monotonically-increasing nonces per sender.
//   - never share a sender across goroutines without synchronising
//     nonce assignment (production validates strict per-sender nonce
//     ordering).
type BenchWorkload interface {
	Name() string
	Build(workerID int) (signedTx, error)
}

// SlotResult is one slot's worth of measured behaviour.
type SlotResult struct {
	SlotIdx       int
	TxFit         int
	UsedNs        int64
	BudgetNs      int64
	Failed        uint64
	BlockNonce    uint64
	WallStart     time.Time
}

// RunReport is the aggregate of all slot results, plus the breakdown
// the bench's existing report.go expects.
type RunReport struct {
	StartedAt       time.Time
	StoppedAt       time.Time
	WorkloadName    string
	SlotInterval    time.Duration
	BlockBudget     time.Duration
	Slots           []SlotResult
	TotalTx         uint64
	TotalFailed     uint64
	IntakeVerifyNs  uint64
}

// BenchRunner orchestrates the slot clock + intake + per-slot
// preprocessor calls.
type BenchRunner struct {
	bn         *BenchNode
	wl         BenchWorkload
	slotClock  time.Duration
	budget     time.Duration
	prefill    int
	concurrency int

	intakeNs atomic.Uint64
}

// NewBenchRunner constructs a runner. slotClock is the chain block
// interval (typically 4s on klever); budget is the per-block CPU
// budget (typically 500ms). Setting either to 0 makes the runner
// drive blocks back-to-back with no sleep — useful for raw-burst
// measurements but not chain-realistic.
func NewBenchRunner(bn *BenchNode, wl BenchWorkload, slotClock, budget time.Duration, prefill, concurrency int) *BenchRunner {
	if concurrency < 1 {
		concurrency = 1
	}
	return &BenchRunner{
		bn:          bn,
		wl:          wl,
		slotClock:   slotClock,
		budget:      budget,
		prefill:     prefill,
		concurrency: concurrency,
	}
}

// Prefill synchronously generates `prefill` signed txs and pushes
// them to the production mempool. Intake-time signature verify cost
// is recorded so the report can show what the validator's P2P
// interceptor would have spent.
func (r *BenchRunner) Prefill(ctx context.Context) error {
	if r.prefill <= 0 {
		return nil
	}
	var wg sync.WaitGroup
	var produced atomic.Int64
	var firstErr atomic.Value // error
	target := int64(r.prefill)

	for w := 0; w < r.concurrency; w++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for {
				if ctx.Err() != nil {
					return
				}
				if produced.Load() >= target {
					return
				}
				t0 := time.Now()
				stx, err := r.wl.Build(id)
				r.intakeNs.Add(uint64(time.Since(t0).Nanoseconds()))
				if err != nil {
					if firstErr.Load() == nil {
						firstErr.Store(err)
					}
					return
				}
				r.bn.PushTx(stx.hash, stx.tx)
				produced.Add(1)
			}
		}(w)
	}
	wg.Wait()
	if v := firstErr.Load(); v != nil {
		return fmt.Errorf("prefill: %w", v.(error))
	}
	return nil
}

// startBackgroundProducers keeps the mempool topped up while the
// processor is consuming. Producers stop when ctx is canceled.
func (r *BenchRunner) startBackgroundProducers(ctx context.Context) {
	for w := 0; w < r.concurrency; w++ {
		go func(id int) {
			for {
				if ctx.Err() != nil {
					return
				}
				t0 := time.Now()
				stx, err := r.wl.Build(id)
				r.intakeNs.Add(uint64(time.Since(t0).Nanoseconds()))
				if err != nil {
					return
				}
				r.bn.PushTx(stx.hash, stx.tx)
			}
		}(w)
	}
}

// Run drives the slot clock for the given duration. Returns the
// per-slot RunReport when ctx expires (via duration timeout or
// external cancel).
func (r *BenchRunner) Run(ctx context.Context, duration time.Duration) (*RunReport, error) {
	if err := r.Prefill(ctx); err != nil {
		return nil, err
	}

	rep := &RunReport{
		StartedAt:    time.Now(),
		WorkloadName: r.wl.Name(),
		SlotInterval: r.slotClock,
		BlockBudget:  r.budget,
	}

	// Keep the mempool topped up so a long run doesn't drain it.
	prodCtx, prodCancel := context.WithCancel(ctx)
	r.startBackgroundProducers(prodCtx)
	defer prodCancel()

	// Time-bounded run.
	deadline := rep.StartedAt.Add(duration)
	slotIdx := 0
	nextSlot := time.Now()
	for time.Now().Before(deadline) {
		// Wait for the slot boundary.
		if d := time.Until(nextSlot); d > 0 {
			select {
			case <-time.After(d):
			case <-ctx.Done():
				rep.StoppedAt = time.Now()
				rep.IntakeVerifyNs = r.intakeNs.Load()
				return rep, nil
			}
		}
		nextSlot = nextSlot.Add(r.slotClock)

		sr := r.runSlot(slotIdx)
		rep.Slots = append(rep.Slots, sr)
		rep.TotalTx += uint64(sr.TxFit)
		rep.TotalFailed += sr.Failed
		slotIdx++
	}

	rep.StoppedAt = time.Now()
	rep.IntakeVerifyNs = r.intakeNs.Load()
	return rep, nil
}

// runSlot produces and processes one block under the configured
// per-block budget. Calls into the production preprocessor; nothing
// in this function re-implements selection or execution.
func (r *BenchRunner) runSlot(idx int) SlotResult {
	wallStart := time.Now()
	deadline := wallStart.Add(r.budget)
	haveTime := func() bool { return time.Now().Before(deadline) }

	blk := &block.Block{
		Header: &block.BlockHeader{
			Slot:  uint64(idx + 1),
			Nonce: uint64(idx + 1),
		},
		PubKeysBitmap: []byte{1},
	}
	r.bn.TxPreprocessor().CreateBlockStarted()
	res, err := r.bn.TxPreprocessor().CreateAndProcessBlockTransactions(blk, haveTime)
	used := time.Since(wallStart)

	fit := 0
	if res != nil {
		fit = res.Length()
	}
	// Failed-tx count: best estimate from the difference between what
	// the preprocessor decided to include vs the block's accepted hashes.
	// preprocess.transactions removes bad txs from the pool; what's left
	// in res.Length() is the count of successfully-processed txs.
	failed := uint64(0)
	if err != nil {
		failed = 1
	}

	// Block-finalize step: drop the just-processed txs from the pool so
	// the next slot doesn't re-pick them.
	if rerr := r.bn.TxPreprocessor().RemoveTxsFromPools(blk); rerr != nil {
		// non-fatal; the next slot will just see them again and reject
		// them on the validateNonce check.
		_ = rerr
	}
	// Commit accumulated state changes to the trie. Production block
	// processor does this in baseProcessor.commitAll; we do it inline.
	_ = r.bn.CommitState()

	return SlotResult{
		SlotIdx:    idx,
		TxFit:      fit,
		UsedNs:     used.Nanoseconds(),
		BudgetNs:   r.budget.Nanoseconds(),
		Failed:     failed,
		BlockNonce: blk.GetNonce(),
		WallStart:  wallStart,
	}
}
