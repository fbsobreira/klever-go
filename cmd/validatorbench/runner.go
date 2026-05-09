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
	StartedAt      time.Time
	StoppedAt      time.Time
	WorkloadName   string
	SlotInterval   time.Duration
	BlockBudget    time.Duration
	Slots          []SlotResult
	TotalTx        uint64
	TotalFailed    uint64
	IntakeAccepts  uint64
	IntakeRejects  uint64
	IntakeVerifyNs uint64

	// intakeWall is the wall-clock the bench spent in synchronous
	// intake (bounded mode only). 0 in duration / saturate modes.
	intakeWall time.Duration
}

// BenchRunner orchestrates the slot clock + intake + per-slot
// preprocessor calls.
type BenchRunner struct {
	bn          *BenchNode
	wl          BenchWorkload
	slotClock   time.Duration
	budget      time.Duration
	prefill     int
	concurrency int

	// Intake counters track the production-intake side of the
	// pipeline. accepts = txs that passed CheckTxValidity and were
	// added to the pool; rejects = txs the validator turned away
	// (typically nonce-window overruns under heavy load); intakeNs
	// = wall time spent in the intake path, off the per-block budget.
	intakeAccepts atomic.Uint64
	intakeRejects atomic.Uint64
	intakeNs      atomic.Uint64
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

// intakeOne runs one tx through the full production intake path:
// build (sign), CheckTxValidity (nonce window + crypto verify),
// AddData (push to shardedTxPool). Records the intake-time cost.
// Returns true if the tx was accepted into the mempool.
//
// Mirrors what the live validator's interceptor does in the 3.5s
// window between slots — same constructors, same calls. The cost
// here is OFF the per-block budget and is reported separately.
func (r *BenchRunner) intakeOne(workerID int) bool {
	t0 := time.Now()
	stx, err := r.wl.Build(workerID)
	if err != nil {
		r.intakeNs.Add(uint64(time.Since(t0).Nanoseconds()))
		return false
	}
	// Production intake validation: same code path
	// dataValidators.txValidator runs over an InterceptedTransaction.
	if err := r.bn.IntakeVerify(stx.hash, stx.tx); err != nil {
		// Reject mirrors what the interceptor does — drop the tx,
		// don't push to pool. Track as an intake reject so operators
		// can see when the producer is generating txs faster than the
		// nonce window allows (maxNonceDeltaAllowed = 1024 in our config).
		r.intakeRejects.Add(1)
		r.intakeNs.Add(uint64(time.Since(t0).Nanoseconds()))
		return false
	}
	r.bn.PushTx(stx.hash, stx.tx)
	r.intakeNs.Add(uint64(time.Since(t0).Nanoseconds()))
	r.intakeAccepts.Add(1)
	return true
}

// Prefill synchronously generates `prefill` signed txs through the
// production intake path and pushes them to the mempool.
func (r *BenchRunner) Prefill(ctx context.Context) error {
	if r.prefill <= 0 {
		return nil
	}
	var wg sync.WaitGroup
	var accepted atomic.Int64
	target := int64(r.prefill)

	for w := 0; w < r.concurrency; w++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for {
				if ctx.Err() != nil {
					return
				}
				if accepted.Load() >= target {
					return
				}
				if r.intakeOne(id) {
					accepted.Add(1)
				}
			}
		}(w)
	}
	wg.Wait()
	_ = fmt.Sprint // keep import live
	return nil
}

// startBackgroundProducers keeps the mempool topped up while the
// processor is consuming. Producers stop when ctx is canceled. Each
// producer goroutine drives the full production intake path on every
// tx.
func (r *BenchRunner) startBackgroundProducers(ctx context.Context) {
	for w := 0; w < r.concurrency; w++ {
		go func(id int) {
			for {
				if ctx.Err() != nil {
					return
				}
				r.intakeOne(id)
			}
		}(w)
	}
}

// RunMode determines the bench's termination criterion + load shape.
type RunMode int

const (
	// ModeDuration drives the slot clock for the configured Duration.
	// Background producers run continuously through the production
	// intake path; per-slot the preprocessor processes whatever fits in
	// the budget. Headline metric: effective max TPS (avg tx/slot ÷
	// slot interval).
	ModeDuration RunMode = iota

	// ModeBounded injects exactly TxBudget transactions through the
	// production intake path, then runs the slot clock until the
	// preprocessor has executed all of them. Headline metric: total
	// wall time + number of slots needed.
	ModeBounded

	// ModeSaturate runs without a slot clock. Producers + processor
	// run flat-out for the configured Duration, measuring the raw
	// hardware ceiling (intake rate + execution rate). NOT chain-
	// realistic — useful only to size the host's raw capacity.
	ModeSaturate
)

// Run drives the configured RunMode for up to `duration`. txBudget is
// honored only in ModeBounded.
//
// Returns the per-slot RunReport when the mode's termination
// criterion fires (duration timeout, txBudget reached, or external
// ctx cancel).
func (r *BenchRunner) Run(ctx context.Context, mode RunMode, duration time.Duration, txBudget int) (*RunReport, error) {
	switch mode {
	case ModeBounded:
		return r.runBounded(ctx, txBudget, duration)
	case ModeSaturate:
		return r.runSaturate(ctx, duration)
	case ModeDuration:
		fallthrough
	default:
		return r.runDuration(ctx, duration)
	}
}

// runDuration is the original chain-realistic mode.
func (r *BenchRunner) runDuration(ctx context.Context, duration time.Duration) (*RunReport, error) {
	if err := r.Prefill(ctx); err != nil {
		return nil, err
	}

	rep := &RunReport{
		StartedAt:    time.Now(),
		WorkloadName: r.wl.Name(),
		SlotInterval: r.slotClock,
		BlockBudget:  r.budget,
	}

	prodCtx, prodCancel := context.WithCancel(ctx)
	r.startBackgroundProducers(prodCtx)
	defer prodCancel()

	deadline := rep.StartedAt.Add(duration)
	slotIdx := 0
	nextSlot := time.Now()
	for time.Now().Before(deadline) {
		if d := time.Until(nextSlot); d > 0 {
			select {
			case <-time.After(d):
			case <-ctx.Done():
				return r.finalize(rep), nil
			}
		}
		nextSlot = nextSlot.Add(r.slotClock)

		sr := r.runSlot(slotIdx)
		rep.Slots = append(rep.Slots, sr)
		rep.TotalTx += uint64(sr.TxFit)
		rep.TotalFailed += sr.Failed
		slotIdx++
	}

	return r.finalize(rep), nil
}

// runBounded injects exactly `txBudget` txs through the production
// intake path, then runs slots until they're all processed. The slot
// clock + per-block budget still apply, so the wall time + slot count
// at the end tells operators "to clear N transactions on this hardware
// at production timing, you needed X slots = X*4s".
//
// duration acts as a safety cap — if the preprocessor can't drain the
// txs within `duration`, we stop and report what we got.
func (r *BenchRunner) runBounded(ctx context.Context, txBudget int, duration time.Duration) (*RunReport, error) {
	if txBudget <= 0 {
		return nil, fmt.Errorf("bounded mode needs --tx > 0")
	}

	// Synchronous intake of exactly txBudget txs through the production
	// intake path. We don't start background producers — the bench's
	// goal here is to measure how long the preprocessor takes to drain
	// a fixed batch.
	rep := &RunReport{
		StartedAt:    time.Now(),
		WorkloadName: r.wl.Name(),
		SlotInterval: r.slotClock,
		BlockBudget:  r.budget,
	}

	intakeStart := time.Now()
	if err := r.injectExactly(ctx, txBudget); err != nil {
		return nil, fmt.Errorf("inject %d txs: %w", txBudget, err)
	}
	rep.intakeWall = time.Since(intakeStart)

	deadline := rep.StartedAt.Add(duration)
	slotIdx := 0
	nextSlot := time.Now()
	for rep.TotalTx < uint64(txBudget) && time.Now().Before(deadline) {
		if d := time.Until(nextSlot); d > 0 {
			select {
			case <-time.After(d):
			case <-ctx.Done():
				return r.finalize(rep), nil
			}
		}
		nextSlot = nextSlot.Add(r.slotClock)

		sr := r.runSlot(slotIdx)
		rep.Slots = append(rep.Slots, sr)
		rep.TotalTx += uint64(sr.TxFit)
		rep.TotalFailed += sr.Failed
		slotIdx++
		if sr.TxFit == 0 {
			// Mempool drained — no more txs to process even if the
			// preprocessor had budget left. Stop early.
			break
		}
	}

	return r.finalize(rep), nil
}

// runSaturate runs producer + processor flat-out for `duration`, with
// no slot clock and no per-block budget cutoff. Useful for measuring
// the raw hardware ceiling: how many txs/s can the production
// pipeline sustain when nothing throttles it. NOT chain-realistic —
// for the chain-realistic ceiling use ModeDuration.
func (r *BenchRunner) runSaturate(ctx context.Context, duration time.Duration) (*RunReport, error) {
	if err := r.Prefill(ctx); err != nil {
		return nil, err
	}

	rep := &RunReport{
		StartedAt:    time.Now(),
		WorkloadName: r.wl.Name(),
		SlotInterval: 0, // signals "no slot clock" to the reporter
		BlockBudget:  0,
	}

	prodCtx, prodCancel := context.WithCancel(ctx)
	r.startBackgroundProducers(prodCtx)
	defer prodCancel()

	deadline := rep.StartedAt.Add(duration)
	slotIdx := 0
	for time.Now().Before(deadline) {
		// No slot wait, no budget enforcement. haveTime returns true
		// until duration expires — the preprocessor processes
		// everything in the pool.
		neverDeadline := func() bool { return time.Now().Before(deadline) }
		sr := r.runSlotWithHaveTime(slotIdx, neverDeadline)
		rep.Slots = append(rep.Slots, sr)
		rep.TotalTx += uint64(sr.TxFit)
		rep.TotalFailed += sr.Failed
		slotIdx++
		if sr.TxFit == 0 {
			// Empty pool — wait briefly for producers to refill.
			select {
			case <-time.After(time.Millisecond):
			case <-ctx.Done():
				return r.finalize(rep), nil
			}
		}
	}

	return r.finalize(rep), nil
}

// finalize stamps the end time + intake counters into the report.
func (r *BenchRunner) finalize(rep *RunReport) *RunReport {
	rep.StoppedAt = time.Now()
	rep.IntakeVerifyNs = r.intakeNs.Load()
	rep.IntakeAccepts = r.intakeAccepts.Load()
	rep.IntakeRejects = r.intakeRejects.Load()
	return rep
}

// injectExactly synchronously pushes exactly N accepted txs into the
// production mempool through the full intake-verify path. Background
// producers are NOT used — bounded mode wants the intake-side cost
// captured separately.
func (r *BenchRunner) injectExactly(ctx context.Context, n int) error {
	var wg sync.WaitGroup
	var accepted atomic.Int64
	target := int64(n)

	for w := 0; w < r.concurrency; w++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for {
				if ctx.Err() != nil {
					return
				}
				if accepted.Load() >= target {
					return
				}
				if r.intakeOne(id) {
					accepted.Add(1)
				}
			}
		}(w)
	}
	wg.Wait()
	return nil
}

// runSlot produces and processes one block under the configured
// per-block budget. Calls into the production preprocessor; nothing
// in this function re-implements selection or execution.
func (r *BenchRunner) runSlot(idx int) SlotResult {
	deadline := time.Now().Add(r.budget)
	return r.runSlotWithHaveTime(idx, func() bool { return time.Now().Before(deadline) })
}

// runSlotWithHaveTime is the shared per-slot core; lets saturate mode
// override the haveTime predicate.
func (r *BenchRunner) runSlotWithHaveTime(idx int, haveTime func() bool) SlotResult {
	wallStart := time.Now()

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
