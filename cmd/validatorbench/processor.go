package main

import (
	"context"
	"sync"
	"sync/atomic"
	"time"
)

// Processor is the single-node block builder + executor. It pulls
// transactions out of the mempool, assembles them into blocks of
// `cfg.BlockSize`, and walks every block through the VM exactly the
// way a validator would during ProcessBlock.
//
// Concurrency model:
//
//   - Signature verification is parallelised across `cfg.Concurrency`
//     verifiers, mirroring how klever-go schedules VerifySignatures
//     across all CPU cores at the start of a block.
//   - VM execution itself is sequential per block. The smart-contract
//     state machine demands deterministic ordering, so this matches
//     real-world behaviour and surfaces VM-bound bottlenecks honestly.
type Processor struct {
	cfg     Config
	env     *VMEnv
	mempool *Mempool
	metrics *Metrics

	pending []*Tx
}

// NewProcessor constructs the block processor.
func NewProcessor(cfg Config, env *VMEnv, mp *Mempool, m *Metrics) *Processor {
	return &Processor{
		cfg:     cfg,
		env:     env,
		mempool: mp,
		metrics: m,
		pending: make([]*Tx, 0, cfg.BlockSize),
	}
}

// Run consumes the mempool until ctx is canceled or the configured tx
// budget is reached. It is meant to be invoked from a single goroutine —
// running multiple Processors against one VM would race the mock world
// in ways that aren't worth fixing for a benchmark.
//
// In as-fast-as-possible mode (BlockTime/BlockBudget unset) it just
// drains and processes blocks back-to-back. In budget mode it produces
// one block per BlockTime and stops adding txs to the current block as
// soon as the per-block deadline is about to be exceeded.
func (p *Processor) Run(ctx context.Context, txBudget int) {
	if p.cfg.BudgetMode() {
		p.runBudgeted(ctx, txBudget)
		return
	}
	p.runASAP(ctx, txBudget)
}

// runASAP is the original processing loop: pull a full block, run it,
// repeat. Saturates the host CPU and reports the raw ceiling.
func (p *Processor) runASAP(ctx context.Context, txBudget int) {
	processed := uint64(0)
	for {
		if ctx.Err() != nil {
			return
		}
		if txBudget > 0 && processed >= uint64(txBudget) {
			return
		}

		n := p.mempool.Drain(ctx, p.cfg.BlockSize, &p.pending)
		if n == 0 {
			return
		}
		p.processBlock(p.pending[:n])
		processed += uint64(n)
	}
}

// runBudgeted simulates a real validator slot clock: produce a block
// every BlockTime, but spend at most BlockBudget of CPU time on it. The
// number of txs that fit per block is the headline output — multiplied
// by 1/BlockTime it gives the chain-realistic max TPS for the
// configured workload. This is the only honest way to size SC capacity
// because raw TPS measurements over-count what the validator can ship
// to consensus on time.
//
// Algorithm per slot:
//  1. wait until the next slot boundary
//  2. drain a large batch from the (overloaded) mempool
//  3. run signature verification in parallel (counts against the budget)
//  4. execute txs sequentially; stop adding once we've used ~95% of the
//     budget — the remainder pays for finalisation
//  5. finalise the block and record what fit
func (p *Processor) runBudgeted(ctx context.Context, txBudget int) {
	processed := uint64(0)
	nextSlot := time.Now()
	// Bound the per-slot drain by what could conceivably fit — a 1 ms tx
	// in a 500 ms budget is at most ~500 txs, so block_size is a safe
	// upper bound. We still let the deadline cut us short.
	maxFetch := p.cfg.BlockSize

	for {
		if ctx.Err() != nil {
			return
		}
		if txBudget > 0 && processed >= uint64(txBudget) {
			return
		}

		// Sleep until the slot boundary. The first iteration runs
		// immediately because nextSlot == time.Now().
		if d := time.Until(nextSlot); d > 0 {
			select {
			case <-time.After(d):
			case <-ctx.Done():
				return
			}
		}
		nextSlot = nextSlot.Add(p.cfg.BlockTime)

		n := p.mempool.Drain(ctx, maxFetch, &p.pending)
		if n == 0 {
			// Mempool empty during this slot — record an empty block so
			// the report shows the slot was wasted.
			p.metrics.RecordEmptySlot()
			continue
		}
		fit, used := p.processBlockBudgeted(p.pending[:n], p.cfg.BlockBudget)
		processed += uint64(fit)
		p.metrics.RecordBudgetSlot(fit, used, p.cfg.BlockBudget)
	}
}

// processBlock measures three distinct phases:
//
//  1. Signature verification (parallelised). This is where ed25519
//     verifies dominate; it scales with cores and rarely shows up in
//     the bottleneck report unless verification is single-threaded.
//  2. Transaction execution (sequential VM dispatch). Smart-contract
//     workloads spend most of their time here.
//  3. Block finalisation (merkle/root hashing). One hash per tx plus
//     the block header.
//
// Each phase contributes to overall block latency, which is what
// operators ultimately see as block time.
func (p *Processor) processBlock(txs []*Tx) {
	if len(txs) == 0 {
		return
	}
	blockStart := time.Now()

	// ---- Phase 1: parallel signature verification ----
	sigStart := time.Now()
	p.verifySignaturesParallel(txs)
	p.metrics.SigVerifyNs.Add(uint64(time.Since(sigStart).Nanoseconds()))

	// ---- Phase 2: sequential VM execution ----
	execStart := time.Now()
	hashes := make([][]byte, 0, len(txs))
	for _, tx := range txs {
		txStart := time.Now()
		out, failed, err := p.env.ExecuteTx(tx, p.metrics.HashStats())
		latency := time.Since(txStart)
		p.metrics.RecordTxByKind(tx.Kind, latency, failed || err != nil, tx.GasLimit, out)
		// Keep the per-tx hash for the merkle root regardless of success.
		hashes = append(hashes, p.env.Hasher().Compute(string(tx.Bytes)))
	}
	p.metrics.ExecNs.Add(uint64(time.Since(execStart).Nanoseconds()))

	// ---- Phase 3: block finalisation ----
	finStart := time.Now()
	_ = p.env.FinalizeBlock(hashes)
	p.metrics.FinalizeNs.Add(uint64(time.Since(finStart).Nanoseconds()))

	blockDur := time.Since(blockStart)
	p.metrics.RecordBlock(blockDur, len(txs))
}

// processBlockBudgeted runs the same three-phase pipeline as processBlock,
// but stops feeding new txs into the VM once the per-block deadline is
// almost up. It returns (fit, used) where:
//
//	fit  = number of txs that actually made it into the block
//	used = wall time spent on this block including finalisation
//
// We reserve ~5% of the budget for sig-verify warmup and finalisation
// hashing so the validator can still ship the block before the slot
// expires. That mirrors what the chain does in practice — it picks a
// soft deadline well below the hard slot boundary.
func (p *Processor) processBlockBudgeted(txs []*Tx, budget time.Duration) (int, time.Duration) {
	if len(txs) == 0 {
		return 0, 0
	}
	blockStart := time.Now()
	deadline := blockStart.Add(budget * 95 / 100)

	// Phase 1: parallel signature verification. We pre-verify everything
	// the validator selected so far (the mempool drain), since real nodes
	// also batch-verify before execution. Time counts against the budget.
	sigStart := time.Now()
	p.verifySignaturesParallel(txs)
	p.metrics.SigVerifyNs.Add(uint64(time.Since(sigStart).Nanoseconds()))

	// Phase 2: execute txs sequentially, stopping when the deadline is
	// near. Anything not executed is left in the pending slice and will
	// eventually fall off the back of the mempool — the same fate it
	// would meet on a busy validator.
	execStart := time.Now()
	hashes := make([][]byte, 0, len(txs))
	fit := 0
	for _, tx := range txs {
		if time.Now().After(deadline) {
			break
		}
		txStart := time.Now()
		out, failed, err := p.env.ExecuteTx(tx, p.metrics.HashStats())
		latency := time.Since(txStart)
		p.metrics.RecordTxByKind(tx.Kind, latency, failed || err != nil, tx.GasLimit, out)
		hashes = append(hashes, p.env.Hasher().Compute(string(tx.Bytes)))
		fit++
	}
	p.metrics.ExecNs.Add(uint64(time.Since(execStart).Nanoseconds()))

	// Phase 3: finalise. Even partial blocks get a merkle root.
	finStart := time.Now()
	_ = p.env.FinalizeBlock(hashes)
	p.metrics.FinalizeNs.Add(uint64(time.Since(finStart).Nanoseconds()))

	blockDur := time.Since(blockStart)
	p.metrics.RecordBlock(blockDur, fit)
	return fit, blockDur
}

// verifySignaturesParallel fans out signature verification across
// up to `cfg.Concurrency` workers. We use a fixed worker pool with a
// simple index counter to avoid the overhead of spawning a goroutine
// per tx, which dominates throughput for small blocks.
func (p *Processor) verifySignaturesParallel(txs []*Tx) {
	workers := p.cfg.Concurrency
	if workers > len(txs) {
		workers = len(txs)
	}
	if workers <= 1 {
		// Cheap fallback: sequential verify so we don't pay the WaitGroup
		// cost on tiny blocks.
		for _, tx := range txs {
			p.env.Hasher().Compute(string(tx.SigBody))
			// Real ed25519 verify happens inside ExecuteTx; here we only
			// pre-warm caches and account for the hashing cost a real
			// validator would incur during initial deduplication.
			_ = tx
		}
		return
	}
	var idx atomic.Uint64
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				i := idx.Add(1) - 1
				if int(i) >= len(txs) {
					return
				}
				p.env.Hasher().Compute(string(txs[i].SigBody))
			}
		}()
	}
	wg.Wait()
}
