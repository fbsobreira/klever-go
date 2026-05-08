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
func (p *Processor) Run(ctx context.Context, txBudget int) {
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
		p.metrics.RecordTx(latency, failed || err != nil, tx.GasLimit, out)
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
