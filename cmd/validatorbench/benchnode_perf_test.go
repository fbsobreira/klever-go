package main

// Per-tx microbenchmark for the production transfer path. Measures
// exactly where time goes when the validator's preprocessor +
// txProcessor processes one signed transfer.
//
// Run with:
//   LD_LIBRARY_PATH=$(pwd)/kvm/wasmer2 \
//     go test -count=1 -bench=BenchmarkProdTransferTx \
//       -benchtime=2s -run=^$ -cpuprofile=/tmp/cpu.prof \
//       ./cmd/validatorbench
//
// Then inspect with:
//   go tool pprof -top -nodecount=30 /tmp/cpu.prof

import (
	"testing"
	"time"

	"github.com/klever-io/klever-go/data/block"
)

// BenchmarkProdTransferTx measures the per-tx cost of running a single
// transfer through the production preprocessor + txProcessor +
// AccountsCacher. Each iteration:
//   1. Builds a fresh signed transfer.
//   2. Pushes to the production shardedTxPool.
//   3. Runs one block through the preprocessor.
//   4. Removes the executed tx from the pool + commits state.
//
// This is the most apples-to-apples per-tx number we can produce: it's
// the same code path the live validator runs.
func BenchmarkProdTransferTx(b *testing.B) {
	cfg := DefaultConfig()
	cfg.NumAccounts = 0
	cfg.WarmupTransactions = 0
	cfg.PrefillMempool = 0

	bn, err := NewBenchNode(cfg)
	if err != nil {
		b.Fatalf("new bench node: %v", err)
	}
	defer bn.Close()

	const balance int64 = 1_000_000_000_000_000

	// Sender + recipient. Single sender so nonce ordering is trivial.
	sender, _ := newBenchAccount()
	recipient, _ := newBenchAccount()
	if err := bn.FundAccount(sender.Address[:], balance); err != nil {
		b.Fatalf("fund sender: %v", err)
	}
	if err := bn.FundAccount(recipient.Address[:], 0); err != nil {
		b.Fatalf("fund recipient: %v", err)
	}
	if err := bn.CommitState(); err != nil {
		b.Fatalf("commit genesis: %v", err)
	}

	// Generous haveTime so the budget never cuts the bench short.
	farFuture := func() bool { return true }

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tx, hash, err := bn.BuildSignedTransfer(sender, recipient.Address[:], 1)
		if err != nil {
			b.Fatalf("build: %v", err)
		}
		bn.PushTx(hash, tx)

		blk := &block.Block{
			Header:        &block.BlockHeader{Slot: uint64(i + 1), Nonce: uint64(i + 1)},
			PubKeysBitmap: []byte{1},
		}
		bn.TxPreprocessor().CreateBlockStarted()
		if _, err := bn.TxPreprocessor().CreateAndProcessBlockTransactions(blk, farFuture); err != nil {
			b.Fatalf("process: %v", err)
		}
		_ = bn.TxPreprocessor().RemoveTxsFromPools(blk)
		_ = bn.CommitState()
	}
}

// BenchmarkProdTransferBatch measures batch processing — how the
// preprocessor handles N txs in one block (which is what a real
// validator does in 500 ms). Set N via -benchtime; e.g. -benchtime=4000x
// runs one block of 4000 txs.
func BenchmarkProdTransferBatch(b *testing.B) {
	cfg := DefaultConfig()
	cfg.NumAccounts = 0
	cfg.WarmupTransactions = 0
	cfg.PrefillMempool = 0

	bn, err := NewBenchNode(cfg)
	if err != nil {
		b.Fatalf("new bench node: %v", err)
	}
	defer bn.Close()

	const balance int64 = 1_000_000_000_000_000
	const numAccounts = 200
	if err := bn.ProvisionAccounts(numAccounts, balance); err != nil {
		b.Fatalf("provision: %v", err)
	}

	wl, err := NewProdTransferWorkload(bn, 1, 1)
	if err != nil {
		b.Fatalf("workload: %v", err)
	}

	// Pre-build all txs to keep the timed section pure execution.
	txs := make([]signedTx, b.N)
	for i := 0; i < b.N; i++ {
		stx, err := wl.Build(0)
		if err != nil {
			b.Fatalf("build %d: %v", i, err)
		}
		txs[i] = stx
	}
	for _, stx := range txs {
		bn.PushTx(stx.hash, stx.tx)
	}

	farFuture := func() bool { return true }

	b.ResetTimer()
	start := time.Now()
	blk := &block.Block{
		Header:        &block.BlockHeader{Slot: 1, Nonce: 1},
		PubKeysBitmap: []byte{1},
	}
	bn.TxPreprocessor().CreateBlockStarted()
	res, err := bn.TxPreprocessor().CreateAndProcessBlockTransactions(blk, farFuture)
	elapsed := time.Since(start)
	b.StopTimer()

	if err != nil {
		b.Fatalf("process batch: %v", err)
	}
	processed := 0
	if res != nil {
		processed = res.Length()
	}
	b.ReportMetric(float64(elapsed.Microseconds())/float64(processed), "us/tx")
	b.ReportMetric(float64(processed)/elapsed.Seconds(), "tx/s")
	b.Logf("batch=%d processed=%d elapsed=%v (%.1f us/tx, %.0f tx/s)",
		b.N, processed, elapsed,
		float64(elapsed.Microseconds())/float64(max1(processed)),
		float64(processed)/elapsed.Seconds())
}

func max1(n int) int {
	if n < 1 {
		return 1
	}
	return n
}
