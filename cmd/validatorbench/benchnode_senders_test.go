package main

// Sender-count sweep bench. Measures how production preprocessor
// throughput varies with the number of distinct sender wallets in
// the mempool. The user's hypothesis: too few senders may hit the
// per-sender selection cap (NumTxPerBatchForFillingBlock = 10) and
// leave the block under-filled even when CPU has budget.

import (
	"testing"
	"time"

	"github.com/klever-io/klever-go/data/block"
)

func BenchmarkProdTransferBatch_SenderCount(b *testing.B) {
	cfg := DefaultConfig()
	cfg.WarmupTransactions = 0
	cfg.PrefillMempool = 0

	for _, senders := range []int{16, 64, 256, 1024} {
		b.Run("senders="+itoa(senders), func(b *testing.B) {
			bn, err := NewBenchNode(cfg)
			if err != nil {
				b.Fatalf("new bench node: %v", err)
			}
			defer bn.Close()

			const balance int64 = 1_000_000_000_000_000
			if err := bn.ProvisionAccounts(senders, balance); err != nil {
				b.Fatalf("provision %d senders: %v", senders, err)
			}
			wl, err := NewProdTransferWorkload(bn, 1, 1)
			if err != nil {
				b.Fatalf("workload: %v", err)
			}

			// Pre-build N txs.
			n := b.N
			if n > 12000 {
				n = 12000
			}
			txs := make([]signedTx, n)
			for i := 0; i < n; i++ {
				stx, err := wl.Build(0)
				if err != nil {
					b.Fatalf("build: %v", err)
				}
				txs[i] = stx
			}
			for _, t := range txs {
				bn.PushTx(t.hash, t.tx)
			}

			b.ResetTimer()
			start := time.Now()
			blk := &block.Block{
				Header:        &block.BlockHeader{Slot: 1, Nonce: 1},
				PubKeysBitmap: []byte{1},
			}
			bn.TxPreprocessor().CreateBlockStarted()
			res, err := bn.TxPreprocessor().CreateAndProcessBlockTransactions(blk, func() bool { return true })
			elapsed := time.Since(start)
			b.StopTimer()

			if err != nil {
				b.Fatalf("process: %v", err)
			}
			processed := 0
			if res != nil {
				processed = res.Length()
			}
			if processed > 0 {
				b.ReportMetric(float64(elapsed.Microseconds())/float64(processed), "us/tx")
				b.ReportMetric(float64(processed)/elapsed.Seconds(), "tx/s")
			}
			b.Logf("senders=%d batch=%d processed=%d elapsed=%v",
				senders, n, processed, elapsed)
		})
	}
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	buf := [12]byte{}
	i := len(buf)
	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}
	return string(buf[i:])
}
