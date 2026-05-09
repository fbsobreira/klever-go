package main

// Smoke test that exercises the full production pipeline end-to-end:
//
//   FundAccount → BuildSignedTransfer → PushTx → preprocessor.CreateAndProcessBlockTransactions
//
// If the bench can run a single transfer through the real txProcessor and
// see the recipient's balance change, the wiring works. This test gates
// the migration commit that replaces the custom Mempool/Processor.

import (
	"testing"
	"time"

	"github.com/klever-io/klever-go/data/block"
)

func TestBenchNode_SingleTransferEndToEnd(t *testing.T) {
	cfg := DefaultConfig()
	cfg.NumAccounts = 0          // we'll create accounts manually
	cfg.WarmupTransactions = 0
	cfg.PrefillMempool = 0
	cfg.HashAlgo = "blake2b"

	bn, err := NewBenchNode(cfg)
	if err != nil {
		t.Fatalf("new bench node: %v", err)
	}
	defer bn.Close()

	// Two real accounts, both funded.
	sender, err := newBenchAccount()
	if err != nil {
		t.Fatalf("sender: %v", err)
	}
	recipient, err := newBenchAccount()
	if err != nil {
		t.Fatalf("recipient: %v", err)
	}
	const initialBalance int64 = 1_000_000_000_000
	if err := bn.FundAccount(sender.Address[:], initialBalance); err != nil {
		t.Fatalf("fund sender: %v", err)
	}
	if err := bn.FundAccount(recipient.Address[:], 0); err != nil {
		t.Fatalf("fund recipient: %v", err)
	}
	if err := bn.CommitState(); err != nil {
		t.Fatalf("commit genesis: %v", err)
	}

	// Build + sign + push a single transfer.
	tx, hash, err := bn.BuildSignedTransfer(sender, recipient.Address[:], 100)
	if err != nil {
		t.Fatalf("build signed transfer: %v", err)
	}
	bn.PushTx(hash, tx)

	// Run one block under a generous budget. haveTime is the validator's
	// budget contract; we give it 1s here so a slow first compile
	// doesn't cause a flaky test.
	deadline := time.Now().Add(1 * time.Second)
	haveTime := func() bool { return time.Now().Before(deadline) }

	blk := &block.Block{
		Header: &block.BlockHeader{
			Slot:  1,
			Nonce: 1,
		},
		PubKeysBitmap: []byte{1},
		TxHashes:      [][]byte{},
	}
	bn.TxPreprocessor().CreateBlockStarted()
	res, err := bn.TxPreprocessor().CreateAndProcessBlockTransactions(blk, haveTime)
	if err != nil {
		t.Fatalf("create+process block: %v", err)
	}
	if res == nil {
		t.Fatal("expected non-nil ProcessResults")
	}

	// The preprocessor should have hashed the tx into the block.
	if res.Length() == 0 {
		t.Fatalf("preprocessor returned 0 txs in block (tx Result=%v ResultCode=%v)",
			tx.Result, tx.ResultCode)
	}

	// Verify state: recipient balance should be 100.
	if err := bn.CommitState(); err != nil {
		t.Fatalf("commit post-block: %v", err)
	}
	rcv, err := bn.AccountsCacher().LoadUser(recipient.Address[:])
	if err != nil {
		t.Fatalf("load recipient: %v", err)
	}
	got := rcv.GetBalance(nil, true)
	if got != 100 {
		t.Errorf("recipient balance: want 100, got %d", got)
	}
}
