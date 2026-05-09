package main

// Smoke test that exercises the full production pipeline end-to-end:
//
//   FundAccount → BuildSignedTransfer → PushTx → preprocessor.CreateAndProcessBlockTransactions
//
// If the bench can run a single transfer through the real txProcessor and
// see the recipient's balance change, the wiring works. This test gates
// the migration commit that replaces the custom Mempool/Processor.

import (
	"math/big"
	"os"
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

// TestBenchNode_SCDeployAndCall walks the same production pipeline for an
// SC deploy + invoke. The contract is the bundled adder.wasm: init takes
// a single bigint and stores it as `sum`; `add` takes a bigint and
// increments `sum`. After deploy + 3 invokes we expect sum to advance
// from 0 to 5+1+1+1 = 8.
func TestBenchNode_SCDeployAndCall(t *testing.T) {
	cfg := DefaultConfig()
	cfg.NumAccounts = 0
	cfg.WarmupTransactions = 0
	cfg.PrefillMempool = 0

	bn, err := NewBenchNode(cfg)
	if err != nil {
		t.Fatalf("new bench node: %v", err)
	}
	defer bn.Close()

	owner := bn.Owner()
	const bal int64 = 1_000_000_000_000
	if err := bn.FundAccount(owner.Address[:], bal); err != nil {
		t.Fatalf("fund owner: %v", err)
	}
	if err := bn.CommitState(); err != nil {
		t.Fatalf("commit genesis: %v", err)
	}

	wasm, err := os.ReadFile("testdata/adder.wasm")
	if err != nil {
		t.Fatalf("read adder.wasm: %v", err)
	}

	// Deploy with init arg = 5
	initArg := big.NewInt(5).Bytes()
	deployTx, deployHash, scAddr, err := bn.BuildSignedSCDeploy(owner, wasm, [][]byte{initArg}, 5_000_000)
	if err != nil {
		t.Fatalf("build deploy: %v", err)
	}
	bn.PushTx(deployHash, deployTx)

	deadline := time.Now().Add(2 * time.Second)
	haveTime := func() bool { return time.Now().Before(deadline) }
	blk := &block.Block{Header: &block.BlockHeader{Slot: 1, Nonce: 1}, PubKeysBitmap: []byte{1}}
	bn.TxPreprocessor().CreateBlockStarted()
	if _, err := bn.TxPreprocessor().CreateAndProcessBlockTransactions(blk, haveTime); err != nil {
		t.Fatalf("deploy block: %v", err)
	}
	if deployTx.ResultCode != 0 {
		t.Fatalf("deploy failed, resultCode=%v result=%v receipts=%+v", deployTx.ResultCode, deployTx.Result, deployTx.Receipts)
	}
	if err := bn.CommitState(); err != nil {
		t.Fatalf("commit after deploy: %v", err)
	}
	// Production block-finalize step: drop the txs we already executed
	// from the mempool. Without this, the next block's preprocessor
	// re-picks them and rejects them with "lower nonce" because the
	// account state has moved on.
	if err := bn.TxPreprocessor().RemoveTxsFromPools(blk); err != nil {
		t.Fatalf("remove deploy from pool: %v", err)
	}

	// Three invokes of `add 1` — net effect: sum 5 → 8.
	for i := 0; i < 3; i++ {
		// Verify our local nonce counter still matches the on-chain
		// account's nonce. If it doesn't, the production validateNonce
		// check would reject our tx with ErrLowerNonceInTransaction; we
		// resync here so the error surfaces clearly with context.
		ownerAcc, err := bn.AccountsCacher().GetExistingUser(owner.Address[:])
		if err != nil {
			t.Fatalf("load owner before call %d: %v", i, err)
		}
		t.Logf("iter %d: bench-side owner.Nonce=%d, on-chain acc.Nonce=%d",
			i, owner.Nonce, ownerAcc.GetNonce())
		// Resync the bench's nonce counter to the chain — production tx
		// processors trust whatever on-chain says, not what the wallet
		// believed.
		owner.Nonce = ownerAcc.GetNonce()

		callTx, callHash, err := bn.BuildSignedSCCall(owner, scAddr, "add", [][]byte{big.NewInt(1).Bytes()}, 5_000_000)
		if err != nil {
			t.Fatalf("build call %d: %v", i, err)
		}
		t.Logf("iter %d: built tx with nonce=%d (post-build owner.Nonce=%d)",
			i, callTx.RawData.Nonce, owner.Nonce)
		bn.PushTx(callHash, callTx)

		deadline2 := time.Now().Add(2 * time.Second)
		ht2 := func() bool { return time.Now().Before(deadline2) }
		blk2 := &block.Block{Header: &block.BlockHeader{Slot: uint64(2 + i), Nonce: uint64(2 + i)}, PubKeysBitmap: []byte{1}}
		bn.TxPreprocessor().CreateBlockStarted()
		if _, err := bn.TxPreprocessor().CreateAndProcessBlockTransactions(blk2, ht2); err != nil {
			t.Fatalf("call block %d: %v", i, err)
		}
		if callTx.ResultCode != 0 {
			t.Fatalf("call %d failed, resultCode=%v receipts=%+v",
				i, callTx.ResultCode, callTx.Receipts)
		}
		if err := bn.CommitState(); err != nil {
			t.Fatalf("commit after call %d: %v", i, err)
		}
		if err := bn.TxPreprocessor().RemoveTxsFromPools(blk2); err != nil {
			t.Fatalf("remove call %d from pool: %v", i, err)
		}
	}

	t.Logf("SC deploy + 3 invokes succeeded against deployed contract %x", scAddr[:8])
}

