package main

// workload_prod.go — workloads for the production-pipeline runner.
// Each implementation builds a real *data/transaction.Transaction
// through BenchNode and returns it to the runner, which pushes it
// into the production shardedTxPool.
//
// Shard senders by workerID so concurrent generators don't race each
// other on per-sender nonce assignment. Production validates strict
// per-sender nonce ordering, so two goroutines using the same sender
// would silently lose txs.

import (
	"fmt"
	"os"
	"sync/atomic"

	"github.com/klever-io/klever-go/data/block"
)

// ProdTransferWorkload generates real transfer transactions through
// the production tx-build path. value is the per-tx KLV amount.
type ProdTransferWorkload struct {
	bn         *BenchNode
	value      int64
	shards     [][]*BenchAccount // shards[i] = senders owned by worker i
	recipients []*BenchAccount   // round-robin recipient pool
	round      atomic.Uint64
}

// NewProdTransferWorkload partitions the bench node's accounts into
// `concurrency` shards; each worker draws senders only from its own
// shard. NumAccounts must be >= concurrency * 2 (one sender + one
// recipient per worker).
func NewProdTransferWorkload(bn *BenchNode, concurrency int, value int64) (*ProdTransferWorkload, error) {
	if concurrency < 1 {
		concurrency = 1
	}
	all := bn.Senders()
	if len(all) < concurrency*2 {
		return nil, fmt.Errorf("transfer workload needs >= concurrency*2 accounts (have %d, need %d)",
			len(all), concurrency*2)
	}

	shards := make([][]*BenchAccount, concurrency)
	for i, a := range all {
		shards[i%concurrency] = append(shards[i%concurrency], a)
	}
	return &ProdTransferWorkload{
		bn:         bn,
		value:      value,
		shards:     shards,
		recipients: all,
	}, nil
}

// Name implements BenchWorkload.
func (w *ProdTransferWorkload) Name() string {
	return fmt.Sprintf("transfer/prod (accounts=%d, value=%d)", len(w.recipients), w.value)
}

// Build implements BenchWorkload.
func (w *ProdTransferWorkload) Build(workerID int) (signedTx, error) {
	round := w.round.Add(1) - 1
	shard := w.shards[workerID%len(w.shards)]
	sender := shard[int(round)%len(shard)]
	// Recipient = a different account picked from the global pool so
	// transfers spread across the trie rather than ping-ponging
	// between two cells.
	recipient := w.recipients[(int(round)+1)%len(w.recipients)]
	tx, hash, err := w.bn.BuildSignedTransfer(sender, recipient.Address[:], w.value)
	if err != nil {
		return signedTx{}, err
	}
	return signedTx{hash: hash, tx: tx}, nil
}

// ProdSCCallWorkload generates real SC-invoke transactions against
// `instances` deployed copies of the same contract. The deploys
// happen at construction time via the production pipeline so the
// runner doesn't need to model a separate "deploy phase".
type ProdSCCallWorkload struct {
	bn       *BenchNode
	function string
	args     [][]byte
	gasLimit uint64
	scAddrs  [][]byte
	shards   [][]*BenchAccount
	round    atomic.Uint64
}

// NewProdSCCallWorkload reads the wasm file, deploys `instances` copies
// of it through the production preprocessor (one tx per block, so the
// addresses commit cleanly), then returns a workload that drives the
// configured function against any of those instances round-robin.
func NewProdSCCallWorkload(bn *BenchNode, contractPath string, initArgs [][]byte, function string, callArgs [][]byte, instances int, gasLimit uint64, concurrency int) (*ProdSCCallWorkload, error) {
	if instances < 1 {
		instances = 1
	}
	wasm, err := os.ReadFile(contractPath)
	if err != nil {
		return nil, fmt.Errorf("read contract %q: %w", contractPath, err)
	}

	addrs := make([][]byte, 0, instances)
	for i := 0; i < instances; i++ {
		addr, err := bn.deployOneSync(wasm, initArgs, gasLimit, uint64(100+i))
		if err != nil {
			return nil, fmt.Errorf("deploy instance %d: %w", i, err)
		}
		addrs = append(addrs, addr)
	}

	all := bn.Senders()
	if len(all) < concurrency {
		return nil, fmt.Errorf("sc-call workload needs >= concurrency accounts (have %d, need %d)",
			len(all), concurrency)
	}
	shards := make([][]*BenchAccount, concurrency)
	for i, a := range all {
		shards[i%concurrency] = append(shards[i%concurrency], a)
	}

	return &ProdSCCallWorkload{
		bn:       bn,
		function: function,
		args:     callArgs,
		gasLimit: gasLimit,
		scAddrs:  addrs,
		shards:   shards,
	}, nil
}

// Name implements BenchWorkload.
func (w *ProdSCCallWorkload) Name() string {
	return fmt.Sprintf("sc-call/prod (fn=%s instances=%d)", w.function, len(w.scAddrs))
}

// Build implements BenchWorkload.
func (w *ProdSCCallWorkload) Build(workerID int) (signedTx, error) {
	round := w.round.Add(1) - 1
	shard := w.shards[workerID%len(w.shards)]
	sender := shard[int(round)%len(shard)]
	scAddr := w.scAddrs[int(round)%len(w.scAddrs)]
	tx, hash, err := w.bn.BuildSignedSCCall(sender, scAddr, w.function, w.args, w.gasLimit)
	if err != nil {
		return signedTx{}, err
	}
	return signedTx{hash: hash, tx: tx}, nil
}

// deployOneSync pushes a single deploy through the production
// preprocessor and returns the address. Lives here (not in
// benchnode.go) because it depends on the runner-level concept of a
// "deploy block" and the data/block package.
func (bn *BenchNode) deployOneSync(wasm []byte, initArgs [][]byte, gasLimit uint64, slot uint64) ([]byte, error) {
	tx, hash, scAddr, err := bn.BuildSignedSCDeploy(bn.Owner(), wasm, initArgs, gasLimit)
	if err != nil {
		return nil, err
	}
	bn.PushTx(hash, tx)

	blk := &block.Block{
		Header: &block.BlockHeader{Slot: slot, Nonce: slot},
		PubKeysBitmap: []byte{1},
	}
	bn.TxPreprocessor().CreateBlockStarted()
	if _, err := bn.TxPreprocessor().CreateAndProcessBlockTransactions(blk, func() bool { return true }); err != nil {
		return nil, fmt.Errorf("execute deploy: %w", err)
	}
	if tx.ResultCode != 0 {
		return nil, fmt.Errorf("deploy failed: resultCode=%v receipts=%+v", tx.ResultCode, tx.Receipts)
	}
	if err := bn.TxPreprocessor().RemoveTxsFromPools(blk); err != nil {
		return nil, fmt.Errorf("remove deploy from pool: %w", err)
	}
	if err := bn.CommitState(); err != nil {
		return nil, fmt.Errorf("commit deploy: %w", err)
	}
	return scAddr, nil
}
