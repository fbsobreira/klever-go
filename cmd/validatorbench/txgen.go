package main

import (
	"crypto/ed25519"
	"crypto/rand"
	"encoding/binary"
	"math/big"
	"sync"
	"sync/atomic"
)

// TxKind discriminates between transaction types so the validator can
// pick the right execution path. Real klever-go has the same split: a
// pure transfer never touches the VM, while an SC-call always does.
type TxKind uint8

const (
	// TxKindSCCall invokes a function on a deployed contract.
	TxKindSCCall TxKind = iota
	// TxKindTransfer moves balance between two regular accounts. No VM.
	TxKindTransfer
)

// Tx is the in-memory representation of a transaction that has been
// produced by the workload generator and is ready to enter the mempool.
//
// It carries everything the simulated validator needs to:
//   - re-marshal it (P2P style)
//   - hash it
//   - verify its ed25519 signature
//   - execute it through the VM (or skip it for transfers)
//
// We keep the marshalled bytes around because that is the dominant
// memory profile in the real validator (mempool stores serialized txs).
type Tx struct {
	Kind      TxKind
	Sender    *Account
	Recipient []byte
	Function  string
	Arguments [][]byte
	GasLimit  uint64
	GasPrice  uint64
	Value     int64 // amount transferred for TxKindTransfer

	SeqID uint64

	// Bytes is the deterministic marshalled form (used for size accounting
	// and to feed P2P-style hashing). It is built once at generation time.
	Bytes []byte

	// SigBody is the exact preimage that was signed. It's a subset of
	// Bytes — typically everything except the signature field — and
	// matches the validator's "tx-sign hash" path.
	SigBody []byte

	// Signature is the ed25519 signature over SHA-256(SigBody).
	Signature []byte
}

// TxBuilder marshals + signs transactions. It is safe to share between
// goroutines: it carries no state and ed25519.Sign is goroutine-safe.
type TxBuilder struct {
	gasLimit uint64
	gasPrice uint64
	padding  int
	scratch  sync.Pool
}

// NewTxBuilder constructs a TxBuilder. The padding parameter inflates the
// data field so operators can simulate heavier P2P payloads.
func NewTxBuilder(cfg Config) *TxBuilder {
	return &TxBuilder{
		gasLimit: cfg.GasLimit,
		gasPrice: cfg.GasPrice,
		padding:  cfg.TxDataPaddingBytes,
		scratch: sync.Pool{
			New: func() any { b := make([]byte, 0, 256); return &b },
		},
	}
}

// seqCounter provides unique per-tx ids without locking.
var seqCounter atomic.Uint64

// BuildTransfer creates a value transfer between two regular accounts.
// No VM dispatch — the executor will just verify, update nonces, and
// move balance.
func (b *TxBuilder) BuildTransfer(sender *Account, recipient []byte, value int64) *Tx {
	tx := b.Build(sender, recipient, "", nil)
	tx.Kind = TxKindTransfer
	tx.Value = value
	return tx
}

// Build creates a fully-signed SC-call Tx. The sender's nonce is
// consumed and incremented atomically so concurrent generators don't
// collide.
//
// Layout of Bytes (deterministic, big-endian):
//
//	[8] nonce | [8] seq | [32] sender | [32] recipient | [8] gasLimit |
//	[8] gasPrice | [2] funcLen | [funcLen] func | [4] argCount |
//	(per arg) [4] len | [len] arg | [4] padLen | [padLen] padding |
//	[64] signature
//
// SigBody is everything before the signature.
func (b *TxBuilder) Build(sender *Account, recipient []byte, fn string, args [][]byte) *Tx {
	nonce := atomic.AddUint64(&sender.Nonce, 1) - 1
	seq := seqCounter.Add(1)

	// Estimate buffer size to minimise reallocations.
	size := 8 + 8 + addressLen + addressLen + 8 + 8 + 2 + len(fn) + 4
	for _, a := range args {
		size += 4 + len(a)
	}
	size += 4 + b.padding + ed25519.SignatureSize

	buf := make([]byte, 0, size)
	buf = appendU64(buf, nonce)
	buf = appendU64(buf, seq)
	buf = append(buf, sender.Address[:]...)
	buf = append(buf, recipient...)
	buf = appendU64(buf, b.gasLimit)
	buf = appendU64(buf, b.gasPrice)
	buf = appendU16(buf, uint16(len(fn)))
	buf = append(buf, fn...)
	buf = appendU32(buf, uint32(len(args)))
	for _, a := range args {
		buf = appendU32(buf, uint32(len(a)))
		buf = append(buf, a...)
	}

	if b.padding > 0 {
		buf = appendU32(buf, uint32(b.padding))
		// Pseudo-random padding so compressors can't squash the payload —
		// we want realistic byte volume on the network simulation path.
		pad := make([]byte, b.padding)
		_, _ = rand.Read(pad)
		buf = append(buf, pad...)
	} else {
		buf = appendU32(buf, 0)
	}

	sigBody := make([]byte, len(buf))
	copy(sigBody, buf)

	sig := ed25519.Sign(sender.Private, sigBody)
	buf = append(buf, sig...)

	return &Tx{
		Sender:    sender,
		Recipient: recipient,
		Function:  fn,
		Arguments: args,
		GasLimit:  b.gasLimit,
		GasPrice:  b.gasPrice,
		SeqID:     seq,
		Bytes:     buf,
		SigBody:   sigBody,
		Signature: sig,
	}
}

// MakeBigIntArg encodes an int as the big-endian unsigned bigint argument
// the WASM contracts expect.
func MakeBigIntArg(v int64) []byte {
	if v == 0 {
		return []byte{0}
	}
	return new(big.Int).SetInt64(v).Bytes()
}

// helpers
func appendU16(b []byte, v uint16) []byte {
	var tmp [2]byte
	binary.BigEndian.PutUint16(tmp[:], v)
	return append(b, tmp[:]...)
}
func appendU32(b []byte, v uint32) []byte {
	var tmp [4]byte
	binary.BigEndian.PutUint32(tmp[:], v)
	return append(b, tmp[:]...)
}
func appendU64(b []byte, v uint64) []byte {
	var tmp [8]byte
	binary.BigEndian.PutUint64(tmp[:], v)
	return append(b, tmp[:]...)
}
