package main

import (
	"context"
	"sync"
	"sync/atomic"
)

// Mempool is a bounded multi-producer, single-consumer queue for txs that
// have been "received from P2P" but not yet selected into a block. It is
// intentionally simple: a buffered channel with stats.
//
// In real validator code the mempool is a sharded LRU keyed by sender +
// nonce. For pure throughput sizing the FIFO behaviour and the
// backpressure are what matter — sender ordering is preserved by the
// generator goroutine that owns each account, not by the mempool.
type Mempool struct {
	in  chan *Tx
	cap int

	enqueued atomic.Uint64
	dequeued atomic.Uint64
	dropped  atomic.Uint64

	// closeOnce guards Close so multiple drains don't panic on a closed
	// channel.
	closeOnce sync.Once
}

// NewMempool returns a mempool with the given buffer capacity. A
// capacity of 0 falls back to BlockSize * 4 so small configurations
// still have some slack between producer and consumer.
func NewMempool(capacity int) *Mempool {
	if capacity <= 0 {
		capacity = 1024
	}
	return &Mempool{
		in:  make(chan *Tx, capacity),
		cap: capacity,
	}
}

// Push tries to enqueue a tx. If the mempool is full and ctx is canceled,
// it returns false with the tx counted as dropped — the same fate the
// real mempool gives to overflow traffic.
func (m *Mempool) Push(ctx context.Context, tx *Tx) bool {
	select {
	case m.in <- tx:
		m.enqueued.Add(1)
		return true
	case <-ctx.Done():
		m.dropped.Add(1)
		return false
	}
}

// Drain pulls up to `n` txs into the provided slice (which may be nil).
// It blocks until at least one tx is available or ctx is canceled.
// Returns the number of txs read.
func (m *Mempool) Drain(ctx context.Context, n int, into *[]*Tx) int {
	if n <= 0 {
		return 0
	}
	*into = (*into)[:0]
	// First read is blocking — we need at least one tx to make a block.
	select {
	case tx, ok := <-m.in:
		if !ok {
			return 0
		}
		*into = append(*into, tx)
		m.dequeued.Add(1)
	case <-ctx.Done():
		return 0
	}
	// Subsequent reads are non-blocking until we hit n or run dry.
	for len(*into) < n {
		select {
		case tx, ok := <-m.in:
			if !ok {
				return len(*into)
			}
			*into = append(*into, tx)
			m.dequeued.Add(1)
		default:
			return len(*into)
		}
	}
	return len(*into)
}

// Close stops accepting new txs.
func (m *Mempool) Close() {
	m.closeOnce.Do(func() { close(m.in) })
}

// Stats returns a point-in-time snapshot of the counters.
func (m *Mempool) Stats() (enq, deq, drop uint64) {
	return m.enqueued.Load(), m.dequeued.Load(), m.dropped.Load()
}
