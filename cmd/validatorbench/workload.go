package main

import (
	"crypto/rand"
	"fmt"
	"math/big"
	"strconv"
	"strings"
	"sync/atomic"
)

// Workload is the abstraction that lets new transaction patterns plug
// into the benchmark without touching the engine. A workload is given a
// VMEnv (so it knows about senders and contract addresses) and returns
// the next Tx every time Next() is called.
//
// Implementations must be safe for concurrent use by `cfg.Concurrency`
// generator goroutines — typically by storing per-call counters in
// atomics rather than mutexes.
type Workload interface {
	// Name is used for logs and the report header.
	Name() string

	// Next returns the next transaction. workerID is the index of the
	// calling generator goroutine, useful for sharding senders so two
	// generators don't constantly fight over the same nonce-locked
	// account.
	Next(workerID int) *Tx
}

// WorkloadRegistry is a tiny string→constructor map. Adding a new
// workload is one line in `registerBuiltinWorkloads`.
var workloadRegistry = map[string]workloadCtor{}

type workloadCtor func(cfg Config, env *VMEnv, builder *TxBuilder) (Workload, error)

// RegisterWorkload exposes the registry for external packages. Built-in
// workloads call this from init().
func RegisterWorkload(name string, ctor workloadCtor) {
	workloadRegistry[name] = ctor
}

// NewWorkload looks up a registered workload by name.
func NewWorkload(cfg Config, env *VMEnv, builder *TxBuilder) (Workload, error) {
	ctor, ok := workloadRegistry[cfg.Workload]
	if !ok {
		names := make([]string, 0, len(workloadRegistry))
		for k := range workloadRegistry {
			names = append(names, k)
		}
		return nil, fmt.Errorf("unknown workload %q (have: %s)", cfg.Workload, strings.Join(names, ", "))
	}
	return ctor(cfg, env, builder)
}

func init() {
	RegisterWorkload("sc-call", newSCCallWorkload)
	RegisterWorkload("transfer", newTransferWorkload)
	RegisterWorkload("mixed", newMixedWorkload)
}

// scCallWorkload drives a function-by-name call across `NumContracts`
// instances of the configured contract. Argument templates support a
// "rand:N" token that emits a fresh N-byte big-endian random integer
// every call, useful for forcing distinct hot keys in storage.
type scCallWorkload struct {
	cfg      Config
	env      *VMEnv
	builder  *TxBuilder
	function string
	argTpls  []argTemplate
	round    atomic.Uint64
}

func newSCCallWorkload(cfg Config, env *VMEnv, builder *TxBuilder) (Workload, error) {
	if cfg.CallFunction == "" {
		return nil, fmt.Errorf("sc-call: call_function must be set")
	}
	tpls, err := parseArgTemplates(cfg.CallArgsTpl)
	if err != nil {
		return nil, fmt.Errorf("sc-call: %w", err)
	}
	return &scCallWorkload{
		cfg:      cfg,
		env:      env,
		builder:  builder,
		function: cfg.CallFunction,
		argTpls:  tpls,
	}, nil
}

func (w *scCallWorkload) Name() string {
	return fmt.Sprintf("sc-call/%s (complexity=%d, contracts=%d)",
		w.function, w.cfg.Complexity, w.cfg.NumContracts)
}

func (w *scCallWorkload) Next(workerID int) *Tx {
	round := w.round.Add(1) - 1
	senders := w.env.Senders()
	contracts := w.env.Contracts()
	// Shard the sender pool across workers to reduce nonce contention while
	// still exercising the cacher with multiple distinct accounts.
	sender := senders[(workerID+int(round))%len(senders)]
	contract := contracts[round%uint64(len(contracts))]

	// Build the argument list, expanding templates to fresh bytes each call.
	args := make([][]byte, 0, len(w.argTpls)*w.cfg.Complexity)
	for c := 0; c < w.cfg.Complexity; c++ {
		for _, t := range w.argTpls {
			args = append(args, t.materialize())
		}
	}
	return w.builder.Build(sender, contract, w.function, args)
}

// transferWorkload simulates pure value transfers between regular
// accounts. It NEVER touches the VM — the executor decodes the tx, runs
// signature verification, then directly updates two account balances
// and the sender's nonce. This is the realistic baseline for
// transfer-only TPS, which klever-go validators do on the
// non-SmartContract code path.
type transferWorkload struct {
	cfg     Config
	env     *VMEnv
	builder *TxBuilder
	round   atomic.Uint64
}

func newTransferWorkload(cfg Config, env *VMEnv, builder *TxBuilder) (Workload, error) {
	if len(env.Senders()) < 2 {
		return nil, fmt.Errorf("transfer workload needs >= 2 accounts")
	}
	return &transferWorkload{cfg: cfg, env: env, builder: builder}, nil
}

func (w *transferWorkload) Name() string {
	return fmt.Sprintf("transfer (accounts=%d, no VM)", w.cfg.NumAccounts)
}

func (w *transferWorkload) Next(workerID int) *Tx {
	round := w.round.Add(1) - 1
	senders := w.env.Senders()
	// Pick distinct sender / recipient indices; modulo arithmetic guarantees
	// no self-transfer when NumAccounts >= 2.
	sIdx := (workerID + int(round)) % len(senders)
	rIdx := (sIdx + 1) % len(senders)
	sender := senders[sIdx]
	recipient := senders[rIdx]
	return w.builder.BuildTransfer(sender, recipient.Address[:], 1)
}

// mixedWorkload alternates between transfers and sc-calls in a 1:1 ratio,
// approximating a realistic block where most txs are SC interactions but
// some are pure value transfers.
type mixedWorkload struct {
	cfg      Config
	transfer Workload
	scCall   Workload
	round    atomic.Uint64
}

func newMixedWorkload(cfg Config, env *VMEnv, builder *TxBuilder) (Workload, error) {
	t, err := newTransferWorkload(cfg, env, builder)
	if err != nil {
		return nil, err
	}
	s, err := newSCCallWorkload(cfg, env, builder)
	if err != nil {
		return nil, err
	}
	return &mixedWorkload{cfg: cfg, transfer: t, scCall: s}, nil
}

func (w *mixedWorkload) Name() string { return "mixed (50/50 transfer + sc-call)" }

func (w *mixedWorkload) Next(workerID int) *Tx {
	if w.round.Add(1)%2 == 0 {
		return w.transfer.Next(workerID)
	}
	return w.scCall.Next(workerID)
}

// argTemplate represents a single configured argument with optional
// runtime expansion. We support:
//   - decimal literal:     "5"      → big-endian bigint
//   - hex literal:         "0xfeed" → raw bytes
//   - random bigint token: "rand:8" → 8 fresh random bytes
//   - raw passthrough:     "abc"    → []byte("abc")
type argTemplate struct {
	kind   argKind
	bytes  []byte
	rndLen int
}

type argKind int

const (
	argLiteral argKind = iota
	argRandom
)

func parseArgTemplates(in []string) ([]argTemplate, error) {
	out := make([]argTemplate, 0, len(in))
	for _, s := range in {
		switch {
		case strings.HasPrefix(s, "rand:"):
			n, err := strconv.Atoi(s[len("rand:"):])
			if err != nil || n <= 0 {
				return nil, fmt.Errorf("invalid rand template %q", s)
			}
			out = append(out, argTemplate{kind: argRandom, rndLen: n})
		case strings.HasPrefix(s, "0x"):
			b, err := hexToBytes(s[2:])
			if err != nil {
				return nil, fmt.Errorf("invalid hex %q: %w", s, err)
			}
			out = append(out, argTemplate{kind: argLiteral, bytes: b})
		default:
			if v, ok := new(big.Int).SetString(s, 10); ok {
				out = append(out, argTemplate{kind: argLiteral, bytes: v.Bytes()})
			} else {
				out = append(out, argTemplate{kind: argLiteral, bytes: []byte(s)})
			}
		}
	}
	return out, nil
}

func (t argTemplate) materialize() []byte {
	if t.kind == argRandom {
		buf := make([]byte, t.rndLen)
		_, _ = rand.Read(buf)
		// Avoid leading zeros so the bigint decoder treats this as a fresh
		// non-trivial value every time.
		if buf[0] == 0 {
			buf[0] = 0x01
		}
		return buf
	}
	out := make([]byte, len(t.bytes))
	copy(out, t.bytes)
	return out
}

// hexToBytes parses a hex string, ignoring the optional "0x" prefix.
// Defined locally to avoid pulling in encoding/hex just for the casing.
func hexToBytes(s string) ([]byte, error) {
	if len(s)%2 != 0 {
		s = "0" + s
	}
	out := make([]byte, len(s)/2)
	for i := 0; i < len(out); i++ {
		hi, err := hexNibble(s[i*2])
		if err != nil {
			return nil, err
		}
		lo, err := hexNibble(s[i*2+1])
		if err != nil {
			return nil, err
		}
		out[i] = (hi << 4) | lo
	}
	return out, nil
}

func hexNibble(c byte) (byte, error) {
	switch {
	case c >= '0' && c <= '9':
		return c - '0', nil
	case c >= 'a' && c <= 'f':
		return c - 'a' + 10, nil
	case c >= 'A' && c <= 'F':
		return c - 'A' + 10, nil
	default:
		return 0, fmt.Errorf("invalid hex char %q", c)
	}
}

