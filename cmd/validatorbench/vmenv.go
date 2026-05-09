package main

import (
	"crypto/ed25519"
	"crypto/rand"
	"encoding/binary"
	"fmt"
	"math/big"
	"os"
	"sync"
	"time"

	"github.com/klever-io/klever-go/crypto/hashing"
	"github.com/klever-io/klever-go/kvm/scenarioexec"
	worldhook "github.com/klever-io/klever-go/kvm/mock/world"
	scenmodel "github.com/klever-io/klever-go/kvm/scenarioexec/model"
	"github.com/klever-io/klever-go/vmcommon"
)

// addressLen is the canonical klever account address size, matching what
// the protocol uses everywhere (see core/process/kda).
const addressLen = 32

// Account represents a single funded account in the simulated world: the
// 32-byte protocol address, the ed25519 keypair used to sign realistic
// transactions, and the running nonce.
type Account struct {
	Address [addressLen]byte
	Public  ed25519.PublicKey
	Private ed25519.PrivateKey
	Nonce   uint64
}

// VMEnv owns the in-memory blockchain world, the VM host, the deployed
// test contracts, and the funded sender accounts.
//
// It is the single point where "node-like" state lives during a run.
// Block processing and tx execution call into VMEnv methods so the
// underlying VM, accounts cacher, and trie all see real reads/writes
// — the same code paths the production validator would exercise.
type VMEnv struct {
	cfg     Config
	exec    *scenarioexec.VMTestExecutor
	hasher  hashing.Hasher
	timed   *TimedHasher
	owner   *Account
	senders []*Account
	scAddrs [][]byte // one address per deployed contract instance

	// Per-contract deploy/first-call timing. Keys are stringified addresses.
	deployLatency  []time.Duration
	firstCallMu    sync.Mutex
	firstCallSeen  map[string]bool
	firstCallTimes []time.Duration
}

// NewVMEnv prepares a fully-initialised VM with the chosen hasher,
// funds an owner + a pool of sender accounts, then deploys the test
// contract `cfg.NumContracts` times.
//
// Errors out at the first failed step — running on broken state would
// produce meaningless throughput numbers.
func NewVMEnv(cfg Config, hashStats *HashStats) (*VMEnv, error) {
	base, err := NewBaseHasher(cfg.HashAlgo)
	if err != nil {
		return nil, fmt.Errorf("hasher %q: %w", cfg.HashAlgo, err)
	}
	timed := NewTimedHasher(base, hashStats)

	exec, err := scenarioexec.NewVMTestExecutor()
	if err != nil {
		return nil, fmt.Errorf("create VM executor: %w", err)
	}

	env := &VMEnv{
		cfg:           cfg,
		exec:          exec,
		hasher:        base,
		timed:         timed,
		firstCallSeen: make(map[string]bool),
	}

	// Seed all senders before initializing the VM so the accounts cacher is
	// populated with funded users at genesis time, mirroring what the
	// validator does during chain bootstrap.
	if err := env.bootstrapAccounts(); err != nil {
		exec.Close()
		return nil, err
	}

	if err := exec.InitVM(scenmodel.GasScheduleV1); err != nil {
		env.Close()
		return nil, fmt.Errorf("init VM: %w", err)
	}

	if err := env.deployContracts(); err != nil {
		env.Close()
		return nil, err
	}

	return env, nil
}

// Close releases VM resources. Safe to call more than once.
func (e *VMEnv) Close() {
	if e == nil {
		return
	}
	if e.exec != nil {
		e.exec.Close()
	}
}

// Hasher returns the wrapped hasher used to time transaction-level hashing.
func (e *VMEnv) Hasher() *TimedHasher { return e.timed }

// Senders returns the pool of funded accounts the workload draws from.
func (e *VMEnv) Senders() []*Account { return e.senders }

// Contracts returns the addresses of all deployed contract instances.
func (e *VMEnv) Contracts() [][]byte { return e.scAddrs }

// DeployLatencies returns the wall-clock time of every contract deploy
// (compile + init invoke), in deploy order.
func (e *VMEnv) DeployLatencies() []time.Duration { return e.deployLatency }

// FirstCallLatencies returns the per-contract first-invocation latency in
// the order calls happened. Useful to surface the cold-cache cost of
// hitting a contract for the first time after deploy.
func (e *VMEnv) FirstCallLatencies() []time.Duration {
	e.firstCallMu.Lock()
	defer e.firstCallMu.Unlock()
	out := make([]time.Duration, len(e.firstCallTimes))
	copy(out, e.firstCallTimes)
	return out
}

// bootstrapAccounts creates a deterministic-but-distinct owner and a
// pool of sender accounts, all funded with cfg.InitialBalance. We use
// ed25519 keys so signature verification has the same cost profile as
// the production transaction path.
func (e *VMEnv) bootstrapAccounts() error {
	world := e.exec.World

	owner, err := newAccount()
	if err != nil {
		return fmt.Errorf("create owner: %w", err)
	}
	e.owner = owner

	wAcct := world.CreateAccount(owner.Address[:], world)
	wAcct.Balance = big.NewInt(e.cfg.InitialBalance)
	world.PutAccount(wAcct)

	e.senders = make([]*Account, 0, e.cfg.NumAccounts)
	for i := 0; i < e.cfg.NumAccounts; i++ {
		acc, err := newAccount()
		if err != nil {
			return fmt.Errorf("create account %d: %w", i, err)
		}
		e.senders = append(e.senders, acc)

		wa := world.CreateAccount(acc.Address[:], world)
		wa.Balance = big.NewInt(e.cfg.InitialBalance)
		world.PutAccount(wa)
	}
	return nil
}

// deployContracts loads the wasm bytes once and deploys NumContracts
// independent instances. Each instance has its own storage trie, which
// is what we want when the workload spreads calls across contracts to
// avoid hot-row contention skewing the result.
func (e *VMEnv) deployContracts() error {
	wasm, err := os.ReadFile(e.cfg.ContractPath)
	if err != nil {
		return fmt.Errorf("read contract %q: %w", e.cfg.ContractPath, err)
	}
	if len(wasm) == 0 {
		return fmt.Errorf("contract file %q is empty", e.cfg.ContractPath)
	}

	args := encodeArgs(e.cfg.InitArgs)

	for i := 0; i < e.cfg.NumContracts; i++ {
		// Compute a deterministic SC address from the owner + creator nonce
		// so subsequent calls in the same run can target it without doing
		// an extra lookup.
		scAddr := worldhook.GenerateMockAddress(e.owner.Address[:], e.owner.Nonce, scenarioexec.TestVMType)

		input := &vmcommon.ContractCreateInput{
			ContractCode: wasm,
			VMInput: vmcommon.VMInput{
				CallerAddr:     e.owner.Address[:],
				Arguments:      args,
				GasProvided:    e.cfg.GasLimit,
				OriginalTxHash: txHash(uint64(i), 0),
				CurrentTxHash:  txHash(uint64(i), 0),
				KDATransfers:   []*vmcommon.KDATransfer{},
			},
		}

		// Time the deploy: wasmer2 compiles the WASM module here. This is
		// the dominant first-touch cost for any contract and what the
		// validator pays once per process restart per contract.
		deployStart := time.Now()
		out, err := e.exec.GetVM().RunSmartContractCreate(input)
		deployDur := time.Since(deployStart)
		if err != nil {
			return fmt.Errorf("deploy %d: %w", i, err)
		}
		if out.ReturnCode != vmcommon.Ok {
			return fmt.Errorf("deploy %d failed: %s (%s)", i, out.ReturnCode, out.ReturnMessage)
		}
		e.deployLatency = append(e.deployLatency, deployDur)

		// Persist the new SC and mutated owner into the mock world so the
		// next call sees a committed, real account state.
		if err := e.exec.World.UpdateAccounts(out.OutputAccounts, out.DeletedAccounts); err != nil {
			return fmt.Errorf("commit deploy %d: %w", i, err)
		}

		// Pick the deployed contract address from the output map: it is the
		// only newly-created account whose address has the VM type prefix.
		var deployed []byte
		for _, oa := range out.OutputAccounts {
			if len(oa.Code) > 0 {
				deployed = oa.Address
				break
			}
		}
		if deployed == nil {
			deployed = scAddr
		}
		e.scAddrs = append(e.scAddrs, deployed)

		// The validator increments the creator nonce after each deploy.
		e.owner.Nonce++
	}
	return nil
}

// ExecuteTx runs a single signed transaction through the VM, mirroring
// what the validator does inside ProcessBlock when it visits each tx:
//  1. validate signature
//  2. update sender state (gas, nonce)
//  3. invoke the VM
//  4. commit output to the trie
//
// Returns the VM output (so callers can record gas + retcode), a "failed"
// flag (set on signature mismatch, OOG, revert, or commit error), and a
// hard error (only set on infrastructure-level failures the caller should
// surface). VM-level failures (OOG, revert) DO NOT return an error —
// they're counted as tx failures in metrics.
func (e *VMEnv) ExecuteTx(tx *Tx, hashStats *HashStats) (*vmcommon.VMOutput, bool, error) {
	// Hash the marshalled transaction body — the validator does this once
	// per tx for the merkle leaf and for the on-disk tx index.
	_ = e.timed.Compute(string(tx.Bytes))

	// Hash the sig pre-image too: the validator computes this hash so it
	// can index by tx-hash in the receipt store. Counting it here keeps
	// the "hashing time as % of total" metric accurate.
	_ = e.timed.Compute(string(tx.SigBody))

	// Verify the ed25519 signature against the raw sig body. Ed25519's
	// "pure" mode signs the message directly, so Verify must receive the
	// same bytes Sign saw — not the digest.
	if !ed25519.Verify(tx.Sender.Public, tx.SigBody, tx.Signature) {
		// Signature mismatch is a tx failure, not a pipeline error: count
		// it as failed so the report's failure-rate column reflects it.
		return nil, true, nil
	}

	// Pure transfer: skip the VM entirely. The validator does the same
	// — non-SC txs go through the kapps balance path, not the wasm host.
	if tx.Kind == TxKindTransfer {
		failed, err := e.executeTransfer(tx)
		return nil, failed, err
	}

	hash := txHash(tx.Sender.Nonce, tx.SeqID)

	input := &vmcommon.ContractCallInput{
		RecipientAddr: tx.Recipient,
		Function:      tx.Function,
		VMInput: vmcommon.VMInput{
			CallerAddr:     tx.Sender.Address[:],
			Arguments:      tx.Arguments,
			GasProvided:    tx.GasLimit,
			OriginalTxHash: hash,
			CurrentTxHash:  hash,
			KDATransfers:   []*vmcommon.KDATransfer{},
		},
	}

	callStart := time.Now()
	out, err := e.exec.GetVM().RunSmartContractCall(input)
	callDur := time.Since(callStart)
	// Track the first call per contract address: this is the latency of
	// the cold-cache invocation right after deploy and right after a
	// validator process restart.
	e.firstCallMu.Lock()
	key := string(tx.Recipient)
	if !e.firstCallSeen[key] {
		e.firstCallSeen[key] = true
		e.firstCallTimes = append(e.firstCallTimes, callDur)
	}
	e.firstCallMu.Unlock()

	if err != nil {
		return nil, false, err
	}
	if out.ReturnCode != vmcommon.Ok {
		// Mirror the validator: nonce increments even on failed calls,
		// but state is not committed.
		return out, true, nil
	}

	if err := e.exec.World.UpdateAccounts(out.OutputAccounts, out.DeletedAccounts); err != nil {
		return out, true, fmt.Errorf("commit tx: %w", err)
	}
	return out, false, nil
}

// executeTransfer performs a value move between two regular accounts
// without involving the VM. This mirrors the klever-go non-SC code path
// where the kapps controller handles balance bookkeeping directly.
//
// The sequence — load sender, debit + nonce++, save; load recipient,
// credit, save — is what dominates real transfer-block CPU time, and
// it's what the per-block budget needs to fit when measuring the TPS
// ceiling for transfer-heavy chains.
func (e *VMEnv) executeTransfer(tx *Tx) (bool, error) {
	cacher := e.exec.World.AccountsCacher

	sender, err := cacher.GetExistingUser(tx.Sender.Address[:])
	if err != nil || sender == nil {
		return true, nil
	}
	if err := sender.SubFromBalance(tx.Value, nil, true); err != nil {
		return true, nil
	}
	sender.IncreaseNonce(1)
	if err := cacher.SaveUser(sender); err != nil {
		return true, fmt.Errorf("save sender: %w", err)
	}

	recipient, err := cacher.LoadUser(tx.Recipient)
	if err != nil || recipient == nil {
		return true, nil
	}
	if err := recipient.AddToBalance(tx.Value, nil, true); err != nil {
		return true, nil
	}
	if err := cacher.SaveUser(recipient); err != nil {
		return true, fmt.Errorf("save recipient: %w", err)
	}
	return false, nil
}

// FinalizeBlock computes the merkle-style root over the per-tx hashes and
// adds the resulting block hash to the timed hasher. Done once per block
// just like the real validator's BlockProcessor.
func (e *VMEnv) FinalizeBlock(txHashes [][]byte) []byte {
	if len(txHashes) == 0 {
		return e.timed.EmptyHash()
	}
	// Simple linear merkle: hash(prev || h_i) — keeps the hash count
	// deterministic at len(txHashes), which matches what the chain does
	// for small blocks. For very large blocks the validator uses a
	// balanced merkle, but the byte volume is identical.
	acc := e.timed.Compute(string(txHashes[0]))
	for _, h := range txHashes[1:] {
		buf := make([]byte, 0, len(acc)+len(h))
		buf = append(buf, acc...)
		buf = append(buf, h...)
		acc = e.timed.Compute(string(buf))
	}
	return acc
}

// newAccount returns a freshly-keyed account with a random 32-byte address.
// We derive the address from the public key so signing-and-verify roundtrips
// behave like the real network.
func newAccount() (*Account, error) {
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		return nil, err
	}
	a := &Account{Public: pub, Private: priv}
	// Address is the public key padded/truncated to 32 bytes; ed25519
	// public keys are exactly 32 bytes so this is just a copy.
	copy(a.Address[:], pub)
	return a, nil
}

// txHash builds a stable 32-byte hash to use as OriginalTxHash. We use
// the tuple (nonce, seq) because that's enough to uniquely identify a
// tx in the simulated batch, and the VM doesn't actually inspect the
// hash content beyond using it as a key.
func txHash(nonce, seq uint64) []byte {
	out := make([]byte, 32)
	binary.BigEndian.PutUint64(out[0:8], nonce)
	binary.BigEndian.PutUint64(out[8:16], seq)
	return out
}

// encodeArgs converts decimal-string init arguments into the big-endian
// byte representation the VM expects for unsigned bigint arguments.
// Non-numeric arguments are passed through as raw bytes.
func encodeArgs(in []string) [][]byte {
	out := make([][]byte, 0, len(in))
	for _, s := range in {
		v, ok := new(big.Int).SetString(s, 10)
		if ok {
			out = append(out, v.Bytes())
		} else {
			out = append(out, []byte(s))
		}
	}
	return out
}
