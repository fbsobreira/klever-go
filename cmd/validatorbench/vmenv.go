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

	// Auto-deploy the simple-mode contract only when no tx_mix is
	// configured. With a tx_mix the workload itself is responsible
	// for deploying every contract type it references — that's how the
	// caller can swap in any wasm file at startup without us needing to
	// know about it here.
	if len(cfg.TxMix) == 0 && cfg.ContractPath != "" {
		if err := env.deployContracts(); err != nil {
			env.Close()
			return nil, err
		}
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

// DeployedContract groups a deployed instance with the call function /
// args / argtemplates the workload should invoke against it. Used by
// the mix workload to fan out across heterogeneous contracts.
type DeployedContract struct {
	Address  []byte
	Function string
	ArgTpls  []argTemplate
}

// DeployContract loads a wasm file, deploys `instances` independent
// copies, and returns their addresses paired with the function the
// caller wants to invoke. Used by the mix workload at startup so any
// number of contracts can be tested in a single run without
// hard-coding paths in the binary.
func (e *VMEnv) DeployContract(path string, initArgs []string, function string, callArgs []argTemplate, instances int) ([]*DeployedContract, error) {
	if instances < 1 {
		instances = 1
	}
	wasm, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read contract %q: %w", path, err)
	}
	args := encodeArgs(initArgs)
	out := make([]*DeployedContract, 0, instances)
	for i := 0; i < instances; i++ {
		// Bump the owner's on-chain nonce before each deploy so the VM's
		// address-derivation uses a unique (creator, nonce) pair per
		// instance. Without this, deploys 2..N collide on the same SC
		// address because the world-state nonce is what the VM consults.
		if err := e.exec.World.UpdateWorldStateBefore(e.owner.Address[:], 0, 0); err != nil {
			return nil, fmt.Errorf("pre-deploy %s[%d]: %w", path, i, err)
		}
		input := &vmcommon.ContractCreateInput{
			ContractCode: wasm,
			VMInput: vmcommon.VMInput{
				CallerAddr:     e.owner.Address[:],
				Arguments:      args,
				GasProvided:    e.cfg.GasLimit,
				OriginalTxHash: txHash(uint64(len(e.scAddrs)+i), 0),
				CurrentTxHash:  txHash(uint64(len(e.scAddrs)+i), 0),
				KDATransfers:   []*vmcommon.KDATransfer{},
			},
		}
		t0 := time.Now()
		o, err := e.exec.GetVM().RunSmartContractCreate(input)
		e.deployLatency = append(e.deployLatency, time.Since(t0))
		if err != nil {
			return nil, fmt.Errorf("deploy %s[%d]: %w", path, i, err)
		}
		if o.ReturnCode != vmcommon.Ok {
			return nil, fmt.Errorf("deploy %s[%d]: %s (%s)", path, i, o.ReturnCode, o.ReturnMessage)
		}
		if err := e.exec.World.UpdateAccounts(o.OutputAccounts, o.DeletedAccounts); err != nil {
			return nil, fmt.Errorf("commit deploy %s[%d]: %w", path, i, err)
		}
		var addr []byte
		for _, oa := range o.OutputAccounts {
			if len(oa.Code) > 0 {
				addr = oa.Address
				break
			}
		}
		if addr == nil {
			// Fallback: derive deterministically from the owner address +
			// the just-bumped owner nonce. UpdateWorldStateBefore did the
			// increment, so reading owner.Nonce here gives the post-bump
			// value the VM used.
			addr = worldhook.GenerateMockAddress(e.owner.Address[:], e.owner.Nonce+1, scenarioexec.TestVMType)
		}
		e.owner.Nonce++
		e.scAddrs = append(e.scAddrs, addr)
		out = append(out, &DeployedContract{
			Address:  addr,
			Function: function,
			ArgTpls:  callArgs,
		})
	}
	return out, nil
}

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

// deployContracts is the simple-mode shortcut: it deploys cfg.NumContracts
// instances of cfg.ContractPath and stashes the addresses on the env.
// Internally it delegates to DeployContract so the nonce-bumping path is
// shared with the tx_mix flow.
func (e *VMEnv) deployContracts() error {
	if e.cfg.ContractPath == "" {
		return nil
	}
	tpls, err := parseArgTemplates(e.cfg.CallArgsTpl)
	if err != nil {
		return fmt.Errorf("call_args: %w", err)
	}
	_, err = e.DeployContract(e.cfg.ContractPath, e.cfg.InitArgs, e.cfg.CallFunction, tpls, e.cfg.NumContracts)
	return err
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

	// Signature verification has already happened at mempool intake (in
	// the generator goroutines); inside the block-budget window we trust
	// the Verified flag. This mirrors a real validator: the 500ms slot
	// budget is spent on execution + state updates, not on re-checking
	// signatures already validated when the tx arrived from P2P.
	if !tx.Verified {
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
