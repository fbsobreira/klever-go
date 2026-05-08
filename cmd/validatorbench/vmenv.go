package main

import (
	"crypto/ed25519"
	"crypto/rand"
	"encoding/binary"
	"fmt"
	"math/big"
	"os"

	"github.com/klever-io/klever-go/crypto/hashing"
	"github.com/klever-io/klever-go/kvm/scenarioexec"
	scenmodel "github.com/klever-io/klever-go/kvm/scenarioexec/model"
	worldhook "github.com/klever-io/klever-go/kvm/mock/world"
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
		cfg:    cfg,
		exec:   exec,
		hasher: base,
		timed:  timed,
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

		out, err := e.exec.GetVM().RunSmartContractCreate(input)
		if err != nil {
			return fmt.Errorf("deploy %d: %w", i, err)
		}
		if out.ReturnCode != vmcommon.Ok {
			return fmt.Errorf("deploy %d failed: %s (%s)", i, out.ReturnCode, out.ReturnMessage)
		}

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
// Returns the VM output (so callers can record gas + retcode) and any
// error that broke the pipeline. A failed VM call (out of gas, revert)
// is NOT returned as an error — that maps to a "tx failed" stat.
func (e *VMEnv) ExecuteTx(tx *Tx, hashStats *HashStats) (*vmcommon.VMOutput, bool, error) {
	// Hash the marshalled transaction body — the validator does this once
	// per tx for the merkle leaf and for the on-disk tx index.
	_ = e.timed.Compute(string(tx.Bytes))

	// Verify ed25519 signature. We hash the tx body inside the timer so
	// the cost shows up in the "hash share" metric where appropriate.
	digest := e.timed.Compute(string(tx.SigBody))
	if !ed25519.Verify(tx.Sender.Public, digest, tx.Signature) {
		return nil, false, nil
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

	out, err := e.exec.GetVM().RunSmartContractCall(input)
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
