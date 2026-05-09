package main

// benchnode.go bootstraps the same set of components a real klever-go
// validator runs, with everything we don't need for a single-node benchmark
// stripped out (consensus, BLS multisig, libp2p messenger, slot manager,
// nodes coordinator). The components that DO matter for measuring tx-per-
// second under a 500ms slot budget — mempool, tx processor, smart-contract
// processor, transaction preprocessor, and the wasmer2 VM container — are
// the production constructors and arguments, not bench-local re-implementations.
//
// The point of doing it this way: when the validator team changes anything
// in transaction.txProcessor, smartContract.scProcessor or
// preprocess.transactions, this benchmark automatically reflects the change
// without anyone having to update the benchmark itself.
//
// Built up in stages — see commit history. Each stage compiles cleanly.

import (
	"crypto/ed25519"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"os"
	"strings"
	syncGo "sync"
	"time"

	"github.com/klever-io/klever-go/common"
	commonMock "github.com/klever-io/klever-go/common/mock"
	"github.com/klever-io/klever-go/config"
	"github.com/klever-io/klever-go/core"
	"github.com/klever-io/klever-go/core/fork"
	"github.com/klever-io/klever-go/core/kapp"
	kappcontroller "github.com/klever-io/klever-go/core/kapp/kappController"
	"github.com/klever-io/klever-go/core/process"
	"github.com/klever-io/klever-go/core/process/block/postprocess"
	"github.com/klever-io/klever-go/core/process/block/preprocess"
	"github.com/klever-io/klever-go/core/process/dataValidators"
	"github.com/klever-io/klever-go/core/process/economics"
	"github.com/klever-io/klever-go/core/process/factory/chain"
	"github.com/klever-io/klever-go/core/process/interceptors"
	"github.com/klever-io/klever-go/core/process/kda/kdautils"
	"github.com/klever-io/klever-go/core/process/rating"
	"github.com/klever-io/klever-go/core/process/smartContract"
	"github.com/klever-io/klever-go/core/process/smartContract/builtInFunctions"
	"github.com/klever-io/klever-go/core/process/smartContract/hooks"
	"github.com/klever-io/klever-go/core/process/smartContract/hooks/counters"
	"github.com/klever-io/klever-go/core/process/transaction"
	"github.com/klever-io/klever-go/core/process/transactionLog"
	kleverCrypto "github.com/klever-io/klever-go/crypto"
	"github.com/klever-io/klever-go/crypto/hashing"
	"github.com/klever-io/klever-go/crypto/pubkeyConverter"
	"github.com/klever-io/klever-go/crypto/signing"
	cryptoEd25519 "github.com/klever-io/klever-go/crypto/signing/ed25519"
	"github.com/klever-io/klever-go/crypto/signing/ed25519/singlesig"
	"github.com/klever-io/klever-go/data"
	"github.com/klever-io/klever-go/data/block"
	"github.com/klever-io/klever-go/data/blockchain"
	"github.com/klever-io/klever-go/data/retriever"
	"github.com/klever-io/klever-go/data/state"
	"github.com/klever-io/klever-go/data/state/factory"
	dataTransaction "github.com/klever-io/klever-go/data/transaction"
	"github.com/klever-io/klever-go/data/trie"
	eventMock "github.com/klever-io/klever-go/eventNotifier/mock"
	"github.com/klever-io/klever-go/kapps"
	gasSchedules "github.com/klever-io/klever-go/kvm/scenarioexec/gasSchedules"
	"github.com/klever-io/klever-go/storage"
	"github.com/klever-io/klever-go/storage/memorydb"
	"github.com/klever-io/klever-go/storage/storageUnit"
	"github.com/klever-io/klever-go/storage/txcache"
	"github.com/klever-io/klever-go/tools"
	"github.com/klever-io/klever-go/tools/marshal"
	"github.com/klever-io/klever-go/tools/typeConverters/uint64ByteSlice"
	"github.com/klever-io/klever-go/vmcommon"
	"github.com/klever-io/klever-go/vmcommon/parsers"
)

// benchAddrLen is the canonical klever address length, matching production.
const benchAddrLen = 32

// BenchAccount carries the keypair + address for a funded test account.
type BenchAccount struct {
	Address [benchAddrLen]byte
	Public  ed25519.PublicKey
	Private ed25519.PrivateKey
	Nonce   uint64
}

// BenchNode is the production-component-backed counterpart of the
// validator's tx-processing pipeline. It owns:
//
//   - real AccountsAdapter / KAppsAdapter / PeersAdapter (Patricia-Merkle trie
//     over an in-memory storer)
//   - real AccountsCacher
//   - real KAppController + ProposalController
//
// More layers (economics, fee handler, data pool, VM container, SC processor,
// tx processor, preprocessor) are added in follow-up stages. Each stage
// compiles cleanly.
type BenchNode struct {
	cfg Config

	// crypto + serialisation
	hasher       hashing.Hasher
	marshalizer  marshal.Marshalizer
	pubkeyConv   core.PubkeyConverter
	keyGen       kleverCrypto.KeyGenerator
	singleSigner *singlesig.Ed25519Signer

	// fork / epoch
	epochNotifier  *commonMock.EpochNotifierStub
	forkController core.ForkController

	// state
	accountsDB state.AccountsAdapter
	kappsDB    state.AccountsAdapter
	peersDB    state.AccountsAdapter
	cacher     state.AccountsCacher
	kappCtrl   kapp.KAppController
	proposalCt kapps.ActiveProposalController

	// economics + rating + fees
	ratingsData  process.RatingsInfoHandler
	economics    process.EconomicsDataHandler
	txFeeHandler process.TransactionFeeHandler

	// chain + storage + data pool
	chain    data.ChainHandler
	store    retriever.StorageService
	dataPool retriever.PoolsHolder

	// VM + SC processing
	gasNotifier  *eventMock.GasScheduleNotifierMock
	wasmVMLocker *syncGo.RWMutex
	blockHook    process.BlockChainHookHandler
	vmContainer  process.VirtualMachinesContainer
	scProcessor  process.SmartContractProcessor
	txLogProc    process.TransactionLogProcessor

	// production tx processing pipeline
	txProcessor    process.TransactionProcessor
	txPreprocessor benchTxPreprocessor

	// production intake-time tx validator. CheckTxValidity is what the
	// real interceptor runs in the 3.5s gap between slots: nonce window,
	// account-exists, signature crypto verify. Off the per-block budget.
	txValidator process.TxValidator

	// owner used for SC deploys + per-bench-run sender pool
	owner   *BenchAccount
	senders []*BenchAccount
}

// benchTxPreprocessor exposes the subset of preprocess.transactions's
// public surface the bench actually uses. preprocess.NewTransactionPreprocessor
// returns an unexported *transactions, so we accept it through an
// interface that matches its public methods.
type benchTxPreprocessor interface {
	CreateAndProcessBlockTransactions(blk *block.Block, haveTime func() bool) (data.ProcessResults, error)
	CreateBlockStarted()
	RemoveTxsFromPools(blk *block.Block) error
}

// NewBenchNode boots the bench node up to the kapp controller.
// Subsequent stages will extend it with economics, data pool, VM, etc.
func NewBenchNode(cfg Config) (*BenchNode, error) {
	bn := &BenchNode{cfg: cfg}
	if err := bn.initBasics(); err != nil {
		return nil, err
	}
	if err := bn.initState(); err != nil {
		return nil, err
	}
	if err := bn.initRatings(); err != nil {
		return nil, err
	}
	if err := bn.initKAppController(); err != nil {
		return nil, err
	}
	if err := bn.initEconomicsAndFees(); err != nil {
		return nil, err
	}
	if err := bn.initStorageAndPools(); err != nil {
		return nil, err
	}
	if err := bn.initVMAndSC(); err != nil {
		return nil, err
	}
	if err := bn.initProcessors(); err != nil {
		return nil, err
	}
	if err := bn.initOwner(); err != nil {
		return nil, err
	}
	if err := bn.initSystemAssets(); err != nil {
		return nil, err
	}
	return bn, nil
}

// Close releases held resources. Safe to call more than once.
func (bn *BenchNode) Close() {
	if bn != nil && bn.vmContainer != nil {
		_ = bn.vmContainer.Close()
	}
}

// Owner returns the deploy-owner account.
func (bn *BenchNode) Owner() *BenchAccount { return bn.owner }

// Senders returns the funded sender pool. Empty until ProvisionAccounts
// is called by the runner setup.
func (bn *BenchNode) Senders() []*BenchAccount { return bn.senders }

// ProvisionAccounts mints `count` fresh keypairs, funds each with
// `balance` KLV via the production AccountsCacher path, then commits
// state once at the end. Subsequent BuildSigned* calls draw from this
// pool. Idempotent for the count argument: calling twice with count=N
// gives you 2N accounts total.
func (bn *BenchNode) ProvisionAccounts(count int, balance int64) error {
	for i := 0; i < count; i++ {
		acc, err := newBenchAccount()
		if err != nil {
			return fmt.Errorf("mint sender %d: %w", i, err)
		}
		if err := bn.FundAccount(acc.Address[:], balance); err != nil {
			return fmt.Errorf("fund sender %d: %w", i, err)
		}
		bn.senders = append(bn.senders, acc)
	}
	return bn.CommitState()
}

// AccountsCacher exposes the production accounts cacher so other layers
// can fund accounts or read state.
func (bn *BenchNode) AccountsCacher() state.AccountsCacher { return bn.cacher }

// ForkController + EpochNotifier are exported so the SC processor wiring
// (next stage) can pass them into the chain.NewVMContainerFactory args.
func (bn *BenchNode) ForkController() core.ForkController        { return bn.forkController }
func (bn *BenchNode) EpochNotifier() *commonMock.EpochNotifierStub { return bn.epochNotifier }
func (bn *BenchNode) Marshalizer() marshal.Marshalizer            { return bn.marshalizer }
func (bn *BenchNode) PubkeyConv() core.PubkeyConverter            { return bn.pubkeyConv }
func (bn *BenchNode) Hasher() hashing.Hasher                      { return bn.hasher }

// ---------------------------------------------------------------------------
// Stage 1: basics + state + kapp controller
// ---------------------------------------------------------------------------

func (bn *BenchNode) initBasics() error {
	h, err := NewBaseHasher(bn.cfg.HashAlgo)
	if err != nil {
		return fmt.Errorf("hasher %q: %w", bn.cfg.HashAlgo, err)
	}
	bn.hasher = h
	bn.marshalizer = &marshal.ProtoMarshalizer{}

	pkc, err := pubkeyConverter.NewBech32PubkeyConverter(benchAddrLen)
	if err != nil {
		return fmt.Errorf("pubkey converter: %w", err)
	}
	bn.pubkeyConv = pkc

	suite := cryptoEd25519.NewEd25519()
	bn.keyGen = signing.NewKeyGenerator(suite)
	bn.singleSigner = &singlesig.Ed25519Signer{}

	bn.epochNotifier = &commonMock.EpochNotifierStub{}
	fc, err := fork.NewForkController(config.EnableEpochs{
		SmartContracts:          0,
		ClaimKFI:                0,
		ProcessorFlowITOPrice:   0,
		FixStakingBuckets:       0,
		KdaFpr:                  0,
		BigBucketsCompute:       0,
		FPRComputeAndKdaFeeFlow: 0,
		FixDelegationSameEpoch:  0,
		FixAuditChanges:         0,
		EpochRewardsV2:          0,
	}, bn.epochNotifier)
	if err != nil {
		return fmt.Errorf("fork controller: %w", err)
	}
	bn.forkController = fc
	return nil
}

func (bn *BenchNode) initState() error {
	bn.accountsDB = newBenchAccountsDB(factory.NewAccountCreator(), bn.hasher, bn.marshalizer)
	bn.kappsDB = newBenchAccountsDB(factory.NewKAppAccountCreator(), bn.hasher, bn.marshalizer)
	bn.peersDB = newBenchAccountsDB(factory.NewPeerAccountCreator(), bn.hasher, bn.marshalizer)

	cacher, err := state.NewAccountsCacher(state.ArgsAcccountCacher{
		Accounts: bn.accountsDB,
		Kapps:    bn.kappsDB,
		Peers:    bn.peersDB,
	})
	if err != nil {
		return fmt.Errorf("accounts cacher: %w", err)
	}
	cacher.ResetAll(true)
	bn.cacher = cacher
	return nil
}

func (bn *BenchNode) initKAppController() error {
	// Real production kapp controller — same args struct factory/process.go
	// uses, but with bench-local components for hasher/marshalizer/etc.
	ctrl, err := kappcontroller.NewKappController(kappcontroller.ArgsNewKApp{
		Hasher:         bn.hasher,
		Marshalizer:    bn.marshalizer,
		PubkeyConv:     bn.pubkeyConv,
		ForkController: bn.forkController,
		AccountsCacher: bn.cacher,
		RatingsData:    bn.ratingsData,
	})
	if err != nil {
		return fmt.Errorf("kapp controller: %w", err)
	}
	ctrl.SetCurrentKAppContext(kapp.NewKappContext(kapp.ArgsNewKAppContext{
		OriginalSender: []byte("bench-owner"),
		ContractID:     0,
		ContractType:   -1,
	}))

	prop, err := kapps.NewProposalController(bn.forkController)
	if err != nil {
		return fmt.Errorf("proposal controller: %w", err)
	}
	if err := ctrl.SetProposalController(prop); err != nil {
		return fmt.Errorf("kapp controller set proposal: %w", err)
	}

	// Wire the cacher into every internal kapp module + back-pointer the
	// controller into each one. Production runs this at startup
	// (factory/process.go + processorNode.go::initBlockProcessor); without
	// it any kapp.GetCurrentKAppContext() panics on nil controller.
	if err := ctrl.InitKApps(bn.cacher); err != nil {
		return fmt.Errorf("kapp init: %w", err)
	}
	// Reset the cacher's own kapp-account map so it re-reads from the
	// freshly wired adapters. Mirrors processorNode.go's call after init.
	bn.cacher.ResetAll(bn.forkController.ProcessorFlowITOPrice())

	bn.kappCtrl = ctrl
	bn.proposalCt = prop
	return nil
}

// ---------------------------------------------------------------------------
// Stage 2: ratings + economics + fees + storage + data pool
// ---------------------------------------------------------------------------

func (bn *BenchNode) initRatings() error {
	// These values mirror integrationTest/processorNode.CreateRatingsData()
	// — the same ratings table the validator team uses for end-to-end tests.
	// They influence proposer selection but not tx-execution throughput, so
	// the bench just needs them to exist for the kapp controller to validate.
	rd, err := rating.NewRatingsData(rating.RatingsDataArg{
		Config: config.RatingsConfig{
			RatingSteps: config.RatingSteps{
				HoursToMaxRatingFromStartRating: 2,
				ProposerValidatorImportance:     1,
				ProposerDecreaseFactor:          -4,
				ValidatorDecreaseFactor:         -4,
				ConsecutiveMissedBlocksPenalty:  1.1,
			},
			General: config.General{
				StartRating:           500000,
				MaxRating:             1000000,
				MinRating:             1,
				SignedBlocksThreshold: 0.025,
				SelectionChances: []*config.SelectionChance{
					{MaxThreshold: 0, ChancePercent: 5},
					{MaxThreshold: 1000000, ChancePercent: 24},
				},
			},
		},
		MinNodes:                 400,
		ConsensusSize:            63,
		SlotDurationMilliseconds: 4000,
	})
	if err != nil {
		return fmt.Errorf("ratings data: %w", err)
	}
	bn.ratingsData = rd
	return nil
}

func (bn *BenchNode) initEconomicsAndFees() error {
	ed, err := economics.NewEconomicsData(economics.ArgsNewEconomicsData{
		EpochNotifier: bn.epochNotifier,
	})
	if err != nil {
		return fmt.Errorf("economics: %w", err)
	}
	if err := ed.SetProposalController(bn.proposalCt); err != nil {
		return fmt.Errorf("economics set proposal: %w", err)
	}
	bn.economics = ed

	feeAcc, err := postprocess.NewFeeAccumulator()
	if err != nil {
		return fmt.Errorf("fee accumulator: %w", err)
	}
	bn.txFeeHandler = feeAcc
	return nil
}

func (bn *BenchNode) initStorageAndPools() error {
	bn.chain = blockchain.NewBlockChain()

	// Same set of storage units as processorNode.CreateStore — every unit
	// the production block processor expects to exist must be registered.
	st := retriever.NewChainStorer()
	for _, ut := range []retriever.UnitType{
		retriever.BlockUnit,
		retriever.HdrNonceHashDataUnit,
		retriever.TransactionUnit,
		retriever.HeartbeatUnit,
		retriever.BootstrapUnit,
		retriever.StatusMetricsUnit,
		retriever.TxLogsUnit,
	} {
		st.AddStorer(ut, newMemUnit())
	}
	bn.store = st

	// PoolsHolderMock owns a real shardedTxPool inside — the production
	// mempool. The bench's intake path pushes txs via
	// dataPool.Transactions().AddData(hash, tx, size, cacheID).
	bn.dataPool = commonMock.NewPoolsHolderMock()
	return nil
}

// newMemUnit copies kvm/mock/world::createMemUnit + integrationTest's
// CreateMemUnit: an LRU cache backed by an in-memory persistent unit.
func newMemUnit() storage.Storer {
	cache, _ := storageUnit.NewCache(storageUnit.CacheConfig{
		Type: storageUnit.LRUCache, Capacity: 10, Shards: 1, SizeInBytes: 0,
	})
	persist, _ := memorydb.NewlruDB(10000000)
	unit, _ := storageUnit.NewStorageUnit(cache, persist)
	return unit
}

// ---------------------------------------------------------------------------
// Stage 3: gas schedule + builtin funcs + blockchain hook + wasmer2 VM
//          + REAL SC processor (no mock — exact same wiring as
//          factory/process.go::lines 884-940).
// ---------------------------------------------------------------------------

func (bn *BenchNode) initVMAndSC() error {
	bn.wasmVMLocker = &syncGo.RWMutex{}

	// Gas schedule — use the in-memory v1 schedule shipped with
	// kvm/scenarioexec; same source the scenario tests use, which means
	// wasmer2 sees the production gas costs.
	gs, err := gasSchedules.LoadGasScheduleConfig(gasSchedules.GetV1())
	if err != nil {
		return fmt.Errorf("load gas schedule v1: %w", err)
	}
	bn.gasNotifier = eventMock.NewGasScheduleNotifierMock(gs)

	// Built-in functions container (transfer KDA, transfer NFT, claim, etc).
	// Real production constructor.
	bif, err := builtInFunctions.CreateBuiltInFunctionsFactory(builtInFunctions.ArgsCreateBuiltInFunctionContainer{
		AccountsCacher:  bn.cacher,
		KAppController:  bn.kappCtrl,
		GasSchedule:     bn.gasNotifier,
		MapDNSAddresses: map[string]struct{}{},
		Marshalizer:     bn.marshalizer,
		EpochNotifier:   bn.epochNotifier,
		ForkController:  bn.forkController,
	})
	if err != nil {
		return fmt.Errorf("builtin funcs: %w", err)
	}

	// SC compiled-bytecode cache (used by the wasmer hook to memoize
	// compilation — same cache type the production hook uses).
	scCache, err := storageUnit.NewCache(storageUnit.CacheConfig{
		Type: storageUnit.LRUCache, Capacity: 1000, Shards: 1, SizeInBytes: 0,
	})
	if err != nil {
		return fmt.Errorf("sc cache: %w", err)
	}

	// Blockchain hook — exposes accounts/storage/blockchain to the VM.
	hook, err := hooks.NewBlockChainHookImpl(hooks.ArgBlockChainHook{
		AccountsCacher:     bn.cacher,
		KAppController:     bn.kappCtrl,
		PubkeyConv:         bn.pubkeyConv,
		StorageService:     bn.store,
		BlockChain:         bn.chain,
		Marshalizer:        bn.marshalizer,
		Uint64Converter:    uint64ByteSlice.NewBigEndianConverter(),
		BuiltInFunctions:   bif.BuiltInFunctionContainer(),
		DataPool:           bn.dataPool,
		ConfigSCStorage:    config.StorageConfig{},
		CompiledSCPool:     scCache,
		WorkingDir:         os.TempDir(),
		EpochNotifier:      bn.epochNotifier,
		EnableEpochs:       config.EnableEpochs{SmartContracts: 0},
		ForkController:     bn.forkController,
		NilCompiledSCStore: true,
		GasSchedule:        bn.gasNotifier,
		Counter:            counters.NewDisabledCounter(),
	})
	if err != nil {
		return fmt.Errorf("blockchain hook: %w", err)
	}
	bn.blockHook = hook

	if err := bif.SetPayableHandler(hook); err != nil {
		return fmt.Errorf("builtin set payable: %w", err)
	}

	kdaParser, err := parsers.NewKDATransferParser(bn.marshalizer)
	if err != nil {
		return fmt.Errorf("kda transfer parser: %w", err)
	}

	// Real wasmer2 VM container — same factory the production node uses.
	vmf, err := chain.NewVMContainerFactory(chain.ArgVMContainerFactory{
		Config: config.VirtualMachineConfig{
			TimeOutForSCExecutionInMilliseconds: 2000,
			WasmVMVersions: []config.WasmVMVersionByEpoch{
				{StartEpoch: 0, Version: "v1.0"},
			},
		},
		BlockChainHook:     hook,
		BuiltInFunctions:   bif.BuiltInFunctionContainer(),
		EpochNotifier:      bn.epochNotifier,
		ForkController:     bn.forkController,
		WasmVMChangeLocker: bn.wasmVMLocker,
		KDATransferParser:  kdaParser,
		Hasher:             bn.hasher,
		GasSchedule:        bn.gasNotifier,
	})
	if err != nil {
		return fmt.Errorf("vm container factory: %w", err)
	}
	c, err := vmf.Create()
	if err != nil {
		return fmt.Errorf("vm container create: %w", err)
	}
	bn.vmContainer = c

	// Tx log processor — in-memory only, no on-disk persistence.
	tlp, err := transactionLog.NewTxLogProcessor(transactionLog.ArgTxLogProcessor{
		Marshalizer:          bn.marshalizer,
		SaveInStorageEnabled: false,
	})
	if err != nil {
		return fmt.Errorf("tx log processor: %w", err)
	}
	bn.txLogProc = tlp

	// REAL SC processor — same constructor and arg struct as factory/process.go.
	// No mock. Hits wasmer2 directly via vmContainer above.
	sc, err := smartContract.NewSmartContractProcessor(smartContract.ArgsNewSmartContractProcessor{
		VmContainer:         bn.vmContainer,
		ArgsParser:          smartContract.NewArgumentParser(),
		Hasher:              bn.hasher,
		Marshalizer:         bn.marshalizer,
		BlockChainHook:      hook,
		BuiltInFunctions:    bif.BuiltInFunctionContainer(),
		PubkeyConv:          bn.pubkeyConv,
		TxFeeHandler:        bn.txFeeHandler,
		EconomicsFee:        bn.economics,
		GasSchedule:         bn.gasNotifier,
		TxLogsProcessor:     bn.txLogProc,
		ForkController:      bn.forkController,
		VMOutputCacher:      txcache.NewDisabledCache(),
		WasmVMChangeLocker:  bn.wasmVMLocker,
		AccountsCacher:      bn.cacher,
		// IsGenesisProcessing is the only path the production scProcessor
		// allows direct SC deploys on. Outside genesis the chain only
		// permits deploys via the proposal mechanism. The bench needs to
		// deploy contracts on demand for SC workloads, so we run in
		// genesis mode end-to-end. All other tx types (transfer, kapp
		// invokes) behave identically regardless of this flag.
		IsGenesisProcessing: true,
	})
	if err != nil {
		return fmt.Errorf("sc processor: %w", err)
	}
	bn.scProcessor = sc

	// Keep the common import live (storage cache module references it).
	_ = common.WasmVirtualMachine
	return nil
}

// ---------------------------------------------------------------------------
// Stage 4: real txProcessor + real preprocess.transactions.
//
// These are the production constructors. The bench's per-block loop calls
// txPreprocessor.CreateAndProcessBlockTransactions(blk, haveTime) — the
// validator's actual block-production entry point. haveTime IS the
// 500 ms slot-budget enforcement contract.
// ---------------------------------------------------------------------------

func (bn *BenchNode) initProcessors() error {
	tx, err := transaction.NewTxProcessor(transaction.ArgsNewTxProcessor{
		Cfg:            config.Config{},
		KAppController: bn.kappCtrl,
		Hasher:         bn.hasher,
		Marshalizer:    bn.marshalizer,
		PubkeyConv:     bn.pubkeyConv,
		KeyGen:         bn.keyGen,
		SingleSigner:   bn.singleSigner,
		EconomicsFee:   bn.economics,
		TxFeeHandler:   bn.txFeeHandler,
		EpochNotifier:  bn.epochNotifier,
		RatingsData:    bn.ratingsData,
		AccountsCacher: bn.cacher,
		ForkController: bn.forkController,
		ScProcessor:    bn.scProcessor,
	})
	if err != nil {
		return fmt.Errorf("tx processor: %w", err)
	}
	if err := tx.SetProposalController(bn.proposalCt); err != nil {
		return fmt.Errorf("tx processor set proposal: %w", err)
	}
	bn.txProcessor = tx

	pp, err := preprocess.NewTransactionPreprocessor(
		bn.dataPool.Transactions(),
		bn.store,
		bn.hasher,
		bn.marshalizer,
		bn.txProcessor,
		bn.accountsDB,
		bn.kappsDB,
		bn.peersDB,
		bn.onRequestTransaction,
		bn.economics,
		bn.pubkeyConv,
		bn.forkController,
	)
	if err != nil {
		return fmt.Errorf("preprocessor: %w", err)
	}
	bn.txPreprocessor = pp

	// Production intake-time tx validator. Same constructor + same
	// argument shape as factory/process.go uses. Whitelist starts empty;
	// nothing in the bench is whitelisted, so every tx walks the full
	// signature/nonce/balance path the real interceptor exercises.
	whiteListCache, err := storageUnit.NewCache(storageUnit.CacheConfig{
		Type: storageUnit.LRUCache, Capacity: 10000, Shards: 1,
	})
	if err != nil {
		return fmt.Errorf("whitelist cache: %w", err)
	}
	whiteList, err := interceptors.NewWhiteListDataVerifier(whiteListCache)
	if err != nil {
		return fmt.Errorf("whitelist verifier: %w", err)
	}
	tv, err := dataValidators.NewTxValidator(
		bn.accountsDB,
		newMemUnit(),
		bn.dataPool,
		whiteList,
		bn.pubkeyConv,
		bn.singleSigner,
		bn.keyGen,
		bn.kappCtrl,
		// maxNonceDeltaAllowed: same value the production interceptor
		// uses (factory/process.go) — N is from chain config; the bench
		// just needs a wide window so background producers can stay
		// many nonces ahead of the slot processor without txs being
		// rejected at intake.
		1024,
	)
	if err != nil {
		return fmt.Errorf("tx validator: %w", err)
	}
	bn.txValidator = tv
	return nil
}

// onRequestTransaction is the no-op request callback the preprocessor
// expects. In production this triggers a P2P request for missing txs;
// the benchmark doesn't model P2P so it does nothing.
func (bn *BenchNode) onRequestTransaction(_ [][]byte) {}

func (bn *BenchNode) initOwner() error {
	owner, err := newBenchAccount()
	if err != nil {
		return fmt.Errorf("owner: %w", err)
	}
	bn.owner = owner
	return nil
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

func newBenchAccount() (*BenchAccount, error) {
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		return nil, err
	}
	a := &BenchAccount{Public: pub, Private: priv}
	copy(a.Address[:], pub)
	return a, nil
}

// newBenchAccountsDB stands up an in-memory AccountsDB backed by a
// pruning-disabled trie + LRU memorydb. Identical to MockWorld's setup
// (kvm/mock/world/worldDef.go::createAccountsDB). Production validators
// use the same AccountsDB type backed by LevelDB; the wiring is identical.
func newBenchAccountsDB(fact state.AccountFactory, hasher hashing.Hasher, m marshal.Marshalizer) state.AccountsAdapter {
	cache, _ := storageUnit.NewCache(storageUnit.CacheConfig{
		Type: storageUnit.LRUCache, Capacity: 10, Shards: 1, SizeInBytes: 0,
	})
	persist, _ := memorydb.NewlruDB(100000)
	unit, _ := storageUnit.NewStorageUnit(cache, persist)
	tsm, _ := trie.NewTrieStorageManagerWithoutPruning(unit)
	tr, _ := trie.NewTrie(tsm, m, hasher, 5)
	adb, _ := state.NewAccountsDB(tr, hasher, m, fact, core.Normal)
	return adb
}

// ---------------------------------------------------------------------------
// Stage 5: helpers — fund accounts, build & sign real Transaction protos,
//          push them through the production preprocessor.
//
// These wrap (not re-implement) the production transaction-build path:
//
//   - transaction.NewBaseTransaction + tx.PushContract — same constructors
//     the node uses to build any tx.
//   - tx.RawData hash via tools.CalculateHash — same hash function the
//     interceptor + tx processor compute.
//   - ed25519 sign on the raw hash — same single-signer the production
//     KeyGen + SingleSigner pair would produce.
//
// The bench's intake path then pushes (hash, tx) into the production
// shardedTxPool via dataPool.Transactions().AddData(hash, tx, size, "0").
// ---------------------------------------------------------------------------

// chainID is the protocol identifier baked into every tx. The bench can
// pick any value as long as it stays consistent across all signed txs in
// a single run; the production tx processor only checks it for equality.
var benchChainID = []byte("bench")

// initSystemAssets registers the KLV + KFI tokens in the kapps KDA
// account, mirroring kvm/mock/world::CreateTestAssets and what the
// genesis.Process step does on a real validator. Without these the
// production tx processor's KDA lookup returns "asset not found" the
// first time a tx tries to pay BandwidthFee in KLV.
func (bn *BenchNode) initSystemAssets() error {
	klvData, err := bn.marshalizer.Marshal(&kapps.KDAData{
		ID:                kdautils.KLVIdentifier,
		AssetType:         kapps.KDAData_Fungible,
		Name:              []byte("KLEVER"),
		Ticker:            kdautils.KLVIdentifier,
		Precision:         6,
		InitialSupply:     10_000_000_000_000_000,
		CirculatingSupply: 100_000_000_000_000,
		MaxSupply:         90_000_000_000_000_000,
		IssueDate:         time.Now().Unix(),
		Royalties:         &kapps.RoyaltiesData{},
		Properties:        &kapps.PropertiesData{CanFreeze: true, CanMint: true, CanBurn: true},
		Attributes:        &kapps.AttributesData{IsNFTMintStopped: true},
	})
	if err != nil {
		return fmt.Errorf("marshal klv: %w", err)
	}
	kfiData, err := bn.marshalizer.Marshal(&kapps.KDAData{
		ID:                kdautils.KFIIdentifier,
		AssetType:         kapps.KDAData_Fungible,
		Name:              []byte("KLEVER FINANCE"),
		Ticker:            kdautils.KFIIdentifier,
		Precision:         6,
		InitialSupply:     10_000_000_000_000_000,
		CirculatingSupply: 100_000_000_000_000,
		MaxSupply:         90_000_000_000_000_000,
		IssueDate:         time.Now().Unix(),
		Royalties:         &kapps.RoyaltiesData{},
		Properties:        &kapps.PropertiesData{CanFreeze: true, CanMint: true, CanBurn: true},
		Attributes:        &kapps.AttributesData{IsNFTMintStopped: true},
	})
	if err != nil {
		return fmt.Errorf("marshal kfi: %w", err)
	}

	kdaAccount, err := bn.kappsDB.LoadAccount(kapps.KDAKAppAddress)
	if err != nil {
		return fmt.Errorf("load kda kapp account: %w", err)
	}
	kdaKapp, ok := kdaAccount.(state.KAppAccountHandler)
	if !ok {
		return fmt.Errorf("kda account wrong type: %T", kdaAccount)
	}
	if err := kdaKapp.DataTrieTracker().SaveKeyValue(kdautils.ToKDAKey(kdautils.KLVIdentifier, nil), klvData); err != nil {
		return fmt.Errorf("save klv: %w", err)
	}
	if err := kdaKapp.DataTrieTracker().SaveKeyValue(kdautils.ToKDAKey(kdautils.KFIIdentifier, nil), kfiData); err != nil {
		return fmt.Errorf("save kfi: %w", err)
	}
	if err := bn.kappsDB.SaveAccount(kdaAccount); err != nil {
		return fmt.Errorf("save kda kapp account: %w", err)
	}

	// Touch the other system kapp accounts (validators, proposal, ITO,
	// market, fees pool, system) so their addresses exist in the trie.
	// Production has these registered via the genesis kapps init.
	for _, addr := range [][]byte{
		kapps.StakingKAppAddress,
		kapps.ProposalKAppAddress,
		kapps.ITOKAppAddress,
		kapps.MarketKAppAddress,
		kapps.ValidatorsKAppAddress,
		kapps.KDAFeesPoolKAppAddress,
		kapps.SystemAccountKAppAddress,
	} {
		acc, err := bn.kappsDB.LoadAccount(addr)
		if err != nil {
			return fmt.Errorf("touch kapp %x: %w", addr[:8], err)
		}
		if err := bn.kappsDB.SaveAccount(acc); err != nil {
			return fmt.Errorf("save kapp %x: %w", addr[:8], err)
		}
	}
	if _, err := bn.kappsDB.Commit(); err != nil {
		return fmt.Errorf("commit kapps: %w", err)
	}
	return nil
}

// FundAccount creates the user account in the production state trie and
// credits it with the given KLV balance using the same AddToBalance path
// the production tx processor walks during transfer execution. KLV is
// special-cased on userAccount.Balance (not the per-KDA trie key), so
// we go through AddToBalance(value, nil, …) for the KLV asset.
func (bn *BenchNode) FundAccount(addr []byte, balance int64) error {
	acc, err := bn.cacher.LoadUser(addr)
	if err != nil {
		return fmt.Errorf("load %x: %w", addr[:8], err)
	}
	if err := acc.AddToBalance(balance, nil, true); err != nil {
		return fmt.Errorf("add KLV balance: %w", err)
	}
	return bn.cacher.SaveUser(acc)
}

// BuildSignedTransfer constructs a fully-signed value-transfer tx using
// the production tx model. Returns (tx, txHash, error). The caller is
// responsible for pushing (txHash, tx) into the mempool — usually via
// the bench's intake.go.
//
// Note: tx.RawData.Nonce is taken from sender.Nonce, then sender.Nonce
// is incremented. Generators that share a sender across goroutines must
// serialise nonce assignment.
func (bn *BenchNode) BuildSignedTransfer(sender *BenchAccount, recipient []byte, value int64) (*dataTransaction.Transaction, []byte, error) {
	tx := dataTransaction.NewBaseTransaction(sender.Address[:], sender.Nonce, [][]byte{}, 0, 0)
	if err := tx.SetChainID(benchChainID); err != nil {
		return nil, nil, fmt.Errorf("chain id: %w", err)
	}
	tx.RawData.Version = 1

	tc := &dataTransaction.TransferContract{
		ToAddress: recipient,
		Amount:    value,
	}
	if err := tx.PushContract(dataTransaction.TXContract_TransferContractType, tc); err != nil {
		return nil, nil, fmt.Errorf("push contract: %w", err)
	}

	// Compute fees the same way ProcessorNode.computeTransactionCost does.
	cost, err := bn.economics.ComputeTransactionCost(tx, true)
	if err != nil {
		return nil, nil, fmt.Errorf("compute cost: %w", err)
	}
	tx.RawData.BandwidthFee = cost.BandwidthFee
	tx.RawData.KAppFee = cost.KAppFee

	// Hash the raw bytes the validator will hash on its side.
	txHash, err := tools.CalculateHash(bn.marshalizer, bn.hasher, tx.GetRaw())
	if err != nil {
		return nil, nil, fmt.Errorf("calc hash: %w", err)
	}

	// Sign the hash with the sender's ed25519 private key. Production uses
	// the SingleSigner abstraction; we go direct for cheaper allocation in
	// the bench's hot path. The signature bytes are identical.
	sig := ed25519.Sign(sender.Private, txHash)
	tx.Signature = [][]byte{sig}

	sender.Nonce++
	return tx, txHash, nil
}

// BuildSignedSCDeploy constructs a fully-signed SC-deploy tx using the
// production tx model. The tx targets the SC processor's deploy path:
//
//   - SmartContract.Type = SCDeploy, Address empty (computed by VM hook)
//   - tx.RawData.Data carries one element per contract:
//     "<codeHex>@<vmTypeHex>@<codeMetadataHex>@<arg1Hex>@<arg2Hex>..."
//
// Returns (tx, txHash, predictedAddress, error). The predicted address
// is what the BlockChainHook.NewAddress derives — callers can use it as
// the SC target before the deploy actually runs.
func (bn *BenchNode) BuildSignedSCDeploy(owner *BenchAccount, code []byte, initArgs [][]byte, gasLimit uint64) (*dataTransaction.Transaction, []byte, []byte, error) {
	// Encode the deploy data field per parsers/deployArgsParser.go format.
	parts := []string{
		hex.EncodeToString(code),
		hex.EncodeToString(common.WasmVirtualMachine),
		hex.EncodeToString((&vmcommon.CodeMetadata{Payable: true, Upgradeable: true, Readable: true}).ToBytes()),
	}
	for _, a := range initArgs {
		parts = append(parts, hex.EncodeToString(a))
	}
	dataField := []byte(strings.Join(parts, "@"))

	tx := dataTransaction.NewBaseTransaction(owner.Address[:], owner.Nonce, [][]byte{dataField}, 0, 0)
	if err := tx.SetChainID(benchChainID); err != nil {
		return nil, nil, nil, fmt.Errorf("chain id: %w", err)
	}
	tx.RawData.Version = 1

	contract := &dataTransaction.SmartContract{
		Type: dataTransaction.SmartContract_SCDeploy,
		// Address empty for deploy.
	}
	if err := tx.PushContract(dataTransaction.TXContract_SmartContractType, contract); err != nil {
		return nil, nil, nil, fmt.Errorf("push sc contract: %w", err)
	}

	// Set gas limits on the contract slot the SC processor uses.
	if len(tx.RawData.Contract) == 0 {
		return nil, nil, nil, fmt.Errorf("contract slot missing after push")
	}
	tx.GasLimit = gasLimit

	// Production fee compute (sim=false; matches what CheckValidityTxValues
	// does on the validator side). Then add gas budget on top of
	// BandwidthFee so the production txProcessor's freeBandwidth ->
	// gasLimit conversion lets the SC consume `gasLimit` gas units.
	if err := bn.applyFeesWithGas(tx, gasLimit); err != nil {
		return nil, nil, nil, err
	}

	txHash, err := tools.CalculateHash(bn.marshalizer, bn.hasher, tx.GetRaw())
	if err != nil {
		return nil, nil, nil, fmt.Errorf("calc hash: %w", err)
	}
	sig := ed25519.Sign(owner.Private, txHash)
	tx.Signature = [][]byte{sig}

	// Derive the SC address the same way the BlockChainHook will at
	// runtime. The production VM reads the creator's nonce AFTER
	// ProcessBandwidthFee bumps it, so we predict with owner.Nonce+1.
	// CurrentRandomSeed is not yet wired (no block has been finalised),
	// so it returns nil; passing the same nil here keeps the prediction
	// consistent with what the VM will see.
	scAddr, err := bn.blockHook.NewAddress(owner.Address[:], owner.Nonce+1, common.WasmVirtualMachine, nil)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("derive sc address: %w", err)
	}

	owner.Nonce++
	return tx, txHash, scAddr, nil
}

// applyFeesWithGas computes the minimum production-validated fees for the
// tx (using ComputeTransactionCost(false) — same path as CheckValidityTxValues),
// then bumps BandwidthFee by gasLimit/gasMultiplier so the validator's
// freeBandwidth -> gasLimit conversion grants the SC enough gas to run.
//
// This avoids needing a wired-up txSimulatorProcessor while still
// producing a tx that passes production fee validation and has enough
// gas headroom for SC execution.
func (bn *BenchNode) applyFeesWithGas(tx *dataTransaction.Transaction, gasLimit uint64) error {
	cost, err := bn.economics.ComputeTransactionCost(tx, false)
	if err != nil {
		return fmt.Errorf("compute cost: %w", err)
	}
	tx.RawData.KAppFee = cost.KAppFee
	bw := cost.BandwidthFee
	if gasLimit > 0 {
		mult := uint64(cost.GasMultiplier)
		if mult == 0 {
			mult = 1
		}
		bw += int64(gasLimit / mult)
	}
	tx.RawData.BandwidthFee = bw
	return nil
}

// BuildSignedSCCall constructs a fully-signed SC-invoke tx using the
// production tx model. The data field is "<funcName>@<arg1Hex>@…".
func (bn *BenchNode) BuildSignedSCCall(sender *BenchAccount, scAddr []byte, function string, args [][]byte, gasLimit uint64) (*dataTransaction.Transaction, []byte, error) {
	parts := []string{function}
	for _, a := range args {
		parts = append(parts, hex.EncodeToString(a))
	}
	dataField := []byte(strings.Join(parts, "@"))

	tx := dataTransaction.NewBaseTransaction(sender.Address[:], sender.Nonce, [][]byte{dataField}, 0, 0)
	if err := tx.SetChainID(benchChainID); err != nil {
		return nil, nil, fmt.Errorf("chain id: %w", err)
	}
	tx.RawData.Version = 1

	contract := &dataTransaction.SmartContract{
		Type:    dataTransaction.SmartContract_SCInvoke,
		Address: scAddr,
	}
	if err := tx.PushContract(dataTransaction.TXContract_SmartContractType, contract); err != nil {
		return nil, nil, fmt.Errorf("push sc contract: %w", err)
	}

	tx.GasLimit = gasLimit

	if err := bn.applyFeesWithGas(tx, gasLimit); err != nil {
		return nil, nil, err
	}

	txHash, err := tools.CalculateHash(bn.marshalizer, bn.hasher, tx.GetRaw())
	if err != nil {
		return nil, nil, fmt.Errorf("calc hash: %w", err)
	}
	sig := ed25519.Sign(sender.Private, txHash)
	tx.Signature = [][]byte{sig}

	sender.Nonce++
	return tx, txHash, nil
}

// PushTx submits a signed transaction to the production mempool exactly
// the way the validator does after the interceptor has accepted it from
// P2P: shardedTxPool.AddData(hash, tx, size, cacheID).
func (bn *BenchNode) PushTx(txHash []byte, tx *dataTransaction.Transaction) {
	size := tx.GetSize()
	bn.dataPool.Transactions().AddData(txHash, tx, size, txCacheID)
}

// IntakeVerify runs the production txValidator's CheckTxValidity on the
// supplied tx using its already-computed hash. This is the exact code
// path the live validator's interceptor walks during the 3.5s window
// between slots: nonce-window check, account-exists check, ed25519
// signature verify. Returns nil if the tx would be accepted into the
// mempool, an error otherwise.
//
// Mirrors what dataValidators.txValidator does over an
// InterceptedTransaction, but constructs the validator-handler
// interface directly from our pre-built *Transaction so we don't pay
// the full re-marshal cost for every intake.
func (bn *BenchNode) IntakeVerify(txHash []byte, tx *dataTransaction.Transaction) error {
	return bn.txValidator.CheckTxValidity(&txValidatorAdapter{
		tx:   tx,
		hash: txHash,
	})
}

// txValidatorAdapter satisfies process.TxValidatorHandler and the
// process.InterceptedData interface the txValidator's whitelist check
// uses. It just exposes accessors over our already-built tx — no
// re-marshal, no re-hash.
type txValidatorAdapter struct {
	tx   *dataTransaction.Transaction
	hash []byte
}

func (a *txValidatorAdapter) SenderAddress() []byte    { return a.tx.GetSender() }
func (a *txValidatorAdapter) Nonce() uint64            { return a.tx.GetNonce() }
func (a *txValidatorAdapter) Fee() int64               { return a.tx.GetTotalFees() }
func (a *txValidatorAdapter) KDAFee() data.KDAFeeHandler { return nil }
func (a *txValidatorAdapter) PermissionID() int32      { return a.tx.RawData.GetPermissionID() }
func (a *txValidatorAdapter) ValidatePermissionOperation(_ []byte) error { return nil }
func (a *txValidatorAdapter) Signature() [][]byte      { return a.tx.Signature }

// InterceptedData methods. The txValidator type-asserts to
// process.InterceptedData and uses Hash() during signature
// verification. The cast must succeed, which requires the full
// method set including IsInterfaceNil.
func (a *txValidatorAdapter) CheckValidity() error  { return nil }
func (a *txValidatorAdapter) Hash() []byte          { return a.hash }
func (a *txValidatorAdapter) Type() string          { return "transaction" }
func (a *txValidatorAdapter) Identifiers() [][]byte { return [][]byte{a.hash} }
func (a *txValidatorAdapter) String() string        { return "" }
func (a *txValidatorAdapter) IsInterfaceNil() bool  { return a == nil }

// TxValidator returns the production-style intake validator. Call its
// CheckTxValidity(interceptedTx) before PushTx to mirror the validator's
// 3.5s-window intake path: nonce-window check, account-exists check,
// crypto signature verify. The intake-verify cost is OFF the per-block
// budget because production runs it in the gap between slots.
func (bn *BenchNode) TxValidator() process.TxValidator { return bn.txValidator }

// txCacheID is the shard cache identifier shardedTxPool keys per-source
// pools by. Single-node bench always uses shard "0".
const txCacheID = "0"

// CommitState flushes the in-memory accounts journal into the trie. The
// production block processor calls this at the end of each block.
func (bn *BenchNode) CommitState() error {
	if err := bn.cacher.SaveAll(); err != nil {
		return err
	}
	if _, err := bn.accountsDB.Commit(); err != nil {
		return err
	}
	if _, err := bn.kappsDB.Commit(); err != nil {
		return err
	}
	if _, err := bn.peersDB.Commit(); err != nil {
		return err
	}
	return nil
}

// TxPreprocessor returns the real production preprocessor. The bench's
// per-slot loop calls .CreateAndProcessBlockTransactions(blk, haveTime)
// directly — that's the validator's actual block-production entry point.
func (bn *BenchNode) TxPreprocessor() benchTxPreprocessor { return bn.txPreprocessor }

// DataPool exposes the underlying pools holder so external code (intake
// + tests) can inspect the mempool.
func (bn *BenchNode) DataPool() retriever.PoolsHolder { return bn.dataPool }
