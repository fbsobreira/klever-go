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
	"fmt"
	"os"
	syncGo "sync"

	"github.com/klever-io/klever-go/common"
	commonMock "github.com/klever-io/klever-go/common/mock"
	"github.com/klever-io/klever-go/config"
	"github.com/klever-io/klever-go/core"
	"github.com/klever-io/klever-go/core/fork"
	"github.com/klever-io/klever-go/core/kapp"
	kappcontroller "github.com/klever-io/klever-go/core/kapp/kappController"
	"github.com/klever-io/klever-go/core/process"
	"github.com/klever-io/klever-go/core/process/block/postprocess"
	"github.com/klever-io/klever-go/core/process/economics"
	"github.com/klever-io/klever-go/core/process/factory/chain"
	"github.com/klever-io/klever-go/core/process/rating"
	"github.com/klever-io/klever-go/core/process/smartContract"
	"github.com/klever-io/klever-go/core/process/smartContract/builtInFunctions"
	"github.com/klever-io/klever-go/core/process/smartContract/hooks"
	"github.com/klever-io/klever-go/core/process/smartContract/hooks/counters"
	"github.com/klever-io/klever-go/core/process/block/preprocess"
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
	"github.com/klever-io/klever-go/data/trie"
	eventMock "github.com/klever-io/klever-go/eventNotifier/mock"
	"github.com/klever-io/klever-go/kapps"
	gasSchedules "github.com/klever-io/klever-go/kvm/scenarioexec/gasSchedules"
	"github.com/klever-io/klever-go/storage"
	"github.com/klever-io/klever-go/storage/memorydb"
	"github.com/klever-io/klever-go/storage/storageUnit"
	"github.com/klever-io/klever-go/storage/txcache"
	"github.com/klever-io/klever-go/tools/marshal"
	"github.com/klever-io/klever-go/tools/typeConverters/uint64ByteSlice"
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

	// owner used for SC deploys
	owner *BenchAccount
}

// benchTxPreprocessor exposes the subset of preprocess.transactions's
// public surface the bench actually uses. preprocess.NewTransactionPreprocessor
// returns an unexported *transactions, so we accept it through an
// interface that matches its public methods.
type benchTxPreprocessor interface {
	CreateAndProcessBlockTransactions(blk *block.Block, haveTime func() bool) (data.ProcessResults, error)
	CreateBlockStarted()
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
		IsGenesisProcessing: false,
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
