package main

import (
	"flag"
	"fmt"
	"os"
	"time"
)

// cliOverrides captures the subset of Config that may be overridden via
// command-line flags. Zero/empty values mean "no override — keep what
// the JSON config (or DefaultConfig) provided".
type cliOverrides struct {
	Workload     string
	Contract     string
	CallFn       string
	NumTx        int
	NumContracts int
	NumAccounts  int
	BlockSize    int
	Concurrency  int
	Duration     time.Duration
	HashAlgo     string
	SHAHardware  string
	OutputDir    string
	Complexity   int
	GasLimit     uint64
	GasPrice     uint64
	TxPadding    int
	WarmupTx     int
	Verbose      bool
	JSON         bool
	CSV          bool
	CompareSHA   bool

	BlockTime    time.Duration
	BlockBudget  time.Duration
	Prefill      int
	Saturate     bool

	// Tracking flags that were actually set so we can distinguish
	// between "user said 0" and "user didn't say".
	setMask map[string]bool
}

// parseFlags parses command-line flags and returns:
//
//	overrides:    fields the user explicitly set (will overwrite cfg)
//	configPath:   path to a JSON config file (or "" if none)
//	showVersion:  whether the user asked for --version
func parseFlags() (cliOverrides, string, bool) {
	o := cliOverrides{setMask: map[string]bool{}}
	var configPath string
	var version bool

	fs := flag.NewFlagSet("validatorbench", flag.ExitOnError)
	fs.StringVar(&configPath, "config", "", "path to JSON config file (CLI flags override config values)")
	fs.StringVar(&o.Workload, "workload", "", "workload name (sc-call|transfer|mixed)")
	fs.StringVar(&o.Contract, "contract", "", "path to .wasm contract file")
	fs.StringVar(&o.CallFn, "call", "", "contract function to invoke")
	fs.IntVar(&o.NumTx, "tx", 0, "total number of transactions to send (0 = use --duration)")
	fs.IntVar(&o.NumContracts, "contracts", 0, "number of contract instances to deploy")
	fs.IntVar(&o.NumAccounts, "accounts", 0, "size of the funded sender pool")
	fs.IntVar(&o.BlockSize, "block-size", 0, "transactions per block")
	fs.IntVar(&o.Concurrency, "concurrency", 0, "concurrent generators / verifiers (default: NumCPU)")
	fs.DurationVar(&o.Duration, "duration", 0, "test duration (e.g. 30s, 5m)")
	fs.StringVar(&o.HashAlgo, "hash", "", "hash algorithm (sha256|blake2b|keccak)")
	fs.StringVar(&o.SHAHardware, "sha-hardware", "", "auto|off (off re-execs with GODEBUG to disable HW SHA)")
	fs.StringVar(&o.OutputDir, "output-dir", "", "directory for JSON/CSV reports")
	fs.IntVar(&o.Complexity, "complexity", 0, "contract complexity multiplier (calls per tx)")
	fs.Uint64Var(&o.GasLimit, "gas-limit", 0, "gas limit per transaction")
	fs.Uint64Var(&o.GasPrice, "gas-price", 0, "gas price per gas unit")
	fs.IntVar(&o.TxPadding, "tx-padding", -1, "extra bytes appended to each tx data field")
	fs.IntVar(&o.WarmupTx, "warmup", -1, "warm-up transactions (0 to disable)")
	fs.BoolVar(&o.Verbose, "verbose", false, "enable verbose internal logging")
	fs.BoolVar(&o.JSON, "json", false, "force-enable JSON report")
	fs.BoolVar(&o.CSV, "csv", false, "force-enable CSV report")
	fs.BoolVar(&o.CompareSHA, "compare-sha", false, "run twice (HW SHA on/off) and emit a comparative summary")
	fs.DurationVar(&o.BlockTime, "block-time", 0, "chain slot interval (e.g. 3s); enables budget mode together with --block-budget")
	fs.DurationVar(&o.BlockBudget, "block-budget", 0, "max processing time per block (e.g. 500ms); the headline EFFECTIVE MAX TPS comes from this")
	fs.IntVar(&o.Prefill, "prefill", 0, "transactions to pre-generate into the mempool before timing starts (recommended in budget mode)")
	fs.BoolVar(&o.Saturate, "saturate", false, "saturate mode: drop slot clock + per-block budget; measure raw hardware ceiling")
	fs.BoolVar(&version, "version", false, "print version and exit")

	fs.Usage = func() {
		fmt.Fprintln(fs.Output(), "klever-validatorbench — validator-node throughput benchmark")
		fmt.Fprintln(fs.Output(), "")
		fmt.Fprintln(fs.Output(), "Usage:")
		fmt.Fprintln(fs.Output(), "  validatorbench [--config FILE] [flags]")
		fmt.Fprintln(fs.Output(), "")
		fmt.Fprintln(fs.Output(), "Flags:")
		fs.PrintDefaults()
		fmt.Fprintln(fs.Output(), "")
		fmt.Fprintln(fs.Output(), "Examples:")
		fmt.Fprintln(fs.Output(), "  validatorbench --duration 30s --workload sc-call")
		fmt.Fprintln(fs.Output(), "  validatorbench --config bench.json --tx 100000 --hash blake2b")
		fmt.Fprintln(fs.Output(), "  validatorbench --compare-sha --duration 20s")
	}

	_ = fs.Parse(os.Args[1:])

	// Track which flags the user actually set so zero values don't get
	// treated as overrides.
	fs.Visit(func(f *flag.Flag) { o.setMask[f.Name] = true })

	return o, configPath, version
}

// mergeCLIIntoConfig overlays user-supplied flags on top of the loaded
// config. Only flags that were explicitly set (per setMask) win — that
// way `--block-size 0` from the command line means "I really mean 0",
// and an unset flag leaves the config's value alone.
func mergeCLIIntoConfig(cfg *Config, o cliOverrides) {
	if o.setMask["workload"] {
		cfg.Workload = o.Workload
	}
	if o.setMask["contract"] {
		cfg.ContractPath = o.Contract
	}
	if o.setMask["call"] {
		cfg.CallFunction = o.CallFn
	}
	if o.setMask["tx"] {
		cfg.NumTransactions = o.NumTx
	}
	if o.setMask["contracts"] {
		cfg.NumContracts = o.NumContracts
	}
	if o.setMask["accounts"] {
		cfg.NumAccounts = o.NumAccounts
	}
	if o.setMask["block-size"] {
		cfg.BlockSize = o.BlockSize
	}
	if o.setMask["concurrency"] {
		cfg.Concurrency = o.Concurrency
	}
	if o.setMask["duration"] {
		cfg.Duration = o.Duration
		// Keep DurationStr in sync so the JSON form of Config (which is
		// embedded in every report) reflects the override.
		cfg.DurationStr = o.Duration.String()
	}
	if o.setMask["hash"] {
		cfg.HashAlgo = o.HashAlgo
	}
	if o.setMask["sha-hardware"] {
		cfg.SHAHardware = o.SHAHardware
	}
	if o.setMask["output-dir"] {
		cfg.OutputDir = o.OutputDir
	}
	if o.setMask["complexity"] {
		cfg.Complexity = o.Complexity
	}
	if o.setMask["gas-limit"] {
		cfg.GasLimit = o.GasLimit
	}
	if o.setMask["gas-price"] {
		cfg.GasPrice = o.GasPrice
	}
	if o.setMask["tx-padding"] {
		cfg.TxDataPaddingBytes = o.TxPadding
	}
	if o.setMask["warmup"] {
		cfg.WarmupTransactions = o.WarmupTx
	}
	if o.setMask["verbose"] {
		cfg.Verbose = o.Verbose
	}
	if o.setMask["json"] {
		cfg.JSONReport = o.JSON
	}
	if o.setMask["csv"] {
		cfg.CSVReport = o.CSV
	}
	if o.setMask["compare-sha"] {
		cfg.CompareSHA = o.CompareSHA
	}
	if o.setMask["block-time"] {
		cfg.BlockTime = o.BlockTime
		cfg.BlockTimeStr = o.BlockTime.String()
	}
	if o.setMask["block-budget"] {
		cfg.BlockBudget = o.BlockBudget
		cfg.BlockBudgetStr = o.BlockBudget.String()
	}
	if o.setMask["prefill"] {
		cfg.PrefillMempool = o.Prefill
	}
	if o.setMask["saturate"] {
		cfg.SaturateMode = o.Saturate
	}
}
