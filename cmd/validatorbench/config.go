package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"runtime"
	"time"
)

// TxMixEntry describes one transaction type in a configurable workload
// mix. Several entries can coexist; the generator picks among them in
// proportion to their Weight values, so a real-world mix like
// "80% transfers, 15% sc-call, 5% sc-deploy" is just three entries.
//
// Per-type fields are ignored when the type doesn't use them (e.g.
// ContractPath is meaningless for "transfer").
type TxMixEntry struct {
	Type        string   `json:"type"`            // transfer | sc-call
	Weight      int      `json:"weight"`          // relative probability
	ContractPath string  `json:"contract_path"`   // sc-call only
	InitArgs    []string `json:"init_args"`       // sc-call: deploy args
	Function    string   `json:"function"`        // sc-call only
	CallArgs    []string `json:"call_args"`       // sc-call only
	Instances   int      `json:"instances"`       // sc-call: distinct deployed copies (default 1)
	Value       int64    `json:"value"`           // transfer: amount per tx (default 1)
	Complexity  int      `json:"complexity"`      // sc-call: argument repetition multiplier
}

// Config drives every aspect of the validator throughput benchmark.
//
// All durations are stored as strings in the JSON form ("3s", "1m") so the
// example config file is human-friendly, but parsed into time.Duration once
// loaded.
type Config struct {
	// Workload selection. Two ways to express it:
	//
	//  1) Simple: set Workload + ContractPath + CallFunction + …
	//     (Original interface; still works for one-shot SC tests.)
	//
	//  2) Mix: populate TxMix with one or more TxMixEntry items. Each
	//     entry describes a tx type, contract, and weight. The generator
	//     picks among them weighted-randomly for every tx, so "80%
	//     transfer, 15% adder, 5% counter" is just three entries.
	//
	// When TxMix is non-empty it takes precedence; the simple fields are
	// then only used as defaults for any unset per-entry attributes.
	Workload string       `json:"workload"`
	TxMix    []TxMixEntry `json:"tx_mix"`

	// Simple-mode contract config. Used when TxMix is empty.
	ContractPath string   `json:"contract_path"`
	InitArgs     []string `json:"init_args"`
	CallFunction string   `json:"call_function"`
	CallArgsTpl  []string `json:"call_args"` // template args; supports "rand:N" tokens

	// Workload sizing
	NumTransactions int           `json:"num_transactions"` // 0 means use Duration only
	NumContracts    int           `json:"num_contracts"`    // distinct contract instances
	NumAccounts     int           `json:"num_accounts"`     // sender pool
	BlockSize       int           `json:"block_size"`       // tx per block
	Concurrency     int           `json:"concurrency"`      // generator + verifier workers
	Duration        time.Duration `json:"-"`                // populated from DurationStr
	DurationStr     string        `json:"duration"`

	// Contract complexity. Each call performs `complexity` increments,
	// approximating heavier contracts. With value 1 it is a single add.
	Complexity int `json:"contract_complexity"`

	// Hashing
	HashAlgo string `json:"hash_algorithm"` // sha256 | blake2b | keccak

	// Hardware SHA acceleration. "auto" leaves Go's defaults (HW path on
	// supported CPUs); "off" re-execs with GODEBUG to disable HW acceleration.
	SHAHardware string `json:"sha_hardware"` // auto | off

	// Test transaction shape
	TxDataPaddingBytes int    `json:"tx_data_padding_bytes"`
	GasLimit           uint64 `json:"gas_limit"`
	GasPrice           uint64 `json:"gas_price"`

	// Initial account funding (in KLV base units).
	InitialBalance int64 `json:"initial_balance"`

	// Output
	OutputDir   string `json:"output_dir"`
	JSONReport  bool   `json:"json_report"`
	CSVReport   bool   `json:"csv_report"`
	Verbose     bool   `json:"verbose"`
	ProgressSec int    `json:"progress_interval_seconds"`

	// Process control
	WarmupTransactions int `json:"warmup_transactions"`

	// CompareSHA, when true, runs the benchmark twice (HW on / HW off) and
	// produces a comparative summary. Implemented by re-executing the binary
	// with --internal-mode flags so that GODEBUG can be set before init().
	CompareSHA bool `json:"compare_sha"`

	// --- Block-budget mode -------------------------------------------------
	//
	// A real validator does not "go as fast as possible" — it produces one
	// block every BlockTime seconds and has at most BlockBudget of CPU time
	// to select transactions, run them, and ship the block to consensus.
	//
	// When BlockTime > 0 AND BlockBudget > 0, the processor switches to
	// budget mode: per slot it pulls from the (preferably overloaded)
	// mempool, processes txs sequentially until the per-block deadline is
	// nearly reached, finalises the block, then sleeps until the next slot.
	//
	// The headline metric in this mode is "max txs that fit in BlockBudget"
	// → effective_max_tps = avg_tx_per_block / BlockTime. That is the
	// number operators need when sizing capacity for SC-heavy chains where
	// 12k tx/block measured on transfers does NOT translate to SC traffic.
	BlockTime       time.Duration `json:"-"`
	BlockTimeStr    string        `json:"block_time"`
	BlockBudget     time.Duration `json:"-"`
	BlockBudgetStr  string        `json:"block_budget"`
	PrefillMempool  int           `json:"prefill_mempool"`

	// SaturateMode (--saturate) drops the slot clock + per-block budget
	// entirely. The runner just lets producer + processor run flat-out
	// for the configured Duration. Useful for sizing the host's raw
	// hardware ceiling (intake rate + execution rate) — NOT for the
	// chain-realistic ceiling under the live network's 4s/500ms timing
	// (which is the default mode).
	SaturateMode bool `json:"saturate_mode"`
}

// DefaultConfig returns a benchmark config tuned for a single-node validator
// simulation that runs in a few seconds end-to-end.
//
// These values err on the side of fitting on a developer laptop. Operators
// that want production-scale numbers should override via JSON config or CLI.
func DefaultConfig() Config {
	cpu := runtime.NumCPU()
	return Config{
		Workload:           "sc-call",
		ContractPath:       "testdata/adder.wasm",
		InitArgs:           []string{"5"},
		CallFunction:       "add",
		CallArgsTpl:        []string{"1"},
		NumTransactions:    0,
		NumContracts:       1,
		NumAccounts:        16,
		BlockSize:          200,
		Concurrency:        cpu,
		Duration:           10 * time.Second,
		DurationStr:        "10s",
		Complexity:         1,
		HashAlgo:           "blake2b",
		SHAHardware:        "auto",
		TxDataPaddingBytes: 0,
		GasLimit:           5_000_000,
		GasPrice:           0,
		InitialBalance:     1_000_000_000_000,
		OutputDir:          "./bench-results",
		JSONReport:         true,
		CSVReport:          true,
		Verbose:            false,
		ProgressSec:        2,
		WarmupTransactions: 100,
		CompareSHA:         false,
		// Klever mainnet slot interval is 4s for all transactions; the
		// validator has 500ms of CPU budget per block to ship to consensus.
		// These are the realistic defaults — operators wanting raw burst
		// numbers can disable budget mode by setting both to "0".
		BlockTime:          4 * time.Second,
		BlockTimeStr:       "4s",
		BlockBudget:        500 * time.Millisecond,
		BlockBudgetStr:     "500ms",
		PrefillMempool:     0,
	}
}

// LoadConfig reads a JSON config file and merges it on top of DefaultConfig.
// Missing keys keep their default value.
func LoadConfig(path string) (Config, error) {
	cfg := DefaultConfig()
	if path == "" {
		return cfg, nil
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return cfg, fmt.Errorf("read config %q: %w", path, err)
	}
	// Use a temporary map so we only override fields that are present in the
	// JSON. We unmarshal once into the existing struct, which is safe because
	// json.Unmarshal does not zero out fields whose keys are absent.
	if err := json.Unmarshal(data, &cfg); err != nil {
		return cfg, fmt.Errorf("parse config %q: %w", path, err)
	}
	if cfg.DurationStr != "" {
		d, err := time.ParseDuration(cfg.DurationStr)
		if err != nil {
			return cfg, fmt.Errorf("invalid duration %q: %w", cfg.DurationStr, err)
		}
		cfg.Duration = d
	}
	if cfg.BlockTimeStr != "" {
		d, err := time.ParseDuration(cfg.BlockTimeStr)
		if err != nil {
			return cfg, fmt.Errorf("invalid block_time %q: %w", cfg.BlockTimeStr, err)
		}
		cfg.BlockTime = d
	}
	if cfg.BlockBudgetStr != "" {
		d, err := time.ParseDuration(cfg.BlockBudgetStr)
		if err != nil {
			return cfg, fmt.Errorf("invalid block_budget %q: %w", cfg.BlockBudgetStr, err)
		}
		cfg.BlockBudget = d
	}
	return cfg, nil
}

// BudgetMode returns true when the processor should produce blocks on a
// fixed slot clock with a hard per-block CPU deadline.
func (c *Config) BudgetMode() bool {
	return c.BlockTime > 0 && c.BlockBudget > 0
}

// Validate sanity-checks the configuration after CLI flags have been merged.
// It clamps values that are easy to mistype and rejects ones that would make
// the benchmark meaningless.
func (c *Config) Validate() error {
	if c.Concurrency < 1 {
		c.Concurrency = 1
	}
	if c.BlockSize < 1 {
		return errors.New("block_size must be >= 1")
	}
	if c.NumAccounts < 1 {
		c.NumAccounts = 1
	}
	if c.NumContracts < 1 {
		c.NumContracts = 1
	}
	if c.Complexity < 1 {
		c.Complexity = 1
	}
	if c.NumTransactions == 0 && c.Duration <= 0 {
		return errors.New("either num_transactions > 0 or duration > 0 must be set")
	}
	switch c.HashAlgo {
	case "sha256", "blake2b", "keccak":
	default:
		return fmt.Errorf("unknown hash_algorithm %q (sha256|blake2b|keccak)", c.HashAlgo)
	}
	switch c.SHAHardware {
	case "auto", "off":
	default:
		return fmt.Errorf("unknown sha_hardware %q (auto|off)", c.SHAHardware)
	}
	if c.OutputDir != "" {
		if err := os.MkdirAll(c.OutputDir, 0o755); err != nil {
			return fmt.Errorf("create output_dir: %w", err)
		}
	}
	if c.BlockBudget > 0 && c.BlockTime > 0 && c.BlockBudget > c.BlockTime {
		return fmt.Errorf("block_budget (%s) must be <= block_time (%s)", c.BlockBudget, c.BlockTime)
	}
	return nil
}
