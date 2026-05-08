package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"runtime"
	"time"
)

// Config drives every aspect of the validator throughput benchmark.
//
// All durations are stored as strings in the JSON form ("3s", "1m") so the
// example config file is human-friendly, but parsed into time.Duration once
// loaded.
type Config struct {
	// Workload selection
	Workload string `json:"workload"`

	// Contract path and configuration. The contract is loaded once at start,
	// deployed by a funded "owner" account, and then the workload drives calls
	// against it just like a real validator would receive them via P2P.
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
	return cfg, nil
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
	return nil
}
