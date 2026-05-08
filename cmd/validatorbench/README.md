# Klever Validator Throughput Benchmark

`validatorbench` simulates a single-node validator processing a contract-heavy
workload end-to-end: signed transactions are produced concurrently, queued in
a mempool, batched into blocks, and executed through the **real** klever-go
WASM VM (`wasmer2`) against the **real** state trie. The goal is to measure
how many transactions per second and how many smart-contract calls per
second the host CPU can sustain under realistic load — and to surface the
specific phase that limits it.

## What it measures

| Metric                                | Source                                            |
|---------------------------------------|---------------------------------------------------|
| Transactions per second               | `tx_total / wall_time`                            |
| Smart-contract calls per second       | successful VM calls / wall_time                   |
| Block processing time (avg/peak)      | per-block timer in `Processor.processBlock`        |
| Hashing time as % of total            | wrapped `Hasher.Compute()` timer                  |
| CPU usage (user + sys)                | `getrusage(RUSAGE_SELF)`                          |
| Memory usage                          | `runtime.ReadMemStats()`                          |
| Latency per transaction               | wall-clock around `RunSmartContractCall`          |
| Failure / timeout rate                | VM `ReturnCode != Ok` divided by total            |

## How "real" it really is

* **Real VM:** `kvm/wasmer2` (CGO) + `kvm/scenarioexec.VMTestExecutor`. Same
  host, same gas schedule, same builtin functions a validator uses.
* **Real state:** every successful tx commits its `OutputAccounts` back to
  the in-memory accounts trie (`MockWorld`), including code-hash updates and
  storage writes.
* **Real signatures:** `crypto/ed25519` signing + verification on every tx.
* **Real hashing:** `crypto/hashing/{blake2b,keccak,sha256}`, selected at
  runtime, instrumented with byte/duration counters.
* **Real serialisation:** transactions are marshalled into bytes and re-fed
  through the hasher just like the P2P inbound path does.

## What it does NOT do

* Drive a libp2p socket — the cost is included only as marshal/hash work.
* Run consensus or BLS aggregation — those phases are validator-set sized,
  not single-node sized, and don't reflect the per-node CPU ceiling we care
  about here.

If you need network or consensus numbers, layer them on top: this tool gives
you the per-block CPU + IO + memory budget.

---

## Build

```bash
# from the repo root
make build-benchmark-throughput

# or directly
LD_LIBRARY_PATH=$(pwd)/kvm/wasmer2 \
  go build -o bin/validatorbench ./cmd/validatorbench
```

The `wasmer2` runtime is a shared library (`libvmexeccapi.so`); set
`LD_LIBRARY_PATH` (Linux) or `DYLD_LIBRARY_PATH` (macOS) when running.

## Quick start

```bash
# 30-second sc-call run with the bundled adder.wasm contract
LD_LIBRARY_PATH=$(pwd)/kvm/wasmer2 \
  ./bin/validatorbench \
    --duration 30s \
    --workload sc-call \
    --hash blake2b
```

### Run with a JSON config

```bash
./bin/validatorbench --config cmd/validatorbench/config.example.json
```

CLI flags override values from the JSON file, so `--duration 60s` on top of
the example config wins.

### Compare SHA hardware acceleration

`--compare-sha` runs the benchmark twice — once with the default Go hash
backend (which uses Intel SHA-NI / ARMv8-CE if present) and once with the
hardware path forcibly disabled via `GODEBUG`. The comparative summary
shows the throughput, hash-time, and CPU deltas.

```bash
./bin/validatorbench --compare-sha --duration 20s --hash sha256
```

The two child runs are spawned with `os.Executable()`, so the binary must be
on disk (running under `go run` won't reach the re-exec path).

### Disable HW SHA without compare mode

```bash
./bin/validatorbench --sha-hardware off --duration 20s --hash sha256
```

This re-execs the binary once with `GODEBUG=cpu.sha=off,...` so the runtime
disables the SHA-NI fast path before `crypto/sha256` is initialised.

---

## CLI flags

```
--config string         path to JSON config file (CLI flags override config values)
--workload string       workload name (sc-call|transfer|mixed)
--contract string       path to .wasm contract file
--call string           contract function to invoke
--tx int                total number of transactions to send (0 = use --duration)
--contracts int         number of contract instances to deploy
--accounts int          size of the funded sender pool
--block-size int        transactions per block
--concurrency int       concurrent generators / verifiers (default: NumCPU)
--duration duration     test duration (e.g. 30s, 5m)
--hash string           hash algorithm (sha256|blake2b|keccak)
--sha-hardware string   auto|off (off re-execs with GODEBUG to disable HW SHA)
--output-dir string     directory for JSON/CSV reports
--complexity int        contract complexity multiplier (calls per tx)
--gas-limit uint64      gas limit per transaction
--gas-price uint64      gas price per gas unit
--tx-padding int        extra bytes appended to each tx data field
--warmup int            warm-up transactions (0 to disable)
--verbose               enable verbose internal logging
--json                  force-enable JSON report
--csv                   force-enable CSV report
--compare-sha           run twice (HW SHA on/off) and emit a comparative summary
--version               print version and exit
```

## Configuration reference

See `config.example.json` for an annotated example. Fields:

| Field                       | Type        | Notes                                              |
|-----------------------------|-------------|----------------------------------------------------|
| `workload`                  | string      | `sc-call`, `transfer`, `mixed`                      |
| `contract_path`             | string      | path to `.wasm` file (relative to cwd)              |
| `init_args`                 | []string    | constructor args (decimal / hex `0x..` / raw)       |
| `call_function`             | string      | exported function called for `sc-call` workload     |
| `call_args`                 | []string    | per-call args; `rand:N` produces N fresh bytes      |
| `num_transactions`          | int         | tx budget; 0 means "use duration only"              |
| `num_contracts`             | int         | distinct contract instances deployed                |
| `num_accounts`              | int         | funded sender pool                                   |
| `block_size`                | int         | tx per block                                        |
| `concurrency`               | int         | generator + verifier workers (0 = NumCPU)           |
| `duration`                  | string      | Go duration (`30s`, `5m`)                            |
| `contract_complexity`       | int         | multiplies the per-tx argument repetition           |
| `hash_algorithm`            | string      | `sha256`, `blake2b`, `keccak`                       |
| `sha_hardware`              | string      | `auto` or `off`                                     |
| `tx_data_padding_bytes`     | int         | bytes of pseudo-random padding per tx                |
| `gas_limit`, `gas_price`    | uint64      | per-tx                                              |
| `initial_balance`           | int64       | KLV base units credited to every account            |
| `output_dir`                | string      | created if missing                                  |
| `json_report`/`csv_report`  | bool        | enable each report file                             |
| `progress_interval_seconds` | int         | stderr ticker (0 disables)                           |
| `warmup_transactions`       | int         | un-timed pre-roll                                   |
| `compare_sha`               | bool        | runs HW-on then HW-off back-to-back                  |

## Output

`validatorbench` always prints a human-readable summary to **stdout**.
When `--json` and/or `--csv` are enabled it also writes
`report-YYYYMMDD-HHMMSS.json` / `.csv` into `--output-dir`.

The JSON file contains the full `Report` struct, including the merged
config and the system info, so it's safe to commit alongside performance
regression baselines.

A typical text summary looks like the [sample-output.txt](./sample-output.txt)
in this directory.

---

## Adding a new workload

`Workload` is a one-method interface:

```go
type Workload interface {
    Name() string
    Next(workerID int) *Tx
}
```

1. Implement it in a new `.go` file under this directory.
2. Register a constructor in an `init()` block:

   ```go
   func init() {
       RegisterWorkload("my-workload", newMyWorkload)
   }
   ```

3. Reference it via `--workload my-workload` or `"workload": "my-workload"`
   in JSON.

The constructor receives the parsed `Config`, the prepared `VMEnv` (so it
can pick senders / contract addresses), and the `TxBuilder` (for signing).
Return any non-nil error to abort the run before timing starts.

## How the pieces fit

```
generators ──push──▶ Mempool ──drain──▶ Processor ──per-block──┐
   ▲                                       │                   │
   └── workload.Next(workerID)             │  parallel sig verify
                                           │  sequential VM exec
                                           │  block finalize hash
                                           ▼
                                       Metrics  ──▶ Report (JSON/CSV/text)
```

* `cmd/validatorbench/vmenv.go` — bootstraps `MockWorld`, funds accounts,
  deploys contracts.
* `cmd/validatorbench/processor.go` — the only goroutine that touches
  the VM (deterministic, mirrors validator behaviour).
* `cmd/validatorbench/metrics.go` — atomic counters + reservoir latency
  histogram + CPU/mem snapshots at end.
* `cmd/validatorbench/hashing.go` — wraps the chosen `klever-go` hasher
  to time every `Compute()` call.

## Caveats

* Signature **verification** runs inside `processBlock` so the cost is
  attributed to block time. If you split that into a dedicated mempool
  pre-screen, copy the timer block.
* `MockWorld` keeps storage in RAM; on production validators the same
  state lives in LevelDB. Real disk pressure adds ~10-30% overhead on
  hot SC calls. Use `cmd/benchmark` (the existing host benchmark) for
  the disk side.
* The `transfer` workload still calls into the VM (with a noop function)
  so its TPS reflects "validation pipeline overhead" rather than a
  pure value-transfer ceiling.
