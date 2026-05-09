# Klever Validator Throughput Benchmark

`validatorbench` measures how many transactions per second and how many
smart-contract calls per second the production klever-go validator code
path can sustain on a given host. **It does not re-implement any part
of the validator pipeline — it wires up the real components and drives
them.** When the validator team changes anything in `txProcessor`,
`scProcessor`, or `preprocess.transactions`, this benchmark's numbers
track the change automatically.

## How it works (production-component reuse)

A single `BenchNode` (`benchnode.go`) wires up the same components a
real klever-go validator runs at startup, with consensus / BLS /
messenger / slot-manager / nodes-coordinator stripped out:

| Production component                           | Used as-is in the bench? |
|------------------------------------------------|--------------------------|
| `data/retriever/txpool.shardedTxPool`          | yes (production mempool) |
| `data/state.AccountsDB` / `AccountsCacher`     | yes (production state)   |
| `core/process/economics.EconomicsData`         | yes                      |
| `core/process/block/postprocess.NewFeeAccumulator` | yes                  |
| `eventNotifier/notifier.GasScheduleNotifier`   | yes (in-memory v1)       |
| `core/kapp/kappController.NewKappController`   | yes                      |
| `core/process/smartContract/builtInFunctions`  | yes (real container)     |
| `core/process/smartContract/hooks.NewBlockChainHookImpl` | yes            |
| `core/process/factory/chain.NewVMContainerFactory` (wasmer2) | yes        |
| `core/process/smartContract.NewSmartContractProcessor` | yes (no mock)    |
| `core/process/transaction.NewTxProcessor`      | yes                      |
| `core/process/block/preprocess.NewTransactionPreprocessor` | yes          |

A `BenchRunner` (`runner.go`) drives the slot clock + mempool intake +
per-slot calls into the real preprocessor:

```
generator (BuildSignedTransfer / BuildSignedSCCall via BenchNode)
   -> dataValidators.txValidator.CheckTxValidity     [PRODUCTION INTAKE: nonce
                                                      window + ed25519 verify,
                                                      OFF the per-block budget,
                                                      runs in the 3.5s gap
                                                      between slots]
   -> shardedTxPool.AddData                         [production mempool]
   -> preprocess.transactions
        .CreateAndProcessBlockTransactions(blk, haveTime)
                                                    [PRODUCTION BLOCK PRODUCTION,
                                                     ON the per-block budget]
        -> transaction.txProcessor.ProcessTransaction
        -> smartContract.scProcessor.ExecuteSmartContractTransaction
        -> wasmer2 VM container
   -> AccountsCacher commit
```

`haveTime` is the per-block CPU budget (klever mainnet: 500 ms). The
slot clock is the chain block interval (klever mainnet: 4 s). Both are
configurable.

The bench reports intake (off-budget) and execution (on-budget) wall
times separately — that's the only honest way to compare against the
"3.5 s window between slots" reasoning operators use.

## What it measures

| Metric                                | Source                                              |
|---------------------------------------|-----------------------------------------------------|
| **Effective max TPS**                 | `avg_tx_per_slot / slot_interval`                   |
| Tx per block (avg/min/max)            | per-slot count from `preprocessor` return            |
| Budget used (avg/max)                 | wall time of preprocessor call ÷ configured budget   |
| Slot count (total/empty)              | filled vs starved slots                              |
| Block processing time (avg/peak)      | wall time of `CreateAndProcessBlockTransactions`     |
| Intake verify (off-budget)            | wall time of generator's tx-build path               |
| Bottleneck classification             | budget-saturated vs starved vs balanced              |

## Build

```bash
make build-benchmark-throughput
# or
LD_LIBRARY_PATH=$(pwd)/kvm/wasmer2 \
  go build -o bin/validatorbench ./cmd/validatorbench
```

## Run modes

The bench has three modes; they all share the same production
pipeline (intake → shardedTxPool → preprocessor → txProcessor →
state). What differs is the termination criterion and load shape.

### 1. Chain-realistic (default — `--duration T`)

Slot clock ticks every `--block-time` (default 4 s). Each slot the
preprocessor has `--block-budget` (default 500 ms) of CPU. Background
producers run continuously through the production intake path. Use
this to measure **the chain-realistic ceiling** — what consensus
actually sees on a saturated chain.

```bash
LD_LIBRARY_PATH=$(pwd)/kvm/wasmer2 \
  ./bin/validatorbench \
    --workload transfer \
    --duration 30s \
    --prefill 30000 \
    --concurrency 4 \
    --accounts 1024
```

Headline metric: `EFFECTIVE MAX TPS = avg_tx_per_slot ÷ slot_interval`.

### 2. Bounded (`--tx N`)

Synchronously inject exactly N transactions through the production
intake path, then run slots until the preprocessor has drained them.
Use this to measure **how long this hardware takes to clear N pending
txs at production timing**.

```bash
LD_LIBRARY_PATH=$(pwd)/kvm/wasmer2 \
  ./bin/validatorbench \
    --workload transfer \
    --tx 12000 \
    --duration 30s \
    --concurrency 4 \
    --accounts 1024
```

Headline metric: total wall time + slot count to drain the batch.

### 3. Saturate (`--saturate`)

Drop the slot clock + per-block budget entirely. Run for `--duration`
with producer + processor flat-out. Use this to measure **the host's
raw hardware ceiling** (intake rate + execution rate, no chain
timing). NOT chain-realistic — it answers "how much can this CPU
do without slot/budget constraints", not "what TPS will the validator
ship to consensus".

```bash
LD_LIBRARY_PATH=$(pwd)/kvm/wasmer2 \
  ./bin/validatorbench \
    --workload transfer \
    --saturate \
    --duration 30s \
    --prefill 12000 \
    --concurrency 4 \
    --accounts 1024
```

Headline metric: total tx ÷ wall time = raw tx/s ceiling.

### SC workloads

All three modes work with the SC workload too — pass
`--workload sc-call --contract <path.wasm> --call <fn>`:

```bash
LD_LIBRARY_PATH=$(pwd)/kvm/wasmer2 \
  ./bin/validatorbench \
    --workload sc-call \
    --contract ./cmd/validatorbench/testdata/adder.wasm \
    --call add \
    --duration 30s \
    --prefill 5000 \
    --concurrency 4 \
    --accounts 200 \
    --contracts 2 \
    --gas-limit 1500000
```

The bench prints a text summary to stdout and writes JSON + CSV to
`--output-dir` (default `./bench-results`).

## Configuration

The default block timing matches klever mainnet:

| Field          | Default | Note                                  |
|----------------|---------|---------------------------------------|
| `block_time`   | `4s`    | slot interval                          |
| `block_budget` | `500ms` | max CPU time per block                 |

Override via `--block-time` / `--block-budget` for other chain configs.
Setting both to `0` runs as-fast-as-possible (raw burst mode, not
chain-realistic).

## Configurable transactions and contracts

The simple-mode flags select between two workloads:

- `--workload transfer` — KLV transfers between accounts (no VM)
- `--workload sc-call` — invoke `--call` on `--contracts` instances of
  the `--contract` wasm file

Both workloads build real `*data/transaction.Transaction` protobufs,
sign them with ed25519, push them through the production
`shardedTxPool.AddData` path, and let the production preprocessor
select + execute them per slot.

For SC workloads:
- `--contracts N` deploys N independent instances of the wasm at startup
  (each deploy is its own block through the production preprocessor)
- `--call FN --gas-limit G` invokes function `FN` with gas budget `G`
- `--init-args` / `--call-args` pass arguments (decimal → bigint, hex
  with `0x` prefix → raw bytes)

## Sample output

See [sample-output.txt](./sample-output.txt) for a captured run.

Test-VM numbers (4 CPUs, in-memory state, blake2b):

| Workload   | Tx/block (avg) | Budget used | Effective TPS |
|------------|----------------|-------------|---------------|
| transfer   | ~3 700         | 100 %       | ~925          |
| sc-call (adder, gasLimit=1.5M) | ~945  | 53 %  | ~236          |

Mainnet hardware (more CPUs, NVMe-backed LevelDB, larger gas budget per
block) will run faster — these are reference numbers from a 4-CPU VM
with in-memory state.

## Constraints + notes

- **Budget over-run**: my deadline check happens between txs. A single
  outlier tx that takes 50+ ms can push a block past the 500 ms
  budget. On a real validator that means the slot is missed; the bench
  surfaces it as `Budget used: max > 100%`.

- **SC gas budget cap**: the production preprocessor admits txs into a
  block while their declared `gasLimit` total stays under
  `MaxGasPerBlock` (default 1.5 B). With `--gas-limit 5000000` you cap
  at 300 SC calls per block by gas alone; lower `--gas-limit` to fit
  more.

- **No bench-side metrics for hashing / latency / CPU**: those came
  from the legacy parallel flow. The production preprocessor does not
  expose per-tx instrumentation hooks; numbers in those Report fields
  read 0. The text summary section "Phase breakdown" still shows the
  exec time (the real preprocessor wall time) and intake verify (the
  generator's sig-build cost), which is what matters for sizing.

- **Genesis-mode SC processor**: the production scProcessor refuses
  direct deploys outside genesis (real chain deploys go through the
  proposal mechanism). The bench runs the SC processor in genesis
  mode end-to-end so it can deploy contracts on demand. This does
  not affect transfer or invoke behavior.

## Open question for a separate PR: the on-budget signature verify

The `BenchmarkProdTransferBatch` profiling work found a cost the
benchmark can't fix from this side without diverging from production:

  Production code path verifies every tx's ed25519 signature TWICE:

    1. At mempool intake (off-budget, in the 3.5s window between slots):
       `core/process/dataValidators/txValidator.go:218`
         txv.singleSigner.Verify(pub, hash, sig)

    2. Again at execution time (on-budget, inside the 500ms slot budget):
       `core/process/transaction/baseProcess.go:129`
         txProc.singleSigner.Verify(pub, txHash, signature)
       called via PreProcessTransaction -> checkTxValues -> validatePermission
       -> verifySignatures, on EVERY tx the preprocessor admits to the block.

  CPU profile of a 12000-tx block on the test VM attributes ~38% of
  the on-budget cost to that second verify. Skipping it (e.g. with a
  "verified at intake" flag set by the interceptor and trusted by the
  txProcessor) would drop per-tx cost from ~125 µs to ~77 µs on this
  hardware — fitting ~6500 transfers per 500 ms slot instead of ~4000.

  **The bench deliberately does not work around this.** The whole point
  of moving the bench onto the production pipeline (BenchNode +
  preprocess.transactions + txProcessor) was so the bench numbers
  track whatever the production code actually does. If we add a
  skip-verify flag here, we'd be measuring something the live
  validator doesn't do.

  Tracked as a follow-up: a separate PR against the validator's
  txProcessor + interceptor to thread the "already verified" signal
  through. Once that lands, this benchmark will automatically reflect
  the speed-up — no changes needed in cmd/validatorbench/.

