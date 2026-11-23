# ✔️ Running Tests

## Test Levels

Each `make` target belongs to one test level. The levels are:

- **Unit**: one package, with its own dependencies in memory or in a test container.
- **Integration**: several components together, without a full running node.
- **System (end to end)**: one or more complete Teranode nodes, started in process or in Docker.
- **Acceptance**: checks against the functional and non-functional requirements.

| Level | Target or command | What it runs |
|---|---|---|
| Unit | `make test` | Every package except `./test/`, with `-race` and coverage. This also replays the committed seed corpus of each fuzz target. |
| Unit | `make test-heavy` | The `TestSortedHashSet_` tests in `services/blockassembly/subtreeprocessor`, without `-race`. |
| Unit | `make fuzz` | Fuzzes each target in `FUZZ_TARGETS` for `FUZZTIME`. |
| Integration | `make longtest` | `TestLegacyHistoricalTestnetSync` (`longtest` build tag), `make test-heavy`, and `test/longtest/`. |
| System | `make smoketest` | `test/e2e/daemon/ready/` with in-process daemons, except the tests in its `-skip` list. That list holds the tests that `make prunertest` and `make legacy-sync` run, and two other tests. |
| System | `make sequentialtest` (and the `-sqlite`, `-postgres`, `-aerospike` and `-shard` variants) | `test/sequentialtest/`, one test at a time, with in-process daemons. |
| System | `make prunertest` | `TestBlobDeletion` and `TestPruner` in `test/e2e/daemon/ready/`. |
| System | `make legacy-sync` | The legacy-sync tests in `test/e2e/daemon/ready/`, which run Teranode with SV Node. |
| System | `make chainintegrity` | `test/e2e/chainintegrity/`, a multi-node chain check. |
| System | `make network-chaos-test`, `make network-chaos-test-split` | `test/multinode/` and `test/multinode_split/` (`network_chaos` build tag) against a Docker multi-node stack. |
| System | `make soaktest` | `TestSoakSteadyLoad` in `test/soak/` (`soak` build tag). |
| System | `go test ./test/chaos -run TestScenarioNN` | The chaos scenarios in `test/chaos/`. No `make` target runs them. |
| Acceptance | `go test -tags test_tna ./test/tna/`, `go test -tags test_tec ./test/tec/`, `go test ./test/tnb/ ./test/tnd/ ./test/tnf/ ./test/tnj/` | The functional requirement categories. See [Automated Testing](automatedTestingHowTo.md). |
| Acceptance | The [node-validation](https://github.com/bsv-blockchain/node-validation) harness | Consensus parity with SV Node and the requirement checks of the test plan. It is a separate repository. |

`make testall` runs `make test`, `make longtest` and `make sequentialtest`.

Gaps in the programme:

- No `make` target and no CI workflow runs the acceptance categories in `test/tna/`, `test/tec/`, `test/tnb/`, `test/tnd/`, `test/tnf/` and `test/tnj/`.
- `make test` excludes `./test/`, and no target or workflow runs the tests in `test/consensus/`, `test/fsm/` or `test/rpc/`. `make bench-test` lists `test/consensus/`, but it runs benchmarks only (`-run='^$'`). On 2026-10-17, `test/consensus/` passed, and `test/fsm/` and `test/rpc/withDaemon/` failed.

## When CI Runs Each Suite

| When | Workflow | Suites |
|---|---|---|
| Every pull request | `teranode_pr_tests.yaml` | `golangci-lint`, `make test`, `make test-heavy`, and the `stores/utxo/aerospike` tests against an Aerospike server with native operations |
| Every pull request that changes files outside `docs/` | `teranode_pr_smoketests.yaml` | `make smoketest` (3 shards), `make prunertest`, `make legacy-sync`, `make chainintegrity`, and `make sequentialtest-shard` (7 shards) |
| Every pull request that changes `fuzz.yaml` or the `Makefile` | `fuzz.yaml` | Each fuzz target for 20 seconds |
| Every push to `main`, `staging` or `release/*`, and every `v*` tag | `teranode_main_build.yaml`, which calls `teranode_main_tests.yaml` | `make test`, `make smoketest` and `make prunertest` |
| Every day at 02:00 UTC | `nightly_tests.yaml` | A three-node chain-integrity run, the `test/e2e/daemon/ready/` tests, the chaos scenarios, and `make soaktest` for two hours |
| Every day at 04:00 UTC | `fuzz.yaml` | Each fuzz target for 5 minutes |
| Manual dispatch only | `long_tests.yaml` | `make testall` |

## Unit Tests

```shell
make test  # Executes unit tests, excluding the test/ directory.
```

## All Test Suites

```shell
make testall  # Runs all test suites: unit tests (make test), long-running tests (make longtest), and sequential tests (make sequentialtest).
```

## Long-Running Tests

```shell
make longtest  # Executes long-running tests in test/longtest/ with a 10-minute timeout.
```

## Soak Test

The soak test runs the in-process daemon under steady transaction load for a long wall-clock duration. It fails if heap or goroutine counts trend upward, which is how slow leaks and unbounded caches show up. It is behind the `soak` build tag, so no other suite runs it. The nightly workflow runs it for two hours.

- **Services.** It runs propagation, validator, block assembly, block and subtree validation, blockchain, block persister and pruner. The pruner keeps the UTXO store bounded by `global_blockHeightRetention` (288 by default). It is triggered by the block persister. The test fails if the pruner deleted no records.
- **Sampling.** It samples `runtime.ReadMemStats` (after a forced GC) and `runtime.NumGoroutine()` at a fixed interval.
- **Warm-up.** Samples before the warm-up are discarded. The warm-up ends at the later of `SOAK_WARMUP` and the chain reaching retention blocks past the first spend. Retention-bounded state is still filling until then, and that ramp is counted in blocks, not minutes.
- **Failure rule.** A metric fails only when both its least-squares growth across the window and its last-quarter-minus-first-quarter mean exceed the allowed growth. The allowed growth is `max(tolerance × first-quarter mean, floor)`.

```shell
make soaktest                                          # 30 minute run (default)
make soaktest SOAK_DURATION=2h SOAK_TIMEOUT=150m       # nightly length; SOAK_TIMEOUT must exceed SOAK_DURATION by 10m+
# Quick check that a leak fails it: a low retention lets the chain reach steady state in under a minute.
global_blockHeightRetention=20 SOAK_INJECT_LEAK=1 SOAK_WARMUP=1m SOAK_SAMPLE_INTERVAL=10s make soaktest SOAK_DURATION=5m SOAK_TIMEOUT=20m
```

| Variable | Default | Meaning |
|---|---|---|
| `SOAK_DURATION` | `30m` | How long to drive load |
| `SOAK_WARMUP` | larger of 5m and 20% of the duration, at most half of it | Minimum warm-up; the run also waits for the steady-state block height |
| `SOAK_SAMPLE_INTERVAL` | `30s` | Time between samples |
| `SOAK_TXS_PER_CYCLE` | `500` | Transactions sent and mined per cycle |
| `SOAK_UTXO_STORE` | sqlite | `aerospike` or `postgres` run the store in a container, so its memory is not measured |
| `SOAK_HEAP_TOLERANCE` / `SOAK_HEAP_TOLERANCE_MIB` | `0.10` / `16` | Allowed heap growth: fraction of the first-quarter mean, and its floor in MiB |
| `SOAK_GOROUTINE_TOLERANCE` / `SOAK_GOROUTINE_TOLERANCE_ABS` | `0.05` / `20` | Allowed goroutine growth: fraction of the first-quarter mean, and its floor |
| `SOAK_OUTPUT_DIR` | a temp dir | Where `soak-samples.csv`, the end-of-warm-up `soak-heap-baseline.pprof` and, on failure, heap and goroutine profiles are written |
| `SOAK_INJECT_LEAK` | `false` | Retain 1 MiB and one goroutine per cycle, to check that the test fails |

### Sensitivity

A steady leak at rate `r` over an analysis window `w` raises the last-quarter mean over the first by about `0.75 × r × w`. That is the binding condition, so the slowest leak a run fails is `allowed / (0.75 × w)`. Every run logs this as `min_detectable` per metric.

The figures below assume a heap of about 285 MiB and about 690 goroutines, the levels the soak daemon settles at. In both cases a ~285 MiB heap makes 10% (28.5 MiB) the binding allowance, above the 16 MiB floor.

| Run | Analysis window | Heap leak caught | Goroutine leak caught |
|---|---|---|---|
| `make soaktest` (30m) | about 20m (warm-up ends at height 292, about 10m in) | about 114 MiB/h | about 138/h (5% / 20) |
| Nightly (2h) | 96m (24m warm-up) | about 24 MiB/h | about 12/h (2% / 12) |

The nightly job sets the tighter goroutine tolerance. With the default it would pass a leak of up to about 29 goroutines an hour, such as one goroutine per 100 blocks. A slower leak than the figures above passes. For a leak per transaction: the soak sends about 240 transactions a second (about 860,000 an hour). The nightly run therefore catches retention of about 30 bytes or more per transaction, and the 30-minute run about 140 bytes or more. Any retained transaction or hash structure is well above both.

Only the Go heap is measured. Memory held outside it, such as C allocations or containerised stores, is not. The UTXO store gains one row per block for the never-spent coinbase output. That row lives in the store, not the Go heap.

## Smoke Tests

```shell
make smoketest  # Runs E2E smoke tests in test/e2e/daemon/ready/ focused on basic functionality.

# With retry support (TEST_RETRY_DELAY is seconds between retries):
make smoketest TEST_RETRY_COUNT=3
make smoketest TEST_RETRY_COUNT=3 TEST_RETRY_DELAY=5

# Disable retries:
make smoketest TEST_RETRY_COUNT=1
```

## Sequential Tests

```shell
make sequentialtest  # Executes tests in test/sequentialtest/ sequentially.

# With retry support:
make sequentialtest TEST_RETRY_COUNT=5 TEST_RETRY_DELAY=3

# Database-backend-specific variants:
make sequentialtest-sqlite
make sequentialtest-postgres
make sequentialtest-aerospike

# Database variants also support retry flags:
make sequentialtest-aerospike TEST_RETRY_COUNT=5
make sequentialtest-postgres TEST_RETRY_COUNT=3
make sequentialtest-sqlite TEST_RETRY_COUNT=3
```

## Single Test

```shell
go test -v -race -tags "testtxmetacache" -run TestNameHere ./path/to/package
```

## Consensus-Critical Coverage Gate

These packages implement consensus rules, or apply them to the UTXO set and the chain tip:

- `services/blockvalidation`
- `services/validator`
- `services/subtreevalidation`
- `services/blockchain` (chain selection, reorg and catch-up)
- `services/blockassembly` and `services/blockassembly/subtreeprocessor` (reorg and conflict handling)
- `stores/utxo`, `stores/utxo/aerospike` and `stores/utxo/sql`

Each of these packages has a minimum statement coverage, in `.github/consensus-coverage.txt`. Every pull request checks the `coverage.out` that `make test` writes against these minimums, in the `Check consensus package coverage` step of `teranode_pr_tests.yaml`. The step fails when a package is below its minimum, or when the profile has no statements for it. The rest of the codebase has no minimum.

To run the check on a profile:

```shell
go run ./cmd/coveragegate -profile coverage.out -thresholds .github/consensus-coverage.txt
```

The minimums are CI values. A local `make test` profile reports different statement counts, so use the `coverage-report` artifact of a CI run to set a minimum. To add or remove a package, or to lower a minimum, change `.github/consensus-coverage.txt` in the pull request that needs it and give the reason in that pull request.
