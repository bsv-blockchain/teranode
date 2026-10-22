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

The soak test runs the in-process daemon under steady transaction load for a long wall-clock duration and fails if heap or goroutine counts trend upward, which is how slow leaks and unbounded caches show up. It samples `runtime.ReadMemStats` (after a forced GC) and `runtime.NumGoroutine()` at a fixed interval, discards a warm-up window, and fails a metric only when both its least-squares growth across the run and its last-quarter-minus-first-quarter mean exceed the tolerance. It is behind the `soak` build tag, so no other suite runs it. The nightly workflow runs it for two hours.

```shell
make soaktest                                          # 30 minute run (default)
make soaktest SOAK_DURATION=2h SOAK_TIMEOUT=150m       # nightly length; SOAK_TIMEOUT must exceed SOAK_DURATION by 10m+
make soaktest SOAK_DURATION=4m SOAK_TIMEOUT=20m SOAK_WARMUP=1m SOAK_SAMPLE_INTERVAL=10s SOAK_INJECT_LEAK=1  # confirm a leak fails it
```

| Variable | Default | Meaning |
|---|---|---|
| `SOAK_DURATION` | `30m` | How long to drive load |
| `SOAK_WARMUP` | larger of 5m and 20% of the duration, at most half of it | Samples before this are ignored |
| `SOAK_SAMPLE_INTERVAL` | `30s` | Time between samples |
| `SOAK_TXS_PER_CYCLE` | `500` | Transactions sent and mined per cycle |
| `SOAK_UTXO_STORE` | sqlite | `aerospike` or `postgres` run the store in a container, so its memory is not measured |
| `SOAK_HEAP_TOLERANCE` / `SOAK_GOROUTINE_TOLERANCE` | `0.10` / `0.05` | Allowed growth as a fraction of the mean (floors of 16 MiB and 20 goroutines) |
| `SOAK_OUTPUT_DIR` | a temp dir | Where `soak-samples.csv` and, on failure, heap and goroutine profiles are written |
| `SOAK_INJECT_LEAK` | `false` | Retain 1 MiB and one goroutine per cycle, to check that the test fails |

Only the Go heap is measured: memory held outside it, such as C allocations or containerised stores, is not.

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
