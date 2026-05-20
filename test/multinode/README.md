# Network chaos tests

Go scenarios that drive `compose/multinode.sh` through real docker-level chaos
(iptables partition, tc latency, docker pause/kill) and assert the invariants
teranode cares about: tip convergence, reorg resolution, catchup recovery.

These tests exist because in-process `TestDaemon` tests share memory and
cannot exercise the peer-to-peer rejoin path. If it involves iptables or a
crashed container, it lives here.

## Running

```bash
make network-chaos-test
```

Prereqs:

- Docker with compose v2
- `teranode:latest` image built (`make build` or `compose/multinode.sh up N --build`)
- Passwordless sudo for the scenarios that use `chaos isolate` / `chaos slow`
  (those scenarios skip when sudo is unavailable)

## Lifecycle model

**One shared stack, many scenarios.** `TestMain` provisions a single 5-node
stack once, runs every scenario serially against it, and tears it down at
the end. This amortises the multi-minute cold-start cost across tests and
avoids repeatedly hitting the Aerospike secondary-index startup race.

Each scenario begins with `stack.Reset(t)`, which:

- heals any lingering iptables isolation rules,
- removes any tc netem latency qdiscs,
- unpauses any frozen containers,
- restarts any containers the previous scenario killed,
- waits for every node's RPC to answer,
- waits for the p2p mesh to re-establish,
- waits for all nodes to converge on a single tip.

After Reset, the scenario records the current tip as its baseline and uses
*relative* heights (`baseline + 5`, etc.) for assertions so test order
doesn't matter.

## Environment variables

| Var | Effect |
|---|---|
| `MULTINODE_BYOS=1` | Skip Provision/Teardown; run against an already-running stack. Useful for iterating on a single scenario. |
| `MULTINODE_ALLOW_TAKEOVER=1` | Do not abort when a stack is already running. Use with care; Teardown will tear it down. |
| `MULTINODE_UP_TIMEOUT=<seconds>` | Override the initial readiness-wait (default 240s). |

## Scenarios

| File | Operates on | Sudo | What it proves |
|---|---|---|---|
| `scenario_01_split_brain_test.go` | all 5 nodes | yes | A partitioned node forks off, heals, and reorgs onto the majority chain. |
| `scenario_02_crash_recovery_test.go` | nodes 1-3 | no | A killed node rejoins and catches up to tip. |
| `scenario_03_blast_under_latency_test.go` | nodes 1-3 | yes | Added latency on one node does not stall propagation or create persistent forks. |

All three scenarios currently mine via `generate` RPC rather than the
coinbase-blaster, so the chaos mechanics are isolated from tx-load
confounds. A follow-up can layer the blaster on top of the same shared
stack once these baselines are stable.

## Adding a scenario

1. Pick a file name `scenario_NN_<slug>_test.go` with the next free number.
2. Start the file with `//go:build network_chaos`.
3. Open with `s := stack(); s.Reset(t)`. If the scenario uses iptables/tc
   primitives, also call `s.RequireSudo(t)` before Reset.
4. Use relative heights (`info.Blocks + N`) rather than absolute, so order
   with other scenarios doesn't matter.
5. Don't call `Up` or `Down` — the shared stack handles those.

## Caveats

- `getchaintips` is server-side-cached for 5 minutes in teranode, so
  `BestTip` (and anything that would use it) goes through
  `getblockchaininfo` instead (10s cache). Scenarios that genuinely need
  fork tip data should call `GetChainTips` directly once, right where
  they need it.
- State bleeds between scenarios: chaos state is reset but chain state
  is cumulative. Use relative heights.

## Debugging a failure

- Stack logs live in `docker logs teranode<N>-multinode`.
- `compose/multinode.sh status` shows container states and per-node
  RPC-reported heights.
- For iterating locally:
  ```bash
  compose/multinode.sh up 5
  MULTINODE_BYOS=1 go test -tags network_chaos -v -run TestCrashRecovery ./test/multinode/
  compose/multinode.sh down
  ```
