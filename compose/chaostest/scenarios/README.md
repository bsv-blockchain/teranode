# chaos-test scenarios for the multinode stack

Scenarios here are loaded by `compose/multinode.sh sim up` alongside chaos-test's bundled
examples. A file here with the same `id` as a bundled one overrides it.

The authoring reference (actions, assertions, `${...}` expressions) is chaos-test's
`examples/scenarios/README.md`.

On a stack started with `up --chaostest` (what `chaosctl stack up a` does) plus `sim up`, every
chaos-test action and check works: partitions, alerts, SV nodes, arcade and the wallet. Without
`--chaostest` the stack has one network, so `partition`, `build_alert`/`push_alert` and SV nodes
are unavailable; use `compose/multinode.sh chaos isolate|heal` by hand.

Bundled scenarios on this stack (28 Oct 2026, image built from `pruner-unmined-tx-loss-repro`,
3 teranodes on Aerospike, 2 SV nodes):

| Scenario | Result | Why |
|---|---|---|
| `arcade-same-height-flip-flop` | passed, 16/16 | |
| `svnode-dust-policy` | passed, 13/13 | needs a chain past height 103: mine 110 blocks first on a new chain |
| `freeze-timing-skew` | 20/22, fails at "C mines blockInside" | A publishes no invalid-block verdict on Kafka; findings: A rejects the below-window block (no height-anchored freeze in this build) |
| `freeze-fork-remine` | 25/28, fails at "C keeps mining on its fork" | same: no invalid-block verdict for forkInside |
| `freeze-skew-rpc` | 7/16, fails at the freeze step | the RPC `freeze` has no effect on Aerospike (teranode bug) |
| `wallet-sustained-flow` | 1/7, fails at the top-up balance check | the wallet's balance counts the fan-out's change, not its coins (307,612 < 1,000,000); the transactions themselves broadcast, propagate and get proofs |

| Scenario | What it checks |
|---|---|
| `multinode-smoke` | Wiring end to end: mine, spend, submit, pause a node, reconverge. Run it first. |
| `multinode-freeze-skew-rpc` | chaos-test's `freeze-skew-rpc`, submitting to a node instead of arcade. |

## Broadcaster end-to-end suite

`compose/chaostest/chaosctl.sh scenario run --suite broadcaster-e2e --reset` runs the scenarios
in `../suites/broadcaster-e2e.txt`. The `multinode-bcast-*` ones submit through the BSV
Broadcaster (`arcade-multinode`) and assert with chaos-test's `arcade_tx_status`, which also
checks the merkle proof against node headers. They need chaos-test with `arcade_tx_status` and
service targets for `chaos` (rebuild with `compose/multinode.sh sim build`).

| Scenario | What it checks |
|---|---|
| `multinode-bcast-happy-path` | SEEN_ON_NETWORK and every mempool, then MINED in the next block with a proof valid on A and C |
| `multinode-bcast-double-spend` | first spend MINED; a rival via the broadcaster ends DOUBLE_SPEND_ATTEMPTED or REJECTED; a rival sent to B is refused |
| `multinode-bcast-reorg` | a tx MINED on the losing side of a partition is mined again on the winning chain, proof against the canonical block |
| `multinode-bcast-merkle-outage` | merkle-service paused across the block: no MINED while down, MINED in that block after recovery |
| `multinode-bcast-node-down` | teranode1 paused: accepted and mined through B and C; MINED once A is back (delay while down is a finding) |
| `multinode-bcast-broadcaster-restart` | broadcaster stopped across the block, then started: still MINED in that block |
| `multinode-wallet-chaos-flow` | wallet send at 2 tx/s through a C partition and a B pause: no failures, no wallet divergence, fleet converges |

Results (3 Nov 2026, teranode `teranode:latest`, broadcaster `ghcr.io/bsv-blockchain/broadcaster:latest`):

| Scenario | Result | Notes |
|---|---|---|
| happy-path, double-spend, reorg, merkle-outage, wallet-chaos-flow | passed | also passed under `--under-load ds-storm` (happy-path, double-spend, wallet) |
| `multinode-bcast-node-down` | passed, 1 finding | MINED waits for teranode1: merkle-service keeps fetching block data from its datahub (`10.200.0.11`) and does not fail over to B or C |
| `multinode-bcast-broadcaster-restart` | fails at "start the broadcaster" | after a clean stop/start the broadcaster answers 404 for a tx it had reported SEEN_ON_NETWORK (pebble WAL replayed; config uses `kafka.backend: memory`) |

`sim up` warms merkle-service by putting a placeholder on its `subtree`, `block` and
`subtree-work` topics: each of its stages starts only after the one before reads a first
message, so on a new chain the first tx would otherwise wait for a block with a subtree before
SEEN_ON_NETWORK, and for a full replay before MINED.
