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
