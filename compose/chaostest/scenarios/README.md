# chaos-test scenarios for the multinode stack

Scenarios here are loaded by `compose/multinode.sh sim up` alongside chaos-test's bundled
examples. A file here with the same `id` as a bundled one overrides it.

The authoring reference (actions, assertions, `${...}` expressions) is chaos-test's
`examples/scenarios/README.md`.

What the multinode stack supports:

- Works: `mine`, `coinbase`, `newkey`, `spend`, `submit` to a node, `rpc_freeze`/`rpc_unfreeze`,
  `chaos` (pause/unpause/stop/start), `automine`, `set`, `mark`, `wait`, and every check that
  reads nodes, Kafka verdicts or container logs (`same_tip`, `height`, `utxo_status`,
  `mempool_has`, `event`, `log_count`, `expr`, ...).
- Not available: `submit` to `arcade`, `build_alert`/`push_alert` (no alert hub), `wallet_*`,
  `arcade_*` checks, and SV nodes. Every bundled scenario uses at least one of these; run them
  on chaos-test's own fleet instead (see "Blasting chaos-test's own fleet" in `MULTINODE.md`).
- `partition` needs chaos-test's iptables partition mode (the stack has one network, so a network
  disconnect would also cut a node off from its stores). Until that is available, use
  `compose/multinode.sh chaos isolate|heal` by hand.

| Scenario | What it checks |
|---|---|
| `multinode-smoke` | Wiring end to end: mine, spend, submit, pause a node, reconverge. Run it first. |
| `multinode-freeze-skew-rpc` | chaos-test's `freeze-skew-rpc`, submitting to a node instead of arcade. |
