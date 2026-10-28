---
name: chaos-blast
description: Run the teranode-coinbase blaster as background load on whichever chaos stack is up (Stack A multinode or Stack B chaos-test fleet), from named mix presets — invalid transactions (bad-sig, double-input, zero-output, spend-nonexistent), double-spend strategies (race, split-node, delayed, storm), orphans, burst traffic, slow/lossy peers, UTXO I/O pressure, Chronicle probes. Funds it, confirms it is sending, reports its stats and stops it cleanly. Use whenever the user wants load, a tx mix, "blast", "double-spend storm", "invalid sigs", "put the network under load", or to start/stop/check the blaster during chaos testing. For running a scenario under load in one go, chaos-scenario's --under-load calls this.
license: MIT
metadata:
  audience: devs, qa
  layer: test-load
---

# Blaster as background load

```bash
compose/chaostest/chaosctl.sh blast presets
compose/chaostest/chaosctl.sh blast start [preset] [--workers N] [--fresh] [--no-fund] [--no-mine]
compose/chaostest/chaosctl.sh blast status
compose/chaostest/chaosctl.sh blast stop
```

A stack must be up (chaos-stack). `blast start` picks the right target itself
(`multinode.sh blast`, or `--target bsv-regtest` on Stack B), starts the blaster headless
in the background with auto-mine, mines 110 blocks so its funding matures (the embedded
coinbase only counts blocks mined while it runs, and needs 100 confirmations), and returns
once it is sending. Log: `data/chaosctl/blast.log`.

## Choosing a preset

| Goal | Preset |
|---|---|
| Plain background load | `default` |
| Rejection paths and `rejected_tx` verdicts | `invalid-bad-sig`, `invalid-double-input`, `invalid-zero-output`, `invalid-spend-nonexistent` |
| Conflict resolution | `ds-race`, `ds-splitnode`, `ds-delayed`, `ds-storm` |
| Orphan pool | `orphan` |
| Slow or lossy peer | `slow-node2` |
| Thundering herd | `burst` |
| UTXO-store I/O | `io-heavy` |
| Everything at once | `adversarial` |
| Chronicle relaxations | `chronicle` (only meaningful where Chronicle is active) |

## Workers

| Stack | Workers | Rough rate |
|---|---|---|
| B (SQLite) | 10 (default) | ~35 tx/s; at 100 workers ~250 tx/s but followers fall 50 blocks behind |
| A (Aerospike) | 100 | ~900 tx/s |

Keep Stack B at 10 when a scenario needs the fleet converged.

Auto-mine runs every 5 s on Stack A but every **30 s on Stack B** (override with
`BLAST_AUTO_MINE_INTERVAL`). Measured on 26 Oct 2026: under `ds-storm` at ~55 tx/s with a block
every 5 s, Stack B's SQLite followers stayed about three blocks behind and never reached one
tip, so every scenario's opening `same_tip` check failed; they converged 30 s after mining
stopped. Funding does not depend on the interval: `blast start` mines 110 blocks up front.

## Rules that matter

- **Load for a scenario: `--no-mine`.** The auto-miner mines on teranode1, which chaos-test
  scenarios use as a role and whose height they check exactly. `chaos-scenario --under-load`
  passes `--no-mine` itself; pass it by hand when you start load for a scenario you will run
  separately.
- **One blaster at a time**; `blast stop` before changing presets.
- **Stop it before any reset** (chaos-stack's `stack reset` does this), then start with
  `--fresh`: its coinbase DB belongs to the old chain and every funding attempt fails with
  missing parents otherwise.
- **Coins**: on both stacks it funds from PK2 (coinbase output 1) and chaos-test scenarios
  spend output 0 (PK1), so they never collide. A blaster state directory from before this
  (funded from PK1 on Stack A) needs `--fresh`.

## Reading the stats line

`[mm:ss] tx=… tx/s=… workers=… utxos=… rejects=… chain_breaks=… fund_req=… fund_err=… drops=…`

- `rejects` rising is expected with `invalid-*` and `ds-*` presets; with `default` it should
  stay low.
- `fund_err` in the first minute is normal (funding still maturing); still rising after the
  110-block funding means the monitor RPC or the coinbase DB is wrong — `blast stop`, then
  `blast start --fresh`.
- `tx/s` at 0 with `utxos=0` for long: starved of funding; mine more or restart `--fresh`.

## Writing a new preset

Add `compose/chaostest/blast-presets/<name>.json`:

```json
{
  "description": "One line people read in 'blast presets'.",
  "mix": {"Weights": [90, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 10, 0, 0, 0], "invalid_mode": "bad-sig"},
  "node_chaos": {"2": {"latency_ms": 50, "latency_max_ms": 500, "drop_pct": 10, "churn_pct": 0}},
  "burst_pattern": "5000,500"
}
```

`Weights` has 16 entries summing to 100, in this order: P2PKH, OpTrue, FanOut, OpReturn,
LargeTx, LongChain, DoubleSpend, CustomScript, Multisig, LargeData, HashPuzzle, TimeLock,
Invalid, Orphan, ChronicleNonPush, ChronicleUncleanStack. Other `mix` keys
(`ds_strategy` 0-4 = Sequential, SplitNode, Race, Delayed, Storm; `invalid_mode`;
`orphan_delay_ms`; `FanOutN`; `ConsolidateN`; `tx_version`) override the blaster's default
mix — the script fills the rest, because a schedule phase replaces the whole mix.
`node_chaos` is keyed by node number; `burst_pattern` becomes `--burst-pattern`.
