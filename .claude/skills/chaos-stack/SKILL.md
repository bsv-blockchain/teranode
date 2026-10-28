---
name: chaos-stack
description: Start, stop, switch, reset and inspect the two local chaos-testing stacks — Stack A, our multinode teranode stack (compose/multinode.sh, Aerospike) with chaos-test's orchestrator, and Stack B, galt-tr/chaos-test's bsv-regtest fleet (SQLite, arcade, alert hub, SV nodes, wallet) running our teranode image. Use whenever the user wants to bring up, tear down, switch between or reset a chaos/test environment, asks "is the fleet up / converged", mentions stack A or stack B, galt's fleet, bsv-regtest, chaos-test, the orchestrator on :8600, or wants a clean chain before a scenario. Pair with chaos-blast (load) and chaos-scenario (assertions).
license: MIT
metadata:
  audience: devs, qa
  layer: test-environment
---

# Chaos test stacks

Everything goes through one script, run from the teranode checkout:

```bash
compose/chaostest/chaosctl.sh stack status
compose/chaostest/chaosctl.sh stack up a [N] [-allinone=0] [--switch]
compose/chaostest/chaosctl.sh stack up b [--switch]
compose/chaostest/chaosctl.sh stack down a|b
compose/chaostest/chaosctl.sh stack reset
```

Always start with `stack status`: it says which stack is up, the image node 1 runs, every
node's height/tip with CONVERGED or SPLIT, the orchestrator, any active scenario run and the
blaster.

## Which stack

| Need | Stack |
|---|---|
| `freeze-skew-rpc` (needs arcade) | **A** (arcade starts with the orchestrator); B to compare on SQLite |
| galt's other bundled scenarios (alert hub, SV-node dust, wallet) | **B**: only it has the alert hub, SV nodes and the wallet |
| Our scenarios in `compose/chaostest/scenarios/`, Aerospike behaviour, split-mode per-service chaos (`-allinone=0`), pruner rigs, high throughput (~900 tx/s) | **A** |
| Same scenario on two stores | both, one after the other (Stack B is SQLite, Stack A Aerospike) |

Both stacks publish the same host ports (20000+), so only one runs at a time. `stack up`
refuses while the other is up; `--switch` brings the other down first.

## Confirm before destroying state

`stack down` and `stack reset` delete the chain (`stack down a` also wipes Stack A's
Aerospike/Postgres data and the blaster state). If a scenario run is active, the blaster is
running, or the user did not ask for it in this turn, say what will be lost and ask first.
`stack up --switch` counts as a down of the other stack.

## What the script handles (so you don't have to)

- Stack A: `multinode.sh up` then `multinode.sh sim up` (builds the orchestrator image from
  `../chaos-test` on first use, and starts arcade on :18080).
- Stack B: writes `bsv-regtest/.env` (all teranodes on `$TERANODE_IMAGE`, default
  `teranode:latest`) and installs `compose/chaostest/bsv-regtest.override.yaml` only if they
  are missing — an existing `.env` is kept, because people pin other builds there for
  comparisons. Builds missing support images, then `make up wait` and `make sim-up`.
- Waits for the orchestrator API and for the fleet to converge.
- Stops the blaster before any down or reset.

## Reading the result

- **SPLIT right after start-up**: SV nodes and followers catch up within a minute or two;
  run `stack status` again before acting.
- **SPLIT that persists after a scenario**: expected after freeze scenarios — the node holding
  the freeze refuses the other chain, and mining cannot heal it. `stack reset`.
- **Stack B nodes lag under load**: SQLite teranodes fall tens of blocks behind with a block
  every second and recover in about 40 s once mining slows. Not a fork; wait.

## Troubleshooting

| Symptom | Cause / fix |
|---|---|
| `chaos-test checkout not found` | `git clone https://github.com/galt-tr/chaos-test ../chaos-test` or set `CHAOS_TEST_DIR` |
| `image teranode:latest not found` | `make build`, or `TERANODE_IMAGE=<image> chaosctl.sh stack up …` |
| `no matching manifest for linux/arm64` | The override is missing or outdated: it runs bitcoin-sv, go-wallet-toolbox and merkle-service as amd64. Compare `../chaos-test/bsv-regtest/compose.override.yaml` with ours |
| Orchestrator restart-looping | Its bind mounts vanished (e.g. a checkout under /tmp was cleaned). `stack down b`, then `stack up b` |
| Port already allocated | Another compose project holds 20000+ ports (`docker ps --format '{{.Names}} {{.Ports}}'`). Ask before stopping someone else's stack |
| A node never converges | Hand over to the `debug-teranode-docker` skill for logs and profiles |

## Comparing builds on Stack B

Put a different image for one node in `../chaos-test/bsv-regtest/.env`
(`TERANODE_IMAGE_1=<image>`), then `stack reset`. `stack status` shows node 1's image.
