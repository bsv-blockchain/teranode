# Multinode Local Testing

Run a configurable N-node teranode network locally with Docker Compose. All nodes form a full-mesh P2P network, each with its own Aerospike, Postgres database, and Kafka topics.

## Quick Start

```bash
# Build the teranode image first (if not already built)
make build

# Start a 5-node network
compose/multinode.sh up 5

# Generate blocks
compose/multinode.sh generate 1,10    # 10 blocks on node 1
compose/multinode.sh generate 1,5 3,5 # 5 blocks on node 1, then 5 on node 3

# Open all dashboards
compose/multinode.sh dashboards

# Check status
compose/multinode.sh status

# Tail logs for a specific node
compose/multinode.sh logs 2

# Tear down
compose/multinode.sh down
```

## Commands

| Command | Description |
|---|---|
| `up <N> [-allinone=0\|1] [--skip N:svc ...] [--chaostest [--svnodes 0-2]]` | Generate config and start N nodes (3-10). See [Split topology](#split-topology) for the flags, and [chaos-test scenarios](#chaos-test-scenarios) for `--chaostest`. |
| `down` | Stop and remove all containers and volumes |
| `restart` | Restart containers (picks up config changes after `make gen-multinode`) |
| `status` | Show container status (split mode: tallies running/total services per node) |
| `logs [node[-svc]]` | Tail logs for all nodes, a specific node, or one split-mode service (e.g. `logs 2-validator`) |
| `dashboards` | Open all node dashboards in the browser |
| `generate <node,count> ...` | Generate blocks on specific nodes |
| `blast [nodes] [--build] [--auto-mine[=N]] [--fresh] [--target multinode\|bsv-regtest] [-- args]` | Run the coinbase blaster against the stack, or against chaos-test's fleet |
| `sim up [--build] [--no-arcade] [--no-wallet]` / `sim run <scenario> [--step]` / `sim status\|logs\|down` | Run chaos-test scenarios against the stack. See [chaos-test scenarios](#chaos-test-scenarios). |

### Blaster

`blast` launches the `teranode-coinbase` blaster and points it at the stack's host-exposed propagation gRPC and RPC ports. `nodes` is a comma- or space-separated list (default: all running nodes). Anything after `--` is passed through to the blaster verbatim.

```bash
# Default TUI, blasting all running nodes
compose/multinode.sh blast

# Rebuild the blaster, then blast all nodes with auto-mining on node 1
compose/multinode.sh blast --build --auto-mine

# Headless, nodes 1 and 3, limited concurrency, auto-stop after 5m
compose/multinode.sh blast 1,3 -- --headless --workers 50 --duration 5m

# Tighter block cadence
BLAST_AUTO_MINE_INTERVAL=2 compose/multinode.sh blast --auto-mine
```

`--auto-mine[=N]` spawns a background loop that generates one block on node N (default: first target) every 5 seconds, overridable via `BLAST_AUTO_MINE_INTERVAL`. The loop is killed automatically when the blaster exits. If you name a different node than the first target the script warns, because split txs then have to propagate across the mesh before the funding RPC sees them.

`--build` rebuilds the blaster via `make build-blaster` before launching (ignored if `BLASTER_BIN` is set).

Before launching, `blast` polls each target node's RPC (`getinfo`) until it answers, with a 60s default timeout overridable via `BLAST_READY_TIMEOUT=<seconds>`. This avoids a race where gRPC dials to propagation land during container startup and end up in a stuck state; without it you'd see empty mined blocks and have to restart the blaster.

The blaster binary is located via `$BLASTER_BIN` (if set), otherwise `../teranode-coinbase/blaster` or `../teranode-coinbase/blaster-tui.run` (whichever is newest). The script does a pre-flight check that the binary supports the required CLI flags; if it's stale you'll get a clear error instead of a cryptic `flag provided but not defined`.

Blaster snapshot + embedded coinbase state goes to `data/multinode-blaster/` (outside the docker-managed `data/multinode/` tree so it stays user-owned). `multinode.sh down` wipes this alongside the chain state so a fresh stack doesn't inherit stale UTXOs.

## Network chaos tests

Go scenarios at `test/multinode/` drive this script through split-brain, crash-recovery, and slow-peer cases and assert invariants (tip convergence, reorg resolution, catchup). Run them with:

```bash
make network-chaos-test
```

The suite is gated behind the `network_chaos` build tag so it does not run under `go test ./...` or `make smoketest`. Scenarios refuse to start if another multinode stack is already running (set `MULTINODE_ALLOW_TAKEOVER=1` to override, or `MULTINODE_BYOS=1` to skip `up`/`down` entirely and reuse a running stack). Prereqs and per-scenario details are documented in `test/multinode/README.md`.

### Chaos commands

| Command | Description |
|---|---|
| `chaos isolate <node>` | Block peer traffic (RPC still works) |
| `chaos heal [node]` | Restore peer traffic, or all nodes if omitted |
| `chaos kill <node> [service]` | Stop a node container, or one service in split mode |
| `chaos start <node> [service]` | Start a stopped node or service container (also materialises a `--skip`-ped service) |
| `chaos pause <node> [service]` | Freeze a node or service (simulates hang/GC pause) |
| `chaos unpause <node> [service]` | Unfreeze a paused node or service |
| `chaos slow <node> <ms>` | Add network latency to a node |
| `chaos unslow <node>` | Remove added latency from a node |
| `chaos seal [node]` | `--chaostest` stacks: make chaos-test partitions hold (see [chaos-test scenarios](#chaos-test-scenarios)) |

```bash
# Isolate node 3 from peers (RPC still works, can still generate blocks)
compose/multinode.sh chaos isolate 3

# Restore all isolated nodes
compose/multinode.sh chaos heal

# Simulate a 500ms network delay on node 1
compose/multinode.sh chaos slow 1 500

# Freeze node 2 (simulates a long GC pause or disk stall)
compose/multinode.sh chaos pause 2

# Kill and restart node 4
compose/multinode.sh chaos kill 4
compose/multinode.sh chaos start 4
```

The `isolate`/`heal`/`slow`/`unslow` commands inject `iptables` and `tc` rules into the target container's network namespace via a short-lived `nicolaka/netshoot` sidecar that shares the netns (`docker run --net=container:teranodeN --cap-add=NET_ADMIN ...`). Rules persist in the target's netns after the sidecar exits. No `sudo`, `nsenter`, or `iproute2` on the host — works identically on Linux and macOS (Docker Desktop). The sidecar image (~200MB) is pulled on first use and cached. Override with `NETSHOOT_IMAGE=...` if you need a different tag or mirror. The `kill`/`start`/`pause`/`unpause` commands are pure Docker operations with no extra dependencies.

## chaos-test scenarios

[chaos-test](https://github.com/galt-tr/chaos-test) is a scenario engine with a web UI. A scenario is a YAML file of ordered steps (mine, spend, submit, freeze, pause a node, ...) with polled assertions (same tip, UTXO status, Kafka verdict events, container-log counts). A failed assertion either aborts the run or, with `should: true`, is recorded as a finding. `sim` runs its orchestrator against this stack.

```bash
git clone https://github.com/galt-tr/chaos-test ../chaos-test   # once; or set CHAOS_TEST_DIR

compose/multinode.sh up 3 --chaostest             # chaos-test's network layout, alert hub, 2 SV nodes
compose/multinode.sh sim up                        # builds the image on first use; UI on http://localhost:8600, arcade on :18080, wallet
compose/multinode.sh sim run freeze-timing-skew    # any bundled scenario; follows the run, exits 0 on pass
compose/multinode.sh sim run multinode-freeze-skew-rpc --step   # pause before each step
compose/multinode.sh sim down                      # 'down' also removes it
```

`--chaostest` (all-in-one only) lays the stack out the way chaos-test's `bsv-regtest` fleet is, which is what its bundled scenarios assume:

| Network | Subnet | Carries |
|---|---|---|
| `teranode-network-multinode-*` | docker-assigned | stores, Kafka, RPC, the orchestrator; never partitioned |
| `teranode-chaosnet-multinode-*` | 10.200.0.0/24, pinned | node p2p (9905), the datahubs, legacy p2p to the SV nodes (18444), arcade |
| `teranode-alertnet-multinode-*` | 192.0.0.128/26, pinned | alert p2p: teranodes (9908), the hub `alert-system-multinode` and SV sidecars (9906), the orchestrator |

- chaos-test partitions a node by disconnecting it from chaosnet (`plane: p2p`) or alertnet (`plane: alert`). A node's own services talk on `localhost`, because its container name resolves to its chaosnet address.
- Both planes are `internal`, and every node container drops packets for them that would leave through its default route (`chaos seal`, applied on `up`, `restart` and `chaos start`). Without that, a disconnected node still reaches the other side through the docker host: OrbStack forwards between bridges even when docker marks a network internal, and the partition silently leaks. Re-run `chaos seal` after restarting a node any other way.
- Alerts use chaos-test's dev genesis keys (read from its `keys.json` at `up`) and throwaway identities from [`gennodes/alert_keys.json`](cmd/gennodes/alert_keys.json). The hub starts once teranode1's alert service listens and serves its API (`:3000`) after syncing with two peers, about a minute after `up`.
- The nodes' asset API rate limits are raised (`asset_httpRateLimit`, `asset_httpHeavyRateLimit`) as on chaos-test's fleet. At the default 10 heavy requests per second per IP, the legacy service, which fetches each block it serves from its own asset API, was rate-limited and the SV nodes stalled.
- `--svnodes N` (0-2, default 2) adds Bitcoin SV nodes (`bitcoinsv/bitcoin-sv:1.2.2`, amd64) following over the legacy protocol, each with an alert sidecar; RPC on host 40332/41332. svnode2 runs with `acceptnonstdoutputs=0`, as on chaos-test's fleet. `up` mines block 1 and restarts the teranodes once: their legacy service advertises NODE_NETWORK, which SV Node needs to sync, only if a block was persisted when it started.
- Needs chaos-test's `alert-system` and `walletd` images; `up` and `sim up` build them from the checkout when missing.

How it fits together:

- `make gen-multinode` / `up` writes `generated/chaostest/inventory.json` (nodes, in-network and host URLs, Kafka verdict topics, containers) alongside the compose file.
- `sim up` copies it to `data/multinode-chaostest/config/`, adding the miner key (`PK1`, which the coinbase pays) and chaos-test's development `keys.json`. Keys never land in `generated/`.
- The orchestrator joins the stack's compose network, so it reaches nodes by name and consumes the `rejectedtx-*` / `invalid-blocks-*` topics that `event` checks match on. It uses the Docker socket (`CONTAINER_SOCKET`, default `/var/run/docker.sock`) for pause/stop and container-log checks.
- `sim up` also starts arcade (`arcade-multinode`, the image and settings of bsv-regtest's `arcade` profile) on the stack network, pointed at every node's datahub and p2p address (on chaosnet with `--chaostest`), and adds it to the inventory, so `submit` with `target: arcade` works. Host ports 18080 (API), 18081 (health), 18082 (events), 18083 (chaintracks); `ARCADE_HOST_PORT` moves them, `--no-arcade` skips it (and the wallet). merkle-service sends it merkle proofs.
- Before arcade it starts merkle-service (`merkle-service-multinode`, bsv-regtest's `merkle` profile, amd64, host 18090), which follows the nodes over p2p and calls arcade back with merkle proofs.
- It then starts the wallet: `wallet-db-multinode` (postgres), `wallet-infra-multinode` (go-wallet-toolbox storage server, amd64, broadcasting through arcade, host 18100) and `walletd-multinode` (host 18700), with chaos-test's dev wallet config and keys. `--no-wallet` skips it.
- With `--chaostest` the orchestrator also joins alertnet, so `push_alert` reaches the nodes. `down` clears its alert log along with the chain.
- Scenarios load from the image's bundled examples plus [`chaostest/scenarios/`](chaostest/scenarios/) (override with `SCENARIOS_DIR`). Bundled scenarios are baked into the image, so pick up new ones with `sim up --build`; ours are mounted live and load on the next `sim up`. Run records are kept in `data/multinode-chaostest/data/runs/`.

With `--chaostest` every action and check chaos-test has works here, so its bundled scenarios run unchanged. Without it there is one network: no alerts, SV nodes or partitions (use `chaos isolate`/`heal`). See [`chaostest/scenarios/README.md`](chaostest/scenarios/README.md) for results on this stack.

The blaster runs alongside: start `blast` in another terminal and run a scenario while it loads the network. On both this stack and chaos-test's fleet the blaster funds itself from **PK2**, the coinbase's second output, while chaos-test scenarios spend the first (PK1), so the two never spend the same coins. A blaster state directory funded from PK1 by an earlier version needs `blast --fresh`.

### chaosctl: both stacks, the blaster and scenarios in one tool

`compose/chaostest/chaosctl.sh` wraps all of this: `stack up a|b [--switch]`, `stack status|reset|down`, `blast start <preset> | status | stop` (presets in `compose/chaostest/blast-presets/`), and `scenario run <id>... [--under-load [preset]] [--reset]`, which records each result in `data/chaosctl/runs/` and compares it with the previous run. The Claude Code skills `chaos-stack`, `chaos-blast` and `chaos-scenario` (`.claude/skills/`) drive it.

### Blasting chaos-test's own fleet

Most of chaos-test's bundled scenarios need services this stack does not run yet (the alert hub, SV nodes, the wallet). To run them unchanged under load, run chaos-test's `bsv-regtest` fleet with our image and point the blaster at it:

```bash
# once: run our image, publish propagation gRPC, run amd64-only images under Rosetta
printf 'TERANODE_IMAGE_%s=teranode:latest\n' 1 2 3 > ../chaos-test/bsv-regtest/.env
cp compose/chaostest/bsv-regtest.override.yaml ../chaos-test/bsv-regtest/compose.override.yaml
make -C ../chaos-test/bsv-regtest RUNTIME=docker build-tools build-alert-system build-walletd up wait
make -C ../chaos-test RUNTIME=docker sim-build sim-up              # UI on http://localhost:8600

# terminal 1: load
compose/multinode.sh blast --target bsv-regtest --fresh --auto-mine -- --workers 10
# terminal 2 (or the UI): a scenario under that load
curl -X POST -d '{"mode":"auto"}' localhost:8600/api/scenarios/freeze-skew-rpc/run
```

- That fleet runs teranode on SQLite, where this stack runs Aerospike, so the same scenario can behave differently on the two (an RPC freeze, for one, takes effect there and not here). With the default 100 workers and a block every second it sustained about 250 tx/s, but the other teranodes fell up to 50 blocks behind and needed about 40 s to catch up once mining slowed. Use `--workers 10` and the default 5 s auto-mine when a scenario needs the fleet converged; use this stack for high throughput.
- chaos-test's `make reset` wipes the chain: stop the blaster first, and restart it with `--fresh`. Funding resumes once 100 blocks have been mined while it runs (`--auto-mine` does this).
- Only one stack can run at a time: both publish the same host ports.

### Nightly chain integrity on this stack

`compose/chaostest/nightly.sh` runs the whole thing unattended, locally or from
`.github/workflows/nightly_chain_integrity_stack.yaml` (05:00 UTC, `teranode-runner-16-core`, amd64):

| Phase | Suite (`compose/chaostest/suites/`) | What it proves |
|---|---|---|
| 1 | `chain-integrity-nightly` | Under wallet load through the broadcaster (and the blaster when available), with mining rotating over the teranodes, then again through a fork, a paused node and a merkle-service outage: `chain_audit` finds the same block hash and merkle root at every height on every teranode and in the broadcaster's chaintracks, and `proof_audit` checks every wallet tx's merkle path - built independently by merkle-service from datahub subtrees - against each node's block at that height. The SV nodes are cross-checked on the heights they hold. |
| 2 | `broadcaster-e2e` | the broadcaster scenarios (happy path, double spend, reorg, outages) |
| 3 | `chaos-library` | chaos-test's bundled scenarios, plus `svnode-dust-policy` after a 110-block pre-mine |

```bash
compose/chaostest/nightly.sh --dry-run       # what would run
compose/chaostest/nightly.sh --quick         # ~15 min smoke of the pipeline (short phase 1)
compose/chaostest/nightly.sh --phases 1      # one phase; --no-build reuses the images, --keep leaves the stack up
```

Results land in `data/nightly/<run>/`: `summary.md` (also the GitHub job summary), `junit.xml`,
every run record (`runs/`, `orchestrator-runs/` with the audit reports), container logs,
diagnostics and merkle-service's consumer groups. The exit code is non-zero for any failure not
listed in `suites/known-failures.txt`; chain divergences and wrong proofs are never quarantined.
Scenario parameters can be overridden per run with `chaosctl.sh scenario run ... --param k=v`.

To run the workflow from a fork on your own machine: register a self-hosted runner for the fork
(Settings → Actions → Runners) with a label such as `chaos-stack`, and set the repository
variable `NIGHTLY_RUNNER` to that label (or a JSON list, `["self-hosted","chaos-stack"]`). The
runner needs Docker, about 16 GB of RAM for the stack, and amd64 or amd64 emulation (Docker
Desktop on Apple silicon works, more slowly). Bring any local stack down first: the run checks
that the stack's host ports are free. Without a `TERANODE_COINBASE_TOKEN` secret the run uses
wallet load only.

## Split topology

By default each node is a single container running every teranode microservice in one process (`-allinone=1`). For chaos testing you usually want finer-grained failure injection. Pass `-allinone=0` and each node is generated as nine sibling containers, one per microservice:

`teranodeN-blockchain`, `teranodeN-blockassembly`, `teranodeN-blockvalidation`, `teranodeN-subtreevalidation`, `teranodeN-validator`, `teranodeN-propagation`, `teranodeN-p2p`, `teranodeN-asset`, and `teranodeN-core` (the catch-all sidecar that bundles rpc, blockpersister, utxopersister, pruner, and legacy so RPC stays reachable).

Each sibling has its own gRPC listener on the docker network; addresses are wired through the generated `settings_multinode.conf`. Sibling services `depends_on` `teranodeN-blockchain` with `service_healthy`, so blockchain comes up first and dependents only start once its listener accepts connections.

```bash
# 3-node split stack: 27 service containers + infra
compose/multinode.sh up 3 -allinone=0

# Kill one service without bringing down the rest of node 2
compose/multinode.sh chaos kill 2 validator

# Bring it back
compose/multinode.sh chaos start 2 validator

# Tail one service's logs
compose/multinode.sh logs 2-validator
```

Resource note: a 3-node split stack runs ~32 containers (3 × 9 teranode services + infra). 16 GB RAM is comfortable; smaller hosts may struggle.

> **Always `down` before switching topology.** Split-mode commands (`chaos kill <node> <svc>`, `logs N-svc`, the per-node service tally in `status`) detect split vs all-in-one by checking whether the monolithic `teranodeN-multinode` container exists. Running `up <N> -allinone=0` over a stack that was previously brought up with the default `-allinone=1` (or vice versa) can leave stale containers around and confuse mode detection. Run `compose/multinode.sh down` first whenever you flip `-allinone`.

### Skipping services at startup (`--skip`)

To bring a node up *without* a given service from the start (instead of killing it post-up), pass `--skip N:svc` to `up`. The flag is repeatable and only valid with `-allinone=0`.

```bash
# Node 2 comes up without block assembly; node 3 without validator
compose/multinode.sh up 3 -allinone=0 --skip 2:blockassembly --skip 3:validator

# Materialise a skipped service later (uses `compose up -d` so it works even
# though the container was never created during the initial up)
compose/multinode.sh chaos start 2 blockassembly
```

`blockchain` cannot be `--skip`-ped because every sibling on the same node declares it as a `service_healthy` dependency, so docker compose would auto-resolve and start it anyway. To stop blockchain after the stack is up, use `chaos kill <N> blockchain`.

## Architecture

Each `up N` invocation generates a self-contained bundle under `compose/generated/`:

```
compose/generated/
  docker-compose-multinode.yml    # Main compose file
  settings_multinode.conf         # Per-node settings overlay
  postgres/init-multinode.sql     # DB roles and schemas for N nodes
  aerospike/aerospike-{1..N}.conf # Per-node Aerospike config
  open-dashboards.sh              # Browser launcher
  generate-blocks.sh              # Block generation helper
```

### Shared infrastructure (one instance)

- **Postgres** - one server, separate database per node (`teranode1`, `teranode2`, ...)
- **Kafka (Redpanda)** - one broker, per-node topic names via env vars
- **Jaeger** - shared tracing collector

### Per-node infrastructure

- **Aerospike** - one instance per node on ports `3010`, `3020`, ..., `3100`
- **Teranode** - each node runs all services in one container (all-in-one default), or nine sibling containers in split mode (see [Split topology](#split-topology))

### Port scheme

Each node gets a 2000-port host range starting at 20000:

| Node | Host base | Dashboard | RPC | P2P | Health |
|------|-----------|-----------|-----|-----|--------|
| 1 | 20000 | 20090 | 21292 | 21905 | 20000 |
| 2 | 22000 | 22090 | 23292 | 23905 | 22000 |
| 3 | 24000 | 24090 | 25292 | 25905 | 24000 |
| N | 20000+(N-1)*2000 | base+90 | base+1292 | base+1905 | base |

Container-internal ports are unchanged (8000, 8090, 9292, 9905, etc.). The host mapping is `host_base + (container_port - 8000)`.

### P2P mesh

All nodes are wired in a full mesh via static peers. Each node's `p2p_static_peers` lists every other node. Bootstrap peers are disabled so discovery stays deterministic.

### Node identity

Libp2p keypairs are pre-seeded in `compose/cmd/gennodes/peer_keys.json` (10 keys). Indices 1-3 reuse the keys from `compose/settings_test.conf` so a 3-node generated stack uses the same peer IDs as the existing `docker-compose-3blasters.yml` setup. To regenerate or extend the pool:

```bash
go run ./compose/cmd/genpeerkeys -n 20 -o compose/cmd/gennodes/peer_keys.json
```

### Coinbase identification

Each node stamps `/teranodeN/` in its mined blocks via `coinbase_arbitrary_text`, so you can tell which node mined a given block.

## Makefile targets

The wrapper script is the recommended interface, but individual Makefile targets are also available:

```bash
make gen-multinode N=5                  # Generate config only (no docker)
make open-dashboards                    # Open dashboards
make generate-blocks ARGS="1,10 3,5"   # Generate blocks
```

## Troubleshooting

**Containers won't start (name conflict)**
Another compose stack may have containers with the same service names. Run `docker ps -a | grep teranode` and remove stale containers, or use `compose/multinode.sh down` first.

**Settings changes not taking effect**
Teranode reads settings at startup. After regenerating with `make gen-multinode`, restart the containers:

```bash
compose/multinode.sh restart
```

**Data directory permissions**
Containers run as root, so `data/multinode/` files are root-owned. Clean up with:

```bash
sudo rm -rf data/multinode/
```

**Postgres port conflict**
The shared Postgres binds to host port 15432. If that's in use, stop the conflicting service or edit the generated compose file.
