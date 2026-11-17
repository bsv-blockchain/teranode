# How to Manage Teranode States

This guide explains how to change and monitor Teranode's state. Fresh production
deployments (`operator` and `docker.m` settings contexts) start in `IDLE`, giving
an operator a safe inspection window. Other contexts keep the automatic
`CATCHINGBLOCKS` default. Configure this with
`blockchain_initializeNodeInState`; it accepts `IDLE`, `CATCHINGBLOCKS`, or
`RUNNING` (uppercase). Invalid values fail startup only when no FSM state is
persisted. On a checkpointed network, configured `RUNNING` requires a pre-seeded
tip at or above the highest checkpoint; otherwise startup fails without fallback.
Use `CATCHINGBLOCKS` to synchronize a fresh node.

After inspecting a fresh production deployment, start synchronization from
`IDLE` with `teranode-cli setfsmstate --fsmstate catchingblocks`. A direct
`RUNNING` request is refused below the highest checkpoint; catch-up promotes the
node automatically once it reaches that checkpoint.

The setting applies only when no FSM state is persisted. Restarts normally
restore the persisted state without validating unused boot configuration. A
persisted `RUNNING` state with a successfully read tip below the active network's
highest checkpoint is durably migrated to `CATCHINGBLOCKS`. Tip-read failures or
missing metadata abort startup and leave the persisted state unchanged.

Automatic `Run` and `CatchUpBlocks` requests cannot leave operator `IDLE`.
This also prevents automatic catchup from bypassing STOP by first entering
CATCHINGBLOCKS and then requesting RUN. Legacy synchronization reaching the tip
cannot reverse an operator STOP. To leave IDLE deliberately, use
`teranode-cli setfsmstate --fsmstate catchingblocks` to start synchronization, or
explicitly request `running` when checkpoint-safe. IDLE does
not prove that already admitted work has drained; rewind still requires service
shutdown. When catchup entry is refused, block validation clears its processing
markers without penalizing the peer. Explicit resume permits a later block
notification to retry; this does not guarantee immediate replay of queued work.

## Prerequisites

- Access to a running Teranode instance
- One of the following access methods:
    - Admin Dashboard (easiest - web-based interface)
    - `teranode-cli` (recommended for scripting - available in all Teranode containers)
    - `grpcurl` (advanced - requires network access to the RPC Server on port 18087)

## Recommended Method: Using Admin Dashboard

The Admin Dashboard provides the easiest way to view and manage Teranode FSM states through a web interface.

### Accessing the Dashboard

**Docker Compose:**

```bash
# Access the dashboard in your browser
# http://localhost:8090/admin
```

**Kubernetes:**

```bash
# Port-forward the asset service
kubectl port-forward -n teranode-operator service/asset 8090:8090

# Then access http://localhost:8090/admin in your browser
```

### Managing FSM State

1. Navigate to the FSM State section in the dashboard
2. View the current state
3. Use the state transition controls to change states
4. Monitor state transition logs in real-time

> **Note:** The dashboard must be enabled via the `dashboard_enabled` setting and may require authentication depending on your configuration (`dashboard.auth.enabled`).

## Alternative Method: Using teranode-cli

The `teranode-cli` is recommended for scripting and automation. It provides a command-line interface that works directly with the blockchain service.

### Docker Compose Environment

#### 1. Check Current State

```bash
docker exec -it blockchain teranode-cli getfsmstate
```

#### 2. Set New State

```bash
docker exec -it blockchain teranode-cli setfsmstate --fsmstate RUNNING
```

### Kubernetes Environment

#### 1. Check Current State

Access any Teranode pod and use teranode-cli directly:

```bash
# Get the name of a pod (blockchain or asset are good options)
kubectl get pods -n teranode-operator -l app=blockchain

# Access the pod and run the command
kubectl exec -it <pod-name> -n teranode-operator -- teranode-cli getfsmstate

# Alternative one-liner
kubectl exec -it $(kubectl get pods -n teranode-operator -l app=blockchain -o jsonpath='{.items[0].metadata.name}') -n teranode-operator -- teranode-cli getfsmstate
```

#### 2. Set New State

```bash
# Change state to RUNNING
kubectl exec -it $(kubectl get pods -n teranode-operator -l app=blockchain -o jsonpath='{.items[0].metadata.name}') -n teranode-operator -- teranode-cli setfsmstate --fsmstate RUNNING
```

## Valid FSM States

The following states are valid for all environments:

- IDLE
- RUNNING
- CATCHINGBLOCKS

### When a transition is refused

One rule constrains which transitions are accepted, and it surfaces as an error
rather than a silent no-op:

- **RUN is refused while the chain tip is below the network's highest hard-coded
  checkpoint.** Mainnet and testnet both have checkpoints; regtest has none. The
  error names both your tip height and the checkpoint it must reach. From IDLE,
  the node remains parked so you can inspect or rewind it; use
  `setfsmstate --fsmstate catchingblocks` when you deliberately want to start
  synchronization.
  A node already in CATCHINGBLOCKS remains there and will move to RUNNING once it
  catches up.

Why the rule exists: going to RUNNING mid-initial-sync lets the mempool and
validator operate under pre-Genesis output rules, and lets the legacy service
relay tx invs that post-Genesis peers ban on sight
(`bad-txns-vout-p2sh BAN THRESHOLD EXCEEDED`).

> **Behaviour change:** the checkpoint rule used to exempt `IDLE -> RUNNING`
> entirely, on the reasoning that a fresh node boots into CATCHINGBLOCKS and so
> could never be in IDLE below the checkpoint. That is not exhaustive — a node
> stopped from RUNNING, or one whose store was persisted in IDLE by an older
> version, both land there — so the rule now applies to every RUN and the IDLE
> case is refused as described above. Out-of-tree boot tooling that forces
> RUNNING on a below-checkpoint mainnet or testnet node will now receive an error
> and leave the FSM unchanged. Regtest has no checkpoints and is unaffected —
> `setfsmstate --fsmstate running` still goes straight to RUNNING there.
>
> **There is no longer a manual route to RUNNING below the checkpoint.** From
> IDLE, explicitly enter CATCHINGBLOCKS to start synchronization. From
> CATCHINGBLOCKS, RUN is refused until catchup completes. If you are looking for
> an override to force a below-checkpoint node into RUNNING, it no longer exists
> — that was the hole this rule closes. Let the node catch up.
>
> **Getting back to IDLE:** `setfsmstate --fsmstate idle` works from both RUNNING
> and CATCHINGBLOCKS. From CATCHINGBLOCKS it records the operator's intent to park
> the node; it does not cancel the catchup batch already in progress. That batch
> keeps validating blocks under IDLE with the catchup safeguards still in force,
> since they apply in every state except RUNNING: nothing is fed to block
> assembly, peer subtrees are ignored, and rejected-transaction messages are not
> published. Its final automatic promotion to RUNNING is refused, so the node
> stays in IDLE. In legacy sync mode
> block download is not FSM-gated and continues. Stop the services promptly, and
> always before destructive recovery such as `rewindblockchain`: IDLE alone does
> not guarantee that no work is in flight.

### Resuming from IDLE

Choose the resume path by how far the node is behind:

- **Needs to synchronize** (below the highest checkpoint, or behind its peers):
  `setfsmstate --fsmstate catchingblocks`. The node promotes itself to RUNNING
  once catchup completes.
- **Already at the tip**: `setfsmstate --fsmstate running`. Resuming an at-tip
  node with `catchingblocks` can leave it in CATCHINGBLOCKS with no catchup work
  to trigger the promotion to RUNNING.

## Validation

After each state change, verify the new state:

1. **Admin Dashboard**: View the current state in the FSM State section
2. **teranode-cli**: Use `getfsmstate` command (see above)
3. **Logs**: Check the logs for transition messages
4. **Services**: Verify that expected services are running/stopped according to the state

## Advanced Method: Using grpcurl

For advanced users or automated scripts, you can use `grpcurl` directly. This method requires network access to the blockchain gRPC service on port 18087. Prefer `teranode-cli` inside a Teranode container when you can: it already has the key.

Every Blockchain RPC except `HealthGRPC` requires the `x-api-key` header, and server reflection is off by default. Run grpcurl from a Teranode source checkout so it can load the service definition, and pass the same `grpc_admin_api_key` the services use:

```bash
# Run from the root of a Teranode source checkout
fsm() {
  grpcurl -plaintext -H "x-api-key: $grpc_admin_api_key" \
    -import-path . -proto services/blockchain/blockchain_api/blockchain_api.proto "$@"
}
```

A missing or wrong key returns `Unauthenticated`.

### Docker Compose Environment

Access the blockchain gRPC service directly:

**Check Current State:**

```bash
# Connect to blockchain service on port 18087
fsm blockchain:18087 blockchain_api.BlockchainAPI.GetFSMCurrentState
```

**Trigger State Transitions:**

```bash
# Transition to RUNNING state
fsm -d '{"event":"RUN"}' blockchain:18087 blockchain_api.BlockchainAPI.SendFSMEvent

# Transition to CATCHINGBLOCKS state
fsm -d '{"event":"CATCHUPBLOCKS"}' blockchain:18087 blockchain_api.BlockchainAPI.SendFSMEvent

# Transition to IDLE state
fsm blockchain:18087 blockchain_api.BlockchainAPI.Idle
```

### Kubernetes Environment

Port-forward the blockchain service:

```bash
# Port forward the blockchain gRPC service
kubectl port-forward -n teranode-operator service/blockchain 18087:18087
```

**Check Current State:**

```bash
fsm localhost:18087 blockchain_api.BlockchainAPI.GetFSMCurrentState
```

Expected output for a fresh Kubernetes operator deployment (a restarted node
normally reports its persisted state):

```json
{
  "state": "IDLE"
}
```

**Trigger State Transitions:**

```bash
# Transition to RUNNING state
fsm -d '{"event":"RUN"}' localhost:18087 blockchain_api.BlockchainAPI.SendFSMEvent

# Transition to CATCHINGBLOCKS state
fsm -d '{"event":"CATCHUPBLOCKS"}' localhost:18087 blockchain_api.BlockchainAPI.SendFSMEvent

# Transition to IDLE state
fsm localhost:18087 blockchain_api.BlockchainAPI.Idle
```

### Wait for State Change

There is no blocking "wait" endpoint. To wait for a specific state, poll the current state until it matches. Bound the loop and stop on a grpcurl error, so a bad key or an unreachable service fails instead of polling forever:

```bash
# Poll for up to 120 seconds until the FSM reaches RUNNING
for i in $(seq 1 120); do
  out=$(fsm localhost:18087 blockchain_api.BlockchainAPI.GetFSMCurrentState) || exit 1
  echo "$out" | grep -q '"state": "RUNNING"' && exit 0
  sleep 1
done
exit 1
```

## Further Reading

- [How To Interact With the RPC Server](minersHowToInteractWithRPCServer.md)
- [State Management Documentation](../../topics/architecture/stateManagement.md)
