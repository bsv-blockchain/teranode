# How to Manage Teranode States

This guide explains how to change and monitor Teranode's state. Note that Teranode instances start in IDLE state and require manual state transitions.

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
- LEGACYSYNCING
- CATCHINGBLOCKS

## Validation

After each state change, verify the new state:

1. **Admin Dashboard**: View the current state in the FSM State section
2. **teranode-cli**: Use `getfsmstate` command (see above)
3. **Logs**: Check the logs for transition messages
4. **Services**: Verify that expected services are running/stopped according to the state

## Advanced Method: Using grpcurl

For advanced users or automated scripts, you can use `grpcurl` directly. This method requires network access to the blockchain gRPC service on port 18087.

### Docker Compose Environment

Access the blockchain gRPC service directly:

**Check Current State:**

```bash
# Connect to blockchain service on port 18087
grpcurl -plaintext blockchain:18087 blockchain_api.BlockchainAPI.GetFSMCurrentState
```

**Trigger State Transitions:**

```bash
# Transition to RUNNING state
grpcurl -plaintext blockchain:18087 blockchain_api.BlockchainAPI.Run

# Transition to LEGACYSYNCING state
grpcurl -plaintext blockchain:18087 blockchain_api.BlockchainAPI.LegacySync

# Transition to CATCHINGBLOCKS state
grpcurl -plaintext blockchain:18087 blockchain_api.BlockchainAPI.CatchUpBlocks

# Transition to IDLE state
grpcurl -plaintext blockchain:18087 blockchain_api.BlockchainAPI.Idle
```

### Kubernetes Environment

Port-forward the blockchain service:

```bash
# Port forward the blockchain gRPC service
kubectl port-forward -n teranode-operator service/blockchain 18087:18087
```

**Check Current State:**

```bash
grpcurl -plaintext localhost:18087 blockchain_api.BlockchainAPI.GetFSMCurrentState
```

Expected output:

```json
{
  "state": "Idle"
}
```

**Trigger State Transitions:**

```bash
# Transition to RUNNING state
grpcurl -plaintext localhost:18087 blockchain_api.BlockchainAPI.Run

# Transition to LEGACYSYNCING state
grpcurl -plaintext localhost:18087 blockchain_api.BlockchainAPI.LegacySync

# Transition to CATCHINGBLOCKS state
grpcurl -plaintext localhost:18087 blockchain_api.BlockchainAPI.CatchUpBlocks

# Transition to IDLE state
grpcurl -plaintext localhost:18087 blockchain_api.BlockchainAPI.Idle
```

### Wait for State Change

You can wait for a specific state transition to complete:

```bash
grpcurl -plaintext -d '{"state":"Running"}' localhost:18087 blockchain_api.BlockchainAPI.WaitFSMToTransitionToGivenState
```

## Further Reading

- [How To Interact With the RPC Server](minersHowToInteractWithRPCServer.md)
- [State Management Documentation](../../topics/architecture/stateManagement.md)
