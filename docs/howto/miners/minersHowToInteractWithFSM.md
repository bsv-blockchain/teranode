# How to Manage Teranode States Using RPC

This guide explains how to change and monitor Teranode's state using gRPC commands. Note that Teranode instances start in IDLE state and require manual state transitions.

## Prerequisites

- Access to a running Teranode instance
- One of the following:
    - `grpcurl` installed on your system (requires network access to the RPC Server on port 18087)
    - Access to the `teranode-cli` (recommended, requires direct access to RPC container)

## Methods

You can manage Teranode states using either `teranode-cli` (recommended) or `grpcurl` directly. The exact commands will depend on your deployment environment (Docker Compose or Kubernetes).

### Using teranode-cli (Recommended)

#### Docker Compose Environment

##### 1. Check Current State
```bash
docker exec -it blockchain teranode-cli getfsmstate
```

##### 2. Set New State

```bash
docker exec -it blockchain teranode-cli setfsmstate --fsmstate RUNNING
```

#### Kubernetes Environment

##### 1. Check Current State

Access any Teranode pod and use teranode-cli directly:

```bash
# Get the name of a pod (blockchain or asset are good options)
kubectl get pods -n teranode-operator -l app=blockchain

# Access the pod and run the command
kubectl exec -it <pod-name> -n teranode-operator -- teranode-cli getfsmstate

# Alternative one-liner
kubectl exec -it $(kubectl get pods -n teranode-operator -l app=blockchain -o jsonpath='{.items[0].metadata.name}') -n teranode-operator -- teranode-cli getfsmstate
```

##### 2. Set New State

```bash
# Change state to RUNNING
kubectl exec -it $(kubectl get pods -n teranode-operator -l app=blockchain -o jsonpath='{.items[0].metadata.name}') -n teranode-operator -- teranode-cli setfsmstate --fsmstate RUNNING
```

### Valid FSM States

The following states are valid for all environments:
- IDLE
- RUNNING
- LEGACYSYNCING
- CATCHINGBLOCKS


### Using grpcurl

#### Docker Compose Environment

##### 1. Check Current State

To check the current state of Teranode in a Docker Compose environment:

```bash
grpcurl -plaintext rpcserver:18087 blockchain_api.BlockchainAPI.GetFSMCurrentState
```

#### Kubernetes Environment

##### 1. Check Current State

For Kubernetes, you need to forward the RPC server port or access it through a service:

```bash
# Port forward the RPC service
kubectl port-forward service/rpcserver -n teranode-operator 18087:18087

# In a new terminal
grpcurl -plaintext localhost:18087 blockchain_api.BlockchainAPI.GetFSMCurrentState
```

Expected output:
```json
{
"state": "Idle"
}
```

##### 2. Send State Transition Events (Docker Compose)

To start Teranode's normal operations in Docker Compose:

```bash
grpcurl -plaintext rpcserver:18087 blockchain_api.BlockchainAPI.Run
```

##### 2. Send State Transition Events (Kubernetes)

In a Kubernetes environment with port forwarding active:

```bash
grpcurl -plaintext localhost:18087 blockchain_api.BlockchainAPI.Run
```

#### Available Events

The following events are available for both environments:
- `Run` - Transitions to RUNNING state
- `LegacySync` - Transitions to LEGACYSYNCING state
- `CatchUpBlocks` - Transitions to CATCHINGBLOCKS state
- `Idle` - Transitions to IDLE state


#### 3. Wait for State Change

To wait for a specific state transition:

```bash
grpcurl -plaintext -d '{"state":"Running"}' rpcserver:18087 blockchain_api.BlockchainAPI.WaitForFSMtoTransitionToGivenState
```

## Validation

After each state change, verify the new state:

1. Use the "get current state" command (see instructions above for `grpcurl` or `teranode-cli`)
2. Check the logs for transition messages
3. Verify that expected services are running/stopped according to the state


## Further Reading

- [How To Interact With the RPC Server](minersHowToInteractWithRPCServer.md)
- [State Management Documentation](../../topics/architecture/stateManagement.md)
