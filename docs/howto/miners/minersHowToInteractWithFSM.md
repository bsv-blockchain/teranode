# How to Manage Teranode States Using RPC

This guide explains how to change and monitor Teranode's state using gRPC commands. Note that Teranode instances start in IDLE state and require manual state transitions.

## Prerequisites

- Access to a running Teranode instance
- One of the following:
    - `grpcurl` installed on your system (requires network access to the RPC Server on port 18087)
    - Access to the `teranode-cli` (recommended, requires direct access to RPC container)

## Methods

You can manage Teranode states using either `teranode-cli` (recommended) or `grpcurl` directly.

### Using teranode-cli (Recommended)

#### 1. Check Current State
```bash
docker exec -it blockchain teranode-cli getfsmstate
```

#### 2. Set New State

```bash
docker exec -it blockchain teranode-cli setfsmstate --fsmstate RUNNING
```

Valid states are:
- IDLE
- RUNNING
- LEGACYSYNCING
- CATCHINGBLOCKS


### Using grpcurl


#### 1. Check Current State

To check the current state of Teranode:

```bash
grpcurl -plaintext rpcserver:18087 blockchain_api.BlockchainAPI.GetFSMCurrentState
```

Expected output:
```json
{
"state": "Idle"
}
```

#### 2. Send State Transition Events

To start Teranode's normal operations:

```bash
grpcurl -plaintext rpcserver:18087 blockchain_api.BlockchainAPI.Run
```

Available events:
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
