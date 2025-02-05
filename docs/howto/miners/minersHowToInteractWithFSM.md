# How to Manage Teranode States Using RPC

This guide explains how to change and monitor Teranode's state using gRPC commands.

## Prerequisites

- Access to a running Teranode instance
- `grpcurl` installed on your system
- Network access to the RPC server (default port: 18087)

## Steps

### 1. Check Current State

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

### 2. Transition to Running State

To start Teranode's normal operations:

```bash
grpcurl -plaintext rpcserver:18087 blockchain_api.BlockchainAPI.Run
```

### 3. Wait for State Change

To wait for a specific state transition:

```bash
grpcurl -plaintext -d '{"state":"Running"}' rpcserver:18087 blockchain_api.BlockchainAPI.WaitForFSMtoTransitionToGivenState
```

## Validation

After each state change, verify the new state:

1. Use the GetFSMCurrentState command
2. Check the logs for transition messages
3. Verify that expected services are running/stopped according to the state


## Further Reading

- [How To Interact With the RPC Server](minersHowToInteractWithRPCServer.md)
- [State Management Documentation](../../topics/architecture/stateManagement.md)
