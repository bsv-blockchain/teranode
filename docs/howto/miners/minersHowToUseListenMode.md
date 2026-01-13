# How to Use Listen Mode for Mining Operations

## Overview

Listen Mode is a special operational mode for Teranode that allows nodes to receive blockchain data without broadcasting or propagating information to the network. This mode is particularly useful for mining operations that need to stay synchronized with the network while maintaining a low profile or reducing network traffic.

## What is Listen Mode?

Listen Mode configures a Teranode instance to:

- **Receive** all network messages (blocks, transactions, subtrees)
- **Process** incoming blockchain data normally
- **Skip** outbound message propagation
- **Maintain** peer connections through handshakes only

This creates a "silent observer" node that stays synchronized without contributing to network message propagation.

## Use Cases for Miners

### 1. Mining Pool Monitoring Nodes

- Monitor blockchain state without affecting network traffic
- Reduce bandwidth costs for monitoring-only nodes
- Maintain multiple observer nodes without network overhead

### 2. Development and Testing

- Test mining software without propagating test blocks
- Monitor network behavior without participation
- Debug connectivity issues in isolation

### 3. Geographic Distribution

- Deploy lightweight monitoring nodes in multiple regions
- Reduce cross-region bandwidth usage
- Maintain global network visibility with minimal overhead

### 4. Backup and Failover Nodes

- Keep backup nodes synchronized without active participation
- Quick failover capability when needed
- Reduced resource usage for standby nodes

## Configuration

### Setting Listen Mode

Add the following setting:

```conf
# Enable listen-only mode
listen_mode = listen_only

# Default mode (normal operation)
# listen_mode = full
```

### Available Modes

| Mode | Description | Use Case |
|------|-------------|----------|
| `full` | Normal operation - receives and sends all messages | Active mining nodes |
| `listen_only` | Receives messages only - no outbound propagation | Monitoring, backup nodes |

### Environment Variable

You can also set listen mode via environment variable:

```bash
export listen_mode=listen_only
```

## Behavior in Listen Mode

### What Still Works

1. **Peer Discovery and Connections**

    - Handshake messages are still sent (required for peer connections)
    - Node maintains connections to peers
    - Receives peer announcements

2. **Data Reception**

    - Receives all blocks
    - Receives all transactions
    - Receives all subtrees
    - Processes blockchain updates normally

3. **Local Operations**

    - RPC interface remains fully functional
    - Mining operations work normally
    - Block validation continues
    - UTXO management unchanged

### What is Disabled

1. **Network Propagation**

    - Does not broadcast new blocks
    - Does not propagate transactions
    - Does not announce subtrees
    - Does not relay peer messages

2. **P2P Participation**

    - Does not contribute to network topology
    - Does not help with message distribution
    - Minimal bandwidth usage

## Mining Considerations

### For Mining Pools

When running a mining pool with listen mode:

1. **Primary Mining Node**: Run in `full` mode

    - This node submits mined blocks
    - Handles transaction propagation
    - Participates fully in the network

2. **Monitor Nodes**: Run in `listen_only` mode

    - Track blockchain state
    - Provide redundancy
    - Reduce operational costs

3. **Configuration Example**:

    ```conf
    # Primary miner (full mode)
    listen_mode = full
    p2p_port = 9906

    # Monitor nodes (listen only)
    listen_mode = listen_only
    p2p_port = 9907  # Different port for each monitor
    ```

### For Solo Miners

Solo miners typically should NOT use listen mode on their primary mining node because:

- Mined blocks won't be propagated
- You cannot claim mining rewards
- Your blocks won't be recognized by the network

Use listen mode only for:

- Secondary monitoring nodes
- Development and testing
- Network analysis

## Best Practices

### 1. Network Architecture

```text
                    ┌─────────────┐
                    │   Internet  │
                    └─────┬───────┘
                          │
                    ┌─────┴───────┐
                    │ Primary Node │ (full mode)
                    │  (Mining)    │
                    └─────┬───────┘
                          │
        ┌─────────────────┼─────────────────┐
        │                 │                 │
  ┌─────┴──────┐   ┌─────┴──────┐   ┌─────┴──────┐
  │Monitor Node│   │Monitor Node│   │Monitor Node│
  │(listen_only)│  │(listen_only)│  │(listen_only)│
  └────────────┘   └────────────┘   └────────────┘
```

### 2. Switching Modes

To switch from listen mode to full mode:

1. Stop the node

2. Update configuration:

    ```conf
    listen_mode = full
    ```

3. Restart the node

4. Verify full participation via logs

## Conclusion

Listen Mode provides miners with a powerful configuration option for:

- Reducing operational costs
- Improving network monitoring
- Enhancing security posture
- Optimizing resource usage

Use it wisely as part of a comprehensive mining infrastructure strategy, ensuring your primary mining nodes always run in full mode to participate in the network and claim rewards.
