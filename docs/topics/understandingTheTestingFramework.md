# Understanding the TERANODE Testing Framework

## Architectural Philosophy

### Why a Custom Test Framework?
Bitcoin SV node testing presents unique challenges that standard testing frameworks can't fully address:

1. **Complex Distributed Systems**
- Multiple nodes need to interact
- Network conditions must be simulated
- State must be synchronized across components
- Services need to be orchestrated independently

2. **State Management**
- Blockchain state must be maintained consistently
- UTXO set needs to be tracked
- Multiple data stores must stay synchronized
- Test state needs to be cleanly reset between runs

3. **Real-world Scenarios**
- Network partitions
- Node failures
- Service degradation
- Race conditions
- Data consistency challenges

### Core Design Principles

1. **Isolation**
- Each test runs in its own containerized environment
- State is cleanly reset between tests
- Network conditions can be controlled
- Services can be manipulated independently

2. **Observability**
- Comprehensive logging across all components
- Health checks for all services
- Clear error reporting
- State inspection capabilities

3. **Reproducibility**
- Deterministic test environments
- Consistent initial states
- Controlled timing and sequencing
- Docker-based setup ensures consistency

## Testing Strategy

### Multi-Node Testing Architecture
The framework uses three nodes by default because this is the minimum number needed to test:

1. **Network Consensus**
- Majority agreement (2 out of 3)
- Fork resolution
- Chain selection

2. **Network Propagation**
- Transaction broadcasting
- Block propagation
- Peer discovery
- Network partitioning scenarios

3. **Failure Scenarios**
- Node isolation
- Service degradation
- Recovery processes
- State synchronization

### Service Organization

```
Node Instance
├── Blockchain Service
├── Coinbase Service
├── Block Assembly Service
├── Distribution Service
├── Block Store
├── UTXO Store
└── Subtree Store
```

This architecture allows:
- Independent service control
- Isolated failure testing
- Clear responsibility boundaries
- Flexible configuration

## Test Categories Explained

### TNA (Node Responsibilities)
Tests verify fundamental node behavior:
- Transaction broadcasting
- Block collection
- Network communication
- Basic node operations

Why it matters: These tests ensure the basic functionality that makes a node part of the network.

### TNB (Transaction Validation)
Focuses on:
- Transaction format verification
- Script validation
- Fee calculation
- UTXO validation

Why it matters: Transaction validation is critical for maintaining network consensus and preventing double-spends.

### TNC (Block Assembly)
Tests cover:
- Merkle tree construction
- Transaction ordering
- Coinbase creation
- Block template generation

Why it matters: Proper block assembly ensures valid block creation and mining operations.

### TEC (Error Cases)
Verifies system resilience:
- Service failures
- Network partitions
- Resource exhaustion
- Recovery procedures

Why it matters: A production system must handle failures gracefully and recover automatically.

## Understanding Test Flow

### Test Lifecycle
1. **Setup**
```
Initialize Framework
├── Create Docker environment
├── Start services
├── Initialize clients
└── Verify health
```

2. **Execution**
```
Run Test
├── Prepare test state
├── Execute test actions
├── Verify results
└── Cleanup
```

3. **Teardown**
```
Cleanup
├── Stop services
├── Remove containers
├── Clean data
└── Release resources
```

## Other Resources

- [QA Guide & Instructions for Functional Requirement Tests](../../test/README.md)
- [Automated Testing How-To](../howto/automatedTestingHowTo.md)
- [Testing Technical Reference](../references/testingTechnicalReference.md)
