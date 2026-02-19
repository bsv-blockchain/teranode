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

```text
Node Instance
├── Blockchain Service
├── Block Assembly Service
├── Validator Service
├── Propagation Service
├── RPC Service
├── P2P Service
├── Block Store (Blob Store)
├── UTXO Store (Aerospike)
└── Subtree Store (Blob Store)
```

This architecture allows:

- Independent service control
- Isolated failure testing
- Clear responsibility boundaries
- Flexible configuration

## Test Categories Explained

The test categories align with Teranode's functional requirements, ensuring comprehensive coverage of all node responsibilities.

### Main Node Activity Tests

#### TNA (Node Responsibilities)

Tests core responsibilities from Bitcoin White Paper Section 5:

- Broadcasting new transactions to all nodes
- Collecting transactions into blocks
- Finding proof-of-work for blocks
- Network message propagation
- Basic node operations

Why it matters: These tests ensure nodes fulfill their fundamental network responsibilities.

#### TNB (Receive and Validate Transactions)

Tests transaction reception and validation:

- Extended Format transaction handling
- Consensus rule validation
- UTXO verification
- Script validation
- Fee calculation
- Transaction batching and processing

Why it matters: Transaction validation is critical for maintaining network consensus and preventing double-spends.

#### TNC (Assemble Blocks)

Tests block assembly functionality:

- Building candidate blocks from validated transactions
- Merkle tree calculation and validation
- Coinbase transaction creation
- Managing multiple candidate blocks
- Subtree verification

Why it matters: Proper block assembly ensures valid block creation and mining operations.

#### TND (Propagate Blocks to Network)

Tests block propagation:

- Broadcasting solved blocks to other nodes
- Block announcement protocols
- Network-wide block distribution

Why it matters: Ensures blocks are efficiently distributed across the network.

#### TNE (Receive and Validate Blocks)

Tests block reception and validation:

- Downloading blocks from other nodes
- Block validation against consensus rules
- Transaction verification within blocks
- Chain acceptance decisions

Why it matters: Ensures nodes can properly validate and accept blocks from the network.

### Supporting Infrastructure Tests

#### TNF (Keep Track of Longest Honest Chain)

Tests chain management:

- Maintaining awareness of the longest valid chain
- Chain synchronization
- Fork tracking and resolution
- Chain reorganization handling

Why it matters: Ensures nodes follow the longest honest chain as per Bitcoin protocol.

#### TNJ (Adherence to Standard Policies - Consensus Rules)

Tests consensus rule compliance:

- Genesis rule adherence
- Block size limits
- Difficulty adjustment
- Transaction validation rules
- Locktime validation
- Script interpretation
- Edge cases in consensus rules

Why it matters: Validates strict adherence to Bitcoin protocol consensus rules.

### Error Handling Tests

#### TEC (Error Cases)

Verifies system resilience:

- Service failures
- Network partitions
- Resource exhaustion
- Recovery procedures
- Invalid block handling
- Service restart scenarios
- Malformed data handling

Why it matters: A production system must handle failures gracefully and recover automatically.

### E2E (End-to-End Tests)

Full system integration tests:

- Multi-node scenarios
- Complete transaction lifecycle
- Block propagation across network
- RPC interface testing
- Legacy node compatibility

Why it matters: Validates the entire system working together in realistic scenarios.

### Consensus Tests

Bitcoin protocol consensus validation:

- Script interpreter tests
- Transaction validation rules
- Signature verification
- Opcode execution
- BIP implementations

Why it matters: Ensures compliance with Bitcoin consensus rules.

## Understanding Test Flow

### Test Lifecycle

1. **Setup**

    ```text
    Initialize Framework
    ├── Create Docker environment
    ├── Start services
    ├── Initialize clients
    └── Verify health
    ```

2. **Execution**

    ```text
    Run Test
    ├── Prepare test state
    ├── Execute test actions
    ├── Verify results
    └── Cleanup
    ```

3. **Teardown**

    ```text
    Cleanup
    ├── Stop services
    ├── Remove containers
    ├── Clean data
    └── Release resources
    ```

## Running Tests

### Makefile Targets

The project provides several make targets for different test scenarios:

```bash
# Run unit tests (excludes test/ directory)
make test

# Run long-running tests
make longtest

# Run sequential tests (one by one, in order)
make sequentialtest

# Run smoke tests (basic functionality)
make smoketest

# Run all tests
make testall
```

### Running Specific Test Categories

Tests can be run with specific build tags:

```bash
# Run TNA tests
go test -v -tags "test_tna" ./test/tna

# Run TNB tests
go test -v ./test/tnb

# Run TEC tests
go test -v -tags "test_tec" ./test/tec

# Run a specific test
go test -v -run "^TestTNA1TestSuite/TestBroadcastNewTxAllNodes$" -tags test_tna ./test/tna/tna1_test.go

# Run E2E daemon tests
go test -v -race -timeout=5m ./test/e2e/daemon/...

# Run consensus tests
go test -v ./test/consensus/...
```

### Test Configuration

#### TConfig System

Tests use a configuration system (`tconfig`) to manage test environments:

- **Settings Contexts**: Different configuration profiles (e.g., `docker.ci`, `test`, `dev.system.test`)
- **Docker Compose Files**: Multiple compose files can be combined for different test scenarios
- **Node Configuration**: Each test can specify which nodes and services to use

Example configuration in test:

```go
TConfig: tconfig.LoadTConfig(
    map[string]any{
        tconfig.KeyTeranodeContexts: []string{
            "docker.teranode1.test.tna1Test",
            "docker.teranode2.test.tna1Test",
            "docker.teranode3.test.tna1Test",
        },
    },
)
```

### Test Data Management

#### Seed Data

The framework includes pre-configured blockchain state for testing:

- Location: `test/utils/data/seed/`
- Contains UTXO sets and headers for known blockchain states
- Used to initialize tests with consistent starting conditions

#### Test Isolation

Each test run creates its own data directory:

- Pattern: `test/data/test/{TEST_ID}`
- Automatically cleaned up after test completion
- Ensures tests don't interfere with each other

### Test Utilities and Helpers

#### TeranodeTestSuite

Base test suite providing common functionality:

- Multi-node setup and teardown
- Service client initialization
- Logging configuration
- Docker compose management

#### TeranodeTestEnv

Test environment management:

- Node client access
- Service health checks
- Shared storage for test coordination
- Network configuration

#### Helper Functions

Common test operations:

- `GetBlockHeight()` - Retrieve current blockchain height
- `CallRPC()` - Make RPC calls with retry logic
- `GetMiningCandidate()` - Get mining candidate from block assembly
- Transaction creation and signing utilities

## Other Resources

- [Functional Requirement Tests Guide](./functionalRequirementTests.md) - Detailed guide on how functional tests map to Teranode requirements
- [Teranode Daemon Reference](../references/teranodeDaemonReference.md) - Guide to using the test daemon for integration testing
- [Automated Testing How-To](../howto/automatedTestingHowTo.md)
- [Testing Technical Reference](../references/testingTechnicalReference.md)
