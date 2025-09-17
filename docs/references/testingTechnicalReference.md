# TERANODE Testing Framework Technical Reference

## Framework Components

### TeranodeTestEnv Structure

```go
type TeranodeTestEnv struct {
    TConfig              tconfig.TConfig       // Test configuration
    Context              context.Context      // Test context
    Compose              tc.ComposeStack      // Docker compose stack
    ComposeSharedStorage tstore.TStoreClient  // Shared storage client
    Nodes                []TeranodeTestClient // Array of test nodes
    LegacyNodes          []SVNodeTestClient   // Array of legacy nodes
    Logger               ulogger.Logger       // Framework logger
    Cancel               context.CancelFunc   // Context cancellation
    Daemon               daemon.Daemon        // Daemon instance
}
```

### TeranodeTestClient Structure

```go
type TeranodeTestClient struct {
    Name                string                  // Node identifier
    SettingsContext     string                  // Configuration context
    BlockchainClient    bc.ClientI              // Blockchain service client
    BlockassemblyClient ba.Client               // Block assembly client
    DistributorClient   distributor.Distributor // Distribution service client
    ClientBlockstore    *bhttp.HTTPStore        // HTTP client for block storage
    ClientSubtreestore  *bhttp.HTTPStore        // HTTP client for subtree storage
    UtxoStore           *utxostore.Store        // UTXO storage
    CoinbaseClient      *stubs.CoinbaseClient   // Coinbase service client stub
    AssetURL            string                  // Asset service URL
    RPCURL              string                  // RPC service URL
    IPAddress           string                  // Node IP address
    SVNodeIPAddress     string                  // Legacy node IP address
    Settings            *settings.Settings      // Node settings
    BlockChainDB        bcs.Store               // Blockchain storage
}
```

### SVNodeTestClient Structure

```go
type SVNodeTestClient struct {
    Name      string // Node identifier
    IPAddress string // Node IP address
}
```

## Framework Setup and Usage

### Creating a Test Environment

The test environment is created using the `NewTeraNodeTestEnv` function, which accepts a test configuration:

```go
func NewTeraNodeTestEnv(c tconfig.TConfig) *TeranodeTestEnv {
    logger := ulogger.New("e2eTestRun", ulogger.WithLevel(c.Suite.LogLevel))
    ctx, cancel := context.WithCancel(context.Background())

    return &TeranodeTestEnv{
        TConfig: c,
        Context: ctx,
        Logger:  logger,
        Cancel:  cancel,
    }
}
```

### Setting Up Docker Nodes

The `SetupDockerNodes` method initializes the Docker Compose environment with the provided settings:

```go
func (t *TeranodeTestEnv) SetupDockerNodes() error {
    // Set up Docker Compose environment with provided settings
    // Create test directory for test-specific data
    // Configure environment settings including TEST_ID
    // Set up shared storage client for local docker-compose
    // Initialize teranode and legacy node configurations
}
```

### Initializing Node Clients

The `InitializeTeranodeTestClients` method sets up all the necessary client connections for the nodes:

```go
func (t *TeranodeTestEnv) InitializeTeranodeTestClients() error {
    // Set up blob stores for block and subtree data
    // Retrieve IP addresses for containers
    // Set up RPC and Asset service URLs
    // Initialize blockchain, blockassembly and distributor clients
    // Configure UTXO stores and other required connections
    // Connect to necessary services for testing
}
```

## Using the Framework in Tests

A typical test using this framework would follow this pattern:

```go
func TestSomeFunctionality(t *testing.T) {
    // Create test configuration
    config := tconfig.NewTConfig("testID")

    // Create test environment
    env := utils.NewTeraNodeTestEnv(config)

    // Set up Docker nodes
    err := env.SetupDockerNodes()
    require.NoError(t, err)

    // Initialize node clients
    err = env.InitializeTeranodeTestClients()
    require.NoError(t, err)

    // Run tests using the environment
    // Access node clients via env.Nodes[i]

    // Clean up when done
    env.Cancel()
}
```

## Test Categories and Tags

### Core Test Categories

| Category | Tag | Description |
|----------|-----|-------------|
| TNA | tnatests | Node responsibilities and network communication |
| TNB | tnbtests | Transaction validation and processing |
| TND | tndtests | Block propagation through network |
| TNF | tnftests | Longest chain management |
| TNJ | tnjtests | Consensus rules compliance |
| TEC | tectests | Error handling and recovery scenarios |

### Service Port Mappings

| Service | Internal Port | External Port Pattern |
|---------|--------------|----------------------|
| Health Check | 8090/tcp | 1009X |
| Coinbase GRPC | 8093/tcp | 1009X |
| Blockchain GRPC | 8087/tcp | 1208X |
| Block Assembly GRPC | 8085/tcp | 1408X |
| Propagation GRPC | 8084/tcp | 1608X |
| Asset HTTP | 8091/tcp | Variable |
| RPC | 8092/tcp | Variable |

## Configuration Reference

### Environment Variables

```bash
# Core Settings
SETTINGS_CONTEXT      # Configuration context for the test run
LOG_LEVEL            # Logging verbosity (DEBUG, INFO, WARN, ERROR)
GITHUB_ACTIONS       # CI environment indicator

# Node Settings
SETTINGS_CONTEXT_1   # Configuration for node 1
SETTINGS_CONTEXT_2   # Configuration for node 2
SETTINGS_CONTEXT_3   # Configuration for node 3
```

### Docker Compose Configuration

```yaml
# Base Configuration (docker-compose.e2etest.yml)
services:
  teranode1:
    image: teranode
    environment:

      - SETTINGS_CONTEXT=${SETTINGS_CONTEXT_1}
    ports:

      - "10090:8090"  # Health check port
      - "10093:8093"  # Coinbase service
      - "10087:8087"  # Blockchain service
      - "10085:8085"  # Block assembly
      - "10091:8091"  # Asset service
      - "10092:8092"  # RPC service

  teranode2:
    # Similar configuration with ports 12090, 12093, 12087, etc.

  teranode3:
    # Similar configuration with ports 14090, 14093, 14087, etc.
```

### Test Suite Configuration

```go
// Default Compose Files
func (suite *BitcoinTestSuite) DefaultComposeFiles() []string {
    return []string{"../../docker-compose.e2etest.yml"}
}

// Default Settings Map
func (suite *BitcoinTestSuite) DefaultSettingsMap() map[string]string {
    return map[string]string{
        "SETTINGS_CONTEXT_1": "docker.ci.teranode1.tc1",
        "SETTINGS_CONTEXT_2": "docker.ci.teranode2.tc1",
        "SETTINGS_CONTEXT_3": "docker.ci.teranode3.tc1",
    }
}
```

## Utility Methods

### Node Management Methods

```go
// StartNode starts a specific TeraNode by name
func (t *TeranodeTestEnv) StartNode(nodeName string) error {
    // Start a specific node in the Docker Compose environment
    // Wait for the node to be healthy
}

// StopNode stops a specific TeraNode by name
func (t *TeranodeTestEnv) StopNode(nodeName string) error {
    // Stop a specific node in the Docker Compose environment
}

// RestartDockerNodes restarts the Docker Compose services
func (t *TeranodeTestEnv) RestartDockerNodes(envSettings map[string]string) error {
    // Stop the current Docker Compose environment
    // Re-initialize with potentially new settings
    // Restart the services with the updated configuration
}

// StopDockerNodes stops the Docker Compose services and removes volumes
func (t *TeranodeTestEnv) StopDockerNodes() error {
    // Stop all services and clean up resources
}
```

### Client Setup Methods

```go
// Sets up HTTP stores for blocks and subtrees
func (t *TeranodeTestEnv) setupBlobStores() error {
    // Create HTTP clients for blob stores
    // Configure block and subtree storage access
}

// Sets up blockchain client for a node
func (t *TeranodeTestEnv) setupBlockchainClient(node *TeranodeTestClient) error {
    // Initialize gRPC connection to blockchain service
    // Create and configure blockchain client
}

// Sets up block assembly client for a node
func (t *TeranodeTestEnv) setupBlockassemblyClient(node *TeranodeTestClient) error {
    // Initialize gRPC connection to block assembly service
    // Create and configure block assembly client
}

// Sets up distributor client for a node
func (t *TeranodeTestEnv) setupDistributorClient(node *TeranodeTestClient) error {
    // Initialize gRPC connection to distributor service
    // Create and configure distributor client
}

// GetMappedPort retrieves the mapped port for a service running in Docker Compose
func (t *TeranodeTestEnv) GetMappedPort(nodeName string, port nat.Port) (nat.Port, error) {
    // Find the exposed port mapping for a container service
}
```

### Transaction Utilities

```go
// CreateAndSendTx creates and sends a transaction
func (n *TeranodeTestClient) CreateAndSendTx(t *testing.T, ctx context.Context, parentTx *bt.Tx) (*bt.Tx, error) {
    // Create a new transaction spending outputs from the parent transaction
    // Sign the transaction with test keys
    // Submit the transaction to the node
    // Return the created transaction
}

// CreateAndSendTxs creates and sends a chain of transactions
func (n *TeranodeTestClient) CreateAndSendTxs(t *testing.T, ctx context.Context, parentTx *bt.Tx, count int) ([]*bt.Tx, []*chainhash.Hash, error) {
    // Create a series of chained transactions
    // Each transaction spends outputs from the previous one
    // Submit all transactions to the node
    // Return the created transactions and their hashes
}

// CreateAndSendTxsConcurrently creates and sends transactions concurrently
func (n *TeranodeTestClient) CreateAndSendTxsConcurrently(t *testing.T, ctx context.Context, parentTx *bt.Tx, count int) ([]*bt.Tx, []*chainhash.Hash, error) {
    // Create multiple transactions concurrently
    // Use goroutines to parallelize transaction creation and submission
    // Wait for all transactions to complete
    // Return the created transactions and their hashes
}
```

## API Reference

### Framework Methods
```go
// Core Framework Methods
func NewTeraNodeTestEnv(c tconfig.TConfig) *TeranodeTestEnv
func (t *TeranodeTestEnv) SetupDockerNodes() error
func (t *TeranodeTestEnv) InitializeTeranodeTestClients() error
func (t *TeranodeTestEnv) StopDockerNodes() error
func (t *TeranodeTestEnv) RestartDockerNodes(envSettings map[string]string) error
func (t *TeranodeTestEnv) StartNode(nodeName string) error
func (t *TeranodeTestEnv) StopNode(nodeName string) error
func (t *TeranodeTestEnv) GetMappedPort(nodeName string, port nat.Port) (nat.Port, error)
func (t *TeranodeTestEnv) GetContainerIPAddress(node *TeranodeTestClient) error
func (t *TeranodeTestEnv) GetLegacyContainerIPAddress(node *SVNodeTestClient) error

// Transaction Utility Methods
func (n *TeranodeTestClient) CreateAndSendTx(t *testing.T, ctx context.Context, parentTx *bt.Tx) (*bt.Tx, error)
func (n *TeranodeTestClient) CreateAndSendTxs(t *testing.T, ctx context.Context, parentTx *bt.Tx, count int) ([]*bt.Tx, []*chainhash.Hash, error)
func (n *TeranodeTestClient) CreateAndSendTxsConcurrently(t *testing.T, ctx context.Context, parentTx *bt.Tx, count int) ([]*bt.Tx, []*chainhash.Hash, error)
```

### Client Interfaces

#### Blockchain Client
```go
type ClientI interface {
    Health(ctx context.Context) (*HealthResponse, error)
    Run(ctx context.Context) error
    SubmitTransaction(ctx context.Context, tx *Transaction) error
    VerifyTransaction(ctx context.Context, txid string) (bool, error)
}
```

#### Block Assembly Client

```go
type Client interface {
    BlockAssemblyAPIClient() blockassembly_api.BlockAssemblyAPIClient
    Health(ctx context.Context) (*HealthResponse, error)
}
```

#### Coinbase Client

```go
type Client interface {
    Health(ctx context.Context) (*HealthResponse, error)
    GetCoinbaseTransaction(ctx context.Context) (*Transaction, error)
}
```

## Test Data Structures

### Health Response

```go
type HealthResponse struct {
    Ok      bool   `json:"ok"`
    Message string `json:"message,omitempty"`
}
```

### Transaction Structure

```go
type Transaction struct {
    ID        string    `json:"id"`
    Data      []byte    `json:"data"`
    Timestamp time.Time `json:"timestamp"`
}
```

## Error Types

```go
// Common error categories
errors.NewConfigurationError(format string, args ...interface{}) error
errors.NewServiceError(format string, args ...interface{}) error
errors.NewValidationError(format string, args ...interface{}) error
```

## Testing Constants

### Node URLs

```go
const (
    NodeURL1 = "http://localhost:10090" // Node 1 base URL
    NodeURL2 = "http://localhost:12090" // Node 2 base URL
    NodeURL3 = "http://localhost:14090" // Node 3 base URL
)
```

### Timeouts and Delays

```go
const (
    DefaultSetupTimeout    = 30 * time.Second
    NodeStartupDelay      = 10 * time.Second
    PropagationDelay      = 2 * time.Second
)
```

## Directory Structure

```text
test/
├── aerospike/           # Aerospike database tests
├── config/             # Test configuration files
├── consensus/          # Consensus mechanism tests
├── e2e/               # End-to-end integration tests
├── fsm/               # Finite State Machine tests
├── longtest/          # Long-running performance tests
├── nodeHelpers/       # Node helper utilities
├── postgres/          # PostgreSQL database tests
├── rpc/              # RPC service tests
├── scripts/          # Test automation scripts
├── sequentialtest/   # Sequential test execution
├── tec/              # Error case tests
├── testcontainers/   # Docker container test utilities
├── tna/              # Node responsibility tests
├── tnb/              # Transaction validation tests
├── tnd/              # Block propagation tests
├── tnf/              # Longest chain management tests
├── tnj/              # Consensus rules compliance tests
├── txregistry/       # Transaction registry tests
└── utils/            # Utility functions and test framework
```

## Other Resources

- [QA Guide & Instructions for Functional Requirement Tests](../topics/functionalRequirementTests.md)
- [Understanding The Testing Framework](../topics/understandingTheTestingFramework.md)
- [Automated Testing How-To](../howto/automatedTestingHowTo.md)
