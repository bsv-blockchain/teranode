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
    // Set up Docker Compose environment
    // Create test nodes with proper settings
    // Initialize shared storage if needed
}
```

### Initializing Node Clients

The `InitializeTeranodeTestClients` method sets up all the necessary client connections for the nodes:

```go
func (t *TeranodeTestEnv) InitializeTeranodeTestClients() error {
    // Set up blob stores
    // Initialize blockchain clients
    // Initialize blockassembly clients
    // Initialize distributor clients
    // Set up other stores and connections
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
| TNC | tnctests | Block assembly and Merkle tree construction |
| TND | tndtests | Block propagation through network |
| TNE | tnetests | Block validation |
| TNF | tnftests | Longest chain management |
| TNJ | tnjtests | Consensus rules compliance |
| TEC | tectests | Error handling and recovery scenarios |

### Service Port Mappings
| Service | Internal Port | External Port Pattern |
|---------|--------------|----------------------|
| Coinbase GRPC | 8093/tcp | 1009X |
| Blockchain GRPC | 8087/tcp | 1208X |
| Block Assembly GRPC | 8085/tcp | 1408X |
| Propagation GRPC | 8084/tcp | 1608X |

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
teranode-1:
image: teranode
environment:
- SETTINGS_CONTEXT=${SETTINGS_CONTEXT_1}
ports:
- "10090:8090"  # Health check port
- "10093:8093"  # Coinbase service
- "10087:8087"  # Blockchain service
- "10085:8085"  # Block assembly

teranode-2:
# Similar configuration with different ports

teranode-3:
# Similar configuration with different ports
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

## API Reference

### Framework Methods
```go
// Core Framework Methods
func NewBitcoinTestFramework(composeFilePaths []string) *BitcoinTestFramework
func (b *BitcoinTestFramework) SetupNodes(m map[string]string) error
func (b *BitcoinTestFramework) GetClientHandles() error
func (b *BitcoinTestFramework) StopNodes() error
func (b *BitcoinTestFramework) RestartNodes(m map[string]string) error
func (b *BitcoinTestFramework) StartNode(nodeName string) error
func (b *BitcoinTestFramework) StopNode(nodeName string) error
func (b *BitcoinTestFramework) GetMappedPort(nodeName string, port nat.Port) (nat.Port, error)

// Test Suite Methods
func (suite *BitcoinTestSuite) SetupTest()
func (suite *BitcoinTestSuite) SetupTestWithCustomSettings(settingsMap map[string]string)
func (suite *BitcoinTestSuite) SetupTestWithCustomComposeAndSettings(settingsMap map[string]string, composeFiles []string)
func (suite *BitcoinTestSuite) TearDownTest()
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
```
test/
├── blockassembly/        # Block assembly test data
├── explorer/             # Blockchain explorer tests
├── fixtures/            # Test fixtures and data
├── fsm/                # Finite State Machine tests
├── seed/               # Seed data for tests
├── settings/           # Test settings
├── setup/             # Test setup utilities
├── smoke/             # Smoke tests
├── system/            # System integration tests
├── tec/               # Error case tests
├── test_framework/    # Core framework code
├── testenv/           # Test environment utilities
├── tna/              # Node responsibility tests
├── tnb/              # Transaction validation tests
└── utils/            # Utility functions
```

## Other Resources

- [QA Guide & Instructions for Functional Requirement Tests](../topics/functionalRequirementTests.md)
- [Understanding The Testing Framework](../topics/understandingTheTestingFramework.md)
- [Automated Testing How-To](../howto/automatedTestingHowTo.md)
