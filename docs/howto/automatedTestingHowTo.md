# Setting Up Test Environment

## How to Set Up Your Local Test Environment

1. Prerequisites:

    ```bash
    # Install required tools
    docker compose
    go 1.26 or higher
    make
    ```

2. Initial setup:

    ```bash
    # Clone and build
    git clone [repository]
    cd teranode
    docker compose build
    ```

3. Verify setup:

    ```bash
    # Run smoke tests
    make smoketest
    ```

## How to Configure Test Settings

1. Create local settings:

    ```bash
    touch settings_local.conf
    ```

2. Add required settings for your specific testing activity. Common test contexts:

    - `test` - Standard testing environment
    - `docker` - Docker-based testing
    - `docker.ci` - CI environment testing

# Running Tests

## How to Run Test Suites

1. Run all tests in a suite:

    ```bash
    # Format
    cd /teranode/test/<suite-name>
    go test -v -tags test_<suite-code>

    # Examples
    # Run TNA tests
    cd /teranode/test/tna
    go test -v -tags test_tna

    # Run TNB tests
    cd /teranode/test/tnb
    go test -v -tags test_tnb

    # Run TEC tests
    cd /teranode/test/tec
    go test -v -tags test_tec
    ```

2. Run specific test:

    ```bash
    # Format
    go test -v -run "^<TestSuiteName>$/^<TestName>$" -tags <test-tag>

    # Example: Run specific TNA test
    go test -v -run "^TestTNA1TestSuite$/^TestBroadcastNewTxAllNodes$" -tags test_tna
    ```

3. Run with different options:

    ```bash
    # With timeout
    go test -v -tags test_tna -timeout 10m

    # With race detection
    go test -v -race -tags test_tna

    # With specific cache size
    go test -v -tags "test_tna,testtxmetacache"
    ```

## How to Debug Failed Tests

1. Enable verbose logging:

    ```bash
    # Set log level to DEBUG
    export LOG_LEVEL=DEBUG
    go test -v -tags test_tna
    ```

2. Check container logs:

    ```bash
    # View logs for specific node
    docker logs teranode1
    docker logs teranode2
    docker logs teranode3
    ```

3. Inspect test state:

    ```bash
    # Check node health (verify actual ports in your settings)
    curl http://localhost:18090/api/v1/bestblockheader/json
    curl http://localhost:28090/api/v1/bestblockheader/json
    curl http://localhost:38090/api/v1/bestblockheader/json
    ```

# Adding New Tests

## How to Add a New Test Case

1. Create test file with proper structure:

    ```go
    //go:build test_tna || debug

    // How to run this test manually:
    // $ go test -v -run "^TestTNA5TestSuite$/^TestNewFeature$" -tags test_tna

    package tna

    import (
        "testing"
        "time"

        helper "github.com/bsv-blockchain/teranode/test/utils"
        "github.com/bsv-blockchain/teranode/test/utils/tconfig"
        "github.com/stretchr/testify/suite"
    )

    type TNA5TestSuite struct {
        helper.TeranodeTestSuite
    }

    func TestTNA5TestSuite(t *testing.T) {
        suite.Run(t, &TNA5TestSuite{
            TeranodeTestSuite: helper.TeranodeTestSuite{
                TConfig: tconfig.LoadTConfig(
                    map[string]any{
                        tconfig.KeyTeranodeContexts: []string{
                            "docker.teranode1.test.tna5Test",
                            "docker.teranode2.test.tna5Test",
                            "docker.teranode3.test.tna5Test",
                        },
                    },
                ),
            },
        },
        )
    }

    func (suite *TNA5TestSuite) TestNewFeature() {
        // Access test environment
        testEnv := suite.TeranodeTestEnv
        ctx := testEnv.Context
        t := suite.T()
        logger := testEnv.Logger

        // Access nodes
        node1 := testEnv.Nodes[0]
        blockchainClient := node1.BlockchainClient

        // Your test logic here
        t.Log("Starting test")

        // Example: Check node health
        health, err := blockchainClient.Health(ctx)
        if err != nil {
            t.Fatalf("Failed to get health: %v", err)
        }
        if !health.Ok {
            t.Error("Expected health to be OK")
        }

        t.Log("Test completed successfully")
    }
    ```

2. Common test patterns:

    ```go
    // Create and send transaction
    tx, err := helper.CreateAndSendTx(ctx, node1)
    if err != nil {
        t.Fatalf("Failed to create transaction: %v", err)
    }

    // Mine a block
    blockHash, err := helper.MineBlock(ctx, node1.Settings, node1.BlockAssemblyClient, logger)
    if err != nil {
        t.Fatalf("Failed to mine block: %v", err)
    }

    // Wait for block height
    err = helper.WaitForNodeBlockHeight(ctx, blockchainClient, targetHeight, 30*time.Second)
    if err != nil {
        t.Fatalf("Failed to reach target height: %v", err)
    }

    // Wait for nodes to sync
    blockchainClients := []blockchain.ClientI{
        testEnv.Nodes[0].BlockchainClient,
        testEnv.Nodes[1].BlockchainClient,
        testEnv.Nodes[2].BlockchainClient,
    }
    err = helper.WaitForNodesToSync(ctx, blockchainClients, 30*time.Second)
    if err != nil {
        t.Fatalf("Nodes failed to sync: %v", err)
    }
    ```

## How to Add Custom Test Configuration

1. Use TConfig system to customize settings:

    ```go
    func TestCustomTestSuite(t *testing.T) {
        suite.Run(t, &CustomTestSuite{
            TeranodeTestSuite: helper.TeranodeTestSuite{
                TConfig: tconfig.LoadTConfig(
                    map[string]any{
                        tconfig.KeyTeranodeContexts: []string{
                            "docker.teranode1.test.customTest",
                        },
                        tconfig.KeyComposeFiles: []string{
                            "docker-compose.e2etest.yml",
                            "docker-compose.custom.override.yml",
                        },
                    },
                ),
            },
        })
    }
    ```

2. Create custom docker compose override:

    ```yaml
    # docker-compose.custom.override.yml
    services:
      teranode1:
        environment:

          - CUSTOM_SETTING=value
    ```

# Working with Multiple Nodes

## How to Test Node Communication

1. Setup multiple nodes:

    ```go
    // Ensure all nodes are running
    for i, node := range testEnv.Nodes {
        health, err := node.BlockchainClient.Health(ctx)
        if err != nil {
            t.Errorf("Node %d health check failed: %v", i, err)
        }
        if !health.Ok {
            t.Errorf("Node %d is not healthy", i)
        }
    }
    ```

2. Test transaction propagation:

    ```go
    // Send transaction to node 0
    tx, err := helper.CreateAndSendTx(ctx, testEnv.Nodes[0])
    if err != nil {
        t.Fatalf("Failed to create transaction: %v", err)
    }

    // Allow time for propagation
    time.Sleep(2 * time.Second)

    // Verify on other nodes
    block, err := testEnv.Nodes[1].BlockchainClient.GetBestBlockHeader(ctx)
    if err != nil {
        t.Fatalf("Failed to get block from node 1: %v", err)
    }
    ```

## How to Test Node Failure Scenarios

1. Stop specific containers:

    ```bash
    # Stop a specific node container
    docker stop teranode2
    ```

2. Restart containers:

    ```bash
    # Restart the node
    docker start teranode2
    ```

3. Test resilience in code:

    ```go
    // Send transaction before node failure
    tx1, _ := helper.CreateAndSendTx(ctx, testEnv.Nodes[0])

    // Simulate node failure by stopping docker container
    // (typically done manually or via docker SDK)

    // Verify other nodes continue working
    tx2, err := helper.CreateAndSendTx(ctx, testEnv.Nodes[1])
    if err != nil {
        t.Fatalf("Remaining nodes should continue working: %v", err)
    }
    ```

# Cleanup and Maintenance

## How to Clean Test Environment

1. Remove test data:

    ```bash
    # Clean data directory
    rm -rf data/test/*
    ```

2. Remove containers:

    ```bash
    # Remove all test containers
    docker ps -a -q --filter label=com.docker.compose.project=e2e | xargs docker rm -f
    ```

3. Full cleanup in test code:

    ```go
    // TearDown is handled automatically by TeranodeTestSuite
    // But you can add custom cleanup in TearDownTest:
    func (suite *CustomTestSuite) TearDownTest() {
        // Custom cleanup here
        suite.TeranodeTestSuite.TearDownTest() // Call parent teardown
    }
    ```

## How to Handle Common Issues

1. Port conflicts:

    ```bash
    # Check port usage
    lsof -i :8087
    lsof -i :8090
    lsof -i :9292
    ```

2. Resource cleanup:

    ```bash
    # Force remove hanging containers
    docker ps -a -q --filter label=com.docker.compose.project=e2e | xargs docker rm -f

    # Remove volumes
    docker volume prune -f
    ```

3. Data persistence issues:

    ```bash
    # Reset all data (use sudo if needed)
    rm -rf ./data/test

    # Or if permission issues:
    sudo rm -rf ./data/test
    ```

# Available Test Tags

Teranode uses build tags to control which tests run:

- `test_tna` - TNA test suite
- `test_tnb` - TNB test suite
- `test_tec` - TEC test suite
- `test_tnd` - TND test suite
- `test_tnf` - TNF test suite
- `test_tnj` - TNJ test suite
- `test_smoke` - Smoke tests
- `test_functional` - Functional tests
- `testtxmetacache` - Use small transaction metadata cache
- `largetxmetacache` - Use production-sized cache
- `aerospike` - Tests requiring Aerospike
- `debug` - Enable debug mode for tests

# Other Resources

- [QA Guide & Instructions for Functional Requirement Tests](../topics/functionalRequirementTests.md)
- [Understanding The Testing Framework](../topics/understandingTheTestingFramework.md)
- [Testing Technical Reference](../references/testingTechnicalReference.md)
- [Settings Reference](../references/settings.md)
- [Teranode Daemon Reference](../references/teranodeDaemonReference.md)
