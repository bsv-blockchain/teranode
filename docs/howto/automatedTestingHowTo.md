## Setting Up Test Environment

### How to Set Up Your Local Test Environment
1. Prerequisites:
```bash
# Install required tools
docker compose
go 1.21 or higher
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
make smoketests
```

### How to Configure Test Settings
1. Create local settings:
```bash
touch settings_local.conf
```

2. Add required settings for your specific testing activity in there. Recommended to use the docker.ci.tc1.run context.


## Running Tests

### How to Run Test Suites
1. Run all tests in a suite:
```bash
# Format
cd /teranode/test/<suite-name>
go test -v -tags test_<suite-code>

# Examples
# Run TNA tests
cd /teranode/test/tna
go test -v -tags test_tna

# Run TNC tests
cd /teranode/test/tnc
go test -v -tags test_tnc
```

2. Run specific test:
```bash
# Format
go test -v -run "^<TestSuiteName>$/<TestName>$"

# Example: Run specific coinbase test
go test -v -run "^TestTNC1TestSuite$/TestCoinbaseTXAmount$" -tags test_tnc
```

3. Run with different options:
```bash
# Format / Skip Build
go test -v -tags test_<suite-code> -timeout <duration>
make smoketests no-build=1

# Kill docker containers after test
make smoketests no-build=1 kill-docker=1

# Keep data directory
make smoketests no-build=1 kill-docker=1 no-reset=1
```


### How to Debug Failed Tests
1. Enable verbose logging:
```bash
# Set log level to DEBUG
export LOG_LEVEL=DEBUG
```

2. Check container logs:
```bash
# View logs for specific node
docker logs teranode-1
docker logs teranode-2
docker logs teranode-3
```

3. Inspect test state:
```bash
# Check node health
curl http://localhost:10090/health
curl http://localhost:12090/health
curl http://localhost:14090/health
```

## Adding New Tests

### How to Add a New Test Case
1. Create test file:
```go
// test/tna/tna_new_test.go
package tna

import (
    "testing"
    "github.com/bitcoin-sv/teranode/test/setup"
)

type TNANewTestSuite struct {
    setup.BitcoinTestSuite
}

func (suite *TNANewTestSuite) TestNewFeature() {
    t := suite.T()
    framework := suite.Framework

    // Your test logic here
}

func TestTNANewTestSuite(t *testing.T) {
    suite.Run(t, new(TNANewTestSuite))
}
```

2. Common test patterns:
```go
// Health check pattern
blockchainHealth, err := framework.Nodes[1].BlockchainClient.Health(framework.Context)
if err != nil {
    t.Errorf("Failed to get health: %v", err)
}
if !blockchainHealth.Ok {
    t.Errorf("Expected health to be OK")
}

// Node restart pattern
err := framework.RestartNodes(settingsMap)
if err != nil {
    t.Errorf("Failed to restart nodes: %v", err)
}
```

### How to Add Custom Test Configuration
1. Create override file:
```yaml
# docker-compose.custom.override.yml
services:
teranode-1:
environment:
CUSTOM_SETTING: "value"
```

2. Use in test:
```go
func (suite *CustomTestSuite) SetupTest() {
    composeFiles := []string{
        "../../docker-compose.e2etest.yml",
        "../../docker-compose.custom.override.yml",
    }

    suite.SetupTestWithCustomComposeAndSettings(
        suite.DefaultSettingsMap(),
        composeFiles,
    )
}
```

## Working with Multiple Nodes

### How to Test Node Communication
1. Setup multiple nodes:
```go
// Ensure all nodes are running
for _, node := range framework.Nodes {
    blockchainHealth, err := node.BlockchainClient.Health(framework.Context)
    if err != nil {
        t.Errorf("Node health check failed: %v", err)
    }
}
```

2. Test node interaction:
```go
// Send transaction to node 1
tx, err := framework.Nodes[0].BlockchainClient.SubmitTransaction(...)

// Verify propagation to node 2
time.Sleep(2 * time.Second)  // Allow propagation
verified, err := framework.Nodes[1].BlockchainClient.VerifyTransaction(...)
```

### How to Test Node Failure Scenarios
1. Stop specific node:
```go
err := framework.StopNode("teranode-2")
if err != nil {
    t.Errorf("Failed to stop node: %v", err)
}
```

2. Restart node:
```go
err := framework.StartNode("teranode-2")
if err != nil {
    t.Errorf("Failed to start node: %v", err)
}
```

## Cleanup and Maintenance

### How to Clean Test Environment
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

3. Full cleanup:
```bash
# Stop framework and clean up
framework.StopNodesWithRmVolume()
framework.Cancel()
```

### How to Handle Common Issues
1. Port conflicts:
```bash
# Check port usage
lsof -i :8084
lsof -i :8085
lsof -i :8087
```

2. Resource cleanup:
```bash
# Force remove hanging containers
docker rm -f $(docker ps -a -q --filter label=com.docker.compose.project=e2e)
```

3. Data persistence issues:
```bash
# Reset all data
sudo rm -rf ../../data/test
```

## Other Resources

- [QA Guide & Instructions for Functional Requirement Tests](../topics/functionalRequirementTests.md)
- [Understanding The Testing Framework](../topics/understandingTheTestingFramework.md)
- [Testing Technical Reference](../references/testingTechnicalReference.md)
- [Settings Reference](../references/settings.md)
- [Teranode Daemon Reference](../references/teranodeDaemonReference.md)
