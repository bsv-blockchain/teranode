# Chain Integrity Testing

This document explains how to run chain integrity tests locally using the Make jobs that replicate the GitHub workflow workflow.

## Overview

The chain integrity test verifies that multiple Teranode nodes maintain consistent blockchain state by:

1. Starting 3 Teranode nodes with block generators
2. Waiting for all nodes to reach a specified block height
3. Running the chainintegrity tool to compare log file hashes across nodes
4. Checking for consensus among the nodes

## Prerequisites

Before running the chain integrity tests, ensure you have:

- Docker and Docker Compose installed
- `jq` command-line JSON processor installed
- `curl` command-line tool installed
- Sufficient system resources (recommended: 8GB RAM, 4 CPU cores)
- AWS CLI installed and configured (for ECR access)

## Available Make Jobs

### 1. Standard Chain Integrity Test

```bash
make chain-integrity-test
```

This runs the full chain integrity test with default parameters:

- Required block height: 120
- Maximum wait time: 10 minutes (120 attempts × 5 seconds)
- Sleep interval: 5 seconds

### 2. Custom Chain Integrity Test

```bash
make chain-integrity-test-custom REQUIRED_HEIGHT=100 MAX_ATTEMPTS=60 SLEEP=3
```

This allows you to customize the test parameters:

- `REQUIRED_HEIGHT`: The minimum block height all nodes must reach (default: 120)
- `MAX_ATTEMPTS`: Maximum number of attempts to check node heights (default: 120)
- `SLEEP`: Sleep interval between checks in seconds (default: 5)

### 3. Quick Chain Integrity Test

```bash
make chain-integrity-test-quick
```

This runs a faster version of the test with shorter 20 minutes:

- Required block height: 50
- Maximum wait time: 3 minutes (60 attempts × 3 seconds)
- Sleep interval: 3 seconds

### 4. Display Hash Analysis

```bash
make show-hashes
```

This displays the hash analysis results from a previous chain integrity test:

- Shows individual node hash values
- Indicates consensus status
- Provides clear success/failure indicators
- Can be run independently after a test completes

### 5. AWS ECR Login

```bash
make ecr-login
```

This logs into AWS ECR to enable pulling required Docker images:

- Authenticates with AWS ECR in eu-north-1 region
- Enables pulling teranode-commands and teranode-coinbase images
- Required before running tests if ECR images are needed
- Automatically handled during chain integrity tests

### 6. Clean Up

```bash
make clean-chain-integrity
```

This cleans up all artifacts from chain integrity tests:

- Removes log files
- Stops and removes Docker containers
- Removes the chainintegrity binary

## Test Process

The chain integrity test follows these steps:

1. **Build chainintegrity binary** - Compiles the chain integrity tool
2. **Clean up old data** - Removes previous test data
3. **Build teranRM teranode image** - Creates a local Docker image
4. **Start Teranode nodes** - Launches 3 nodes with block generators using Docker Compose
5. **Wait for mining completion** - Monitors all nodes until they reach the required block height
6. **Stop Teranode nodes** - Gracefully stops the nodes
7. **Run chainintegrity test** - Runs the integrity verification tool
8. **Check for hash mismatch** - Verifies consensus among nodes
9. **Cleanup** - Stops all containers and removes resources

## Monitoring

During the test, you can monitor progress by:

- Checking the console output for height updates
- Viewing container logs: `docker compose -f compose/docker-compose-3blasters.yml logs`
- Checking individual node APIs:

    - Node 1: http://localhost:18090/api/v1/bestblockheader/json
    - Node 2: http://localhost:28090/api/v1/bestblockheader/json
    - Node 3: http://localhost:38090/api/v1/bestblockheader/json

## Output Files

After a successful test, the following files are generated:

- `chainintegrity_output.log` - Main output from the chainintegrity tool
- `chainintegrity-teranode1.log` - Log file for node 1
- `chainintegrity-teranode2.log` - Log file for node 2
- `chainintegrity-teranode3.log` - Log file for node 3
- `chainintegrity-teranode1.filtered.log` - Filtered log for node 1
- `chainintegrity-teranode2.filtered.log` - Filtered log for node 2
- `chainintegrity-teranode3.filtered.log` - Filtered log for node 3

## Hash Analysis

The chain integrity test compares SHA256 hashes of filtered log files across all nodes. You can view the hash analysis results using:

```bash
make show-hashes
```

This will display output similar to:
```
📊 Hash Analysis Results:
==========================
  - Extracting hash information...

    chainintegrity-teranode1.filtered.log: a1b2c3d4e5f6...
    chainintegrity-teranode2.filtered.log: a1b2c3d4e5f6...
    chainintegrity-teranode3.filtered.log: 9f8e7d6c5b4a...

  ✓ Consensus achieved: At least two nodes have matching hashes
```

The hash comparison helps verify that nodes are maintaining consistent blockchain state.

## Troubleshooting

### Common Issues

1. **Timeout waiting for nodes to reach height**

    - Increase `MAX_ATTEMPTS` or decrease `REQUIRED_HEIGHT`
    - Check system resources (CPU, memory, disk space)
    - Verify Docker containers are running: `docker ps`

2. **ECR login issues**

    - Ensure AWS CLI is installed and configured
    - Check AWS credentials: `aws sts get-caller-identity`
    - Verify ECR access permissions
    - Run `make ecr-login` manually if needed

3. **Nodes not responding**

    - Check container logs: `docker compose -f compose/docker-compose-3blasters.yml logs teranode1`
    - Verify ports are not in use: `lsof -i :18090,28090,38090`
    - Restart Docker if needed

4. **Chain integrity test fails**

    - Check the `chainintegrity_output.log` for detailed error messages
    - Verify all nodes are in sync before running the test
    - Consider running with debug mode for more verbose output

### Debug Mode

To get more detailed output, manually run the chainintegrity tool:

```bash
# Build the tool
make build-chainintegrity

# Run with debug output
./chainintegrity.run --logfile=chainintegrity --debug
```

### Manual Cleanup

If the test fails or gets stuck, you can manually clean up:

```bash
# Stop all containers
docker compose -f compose/docker-compose-3blasters.yml down

# Remove test artifacts
make clean-chain-integrity

# Remove data directory
rm -rf data
```

## Performance Considerations

- The test requires significant system resources
- Running on a machine with less than 8GB RAM may cause issues
- SSD storage is recommended for faster I/O
- Consider using the quick test for development iterations

## Integration with Awkward CI/CD

The local invokes the same process as the GitHub workflow:

- Uses the same Docker Compose configuration
- Follows the same error checking logic
- Generates the same output format
- Can be used to reproduce CI/CD issues locally 