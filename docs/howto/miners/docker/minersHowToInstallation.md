# How to Install Teranode with Docker Compose

Last modified: 29-Oct-2025

## Index

- [Introduction](#introduction)
- [Prerequisites](#prerequisites)
- [Hardware Requirements](#hardware-requirements)
- [Software Requirements](#software-requirements)
- [Network Considerations](#network-considerations)
- [Installation Process](#installation-process)
    - [Teranode Initial Block Synchronization](#teranode-initial-block-synchronization)
        - [Full P2P Download](#full-p2p-download)
        - [Initial Data Set Installation](#initial-data-set-installation)
    - [Teranode Installation Types](#teranode-installation-types)
        - [Pre-built Binaries](#pre-built-binaries)
        - [Docker Container](#docker-container)
        - [Docker Compose](#docker-compose)
    - [Installing Teranode with Docker Compose](#installing-teranode-with-docker-compose)
- [Reference Settings](#settings-reference)

## Introduction

This guide provides step-by-step instructions for installing Teranode using Docker Compose. Notice that this approach is only recommended for testing purposes and not for production usage.

This guide is applicable to:

1. Miners and node operators testing Teranode.

2. Single-node Teranode setups, using Docker Compose setup.

3. Configurations designed to connect to and process the BSV mainnet with current production load.

This guide does not cover:

1. Advanced clustering configurations.

2. Scaled-out Teranode configurations. The Teranode installation described in this document is capable of handling the regular BSV production load, not the scaled-out 1 million tps configuration.

3. Custom mining software integration.

4. Advanced network configurations.

5. Any sort of source code build or manipulation of any kind.

## Prerequisites

- Docker Engine 17.03+
- Docker Compose
- 100GB+ available disk space
- Stable internet connection
- Root or sudo access

## Hardware Requirements

For detailed hardware specifications including CPU, RAM, and storage requirements, see the [System Requirements](../systemRequirements.md) document.

**Quick reference for Docker Compose mainnet:**

| Resource | Minimum | Recommended |
|----------|---------|-------------|
| CPU | 8 cores | 16 cores |
| RAM | 128 GB | 256 GB |
| Storage | 1 TB | 2 TB |
| Storage Type | NVMe SSD | NVMe SSD |

These specifications assume a seeded, pruned node with default retention settings.

## Software Requirements

Teranode relies on a number of third-party software dependencies, some of which can be sourced from different vendors.

BSV provides both a Docker Compose that initialises all dependencies within a single server node, and a `Kubernetes operator` that provides a production-live multi-node setup. This document covers the Docker Compose approach.

This section will outline the various vendors in use in Teranode.

To know more, please refer to the [Third Party Reference Documentation](../../../references/thirdPartySoftwareRequirements.md)

Note - if using Docker Compose, the shared Docker storage, which is automatically managed by Docker Compose, is used instead. This approach provides a more accessible testing environment while still allowing for essential functionality and performance evaluation.

## Network Considerations

Network requirements for running a Teranode BSV node:

- Inbound: 5-50 GB per day
- Outbound: 50-150 GB per day

Key considerations:

1. Ensure reliable internet connection with sufficient bandwidth
2. Plan for higher bandwidth during initial blockchain synchronization
3. Monitor network usage against ISP limits

## Installation Process

### Teranode Initial Block Synchronization

Teranode requires an initial block synchronization to function properly. There are two approaches for completing the synchronization process.

#### Full P2P Download

- Start the node and download all blocks from genesis using peer-to-peer (P2P) network.
- This method downloads the entire blockchain history.

**Pros:**

- Simple to implement
- Ensures the node has the complete blockchain history

**Cons:**

- Time-consuming process
- Can take 5-8 days, depending on available bandwidth

#### Initial Data Set Installation

To speed up the initial synchronization process, you have the option to seed Teranode from pre-existing data. To know more about this approach, please refer to the [How to Sync the Node](minersHowToSyncTheNode.md) guide.

Pros:

- Significantly faster than full P2P download
- Allows for quicker node setup

Cons:

- Requires additional steps
- The data set must be validated, to ensure it has not been tampered with

Where possible, BSV recommends using the Initial Data Set Installation approach.

### Teranode Installation Types

Teranode supports four installation methods:

- Pre-built Binaries
- Docker Container
- Docker Compose
- Kubernetes Operator

Note: Only Docker Compose (testing) and Kubernetes Operator (production) are officially supported.

#### Pre-built Binaries

- Pre-built executables are available for both arm64 and amd64 architectures.
- This method provides the most flexibility but requires manual setup of dependencies.
- Suitable for users who need fine-grained control over their installation.

#### Docker Container

- A Docker image is published to a Docker registry, containing the Teranode binary and internal dependencies.
- This method offers a balance between ease of use and customization.
- Ideal for users familiar with Docker who want to integrate Teranode into existing container ecosystems.

#### Docker Compose

- A Docker Compose file is provided to alpha testing miners.
- This method sets up a single-node Teranode instance along with all required external dependencies.
- Components included:

    - Teranode Docker image
    - Kafka
    - PostgreSQL
    - Aerospike
    - Grafana
    - Prometheus
    - Docker Shared Storage

- Advantages:

    - Easiest method to get a full Teranode environment running quickly.
    - Automatically manages start order and networking between components.
    - Used by developers and QA for testing, ensuring reliability.

- Considerations:

    - This setup uses predefined external dependencies, which may not be customizable.
    - While convenient for development and testing, it is not optimized, nor intended, for production usage.

Note: The Docker Compose method is recommended for testing in single node environments as it provides a consistent environment that mirrors the development setup. However, for production deployments or specific performance requirements, users need to consider using the Kubernetes operator approach (separate document).

### Installing Teranode with Docker Compose

**Prerequisites**:

- Docker and Docker Compose installed on your system

- The Alpha testing Teranode docker compose file should have been checked out in your system (see the next section)

- Sufficient disk space (at least 100GB recommended)

- A stable internet connection

#### Step 1: Create Repository

##### Open Terminal

Open a terminal or command prompt.

##### Clone Repository

Clone the Teranode repository:

```bash
cd $YOUR_WORKING_DIR
git clone git@github.com:bsv-blockchain/teranode.git
cd teranode
```

#### Step 2: Configure Environment Settings Context [Optional]

##### Create Environment File

Create a `.env` file in the root directory of the project.

##### Set Context (Optional)

While not required (your docker compose file will be preconfigured), the following line can be used to set the context:

```bash
SETTINGS_CONTEXT_1=docker.m
```

#### Step 3: Prepare Local Settings

##### Review Settings File

You can see the current settings under the `$YOUR_WORKING_DIR/teranode/settings.conf` file. You can override any settings here.

##### Settings Reference

For a list of settings, and their default values, please refer to the reference at the end of this document.

#### Step 4: Pull Docker Images

##### Navigate to Docker Compose Directory

Go to either the testnet Docker compose folder:

```bash
cd $YOUR_WORKING_DIR/teranode/deploy/docker/testnet
```

Or to the mainnet Docker compose folder:

```bash
cd $YOUR_WORKING_DIR/teranode/deploy/docker/mainnet
```

##### Pull Docker Images

Pull the required Docker images:

```bash
docker compose pull
```

#### Step 5: Start the Teranode Stack

Launch the entire Teranode stack using Docker Compose:

```bash
docker compose up -d
```

Force the node to transition to Run mode:

**Option 1: Using Admin Dashboard (Easiest)**

Access the dashboard at <http://localhost:8090/admin> and use the FSM State controls to transition to **RUNNING** or **LEGACYSYNCING**.

**Option 2: Using teranode-cli**

```bash
# Transition to Run mode
docker exec -it blockchain teranode-cli setfsmstate --fsmstate running

# Or transition to LegacySync mode
docker exec -it blockchain teranode-cli setfsmstate --fsmstate legacysyncing
```

You can verify the current state with:

```bash
docker exec -it blockchain teranode-cli getfsmstate
```

Or view it in the Admin Dashboard at <http://localhost:8090/admin>

#### Step 6: Verify Services

##### Check Service Status

Check if all services are running correctly:

```bash
docker compose ps
```

Example output:

| NAME | IMAGE | COMMAND | SERVICE | CREATED | STATUS | PORTS                                                                                                                                                                                                                                                                                                                                                                       |
|------|-------|---------|----------|---------|---------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| aerospike | aerospike:ce-7.1.0.6 | `/usr/bin/as-tini-st…` | aerospike | 2 minutes ago | Up 2 minutes | 0.0.0.0:3000->3000/tcp, :::3000->3000/tcp, 3001-3002/tcp                                                                                                                                                                                                                                                                                                                    |
| aerospike-exporter | aerospike/aerospike-prometheus-exporter:latest | `/docker-entrypoint.…` | aerospike-exporter | 2 minutes ago | Up 2 minutes | 9145/tcp                                                                                                                                                                                                                                                                                                                                                                    |
| asset | ghcr.io/bsv-blockchain/teranode:v0.5.50 | `/app/entrypoint.sh …` | asset | 2 minutes ago | Up 2 minutes (healthy) | 0/tcp, 3005/tcp, 8098/tcp, 9292/tcp, 0.0.0.0:8090->8090/tcp, :::8090->8090/tcp, 0.0.0.0:32796->4040/tcp, [::]:32796->4040/tcp, 0.0.0.0:32797->8000/tcp, [::]:32797->8000/tcp, 0.0.0.0:32798->8091/tcp, [::]:32798->8091/tcp, 0.0.0.0:32799->9091/tcp, [::]:32799->9091/tcp                                                                                                  |
| blockassembly | ghcr.io/bsv-blockchain/teranode:v0.5.50 | `/app/entrypoint.sh …` | blockassembly | 2 minutes ago | Up 2 minutes (healthy) | 0/tcp, 3005/tcp, 8098/tcp, 9292/tcp, 0.0.0.0:32768->4040/tcp, [::]:32768->4040/tcp, 0.0.0.0:32772->8000/tcp, [::]:32772->8000/tcp, 0.0.0.0:32774->8085/tcp, [::]:32774->8085/tcp, 0.0.0.0:32777->8285/tcp, [::]:32777->8285/tcp, 0.0.0.0:32779->9091/tcp, [::]:32779->9091/tcp                                                                                              |
| blockchain | ghcr.io/bsv-blockchain/teranode:v0.5.50 | `/app/entrypoint.sh …` | blockchain | 2 minutes ago | Up 2 minutes (healthy) | 0/tcp, 3005/tcp, 8098/tcp, 9292/tcp, 0.0.0.0:32769->4040/tcp, [::]:32769->4040/tcp, 0.0.0.0:32771->8000/tcp, [::]:32771->8000/tcp, 0.0.0.0:32775->8082/tcp, [::]:32775->8082/tcp, 0.0.0.0:32776->8087/tcp, [::]:32776->8087/tcp, 0.0.0.0:32778->9091/tcp, [::]:32778->9091/tcp                                                                                              |
| blockvalidation | ghcr.io/bsv-blockchain/teranode:v0.5.50 | `/app/entrypoint.sh …` | blockvalidation | 2 minutes ago | Up 2 minutes (healthy) | 0/tcp, 3005/tcp, 8098/tcp, 9292/tcp, 0.0.0.0:32781->4040/tcp, [::]:32781->4040/tcp, 0.0.0.0:32787->8000/tcp, [::]:32787->8000/tcp, 0.0.0.0:32788->8088/tcp, [::]:32788->8088/tcp, 0.0.0.0:32790->8188/tcp, [::]:32790->8188/tcp, 0.0.0.0:32792->9091/tcp, [::]:32792->9091/tcp                                                                                              |
| grafana | grafana/grafana:latest | `/run.sh` | grafana | 2 minutes ago | Up 2 minutes | 0.0.0.0:3005->3000/tcp, [::]:3005->3000/tcp                                                                                                                                                                                                                                                                                                                                 |
| kafka-console-shared | docker.redpanda.com/redpandadata/console:latest | `/bin/sh -c 'echo \"$…'` | kafka-console-shared | 2 minutes ago | Up 2 minutes | 0.0.0.0:8080->8080/tcp, :::8080->8080/tcp                                                                                                                                                                                                                                                                                                                                   |
| kafka-shared | vectorized/redpanda:latest | `/entrypoint.sh 'red…'` | kafka-shared | 2 minutes ago | Up 2 minutes | 8082/tcp, 9644/tcp, 0.0.0.0:9092-9093->9092-9093/tcp, :::9092-9093->9092-9093/tcp, 0.0.0.0:32794->8081/tcp, [::]:32794->8081/tcp                                                                                                                                                                                                                                            |
| legacy | ghcr.io/bsv-blockchain/teranode:v0.5.50 | `/app/entrypoint.sh …` | legacy | 2 minutes ago | Up 2 minutes | 0/tcp, 3005/tcp, 0.0.0.0:8333->8333/tcp, :::8333->8333/tcp, 9292/tcp, 0.0.0.0:18333->18333/tcp, :::18333->18333/tcp, 0.0.0.0:32782->4040/tcp, [::]:32782->4040/tcp, 0.0.0.0:32783->8000/tcp, [::]:32783->8000/tcp, 0.0.0.0:32784->8098/tcp, [::]:32784->8098/tcp, 0.0.0.0:32785->8099/tcp, [::]:32785->8099/tcp, 0.0.0.0:32786->9091/tcp, [::]:32786->9091/tcp              |
| postgres | postgres:latest | `docker-entrypoint.s…` | postgres | 2 minutes ago | Up 2 minutes (healthy) | 0.0.0.0:5432->5432/tcp, :::5432->5432/tcp                                                                                                                                                                                                                                                                                                                                   |
| prometheus | prom/prometheus:v2.44.0 | `/bin/prometheus --c…` | prometheus | 2 minutes ago | Up 2 minutes | 0.0.0.0:9090->9090/tcp, :::9090->9090/tcp                                                                                                                                                                                                                                                                                                                                   |
| rpc | ghcr.io/bsv-blockchain/teranode:v0.5.50 | `/app/entrypoint.sh …` | rpc | 2 minutes ago | Up 2 minutes | 0/tcp, 3005/tcp, 8098/tcp, 0.0.0.0:9292->9292/tcp, :::9292->9292/tcp, 0.0.0.0:32770->4040/tcp, [::]:32770->4040/tcp, 0.0.0.0:32773->8000/tcp, [::]:32773->8000/tcp, 0.0.0.0:32780->9091/tcp, [::]:32780->9091/tcp                                                                                                                                                           |
| subtreevalidation | ghcr.io/bsv-blockchain/teranode:v0.5.50 | `/app/entrypoint.sh …` | subtreevalidation | 2 minutes ago | Up 2 minutes (healthy) | 0/tcp, 3005/tcp, 8098/tcp, 9292/tcp, 0.0.0.0:32789->4040/tcp, [::]:32789->4040/tcp, 0.0.0.0:32791->8000/tcp, [::]:32791->8000/tcp, 0.0.0.0:32793->8086/tcp, [::]:32793->8086/tcp, 0.0.0.0:32795->9091/tcp, [::]:32795->9091/tcp                                                                                                                                             |

##### Verify Service Health

Ensure all services show a status of "Up" or "Healthy".

#### Step 7: Access Monitoring Tools

1. **Grafana**: Access the Grafana dashboard at `http://localhost:3005`
    - Default credentials are `admin/admin`
    - Navigate to the "Teranode - Service Overview" dashboard for key metrics
    - Explore other dashboards for detailed service metrics. For example, you can check the Legacy sync metrics in the "Teranode - Legacy Service" dashboard.

2. **Kafka Console**: Available internally on port 8080

3. **Prometheus**: Available internally on port 9090

4. **Teranode Blockchain Viewer**: A basic blockchain viewer is available and can be accessed via the asset container. It provides an interface to browse blockchain data.
    - **Port**: Exposed on port **8090** of the asset container.
    - **Access URL**: <http://localhost:8090/viewer>

Note: You must set the setting `dashboard_enabled` as true in order to see the viewer.

#### Step 8: Interact with Teranode

- The various Teranode services expose different ports for interaction:

    - **Blockchain service**: Port 8082
    - **Asset service**: Ports 8090
    - **Miner service**: Ports 8089, 8092, 8099
    - **P2P service**: Ports 9905, 9906
    - **RPC service**: 9292

Notice that those ports might be mapped to random ports on your host machine. You can check the mapping by running `docker compose ps`.

#### Step 9: Logging and Troubleshooting

##### View All Service Logs

View logs for all services:

```bash
docker compose logs
```

##### View Specific Service Logs

View logs for a specific service (e.g., teranode-blockchain):

```bash
docker compose logs -f legacy
docker compose logs -f blockchain
docker compose logs -f asset
```

#### Step 10: Docker Log Rotation

Teranode is very verbose and will output a lot of information, especially with logLevel=DEBUG. To avoid running out of disk space, you can specify logging options directly in your docker-compose.yml file for each service.

```yaml
services:
  [servicename]:
    logging:
      options:
        max-size: "100m"
        max-file: "3"
```

#### Step 11: Stopping the Stack

1. To stop all services:

   ```bash
   docker compose down
   ```

Additional Notes:

- The `data` directory (under `testnet` or `mainnet`) will contain persistent data. This includes blockchain data and other persistent storage required by the Teranode components. By default, Docker Compose is configured to mount these directories into the respective containers, ensuring that data persists across restarts and container recreation. Ensure regular backups.
- Under no circumstances should you use this Docker Compose approach for production usage.
- Please discuss with the Teranode support team for advanced configuration options and optimizations not covered in the present document.

## Optimizations

If you have local access to SV Nodes, you can use them to speed up the initial block synchronization. You can set specific peer connections in your docker-compose.yml by adding `legacy_config_ConnectPeers=172.x.x.x:8333|10.x.x.x:8333` to force the legacy service to only connect to those peers.

## Aerospike Operations and Configuration

Teranode includes Aerospike 8.0 community edition for storing the UTXO set. The Aerospike database stores all indexes in memory and the data on disk.

### Aerospike Configuration

The default configuration is set to stop writing when 50% of the system memory has been consumed. To future proof, you might want to run a dedicated cluster, with more memory and disk space allocated.

See [aerospike.conf / stop-writes-sys-memory-pct](https://github.com/bsv-blockchain/teranode/blob/main/deploy/docker/base/aerospike.conf)

By default, the Aerospike data is written to a single mount mounted in the `aerospike` container. For performance reasons, it is recommended to use at least 4 dedicated disks for the Aerospike data.

See [aerospike.conf / storage-engine](https://github.com/bsv-blockchain/teranode/blob/main/deploy/docker/base/aerospike.conf)

### Aerospike Administration

You can access the Aerospike container and run administrative commands:

```bash
docker exec -it aerospike /bin/bash

# Command for basic sanity checking
asadm -e "info"
asadm -e "summary -l"
```

For more information about Aerospike, you can access the [Aerospike documentation](https://www.aerospike.com/docs).

## Docker Logs Management

Teranode is very verbose and will output a lot of information, especially with `logLevel=DEBUG`. Make sure to setup log rotation for the containers to avoid running out of disk space.

### System-wide Docker Log Configuration

```bash
sudo bash -c 'cat <<EOF > /etc/docker/daemon.json
{
  "log-driver": "json-file",
  "log-opts": {
    "max-size": "100m"
  }
}
EOF'
sudo systemctl restart docker
```

### Per-Service Log Configuration

Alternatively, you can specify logging options directly in your docker-compose.yml file for each service:

```yaml
services:
  [servicename]:
    logging:
      driver: "json-file"
      options:
        max-size: "100m"
        max-file: "3"
```

## Teranode Network Architecture

Teranode offers two network connectivity options:

### SVNode P2P Network (Default for Docker Compose)

- **Description**: All data transmitted over P2P connections (via `legacy` service)
- **Use Case**: Traditional Bitcoin SV network connectivity
- **Characteristics**: Compatible with existing SV Node infrastructure
- **Status**: Enabled by default in Docker Compose for initial blockchain synchronization

### Teranode P2P Network (Advanced)

- **Description**: Only small messages over P2P, with bulk data downloaded via HTTP(S) (via `peer` and `asset` services)
- **Use Case**: Enhanced performance and stability when connecting to other Teranode nodes
- **Characteristics**: Significant performance advantages over traditional SV Node network
- **Status**: **Disabled by default** in Docker Compose testnet/mainnet configurations

> **Note**: The `peer` service is commented out in the default Docker Compose configuration. It is only needed when connecting to a private Teranode network with other Teranode nodes. For initial setup and blockchain synchronization from the traditional BSV network, the `legacy` service is sufficient.

### Enabling the Peer Service

The peer service should only be enabled if you:

1. Are connecting to an existing **private Teranode network** with other Teranode nodes
2. Have confirmed network connectivity with Teranode bootstrap nodes
3. Need to participate in Teranode-to-Teranode block and subtree propagation

To enable the peer service, uncomment the `peer` section in your `docker-compose.yml` file and ensure proper network configuration.

### Teranode Network Requirements

If you choose to enable and connect to the Teranode P2P network, you'll need:

1. **Public-facing `peer` service** exposed via TCP (defaults to port 9905, P2P_PORT setting)
2. **Public-facing `asset` service** exposed via HTTP(S) (selective endpoint exposure recommended)
3. **Valid SSL certificates** if using HTTPS for the asset service
4. **Bootstrap node addresses** or other Teranode peers to connect to

## Asset Service Setup

The asset service provides HTTP access to blockchain data. For production deployments, you should configure proper caching and security.

### Basic Asset Service Configuration

Update your configuration with your publicly accessible endpoints:

```bash
asset_httpPublicAddress = https://teranode-1-asset-cache.example.com/api/v1
peer_p2pPublicAddress = /ip4/1.2.3.4/tcp/9905
```

### Asset Service with Nginx Caching

For the `asset` service, we have provided an `asset-cache` example using nginx to limit endpoints and provide a caching mechanism. This improves performance and provides better security by exposing only necessary endpoints.

### Asset Service Viewer

The asset service includes a web-based viewer for blockchain data:

- **Port**: Exposed on **port 8090** of the asset container
- **Access URL**: `http://localhost:8090/viewer`

## RPC Interface

A SVNode compatible RPC interface is available for interacting with Teranode programmatically.

### RPC Configuration

- **Port**: 9292
- **Default Credentials**: `bitcoin:bitcoin`
- **Protocol**: JSON-RPC over HTTP

### RPC Usage Example

```bash
curl --user bitcoin:bitcoin --data-binary '{"jsonrpc":"1.0","id":"curltext","method":"version","params":[]}' -H 'content-type: text/plain;' http://localhost:9292/
```

> **Note**: Most methods have not been implemented yet. The RPC interface is primarily for compatibility and basic operations.

## teranode-cli Command Reference

The `teranode-cli` is a command-line tool that can be used to interact with the Teranode services. It is available in all containers.

### Accessing teranode-cli

```bash
docker exec -it blockchain teranode-cli
```

### Available Commands

Running `teranode-cli` without arguments will show a list of all available commands:

```bash
Usage: teranode-cli <command> [options]

Available Commands:
  aerospikereader      Aerospike Reader
  checkblocktemplate   Check block template
  export-blocks        Export blockchain to CSV
  filereader           File Reader
  getfsmstate          Get the current FSM State
  import-blocks        Import blockchain from CSV
  seeder               Seeder
  setfsmstate          Set the FSM State
  settings             Settings

Use 'teranode-cli <command> --help' for more information about a command
```

### Common CLI Operations

**Check FSM State**:

```bash
docker exec -it blockchain teranode-cli getfsmstate
```

**Set FSM State to Legacy Syncing**:

```bash
# Via teranode-cli
docker exec -it blockchain teranode-cli setfsmstate --fsmstate LEGACYSYNCING

# Or via Admin Dashboard at http://localhost:8090/admin
```

**Set FSM State to Running**:

```bash
# Via teranode-cli
docker exec -it blockchain teranode-cli setfsmstate --fsmstate RUNNING

# Or via Admin Dashboard at http://localhost:8090/admin
```

## Teranode Reset Procedures

For complete reset instructions including granular per-service cleanup options, see the [How to Reset Teranode](minersHowToResetTeranode.md) guide.

**Quick reference for Docker Compose:**

```bash
# Stop all services
docker compose down

# Delete all data (simplest method - includes Aerospike, Postgres, Kafka, and filesystem data)
sudo rm -rf ./data/*

# Restart services
docker compose up -d
```

## Teranode Seeding

If you have access to an SV Node, you can speed up the Initial Block Download (IBD) by seeding the Teranode with an export from SV Node. Only run this on a gracefully shut down SV Node instance (e.g., after using the RPC `stop` method).

### Step 1: Export from SV Node

```bash
docker run -it \
  -v /mnt/teranode/seed:/mnt/teranode/seed \
  --entrypoint="" \
  434394763103.dkr.ecr.eu-north-1.amazonaws.com/teranode-public:v0.8.12 \
  /app/teranode-cli bitcointoutxoset -bitcoinDir=/home/ubuntu/bitcoin-data -outputDir=/mnt/teranode/seed/export
```

This script assumes your `bitcoin-data` directory is located in `/home/ubuntu/bitcoin-data` and contains the `blocks` and `chainstate` directories. It will generate `${blockhash}.utxo-headers` and `${blockhash}.utxo-set` files.

> **Note**: If you get a `Block hash mismatch between last block and chainstate` error message, you should try starting and stopping the SV Node again, it means there are still a few unprocessed blocks.

### Step 2: Seed Teranode

Once you have the export, you can seed any fresh Teranode with the following command:

```bash
docker run -it \
  -e SETTINGS_CONTEXT=docker.m \
  -v ${teranode-location}/docker/base/settings_local.conf:/app/settings_local.conf \
  -v ${teranode-location}/docker/mainnet/data/teranode:/app/data \
  -v /mnt/teranode/seed:/mnt/teranode/seed \
  --network my-teranode-network \
  --entrypoint="" \
  434394763103.dkr.ecr.eu-north-1.amazonaws.com/teranode-public:v0.8.12 \
  /app/teranode-cli seeder -inputDir /mnt/teranode/seed -hash 0000000000013b8ab2cd513b0261a14096412195a72a0c4827d229dcc7e0f7af
```

### Step 3: Complete Seeding Process

For complete seeding instructions including Docker Compose setup, database preparation, and FSM state transitions, refer to the dedicated sync guide:

- **[Syncing the Blockchain Guide](./minersHowToSyncTheNode.md)** - Comprehensive instructions for all synchronization methods

## CPU Mining

For CPU mining setup with Teranode, please refer to the dedicated guide:

- **[CPU Mining Guide](../minersHowToCPUMiner.md)** - Complete setup instructions for CPU mining with Teranode

This guide covers the BSV CPU miner configuration, parameters, troubleshooting, and usage examples.

> **Note**: The example address used here is a testnet address, linked to the [BSV Faucet](https://bsvfaucet.org/). For mainnet mining, use a valid mainnet address.

## Advanced Configuration Examples

### Peer Connection Optimization

If you have local access to SV Nodes, that will speed up the IBD. You can set specific peer connections in your docker-compose.yml:

```yaml
environment:

  - legacy_config_ConnectPeers=172.x.x.x:8333|10.x.x.x:8333
```

### Settings Override Examples

You can override any setting using environment variables in your docker-compose.yml. See the `x-teranode-settings` section for examples:

```yaml
x-teranode-settings: &teranode-settings
  SETTINGS_CONTEXT: docker.m
  # Override specific settings
  logLevel: DEBUG
  asset_httpPublicAddress: "https://your-domain.com/api/v1"
```

### Network-Specific Settings

**Testnet Configuration**:

```yaml
environment:

  - SETTINGS_CONTEXT=docker.t
  - legacy_config_TestNet=true
```

**Mainnet Configuration**:

```yaml
environment:

  - SETTINGS_CONTEXT=docker.m
  - legacy_config_TestNet=false
```

## Troubleshooting

### Common Issues and Solutions

**Issue**: Container fails to start with permission errors
**Solution**: Ensure proper file permissions on data directories:

```bash
sudo chown -R 1000:1000 ./data/
```

**Issue**: Aerospike stops writing due to memory limits
**Solution**: Increase system memory or adjust `stop-writes-sys-memory-pct` in aerospike.conf

**Issue**: Postgres connection errors
**Solution**: Check database initialization and ensure proper network connectivity between containers

**Issue**: Slow synchronization
**Solution**:

- Use seeding process for faster initial sync
- Configure specific peer connections for better connectivity
- Ensure adequate system resources (CPU, memory, disk I/O)

### Log Analysis

Monitor service logs for troubleshooting:

```bash
# Follow logs for all containers
docker compose logs -f -n 100

# Follow logs for specific container
docker logs -f legacy -n 100

# Check container health status
docker ps --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"
```

## Settings Reference

You can find the pre-configured [settings file](https://github.com/bsv-blockchain/teranode/blob/main/settings.conf). You can refer to this document in order to identify the current system behaviour and in order to override desired settings in your `settings_local.conf`.

The Docker deployment includes an empty `settings_local.conf` file by default. To help you configure your node, a comprehensive template is available at [`deploy/docker/base/settings_local.conf.template`](https://github.com/bsv-blockchain/teranode/blob/main/deploy/docker/base/settings_local.conf.template) that documents all commonly customized settings for Docker deployments, including:

- Network participation mode (listen_only vs full)
- Public endpoints for asset service and P2P
- RPC authentication credentials
- Mining configuration (coinbase text/tags)
- Node identification

Refer to the template file for detailed explanations and examples of each setting.
