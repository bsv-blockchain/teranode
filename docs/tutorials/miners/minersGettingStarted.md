# Getting Started with Teranode

## Index

- [Introduction](#introduction)
- [Prerequisites](#prerequisites)
- [What is Teranode?](#what-is-teranode)
- [Components Overview](#components-overview)
- [First-Time Setup](#first-time-setup)
    - [Step 1: Prepare Your Environment](#step-1-prepare-your-environment)
    - [Step 2: Initial Setup](#step-2-initial-setup)
    - [Step 3: Start Teranode](#step-3-start-teranode)
    - [Step 4: Verify Installation](#step-4-verify-installation)
    - [Common Issues](#common-issues)
- [Basic Operations](#basic-operations)
    - [Checking Node Status](#checking-node-status)
    - [Working with Transactions](#working-with-transactions)
    - [Monitoring Your Node](#monitoring-your-node)
    - [Basic Maintenance](#basic-maintenance)
    - [Common Operations](#common-operations)
    - [Next Steps](#next-steps)
    - [Docker Compose Setup](#docker-compose-setup)
    - [Kubernetes Deployment](#kubernetes-deployment)

## Introduction

This tutorial will guide you through your first steps with Teranode using Docker Compose. By the end of this guide, you'll have a running **testnet** **Teranode** instance suitable for testing and development.

## Prerequisites

Before you begin, ensure you have:

- Basic understanding of blockchain technology
- Familiarity with command-line operations
- The AWS CLI
- Docker Engine 17.03+
- Docker Compose
- The Teranode Docker Compose file
- 100GB+ available disk space
- Stable internet connection

## What is Teranode?

Teranode is a scalable Bitcoin SV node implementation that:

- Processes over 1 million transactions per second
- Uses a microservices architecture
- Maintains full Bitcoin protocol compatibility

## Components Overview

Your Teranode Docker Compose setup will include:

1. **Core Teranode Services**

    - Asset Server
    - Block Assembly
    - Block Validation
    - Blockchain
    - Legacy Gateway
    - P2P
    - Propagation
    - Subtree Validation

2. **Optional Services**

    - Block Persister
    - UTXO Persister

3. **Supporting Services**
    - Kafka for message queuing
    - PostgreSQL for blockchain data
    - Aerospike for UTXO storage
    - Grafana and Prometheus for monitoring

## First-Time Setup

### Step 1: Prepare Your Environment

- Checkout the Teranode public repository:

```bash
cd $YOUR_WORKING_DIR
git clone git@github.com:bsv-blockchain/teranode.git
cd teranode
```

### Step 2: Initial Setup

- Go to the testnet docker compose path:

```bash
cd $YOUR_WORKING_DIR/teranode/deploy/docker/testnet
```

- Pull required images:

```bash
docker compose pull
```

### Step 3: Start Teranode

- Launch all services:

```bash
docker compose up -d
```

Force the node to transition to Run mode:

#### Option 1: Using Admin Dashboard (Easiest)

```bash
# Access the dashboard at http://localhost:8090/admin (default credentials bitcoin:bitcoin)
# Navigate to FSM State section and select RUNNING or LEGACYSYNCING
```

> **Note:** The embedded dashboard is only available when Teranode is built with dashboard support (default in Docker images). The dashboard provides:
>
> - `/admin` - FSM management, block invalidation/revalidation (requires authentication)
> - `/viewer` - Blockchain viewer (blocks, transactions, UTXOs, subtrees)
> - `/home` - Node overview and statistics
> - `/peers` - Peer management and reputation
> - `/network` - Network status and connected nodes
> - `/p2p` - P2P message monitor
>
> For comprehensive dashboard documentation, see [Dashboard Documentation](../../topics/dashboard.md).

#### Option 2: Using teranode-cli

```bash
# Transition to Run mode
docker exec -it blockchain teranode-cli setfsmstate --fsmstate RUNNING

# Or transition to LegacySync mode
docker exec -it blockchain teranode-cli setfsmstate --fsmstate LEGACYSYNCING
```

- Verify services are running:

```bash
docker compose ps
```

- Check individual service logs:

```bash
# Example commands
docker compose logs asset
docker compose logs blockchain
```

- Verify legacy sync status:

When the node is started for the first time, its first action is to perform a initial blockchain sync. You can check the sync progress by checking the Legacy service logs:

```bash
docker compose logs legacy
```

### Step 4: Verify Installation

- Check service health:

```bash
curl http://localhost:8090/health
```

- Access the built-in Teranode dashboard:

    - **Service Viewer**: <http://localhost:8090/viewer> - View blocks, transactions, UTXOs, and subtrees
    - **Admin Interface**: <http://localhost:8090/admin> (credentials: bitcoin/bitcoin) - FSM state management and block operations
    - **Home Overview**: <http://localhost:8090/home> - Node statistics and block graphs
    - **Peer Management**: <http://localhost:8090/peers> - Connected peers and reputation scores
    - **Network Status**: <http://localhost:8090/network> - Connected nodes and chain status
    - For complete dashboard features, see [Dashboard Documentation](../../topics/dashboard.md)

- Access Grafana monitoring (metrics and time-series data):

    - Open Grafana: <http://localhost:3005>
    - Login with the default credentials: admin/admin
    - Navigate to the "Teranode - Service Overview" dashboard for key metrics
    - Explore other dashboards for detailed service metrics. For example, you can check the Legacy sync metrics in the "Teranode - Legacy Service" dashboard.

### Common Issues

1. **Services fail to start**

    - Check logs: `docker compose logs`
    - Verify disk space: `df -h`
    - Ensure all ports are available

2. **Cannot connect to services**

    - Verify services are running: `docker compose ps`
    - Check service logs for specific errors
    - Ensure ports are not blocked by firewall

## Basic Operations

### Checking Node Status

1. View all services status:

    ```bash
    docker compose ps
    ```

2. Check blockchain sync:

    ```bash
    curl http://localhost:8090/api/v1/blockstats
    ```

3. Monitor specific service logs:

    ```bash
    docker compose logs -f legacy
    docker compose logs -f blockchain
    docker compose logs -f asset
    ```

### Working with Transactions

1. Get transaction details:

    ```bash
    curl http://localhost:8090/api/v1/tx/<txid>
    ```

### Monitoring Your Node

1. Access Grafana dashboards:

    - Open <http://localhost:3005>
    - Navigate to "TERANODE Service Overview"

2. Key metrics to watch:

    - Block queue length (should be near 0)
    - Transaction processing rate
    - Memory and CPU usage
    - Disk space utilization

### Basic Maintenance

1. View logs:

    ```bash
    # All services
    docker compose logs

    # Specific service
    docker compose logs blockchain
    ```

2. Check disk usage:

    ```bash
    df -h
    ```

3. Restart a specific service:

    ```bash
    docker compose restart blockchain
    ```

4. Restart all services:

    ```bash
    docker compose down
    docker compose up -d
    ```

### Common Operations

1. Check current block height:

    ```bash
    curl http://localhost:8090/api/v1/bestblockheader/json
    ```

2. Get block information:

    ```bash
    curl http://localhost:8090/api/v1/block/<blockhash>
    ```

3. Check UTXO status:

    ```bash
    curl http://localhost:8090/api/v1/utxo/<utxohash>
    ```

### Next Steps

- Explore the How-to Guides for advanced tasks
- Review the Reference documentation for detailed endpoint information

#### Docker Compose Setup

1. [Installation Guide](../../howto/miners/docker/minersHowToInstallation.md)
2. [Starting and Stopping Teranode](../../howto/miners/docker/minersHowToStopStartDockerTeranode.md)
3. [Configuration Guide](../../howto/miners/docker/minersHowToConfigureTheNode.md)
4. [Blockchain Synchronization](../../howto/miners/docker/minersHowToSyncTheNode.md)
5. [Update Procedures](../../howto/miners/docker/minersUpdatingTeranode.md)
6. [Troubleshooting Guide](../../howto/miners/docker/minersHowToTroubleshooting.md)
7. [Security Best Practices](../../howto/miners/docker/minersSecurityBestPractices.md)

#### Kubernetes Deployment

1. [Installation with Kubernetes Operator](../../howto/miners/kubernetes/minersHowToInstallation.md)
2. [Starting and Stopping Teranode](../../howto/miners/kubernetes/minersHowToStopStartKubernetesTeranode.md)
3. [Configuration Guide](../../howto/miners/kubernetes/minersHowToConfigureTheNode.md)
4. [Blockchain Synchronization](../../howto/miners/kubernetes/minersHowToSyncTheNode.md)
5. [Update Procedures](../../howto/miners/kubernetes/minersUpdatingTeranode.md)
6. [Backup Procedures](../../howto/miners/minersHowToBackup.md)
7. [Troubleshooting Guide](../../howto/miners/kubernetes/minersHowToTroubleshooting.md)
8. [Security Best Practices](../../howto/miners/kubernetes/minersSecurityBestPractices.md)
9. [Remote Debugging Guide](../../howto/howToRemoteDebugTeranode.md)
