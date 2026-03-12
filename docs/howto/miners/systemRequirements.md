# System Requirements

## Index

- [Overview](#overview)
- [Operating System](#operating-system)
- [Docker Compose Requirements](#docker-compose-requirements)
    - [Mainnet](#mainnet)
    - [Testnet](#testnet)
- [Kubernetes Requirements](#kubernetes-requirements)
    - [Mainnet](#kubernetes-mainnet)
    - [Testnet](#kubernetes-testnet)
- [Storage Breakdown](#storage-breakdown)
- [Network Configuration](#network-configuration)
- [Important Notes](#important-notes)

## Overview

This document outlines the system requirements for running Teranode. Requirements differ based on:

1. **Network**: Mainnet vs Testnet
2. **Deployment type**: Docker Compose (single-node) vs Kubernetes (multi-node)

All specifications assume a **seeded, pruned node** with default retention settings. Requirements may increase if:

- Performing full blockchain sync from genesis (instead of seeding)
- Increasing data retention values
- Running additional services or replicas

## Operating System

**Recommended**: Ubuntu 24.04 LTS

Both Docker and Kubernetes deployments should work on other Linux distributions, but this is untested. Ubuntu 24.04 LTS is the recommended and tested platform.

## Docker Compose Requirements

Docker Compose deployments run all services on a single node. This is suitable for production workloads but you lose the ability to scale individual services independently. For horizontal scaling of specific services, use Kubernetes.

### Mainnet

| Resource | Minimum | Recommended |
|----------|---------|-------------|
| CPU | 8 cores | 16 cores |
| RAM | 128 GB | 256 GB |
| Storage | 1 TB | 2 TB |
| Storage Type | NVMe SSD | NVMe SSD |

**Note**: NVMe SSD storage is strongly recommended for mainnet. Spinning disks (HDD) are not supported due to IOPS requirements from Aerospike and blob storage.

### Testnet

| Resource | Minimum | Recommended |
|----------|---------|-------------|
| CPU | 4 cores | 8 cores |
| RAM | 16 GB | 32 GB |
| Storage | 64 GB | 128 GB |
| Storage Type | SSD | NVMe SSD |

## Kubernetes Requirements

Kubernetes deployments allow horizontal scaling of individual services. External dependencies (Aerospike, PostgreSQL, Kafka) should be deployed separately or use managed services.

### Kubernetes Mainnet

**Teranode Services (per pod):**

*Service names match those used in teranode-operator managed CRs. CPU and memory requirements should be monitored and adjusted based on network activity. These values are highly dependent on transaction volume and block sizes on the blockchain network.*

| Service | CPU Request | Memory Request |
|---------|-------------|----------------|
| alertSystem | 1 | 1Gi |
| asset | 1 | 1Gi |
| blockAssembly | 1 | 4Gi |
| blockchain | 1 | 1Gi |
| blockValidator | 1 | 8Gi |
| legacy | 4 | 32Gi |
| peer | 1 | 1Gi |
| propagation | 1 | 1Gi |
| rpc | 1 | 1Gi |
| subtreeValidator | 1 | 16Gi |

*Optional services such as `validator`, `blockPersister`, `utxoPersister`, and `coinbase` can be enabled with a baseline of 1 CPU / 1Gi memory.*

**External Dependencies:**

| Component | CPU | Memory | Storage |
|-----------|-----|--------|---------|
| Aerospike | 4 cores | 32 GB | 400 GB NVMe |
| PostgreSQL | 2 cores | 4 GB | 50 GB SSD |
| Kafka | 2 cores | 4 GB | 50 GB SSD |

**Shared Storage (RWX):** 1 TB

### Kubernetes Testnet

**Teranode Services (per pod):**

A baseline of 100m CPU / 512Mi memory should be sufficient for all services on testnet.

**External Dependencies:**

| Component | CPU | Memory | Storage |
|-----------|-----|--------|---------|
| Aerospike | 2 cores | 8 GB | 50 GB NVMe |
| PostgreSQL | 1 core | 2 GB | 20 GB SSD |
| Kafka | 1 core | 2 GB | 20 GB SSD |

**Shared Storage (RWX):** 50 GB

## Storage Breakdown

Understanding where storage is consumed helps with capacity planning.

**Mainnet reference (seeded, pruned, default retention):**

| Component | Storage Used | Description |
|-----------|--------------|-------------|
| Aerospike | ~400 GB | UTXO set (~340M records) |
| Blob Storage | ~600 GB | Transactions and subtrees |
| PostgreSQL | < 1 GB | Block headers and chain state |
| Prometheus | ~1 GB | Metrics (varies with retention) |
| **Total** | **~1 TB** | |

**Storage type requirements:**

- **Aerospike**: NVMe SSD required. High random IOPS for UTXO lookups.
- **Blob Storage**: SSD recommended. Sequential read/write for transaction data.
- **PostgreSQL**: SSD recommended. Standard database workload.

## Network Configuration

### UDP Buffer Sizes (Required for QUIC)

Teranode's P2P service uses QUIC transport via libp2p, which requires increased UDP buffer sizes for stable operation. The default Linux kernel buffer limits are too small for high-bandwidth QUIC transfers, causing packet drops and connection instability.

**Apply these settings on all hosts running Teranode:**

```bash
# Create a dedicated sysctl configuration file
cat << 'EOF' | sudo tee /etc/sysctl.d/99-teranode.conf
net.core.rmem_max=7500000
net.core.wmem_max=7500000
EOF

# Apply immediately
sudo sysctl --system
```

**For Docker deployments:** These settings must be applied on the host machine, not inside containers. Containers inherit the host's kernel limits.

**For Kubernetes deployments:** Apply these settings to all worker nodes in your cluster, or configure via a DaemonSet that runs a privileged init container.

| Parameter | Value | Description |
|-----------|-------|-------------|
| `net.core.rmem_max` | 7500000 | Maximum UDP receive buffer size (~7.5 MB) |
| `net.core.wmem_max` | 7500000 | Maximum UDP send buffer size (~7.5 MB) |

**Why this is needed:** QUIC is a UDP-based protocol. When the receive buffer fills up, the kernel drops incoming packets. The quic-go library attempts to request larger buffers but is constrained by the kernel maximum. Without this configuration, you may see warnings like: `failed to sufficiently increase receive buffer size`.

## Important Notes

1. **Seeding vs Full Sync**: These requirements assume a seeded node. Full blockchain sync from genesis requires additional temporary storage and significantly more time. See the [Blockchain Synchronization Guide](docker/minersHowToSyncTheNode.md).

2. **Pruning**: Teranode prunes old transaction data by default. Disabling pruning or increasing retention will require proportionally more blob storage.

3. **Scaling**: These are baseline requirements. High-throughput deployments targeting 1M+ tps require significantly more resources and horizontal scaling via Kubernetes.

4. **Aerospike Memory**: Aerospike stores its primary index in memory. The ~20 GB RAM requirement for mainnet UTXO index will grow as the UTXO set grows.

5. **Docker Compose vs Kubernetes**: Docker Compose runs all services on a single node and is suitable for production workloads at current mainnet transaction volumes. However, you cannot scale individual services independently. For horizontal scaling or high-availability deployments, use Kubernetes.
