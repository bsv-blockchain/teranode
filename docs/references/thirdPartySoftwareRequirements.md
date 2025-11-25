# Teranode Third Party Software Requirements

## Index

- [Introduction](#introduction)
- [Apache Kafka](#apache-kafka)
    - [What is Kafka?](#what-is-kafka)
    - [Kafka in BSV Teranode](#kafka-in-bsv-teranode)
- [PostgreSQL](#postgresql)
    - [What is PostgreSQL?](#what-is-postgresql)
    - [PostgreSQL in Teranode](#postgresql-in-teranode)
- [UTXO Storage](#utxo-storage)
    - [Aerospike](#aerospike)
    - [SQL-Based UTXO Storage](#sql-based-utxo-storage)
- [Blob Storage](#blob-storage)
    - [Shared Filesystem Storage](#shared-filesystem-storage)
    - [S3-Compatible Storage](#s3-compatible-storage)
    - [HTTP Blob Storage](#http-blob-storage)
    - [File System Storage](#file-system-storage)
- [Transaction Metadata Cache](#transaction-metadata-cache)
- [Grafana and Prometheus](#grafana-and-prometheus)
    - [What is Grafana?](#what-is-grafana)
    - [What is Prometheus?](#what-is-prometheus)
    - [Grafana and Prometheus in Teranode](#grafana-and-prometheus-in-teranode)
- [Jaeger](#jaeger)
    - [What is Jaeger?](#what-is-jaeger)
    - [Jaeger in Teranode](#jaeger-in-teranode)
- [Additional Components](#additional-components)

## Introduction

Teranode relies on a number of third-party software dependencies, some of which can be sourced from different vendors. The modular architecture allows operators to choose between different storage backends and configurations based on their specific requirements, performance needs, and infrastructure constraints.

BSV provides both a `docker compose` that initializes all dependencies within a single server node, and a `Kubernetes operator` that provides a production-ready multi-node setup.

This document outlines the various third-party dependencies used in Teranode, their roles, and the available alternatives for different deployment scenarios.

## Apache Kafka

### What is Kafka?

Apache Kafka is an open-source platform for handling real-time data feeds, designed to provide high-throughput, low-latency data pipelines and stream processing. It supports publish-subscribe messaging, fault-tolerant storage, and real-time stream processing.

To know more, please refer to the Kafka official site: <https://kafka.apache.org/>.

### Kafka in BSV Teranode

In BSV Teranode, Kafka serves as the event-driven messaging backbone for asynchronous communication between microservices. It enables services to process blockchain events independently and at scale.

- **Transaction Processing**: `txmeta`, `validatortxs`, `rejectedtx` topics
- **Block Processing**: `blocks`, `blocks-final`, `invalid-blocks` topics  
- **Subtree Management**: `subtrees`, `invalid-subtrees` topics
- **Legacy Support**: `legacy-inv` topic for Bitcoin protocol compatibility

**Configuration**: See `settings/settings.go` for Kafka settings and `deploy/docker/base/docker-services.yml` for deployment configuration.


**Kafka Topics Used:**

- **blocks**: Block propagation events for new blocks received by the network
- **blocks-final**: Finalized block events after validation is complete
- **invalid-blocks**: Notifications of blocks that failed validation
- **invalid-subtrees**: Notifications of merkle subtrees that failed validation
- **subtrees**: Merkle subtree data for transaction validation
- **txmeta**: Transaction metadata for caching and quick lookups
- **validatortxs**: Transactions submitted to the validator service
- **rejectedtx**: Transactions rejected during validation
- **legacy-inv**: Legacy Bitcoin protocol inventory messages

**Services Using Kafka:**

- **Blockchain Service**: Publishes block events when blocks are validated and stored
- **Block Validation**: Consumes blocks topic, publishes to blocks-final or invalid-blocks
- **Subtree Validation**: Processes subtree validation requests
- **Validator**: Consumes transactions for validation, publishes validation results
- **Legacy Service**: Bridges legacy Bitcoin protocol messages to Kafka topics

Kafka's distributed architecture ensures data consistency and fault tolerance across the Teranode network, enabling horizontal scaling of transaction and block processing.

## PostgreSQL

### What is PostgreSQL?

Postgres, short for PostgreSQL, is an advanced, open-source relational database management system (RDBMS). It is known for its robustness, extensibility, and standards compliance. Postgres supports SQL for querying and managing data, and it also provides features like:

- **ACID Compliance:** Ensures reliable transactions.
- **Complex Queries:** Supports joins, subqueries, and complex operations.
- **Extensibility:** Allows custom functions, data types, operators, and more.
- **Concurrency:** Handles multiple transactions simultaneously with high efficiency.
- **Data Integrity:** Supports constraints, triggers, and foreign keys to maintain data accuracy.

To know more, please refer to the PostgreSQL official site: <https://www.postgresql.org/>

### PostgreSQL in Teranode

In Teranode, PostgreSQL is the primary backend for **Blockchain Storage**, storing the complete blockchain state and block metadata.

**Schema Overview:**

- **blocks table**: Stores complete block headers and metadata
    - Block hash, previous hash, merkle root
    - Block height, chain work (cumulative proof-of-work)
    - Transaction count, block size, subtree count
    - Validation status flags (invalid, mined_set, subtrees_set)
    - Coinbase transaction data
    - Timestamps and peer information

- **state table**: Stores blockchain state information
    - Key-value storage for FSM (Finite State Machine) state
    - Chain tip information
    - Network parameters

PostgreSQL serves as the primary **Blockchain Storage** backend for:

- **Block Storage**: Complete block data, headers, and metadata
- **Chain State Management**: Current blockchain state and chain tips
- **Block Validation**: Validation results and chain reorganizations
- **Transaction Indexing**: Efficient transaction data access

**Note**: Also supports SQLite backend through the same SQL store interface.

**Configuration**: See `stores/blockchain/sql/` for implementation details.

- **ACID guarantees**: Critical for maintaining blockchain consistency during reorganizations
- **Foreign key relationships**: Efficiently maintains parent-child block relationships
- **Complex queries**: Supports sophisticated chain state queries needed for consensus logic
- **Indexing**: Fast lookups by hash, height, and chain work for block validation
- **Reliability**: Production-proven for critical financial data storage

PostgreSQL provides a reliable and efficient database solution for handling large volumes of block data while maintaining the integrity required for blockchain consensus operations.

## UTXO Storage

The Unspent Transaction Output (UTXO) store is a critical component that tracks all spendable transaction outputs in the blockchain. This requires high-performance, low-latency storage capable of handling millions of read/write operations per second.

### Aerospike

#### What is Aerospike?

Aerospike is an open-source, high-performance, NoSQL database designed for real-time analytics and high-speed transaction processing. It is known for its ability to handle large-scale data with low latency and high reliability. Key features include:

- **High Performance:** Optimized for fast read and write operations.
- **Scalability:** Easily scales horizontally to handle massive data loads.
- **Consistency and Reliability:** Ensures data consistency and supports ACID transactions.
- **Hybrid Memory Architecture:** Utilizes both RAM and flash storage for efficient data management.

#### How Aerospike is Used in Teranode

In the context of Teranode, Aerospike is the recommended production backend for **UTXO (Unspent Transaction Output) storage**, which requires a robust, high-performance, and reliable solution.

**Why Aerospike for UTXO Storage?**

- **Sub-millisecond latency**: Critical for validating transactions at scale (1M+ tx/s target)
- **Horizontal scalability**: UTXO set grows with blockchain size, requiring distributed storage
- **Memory + SSD hybrid**: Balances performance with cost-effective storage
- **High throughput**: Handles concurrent reads/writes from multiple validator instances
- **Data replication**: Ensures UTXO availability across cluster nodes

Aerospike's performance characteristics make it ideal for Teranode's high-throughput transaction validation requirements.

### SQL-Based UTXO Storage

For development, testing, or lower-throughput environments, Teranode supports **SQL-based UTXO storage** using PostgreSQL or SQLite backends.

**When to Use SQL UTXO Storage:**

- **Development environments**: Easier to set up and inspect data
- **Testing**: Simpler configuration for integration tests
- **Small-scale deployments**: Lower operational complexity
- **Budget constraints**: No need for dedicated Aerospike cluster

**Trade-offs:**

- Lower throughput compared to Aerospike
- Higher latency for UTXO lookups
- Not recommended for production high-volume scenarios
- Suitable for nodes processing < 10,000 transactions per second

The SQL backend implements the same UTXO store interface, allowing seamless switching between storage engines via configuration.

## Blob Storage

Teranode requires robust blob storage for **transaction data** and **merkle subtrees**. Multiple backend options are available depending on deployment requirements.

### Shared Filesystem Storage

For production deployments, Teranode uses high-performance shared filesystems like **Lustre** (AWS FSx for Lustre) for optimal performance and scalability.

**Benefits of Using Lustre:**

- **High Availability:** Ensures continuous access to shared data across multiple service instances
- **Low Latency:** Provides sub-millisecond latency for consistent filesystem state
- **Automatic Data Archiving:** Facilitates seamless data transfer to S3 for long-term storage
- **Scalability:** Supports scalable data sharing between various microservices
- **Concurrent Access:** Multiple services can read/write simultaneously without conflicts

**Services Accessing Blob Storage:**

- **Block Validation**: Reads subtrees for block validation
- **Subtree Validation**: Writes validated subtrees
- **Validator**: Reads transactions for validation
- **Asset Server**: Serves transaction data to external clients
- **UTXO Persister**: Archives UTXO snapshots

### S3-Compatible Storage

Teranode supports **S3-compatible blob storage** for cloud-native deployments or long-term archival.

**Use Cases:**

- Cloud deployments (AWS S3, Google Cloud Storage, Azure Blob Storage)
- Long-term transaction archival
- Disaster recovery and backup
- Cost-effective storage for infrequently accessed data

**Configuration:** Set blob store URL scheme to `s3://` in settings.conf

### HTTP Blob Storage

For distributed deployments or CDN-backed scenarios, Teranode supports **HTTP/HTTPS blob storage** where blobs are fetched via HTTP requests.

**Use Cases:**

- Content delivery networks (CDN) for transaction data distribution
- Read-only blob access from remote services
- Hybrid architectures with external blob storage services

**Configuration:** Set blob store URL scheme to `http://` or `https://` in settings.conf

### File System Storage

For development and testing environments, **local filesystem storage** provides the simplest blob storage option.

**Use Cases:**

- Local development with `make dev`
- Docker Compose testing environments
- Single-node deployments
- CI/CD integration tests

**Configuration:** Set blob store URL scheme to `file://` in settings.conf

**Note:** When using Docker Compose, shared Docker storage is automatically managed by `docker compose`, providing an accessible testing environment while allowing for essential functionality and performance evaluation.

## Transaction Metadata Cache

Teranode includes a **transaction metadata cache** (`txmetacache`) that stores frequently accessed transaction information to reduce database and blob storage queries.

**Purpose:**

- Caches transaction hashes, block heights, and validation status
- Reduces latency for transaction lookups
- Improves performance during block validation and transaction queries

**Configuration:**

- Cache size configurable via settings (small for tests, large for production)
- Uses in-memory storage for fast access
- Automatically expires old entries based on TTL

This cache layer significantly improves Teranode's performance when handling high query volumes from external clients or internal validation processes.

## Grafana and Prometheus

### What is Grafana?

**Grafana** is an open-source platform used for monitoring, visualization, and analysis of data. It allows users to create and share interactive dashboards to visualize real-time data from various sources.

### What is Prometheus?

**Prometheus** is an open-source systems monitoring and alerting toolkit. It is particularly well-suited for monitoring dynamic and cloud-native environments.

Grafana and Prometheus are used together to provide a comprehensive monitoring and visualization solution. Prometheus scrapes metrics from configured targets at regular intervals, and stores them in its time series database. Grafana then connects to Prometheus as a data source.

Users can create dashboards in Grafana to visualize the metrics collected by Prometheus. Additionally, both Prometheus and Grafana support alerting.

### Grafana and Prometheus in Teranode

In the context of Teranode, Grafana and Prometheus are used to provide comprehensive monitoring and visualization of the blockchain node's performance, health, and various metrics.

**Metrics Monitored:**

- **Block processing rate**: Blocks validated per second
- **Transaction throughput**: Transactions processed per second
- **UTXO operations**: Reads, writes, and cache hit rates
- **Kafka message rates**: Producer/consumer throughput per topic
- **Service health**: Up/down status and response times
- **Database performance**: Query latencies, connection pool usage
- **Memory and CPU usage**: Per-service resource consumption

**Dashboards:**

Teranode includes pre-configured Grafana dashboards located in `compose/grafana/datasources/` that provide real-time visibility into:

- Overall system health and status
- Service-level performance metrics
- Blockchain synchronization progress
- Transaction validation pipeline metrics

These monitoring tools are essential for operators to ensure optimal performance and quickly identify issues in production deployments.

## Docker Compose

**Docker Compose** is used extensively in Teranode for orchestrating multi-container development and testing environments.

**Purpose:**

- **Development**: `make dev` runs Teranode services with Docker Compose
- **Testing**: Integration tests use Docker Compose to spin up dependencies
- **CI/CD**: Continuous integration environments use compose for reproducible builds
- **Local Deployment**: Single-machine deployments for testing or small-scale nodes

**Managed Services:**

Docker Compose configurations automatically provision:

- Apache Kafka and Zookeeper
- PostgreSQL database
- Aerospike clusters
- Grafana and Prometheus
- Shared storage volumes
- Teranode microservices

**Configuration Files:**

Multiple compose files are available for different scenarios:

- `docker-compose.yml`: Main development environment
- `docker-compose.e2etest.yml`: End-to-end testing setup
- `docker-compose-ss.yml`: Shared storage testing
- `docker-compose-chainintegrity.yml`: Chain integrity testing

Comprehensive monitoring and observability for Teranode:

**Prometheus Metrics:**

- Service metrics from each Teranode component
- System metrics (CPU, memory, disk, network)
- Database metrics (PostgreSQL, Aerospike)
- Kafka metrics (throughput, lag, broker health)
- Business metrics (transaction rates, block validation times)

**Grafana Features:**

- Pre-configured dashboards for key components
- Real-time blockchain operations monitoring
- Configurable alerting
- Multi-node deployment support

**Configuration**: See `deploy/docker/base/docker-services.yml` for setup and `deploy/docker/base/grafana_dashboards/` for dashboard definitions.


## Jaeger

### What is Jaeger?

**Jaeger** is an open-source, distributed tracing system originally developed by Uber. It is used for monitoring and troubleshooting microservices-based distributed systems. Jaeger helps track requests as they flow through multiple services, providing insights into performance bottlenecks and system behavior.

Key features include:

- **Distributed Context Propagation**: Tracks requests across service boundaries
- **Performance Monitoring**: Identifies slow operations and bottlenecks
- **Root Cause Analysis**: Helps diagnose issues in complex distributed systems
- **Service Dependency Analysis**: Visualizes service interactions and dependencies

### Jaeger in Teranode

Distributed tracing across Teranode's microservices architecture:

**Tracing Capabilities:**

- Service-to-service communication
- Database operations (PostgreSQL, Aerospike)
- Kafka message processing
- Block processing pipeline
- UTXO operations and transaction validation

**Features**: OpenTelemetry integration, configurable sampling, web UI for trace analysis.

**Configuration**: See `util/tracing/` for implementation and Docker compose files for deployment setup.


## Additional Components

**RedPanda Console**: Web-based Kafka management interface for topic management and message inspection.

**Nginx Asset Cache**: HTTP caching layer for asset service responses with reverse proxy capabilities.

**OpenTelemetry**: Observability framework used with Jaeger for comprehensive tracing.

**Container Orchestration**: Docker Compose for development, Kubernetes for production deployments.

**Configuration**: See respective Docker compose files and Kubernetes manifests in `deploy/` directory.
