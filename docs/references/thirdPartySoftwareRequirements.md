# Teranode Third Party Software Requirements

## Index

- [Introduction](#introduction)
- [Apache Kafka](#apache-kafka)
    - [What is Kafka?](#what-is-kafka)
    - [Kafka in BSV Teranode](#kafka-in-bsv-teranode)
- [PostgreSQL](#postgresql)
    - [What is PostgreSQL?](#what-is-postgresql)
    - [PostgreSQL in Teranode](#postgresql-in-teranode)
- [Aerospike](#aerospike)
    - [What is Aerospike?](#what-is-aerospike)
    - [How Aerospike is Used in Teranode](#how-aerospike-is-used-in-teranode)
- [Shared Storage](#shared-storage)
- [Grafana and Prometheus](#grafana-and-prometheus)
    - [What is Grafana?](#what-is-grafana)
    - [What is Prometheus?](#what-is-prometheus)
    - [Grafana and Prometheus in Teranode](#grafana-and-prometheus-in-teranode)
- [Jaeger](#jaeger)
    - [What is Jaeger?](#what-is-jaeger)
    - [Jaeger in Teranode](#jaeger-in-teranode)
- [Additional Components](#additional-components)

## Introduction

Teranode relies on a number of third-party software dependencies, some of which can be sourced from different vendors.

BSV provides both a `docker compose` that initialises all dependencies within a single server node, and a `Kubernetes operator` that provides a production-live multi-node setup.


This section will outline the various vendors in use in Teranode.


## Apache Kafka



### What is Kafka?

Apache Kafka is an open-source platform for handling real-time data feeds, designed to provide high-throughput, low-latency data pipelines and stream processing. It supports publish-subscribe messaging, fault-tolerant storage, and real-time stream processing.



To know more, please refer to the Kafka official site: https://kafka.apache.org/.



### Kafka in BSV Teranode

Kafka serves as the central messaging system for inter-service communication:

- **Transaction Processing**: `txmeta`, `validatortxs`, `rejectedtx` topics
- **Block Processing**: `blocks`, `blocks-final`, `invalid-blocks` topics  
- **Subtree Management**: `subtrees`, `invalid-subtrees` topics
- **Legacy Support**: `legacy-inv` topic for Bitcoin protocol compatibility

**Configuration**: See `settings/settings.go` for Kafka settings and `deploy/docker/base/docker-services.yml` for deployment configuration.





## PostgreSQL




### What is PostgreSQL?

Postgres, short for PostgreSQL, is an advanced, open-source relational database management system (RDBMS). It is known for its robustness, extensibility, and standards compliance. Postgres supports SQL for querying and managing data, and it also provides features like:

- **ACID Compliance:** Ensures reliable transactions.
- **Complex Queries:** Supports joins, subqueries, and complex operations.
- **Extensibility:** Allows custom functions, data types, operators, and more.
- **Concurrency:** Handles multiple transactions simultaneously with high efficiency.
- **Data Integrity:** Supports constraints, triggers, and foreign keys to maintain data accuracy.



To know more, please refer to the PostgreSQL official site: https://www.postgresql.org/



### PostgreSQL in Teranode

PostgreSQL serves as the primary **Blockchain Storage** backend for:

- **Block Storage**: Complete block data, headers, and metadata
- **Chain State Management**: Current blockchain state and chain tips
- **Block Validation**: Validation results and chain reorganizations
- **Transaction Indexing**: Efficient transaction data access

**Note**: Also supports SQLite backend through the same SQL store interface.

**Configuration**: See `stores/blockchain/sql/` for implementation details.



## Aerospike



### What is Aerospike?

Aerospike is an open-source, high-performance, NoSQL database designed for real-time analytics and high-speed transaction processing. It is known for its ability to handle large-scale data with low latency and high reliability. Key features include:

- **High Performance:** Optimized for fast read and write operations.
- **Scalability:** Easily scales horizontally to handle massive data loads.
- **Consistency and Reliability:** Ensures data consistency and supports ACID transactions.
- **Hybrid Memory Architecture:** Utilizes both RAM and flash storage for efficient data management.



### How Aerospike is Used in Teranode

Aerospike serves as the high-performance **UTXO (Unspent Transaction Output) store** for:

- **UTXO Management**: Fast storage and retrieval of unspent transaction outputs
- **Transaction Validation**: Quick lookups for input validation
- **Conflict Detection**: Double-spend detection and management
- **Chain Reorganization**: UTXO rollback support during reorgs
- **Batch Operations**: Bulk UTXO operations for block processing

**Additional Features**: Custom Lua scripts, cleanup services, Prometheus metrics, and Aerospike-Kafka connector for CDC.

**Configuration**: See `stores/utxo/aerospike/` and `deploy/docker/base/docker-services.yml`.



## Shared Storage

Teranode uses a flexible blob storage system for **Subtree** and **Transaction** data shared across microservices.

**Supported Backends:**

- **File System**: Local or shared filesystem (`file://` URLs)
- **S3-Compatible**: AWS S3 or compatible object storage
- **HTTP**: RESTful blob storage services

**Production Options:**
- AWS FSx for Lustre, NFS, or S3 object storage

**Development:**
- Docker volumes or local filesystem

**Configuration**: See `stores/blob/` for backend implementations and URL-based configuration.


## Grafana and Prometheus



### What is Grafana?

**Grafana** is an open-source platform used for monitoring, visualization, and analysis of data. It allows users to create and share interactive dashboards to visualize real-time data from various sources.



### What is Prometheus?

**Prometheus** is an open-source systems monitoring and alerting toolkit. It is particularly well-suited for monitoring dynamic and cloud-native environments.



Grafana and Prometheus are used together to provide a comprehensive monitoring and visualization solution. Prometheus scrapes metrics from configured targets at regular intervals, and stores them in a its time series database. Grafana then connects to Prometheus as a data source.


Users can create dashboards in Grafana to visualize the metrics collected by Prometheus. Additionally, both Prometheus and Grafana support alerting.


### Grafana and Prometheus in Teranode

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
