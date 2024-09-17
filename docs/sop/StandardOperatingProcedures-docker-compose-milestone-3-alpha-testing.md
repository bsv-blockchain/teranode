# Standard Operating Procedure (Alpha Testing)

## Index

1. [Glossary of Teranode Terms](#1-glossary-of-teranode-terms)
2. [Introduction](#2-introduction)
- [2.1. Purpose of the SOP](#21-purpose-of-the-sop)
- [2.2. Scope and Applicability](#22-scope-and-applicability)
- [2.3. The Teranode Microservices](#23-the-teranode-microservices)
3. [Node Setup](#3-node-setup)
- [3.1. Hardware Requirements](#31-hardware-requirements)
- [3.2. Software Requirements](#32-software-requirements)
    - [3.2.1. Apache Kafka](#321-apache-kafka)
    - [3.2.2. PostgreSQL](#322-postgresql)
    - [3.2.3. Aerospike](#323-aerospike)
    - [3.2.4. Shared Storage](#324-shared-storage)
    - [3.2.5. Grafana + Prometheus](#325-grafana--prometheus)
- [3.3. Network Considerations](#33-network-considerations)
4. [Installation Process](#4-installation-process)
- [4.1. Teranode Initial Block Download (IBD)](#41-teranode-initial-block-download-ibd)
    - [4.1.1 Full P2P Download](#411-full-p2p-download)
    - [4.1.2. Initial Data Set Installation](#412-initial-data-set-installation)
- [4.2. Teranode Installation Types](#42-teranode-installation-types)
    - [4.2.1. Pre-built Binaries](#421-pre-built-binaries)
    - [4.2.2. Docker Container](#422-docker-container)
    - [4.2.3. Docker Compose](#423-docker-compose)
- [4.3. Installing Teranode with Docker Compose](#43-installing-teranode-with-docker-compose)
5. [Configuration](#5-configuration)
- [5.1. Configuring Setting Files](#51-configuring-setting-files)
6. [Node Operation](#6-node-operation)
- [6.1. Docker Compose Starting and Stopping the Node](#61-docker-compose---starting-and-stopping-the-node)
    - [6.1.1. Starting the Node](#611-starting-the-node)
    - [6.1.2. Stopping the Node](#612-stopping-the-node)
- [6.2. Syncing the Blockchain](#62-syncing-the-blockchain)
- [6.3. How to Interact with the Node](#63-how-to-interact-with-the-node)
    - [6.3.1. Teranode RPC HTTP API](#631-teranode-rpc-http-api)
    - [6.3.2. Teranode Asset Server HTTP API](#632-teranode-asset-server-http-api)
7. [Maintenance](#7-maintenance)
- [7.1. Updating Teranode to a New Version](#71-updating-teranode-to-a-new-version)
    - [7.1.1. Docker Compose](#711-docker-compose)
- [7.2. Managing Disk Space](#72-managing-disk-space)
- [7.3. Backing Up Data](#73-backing-up-data)
8. [Troubleshooting](#8-troubleshooting)
- [8.1. Health Checks and System Monitoring](#81-health-checks-and-system-monitoring)
    - [8.1.1. Health Checks](#811-health-checks)
    - [8.1.2. Monitoring System Resources](#812-monitoring-system-resources)
    - [8.1.3. Check Logs for Errors](#813-check-logs-for-errors)
- [8.2 Recovery Procedures](#82-recovery-procedures)
    - [8.2.1. Third Party Component Failure](#821-third-party-component-failure)
9. [Security Best Practices](#9-security-best-practices)
- [9.1. Firewall Configuration](#91-firewall-configuration)
- [9.2. Regular System Updates](#92-regular-system-updates)
- [10. Bug Reporting](#10-bug-reporting)

----



Version 1.0.2 - Last Issued - 16 September 2024

----

## 1. Glossary of Teranode Terms

**Asset Server**: A component of Teranode that manages and serves information about blockchain assets.

**Block Assembly**: The process of combining validated transactions into a new block.

**Block Persister**: A component responsible for storing validated blocks in the blockchain database.

**Block Validation**: The process of verifying that a block and all its transactions conform to the network rules.

**Blockchain**: A distributed ledger that records all transactions across a network of computers.

**BSV**: Bitcoin Satoshi Vision, the blockchain network that Teranode is designed to support.

**Coinbase**: The first transaction in a block, which creates new coins as a reward for the miner.

**Docker**: A platform used to develop, ship, and run applications inside containers.

**Docker Compose**: A tool for defining and running multi-container Docker applications.

**gRPC**: A high-performance, open-source universal RPC framework.

**Initial Block Download (IBD)**: The process of downloading and validating the entire blockchain when setting up a new node.

**Kafka**: A distributed streaming platform used in Teranode for handling real-time data feeds.

**Microservices**: An architectural style that structures an application as a collection of loosely coupled services.

**Miner**: A node on the network that processes transactions and creates new blocks.

**P2P (Peer-to-Peer)**: A decentralized network where nodes can act as both clients and servers.

**PostgreSQL**: An open-source relational database used in Teranode for storing blockchain data.

**Propagation**: The process of spreading transactions and blocks across the network.

**RPC (Remote Procedure Call)**: A protocol that one program can use to request a service from a program located on another computer on a network.

**Subtree**: A portion of the transaction tree within a block.

**Subtree Validation**: The process of verifying the validity of a subtree within a block.

**Teranode**: A high-performance implementation of the Bitcoin SV node software, designed for scalability.

**UTXO (Unspent Transaction Output)**: An output of a blockchain transaction that has not been spent, i.e., used as an input in a new transaction.

**UTXO Set**: The set of all unspent transaction outputs on the blockchain.

**Validator**: A component that checks the validity of transactions and blocks according to the network rules. This runs as a library within the Propagation service.



## 2. Introduction



### 2.1. Purpose of the SOP



The purpose of this Standard Operating Procedure (SOP) is to provide a comprehensive guide for miners to set up and run a BSV Bitcoin node using Teranode in LISTENER mode during the alpha testing phase. This document aims to:

1. Introduce miners to the Teranode architecture and its dependencies.
2. Provide step-by-step instructions for installing and configuring a single-server Teranode setup.
3. Explain the various microservices and their relationships within the Teranode ecosystem.
4. Guide users through basic maintenance operations and monitoring procedures.
5. Offer insights into optimal settings and configuration options for different operational modes.

This SOP serves as a foundational document to help stakeholders familiarize themselves with the Teranode software, its requirements, and its operation in a production-like environment.



### 2.2. Scope and Applicability



This SOP is applicable to:

1. Miners and node operators participating in the alpha testing phase of Teranode.

2. Single-node Teranode setups running in LISTENER mode.

3. Deployments using either a binary executable or `Docker Compose` setup.

4. Configurations designed to connect to and process the BSV mainnet with current production load.



The document covers:

1. Hardware and software requirements for running a Teranode instance through a single-server `docker compose`.
2. Installation and configuration of Teranode and its dependencies (Aerospike, PostgreSQL, Kafka, Prometheus, and Grafana).
3. Basic operational procedures and maintenance tasks.
4. Monitoring and performance optimization guidelines.
5. Security considerations for running a Teranode instance.



This SOP does not cover:

1. Advanced clustering configurations.

2. Scaled-out Teranode configurations. The Teranode installation described in this document is capable of handling the regular BSV production load, not the scaled-out 1 million tps configuration.

3. Custom mining software integration (the SOP assumes use of the provided bitcoind miner).

4. Advanced network configurations.

5. Any sort of source code access, build or manipulation of any kind.



The information and procedures outlined in this document are specific to the alpha testing phase and may be subject to change as the Teranode software evolves.





### 2.3. The Teranode Microservices



BSV Teranode is built on a microservices architecture, consisting of several core services and external dependencies. This design allows for greater scalability, flexibility, and performance compared to traditional monolithic node structures.

Teranode's architecture includes:

1. **Core Teranode Services**: These are the primary components that handle the essential functions of a Bitcoin node, such as block, subtree and transaction validation, block assembly, and blockchain management.
2. **Overlay Services**: Additional services that provide support for specific functions like legacy network compatibility and node discovery.
3. **External Dependencies**: Third-party software components that Teranode relies on for various functionalities, including data storage, message queuing, and monitoring.




![UBSV_Overview_Public.png](../architecture/img/UBSV_Overview_Public.png)



While this document will touch upon these components as needed for setup and operation, it is not intended to provide a comprehensive architectural overview. For a detailed explanation of Teranode's architecture and the interactions between its various components, please refer to the `teranode-architecture-1.5.pdf` document.



## 3. Node Setup





### 3.1. Hardware Requirements



The Teranode team will provide you with current hardware recommendations. These recommendations will be:

1. Tailored to your specific configuration settings
2. Designed to handle the expected production transaction volume
3. Updated regularly to reflect the latest performance requirements

This ensures your system is appropriately equipped to manage the projected workload efficiently.




### 3.2. Software Requirements

Teranode relies on a number of third-party software dependencies, some of which can be sourced from different vendors.

BSV provides both a `docker compose` that initialises all dependencies within a single server node, and a `Kubernetes operator` that provides a production-live multi-node setup. This document covers the `docker compose` approach.


This section will outline the various vendors in use in Teranode.


#### 3.2.1. Apache Kafka



##### What is Kafka?

Apache Kafka is an open-source platform for handling real-time data feeds, designed to provide high-throughput, low-latency data pipelines and stream processing. It supports publish-subscribe messaging, fault-tolerant storage, and real-time stream processing.



To know more, please refer to the Kafka official site: https://kafka.apache.org/.



##### Kafka in BSV Teranode

In BSV Teranode, Kafka is used to manage and process large volumes of transaction data efficiently. Kafka serves as a data pipeline, ingesting high volumes of transaction data from multiple sources in real-time. Kafka’s distributed architecture ensures data consistency and fault tolerance across the network.





#### 3.2.2. PostgreSQL




##### What is PostgreSQL?

Postgres, short for PostgreSQL, is an advanced, open-source relational database management system (RDBMS). It is known for its robustness, extensibility, and standards compliance. Postgres supports SQL for querying and managing data, and it also provides features like:

- **ACID Compliance:** Ensures reliable transactions.
- **Complex Queries:** Supports joins, subqueries, and complex operations.
- **Extensibility:** Allows custom functions, data types, operators, and more.
- **Concurrency:** Handles multiple transactions simultaneously with high efficiency.
- **Data Integrity:** Supports constraints, triggers, and foreign keys to maintain data accuracy.



To know more, please refer to the PostgreSQL official site: https://www.postgresql.org/



##### PostgreSQL in Teranode

In Teranode, PostgreSQL is used for **Blockchain Storage**. Postgres stores the blockchain data, providing a reliable and efficient database solution for handling large volumes of block data.



#### 3.2.3. Aerospike



##### What is Aerospike?

Aerospike is an open-source, high-performance, NoSQL database designed for real-time analytics and high-speed transaction processing. It is known for its ability to handle large-scale data with low latency and high reliability. Key features include:

- **High Performance:** Optimized for fast read and write operations.
- **Scalability:** Easily scales horizontally to handle massive data loads.
- **Consistency and Reliability:** Ensures data consistency and supports ACID transactions.
- **Hybrid Memory Architecture:** Utilizes both RAM and flash storage for efficient data management.



##### How Aerospike is Used in Teranode

In the context of Teranode, Aerospike is utilized for **UTXO (Unspent Transaction Output) storage**, which requires of a robust high performance and reliable solution.



#### 3.2.4. Shared Storage



Teranode requires a robust and scalable shared storage solution to efficiently manage its critical **Subtree** and **Transaction** data. This shared storage is accessed and modified by various microservices within the Teranode ecosystem.



In a full production deployment, Teranode is designed to use Lustre (or a similar high-performance shared filesystem) for optimal performance and scalability. Lustre is a clustered, high-availability, low-latency shared filesystem provided by AWS FSx for Lustre service.



Benefits of Using Lustre with Teranode:



- **High Availability:** Ensures continuous access to shared data.
- **Low Latency:** Provides sub-millisecond latency for consistent filesystem state.
- **Automatic Data Archiving:** Facilitates seamless data transfer to S3 for long-term storage.
- **Scalability:** Supports scalable data sharing between various services.



Note - if using Docker Compose, the shared Docker storage, which is automatically managed by `docker compose`, is used instead. This approach provides a more accessible testing environment while still allowing for essential functionality and performance evaluation.



#### 3.2.5. Grafana + Prometheus



##### What is Grafana?

**Grafana** is an open-source platform used for monitoring, visualization, and analysis of data. It allows users to create and share interactive dashboards to visualize real-time data from various sources.



##### What is Prometheus?

**Prometheus** is an open-source systems monitoring and alerting toolkit. It is particularly well-suited for monitoring dynamic and cloud-native environments.



Grafana and Prometheus are used together to provide a comprehensive monitoring and visualization solution. Prometheus scrapes metrics from configured targets at regular intervals, and stores them in a its time series database. Grafana then connects to Prometheus as a data source.



Users can create dashboards in Grafana to visualize the metrics collected by Prometheus. Additionally, both Prometheus and Grafana support alerting.



##### Grafana and Prometheus in Teranode

In the context of Teranode, Grafana and Prometheus are used to provide comprehensive monitoring and visualization of the blockchain node’s performance, health, and various metrics.




### 3.3. Network Considerations



For the purposes of the alpha testing phase, running a Teranode BSV listener node has relatively low bandwidth requirements compared to many other server applications. The primary network traffic consists of receiving blockchain data, including new transactions and blocks.

While exact bandwidth usage can vary depending on network activity and node configuration, Bitcoin nodes typically require:

- Inbound: 5-50 GB per day
- Outbound: 50-150 GB per day

These figures are approximate. In general, any stable internet connection should be sufficient for running a Teranode instance.

Key network considerations:

1. Ensure your internet connection is reliable and has sufficient bandwidth to handle continuous data transfer.
2. Be aware that initial blockchain synchronization, depending on your installation method, may require higher bandwidth usage. If you synchronise automatically starting from the genesis block, you will have to download every block. However, the BSV recommended approach is to install a seed UTXO Set and blockchain.
3. Monitor your network usage to ensure it stays within your ISP's limits and adjust your node's configuration if needed.




## 4. Installation Process



### 4.1. Teranode Initial Block Download (IBD)



Teranode requires an initial block download (IBD) to function properly. There are two approaches for completing the IBD process.



#### 4.1.1 Full P2P Download



- Start the node and download all blocks from genesis using peer-to-peer (P2P) network.
- This method downloads the entire blockchain history.



**Pros:**

- Simple to implement
- Ensures the node has the complete blockchain history



**Cons:**

- Time-consuming process
- Can take 5-8 days, depending on available bandwidth



#### 4.1.2. Initial Data Set Installation



- Receive and install an initial data set comprising two files:
  1. Initial UTXO (Unspent Transaction Output) set
  2. Blockchain DB dump
- Start the application at a given block height and sync up from there



Pros:

- Significantly faster than full P2P download
- Allows for quicker node setup



Cons:

- Requires additional steps
- The data set must be validated, to ensure it has not been tampered with



To keep the initial data sets up-to-date for new nodes, the BSV Association, directly or through its partners, continuously creates and exports new data sets.

- Teranode typically makes available the UTXO set for a block 100 blocks prior to the current block
- This ensures the UTXO set won't change (is finalized)
- The data set is approximately 3/4 of a day old
- New nodes still need to catch up on the most recent blocks



Where possible, BSV recommends using the Initial Data Set Installation approach.





### 4.2. Teranode Installation Types



Teranode can be installed and run using four primary methods: pre-built binaries, Docker containers, Docker Compose, or using a Kubernetes Operator. Each method offers different levels of flexibility and ease of use, however Teranode only recommends using the Docker Compose (for testing) or Kubernetes Operator (for Production) approaches. No guidance or support is offered for the other variants. Only the Kubernetes operator approach is recommended for production usage. This document covers the `docker compose` approach only.



#### 4.2.1. Pre-built Binaries

- Pre-built executables are available for both arm64 and amd64 architectures.
- This method provides the most flexibility but requires manual setup of dependencies.
- Suitable for users who need fine-grained control over their installation.

#### 4.2.2. Docker Container

- A Docker image is published to a Docker registry, containing the Teranode binary and internal dependencies.
- This method offers a balance between ease of use and customization.
- Ideal for users familiar with Docker who want to integrate Teranode into existing container ecosystems.

#### 4.2.3. Docker Compose

- A `Docker Compose` file is provided to alpha testing miners.
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

Note: The `Docker Compose` method is recommended for testing in single node environments as it provides a consistent environment that mirrors the development setup. However, for production deployments or specific performance requirements, users need to consider using the Kubernetes operator approach (separate document).




### 4.3. Installing Teranode with Docker Compose



**Prerequisites**:

- Docker and Docker Compose installed on your system

- The Alpha testing Teranode docker compose file should be available in your system

- Sufficient disk space (at least 100GB recommended)

- A stable internet connection



**Step 1: Create Repository**

1. Open a terminal or command prompt.
2. Create the repository:
   ```
   cd teranode
   ```

3. Copy the docker compose file into the `teranode` folder. This was provided to you by the Teranode team.



**Step 2: Configure Environment**

1. Create a `.env` file in the root directory of the project.
2. While not required (your docker compose file will be preconfigured), the following line can be used to set the context:
   ```
   SETTINGS_CONTEXT_1=docker.m
   ```



**Step 3: Prepare Local Settings**

1. Create a `settings_local.conf` file in the root directory.
2. Unless instructed otherwise, please put the following content in it.



```
PROPAGATION_GRPC_PORT=8084
PROPAGATION_HTTP_PORT=8833
VALIDATOR_GRPC_PORT=8081
BLOCK_ASSEMBLY_GRPC_PORT=8085
SUBTREE_VALIDATION_GRPC_PORT=8086
BLOCK_VALIDATION_GRPC_PORT=8088
BLOCK_VALIDATION_HTTP_PORT=8188
BLOCKCHAIN_GRPC_PORT=8087
ASSET_GRPC_PORT=8091
ASSET_HTTP_PORT=8090
COINBASE_GRPC_PORT=8093
P2P_HTTP_PORT=9906
RPC_PORT=9292

propagation_grpcAddresses.docker.m=ubsv-propagation:${PROPAGATION_GRPC_PORT}
propagation_grpcAddress.docker.m=ubsv-propagation:${PROPAGATION_GRPC_PORT}
propagation_httpAddresses.docker.m=http://ubsv-propagation:${PROPAGATION_HTTP_PORT}
validator_grpcAddress.docker.m=localhost:${VALIDATOR_GRPC_PORT}
blockassembly_grpcAddress.docker.m=ubsv-blockassembly:${BLOCK_ASSEMBLY_GRPC_PORT}
subtreevalidation_grpcAddress.docker.m=ubsv-subtreevalidation:${SUBTREE_VALIDATION_GRPC_PORT}
blockvalidation_httpAddress.docker.m=http://ubsv-blockvalidation:${BLOCK_VALIDATION_HTTP_PORT}
blockvalidation_grpcAddress.docker.m=ubsv-blockvalidation:${BLOCK_VALIDATION_GRPC_PORT}
utxostore.docker.m=aerospike://aerospike:3000/test?WarmUp=32&ConnectionQueueSize=32&LimitConnectionsToQueueSize=true&MinConnectionsPerNode=8&expiration=300
utxoblaster_utxostore_aerospike.docker.m=aerospike://editor:password1234@aerospike:3000/utxo-store?WarmUp=0&ConnectionQueueSize=640&LimitConnectionsToQueueSize=true&MinConnectionsPerNode=64&expiration=900
blockchain_grpcAddress.docker.m=ubsv-blockchain:${BLOCKCHAIN_GRPC_PORT}
blockchain_store.docker.m=postgres://miner1:miner1@postgres:5432/ubsv1
asset_grpcListenAddress.docker.m=:${ASSET_GRPC_PORT}
asset_grpcAddress.docker.m=ubsv-asset:${ASSET_GRPC_PORT}
asset_httpAddress.docker.m=http://ubsv-asset:${ASSET_HTTP_PORT}${asset_apiPrefix}
coinbase_assetGrpcAddress.docker.m=ubsv-asset:${ASSET_GRPC_PORT}
coinbase_grpcAddress.docker.m=ubsv-coinbase:${COINBASE_GRPC_PORT}
coinbase_store.docker.m=postgres://coinbase1:coinbase1@postgres:5432/coinbase1
p2p_httpListenAddress.docker.m=:${P2P_HTTP_PORT}
p2p_httpAddress.docker.m=ubsv-p2p:${P2P_HTTP_PORT}
p2p_static_peers.docker.m=
coinbase_p2p_static_peers.docker.m=
rpc_listener_url=:${RPC_PORT}
```



**Step 4: Create Data Directories**

1. Create the following directories in the project root:
   ```
   mkdir -p data/ubsv/txstore data/ubsv/subtreestore data/ubsv/blockstore data/postgres data/aerospike/data
   ```



**Step 5: Pull Docker Images**

1. Pull the required Docker images:
   ```
   docker-compose pull
   ```



**Step 6: Build the Teranode Image**

1. Build the Teranode image using the provided Dockerfile:
   ```
   docker-compose build ubsv-builder
   ```



**Step 7: Start the Teranode Stack**

1. Launch the entire Teranode stack using Docker Compose:
   ```
   docker-compose up -d
   ```



**Step 8: Verify Services**

1. Check if all services are running correctly:
   ```
   docker-compose ps
   ```
2. Ensure all services show a status of "Up" or "Healthy".



**Step 9: Access Monitoring Tools**

1. **Grafana**: Access the Grafana dashboard at `http://localhost:3005`
- Default credentials are `admin/admin`



2. **Kafka Console**: Available internally on port 8080



3. **Prometheus**: Available internally on port 9090



**Step 10: Interact with Teranode**

- The various Teranode services expose different ports for interaction:
    - **Blockchain service**: Port 8082
    - **Asset service**: Ports 8090, 8091
    - **Miner service**: Ports 8089, 8092, 8099
    - **P2P service**: Ports 9905, 9906
    - **RPC service**: 9292



**Step 11: Logging and Troubleshooting**

1. View logs for all services:
   ```
   docker-compose logs
   ```
2. View logs for a specific service (e.g., ubsv-blockchain):
   ```
   docker-compose logs ubsv-blockchain
   ```



**Step 12: Stopping the Stack**

1. To stop all services:
   ```
   docker-compose down
   ```



Additional Notes:

- The `data` directory will contain persistent data. Ensure regular backups.
- Under no circumstances should you use this `docker compose` approach for production usage.
- Please discuss with the Teranode support team for advanced configuration options and optimizations not covered in the present document.





## 5. Configuration



### 5.1. Configuring Setting Files



The following settings can be configured in the Docker Compose  `settings_local.conf`.



**Service Ports**

```
PROPAGATION_GRPC_PORT=8084
PROPAGATION_HTTP_PORT=8833
VALIDATOR_GRPC_PORT=8081
BLOCK_ASSEMBLY_GRPC_PORT=8085
SUBTREE_VALIDATION_GRPC_PORT=8086
BLOCK_VALIDATION_GRPC_PORT=8088
BLOCK_VALIDATION_HTTP_PORT=8188
BLOCKCHAIN_GRPC_PORT=8087
ASSET_GRPC_PORT=8091
ASSET_HTTP_PORT=8090
COINBASE_GRPC_PORT=8093
P2P_HTTP_PORT=9906
```

**Service Addresses**

```
propagation_grpcAddresses.docker.m=ubsv-propagation:${PROPAGATION_GRPC_PORT}
propagation_grpcAddress.docker.m=ubsv-propagation:${PROPAGATION_GRPC_PORT}
propagation_httpAddresses.docker.m=http://ubsv-propagation:${PROPAGATION_HTTP_PORT}
validator_grpcAddress.docker.m=localhost:${VALIDATOR_GRPC_PORT}
blockassembly_grpcAddress.docker.m=ubsv-blockassembly:${BLOCK_ASSEMBLY_GRPC_PORT}
subtreevalidation_grpcAddress.docker.m=ubsv-subtreevalidation:${SUBTREE_VALIDATION_GRPC_PORT}
blockvalidation_httpAddress.docker.m=http://ubsv-blockvalidation:${BLOCK_VALIDATION_HTTP_PORT}
blockvalidation_grpcAddress.docker.m=ubsv-blockvalidation:${BLOCK_VALIDATION_GRPC_PORT}
blockchain_grpcAddress.docker.m=ubsv-blockchain:${BLOCKCHAIN_GRPC_PORT}
asset_grpcListenAddress.docker.m=:${ASSET_GRPC_PORT}
asset_grpcAddress.docker.m=ubsv-asset:${ASSET_GRPC_PORT}
asset_httpAddress.docker.m=http://ubsv-asset:${ASSET_HTTP_PORT}${asset_apiPrefix}
coinbase_assetGrpcAddress.docker.m=ubsv-asset:${ASSET_GRPC_PORT}
coinbase_grpcAddress.docker.m=ubsv-coinbase:${COINBASE_GRPC_PORT}
p2p_httpListenAddress.docker.m=:${P2P_HTTP_PORT}
p2p_httpAddress.docker.m=ubsv-p2p:${P2P_HTTP_PORT}
```

**Database Connections**

```
utxostore.docker.m=aerospike://aerospike:3000/test?WarmUp=32&ConnectionQueueSize=32&LimitConnectionsToQueueSize=true&MinConnectionsPerNode=8&expiration=300
utxoblaster_utxostore_aerospike.docker.m=aerospike://editor:password1234@aerospike:3000/utxo-store?WarmUp=0&ConnectionQueueSize=640&LimitConnectionsToQueueSize=true&MinConnectionsPerNode=64&expiration=900
blockchain_store.docker.m=postgres://miner1:miner1@postgres:5432/ubsv1
coinbase_store.docker.m=postgres://coinbase1:coinbase1@postgres:5432/coinbase1
```

**P2P Configuration**

```
p2p_static_peers.docker.m=
coinbase_p2p_static_peers.docker.m=
```



For the purposes of the alpha testing, it is recommended not to modify any settings unless strictly necessary.




### 5.2. Optional vs Required services

While most services are required for the proper functioning of Teranode, some services are optional and can be disabled if not needed. The following table provides an overview of the services and their status:

| Required          | Optional          |
|-------------------|-------------------|
| Asset Server      | Block Persister   |
| Block Assembly    | UTXO Persister    |
| Block Validator   |                   |
| Subtree Validator |                   |
| Blockchain        |                   |
| Coinbase          |                   |
| Propagation       |                   |
| P2P               |                   |
| Legacy Gateway    |                   |

The Block and UTXO persister services are optional and can be disabled. If enabled, your node will be in Archive Mode, storing historical block and UTXO data. This data can be useful for analytics and historical lookups but comes with additional storage and processing overhead. Additionally, it can be used as a backup for the UTXO store.




## 6. Node Operation



### 6.1. Docker Compose - Starting and Stopping the Node



#### 6.1.1. Starting the Node

Starting your Teranode instance involves initializing all the necessary services in the correct order. The `docker compose` configuration file handles this complexity for us. Follow these steps to **start** your node:

1. **Pre-start Checklist:**

    - Ensure all required directories are in place (data, config, etc.).
    - Verify that `settings_local.conf` is properly configured.
    - Check that Docker and Docker Compose are installed and up to date.

2. **Navigate to the Teranode Directory:**

   ```
   cd /path/to/teranode
   ```

3. **Pull Latest Images **(optional, but recommended before each start)**:**

   ```
   docker-compose pull
   ```

4. **Start the Teranode Stack:**

   ```
   docker-compose up -d
   ```
   This command starts all services defined in the docker-compose.yml file in detached mode.

5. **Verify Service Startup:**

   ```
   docker-compose ps
   ```
   Ensure all services show a status of "Up" or "Healthy".

6. **Monitor Startup Logs:**

   ```
   docker-compose logs -f
   ```
   This allows you to watch the startup process in real-time. Use Ctrl+C to exit the log view.

7. **Check Individual Service Logs:**
   If a specific service isn't starting correctly, check its logs:

   ```
   docker-compose logs [service-name]
   ```
   Replace [service-name] with services like ubsv-blockchain, ubsv-asset, etc.

8. **Verify Network Connections:**
   Once all services are up, ensure the node is connecting to the BSV network:

   ```
   docker-compose exec ubsv-p2p /app/ubsv.run -p2p=1 getpeerinfo
   ```

9. **Check Synchronization Status:**
   Monitor the blockchain synchronization process:

   ```
   docker-compose exec ubsv-blockchain /app/ubsv.run -blockchain=1 getblockchaininfo
   ```

10. **Access Monitoring Tools:**

    - Open Grafana at http://localhost:3005 to view system metrics.
    - Check Prometheus at http://localhost:9090 for raw metrics data.

11. **Troubleshooting:**

    - If any service fails to start, check its specific logs and configuration.
    - Ensure all required ports are open and not conflicting with other applications.
    - Verify that all external dependencies (Kafka, Postgres, Aerospike) are running correctly.

12. **Post-start Checks:**

    - Verify that the node is accepting incoming connections (if configured as such).
    - Check that the miner service is operational if you're running a mining node.
    - Ensure all microservices are communicating properly with each other.

Remember, the initial startup may take some time, especially if this is the first time starting the node or if there's a lot of blockchain data to sync. Be patient and monitor the logs for any issues.

For subsequent starts after the initial setup, you typically only need to run the `docker-compose up -d` command, unless you've made configuration changes or updates to the system.



#### 6.1.2. Stopping the Node

Properly stopping your Teranode instance is crucial to maintain data integrity and prevent potential issues. Follow these steps to safely stop your node:

1. **Navigate to the Teranode Directory:**

   ```
   cd /path/to/teranode
   ```

2. **Graceful Shutdown:**
   To stop all services defined in your docker-compose.yml file:

   ```
   docker-compose down
   ```
   This command stops and removes all containers, but preserves your data volumes.

3. **Verify Shutdown:**
   Ensure all services have stopped:

   ```
   docker-compose ps
   ```
   This should show no running containers related to your Teranode setup.

4. **Check for Any Lingering Processes:**

   ```
   docker ps
   ```
   Verify that no Teranode-related containers are still running.

5. **Stop Specific Services (if needed):**
   If you need to stop only specific services:

   ```
   docker-compose stop [service-name]
   ```
   Replace [service-name] with the specific service you want to stop, e.g., ubsv-blockchain.

6. **Forced Shutdown (use with caution!):**
   If services aren't responding to the normal shutdown command:

   ```
   docker-compose down --timeout 30
   ```
   This forces a shutdown after 30 seconds. Adjust the timeout as needed.

7. **Cleanup** (optional):
   To remove all stopped containers and free up space:

   ```
   docker system prune
   ```
   Be **cautious** with this command as it removes all stopped containers, not just Teranode-related ones.

8. **Data Preservation:**
   The `docker-compose down` command doesn't remove volumes by default. Your data should be preserved in the ./data directory.

9. **Backup Consideration:**
   Consider creating a backup of your data before shutting down, especially if you plan to make changes to your setup.

10. **Monitoring Shutdown:**
    Watch the logs during shutdown to ensure services are stopping cleanly:

    ```
    docker-compose logs -f
    ```

11. **Restart Preparation:**
    If you plan to restart soon, no further action is needed. Your data and configurations will be preserved for the next startup.



Remember, <u>always</u> use the <u>graceful shutdown</u> method (`docker-compose down`) unless absolutely necessary to force a shutdown. This ensures that all services have time to properly close their connections, flush data to disk, and perform any necessary cleanup operations.



### 6.2. Syncing the Blockchain

Teranode allows to sync the blockchain from a remote node, or from a local snapshot. The Teranode team publishes regular snapshots of the blockchain and UTXO set, which can be used to speed up the initial sync process.
For more information, please refer to the Teranode team.



##### Handling Extended Downtime:

If Teranode has been offline for an extended period, consider the following:

1. **Database Integrity Check**:
   - After a long downtime, it's advisable to check the integrity of your blockchain and UTXO databases.

   - This can be done by examining the logs for any corruption warnings or errors.



2. **Manual Intervention**:
   - In rare cases of significant divergence or data corruption, you might need to manually intervene.

   - This could involve reseeding the node with a more recent blockchain snapshot or UTXO set.



3. **Monitoring Catch-up Time**:
   - The time required for catch-up synchronization depends on the duration of the downtime and the number of missed blocks.
   - Use monitoring tools (e.g., Prometheus and Grafana) to track the sync progress and estimate completion time.




### 6.3. How to Interact with the Node



There are 2 primary ways to interact with the node, using the RPC Server, and using the Asset Server.



#### 6.3.1. Teranode RPC HTTP API



The Teranode RPC server provides a JSON-RPC interface for interacting with the node. Below is a list of implemented RPC methods:



**Supported Methods**

1. `createrawtransaction`: Creates a raw transaction.
2. `generate`: Generates new blocks.
3. `getbestblockhash`: Returns the hash of the best (tip) block in the longest blockchain.
4. `getblock`: Retrieves a block by its hash.
5. `getminingcandidate`: Obtain a mining candidate from the node.
6. `sendrawtransaction`: Submits a raw transaction to the network.
7. `submitminingsolution`: Submits a new mining solution.
8. `stop`: Stops the Teranode server.
9. `version`: Returns version information about the RPC server.



**Authentication**

The RPC server uses HTTP Basic Authentication. Credentials are configured in the settings (see the section 4.1 for details). There are two levels of access:

1. Admin access: Full access to all RPC methods.
2. Limited access: Access to a subset of RPC methods defined in `rpcLimited`.



**Request Format**

Requests should be sent as HTTP POST requests with a JSON-RPC 1.0 or 2.0 formatted body. For example:

```json
{
    "jsonrpc": "1.0",
    "id": "1",
    "method": "getbestblockhash",
    "params": []
}
```



**Response Format**

Responses are JSON objects containing the following fields:

- `result`: The result of the method call (if successful).
- `error`: Error information (if an error occurred).
- `id`: The id of the request.



**Error Handling**

Errors are returned as JSON-RPC error objects with a code and message. Common error codes include:

- `-32600`: Invalid Request
- `-32601`: Method not found
- `-32602`: Invalid params
- `-32603`: Internal error
- `-32700`: Parse error



For detailed information on each method's parameters and return values, refer to the Bitcoin SV protocol documentation or the specific Teranode implementation details.



#### 6.3.2. Teranode Asset Server HTTP API



The Teranode Asset Server provides the following HTTP endpoints. Unless otherwise specified, all endpoints are GET requests.



Base URL: `/api/v1` (configurable)

Port: `8090` (configurable)



**Health and Status Endpoints**

- GET `/alive`
    - Returns the service status and uptime

- GET `/health`
    - Performs a health check on the service



**Transaction Endpoints**

- GET `/tx/:hash`
    - Retrieves a transaction in binary stream format
- GET `/tx/:hash/hex`
    - Retrieves a transaction in hexadecimal format
- GET `/tx/:hash/json`
    - Retrieves a transaction in JSON format

- POST `/txs`
    - Retrieves multiple transactions (binary stream format)
    - The request body is a concatenated series of 32-byte transaction hashes:
      [32-byte hash][32-byte hash][32-byte hash]...
      - Example (in hex):
      123456789abcdef123456789abcdef123456789abcdef123456789abcdef1234
      567890abcdef123456789abcdef123456789abcdef123456789abcdef123456
      ...
      - Each hash is exactly 32 bytes (256 bits)
      - No separators between hashes
      - The body can contain multiple hashes
      - The server reads until it reaches the end of the body (EOF)

- GET `/txmeta/:hash/json`
    - Retrieves transaction metadata in JSON format

- GET `/txmeta_raw/:hash`
    - Retrieves raw transaction metadata in binary stream format
- GET `/txmeta_raw/:hash/hex`
    - Retrieves raw transaction metadata in hexadecimal format
- GET `/txmeta_raw/:hash/json`
    - Retrieves raw transaction metadata in JSON format



**Subtree Endpoints**

- GET `/subtree/:hash`
    - Retrieves a subtree in binary stream format
- GET `/subtree/:hash/hex`
    - Retrieves a subtree in hexadecimal format
- GET `/subtree/:hash/json`
    - Retrieves a subtree in JSON format

- GET `/subtree/:hash/txs/json`
    - Retrieves transactions in a subtree in JSON format



**Block and Header Endpoints**

- GET `/headers/:hash`
    - Retrieves block headers in binary stream format
- GET `/headers/:hash/hex`
    - Retrieves block headers in hexadecimal format
- GET `/headers/:hash/json`
    - Retrieves block headers in JSON format
- GET `/header/:hash`
    - Retrieves a single block header in binary stream format
- GET `/header/:hash/hex`
    - Retrieves a single block header in hexadecimal format
- GET `/header/:hash/json`
    - Retrieves a single block header in JSON format
- GET `/blocks`
    - Retrieves multiple blocks
- GET `/blocks/:hash`
    - Retrieves N blocks starting from a specific hash in binary stream format
- GET `/blocks/:hash/hex`
    - Retrieves N blocks starting from a specific hash in hexadecimal format
- GET `/blocks/:hash/json`
    - Retrieves N blocks starting from a specific hash in JSON format
- GET `/block_legacy/:hash`
    - Retrieves a block in legacy format (binary stream)
- GET `/block/:hash`
    - Retrieves a block by hash in binary stream format
- GET `/rest/block/:hash.bin`
    - (Deprecated). Retrieves a block by hash in binary stream format.

- GET `/block/:hash/hex`
    - Retrieves a block by hash in hexadecimal format
- GET `/block/:hash/json`
    - Retrieves a block by hash in JSON format
- GET `/block/:hash/forks`
    - Retrieves fork information for a specific block
- GET `/block/:hash/subtrees/json`
    - Retrieves subtrees of a block in JSON format
- GET `/lastblocks`
    - Retrieves the last N blocks
- GET `/bestblockheader`
    - Retrieves the best block header in binary stream format
- GET `/bestblockheader/hex`
    - Retrieves the best block header in hexadecimal format
- GET `/bestblockheader/json`
    - Retrieves the best block header in JSON format



**UTXO and Balance Endpoints**

- GET `/utxo/:hash`
    - Retrieves a UTXO in binary stream format
- GET `/utxo/:hash/hex`
    - Retrieves a UTXO in hexadecimal format
- GET `/utxo/:hash/json`
    - Retrieves a UTXO in JSON format

- GET `/utxos/:hash/json`
    - Retrieves UTXOs by transaction ID in JSON format

- GET `/balance`
    - Retrieves balance information



**Miscellaneous Endpoints**

- GET `/search?q=:hash`
    - Performs a search

- GET `/blockstats`
    - Retrieves block statistics

- GET `/blockgraphdata/:period`
    - Retrieves block graph data for a specified period





## 7. Maintenance



### 7.1. Updating Teranode to a New Version



#### 7.1.1. Docker Compose



1. Download and copy any newer version of the **docker compose file**, if available, into your project repository.

2. **Update Docker Images**
   Pull the latest versions of all images:

   ```
   docker-compose pull
   ```

3. **Rebuild the Teranode Image**
   If there are changes to the Dockerfile or local build context:

   ```
   docker-compose build ubsv-builder
   ```

4. **Stop the Current Stack**

   ```
   docker-compose down
   ```

5. **Start the Updated Stack**

   ```
   docker-compose up -d
   ```

6. **Verify the Update**
   Check that all services are running and healthy:

   ```
   docker-compose ps
   ```

7. **Check Logs for Any Issues**

   ```
   docker-compose logs
   ```



**Important Considerations:**

- **Data Persistence**: The update process should not affect data stored in volumes (./data directory). However, it's always good practice to backup important data before updating.
- **Configuration Changes**: Check the release notes or documentation for any required changes to `settings_local.conf` or environment variables.
- **Database Migrations**: Some updates may require database schema changes. Where applicable, this will be handled transparently by docker compose.
- **Downtime**: Be aware that this process involves stopping and restarting all services, which will result in some downtime. Rolling updates are not possible with the `docker compose` setup.



**After the update:**

- Monitor the system closely for any unexpected behavior.
- Check the Grafana dashboards to verify that performance metrics are normal.



**If you encounter any issues during or after the update, you may need to:**

- Check the specific service logs for error messages.
- Consult the release notes or documentation for known issues and solutions.
- Reach out to the Teranode support team for assistance.



Remember, the exact update process may vary depending on the specific changes in each new version. Always refer to the official update instructions provided with each new release for the most accurate and up-to-date information.


### 7.2. Managing Disk Space



Key considerations and strategies:



1. **Monitoring Disk Usage:**
    - Regularly check available disk space using tools like `df -h` or through the Grafana dashboard.
    - Set up alerts to notify you when disk usage reaches certain thresholds (e.g., 80% full).
2. **Understanding Data Growth:**
    - The blockchain data, transaction store, and subtree store will grow over time as new blocks are added to the network.
    - Growth rate depends on network activity and can vary significantly.
3. **Pruning Strategies:**
    - Teranode implements regular pruning of old data that's no longer needed for immediate operations.
    - While retention policies for different data types are configurable, this is not documented in this SOP. It is generally not advised to change the defaults for alpha testing purposes.
7. **Log Management:**
    - Consider offloading logs to a separate storage system or log management service.
10. **Backup and Recovery:**
    - Implement a backup strategy that doesn't interfere with disk space management.
    - Ensure backups are stored on separate physical media or cloud storage.
11. **Performance Impact:**
    - Be aware that very high disk usage (>90%) can negatively impact performance.
    - Monitor I/O performance alongside disk space usage.




### 7.3. Backing Up Data



Regular and secure backups are essential for protecting a Teranode installation, ensuring data integrity, and safeguarding your wallet. The next steps outline how to back up your Teranode wallet and data:



1. **Configuration Files:**
    - Backup all custom configuration files, including `settings_local.conf`.
    - Store these separately from the blockchain data for easy restoration.
2. **Database Backups:**
    - PostgreSQL: Use `pg_dump` for regular database backups.
    - Aerospike: Utilize Aerospike's backup tools for consistent snapshots of the UTXO store.


Alternatively, consider downloading a fresh blockchain snapshot (UTXO set and blockchain dump) from the Teranode team for a quick restore.

## 8. Troubleshooting



### 8.1. Health Checks and System Monitoring



#### 8.1.1. Health Checks



##### 8.1.1.1. Service Status



**Docker Compose:**

```bash
docker-compose ps
```
This command lists all services defined in your docker-compose.yml file, along with their current status (Up, Exit, etc.) and health state if health checks are configured.





##### 8.1.1.2. Detailed Container/Pod Health



**Docker Compose:**

```bash
docker inspect --format='{{json .State.Health}}' container_name
```
Replace `container_name` with the name of your specific Teranode service container.



##### 8.1.1.3. Configuring Health Checks



**Docker Compose:**
In your docker-compose.yml file:

```yaml
services:
  ubsv-blockchain:
    ...
    healthcheck:
      test: ["CMD", "curl", "-f", "http://localhost:8087/health"]
      interval: 30s
      timeout: 10s
      retries: 3
      start_period: 40s
```




##### 8.1.1.4. Viewing Health Check Logs



**Docker Compose:**

```bash
docker inspect --format='{{json .State.Health}}' container_name | jq
```



#### 8.1.2. Monitoring System Resources



**Docker Compose:**

* Use `docker stats` to monitor CPU, memory, and I/O usage of containers:
  ```bash
  docker stats
  ```


* Consider using Prometheus and Grafana for comprehensive monitoring.
* Look for services consuming unusually high resources.



#### 8.1.3. Check Logs for Errors



##### 8.1.3.1. Viewing Global Logs



**Docker Compose:**

```bash
docker-compose logs
docker-compose logs -f  # Follow logs in real-time
docker-compose logs --tail=100  # View only the most recent logs
```




##### 8.1.3.2. Viewing Logs for Specific Microservices



**Docker Compose:**

```bash
docker-compose logs [service_name]
```



##### 8.1.3.3. Useful Options for Log Viewing



**Docker Compose:**

* Show timestamps:
  ```bash
  docker-compose logs -t
  ```
* Limit output:
  ```bash
  docker-compose logs --tail=50 [service_name]
  ```
* Since time:
  ```bash
  docker-compose logs --since 2023-07-01T00:00:00 [service_name]
  ```




##### 8.1.3.4. Checking Logs for Specific Teranode Microservices



For Docker Compose, replace `[service_name]` with the appropriate service or pod name:

* Propagation Service
* Blockchain Service
* Asset Service
* Block Validation Service
* P2P Service
* Block Assembly Service
* Subtree Validation Service
* Miner Service
* Coinbase Service
* RPC Service
* Block Persister Service
* UTXO Persister Service
* Postgres Database    [*Only in Docker*]
* Aerospike Database  [*Only in Docker*]
* Kafka                            [*Only in Docker*]
* Kafka Console            [*Only in Docker*]
* Prometheus               [*Only in Docker*]
* Grafana                       [*Only in Docker*]



##### 8.1.3.5. Redirecting Logs to a File



**Docker Compose:**

```bash
docker-compose logs > teranode_logs.txt
docker-compose logs [service_name] > [service_name]_logs.txt
```



Remember to replace placeholders like `[service_name]`, and label selectors with the appropriate values for your Teranode setup.



#### **8.1.4. Check Services Dashboard**



Check your Grafana `UBSV Service Overview` dashboard:



7.1.4.1. Check that there's no blocks in the queue (`Queued Blocks in Block Validation`). We expect little or no queueing, and not creeping up. 3 blocks queued up are already a concern.



7.1.4.2. Check that the propagation instances are handling around the same load to make sure the load is equally distributed among all the propagation servers. See the `Propagation Processed Transactions per Instance` diagram.



7.1.4.3. Check that the cache is at a sustainable pattern rather than "exponentially" growing (see both the `Tx Meta Cache in Block Validation` and `Tx Meta Cache Size in Block Validation` diagrams).



7.1.4.4. Check that go routines (`Goroutines` graph) are not creeping up or reaching excessive levels.



### 8.2 Recovery Procedures



#### 8.2.1. Third Party Component Failure



Teranode is highly dependent on its third party dependencies. Postgres, Kafka and Aerospike are critical for Teranode operations, and the node cannot work without them.



If a third party service fails, you must restore its functionality. Once it is back, please restart Teranode cleanly, as per the instructions in the Section 6 of this document.







------



Should you encounter a bug, please report it following the instructions in the Bug Reporting section.




## 9. Security Best Practices



### 9.1. Firewall Configuration





While Docker Compose creates an isolated network for the Teranode services, some ports are exposed to the host system and potentially to the external network. Here are some firewall configuration recommendations:



1. **Publicly Exposed Ports:**
   Review the ports exposed in the Docker Compose configuration file(s) and ensure your firewall is configured to handle these appropriately:
    - `9292`: RPC Server. Open to receive RPC API requests.

    - `8090,8091`: Asset Server. Open for incoming HTTP and gRPC asset requests.

    - `9905,9906`:  P2P Server. Open for incoming connections to allow peer discovery and communication.



2. **Host Firewall:**

    - Configure your host's firewall to allow incoming connections only on the necessary ports.
    - For ports that don't need external access, strictly restrict them to localhost (127.0.0.1) or your internal network.



3. **External Access:**

    - Only expose ports to the internet that are absolutely necessary for node operation (e.g., P2P, RPC and Asset server ports).
    - Use strong authentication for any services that require external access. See the section 4.1 of this document for more details.



4. **Docker's Built-in Firewall:**

    - Docker manages its own iptables rules. Ensure these don't conflict with your host firewall rules.



5. **Network Segmentation:**

    - If possible, place your Teranode host on a separate network segment with restricted access to other parts of your infrastructure.



6. **Regular Audits:**

    - Periodically review your firewall rules and exposed ports to ensure they align with your security requirements.



8. **Service-Specific Recommendations:**

    - **PostgreSQL (5432**): If you want to expose it, restrict to internal network, never publicly.
    - **Kafka (9092, 9093)**: If you want to expose it, restrict to internal network, never publicly.
    - **Aerospike (3000)**: If you want to expose it, restrict to internal network, never publicly.
    - **Grafana (3005)**: Secure with strong authentication if exposed externally.



9. **P2P Communication:**

    - Ensure ports 9905 and 9906 are open for incoming connections to allow peer discovery and communication.



Remember, the exact firewall configuration will depend on your specific network setup, security requirements, and how you intend to operate your Teranode. Always follow the principle of least privilege, exposing only what is necessary for operation.




### 9.2. Regular System Updates



In order to receive the latest bug fixes and vulnerability patches, please ensure you perform periodic system updates, as regularly as feasible. Please refer to the Teranode update process outlined in the Section 6 of this document.



## 10. Bug Reporting



When you encounter issues, please follow these guidelines to report bugs to the Teranode support team:

**10.1. Before Reporting**

1. Check the documentation and FAQ to ensure the behavior is indeed a bug.
2. Search existing GitHub issues to see if the bug has already been reported.

**10.2. Collecting Information**

Before submitting a bug report, gather the following information:

1. **Environment Details**:
    - Operating System and version
    - Docker version
    - Docker Compose version
2. **Configuration Files**:
    - `settings_local.conf`
    - Docker Compose file
3. **System Resources**:
    - CPU usage
    - Memory usage
    - Disk space and I/O statistics
4. **Network Information:**
    - Firewall configuration
    - Any relevant network errors
5. **Steps to Reproduce**:
    - Detailed, step-by-step description of how to reproduce the issue
6. **Expected vs Actual Behavior**:
    - What you expected to happen
    - What actually happened
7. **Screenshots or Error Messages**:
    - Include any relevant visual information

**10.3. Submitting the Bug Report**

1. Go to the Teranode GitHub repository.
2. Click on "Issues" and then "New Issue"
3. Select the "Bug Report" template
4. Fill out the template with the information you've gathered
5. Attach the compressed file from the log collection tool
6. Submit the issue

**10.4. Bug Report Template**

When creating a new issue, use the following template:

```markdown
## Bug Description
[Provide a clear and concise description of the bug]

## Teranode Version
[e.g., 1.2.3]

## Environment
- OS: [e.g., Ubuntu 20.04]
- Docker Version: [e.g., 20.10.7]
- Docker Compose Version: [e.g., 1.29.2]
- Kubectl version

## Steps to Reproduce
1. [First Step]
2. [Second Step]
3. [and so on...]

## Expected Behavior
[What you expected to happen]

## Actual Behavior
[What actually happened]

## Additional Context
[Any other information that might be relevant]

## Logs and Configuration
```

**10.6. After Submitting**

- Be responsive to any follow-up questions from the development team.
- If you discover any new information about the bug, update the issue.
- If the bug is resolved in a newer version, please confirm and close the issue.







---
