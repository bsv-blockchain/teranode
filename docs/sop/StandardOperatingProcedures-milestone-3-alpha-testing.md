# Standard Operating Procedure (Alpha Testing)

## Index


- [1.1. Purpose of the SOP](#11-purpose-of-the-sop)
- [1.1.2. Scope and Applicability](#112-scope-and-applicability)
- [1.1.3. The Teranode Microservices](#113-the-teranode-microservices)
- [2.1. Hardware Requirements](#21-hardware-requirements)
- [2.2. Software Requirements](#22-software-requirements)
    - [2.2.1. Apache Kafka](#221-apache-kafka)
        - [What is Kafka?](#what-is-kafka)
        - [Kafka in BSV Teranode](#kafka-in-bsv-teranode)
    - [2.2.2. PostgreSQL](#222-postgresql)
        - [What is PostgreSQL?](#what-is-postgresql)
        - [PostgreSQL in Teranode](#postgresql-in-teranode)
    - [2.2.3. Aerospike](#223-aerospike)
        - [What is Aerospike?](#what-is-aerospike)
        - [How Aerospike is Used in Teranode](#how-aerospike-is-used-in-teranode)
    - [2.2.4. Shared Storage](#224-shared-storage)
    - [2.2.5. Grafana + Prometheus](#225-grafana--prometheus)
        - [What is Grafana?](#what-is-grafana)
        - [What is Prometheus?](#what-is-prometheus)
        - [Grafana and Prometheus in Teranode](#grafana-and-prometheus-in-teranode)
- [2.3. Network Considerations](#23-network-considerations)
- [3.1. Teranode Installation Types](#31-teranode-installation-types)
    - [3.1.1. Pre-built Binaries](#311-pre-built-binaries)
    - [3.1.2. Docker Container](#312-docker-container)
    - [3.1.3. Docker Compose (Recommended for Alpha Testing)](#313-docker-compose-recommended-for-alpha-testing)
- [3.2. Installing Teranode with Docker Compose](#32-installing-teranode-with-docker-compose)
- [4.1. Configuring Setting Files](#41-configuring-setting-files)
- [5.1. Starting and Stopping the Node](#51-starting-and-stopping-the-node)
    - [5.1.1. Starting the Node](#511-starting-the-node)
    - [5.1.2. Stopping the Node](#512-stopping-the-node)
- [5.2. Syncing the Blockchain **TODO** - ask Simon](#52-syncing-the-blockchain---todo---ask-simon)
- [5.3. Monitoring Node status](#53-monitoring-node-status)
- [5.4. How to Interact with the Node](#54-how-to-interact-with-the-node)
    - [5.4.1. Teranode RPC HTTP API](#541-teranode-rpc-http-api)
    - [5.4.2. Teranode Asset Server HTTP API](#542-teranode-asset-server-http-api)
- [6.1. Updating Teranode to a New Version](#61-updating-teranode-to-a-new-version)
- [6.2. Managing Disk Space](#62-managing-disk-space)
- [6.3. Backing Up Data](#63-backing-up-data)
- [8.1. Firewall Configuration](#81-firewall-configuration)
- [8.2. Regular System Updates](#82-regular-system-updates)

----



# 1. Introduction



## 1.1. Purpose of the SOP



The purpose of this Standard Operating Procedure (SOP) is to provide a comprehensive guide for miners to set up and run a BSV Bitcoin node using Teranode in LISTENER mode during the alpha testing phase. This document aims to:

1. Introduce miners to the Teranode architecture and its dependencies.
2. Provide step-by-step instructions for installing and configuring a single-server Teranode setup.
3. Explain the various microservices and their relationships within the Teranode ecosystem.
4. Guide users through basic maintenance operations and monitoring procedures.
5. Offer insights into optimal settings and configuration options for different operational modes.

This SOP serves as a foundational document to help stakeholders familiarize themselves with the Teranode software, its requirements, and its operation in a production-like environment.



## 1.1.2. Scope and Applicability



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

1. Multi-node Teranode setups or advanced clustering configurations.

2. Scaled-out Teranode configurations. The Teranode installation described in this document is capable of handling the regular BSV production load, not the scaled-out 1 million tps configuration.

3. Custom mining software integration (the SOP assumes use of the provided bitcoind miner).

4. Advanced network configurations.

5. Any sort of source code access, build or manipulation of any kind.



The information and procedures outlined in this document are specific to the alpha testing phase and may be subject to change as the Teranode software evolves.





## 1.1.3. The Teranode Microservices



BSV Teranode is built on a microservices architecture, consisting of several core services and external dependencies. This design allows for greater scalability, flexibility, and performance compared to traditional monolithic node structures.

Teranode's architecture includes:

1. **Core Teranode Services**: These are the primary components that handle the essential functions of a Bitcoin node, such as block, subtree and transaction validation, block assembly, and blockchain management.
2. **Overlay Services**: Additional services that provide support for specific functions like legacy network compatibility and node discovery.
3. **External Dependencies**: Third-party software components that Teranode relies on for various functionalities, including data storage, message queuing, and monitoring.





![UBSV_Container_Diagram.png](../architecture/img/UBSV_Container_Diagram.png)

While this document will touch upon these components as needed for setup and operation, it is not intended to provide a comprehensive architectural overview. For a detailed explanation of Teranode's architecture and the interactions between its various components, please refer to the `teranode-architecture-1.4.pdf` document.




# 2. Node Setup





## 2.1. Hardware Requirements



- Linear Scalability throughput (3 snapshots - 10K, 100K, 500K) **TODO** <--- To be worked with Joe [19 July]

- Max 17,000 (seen in BSV production - perhaps size it up for 30,000 max?)




## 2.2. Software Requirements

Teranode relies on a number of third-party software dependencies, some of which can be sourced from different vendors.



For the purpose of the alpha testing, BSV provides a `docker compose` that initialises all dependencies within a single server node, as seen in the Installation document in this document. However, it is the operator responsibility to support and monitor these third parties.



This section will outline the various vendors in use in Teranode.



### 2.2.1. Apache Kafka

#### What is Kafka?

Apache Kafka is an open-source platform for handling real-time data feeds, designed to provide high-throughput, low-latency data pipelines and stream processing. It supports publish-subscribe messaging, fault-tolerant storage, and real-time stream processing.



To know more, please refer to the Kafka official site: https://kafka.apache.org/.



#### Kafka in BSV Teranode

In BSV Teranode, Kafka is used to manage and process large volumes of transaction data efficiently. Kafka serves as a data pipeline, ingesting high volumes of transaction data from multiple sources in real-time. Kafka’s distributed architecture ensures data consistency and fault tolerance across the network.



### 2.2.2. PostgreSQL


#### What is PostgreSQL?

Postgres, short for PostgreSQL, is an advanced, open-source relational database management system (RDBMS). It is known for its robustness, extensibility, and standards compliance. Postgres supports SQL for querying and managing data, and it also provides features like:

- **ACID Compliance:** Ensures reliable transactions.
- **Complex Queries:** Supports joins, subqueries, and complex operations.
- **Extensibility:** Allows custom functions, data types, operators, and more.
- **Concurrency:** Handles multiple transactions simultaneously with high efficiency.
- **Data Integrity:** Supports constraints, triggers, and foreign keys to maintain data accuracy.



To know more, please refer to the PostgreSQL official site: https://www.postgresql.org/



#### PostgreSQL in Teranode

In Teranode, PostgreSQL is used for **Blockchain Storage**. Postgres stores the blockchain data, providing a reliable and efficient database solution for handling large volumes of block data.



### 2.2.3. Aerospike



#### What is Aerospike?

Aerospike is an open-source, high-performance, NoSQL database designed for real-time analytics and high-speed transaction processing. It is known for its ability to handle large-scale data with low latency and high reliability. Key features include:

- **High Performance:** Optimized for fast read and write operations.
- **Scalability:** Easily scales horizontally to handle massive data loads.
- **Consistency and Reliability:** Ensures data consistency and supports ACID transactions.
- **Hybrid Memory Architecture:** Utilizes both RAM and flash storage for efficient data management.



#### How Aerospike is Used in Teranode

In the context of Teranode, Aerospike is utilized for **UTXO (Unspent Transaction Output) storage**, which requires of a robust high performance and reliability solution.



### 2.2.4. Shared Storage



Teranode requires a robust and scalable shared storage solution to efficiently manage its critical **Subtree** and **Transaction** data. This shared storage is accessed and modified by various microservices within the Teranode ecosystem.



In a full production deployment, Teranode is designed to use Lustre (or a similar high-performance shared filesystem) for optimal performance and scalability. However, for the purposes of alpha testing, a simpler approach is employed. The alpha setup utilizes shared Docker storage, which is automatically managed by `docker compose`. This approach provides a more accessible testing environment while still allowing for essential functionality and performance evaluation.



### 2.2.5. Grafana + Prometheus

#### What is Grafana?

**Grafana** is an open-source platform used for monitoring, visualization, and analysis of data. It allows users to create and share interactive dashboards to visualize real-time data from various sources.



#### What is Prometheus?

**Prometheus** is an open-source systems monitoring and alerting toolkit. It is particularly well-suited for monitoring dynamic and cloud-native environments.



Grafana and Prometheus are used together to provide a comprehensive monitoring and visualization solution. Prometheus scrapes metrics from configured targets at regular intervals, and stores them in a its time series database. Grafana then connects to Prometheus as a data source.



Users can create dashboards in Grafana to visualize the metrics collected by Prometheus. Additionally, both Prometheus and Grafana support alerting.



#### Grafana and Prometheus in Teranode



In the context of Teranode, Grafana and Prometheus are used to provide comprehensive monitoring and visualization of the blockchain node’s performance, health, and various metrics.



For the purposes of the alpha testing, an initial set of preconfigured dashboards are provided.



**TODO** - https://grafana.com/grafana/dashboards/ we’ll publish a teranode dashboard here - People can download and install an instance from here.  -- To be discussed with Joe, for analysis.





## 2.3. Network Considerations



For the purposes of the alpha testing phase, running a Teranode BSV listener node has relatively low bandwidth requirements compared to many other server applications. The primary network traffic consists of receiving blockchain data, including new transactions and blocks.

While exact bandwidth usage can vary depending on network activity and node configuration, Bitcoin nodes typically require:

- Inbound: 5-50 GB per day
- Outbound: 50-150 GB per day

These figures are approximate. In general, any stable internet connection should be sufficient for running a Teranode instance.

Key network considerations:

1. Ensure your internet connection is reliable and has sufficient bandwidth to handle continuous data transfer.
2. Be aware that initial blockchain synchronization may require higher bandwidth usage. **TODO** - clarify how relevant this is given the seed DB conversation with Simon **xx**
3. Monitor your network usage to ensure it stays within your ISP's limits and adjust your node's configuration if needed.



# 3. Installation Process



## 3.1. Teranode Installation Types



Teranode can be installed and run using three primary methods: pre-built binaries, Docker containers, or Docker Compose. Each method offers different levels of flexibility and ease of use.

### 3.1.1. Pre-built Binaries

- Pre-built executables are available for both arm64 and amd64 architectures.
- This method provides the most flexibility but requires manual setup of dependencies.
- Suitable for users who need fine-grained control over their installation.

### 3.1.2. Docker Container

- A Docker image is published to a Docker registry, containing the Teranode binary and internal dependencies.
- This method offers a balance between ease of use and customization.
- Ideal for users familiar with Docker who want to integrate Teranode into existing container ecosystems.

### 3.1.3. Docker Compose (Recommended for Alpha Testing)

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

Note: The `Docker Compose` method is recommended for alpha testing as it provides a consistent environment that mirrors the development setup. However, for production deployments or specific performance requirements, users may need to consider alternative setups.

In the following sections, we will focus on the `Docker Compose` installation method, as it is the most suitable for alpha testing purposes. Users interested in directly running the binaries or managing the docker containers with their own custom setups can reach out to the Teranode support team for further discussion.





## 3.2. Installing Teranode with Docker Compose



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

3. Copy the docker compose file into the `teranode` folder.



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



**Step 6: Build the UBSV Image**

1. Build the UBSV image using the provided Dockerfile:
   ```
   docker-compose build ubsv-builder
   ```



**Step 7: Start the Teranode Stack**

1. Launch the entire Teranode stack using Docker Compose:
   ```
   docker-compose up -d
   ```

**TODO** - initial bootstrapping of nodes / sync etc ?



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

- The various UBSV services expose different ports for interaction:
  - **Blockchain service**: Port 8082
  - **Asset service**: Ports 8090, 8091
  - **Miner service**: Ports 8089, 8092, 8099
  - **P2P service**: Ports 9905, 9906
  - **RPC service**: 9292

**TODO - what can the user do with those ports? Should the Blockchain, Miner and P2P be removed from this list? **



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



# 4. Configuration



## 4.1. Configuring Setting Files



The following settings can be configured in the `settings_local.conf`.



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







# 5. Node Operation



## 5.1. Starting and Stopping the Node



### 5.1.1. Starting the Node

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



### 5.1.2. Stopping the Node

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





## 5.2. Syncing the Blockchain - **TODO** - ask Simon



**TODO** Get a brain dump from Simon [July 19]



## 5.3. Monitoring Node status

- Grafana + Prometheus + Logs **TODO**



## 5.4. How to Interact with the Node



There are 2 primary ways to interact with the node, using the RPC Server, and using the Asset Server.



### 5.4.1. Teranode RPC HTTP API



The Teranode RPC server provides a JSON-RPC interface for interacting with the node. Below is a list of implemented RPC methods:



**Supported Methods**

1. `createrawtransaction`: Creates a raw transaction.
2. `generate`: Generates new blocks.
3. `getbestblockhash`: Returns the hash of the best (tip) block in the longest blockchain.
4. `getblock`: Retrieves a block by its hash.
5. `sendrawtransaction`: Submits a raw transaction to the network.
6. `stop`: Stops the Teranode server.
7. `version`: Returns version information about the RPC server.



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



### 5.4.2. Teranode Asset Server HTTP API



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

- GET `/search`
  - Performs a search (details depend on implementation)

- GET `/blockstats`
  - Retrieves block statistics

- GET `/blockgraphdata/:period`
  - Retrieves block graph data for a specified period





# 6. Maintenance



## 6.1. Updating Teranode to a New Version



1. Download and copy any newer version of the **docker compose file**, if available, into your project repository.

2. **Update Docker Images**
   Pull the latest versions of all images:

   ```
   docker-compose pull
   ```

3. **Rebuild the UBSV Image**
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
- **Database Migrations**: Some updates may require database schema changes.***TODO - is this handled transparently by Docker Compose???? [July 19] - not really - clarify this with Jeff**
- **Downtime**: Be aware that this process involves stopping and restarting all services, which will result in some downtime. Rolling updates are not possible with the `docker compose` setup.



**After the update:**

- Monitor the system closely for any unexpected behavior.
- Check the Grafana dashboards to verify that performance metrics are normal.



**If you encounter any issues during or after the update, you may need to:**

- Check the specific service logs for error messages.
- Consult the release notes or documentation for known issues and solutions.
- Reach out to the Teranode support team for assistance.



Remember, the exact update process may vary depending on the specific changes in each new version. Always refer to the official update instructions provided with each new release for the most accurate and up-to-date information.



## 6.2. Managing Disk Space



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



## 6.3. Backing Up Data



Regular and secure backups are essential for protecting a Teranode installation, ensuring data integrity, and safeguarding your wallet. The next steps outline how to back up your Teranode wallet and data:



1. **Configuration Files:**
   - Backup all custom configuration files, including `settings_local.conf`.
   - Store these separately from the blockchain data for easy restoration.
2. **Database Backups:**
   - PostgreSQL: Use `pg_dump` for regular database backups.
   - Aerospike: Utilize Aerospike's backup tools for consistent snapshots of the UTXO store.
3. **Transaction and Subtree Stores:**
   - Implement regular backups of the txstore, subtreestore and block persister directories.
   - Consider incremental backups to save space and time.



Note: During the alpha testing phase, all Teranodes run in listener mode. Should the data be corrupted or lost, it is possible to simply wipe out the `data` folder and re-create the docker node. This is however not a best practice, nor a recommendation for future phases.



# 7. Troubleshooting

- Common issues and solutions
    - ???? Runbook? **TODO**
- Log file analysis
-



Should you encounter a bug, please report it following the instructions in the Bug Reporting section.




# 8. Security Best Practices



## 8.1. Firewall Configuration





While Docker Compose creates an isolated network for the Teranode services, some ports are exposed to the host system and potentially to the external network. Here are some firewall configuration recommendations:

1. **Publicly Exposed Ports:**
   Review the ports exposed in the Docker Compose file and ensure your firewall is configured to handle these appropriately:

   - `9292`: RPC Server. Open to receive RPC API requests.

   - `8090,8091`: Asset Server. Open for incoming HTTP and gRPC asset requests.

   - `8090,8091`:  P2P Server. Open for incoming connections to allow peer discovery and communication.



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




## 8.2. Regular System Updates



In order to receive the latest bug fixes and vulnerability patches, please ensure you perform periodic system updates, as regularly as feasible. Please refer to the Teranode update process outlined in the Section 6 of this document.



# 9. Bug Reporting



When you encounter issues, please follow these guidelines to report bugs to the Teranode support team:

**9.1. Before Reporting**

1. Check the documentation and FAQ to ensure the behavior is indeed a bug.
2. Search existing GitHub issues to see if the bug has already been reported.

**9.2. Collecting Information**

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

**9.3. Using the Log Collection Tool**

To standardize and simplify the bug reporting process, we provide a log collection tool similar to Aerospike's `collect_logs`:

1. Run the Teranode log collection tool:
   ```
   ./teranode_collect_logs.sh
   ```
   **TODO - Refine this once the tool really exists!!**

2. This tool will automatically gather:
   - Current Teranode settings
   - Recent logs from all services
   - System information
   - Configuration files

3. The tool will create a compressed file with all the collected information.

**9.4. Submitting the Bug Report**

1. Go to the Teranode GitHub repository: [**TODO - Insert Teranode GitHub URL**]
2. Click on "Issues" and then "New Issue"
3. Select the "Bug Report" template
4. Fill out the template with the information you've gathered
5. Attach the compressed file from the log collection tool
6. Submit the issue

**9.5. Bug Report Template**

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
[Mention that you've attached the output from teranode_collect_logs.sh]
```

**9.6. After Submitting**

- Be responsive to any follow-up questions from the development team.
- If you discover any new information about the bug, update the issue.
- If the bug is resolved in a newer version, please confirm and close the issue.







---
