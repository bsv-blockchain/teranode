# Standard Operating Procedure (Alpha Testing)

## Index


- [1.1. Purpose of the SOP](#11-purpose-of-the-sop)
- [1.1.2. Scope and applicability](#112-scope-and-applicability)
- [1.1.3. The Teranode microservices](#113-the-teranode-microservices)
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
        - [What is Grafana? ](#what-is-grafana-)
        - [What is Prometheus?](#what-is-prometheus)
        - [Grafana and Prometheus in Teranode](#grafana-and-prometheus-in-teranode)
- [2.3. Network Considerations](#23-network-considerations)
- [3.1. Teranode Installation Types](#31-teranode-installation-types)
    - [3.1.1. Pre-built Binaries](#311-pre-built-binaries)
    - [3.1.2. Docker Container](#312-docker-container)
    - [3.1.3. Docker Compose (Recommended for Alpha Testing)](#313-docker-compose-recommended-for-alpha-testing)
- [3.2. Installing Teranode with Docker Compose](#32-installing-teranode-with-docker-compose)
- [3.3. Verifying software integrity](#33-verifying-software-integrity)
- [4.2. Configuring config files](#42-configuring-config-files)
- [4.3. Security settings](#43-security-settings)
- [5.1. Starting and stopping the node](#51-starting-and-stopping-the-node)
    - [5.1.1. Starting the Node](#511-starting-the-node)
    - [5.1.2. Stopping the Node](#512-stopping-the-node)
- [5.2. Syncing the blockchain **TODO** - ask Simon](#52-syncing-the-blockchain---todo---ask-simon)
- [5.3. Monitoring node status](#53-monitoring-node-status)
- [5.4. How to interact with the current node. APIs, what’s feasible externally and what’s not](#54-how-to-interact-with-the-current-node-apis-whats-feasible-externally-and-whats-not)
- [6.1. Updating Teranode to a new version](#61-updating-teranode-to-a-new-version)
- [6.2. Managing disk space](#62-managing-disk-space)
- [6.3. Backing up data](#63-backing-up-data)
- [8.1. Firewall configuration](#81-firewall-configuration)
- [8.2. Regular system updates](#82-regular-system-updates)

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



## 1.1.2. Scope and applicability



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





## 1.1.3. The Teranode microservices



BSV Teranode is built on a microservices architecture, consisting of several core services and external dependencies. This design allows for greater scalability, flexibility, and performance compared to traditional monolithic node structures.

Teranode's architecture includes:

1. Core Teranode Services: These are the primary components that handle the essential functions of a Bitcoin node, such as block, subtree and transaction validation, block assembly, and blockchain management.
2. Overlay Services: Additional services that provide support for specific functions like legacy network compatibility and node discovery.
3. External Dependencies: Third-party software components that Teranode relies on for various functionalities, including data storage, message queuing, and monitoring.





![UBSV_Container_Diagram.png](../architecture/img/UBSV_Container_Diagram.png)

While this document will touch upon these components as needed for setup and operation, it is not intended to provide a comprehensive architectural overview. For a detailed explanation of Teranode's architecture and the interactions between its various components, please refer to the `teranode-architecture-1.4.pdf` document.




# 2. Node Setup





## 2.1. Hardware Requirements

***TODO***




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



## 2.3. Network Considerations



For the purposes of the alpha testing phase, running a Teranode BSV listener node has relatively low bandwidth requirements compared to many other server applications. The primary network traffic consists of receiving blockchain data, including new transactions and blocks.

While exact bandwidth usage can vary depending on network activity and node configuration, Bitcoin nodes typically require:

- Inbound: 5-50 GB per day
- Outbound: 50-150 GB per day

These figures are approximate. In general, any stable internet connection should be sufficient for running a Teranode instance.

Key network considerations:

1. Ensure your internet connection is reliable and has sufficient bandwidth to handle continuous data transfer.
2. Be aware that initial blockchain synchronization may require higher bandwidth usage.
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
- Advantages:
  - Easiest method to get a full Teranode environment running quickly.
  - Automatically manages start order and networking between components.
  - Used by developers and QA for testing, ensuring reliability.
- Considerations:
  - This setup uses predefined external dependencies, which may not be customizable.
  - While convenient for development and testing, it is not optimized, nor intended, for production usage.

Note: The Docker Compose method is recommended for alpha testing as it provides a consistent environment that mirrors the development setup. However, for production deployments or specific performance requirements, users may need to consider alternative setups.

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
2. Add the following line to set the context:
   ```
   SETTINGS_CONTEXT_1=docker.m
   ```

*** TODO is this correct? Test with Jeff's final compose***



**Step 3: Prepare Local Settings**

1. Create a `settings_local.conf` file in the root directory.
2. Add any local overrides or specific configurations needed for your environment.



***TODO - what overrides we want to suggest / recommend / mention????****



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



**Step 8: Verify Services**

1. Check if all services are running correctly:
   ```
   docker-compose ps
   ```
2. Ensure all services show a status of "Up" or "Healthy".



**Step 9: Access Monitoring Tools**

1. **Grafana**: Access the Grafana dashboard at `http://localhost:3005`

   - Default credentials are admin/admin ***TODO - confirm this point with Jeff****



2. **Kafka Console**: Available internally on port 8080



3. **Prometheus**: Available internally on port 9090



**Step 10: Interact with Teranode**

- The various UBSV services expose different ports for interaction:
  - **Blockchain service**: Port 8082
  - **Asset service**: Ports 8090, 8091
  - **Miner service**: Ports 8089, 8092, 8099
  - **P2P service**: Ports 9905, 9906
  - **RPC service**: No external port exposed by default

**TODO - what can the user do with those ports? what should we document here?** Include portions of the Asset Server doc in the next sections of the doc?



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





## 3.3. Verifying software integrity

**TODO** - is this required for the alpha testing phase of the project?





# 4. Configuration



## 4.1. Configuring config files

- TestNet vs MainNet????
- Modes (listener vs "full node", pruned vs full) ????
- Enabling RPC access (if needed) - in scope???
- Settings per application.
- ...





## 4.2. Security settings

??????? **TODO**





# 5. Node Operation



## 5.1. Starting and stopping the node



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





## 5.2. Syncing the blockchain - **TODO** - ask Simon





## 5.3. Monitoring node status

- Grafana + Prometheus + Logs



## 5.4. How to interact with the current node. APIs, what’s feasible externally and what’s not

- Asset Server
- RPC
- Others?



# 6. Maintenance



## 6.1. Updating Teranode to a new version



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
- **Database Migrations**: Some updates may require database schema changes.***TODO - is this handled transparently by Docker Compose???? Ask Jeff**
- **Downtime**: Be aware that this process involves stopping and restarting all services, which will result in some downtime. Rolling updates are not possible with the `docker compose` setup.



**After the update:**

- Monitor the system closely for any unexpected behavior.
- Check the Grafana dashboards to verify that performance metrics are normal.



**If you encounter any issues during or after the update, you may need to:**

- Check the specific service logs for error messages.
- Consult the release notes or documentation for known issues and solutions.
- Reach out to the Teranode support team for assistance.



Remember, the exact update process may vary depending on the specific changes in each new version. Always refer to the official update instructions provided with each new release for the most accurate and up-to-date information.



## 6.2. Managing disk space



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



## 6.3. Backing up data



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
    - ???? Runbook?
- Log file analysis
- Escalation path to BSV Teranode official support team?



# 8. Security Best Practices



## 8.1. Firewall configuration

- **TODO** - Possibly nothing much? Docker network is self contained, and as long as we do not allow external access to the node, there is not much to worry about.



## 8.2. Regular system updates



In order to receive the latest bug fixes and vulnerability patches, please ensure you perform periodic system updates, as regularly as feasible. Please refer to the Teranode update process outlined in the Section 6 of this document.



# 9. Documentation and Logging

- Keeping records of changes and updates
- Incident reporting

  - #### Public  /  bugs

  collect_logs
  https://aerospike.com/docs/tools/asadm/live_cluster_mode_guide#collectlogs

  #### Private - Security / vulnerabilities

  Joe's note - this needs to be another private channels to report security flaws and maybe even come up with a bounty program. these cannot go on public channels for security reasons



---
