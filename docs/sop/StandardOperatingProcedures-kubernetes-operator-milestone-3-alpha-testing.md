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
- [4.2. Teranode Installation Introduction to the Kubernetes Operator](#42-teranode-installation---introduction-to-the-kubernetes-operator)
- [4.3. Installing Teranode with the Custom Kubernetes Operator](#43-installing-teranode-with-the-custom-kubernetes-operator)
5. [Configuration](#5-configuration)
- [5.1. Configuring Setting Files](#51-configuring-setting-files)
- [5.2. Optional vs Required services](#52-optional-vs-required-services)
6. [Node Operation](#6-node-operation)
- [6.1. Custom Kubernetes Operator](#61-custom-kubernetes-operator)
    - [6.1.1. Starting the Node](#611-starting-the-node)
    - [6.1.2. Stopping the Node](#612-stopping-the-node)
- [6.2. Syncing the Blockchain](#62-syncing-the-blockchain)
- [6.3. How to Interact with the Node](#63-how-to-interact-with-the-node)
    - [6.3.1. Teranode RPC HTTP API](#631-teranode-rpc-http-api)
    - [6.3.2. Teranode Asset Server HTTP API](#632-teranode-asset-server-http-api)
7. [Maintenance](#7-maintenance)
- [7.1. Updating Teranode to a New Version](#71-updating-teranode-to-a-new-version)
- [7.2. Managing Disk Space](#72-managing-disk-space)
- [7.3. Backing Up Data](#73-backing-up-data)
    - [Option 1: Full System Backup](#option-1-full-system-backup)
    - [Option 2: Archive Mode Backup](#option-2-archive-mode-backup)
    - [Option 3: Fresh UTXO and Blockchain Export](#option-3-fresh-utxo-and-blockchain-export)
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
10. [Bug Reporting](#10-bug-reporting)


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

**BSVA Catalog**: A curated collection of operators and services provided by the Bitcoin SV Association for easy deployment of Teranode and related components.

**Coinbase**: The first transaction in a block, which creates new coins as a reward for the miner.

**Docker**: A platform used to develop, ship, and run applications inside containers.

**Docker Compose**: A tool for defining and running multi-container Docker applications.

**gRPC**: A high-performance, open-source universal RPC framework.

**Initial Block Download (IBD)**: The process of downloading and validating the entire blockchain when setting up a new node.

**Kafka**: A distributed streaming platform used in Teranode for handling real-time data feeds.

**Kubernetes**: An open-source system for automating deployment, scaling, and management of containerized applications.

**Microservices**: An architectural style that structures an application as a collection of loosely coupled services.

**Miner**: A node on the network that processes transactions and creates new blocks.

**Operator Lifecycle Manager (OLM)**: A tool to help manage the lifecycle of operators in Kubernetes clusters.

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

3. Deployments using `kubernetes operator`.

4. Configurations designed to connect to and process the BSV mainnet with current production load.



The document covers:

1. Hardware and software requirements for running a Teranode instance using `kubernetes operator`.
2. Installation and configuration of Teranode.
3. Basic operational procedures and maintenance tasks.
4. Monitoring and performance optimization guidelines.
5. Security considerations for running a Teranode instance.



This SOP does not cover:

1. Custom mining software integration (the SOP assumes use of the provided bitcoind miner).

2. Advanced network configurations.

3. Any sort of source code access, build or manipulation of any kind.



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

As seen in the Installation section in this document, BSV provides botha `Kubernetes operator` that provides a production-live multi-node setup. However, it is the operator responsibility to support and monitor the various third parties.



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




#### 3.2.5. Grafana + Prometheus



##### What is Grafana?

**Grafana** is an open-source platform used for monitoring, visualization, and analysis of data. It allows users to create and share interactive dashboards to visualize real-time data from various sources.



##### What is Prometheus?

**Prometheus** is an open-source systems monitoring and alerting toolkit. It is particularly well-suited for monitoring dynamic and cloud-native environments.



Grafana and Prometheus are used together to provide a comprehensive monitoring and visualization solution. Prometheus scrapes metrics from configured targets at regular intervals, and stores them in a its time series database. Grafana then connects to Prometheus as a data source.



Users can create dashboards in Grafana to visualize the metrics collected by Prometheus. Additionally, both Prometheus and Grafana support alerting.



##### Grafana and Prometheus in Teranode

In the context of Teranode, Grafana and Prometheus are used to provide comprehensive monitoring and visualization of the blockchain node’s performance, health, and various metrics.

A Grafana Json Export is provided as part of your Teranode installation. This export can be imported into your Grafana instance to visualize the metrics collected by Prometheus.


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



### 4.2. Teranode Installation - Introduction to the Kubernetes Operator


Teranode is a complex system that can be deployed on Kubernetes using a custom operator (https://kubernetes.io/docs/concepts/extend-kubernetes/operator/). The deployment is managed through Kubernetes manifests, specifically using a Custom Resource Definition (CRD) of kind "Cluster" in the teranode.bsvblockchain.org/v1alpha1 API group. This Cluster resource defines various components of the Teranode system, including Asset, Block Validator, Block Persister, UTXO Persister, Blockchain, Block Assembly, Miner, Peer-to-Peer, Propagation, Subtree Validator, and Coinbase services.


The deployment uses kustomization for managing Kubernetes resources, allowing for easier customization and overlay configurations.



The Cluster resource allows fine-grained control over resource allocation, enabling users to adjust CPU and memory requests and limits for each component.



It must be noted that the `Kubernetes operator` production-like setup does not install or manage the third party dependencies, explicitly:

- **Kafka**
- **PostgreSQL**
- **Aerospike**
- **Grafana**
- **Prometheus**



##### Configuration Management
Environment variables and settings are managed using ConfigMaps. The Cluster custom resource specifies a `configMapName` (e.g., "shared-config") which is referenced by the various Teranode components. Users will need to create this ConfigMap before deploying the Cluster resource.



##### Storage Requirements
Teranode uses PersistentVolumeClaims (PVCs) for storage in some components. For example, the SubtreeValidator specifies storage resources and a storage class. Users should ensure their Kubernetes cluster has the necessary storage classes and capacity available.



##### Service Deployment
The Teranode services are deployed as separate components within the Cluster custom resource. Each service (e.g., Asset, BlockValidator, Blockchain, Peer-to-Peer, etc.) is defined with its own specification, including resource requests and limits. The Kubernetes operator manages the creation and lifecycle of these components based on the Cluster resource definition.



##### Namespace Usage
Users can deploy Teranode to a specific namespace by specifying it during the operator installation or when applying the Cluster resource.



##### Networking and Ingress
Networking is handled through Kubernetes Services and Ingress resources. The Cluster resource allows specification of ingress for Asset, Peer, and Propagation services. It supports customization of ingress class, annotations, and hostnames. The setup appears to use Traefik as the ingress controller, but it's designed to be flexible for different ingress providers.



##### Third Party Dependencies
Tools like Grafana, Prometheus, Aerospike Postgres and Kafka are not included in the Teranode operator deployment. Users are expected to set up these tools separately in their Kubernetes environment (or outside of it).



##### Logging and Troubleshooting
Standard Kubernetes logging and troubleshooting approaches apply. Users can use `kubectl logs` and `kubectl describe` commands to investigate issues with the deployed pods and resources.



In the following sections, we will focus on the `Kubernetes operator` installation method, as it is the most suitable for production purposes.




### 4.3. Installing Teranode with the Custom Kubernetes Operator



**Prerequisites**:

- Go version 1.20.0+
- Docker version 17.03+
- kubectl version 1.11.3+
- Access to a Kubernetes v1.11.3+ cluster
- Operator Lifecycle Manager (OLM) installed
- Sufficient cluster resources as defined in the Cluster spec
- A stable internet connection



**Step 1: Prepare the Environment**

1. Ensure you have kubectl installed and configured to access your Kubernetes cluster.

2. Verify access to your Kubernetes cluster:

   ```
   kubectl cluster-info
   ```



**Step 2: Install Operator Lifecycle Manager (OLM)**

1. If OLM is not already installed, install it using the following command:

   ```
   operator-sdk olm install
   ```



**Step 3: Create BSVA CatalogSource**

1. Create the BSVA CatalogSource in the OLM namespace:

   ```
   kubectl create -f olm/catalog-source.yaml
   ```



**Step 4: Create Target Namespace**

1. Create the namespace where you want to install the Teranode operator (this example uses 'teranode-operator'):

   ```
   kubectl create namespace teranode-operator
   ```



**Step 5: Create OperatorGroup and Subscription**

1. (Optional) If you're deploying to a namespace other than 'teranode-operator', modify the OperatorGroup to specify your installation namespace:

   ```
   echo "  - <your-namespace>" >> olm/og.yaml
   ```

2. Create the OperatorGroup and Subscription resources:

   ```
   kubectl create -f olm/og.yaml -n teranode-operator
   kubectl create -f olm/subscription.yaml -n teranode-operator
   ```



**Step 6: Verify Deployment**

1. Check if all pods are running (your output should be similar to the below):
   ```
   kubectl get pods
   NAME                                                              READY   STATUS      RESTARTS   AGE
   asset-5cc5745c75-6m5gf                                            1/1     Running     0          3d11h
   asset-5cc5745c75-84p58                                            1/1     Running     0          3d11h
   block-assembly-649dfd8596-k8q29                                   1/1     Running     0          3d11h
   block-assembly-649dfd8596-njdgn                                   1/1     Running     0          3d11h
   block-persister-57784567d6-tdln7                                  1/1     Running     0          3d11h
   block-persister-57784567d6-wdx84                                  1/1     Running     0          3d11h
   block-validator-6c4bf46f8b-bvxmm                                  1/1     Running     0          3d11h
   blockchain-ccbbd894c-k95z9                                        1/1     Running     0          3d11h
   coinbase-6d769f5f4d-zkb4s                                         1/1     Running     0          3d11h
   dkr-ecr-eu-north-1-amazonaws-com-teranode-operator-bundle-v0-1    1/1     Running     0          3d11h
   ede69fe8f248328195a7b76b2fc4c65a4ae7b7185126cdfd54f61c7eadffnzv   0/1     Completed   0          3d11h
   miner-6b454ff67c-jsrgv                                            1/1     Running     0          3d11h
   peer-6845bc4749-24ms4                                             1/1     Running     0          3d11h
   propagation-648cd4cc56-cw5bp                                      1/1     Running     0          3d11h
   propagation-648cd4cc56-sllxb                                      1/1     Running     0          3d11h
   subtree-validator-7879f559d5-9gg9c                                1/1     Running     0          3d11h
   subtree-validator-7879f559d5-x2dd4                                1/1     Running     0          3d11h
   teranode-operator-controller-manager-768f498c4d-mk49k             2/2     Running     0          3d11h
   ```

2. Ensure all services show a status of "Running" or "Completed".



**Step 7: Configure Ingress (if applicable)**

1. Verify that ingress resources are created for Asset, Peer, and Propagation services:
   ```
   kubectl get ingress
   ```

2. Configure your ingress controller or external load balancer as needed.



**Step 8: Access Teranode Services**

- The various Teranode services will be accessible through the configured ingress or service endpoints.
- Refer to your specific ingress or network configuration for exact URLs and ports.



**Step 9: Monitoring and Logging**

- Set up your preferred monitoring stack (e.g., Prometheus, Grafana) to monitor the Teranode cluster.
- Use standard Kubernetes logging practices to access logs:
  ```
  kubectl logs <pod-name>
  ```



**Step 10: Troubleshooting**

1. Check pod status:
   ```
   kubectl describe pod <pod-name>
   ```

2. View pod logs:
   ```
   kubectl logs <pod-name>
   ```

3. Verify ConfigMaps and Secrets:
   ```
   kubectl get configmaps
   kubectl get secrets
   ```



Additional Notes:

- You can also refer to the https://github.com/bitcoin-sv/teranode-operator repository for up to date instructions.
- This installation uses the 'stable' channel of the BSVA Catalog, which includes automatic upgrades for minor releases.
- To change the channel or upgrade policy, modify the `olm/subscription.yaml` file before creating the Subscription.
- SharedPVCName represents a persistent volume shared across a number of services (Block Validation, Subtree Validation, Block Assembly, Asset Server, Block Persister, UTXO Persister). While the implementation of the storage is left at the user's discretion, the BSV Association has successfully tested using an AWS FSX for Lustre volume at high throughput, and it can be considered as a reliable option for any Teranode deployment. **TODO  https://kubernetes.io/docs/concepts/storage/persistent-volumes/#access-modes** - ReadWriteMany**
- Ensure proper network policies and security contexts are in place for your Kubernetes environment.
- Regularly back up any persistent data stored in PersistentVolumeClaims.
- The Teranode operator manages the lifecycle of the Teranode services. Direct manipulation of the underlying resources is not recommended.



## 5. Configuration



### 5.1. Configuring Setting Files



The following settings can be configured in the custom operator ConfigMap.



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
p2p_static_peers.m=
coinbase_p2p_static_peers.m=
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

The Block and UTXO persister services are optional and can be disabled. If enabled, your node will be in Archive Mode, storing historical block and UTXO data.
As Teranode does not retain historical transaction data, this data can be useful for analytics and historical lookups, but comes with additional storage and processing overhead.
Additionally, it can be used as a backup for the UTXO store.


## 6. Node Operation



### 6.1. Custom Kubernetes Operator



#### 6.1.1. Starting the Node

Starting your Teranode instance in Kubernetes involves deploying the Cluster custom resource, which the operator will use to create and manage all necessary services. Follow these steps to start your node:

1. **Pre-start Checklist:**
   - Ensure your Kubernetes cluster is up and running.
   - Verify that the Teranode operator is installed and running.
   - Check that your `teranode_your_cluster.yaml` file is properly configured.
   - Ensure any required ConfigMaps or Secrets are in place.

2. **Navigate to the Teranode Configuration Directory:**
   ```
   cd /path/to/teranode-config
   ```

3. **Apply the Cluster Custom Resource:**

   ```
   kubectl apply -f teranode_your_cluster.yaml
   ```
   This command creates or updates the Teranode Cluster resource, which the operator will use to deploy all necessary components.

4. **Verify Resource Creation:**

   ```
   kubectl get clusters
   ```
   Ensure your Cluster resource is listed and in the process of being created.

5. **Monitor Pod Creation:**
   ```
   kubectl get pods -w
   ```
   Watch as the operator creates the necessary pods for each Teranode service.

6. **Check Individual Service Logs:**
   If a specific service isn't starting correctly, check its logs:
   ```
   kubectl logs <pod-name>
   ```
   Replace <pod-name> with the name of the specific pod you want to investigate.

7. **Access Monitoring Tools:**
   - If you've set up Grafana and Prometheus, access them through their respective Service or Ingress endpoints.
   - Use `kubectl port-forward` if needed to access these tools locally.

8. **Troubleshooting:**
    - If any pod fails to start, check its logs and events:
      ```
      kubectl describe pod <pod-name>
      ```
    - Ensure all required ConfigMaps and Secrets are correctly referenced in your Cluster resource.
    - Verify that all PersistentVolumeClaims are bound and have the correct storage class.

9. **Post-start Checks:**
    - Verify that all Services are created and have the correct endpoints:
      ```
      kubectl get services
      ```
    - Check that any configured Ingress resources are properly set up:
      ```
      kubectl get ingress
      ```
    - Ensure all microservices are communicating properly with each other by checking their logs for any connection errors.

Remember, the initial startup may take some time, especially if this is the first time starting the node or if there's a lot of blockchain data to sync. Be patient and monitor the logs for any issues.

For subsequent starts after the initial setup, you typically only need to ensure the Cluster resource is applied, unless you've made configuration changes or updates to the system.



#### 6.1.2. Stopping the Node

Properly stopping your Teranode instance in Kubernetes is crucial to maintain data integrity and prevent potential issues. Follow these steps to safely stop your node:

1. **Navigate to the Teranode Configuration Directory:**
   ```
   cd /path/to/teranode-config
   ```

2. **Graceful Shutdown:**
   To stop all services defined in your Cluster resource:
   ```
   kubectl delete -f teranode_your_cluster.yaml
   ```
   This command tells the operator to remove all resources associated with your Teranode instance.

3. **Verify Shutdown:**
   Ensure all pods are being terminated:
   ```
   kubectl get pods -w
   ```
   Watch as the pods are terminated. This may take a few minutes.

4. **Check for Any Lingering Resources:**
   ```
   kubectl get all,pvc,configmap,secret -l app.kubernetes.io/part-of=teranode-operator
   ```
   Verify that no Teranode-related resources are still present.

5. **Stop Specific Services (if needed):**
   If you need to stop only specific services, you can scale down their deployments:
   ```
   kubectl scale deployment <deployment-name> --replicas=0
   ```
   Replace <deployment-name> with the specific deployment you want to stop.

6. **Forced Deletion (use with caution!):**
   If resources aren't being removed properly:
   ```
   kubectl delete -f teranode_v1alpha1_cluster.yaml --grace-period=0 --force
   ```
   This forces deletion of resources. Use this only if the normal deletion process is stuck.

7. **Cleanup (optional):**
   To remove any orphaned resources:
   ```
   kubectl delete all,pvc,configmap,secret -l app.kubernetes.io/part-of=teranode-operator
   ```
   Be **cautious** with this command as it removes all resources with the Teranode label.



8. **Data Preservation:**
   PersistentVolumeClaims are not automatically deleted. Your data should be preserved unless you explicitly delete the PVCs.



9. **Backup Consideration:**
   Consider creating a backup of your data before shutting down, especially if you plan to make changes to your setup. You can use tools like Velero for Kubernetes backups.



10. **Monitoring Shutdown:**
    Watch the events during shutdown to ensure resources are being removed cleanly:
    ```
    kubectl get events -w
    ```



11. **Restart Preparation:**
    If you plan to restart soon, no further action is needed. Your PersistentVolumeClaims and configurations will be preserved for the next startup.



Remember, always use the graceful shutdown method (deleting the Cluster resource) unless absolutely necessary to force a shutdown. This ensures that all services have time to properly close their connections, flush data to disk, and perform any necessary cleanup operations.



The operator will manage the orderly shutdown of services, but be prepared to manually clean up any resources that might not be removed automatically if issues occur during the shutdown process.







### 6.2. Syncing the Blockchain



When a new Teranode instance is deployed in a Kubernetes cluster, it begins the synchronization process automatically. This process, known as the Initial Block Download (IBD), involves downloading and validating the entire blockchain history from other nodes in the network.

##### Synchronization Process:

1. **Peer Discovery**:
   - Upon startup, the Teranode peer service (typically running in the `peer` pod) begins to discover and connect to other nodes in the BSV network.



2. **Block Download**:
   - Once connected, Teranode requests blocks from its peers, typically starting with the first node it successfully connects to.

   - This peer could be either a traditional BSV node or another BSV Teranode.



3. **Validation and Storage**:
   - As blocks are received, they are validated by the various Teranode services (e.g., `block-validator`, `subtree-validator`).

   - Valid blocks are then stored in the blockchain database, managed by the `blockchain` service.

   - The UTXO (Unspent Transaction Output) set is updated accordingly, typically managed by the `asset` service.



4. **Progress Monitoring**:
   - You can monitor the synchronization progress by checking the logs of relevant pods:
     ```
     kubectl logs <blockchain-pod-name>
     kubectl logs <peer-pod-name>
     ```
   - The `getblockchaininfo` command can provide detailed sync status:
     ```
     kubectl exec <blockchain-pod-name> -- /app/ubsv.run -blockchain=1 getblockchaininfo
     ```



##### Optimizing Initial Sync

To speed up the initial synchronization process, you have the option to seed Teranode with pre-existing data:

1. **Pre-created UTXO Set**:

   - Prepare a PersistentVolume with a pre-created UTXO set.

   - Configure your Cluster resource to use this PersistentVolume for the `asset` service.



2. **Existing Blockchain DB**:
   - Similarly, prepare a PersistentVolume with an existing blockchain database.

   - Configure your Cluster resource to use this PersistentVolume for the `blockchain` service.



3. **Applying Pre-existing Data**:
   - Update your Cluster custom resource to point to these pre-populated PersistentVolumes.
   - Apply the updated Cluster resource:
     ```
     kubectl apply -f teranode_your_cluster.yaml
     ```

By using pre-existing data, Teranode can skip a significant portion of the initial sync process, starting from a more recent point in the blockchain history.



##### Recovery After Downtime

In a Kubernetes environment, Teranode is designed to be resilient and can recover from various types of downtime or disconnections.



1. **Automatic Restart**:
   - If a pod crashes or is terminated, Kubernetes will automatically restart it based on the deployment configuration.

   - This is handled by the ReplicaSet controller in Kubernetes.



2. **Reconnection**:
   - Upon restart, the `peer` service will re-establish connections with other nodes in the network.



3. **Block Request**:
   - Teranode will determine the last block it has and request subsequent blocks from its peers.

   - This process is automatic and doesn't require manual intervention.



4. **Catch-up Synchronization**:
   - The node will download and process all blocks it missed during the downtime.

   - This process is typically faster than the initial sync as it involves fewer blocks.



5. **Monitoring Recovery**:

   - Monitor the recovery process using the same methods as the initial sync:
     ```
     kubectl logs <peer-pod-name>
     kubectl exec <blockchain-pod-name> -- /app/ubsv.run -blockchain=1 getblockchaininfo
     ```



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

With the BSVA Catalog installation method, updates are typically handled automatically for minor releases. However, for major version updates or to change the update policy, follow these steps:

1. **Check Current Version**

   ```
   kubectl get csv -n teranode-operator
   ```

2. **Update the Subscription**

   If you need to change the channel or update behavior, edit the subscription:

   ```
   kubectl edit subscription teranode-operator -n teranode-operator
   ```

   You can modify the

   ```
   channel
   ```

    or

   ```
   installPlanApproval
   ```

    fields as needed.

3. **Monitor the Update Process**

   Watch the ClusterServiceVersion (CSV) and pods as they update:

   ```
   kubectl get csv -n teranode-operator -w
   kubectl get pods -n teranode-operator -w
   ```

4. **Verify the Update**

   Check that all pods are running and ready:

   ```
   kubectl get pods -n teranode-operator
   ```

5. **Check Logs for Any Issues**

   ```
   kubectl logs deployment/teranode-operator-controller-manager -n teranode-operator
   ```



**Important Considerations:**
* **Data Persistence**: The update process should not affect data stored in PersistentVolumes. However, it's always good practice to backup important data before updating.
* **Configuration Changes**: Check the release notes or documentation for any required changes to ConfigMaps or Secrets.
* **Custom Resource Changes**: Be aware of any new fields or changes in the Cluster custom resource structure.
* **Database Migrations**: Some updates may require database schema changes. The operator handles this automatically.
* Some updates may require manual intervention, especially for major version changes. Always refer to the official documentation for specific update instructions



**After the update:**

* Monitor the system closely for any unexpected behavior.

* If you've set up Prometheus and Grafana, check the dashboards to verify that performance metrics are normal.

* Verify that all services are communicating correctly and that the node is in sync with the network.



**If you encounter any issues during or after the update:**
* Check the specific pod logs for error messages:
  ```
  kubectl logs <pod-name>
  ```

* Check the operator logs for any issues during the update process:
  ```
  kubectl logs -l control-plane=controller-manager -n <operator-namespace>
  ```

* Consult the release notes for known issues and solutions.

* If needed, you can rollback to the previous version by applying the old Cluster resource and downgrading the operator.

* Reach out to the Teranode support team for assistance if problems persist.



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

There are three main options for backing up and restoring a Teranode system. Each option has its own use cases and procedures.

#### Option 1: Full System Backup

This option involves stopping all services, then performing backups of both the Aerospike UTXO data and the PostgreSQL blockchain data.

##### Procedure:

1. Stop all Teranode services.
2. Perform an Aerospike UTXO backup.
3. Perform a PostgreSQL blockchain backup.
4. Restart services after backup completion.

##### Backup Commands:

###### Aerospike Backup (Version 6.3.12):
```bash
REGION=eu-west-1
asbackup --s3-region $REGION \
         -d s3://$PATH \
         --s3-endpoint-override https://s3.$REGION.amazonaws.com \
         -n utxo-store --parallel 64 -r -z zstd
```

###### PostgreSQL Backup (Version 16):
```bash
./pg_dump -Fc -Z 9 -d $DB-1 -U $USERNAME -f ./$DB-1.pg_dump -h postgres-postgresql.postgres
```

##### Restore Commands:

###### PostgreSQL Restore:
```bash
pg_restore -d $DB-1 -U $USERNAME -h localhost ./$DB-1.pg_dump
```

###### Aerospike Restore:
```bash
REGION=eu-west-1
asrestore --ignore-record-error --s3-region $REGION \
          -d s3://$PATH \
          --s3-endpoint-override https://s3.$REGION.amazonaws.com \
          -n utxo-store --parallel 64 --compress zstd
```

Note: This is not a hot backup. All services must be stopped before performing the backup.

#### Option 2: Archive Mode Backup

If running the node in Archive mode (with Block Persister and UTXO Persister), users can use their own exported UTXO set as an alternative to exporting the Aerospike data.

##### Procedure:

1. Ensure Block Persister and UTXO Persister are enabled and running.
2. Use the persisted UTXO set files, together with a blockchain postgres export, for backup purposes.
3. Follow the restoration procedure in the Installation section.

#### Option 3: Fresh UTXO and Blockchain Export

Operators can download a fresh UTXO and blockchain export from the Teranode repository to reset the node to a specific point.

##### Procedure:

1. Download the latest UTXO and blockchain export from the Teranode repository.
2. Stop all Teranode services.
3. Replace existing UTXO and blockchain data with the downloaded exports.
4. Restart Teranode services.
5. The node will automatically sync up to the current blockchain height.



## 8. Troubleshooting



### 8.1. Health Checks and System Monitoring



#### 8.1.1. Health Checks



##### 8.1.1.1. Service Status



**Kubernetes:**

```bash
kubectl get pods
```
This command lists all pods in the current namespace, showing their status and readiness.





##### 8.1.1.2. Detailed Container/Pod Health




**Kubernetes:**

```bash
kubectl describe pod <pod-name>
```
This provides detailed information about the pod, including its current state, recent events, and readiness probe results.



##### 8.1.1.3. Configuring Health Checks




**Kubernetes:**
In your Deployment or StatefulSet specification:

```yaml
spec:
  template:
    spec:
      containers:
      - name: ubsv-blockchain
        ...
        readinessProbe:
          httpGet:
            path: /health
            port: 8087
          periodSeconds: 30
          timeoutSeconds: 10
          failureThreshold: 3
        livenessProbe:
          httpGet:
            path: /health
            port: 8087
          periodSeconds: 30
          timeoutSeconds: 10
          failureThreshold: 3
          initialDelaySeconds: 40
```



##### 8.1.1.4. Viewing Health Check Logs





**Kubernetes:**
Health check results are typically logged in the pod events:

```bash
kubectl describe pod <pod-name>
```
Look for events related to readiness and liveness probes.



#### 8.1.2. Monitoring System Resources



**Kubernetes:**

* Use `kubectl top` to view resource usage:
  ```bash
  kubectl top pods
  kubectl top nodes
  ```



For both environments:

* Consider setting up Prometheus and Grafana for more comprehensive monitoring.
* Look for services consuming unusually high resources.



#### 8.1.3. Check Logs for Errors



##### 8.1.3.1. Viewing Global Logs




**Kubernetes:**

```bash
kubectl logs -l app.kubernetes.io/part-of=teranode-operator
kubectl logs -f -l app.kubernetes.io/part-of=teranode-operator  # Follow logs in real-time
kubectl logs --tail=100 -l app.kubernetes.io/part-of=teranode-operator  # View only the most recent logs
```



##### 8.1.3.2. Viewing Logs for Specific Microservices



**Kubernetes:**

```bash
kubectl logs <pod-name>
```



##### 8.1.3.3. Useful Options for Log Viewing



**Kubernetes:**

* Show timestamps:
  ```bash
  kubectl logs <pod-name> --timestamps=true
  ```
* Limit output:
  ```bash
  kubectl logs <pod-name> --tail=50
  ```
* Since time:
  ```bash
  kubectl logs <pod-name> --since-time="2023-07-01T00:00:00Z"
  ```



##### 8.1.3.4. Checking Logs for Specific Teranode Microservices



Replace `[service_name]` or `<pod-name>` with the appropriate service or pod name:

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


##### 8.1.3.5. Redirecting Logs to a File



**Kubernetes:**

```bash
kubectl logs -l app.kubernetes.io/part-of=teranode-operator > teranode_logs.txt
kubectl logs <pod-name> > pod_logs.txt
```

Remember to replace placeholders like `[service_name]`, `<pod-name>`, and label selectors with the appropriate values for your Teranode setup.



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


Here are some firewall configuration recommendations:



1. **Publicly Exposed Ports:**
   Review the ports exposed in the Kubernetes operator configuration file(s) and ensure your firewall is configured to handle these appropriately:
    - `9292`: RPC Server. Open to receive RPC API requests.

    - `8090,8091`: Asset Server. Open for incoming HTTP and gRPC asset requests.

    - `9905,9906`:  P2P Server. Open for incoming connections to allow peer discovery and communication.



2. **Host Firewall:**

    - Configure your host's firewall to allow incoming connections only on the necessary ports.
    - For ports that don't need external access, strictly restrict them to localhost (127.0.0.1) or your internal network.



3. **External Access:**

    - Only expose ports to the internet that are absolutely necessary for node operation (e.g., P2P, RPC and Asset server ports).
    - Use strong authentication for any services that require external access. See the section 4.1 of this document for more details.

4. **Network Segmentation:**

    - If possible, place your Teranode host on a separate network segment with restricted access to other parts of your infrastructure.



5. **Regular Audits:**

    - Periodically review your firewall rules and exposed ports to ensure they align with your security requirements.



6. **Service-Specific Recommendations:**

    - **PostgreSQL (5432**): If you want to expose it, restrict to internal network, never publicly.
    - **Kafka (9092, 9093)**: If you want to expose it, restrict to internal network, never publicly.
    - **Aerospike (3000)**: If you want to expose it, restrict to internal network, never publicly.
    - **Grafana (3005)**: Secure with strong authentication if exposed externally.



7. **P2P Communication:**

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
2. **Configuration Files**:
    - `settings_local.conf`
    - Kubernetes custom operator file
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
5. Submit the issue

**10.4. Bug Report Template**

When creating a new issue, use the following template:

```markdown
## Bug Description
[Provide a clear and concise description of the bug]

## Teranode Version
[e.g., 1.2.3]

## Environment
- OS: [e.g., Ubuntu 20.04]
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
