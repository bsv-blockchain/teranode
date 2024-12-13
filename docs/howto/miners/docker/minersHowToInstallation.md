# How to Install Teranode with Docker Compose

Last modified: 13-December-2024

## Index

- [Introduction](#introduction)
- [Prerequisites](#prerequisites)
- [Hardware Requirements](#hardware-requirements)
- [Software Requirements](#software-requirements)
- [Network Considerations](#network-considerations)
- [Installation Process](#installation-process)
    - [ Teranode Initial Block Synchronization](#-teranode-initial-block-synchronization)
        - [Full P2P Download](#full-p2p-download)
        - [Initial Data Set Installation](#initial-data-set-installation)
    - [Teranode Installation Types](#teranode-installation-types)
        - [Pre-built Binaries](#pre-built-binaries)
        - [Docker Container](#docker-container)
        - [Docker Compose](#docker-compose)
    - [Installing Teranode with Docker Compose](#installing-teranode-with-docker-compose)
- [Reference Settings](#reference---settings)


## Introduction

This guide provides step-by-step instructions for installing Teranode using Docker Compose. Notice that this approach is only recommended for testing purposes and not for production usage.


This guide is applicable to:

1. Miners and node operators testing Teranode.

2. Single-node Teranode setups, using `Docker Compose` setup.

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
- A Docker Compose file
- 100GB+ available disk space
- Stable internet connection
- Root or sudo access

## Hardware Requirements


The Teranode team will provide you with current hardware recommendations. These recommendations will be:

1. Tailored to your specific configuration settings
2. Designed to handle the expected production transaction volume
3. Updated regularly to reflect the latest performance requirements

This ensures your system is appropriately equipped to manage the projected workload efficiently.

## Software Requirements

Teranode relies on a number of third-party software dependencies, some of which can be sourced from different vendors.

BSV provides both a `docker compose` that initialises all dependencies within a single server node, and a `Kubernetes operator` that provides a production-live multi-node setup. This document covers the `docker compose` approach.

This section will outline the various vendors in use in Teranode.

To know more, please refer to the [Third Party Reference Documentation](../../../references/thirdPartySoftwareRequirements.md)


Note - if using Docker Compose, the shared Docker storage, which is automatically managed by `docker compose`, is used instead. This approach provides a more accessible testing environment while still allowing for essential functionality and performance evaluation.




## Network Considerations



For the purposes of the alpha testing phase, running a Teranode BSV listener node has relatively low bandwidth requirements compared to many other server applications. The primary network traffic consists of receiving blockchain data, including new transactions and blocks.

While exact bandwidth usage can vary depending on network activity and node configuration, Bitcoin nodes typically require:

- Inbound: 5-50 GB per day
- Outbound: 50-150 GB per day

These figures are approximate. In general, any stable internet connection should be sufficient for running a Teranode instance.

Key network considerations:

1. Ensure your internet connection is reliable and has sufficient bandwidth to handle continuous data transfer.
2. Be aware that initial blockchain synchronization, depending on your installation method, may require higher bandwidth usage. If you synchronise automatically starting from the genesis block, you will have to download every block. However, the BSV recommended approach is to install a seed UTXO Set and blockchain.
3. Monitor your network usage to ensure it stays within your ISP's limits and adjust your node's configuration if needed.


## Installation Process



###  Teranode Initial Block Synchronization



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

To speed up the initial synchronization process, you have the option to seed Teranode from pre-existing data.

Pros:

- Significantly faster than full P2P download
- Allows for quicker node setup


Cons:

- Requires additional steps
- The data set must be validated, to ensure it has not been tampered with


##### Option 1 - Seed Teranode from a bitcoind instance


Precondition: You must have a bitcoind instance running.

1. Stop your bitcoind instance.
2. Export the level DB from the bitcoind instance, and mount it in a location accessible to Teranode.
3. Run the Teranode legacy seeder:

**TODO** <-----------

4. Start Teranode.


##### Option 2

- Receive and install an initial data set comprising two files:
    1. Initial UTXO (Unspent Transaction Output) set
    2. Blockchain DB dump


       **TODO** <-----------

- Start the application at a given block height and sync up from there


To keep the initial data sets up-to-date for new nodes, the BSV Association, directly or through its partners, will continuously create and exports new data sets.

- Teranode typically makes available the UTXO set for a block 100 blocks prior to the current block
- This ensures the UTXO set won't change (is finalized)
- The data set is approximately 3/4 of a day old
- New nodes still need to catch up on the most recent blocks


Where possible, BSV recommends using the Initial Data Set Installation approach.


### Teranode Installation Types


Teranode can be installed and run using four primary methods: pre-built binaries, Docker containers, Docker Compose, or using a Kubernetes Operator. Each method offers different levels of flexibility and ease of use, however Teranode only recommends using the Docker Compose (for testing) or Kubernetes Operator (for Production) approaches. No guidance or support is offered for the other variants. Only the Kubernetes operator approach is recommended for production usage. This document covers the `docker compose` approach only.



#### Pre-built Binaries

- Pre-built executables are available for both arm64 and amd64 architectures.
- This method provides the most flexibility but requires manual setup of dependencies.
- Suitable for users who need fine-grained control over their installation.

#### Docker Container

- A Docker image is published to a Docker registry, containing the Teranode binary and internal dependencies.
- This method offers a balance between ease of use and customization.
- Ideal for users familiar with Docker who want to integrate Teranode into existing container ecosystems.

#### Docker Compose

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




### Installing Teranode with Docker Compose



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
3. Authenticate with AWS ECR (please check with your Teranode team for the latest credentials).


**Step 3: Prepare Local Settings**

1. Create a `settings_local.conf` file in the root directory.
2. Any overridden settings can be placed in this file. For a list of settings, and their default values, please refer to the reference at the end of this document.


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


4. **Teranode Blockchain Viewer**: A basic blockchain viewer is available and can be accessed via the asset container. It provides an interface to browse blockchain data.
   5. **Port**: Exposed on port **8090** of the asset container.
   6. **Access URL**: http://localhost:8090/viewer


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


**Step 12: Docker Log Rotation**

Teranode is very verbose and will output a lot of information, especially with logLevel=DEBUG. Make sure to setup log rotation for the containers to avoid running out of disk space.

```
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

**Step 13: Stopping the Stack**

1. To stop all services:
   ```
   docker-compose down
   ```



Additional Notes:

- The `data` directory will contain persistent data. This includes blockchain data and other persistent storage required by the Teranode components. By default, Docker Compose is configured to mount these directories into the respective containers, ensuring that data persists across restarts and container recreation. Ensure regular backups.
- Under no circumstances should you use this `docker compose` approach for production usage.
- Please discuss with the Teranode support team for advanced configuration options and optimizations not covered in the present document.


## Optimizations

When running on a box without a public IP, you should enable `legacy_config_Upnp`, so you don't get banned by the SV Nodes.

If you have local access to SV Nodes, you can use them to speed up the initial block synchronization too. You can set `legacy_connect_peers: "172.x.x.x:8333|10.x.x.x:8333"` in your `docker-compose.yml` to force the legacy service to only connect to those peers.


## Reference - Settings

You can find the pre-configured settings file [here](https://github.com/bitcoin-sv/teranode-public/blob/master/docker/base/settings_local.conf). These are the pre-configured settings in your docker compose. You can refer to this document in order to identify the current system behaviour and in order to override desired settings in your `settings_local.conf`.
