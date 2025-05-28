# How to Install Teranode with Docker Compose

Last modified: 26-May-2025

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
- AWS CLI
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

To speed up the initial synchronization process, you have the option to seed Teranode from pre-existing data. To know more about this approach, please refer to the [How to Sync the Node](../minersHowToSyncTheNode.md) guide.

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



**Step 1: Create Repository**

1. Open a terminal or command prompt.
2. Clone the Teranode repository:

```bash
cd $YOUR_WORKING_DIR
git clone git@github.com:bitcoin-sv/teranode-public.git
cd teranode-public
```

**Step 2: Configure Environment Settings Context [Optional]**

1. Create a `.env` file in the root directory of the project.
2. While not required (your docker compose file will be preconfigured), the following line can be used to set the context:
   ```
   SETTINGS_CONTEXT_1=docker.m
   ```
3. Authenticate with AWS ECR (please check with your Teranode team for the latest credentials).

This step is not mandatory, but useful if you want to create settings variants for specific contexts.

**Step 3: Prepare Local Settings**

1. You can see the current settings under the `$YOUR_WORKING_DIR/docker/base/settings_local.conf` file. You can override any settings here.
2. For a list of settings, and their default values, please refer to the reference at the end of this document.


**Step 4: Authenticate with AWS ECR (only required during the private beta phase)**

```bash
# authenticate with AWS ECR
aws ecr get-login-password --region eu-north-1 | docker login --username AWS --password-stdin 434394763103.dkr.ecr.eu-north-1.amazonaws.com
```

**Step 5: Pull Docker Images**

1. Go to either the testnet Docker compose folder:

```bash
cd $YOUR_WORKING_DIR/teranode-public/docker/testnet
```

Or to the mainnet Docker compose folder:

```bash
cd $YOUR_WORKING_DIR/teranode-public/docker/mainnet
```


2. Pull the required Docker images:
   ```
   docker-compose pull
   ```


**Step 6: Start the Teranode Stack**

1. When running on a box without a public IP, you should enable `legacy_config_Upnp` (in your settings file), so you don't get banned by the SV Nodes.

2. Launch the entire Teranode stack using Docker Compose:
   ```
   docker-compose up -d
   ```

Force the node to transition to Run mode:
```bash
docker exec -it blockchain teranode-cli setfsmstate --fsmstate running
```

or LegacySync mode:
```bash
docker exec -it blockchain teranode-cli setfsmstate --fsmstate legacysyncing
```

You can verify the current state with:
```bash
docker exec -it blockchain teranode-cli getfsmstate
```


**Step 7: Verify Services**

1. Check if all services are running correctly:
   ```
   docker-compose ps
   ```

Example output:


| NAME | IMAGE | COMMAND | SERVICE | CREATED | STATUS | PORTS                                                                                                                                                                                                                                                                                                                                                                       |
|------|-------|---------|----------|---------|---------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| aerospike | aerospike:ce-7.1.0.6 | `/usr/bin/as-tini-st…` | aerospike | 2 minutes ago | Up 2 minutes | 0.0.0.0:3000->3000/tcp, :::3000->3000/tcp, 3001-3002/tcp                                                                                                                                                                                                                                                                                                                    |
| aerospike-exporter | aerospike/aerospike-prometheus-exporter:latest | `/docker-entrypoint.…` | aerospike-exporter | 2 minutes ago | Up 2 minutes | 9145/tcp                                                                                                                                                                                                                                                                                                                                                                    |
| asset | 434394763103.dkr.ecr.eu-north-1.amazonaws.com/teranode-public:v0.5.50 | `/app/entrypoint.sh …` | asset | 2 minutes ago | Up 2 minutes (healthy) | 0/tcp, 3005/tcp, 8098/tcp, 9292/tcp, 0.0.0.0:8090->8090/tcp, :::8090->8090/tcp, 0.0.0.0:32796->4040/tcp, [::]:32796->4040/tcp, 0.0.0.0:32797->8000/tcp, [::]:32797->8000/tcp, 0.0.0.0:32798->8091/tcp, [::]:32798->8091/tcp, 0.0.0.0:32799->9091/tcp, [::]:32799->9091/tcp                                                                                                  |
| blockassembly | 434394763103.dkr.ecr.eu-north-1.amazonaws.com/teranode-public:v0.5.50 | `/app/entrypoint.sh …` | blockassembly | 2 minutes ago | Up 2 minutes (healthy) | 0/tcp, 3005/tcp, 8098/tcp, 9292/tcp, 0.0.0.0:32768->4040/tcp, [::]:32768->4040/tcp, 0.0.0.0:32772->8000/tcp, [::]:32772->8000/tcp, 0.0.0.0:32774->8085/tcp, [::]:32774->8085/tcp, 0.0.0.0:32777->8285/tcp, [::]:32777->8285/tcp, 0.0.0.0:32779->9091/tcp, [::]:32779->9091/tcp                                                                                              |
| blockchain | 434394763103.dkr.ecr.eu-north-1.amazonaws.com/teranode-public:v0.5.50 | `/app/entrypoint.sh …` | blockchain | 2 minutes ago | Up 2 minutes (healthy) | 0/tcp, 3005/tcp, 8098/tcp, 9292/tcp, 0.0.0.0:32769->4040/tcp, [::]:32769->4040/tcp, 0.0.0.0:32771->8000/tcp, [::]:32771->8000/tcp, 0.0.0.0:32775->8082/tcp, [::]:32775->8082/tcp, 0.0.0.0:32776->8087/tcp, [::]:32776->8087/tcp, 0.0.0.0:32778->9091/tcp, [::]:32778->9091/tcp                                                                                              |
| blockvalidation | 434394763103.dkr.ecr.eu-north-1.amazonaws.com/teranode-public:v0.5.50 | `/app/entrypoint.sh …` | blockvalidation | 2 minutes ago | Up 2 minutes (healthy) | 0/tcp, 3005/tcp, 8098/tcp, 9292/tcp, 0.0.0.0:32781->4040/tcp, [::]:32781->4040/tcp, 0.0.0.0:32787->8000/tcp, [::]:32787->8000/tcp, 0.0.0.0:32788->8088/tcp, [::]:32788->8088/tcp, 0.0.0.0:32790->8188/tcp, [::]:32790->8188/tcp, 0.0.0.0:32792->9091/tcp, [::]:32792->9091/tcp                                                                                              |
| grafana | grafana/grafana:latest | `/run.sh` | grafana | 2 minutes ago | Up 2 minutes | 0.0.0.0:3005->3000/tcp, [::]:3005->3000/tcp                                                                                                                                                                                                                                                                                                                                 |
| kafka-console-shared | docker.redpanda.com/redpandadata/console:latest | `/bin/sh -c 'echo \"$…'` | kafka-console-shared | 2 minutes ago | Up 2 minutes | 0.0.0.0:8080->8080/tcp, :::8080->8080/tcp                                                                                                                                                                                                                                                                                                                                   |
| kafka-shared | vectorized/redpanda:latest | `/entrypoint.sh 'red…'` | kafka-shared | 2 minutes ago | Up 2 minutes | 8082/tcp, 9644/tcp, 0.0.0.0:9092-9093->9092-9093/tcp, :::9092-9093->9092-9093/tcp, 0.0.0.0:32794->8081/tcp, [::]:32794->8081/tcp                                                                                                                                                                                                                                            |
| legacy | 434394763103.dkr.ecr.eu-north-1.amazonaws.com/teranode-public:v0.5.50 | `/app/entrypoint.sh …` | legacy | 2 minutes ago | Up 2 minutes | 0/tcp, 3005/tcp, 0.0.0.0:8333->8333/tcp, :::8333->8333/tcp, 9292/tcp, 0.0.0.0:18333->18333/tcp, :::18333->18333/tcp, 0.0.0.0:32782->4040/tcp, [::]:32782->4040/tcp, 0.0.0.0:32783->8000/tcp, [::]:32783->8000/tcp, 0.0.0.0:32784->8098/tcp, [::]:32784->8098/tcp, 0.0.0.0:32785->8099/tcp, [::]:32785->8099/tcp, 0.0.0.0:32786->9091/tcp, [::]:32786->9091/tcp              |
| postgres | postgres:latest | `docker-entrypoint.s…` | postgres | 2 minutes ago | Up 2 minutes (healthy) | 0.0.0.0:5432->5432/tcp, :::5432->5432/tcp                                                                                                                                                                                                                                                                                                                                   |
| prometheus | prom/prometheus:v2.44.0 | `/bin/prometheus --c…` | prometheus | 2 minutes ago | Up 2 minutes | 0.0.0.0:9090->9090/tcp, :::9090->9090/tcp                                                                                                                                                                                                                                                                                                                                   |
| rpc | 434394763103.dkr.ecr.eu-north-1.amazonaws.com/teranode-public:v0.5.50 | `/app/entrypoint.sh …` | rpc | 2 minutes ago | Up 2 minutes | 0/tcp, 3005/tcp, 8098/tcp, 0.0.0.0:9292->9292/tcp, :::9292->9292/tcp, 0.0.0.0:32770->4040/tcp, [::]:32770->4040/tcp, 0.0.0.0:32773->8000/tcp, [::]:32773->8000/tcp, 0.0.0.0:32780->9091/tcp, [::]:32780->9091/tcp                                                                                                                                                           |
| subtreevalidation | 434394763103.dkr.ecr.eu-north-1.amazonaws.com/teranode-public:v0.5.50 | `/app/entrypoint.sh …` | subtreevalidation | 2 minutes ago | Up 2 minutes (healthy) | 0/tcp, 3005/tcp, 8098/tcp, 9292/tcp, 0.0.0.0:32789->4040/tcp, [::]:32789->4040/tcp, 0.0.0.0:32791->8000/tcp, [::]:32791->8000/tcp, 0.0.0.0:32793->8086/tcp, [::]:32793->8086/tcp, 0.0.0.0:32795->9091/tcp, [::]:32795->9091/tcp                                                                                                                                             |


2. Ensure all services show a status of "Up" or "Healthy".



**Step 8: Access Monitoring Tools**

1. **Grafana**: Access the Grafana dashboard at `http://localhost:3005`
- Default credentials are `admin/admin`
- Navigate to the "Teranode - Service Overview" dashboard for key metrics
- Explore other dashboards for detailed service metrics. For example, you can check the Legacy sync metrics in the "Teranode - Legacy Service" dashboard.


2. **Kafka Console**: Available internally on port 8080



3. **Prometheus**: Available internally on port 9090


4. **Teranode Blockchain Viewer**: A basic blockchain viewer is available and can be accessed via the asset container. It provides an interface to browse blockchain data.
    - **Port**: Exposed on port **8090** of the asset container.
    - **Access URL**: http://localhost:8090/viewer

Note: You must set the setting `dashboard_enabled` as true in order to see the viewer.


**Step 9: Interact with Teranode**

- The various Teranode services expose different ports for interaction:
    - **Blockchain service**: Port 8082
    - **Asset service**: Ports 8090
    - **Miner service**: Ports 8089, 8092, 8099
    - **P2P service**: Ports 9905, 9906
    - **RPC service**: 9292

Notice that those ports might be mapped to random ports on your host machine. You can check the mapping by running `docker-compose ps`.


**Step 10: Logging and Troubleshooting**

1. View logs for all services:
   ```
   docker-compose logs
   ```
2. View logs for a specific service (e.g., teranode-blockchain):
   ```
    docker-compose logs -f legacy
    docker-compose logs -f blockchain
    docker-compose logs -f asset
   ```


**Step 11: Docker Log Rotation**

Teranode is very verbose and will output a lot of information, especially with logLevel=DEBUG. To avoid running out of disk space, you can specify logging options directly in your docker-compose.yml file for each service.

```
services:
  [servicename]:
    logging:
      options:
        max-size: "100m"
        max-file: "3"
```

**Step 12: Stopping the Stack**

1. To stop all services:
   ```
   docker-compose down
   ```

Additional Notes:

- The `data` directory (under `testnet` or `mainnet`) will contain persistent data. This includes blockchain data and other persistent storage required by the Teranode components. By default, Docker Compose is configured to mount these directories into the respective containers, ensuring that data persists across restarts and container recreation. Ensure regular backups.
- Under no circumstances should you use this Docker Compose approach for production usage.
- Please discuss with the Teranode support team for advanced configuration options and optimizations not covered in the present document.


## Optimizations


If you have local access to SV Nodes, you can use them to speed up the initial block synchronization too. You can set `legacy_connect_peers: "172.x.x.x:8333|10.x.x.x:8333"` in your `docker-compose.yml` to force the legacy service to only connect to those peers.


## Settings Reference

You can find the pre-configured settings file [here](https://github.com/bitcoin-sv/teranode-public/blob/master/docker/base/settings_local.conf). These are the pre-configured settings in your Docker Compose. You can refer to this document in order to identify the current system behaviour and in order to override desired settings in your `settings_local.conf`.
