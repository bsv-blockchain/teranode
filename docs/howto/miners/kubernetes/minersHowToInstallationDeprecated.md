# How to Install Teranode with Kubernetes Operator

Last modified: 29-January-2025

# Index

- [Introduction](#introduction)
- [Prerequisites](#prerequisites)
- [Hardware Requirements](#hardware-requirements)
- [Software Requirements](#software-requirements)
- [Network Considerations](#network-considerations)
- [Installation Process](#installation-process)
  - [Teranode Initial Synchronization](#teranode-initial-synchronization)
    - [Full P2P Download](#full-p2p-download)
    - [Initial Data Set Installation](#initial-data-set-installation)
  - [Teranode Installation - Introduction to the Kubernetes Operator](#teranode-installation-introduction-to-the-kubernetes-operator)
  - [Installing Teranode with the Custom Kubernetes Operator](#installing-teranode-with-the-custom-kubernetes-operator)
- [Optimizations](#optimizations)
- [Reference - Settings](#reference-settings)

## Introduction

This guide provides step-by-step instructions for installing Teranode with the Kubernetes Operator.

This guide is applicable to:

1. Miners and node operators using `kubernetes operator`.

2. Configurations designed to connect to and process the BSV mainnet with current production load.

This guide does not cover:

1. Advanced network configurations.

2. Any sort of source code build or manipulation of any kind.

## Prerequisites

- Go version 1.20.0+
- Docker version 17.03+
- kubectl version 1.11.3+
- Access to a Kubernetes v1.11.3+ cluster
- Operator Lifecycle Manager (OLM) installed
- Operator SDK
- Sufficient cluster resources as defined in the Cluster spec
- A stable internet connection

## Hardware Requirements

The Teranode team will provide you with current hardware recommendations. These recommendations will be:

1. Tailored to your specific configuration settings
2. Designed to handle the expected production transaction volume
3. Updated regularly to reflect the latest performance requirements

This ensures your system is appropriately equipped to manage the projected workload efficiently.

## Software Requirements

Teranode relies on a number of third-party software dependencies, some of which can be sourced from different vendors.

BSV provides both a `Kubernetes operator` that provides a production-live multi-node setup. However, it is the operator responsibility to support and monitor the various third parties.

This section will outline the various vendors in use in Teranode.

To know more, please refer to the [Third Party Reference Documentation](../../../references/thirdPartySoftwareRequirements.md)

## Network Considerations

Running a Teranode BSV listener node has relatively low bandwidth requirements compared to many other server applications. The primary network traffic consists of receiving blockchain data, including new transactions and blocks.

While exact bandwidth usage can vary depending on network activity and node configuration, Bitcoin nodes typically require:

- Inbound: 5-50 GB per day
- Outbound: 50-150 GB per day

These figures are approximate. In general, any stable internet connection should be sufficient for running a Teranode instance.

Key network considerations:

1. Ensure your internet connection is reliable and has sufficient bandwidth to handle continuous data transfer.
2. Be aware that initial blockchain synchronization, depending on your installation method, may require higher bandwidth usage. If you synchronise automatically starting from the genesis block, you will have to download every block. However, the BSV recommended approach is to install a seed UTXO Set and blockchain.
3. Monitor your network usage to ensure it stays within your ISP's limits and adjust your node's configuration if needed.

## Installation Process

### Teranode Initial Synchronization

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

To speed up the initial synchronization process, you have the option to seed Teranode from pre-existing data. To know more about this approach, please refer to the [How to Sync the Node](minersHowToSyncTheNode.md) guide.

Pros:

- Significantly faster than full P2P download
- Allows for quicker node setup

Cons:

- Requires additional steps
- The data set must be validated, to ensure it has not been tampered with

Where possible, BSV recommends using the Initial Data Set Installation approach.

### Teranode Installation - Introduction to the Kubernetes Operator

Teranode is a complex system that can be deployed on Kubernetes using a custom operator ([Kubernetes Operator Pattern](https://kubernetes.io/docs/concepts/extend-kubernetes/operator/)). The deployment is managed through Kubernetes manifests, specifically using a Custom Resource Definition (CRD) of kind "Cluster" in the teranode.bsvblockchain.org/v1alpha1 API group. This Cluster resource defines various components of the Teranode system, including Asset, Block Validator, Block Persister, UTXO Persister, Blockchain, Block Assembly, Miner, Peer-to-Peer, Propagation, and Subtree Validator services.

The deployment uses kustomization for managing Kubernetes resources, allowing for easier customization and overlay configurations.

The Cluster resource allows fine-grained control over resource allocation, enabling users to adjust CPU and memory requests and limits for each component.

It must be noted that the `Kubernetes operator` production-like setup does not install or manage the third party dependencies, explicitly:

- **Kafka**
- **PostgreSQL**
- **Aerospike**
- **Grafana**
- **Prometheus**

#### Configuration Management

Environment variables and settings are managed using ConfigMaps. The Cluster custom resource specifies a `configMapName` (e.g., "shared-config") which is referenced by the various Teranode components. Users will need to create this ConfigMap before deploying the Cluster resource.

Sample config:

```yml
apiVersion: v1
kind: ConfigMap
metadata:
  name: teranode-operator-config
data:
  SETTINGS_CONTEXT: operator.testnet
  KAFKA_HOSTS: ...
  blockchain_store: 'postgres://...'
  utxostore: 'aerospike://...'
```

To review the list of settings you could configure in the ConfigMap, please refer to the list [here](https://github.com/bsv-blockchain/teranode/blob/main/settings.conf).

#### Storage Requirements

Teranode uses PersistentVolumeClaims (PVCs) for storage in some components. For example, the SubtreeValidator specifies storage resources and a storage class. Users should ensure their Kubernetes cluster has the necessary storage classes and capacity available.

#### Service Deployment

The Teranode services are deployed as separate components within the Cluster custom resource. Each service (e.g., Asset, BlockValidator, Blockchain, Peer-to-Peer, etc.) is defined with its own specification, including resource requests and limits. The Kubernetes operator manages the creation and lifecycle of these components based on the Cluster resource definition.

#### Namespace Usage

Users can deploy Teranode to a specific namespace by specifying it during the operator installation or when applying the Cluster resource.

#### Networking and Ingress

Networking is handled through Kubernetes Services and Ingress resources. The Cluster resource allows specification of ingress for Asset, Peer, and Propagation services. It supports customization of ingress class, annotations, and hostnames. The setup appears to use Traefik as the ingress controller, but it's designed to be flexible for different ingress providers.

#### Third Party Dependencies

Tools like Grafana, Prometheus, Aerospike Postgres and Kafka are not included in the Teranode operator deployment. Users are expected to set up these tools separately in their Kubernetes environment (or outside of it).

#### Logging and Troubleshooting

Standard Kubernetes logging and troubleshooting approaches apply. Users can use `kubectl logs` and `kubectl describe` commands to investigate issues with the deployed pods and resources.

### Installing Teranode with the Custom Kubernetes Operator

### Step 1: Prepare the Environment

1. **Ensure you have kubectl installed and configured to access your Kubernetes cluster.**

2. **Verify access to your Kubernetes cluster:**

   ```bash
   kubectl cluster-info
   ```

### Step 2: Install Operator Lifecycle Manager (OLM)

1. **If OLM is not already installed, install it using the following command:**

   ```bash
   operator-sdk olm install
   ```

### Step 3: Create BSVA CatalogSource

1. **Clone the Teranode repository:**

    ```bash
    cd $YOUR_WORKING_DIR
    git clone git@github.com:bsv-blockchain/teranode-operator.git
    cd teranode-operator
    ```

2. **Create the BSVA CatalogSource in the OLM namespace:**

    ```bash
    kubectl create -f olm/catalog-source.yaml
    ```

### Step 4: Create Target Namespace

1. **Create the namespace where you want to install the Teranode operator** (this example uses 'teranode-operator'):

   ```bash
   kubectl create namespace teranode-operator
   ```

### Step 5: Create OperatorGroup and Subscription

1. **(Optional) If you're deploying to a namespace other than 'teranode-operator', modify the OperatorGroup to specify your installation namespace:**

    ```bash
    echo "  - <your-namespace>" >> olm/og.yaml
    ```

2. **Create the OperatorGroup and Subscription resources:**

    ```bash
    kubectl create -f olm/og.yaml -n teranode-operator
    kubectl create -f olm/subscription.yaml -n teranode-operator
    ```

### Step 6: Verify Deployment

1. **Check if all pods are running** (your output should be similar to the below):

    ```bash
    # Check catalog source pod
    kubectl get pods -n olm
    ```

    ```bash
    NAME                                READY   STATUS    RESTARTS   AGE
    bsva-catalog-8922m                  1/1     Running   0          22s
    catalog-operator-577f8b4bf5-sczlj   1/1     Running   0          86m
    olm-operator-8685b95f84-8wkf4       1/1     Running   0          86m
    operatorhubio-catalog-thvck         1/1     Running   0          85m
    packageserver-b54f9549f-kzqn9       1/1     Running   0          85m
    packageserver-b54f9549f-tr24v       1/1     Running   0          85m
    ```

    ```bash
    # Check operator deployment
    kubectl get pods -n teranode-operator
    ```

    ```bash
    NAME                                                              READY   STATUS      RESTARTS   AGE
    asset-5cc5745c75-6m5gf                                            1/1     Running     0          3d11h
    asset-5cc5745c75-84p58                                            1/1     Running     0          3d11h
    block-assembly-649dfd8596-k8q29                                   1/1     Running     0          3d11h
    block-assembly-649dfd8596-njdgn                                   1/1     Running     0          3d11h
    block-persister-57784567d6-tdln7                                  1/1     Running     0          3d11h
    block-persister-57784567d6-wdx84                                  1/1     Running     0          3d11h
    block-validator-6c4bf46f8b-bvxmm                                  1/1     Running     0          3d11h
    blockchain-ccbbd894c-k95z9                                        1/1     Running     0          3d11h
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

2. **Ensure all services show a status of "Running" or "Completed".**

### Step 7: Configure Ingress (if applicable)

1. **Verify that ingress resources are created for Asset, Peer, and Propagation services:**

    ```bash
    kubectl get ingress
    ```

2. **Configure your ingress controller or external load balancer as needed.**

### Step 8: Access Teranode Services

- The various Teranode services will be accessible through the configured ingress or service endpoints.
- Refer to your specific ingress or network configuration for exact URLs and ports.

### Step 9: Change the node status to Run or LegacySync

1. **Force the node to transition to Run mode:**

    ```bash
    kubectl exec -it $(kubectl get pods -n teranode-operator -l app=blockchain -o jsonpath='{.items[0].metadata.name}') -n teranode-operator -- teranode-cli setfsmstate --fsmstate RUNNING
    ```

2. **Or LegacySync mode:**

    ```bash
    kubectl exec -it $(kubectl get pods -n teranode-operator -l app=blockchain -o jsonpath='{.items[0].metadata.name}') -n teranode-operator -- teranode-cli setfsmstate --fsmstate LEGACYSYNCING
    ```

### Step 10: Access Monitoring Tools

**Teranode Blockchain Viewer**: A basic blockchain viewer is available and can be accessed via the asset container. It provides an interface to browse blockchain data.

- **Port**: Exposed on port **8090** of the asset container.
- **Access URL**: <http://localhost:8090/viewer>

!!! note
    You must set the setting `dashboard_enabled` as true in order to see the viewer.

### Step 11: Monitoring and Logging

1. **Set up your preferred monitoring stack** (e.g., Prometheus, Grafana) to monitor the Teranode cluster.

2. **Use standard Kubernetes logging practices to access logs:**

   ```bash
   kubectl logs <pod-name>
   ```

### Step 12: Troubleshooting

1. Check pod status:

    ```bash
    kubectl describe pod <pod-name>
    ```

2. View pod logs:

    ```bash
    kubectl logs <pod-name>
    ```

3. Verify ConfigMaps and Secrets:

    ```bash
    kubectl get configmaps
    kubectl get secrets
    ```

Additional Notes:

- You can also refer to the [teranode-operator repository](https://github.com/bsv-blockchain/teranode-operator) for up to date instructions.
- This installation uses the 'stable' channel of the BSVA Catalog, which includes automatic upgrades for minor releases.
- To change the channel or upgrade policy, modify the `olm/subscription.yaml` file before creating the Subscription.
- **SharedPVCName** represents a persistent volume shared across a number of services (Block Validation, Subtree Validation, Block Assembly, Asset Server, Block Persister, UTXO Persister). The Persistent Volume must be in access mode *ReadWriteMany* ([Access Modes](https://kubernetes.io/docs/concepts/storage/persistent-volumes/#access-modes)). While the implementation of the storage is left at the user's discretion, the BSV Association has successfully tested using an AWS FSX for Lustre volume at high throughput, and it can be considered as a reliable option for any Teranode deployment.
- Ensure proper network policies and security contexts are in place for your Kubernetes environment.
- Regularly back up any persistent data stored in PersistentVolumeClaims.
- The Teranode operator manages the lifecycle of the Teranode services. Direct manipulation of the underlying resources is not recommended.

## Optimizations

If you have local access to SV Nodes, you can use them to speed up the initial block synchronization. You can configure specific peer connections in your ConfigMap by setting `legacy_config_ConnectPeers: "172.x.x.x:8333|10.x.x.x:8333"` to force the legacy service to only connect to those peers.

## Reference - Settings

You can find the pre-configured settings file [here](https://github.com/bsv-blockchain/teranode/blob/main/settings.conf). You can refer to this document in order to identify the current system behaviour and in order to override desired settings in your `settings_local.conf`.
