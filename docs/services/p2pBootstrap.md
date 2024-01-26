# 🌐 Private P2P Bootstrap Service

## Index


## 1. Description

The service represents a bootstrap implementation in a peer-to-peer (P2P) network using the `libp2p` library, a modular network stack for peer-to-peer applications.

The bootstrap service helps new nodes discover other peers and join the network. In the context of this service, the network is intended as a private network.


## 2. Architecture

The P2P Bootstrap service is a standalone service that runs in each UBSV node, acting as a bootstrap node for new nodes joining the network.

The service is implemented using the `libp2p` library, a modular network stack for peer-to-peer applications. Please refer to the [libp2p documentation](https://docs.libp2p.io/concepts/introduction/overview/) for more information.

`libp2p` is integraded with `Kademlia`, a distributed hash table library, and further configured to create a secure, decentralized P2P network with shared pre-shared keys (PSKs). Kademlia provides the mechanisms for efficient routing and peer discovery, while the PSK ensures that only authorized nodes can join and communicate within the network.

![P2P_Bootstrap_Component_Service.png](img%2FP2P_Bootstrap_Component_Service.png)


### 2.1. Kademlia in libp2p:

1. **Distributed Hash Table (DHT)**: Kademlia is used in `libp2p` as the basis for a distributed hash table. A DHT is a decentralized data storage system that allows nodes to store and retrieve data (like peer addresses) in a distributed manner.

2. **Node IDs and Buckets**: Each node in a Kademlia-based DHT is assigned a unique node ID, derived from its public key. The network is structured as an overlay, where nodes are organized in a series of "buckets" based on their ID distance from other nodes.

3. **Peer Discovery**: Kademlia enables efficient peer discovery. Nodes can find other nodes by querying their closest known peers.

4. **Routing Table**: Each `libp2p` node maintains a routing table with information about other nodes. This table is updated as the node communicates with others, improving the efficiency of network operations like content discovery and data retrieval.

Please refer to the [Kademlia paper](https://pdos.csail.mit.edu/~petar/papers/maymounkov-kademlia-lncs.pdf) for more information.

### 2.2. Private Networks in libp2p:

1. **Pre-shared Key (PSK)**: `libp2p` supports private networks using a shared secret key known as a pre-shared key. All participants in the network must have this key to communicate with each other.

2. **Network Isolation**: The PSK is used to isolate the private network from the public network. Only nodes possessing the correct PSK can decode and verify each other's communications, effectively making the network a closed group.

3. **Connection Establishment**: When two `libp2p` nodes attempt to establish a connection, they use the PSK to authenticate and encrypt their communication. If a node doesn't have the correct PSK, the connection is rejected.

4. **Security and Privacy**: The use of a PSK adds an additional layer of security and privacy. It ensures that only authorized nodes can participate in the network and access its resources.

Please refer to the [Pre-shared Key Based Private Networks in libp2p](https://github.com/libp2p/specs/blob/master/pnet/Private-Networks-PSK-V1.md) document for more information.



## 3. Functionality

The application code can be found in `p2pBoostrap/Server.go`.

![p2pBootstrap.svg](img%2Fplantuml%2Fp2pBootstrap%2Fp2pBootstrap.svg)


1. **Libp2p Host Creation**: Using the `libp2p.New` function, a new libp2p host is created with specific options:
   - Listenw on a settings-driven given address and port.
   - A generated private key is used to identify the node.
   - Sets up a private network with the generated PSK.

2. **Distributed Hash Table (DHT) Configuration**: A DHT is initialized with the host, using the specified mode (`ModeServer`) and protocol prefix.

3. **Starting the Bootstrap Node**: The program prints the addresses at which the bootstrap node is running. The bootstrap node acts as an initial point of contact for new nodes joining the network, helping them discover other peers.


## 4. Directory Structure and Main Files

```
/modules
└── p2pBootstrap
    ├── Dockerfile
        # Contains Docker build instructions for the p2pBootstrap module.
        # Defines environment setup, dependencies installation, and runtime configurations.
    ├── Server.go
        # Main Go source file for the p2pBootstrap server.
        # Initiates and manages peer-to-peer (P2P) network connections.
    ├── go.mod
        # Go module file for managing module dependencies.
        # Lists dependencies along with their versions.
    ├── go.sum
        # Accompanies go.mod and contains checksums for dependencies.
        # Ensures integrity and consistency of module dependencies.
    ├── settings.conf
        # Configuration file for the p2pBootstrap module.
        # Stores default or global settings, used across various environments.
    └── settings_local.conf
        # Configuration file for local or specific environment settings.
        # Overrides certain configurations from settings.conf for specific deployment scenarios.
```


## 5. Settings

1. **`p2p_dht_protocol_id`**: Specifies the protocol identifier for the Distributed Hash Table (DHT) used in the peer-to-peer network. The DHT, based on Kademlia, is required for peer discovery and routing. The protocol ID ensures that peers in the network follow the same protocol rules.

2. **`p2p_shared_key`**: This represents the pre-shared key (PSK) for creating a private network. All communications within this network are encrypted using this PSK, ensuring that only authorized nodes with the correct key can participate in the network. This key is crucial for maintaining privacy and security in the P2P network.

3. **`p2p_bootstrap_privkey`**: A private key used to establish the identity of a bootstrap node in the network. The private key is used to derive the node's public key and ID, contributing to the secure identity in the libp2p network.

4. **`p2p_bootstrap_listenAddress`** and **`p2p_bootstrap_listenPort`**: These settings define the IP address and port on which the bootstrap node listens for incoming connections.


## 6. Usage

The service is started as a standalone process as a Docker container.
