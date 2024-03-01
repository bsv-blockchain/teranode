# 🌐 P2P Service

## Index

1. [Description](#1-description)
2. [Functionality ](#2-functionality)
- [2.1. Creating, initializing and starting a new P2P Server](#21-creating-initializing-and-starting-a-new-p2p-server)
- [2.1.1. Creating a new P2P Server](#211-creating-a-new-p2p-server)
   - [2.1.2. Initializing the P2P Server](#212-initializing-the-p2p-server)
   - [2.1.3. Starting the P2P Server](#213-starting-the-p2p-server)
- [2.2. Peer Discovery and Connection](#22-peer-discovery-and-connection)
- [2.3. Best Block Messages](#23-best-block-messages)
- [2.4. Blockchain Messages](#24-blockchain-messages)
- [2.5. TX Validator Messages](#25-tx-validator-messages)
- [2.6. Websocket notifications ](#26-websocket-notifications-)
3. [Technology](#3-technology)
4. [Data Model](#4-data-model)
5. [Directory Structure and Main Files](#5-directory-structure-and-main-files)
6. [How to run](#6-how-to-run)
7. [Configuration options (settings flags)](#7-configuration-options-settings-flags)


## 1. Description


The `p2p` package implements a peer-to-peer (P2P) server using `libp2p` (`github.com/libp2p/go-libp2p`, `https://libp2p.io`), a modular network stack that allows for direct peer-to-peer communication.

The p2p service allows peers to subscribe and receive blockchain notifications, effectively allowing nodes to receive notifications about new blocks and subtrees in the network.

The p2p peers are part of a private network. This private network is managed by the p2p bootstrap service, which is responsible for bootstrapping the network and managing the network topology. To read more about the p2p bootstrap service, please refer to the [p2p-bootstrap](p2pBootstrap.md) documentation.

1. **Initialization and Configuration**:
  - The `Server` struct holds essential information for the P2P server, such as hosts, topics, subscriptions, clients for blockchain and validation, and logger for logging activities.
  - The `NewServer` function creates a new instance of the P2P server, sets up the host with a private key, and defines various topic names for message publishing and subscribing.
  - The `Init` function configures the server with necessary clients and settings.

2. **Message Types**:
  - There are several message types (`BestBlockMessage`, `MiningOnMessage`, `BlockMessage`, `SubtreeMessage`, `RejectedTxMessage`) used for communicating different types of data over the network.

3. **Networking and Communication**:
  - The server uses `libp2p` for network communication. It sets up a host with given IP and port and handles different topics for publishing and subscribing to messages.
  - The server uses Kademlia for peer discovery.
  - The server uses GossipPub for PubSub.
  - `handleBlockchainMessage`, `handleBestBlockTopic`, `handleBlockTopic`, `handleSubtreeTopic`, and `handleMiningOnTopic` are functions to handle incoming messages for different topics.
  - `sendBestBlockMessage` and `sendPeerMessage` are used for sending messages to peers in the network.

4. **Peer Discovery and Connection**:
  - `discoverPeers` function is responsible for discovering peers in the network and attempting to establish connections with them.

5. **HTTP Server and WebSockets**:
  - An HTTP server is started using `echo`, a Go web framework, providing routes for health checks and WebSocket connections.
  - `HandleWebSocket` is used to handle incoming WebSocket connections and manage the communication with connected clients.

6. **Subscription Listeners**:
  - The server listens for notifications from the blockchain and the tx validator services. It subscribes to these services and handles incoming notifications.

7. **Publish-Subscribe Mechanism**:
  - The server uses the publish-subscribe model over `libp2p` pubsub for message dissemination. It joins topics and subscribes to them to receive and broadcast messages related to blockchain events.

8. **Private Key Management**:
  - The server generates or reads a private key for secure communication (peerId and encryption) in the P2P network.
  - The generated private key is persisted.


![P2P_System_Container_Diagram.png](img%2FP2P_System_Container_Diagram.png)

In the diagram above:

* The node P2P service subscribes and listens to Blockchain and Tx Validator service notifications. When blocks, subtrees or rejected transactions are detected, the P2P service publishes these messages to the network.
* When the P2P service receives a message from the network (i.e. a Blockchain message from another node), it forwards it to the node Block Validation Service for processing.

In more detail:

![P2P_System_Component_Diagram.png](img%2FP2P_System_Component_Diagram.png)

## 2. Functionality


### 2.1. Creating, initializing and starting a new P2P Server

![p2p_create_init_start.svg](img/plantuml/p2p/p2p_create_init_start.svg)

### 2.1.1. Creating a new P2P Server
Upon node startup, the `main.go` will invoke the  `p2p.NewServer` (see `services/p2p/Server.go`) function in the P2P package. This function is responsible for instantiating a new P2P server.

1. **Private Key Management**:
    - The server attempts to read an existing private key from storage using the `readPrivateKey()` function.
    - If this fails (i.e., no key exists), it generates a new private key using `generatePrivateKey()`.

3. **Configuration Retrieval and Topic Registration**: The function retrieves necessary configuration settings:
    - `p2p_ip`: The IP address on which the P2P service will listen.
    - `p2p_port`: The port number for the P2P service.
    - `p2p_topic_prefix`: A prefix for naming P2P communication topics.
    - Specific topic names for different P2P communication topics like `p2p_block_topic`, `p2p_subtree_topic`, `p2p_bestblock_topic`, `p2p_mining_on_topic`, and `p2p_rejected_tx_topic`.

4. **P2P Host Initialization**: The server initializes a new libp2p host with the given IP, port, and private key. The libp2p host handles connections and communications between the different nodes.

#### 2.1.2. Initializing the P2P Server

The `Init` function of the P2P server in the `p2p` package is responsible for initializing the server:

1. **Blockchain Client Initialization**: The server creates a new blockchain client using `blockchain.NewClient(ctx, s.logger)`.

3. **Asset HTTP Address Configuration**: The function retrieves the Asset HTTP Address URL configuration using `gocore.Config().GetURL("asset_httpAddress")`.

4. **Block Validation Client Initialization**: The server initializes a block validation client using `blockvalidation.NewClient(ctx)`.

5. **Validator Client Initialization**: The server initializes a new Tx validator client using `validator.NewClient(ctx, s.logger)`.

#### 2.1.3. Starting the P2P Server

The `Start` function in the P2P server of the `p2p` package is responsible for starting the P2P service:

1. **HTTP Server Setup**: Initializes an HTTP server using the Echo framework.

2. **HTTP Endpoints**:
    - Registers an endpoint for health checks (`/health`) that simply responds with "OK" to indicate the service is up and running.
    - Registers a WebSocket endpoint (`/ws`) for real-time communication, handled by `s.HandleWebSocket`.

3. **Start HTTP Server**: Launches a goroutine to start the HTTP server in a non-blocking manner, calling `s.StartHttp`.

4. **PubSub Topics Setup**:
    - Initializes the GossipSub system for pub-sub messaging.
    - Creates and subscribes to various topics: `bestBlockTopicName`, `blockTopicName`, `subtreeTopicName`, `miningOnTopicName`, and `rejectedTxTopicName`.

5. **Peer-to-Peer Host Configuration**:
    - Sets a libp2p stream handler for the P2P host to handle incoming blockchain messages.

6. **Topic Handlers**:
    - Launches goroutines to handle messages for different topics (`bestBlockTopic`, `blockTopic`, `subtreeTopic`, `miningOnTopic`).

8. **Subscription Listeners**:
    - Starts listeners for blockchain and validator subscriptions to process incoming notifications related to blocks and transactions.

9. **Best Block Message Broadcast**: Sends out a best block message request to the network as a part of the initialization process. Peers receiving this message will respond providing the best block hash.

At this point, the server is initialised and ready to accept connections and messages from peers, as well as listen to activity in the network.

### 2.2. Peer Discovery and Connection

The P2P service uses a DHT initialized by `p2p.InitDHT()` to discover and connect to peers in the libp2p network based on shared interests (topics). This process enables the node to form a network with other peers, facilitating decentralized communication and resource sharing.

![p2p_peer_discovery.svg](img%2Fplantuml%2Fp2p%2Fp2p_peer_discovery.svg)


1. **Initialization of DHT**:
   - The `discoverPeers` (Server.go) function begins by calling `InitDHT(ctx, s.host)`. The `InitDHT` function initializes a Distributed Hash Table (DHT) for the given host (`s.host`), which is part of the libp2p network. The DHT is a key component in P2P networks used for efficient peer discovery and content routing.
   - The DHT is bootstrapped with default peers to integrate the node into the existing P2P network.

2. **Setting Up Routing Discovery**:
   - After initializing the DHT, `discoverPeers` sets up a `routingDiscovery` using the newly created DHT instance. `routingDiscovery` is used to discover peers in the network and to advertise the node's presence to others.

3. **Advertising and Searching for Peers**:
   - The function iterates over a list of topic names (`tn`). For each topic, it uses `dutil.Advertise` to advertise the node's presence under that topic in the network. This step makes the node discoverable to others who are interested in the same topics.
   - The function then enters a loop where it searches for peers who have announced themselves under these topics. This is done using `routingDiscovery.FindPeers`, which returns a channel of peer addresses (`peerChan`).

4. **Connecting to Discovered Peers**:
   - The function iterates over the `peerChan` to connect to each discovered peer. It checks to ensure that it does not attempt to connect to itself. If a connection to a peer is successful, it marks `anyConnected` as `true`.
   - If no peers are found initially, the search continues. Once peers are found and connected, the loop breaks, indicating that peer discovery is complete.

5. **Integration with P2P Network**:
   - By using DHT for discovery and routing, along with topic-based advertising, the node becomes an integrated part of the P2P network. It can discover peers interested in similar topics and establish connections with them.

### 2.3. Best Block Messages

![p2p_handle_blockchain_messages.svg](img%2Fplantuml%2Fp2p%2Fp2p_handle_blockchain_messages.svg)

1. **Node (Peer 1) Starts**:
   - The server's `Start()` method is invoked. Within `Start()`, `s.sendBestBlockMessage(ctx)` is called to send the best block message.
   - This message is published to a topic using `s.topics[bestBlockTopicName].Publish(ctx, msgBytes)`.

2. **Peer 2 Receives the Best Block Topic Message**:
   - Peer 2's server handles the best block topic through `s.handleBestBlockTopic(ctx)`.
   - A peer message is sent using `s.sendPeerMessage(ctx, peer, msgBytes)`. A new stream to the Peer 1 LibP2P host is established with `s.host.NewStream(ctx, peer.ID, protocol.ID(s.bitcoinProtocolId))`.

3. **Node (Peer 1) Receives the Stream Response from Peer 2**:
   - The LibP2P host sends the response stream to Node (Peer 1).
   - Node (Peer 1) handles the blockchain message using `s.handleBlockchainMessage(ctx, stream)`.
   - The message received is logged with `s.logger.Debugf("Received block topic message: %s", string(buf))` (at the time of writing, the `handleBlockchainMessage` function simply logs block topic messages).



### 2.4. Blockchain Messages

When a node creates a new subtree, or finds a new block hashing solution, it will broadcast this information to the network. This is done by publishing a message to the relevant topic. The message is then received by all peers subscribed to that topic. Listening peers can then feed relevant messages to their own Block Validation Service.


![p2p_blockchain_subscription.svg](img%2Fplantuml%2Fp2p%2Fp2p_blockchain_subscription.svg)


1. **Blockchain Subscription**:
   - The server subscribes to the blockchain service using `s.blockchainClient.Subscribe(ctx, blockchain.SubscriptionType_Blockchain)`.
   - The server listens for blockchain notifications (`Block`, `Subtree` or `MiningOn` notifications) using `s.blockchainSubscriptionListener(ctx)`.

1. **New Block Notification**:
   - Node 1 listens for blockchain notifications.
   - If a new block notification is detected, it publishes the block message to the PubSub System.
   - The PubSub System then delivers this message to Node 2.
   - Node 2 receives the message on the block topic, **submits the block message to its own Block Validation Service**, and notifies the block message on its notification channel.

2. **New Mined Block Notification**:
   - Node 1 listens for blockchain notifications.
   - If a new mined block notification is detected, it publishes the mining on message to the PubSub System.
   - The PubSub System delivers this message to Node 2.
   - Node 2 receives the mining message on the mining topic and notifies the "mining on" message on its notification channel.

3. **New Subtree Notification**:
   - Node 1 listens for blockchain notifications.
   - If a new subtree notification is detected, it publishes the subtree message to the PubSub System.
   - The PubSub System delivers this message to Node 2.
   - Node 2 receives the subtree message on the subtree topic, **submits the subtree message to its own Block Validation Service**, and notifies the subtree message on its notification channel.


### 2.5. TX Validator Messages

Nodes will broadcast rejected transaction notifications to the network. This is done by publishing a message to the relevant topic. The message is then received by all peers subscribed to that topic.

![p2p_tx_validator_messages.svg](img%2Fplantuml%2Fp2p%2Fp2p_tx_validator_messages.svg)

 - The Node 1 listens for validator subscription events.
 - When a new rejected transaction notification is detected, the Node 1 publishes this message to the PubSub System using the topic name `rejectedTxTopicName`, forwarding it to any subscribers of the `rejectedTxTopicName` topic.


### 2.6. Websocket notifications

All notifications collected from the Block and Validator listeners are sent over to the Websocket clients. The process can be seen below:

![p2p_websocket_activity_diagram.svg](img%2Fplantuml%2Fp2p%2Fp2p_websocket_activity_diagram.svg)

* WebSocket Request Handling:
  - An HTTP request is upgraded to a WebSocket connection. A new client channel is associated to this Websocket client.
  - Data is sent over the WebSocket, using its dedicated client channel.
  - If there's an error in sending data, the channel is removed from the `clientChannels`.



* The server listens for various types of events in a concurrent process:
  * The server tracks all active client channels (`clientChannels`).
  * When a new client connects, it is added to the `clientChannels`.
  * If a client disconnects, it is removed from `clientChannels`.
  * Periodically, we ping all connected clients. Any error would have the client removed from the list of clients.
  * When a notification is received (from the block validation or transaction listeners described in the previous sections), it is sent to all connected clients.

As a sequence:

![p2p_websocket_sequence_diagram.svg](img%2Fplantuml%2Fp2p%2Fp2p_websocket_sequence_diagram.svg)

1. A client requests a WebSocket connection to the server. The new client is added to the `newClientCh` queue, which then adds the client to the active client channels.
2. The server enters a loop for WebSocket communication, where it can either receive new notifications or pings.
3. For each new notification or ping, the server dispatches this data to all client channels.
4. If there's a WebSocket error or the client disconnects, the client is added to the `deadClientCh` queue, which leads to its removal from the active client channels.




## 3. Technology


1. **Go Programming Language**:
  - The entire package is written in Go (Golang).

2. **libp2p**:
  - A modular network framework that allows peers to communicate directly with each other. It's widely used in decentralized systems for handling various network protocols, peer discovery, transport, encryption, and stream multiplexing.

3. **pubsub (go-libp2p-pubsub)**:
  - A library for publish-subscribe functionality in `libp2p`. It's used for messaging between nodes in a decentralized network, supporting various messaging patterns like broadcasting.

4. **crypto (go-libp2p-core/crypto)**:
  - This library provides cryptographic functions for `libp2p`, including key generation, marshaling, and unmarshaling. It's crucial for maintaining secure communication channels in the P2P network.

5. **Echo (labstack/echo/v4)**:
  - A high-performance, extensible web framework for Go.

6. **WebSockets**:
  - A communication protocol providing full-duplex channels over a single TCP connection. Used here for real-time, two-way interaction between the server and clients.

7. **JSON (JavaScript Object Notation)**:
  - A lightweight data-interchange format, used here for serializing and transmitting structured data over the network (like the various message types).

8. **Distributed Hash Table (DHT) for Peer Discovery**:
  - A decentralized method of discovering peers in the network. It allows a node to efficiently find other nodes in the P2P network.

9. **HTTP/HTTPS Protocols**:
  - Used for setting up the web server and handling requests. The server can handle both HTTP and HTTPS requests.

10. **Middleware & Logging (middleware, ulogger)**:
  - Used for handling common tasks across requests (like logging, error handling, CORS settings) in the Echo web framework.

11. **gocore (ordishs/gocore)**:
  - A utility library for configuration management and other core functionalities.

12. **Environmental Configuration**:
  - Configuration management using environment variables, required for setting up network parameters, topic names, etc.


## 4. Data Model

Please refer to the [Architecture Overview](../architecture/teranode-architecture.md) document for a detailed description of the Block and Subtree data model.

Within the P2P service, notifications are sent to the Websocket clients using the following data model:

* Block notifications:

```go
			s.notificationCh <- &notificationMsg{
				Timestamp: time.Now().UTC(),
				Type:      "block",
				Hash:      blockMessage.Hash,
				BaseURL:   blockMessage.DataHubUrl,
				PeerId:    blockMessage.PeerId,
			}

```

* Subtree notifications:

```go
			s.notificationCh <- &notificationMsg{
				Type:    "subtree",
				Hash:    subtreeMessage.Hash,
				BaseURL: subtreeMessage.DataHubUrl,
				PeerId:  subtreeMessage.PeerId,
			}
```

* "MiningOn" notifications:

```go
			s.notificationCh <- &notificationMsg{
				Timestamp:    time.Now().UTC(),
				Type:         "mining_on",
				Hash:         miningOnMessage.Hash,
				BaseURL:      miningOnMessage.DataHubUrl,
				PeerId:       miningOnMessage.PeerId,
				PreviousHash: miningOnMessage.PreviousHash,
				Height:       miningOnMessage.Height,
				Miner:        miningOnMessage.Miner,
				SizeInBytes:  miningOnMessage.SizeInBytes,
				TxCount:      miningOnMessage.TxCount,
			}
```

## 5. Directory Structure and Main Files


```
./services/p2p
│
├── HandleWebsocket.go   - Manages WebSocket connections and communications.
├── Server.go            - Main server logic for the P2P service, including network handling and peer interactions.
├── client.html          - A client-side HTML file for testing or interacting with the WebSocket server.
└── dht.go               - Implements the Distributed Hash Table (DHT) functionality for the P2P network.
```


## 6. How to run

To run the P2P Service locally, you can execute the following command:

```shell
SETTINGS_CONTEXT=dev.[YOUR_USERNAME] go run -P2P=1
```

Please refer to the [Locally Running Services Documentation](../locallyRunningServices.md) document for more information on running the P2P Service locally.

## 7. Configuration options (settings flags)

The following settings can be configured for the p2p service:

- **`p2p_ip`**: Specifies the IP address for the P2P service to bind to.
- **`p2p_port`**: Defines the port number on which the P2P service listens.
- **`p2p_topic_prefix`**: Used as a prefix for naming P2P topics to ensure they are unique across different deployments or environments.
- **`p2p_block_topic`**: The topic name used for block-related messages in the P2P network.
- **`p2p_subtree_topic`**: Specifies the topic for subtree-related messages within the P2P network.
- **`p2p_bestblock_topic`**: Defines the topic for broadcasting the best block information among peers.
- **`p2p_mining_on_topic`**: The topic used for messages related to the start of mining a new block.
- **`p2p_rejected_tx_topic`**: Specifies the topic for broadcasting information about rejected transactions.
- **`p2p_shared_key`**: A shared key for securing P2P communications, required for private network configurations.
- **`p2p_dht_use_private`**: A boolean flag indicating whether a private Distributed Hash Table (DHT) should be used, enhancing network privacy.
- **`p2p_optimise_retries`**: A boolean setting to optimize retry behavior in P2P communications, potentially improving network efficiency.
- **`p2p_static_peers`**: A list of static peer addresses to connect to, ensuring the P2P node can always reach known peers.
- **`p2p_private_key`**: The private key for the P2P node, used for secure communications within the network.
- **`p2p_httpListenAddress`**: Specifies the HTTP listen address for the P2P service, enabling HTTP-based interactions.
- **`securityLevelHTTP`**: Defines the security level for HTTP communications, where a higher level might enforce HTTPS.
- **`server_certFile`** and **`server_keyFile`**: These settings specify the paths to the SSL certificate and key files, respectively, required for setting up HTTPS.
