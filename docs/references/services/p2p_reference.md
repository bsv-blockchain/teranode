# P2P Server Reference Documentation

## Overview

The P2P Server facilitates peer-to-peer communication within the Bitcoin SV network, managing the distribution of blocks, transactions, and network-related data. The server integrates with blockchain services, validation systems, and Kafka messaging to ensure efficient data propagation across the network. It implements both WebSocket and HTTP interfaces for peer communication while maintaining secure, scalable connections.

## Core Components

### Server Structure

The P2P Server is implemented through the Server struct, which coordinates all peer-to-peer communication:

```go
type Server struct {
    p2p_api.UnimplementedPeerServiceServer
    P2PNode                       *p2p.P2PNode
    logger                        ulogger.Logger
    settings                      *settings.Settings
    bitcoinProtocolID             string
    blockchainClient              blockchain.ClientI
    blockValidationClient         *blockvalidation.Client
    AssetHTTPAddressURL           string
    e                             *echo.Echo
    notificationCh                chan *notificationMsg
    rejectedTxKafkaConsumerClient kafka.KafkaConsumerGroupI
    subtreeKafkaProducerClient    kafka.KafkaAsyncProducerI
    blocksKafkaProducerClient     kafka.KafkaAsyncProducerI
    banList                       *BanList
    banChan                       chan BanEvent
}
```

The server manages several key components, each serving a specific purpose in the P2P network:
- The P2PNode handles direct peer connections and message routing
- The various Kafka clients manage message distribution across the network
- The ban system maintains network security by managing peer access
- The notification channel handles real-time event propagation

## Server Operations

### Server Initialization

The server initializes through the NewServer function:

```go
func NewServer(ctx context.Context, logger ulogger.Logger, tSettings *settings.Settings, blockchainClient blockchain.ClientI, rejectedTxKafkaConsumerClient kafka.KafkaConsumerGroupI, subtreeKafkaProducerClient kafka.KafkaAsyncProducerI, blocksKafkaProducerClient kafka.KafkaAsyncProducerI) (*Server, error)
```

This function establishes the server with required settings and dependencies, including:
- P2P network configuration (IP, port, topics)
- Topic name generation for various message types
- Ban list initialization
- Connection to blockchain services
- Kafka producer and consumer setup

### Health Management

The server implements comprehensive health checking through:

```go
func (s *Server) Health(ctx context.Context, checkLiveness bool) (int, string, error)
```

This method performs two types of health verification:

For liveness checks, it verifies basic server operation without dependency checks.

For readiness checks, it verifies:
- Kafka broker connectivity
- Blockchain client functionality
- FSM state verification
- Block validation client status

### Server Lifecycle Management

The server manages its lifecycle through several key methods:

The Init method prepares the server for operation:
```go
func (s *Server) Init(ctx context.Context) (err error)
```
It verifies and adjusts HTTP/HTTPS settings based on security requirements and establishes necessary connection URLs.

The Start method initiates server operations:
```go
func (s *Server) Start(ctx context.Context) error
```
It begins:
- Kafka message processing
- Block validation client setup
- HTTP/WebSocket server operation
- P2P network communication
- Blockchain subscription monitoring
- Ban event processing

The Stop method ensures graceful shutdown:
```go
func (s *Server) Stop(ctx context.Context) error
```
It manages the orderly shutdown of all server components and connections.


### Message Handlers

- `handleBestBlockTopic`: Handles incoming best block messages.
- `handleBlockTopic`: Handles incoming block messages.
- `handleSubtreeTopic`: Handles incoming subtree messages.
- `handleMiningOnTopic`: Handles incoming mining-on messages.

## Key Processes

### P2P Communication

The server uses a P2P node for communication with other peers in the network. It publishes and subscribes to various topics such as best blocks, blocks, subtrees, and mining-on notifications.

### Blockchain Interaction

The server interacts with the blockchain client to retrieve and validate block information. It also subscribes to blockchain notifications to propagate relevant information to peers.

### Kafka Integration

The server uses Kafka producers and consumers for efficient distribution of block and subtree data across the network.

## Configuration

The P2P Server uses various configuration values from `gocore.Config()`, including:

- P2P network configuration (IP, port, topics, etc.)
- Kafka configuration for various message types
- HTTP/HTTPS server configuration
- Security settings

## Dependencies

The P2P Server depends on several components:

- `blockchain.ClientI`: Interface for blockchain operations
- `blockvalidation.Client`: Client for block validation operations
- `p2p.P2PNode`: Node for P2P communication
- Kafka producers and consumers for message distribution

These dependencies are injected into the `Server` struct during initialization.

## Error Handling

Errors are logged using the provided logger. Critical errors may result in the server shutting down or specific components failing to start.

## Concurrency

The server uses goroutines for handling concurrent operations, such as message processing, HTTP server, and blockchain subscription listening. It also uses contexts for cancellation and timeout management.

## Security

The server supports both HTTP and HTTPS configurations based on the `securityLevelHTTP` setting. When using HTTPS, it requires certificate and key files to be specified in the configuration.

## Metrics and Monitoring

While not explicitly shown in the provided code, the server likely initializes Prometheus metrics for monitoring various aspects of its operation, as indicated by the `initPrometheusMetrics()` call in the `NewServer` function.

## Extensibility

The server is designed to be extensible, with separate handlers for different types of messages (blocks, subtrees, mining-on notifications). New message types can be added by implementing additional handler functions and subscribing to new topics.
