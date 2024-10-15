# P2P Server Reference Documentation

## Overview

The P2P Server is a component of a Bitcoin SV implementation that facilitates peer-to-peer communication for blocks, transactions, and other network-related data. It integrates with various services such as blockchain, block validation, and Kafka for efficient data distribution and processing.

## Types

### Server

```go
type Server struct {
P2PNode                       *p2p.P2PNode
logger                        ulogger.Logger
bitcoinProtocolID             string
blockchainClient              blockchain.ClientI
blockValidationClient         *blockvalidation.Client
AssetHTTPAddressURL           string
e                             *echo.Echo
notificationCh                chan *notificationMsg
rejectedTxKafkaConsumerClient *kafka.KafkaConsumerGroup
subtreeKafkaProducerClient    *kafka.KafkaAsyncProducer
blocksKafkaProducerClient     *kafka.KafkaAsyncProducer
kafkaHealthURL                *url.URL
}
```

The `Server` struct is the main type for the P2P Server. It contains various components for P2P communication, blockchain interaction, and message handling.

## Functions

### NewServer

```go
func NewServer(ctx context.Context, logger ulogger.Logger, blockchainClient blockchain.ClientI) (*Server, error)
```

Creates a new instance of the P2P Server with the provided dependencies.

## Methods

### Health

```go
func (s *Server) Health(ctx context.Context, checkLiveness bool) (int, string, error)
```

Performs health checks on the server and its dependencies. It returns an HTTP status code, a description, and an error if any.

### Init

```go
func (s *Server) Init(ctx context.Context) (err error)
```

Initializes the P2P Server, setting up Kafka producers and consumers, and other necessary components.

### Start

```go
func (s *Server) Start(ctx context.Context) error
```

Starts the P2P Server, including HTTP server, P2P node, and blockchain subscription listener.

### StartHTTP

```go
func (s *Server) StartHTTP(ctx context.Context) error
```

Starts the HTTP server for the P2P Server.

### Stop

```go
func (s *Server) Stop(ctx context.Context) error
```

Stops the P2P Server and its components.

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
