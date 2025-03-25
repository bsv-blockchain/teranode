# Propagation Server Reference Documentation

## Overview

The Propagation Server is a component of a Bitcoin SV implementation that handles the propagation of transactions across the network. It supports multiple communication protocols, including UDP and gRPC, and integrates with various services such as transaction validation, blockchain, and Kafka for efficient data distribution and processing.

## Types

### PropagationServer

```go
type PropagationServer struct {
    propagation_api.UnsafePropagationAPIServer
    logger                       ulogger.Logger
    settings                     *settings.Settings
    stats                        *gocore.Stat
    txStore                      blob.Store
    validator                    validator.Interface
    blockchainClient             blockchain.ClientI
    validatorKafkaProducerClient kafka.KafkaAsyncProducerI
}
```

The `PropagationServer` struct is the main type for the Propagation Server. It contains various components for transaction processing, validation, and distribution.

## Functions

### New

```go
func New(logger ulogger.Logger, tSettings *settings.Settings, txStore blob.Store, validatorClient validator.Interface, blockchainClient blockchain.ClientI, validatorKafkaProducerClient kafka.KafkaAsyncProducerI) *PropagationServer
```

Creates a new instance of the Propagation Server with the provided dependencies.

## Methods

### Health

```go
func (ps *PropagationServer) Health(ctx context.Context, checkLiveness bool) (int, string, error)
```

Performs health checks on the server and its dependencies.

### HealthGRPC

```go
func (ps *PropagationServer) HealthGRPC(ctx context.Context, _ *propagation_api.EmptyMessage) (*propagation_api.HealthResponse, error)
```

Performs a gRPC health check on the Propagation Server.

### Init

```go
func (ps *PropagationServer) Init(_ context.Context) (err error)
```

Initializes the Propagation Server.

### Start

```go
func (ps *PropagationServer) Start(ctx context.Context) (err error)
```

Starts the Propagation Server, including UDP6 listeners and Kafka producer.

### Stop

```go
func (ps *PropagationServer) Stop(_ context.Context) error
```

Stops the Propagation Server.

### ProcessTransaction

```go
func (ps *PropagationServer) ProcessTransaction(ctx context.Context, req *propagation_api.ProcessTransactionRequest) (*propagation_api.EmptyMessage, error)
```

Processes a single transaction.

### ProcessTransactionBatch

```go
func (ps *PropagationServer) ProcessTransactionBatch(ctx context.Context, req *propagation_api.ProcessTransactionBatchRequest) (*propagation_api.ProcessTransactionBatchResponse, error)
```

Processes a batch of transactions.


## Key Processes

### Transaction Processing

1. The server receives transactions through various protocols (UDP, gRPC).
2. Transactions are validated to ensure they are not coinbase transactions and are in the extended format.
3. Valid transactions are stored in the transaction store.
4. Transactions are sent to the validator (either directly or via Kafka) for further processing.

### UDP6 Multicast Listening

The server can listen on multiple IPv6 multicast addresses for incoming transactions.

### Kafka Integration

The server uses a Kafka producer to send transactions to a validator service for asynchronous processing.

## Configuration

The Propagation Server uses various configuration values from `gocore.Config()`, including:

- IPv6 multicast addresses and interface
- Kafka configuration for validator transactions
- gRPC server settings

## Dependencies

The Propagation Server depends on several components:

- `blob.Store`: For storing transactions
- `validator.Interface`: For transaction validation
- `blockchain.ClientI`: For blockchain interactions
- Kafka producer for sending transactions to the validator

These dependencies are injected into the `PropagationServer` struct during initialization.

## Error Handling

Errors are wrapped using a custom error package, providing additional context and maintaining consistency across the application. The server logs errors and, in many cases, returns them to the caller.

## Concurrency

The server uses goroutines and error groups for handling concurrent operations, such as processing batches of transactions. It also uses contexts for cancellation and timeout management.

## Metrics

The server initializes Prometheus metrics for monitoring various aspects of its operation, including:

- Processed transactions count and duration
- Transaction sizes
- Invalid transactions count

## Extensibility

The server is designed to be extensible, supporting multiple communication protocols (UDP, gRPC) for transaction ingestion. New protocols or processing methods can be added by implementing additional handlers and integrating them into the server's start-up process.
