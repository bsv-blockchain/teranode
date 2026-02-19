# Propagation Server Reference Documentation

## Overview

The Propagation Server is a component of a Bitcoin SV implementation that handles the propagation of transactions across the network. It supports multiple communication protocols, including UDP and gRPC, and integrates with various services such as transaction validation, blockchain, and Kafka for efficient data distribution and processing.

## Types

### PropagationServer

```go
type PropagationServer struct {
    propagation_api.UnsafePropagationAPIServer
    logger                       ulogger.Logger                // Structured logging interface
    settings                     *settings.Settings           // Service configuration settings
    stats                        *gocore.Stat                 // Performance metrics collection
    txStore                      blob.Store                   // Transaction storage backend
    validator                    validator.Interface          // Transaction validation service
    blockchainClient             blockchain.ClientI           // Blockchain state interface
    validatorKafkaProducerClient kafka.KafkaAsyncProducerI    // Kafka producer for async validation
    httpServer                   *echo.Echo                   // HTTP server for REST endpoints
    validatorHTTPAddr            *url.URL                     // Validator HTTP endpoint URL
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
func (ps *PropagationServer) Start(ctx context.Context, readyCh chan<- struct{}) (err error)
```

Starts the Propagation Server, including FSM state restoration (if configured), UDP6 multicast listeners, Kafka producer initialization, HTTP server, and gRPC server setup. Once initialized, it signals readiness by closing the readyCh channel. The function blocks until the gRPC server is running or an error occurs.

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

Processes a batch of transactions with the following limits:

- Maximum 1024 transactions per batch request
- Maximum 32 MB total data size per batch request

## Additional Methods

### StartUDP6Listeners

```go
func (ps *PropagationServer) StartUDP6Listeners(ctx context.Context, ipv6Addresses string) error
```

Initializes IPv6 multicast listeners for transaction propagation. It creates UDP listeners on specified interfaces and addresses, processing incoming transactions in separate goroutines. The `ipv6Addresses` parameter is a comma-separated list of IPv6 multicast addresses to listen on.

### HTTP Server Methods

```go
func (ps *PropagationServer) handleSingleTx(ctx context.Context) echo.HandlerFunc
```

Handles a single transaction request on the `/tx` endpoint.

```go
func (ps *PropagationServer) handleMultipleTx(ctx context.Context) echo.HandlerFunc
```

Handles multiple transactions on the `/txs` endpoint.

```go
func (ps *PropagationServer) startHTTPServer(ctx context.Context, httpAddresses string) error
```

Initializes and starts the HTTP server for transaction processing. The `httpAddresses` parameter is a comma-separated list of address:port combinations to bind to.

```go
func (ps *PropagationServer) startAndMonitorHTTPServer(ctx context.Context, httpAddresses string)
```

Starts the HTTP server and monitors for shutdown. This method launches the HTTP server in a non-blocking manner and ensures proper cleanup when the context is canceled.

### Internal Transaction Processing

```go
func (ps *PropagationServer) processTransaction(ctx context.Context, req *propagation_api.ProcessTransactionRequest) error
```

Handles the core transaction processing logic including validation, storage, and triggering async validation.

```go
func (ps *PropagationServer) processTransactionInternal(ctx context.Context, btTx *bt.Tx) error
```

Performs the core business logic for processing a transaction, including coinbase validation, sanity checks, storage, and routing to appropriate validation paths (Kafka or HTTP).

```go
func (ps *PropagationServer) storeTransaction(ctx context.Context, btTx *bt.Tx) error
```

Persists a transaction to the configured storage backend using its chain hash as the key.

```go
func (ps *PropagationServer) validateTransactionViaKafka(btTx *bt.Tx) error
```

Sends a transaction to the validator through Kafka.

```go
func (ps *PropagationServer) validateTransactionViaHTTP(ctx context.Context, btTx *bt.Tx, txSize int, maxKafkaMessageSize int) error
```

Sends a transaction to the validator's HTTP endpoint. This is used as a fallback when Kafka message size limits are exceeded.

```go
func (ps *PropagationServer) txSanityChecks(btTx *bt.Tx) error
```

Performs basic sanity checks on transactions to ensure they have at least one input and one output.

## Key Processes

### Transaction Processing

1. The server receives transactions through various protocols (UDP6 multicast, HTTP, gRPC).
2. Transactions undergo basic sanity checks to ensure they have at least one input and one output.
3. Transactions are validated to ensure they are not coinbase transactions (coinbase transactions are rejected).
4. Valid transactions are stored in the transaction store using their chain hash as the key.
5. Transactions are sent to the validator either via Kafka (for normal-sized transactions) or HTTP (for large transactions exceeding Kafka message size limits) for further processing.
6. Size-based routing: transactions larger than the configured Kafka message size limit automatically use HTTP fallback validation.

### UDP6 Multicast Listening

The server listens on multiple IPv6 multicast addresses for incoming transactions. The implementation has the following characteristics:

- Supports configurable UDP datagram size (default: 512 bytes)
- Uses the default IPv6 port 9999 for multicast listeners
- Creates independent listeners for each multicast address specified in `settings.Propagation.IPv6Addresses`
- Processes incoming datagrams concurrently through separate goroutines

### HTTP Integration

The server provides HTTP endpoints for transaction submission configured through `settings.Propagation.HTTPListenAddress`:

- `/tx` endpoint for single transaction submissions
- `/txs` endpoint for batch transaction submissions
- `/health` endpoint for service health checks
- `/*` catch-all endpoint that returns "Unknown route" for unmatched paths
- Supports rate limiting for API protection when `HTTPRateLimit` is configured

### Kafka Integration

The server uses a Kafka producer to send transactions to a validator service for asynchronous processing. When transactions exceed the Kafka message size limit, it automatically falls back to HTTP-based validation.

## Configuration

The Propagation Server is configured through the settings system instead of directly using `gocore.Config()`, including:

### Propagation Settings

- `settings.Propagation.IPv6Addresses`: Comma-separated list of IPv6 multicast addresses for UDP listeners
- `settings.Propagation.IPv6Interface`: Network interface for IPv6 multicast (default: "en0")
- `settings.Propagation.HTTPListenAddress`: HTTP addresses for transaction submission endpoints
- `settings.Propagation.HTTPAddresses`: Array of HTTP addresses for multiple endpoint configurations
- `settings.Propagation.HTTPRateLimit`: HTTP request rate limiting (requests per second)
- `settings.Propagation.AlwaysUseHTTP`: Boolean flag to prefer HTTP over other protocols
- `settings.Propagation.SendBatchSize`: Batch size for sending transactions (default: 100)
- `settings.Propagation.SendBatchTimeout`: Timeout for batch sending operations (default: 5 seconds)
- `settings.Propagation.GRPCListenAddress`: gRPC server address for the Propagation API
- `settings.Propagation.GRPCAddresses`: Array of gRPC addresses for multiple endpoint configurations
- `settings.Propagation.GRPCMaxConnectionAge`: Maximum age for gRPC connections before forced refresh

### Validator Settings

- `settings.Validator.HTTPAddress`: HTTP address for the validator service (used for fallback validation)
- `settings.Validator.KafkaMaxMessageBytes`: Maximum Kafka message size for transaction routing (default: 1MB)

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

## Security

The server supports various security levels for HTTP/HTTPS configurations.

## Metrics

The server initializes Prometheus metrics for monitoring various aspects of its operation, including:

- Processed transactions count and duration
- Transaction sizes
- Invalid transactions count

## Extensibility

The server is designed to be extensible, supporting multiple communication protocols (UDP, gRPC) for transaction ingestion. New protocols or processing methods can be added by implementing additional handlers and integrating them into the server's start-up process.

## Related Documents

- [Propagation Topic Guide](../../topics/services/propagation.md)
- [Propagation Settings](../settings/services/propagation_settings.md)
- [Propagation Protobuf Reference](../protobuf_docs/propagationProto.md)
- [Prometheus Metrics](../prometheusMetrics.md)
