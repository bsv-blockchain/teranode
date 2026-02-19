# TX Validator Service Reference Documentation

## Overview

The TX Validator Service is responsible for validating transactions in a Bitcoin SV blockchain system. It ensures that transactions conform to the network's consensus rules and policy requirements before they are added to the blockchain. The service provides both gRPC and HTTP endpoints for transaction validation and integrates with block assembly for mining operations.

The validator package implements comprehensive transaction validation functionality for Bitcoin SV nodes, including script verification, UTXO management, and policy enforcement. It is a critical component of the Teranode architecture that handles transaction validation according to Bitcoin SV consensus rules, manages UTXO state transitions, and ensures that only valid transactions are accepted into the mempool and blocks.

## Core Components

### Server

The `Server` struct is the main component of the TX Validator Service. It acts as the primary coordinator for transaction validation requests, integrating with multiple components (UTXO store, blockchain, Kafka) to provide a complete validation service.

```go
type Server struct {
    // UnsafeValidatorAPIServer embeds the base gRPC server implementation for the validator API,
    // providing the foundational gRPC service methods that can be overridden by this Server implementation.
    // This embedding pattern allows the Server to implement the validator API interface while maintaining
    // flexibility for custom method implementations and middleware integration.
    validator_api.UnsafeValidatorAPIServer

    // validator is the core validation engine that implements the Interface contract for transaction validation.
    // This component handles all validation logic including consensus rule enforcement, script execution,
    // and UTXO verification. It serves as the primary business logic layer for the validation service.
    validator Interface

    // logger provides structured logging capabilities for the validator server, enabling comprehensive
    // monitoring and debugging of validation operations. All server activities, errors, and performance
    // metrics are logged through this component for operational visibility.
    logger ulogger.Logger

    // settings contains the complete configuration for the validator service, including validation parameters,
    // network settings, database connections, and operational thresholds. These settings control the
    // behavior of all validation operations and service integrations.
    settings *settings.Settings

    // utxoStore provides direct access to the UTXO database for transaction input validation and
    // double-spend prevention. This store is used to verify transaction inputs, check UTXO availability,
    // and maintain the current state of unspent transaction outputs across the blockchain.
    utxoStore utxo.Store

    // kafkaSignal is a channel used to coordinate graceful shutdown of Kafka-related components.
    // When a shutdown signal is received, this channel notifies all Kafka consumers and producers
    // to complete their current operations and terminate cleanly.
    kafkaSignal chan os.Signal

    // stats collects performance metrics for the validator service, including request counts,
    // processing times, and error rates. These metrics are used for monitoring and optimization.
    stats *gocore.Stat

    // ctx is the server's main context used for operation management and cancellation.
    // This context is used to manage the lifecycle of the server and its components.
    ctx context.Context

    // blockchainClient connects to the blockchain service for block-related operations,
    // including block height retrieval, chain state verification, and FSM synchronization.
    // This client is used to ensure the validator service remains synchronized with the blockchain.
    blockchainClient blockchain.ClientI

    // consumerClient receives validation requests via Kafka, providing an asynchronous
    // validation path for high-throughput processing. This client is used to consume
    // Kafka messages and trigger validation operations.
    consumerClient kafka.KafkaConsumerGroupI

    // txMetaKafkaProducerClient publishes transaction metadata to Kafka, enabling
    // downstream processing and analytics. This producer is used to send transaction
    // metadata to Kafka topics for further processing.
    txMetaKafkaProducerClient kafka.KafkaAsyncProducerI

    // rejectedTxKafkaProducerClient publishes rejected transaction information to Kafka,
    // providing visibility into validation failures and error conditions. This producer
    // is used to send rejected transaction data to Kafka topics for monitoring and analysis.
    rejectedTxKafkaProducerClient kafka.KafkaAsyncProducerI

    // blockAssemblyClient connects to the block assembly service for mining integration,
    // enabling the validator service to participate in block template generation and
    // transaction inclusion in mining operations. This client is used to interact with
    // the block assembly service and coordinate transaction inclusion.
    blockAssemblyClient blockassembly.ClientI

    // httpServer handles HTTP API requests for transaction validation, providing a
    // synchronous validation path for clients. This server is used to process HTTP
    // requests and return validation results.
    httpServer *echo.Echo
}
```

#### Constructor

```go
func NewServer(
    logger ulogger.Logger,
    tSettings *settings.Settings,
    utxoStore utxo.Store,
    blockchainClient blockchain.ClientI,
    consumerClient kafka.KafkaConsumerGroupI,
    txMetaKafkaProducerClient kafka.KafkaAsyncProducerI,
    rejectedTxKafkaProducerClient kafka.KafkaAsyncProducerI,
    blockAssemblyClient blockassembly.ClientI,
) *Server
```

Creates and initializes a new validator server instance with the specified components. This function initializes Prometheus metrics and configures the server with all required dependencies for transaction validation. It does not start any background processes or establish connections - that happens in the `Init` and `Start` methods.

#### Methods

##### gRPC Endpoints
- `Health(ctx context.Context, checkLiveness bool) (int, string, error)`: Performs health checks on the validator service and its dependencies. When used as a liveness check (checkLiveness=true), it only verifies basic service operation. When used as a readiness check (checkLiveness=false), it performs comprehensive dependency checks.
- `HealthGRPC(ctx context.Context, _ *validator_api.EmptyMessage) (*validator_api.HealthResponse, error)`: Implements the gRPC health check endpoint. This method provides the gRPC interface for health checking and records metrics for monitoring purposes.
- `ValidateTransaction(ctx context.Context, req *validator_api.ValidateTransactionRequest) (*validator_api.ValidateTransactionResponse, error)`: Validates a single transaction. This method is part of the validator_api.ValidatorAPIServer interface and serves as the public API entry point for transaction validation requests.
- `ValidateTransactionBatch(ctx context.Context, req *validator_api.ValidateTransactionBatchRequest) (*validator_api.ValidateTransactionBatchResponse, error)`: Validates a batch of transactions. This method provides significant performance optimization over individual validation by processing multiple transactions in parallel using Go's errgroup.
- `GetBlockHeight(ctx context.Context, _ *validator_api.EmptyMessage) (*validator_api.GetBlockHeightResponse, error)`: Returns the current block height. This method provides a critical service for clients needing to know the current chain state.
- `GetMedianBlockTime(ctx context.Context, _ *validator_api.EmptyMessage) (*validator_api.GetMedianBlockTimeResponse, error)`: Returns the median time of recent blocks. This method provides access to the median timestamp of the last several blocks, which is critical for time-based transaction features like nLockTime.

##### HTTP Endpoints
- `handleSingleTx(ctx context.Context) echo.HandlerFunc`: Handles HTTP requests for single transaction validation. This method implements an HTTP handler for validating a single Bitcoin transaction submitted via POST request.
- `handleMultipleTx(ctx context.Context) echo.HandlerFunc`: Handles HTTP requests for validating multiple transactions. This method implements an HTTP handler for validating a stream of Bitcoin transactions submitted via POST request.
- `startHTTPServer(ctx context.Context, httpAddresses string) error`: Initializes and starts the HTTP server for transaction processing. This method configures and launches an Echo web server that provides HTTP REST endpoints for transaction validation.
- `startAndMonitorHTTPServer(ctx context.Context, httpAddresses string)`: Starts the HTTP server and monitors for shutdown. This method launches the HTTP server in a non-blocking manner using goroutines, allowing the main server thread to continue execution.

##### Lifecycle Methods
- `Init(ctx context.Context) error`: Initializes the validator server and sets up the core validation engine. This method must be called before Start() and after NewServer(). It creates the core validator component and performs necessary setup operations.
- `Start(ctx context.Context, readyCh chan<- struct{}) error`: Begins the validator server operation and registers handlers for validation requests. This method initiates all background processing, including Kafka consumer setup, HTTP API servers, and synchronization with the blockchain FSM.
- `Stop(_ context.Context) error`: Gracefully shuts down the validator server and all associated components. This method performs an orderly shutdown of all server resources, including Kafka producers/consumers and any background tasks.

### Validator

The `Validator` struct implements Bitcoin SV transaction validation and manages the lifecycle of transactions from validation through block assembly.

```go
type Validator struct {
    logger                        ulogger.Logger
    settings                      *settings.Settings
    txValidator                   TxValidatorI
    utxoStore                     utxo.Store
    blockAssembler                blockassembly.Store
    blockchainClient              blockchain.ClientI
    stats                         *gocore.Stat
    txmetaKafkaProducerClient     kafka.KafkaAsyncProducerI
    rejectedTxKafkaProducerClient kafka.KafkaAsyncProducerI
}
```

#### Constructor

```go
func New(ctx context.Context, logger ulogger.Logger, tSettings *settings.Settings, store utxo.Store,
    txMetaKafkaProducerClient kafka.KafkaAsyncProducerI,
    rejectedTxKafkaProducerClient kafka.KafkaAsyncProducerI,
    blockAssemblyClient blockassembly.ClientI, blockchainClient blockchain.ClientI) (Interface, error)
```

Creates a new `Validator` instance with the provided configuration. It initializes the validator with the given logger, UTXO store, and Kafka producers. Returns an error if initialization fails.

#### Methods

- `Health(ctx context.Context, checkLiveness bool) (int, string, error)`: Performs health checks on the validator and its dependencies. When checkLiveness is true, only checks service liveness. When false, performs full readiness check including dependencies.
- `GetBlockHeight() uint32`: Returns the current block height from the UTXO store.
- `GetMedianBlockTime() uint32`: Returns the median block time from the UTXO store.
- `Validate(ctx context.Context, tx *bt.Tx, blockHeight uint32, opts ...Option) (*meta.Data, error)`: Performs comprehensive validation of a transaction. It checks transaction finality, validates inputs and outputs, updates the UTXO set, and optionally adds the transaction to block assembly.
- `ValidateWithOptions(ctx context.Context, tx *bt.Tx, blockHeight uint32, validationOptions *Options) (*meta.Data, error)`: Performs comprehensive validation of a transaction with explicit options. This method is the core transaction validation entry point that implements the full Bitcoin validation ruleset.
- `TriggerBatcher()`: Triggers the batcher (currently a no-op).
- `CreateInUtxoStore(traceSpan tracing.Span, tx *bt.Tx, blockHeight uint32, markAsConflicting bool, markAsLocked bool) (*meta.Data, error)`: Stores transaction metadata in the UTXO store. Returns transaction metadata and error if storage fails.

### TxValidator

The `TxValidator` implements transaction validation logic based on Bitcoin SV consensus rules and configurable policy settings.

```go
type TxValidator struct {
    logger      ulogger.Logger
    settings    *settings.Settings
    interpreter TxScriptInterpreter
    options     *TxValidatorOptions
}
```

The validator package defines multiple interfaces for transaction validation:

#### TxValidatorI Interface

```go
type TxValidatorI interface {
    // ValidateTransaction performs comprehensive validation of a transaction.
    // This method enforces all consensus and policy rules against the transaction,
    // including format, structure, inputs/outputs, signature verification, and fees.
    ValidateTransaction(tx *bt.Tx, blockHeight uint32, validationOptions *Options) error

    // ValidateTransactionScripts performs script validation for a transaction.
    // This method specifically handles the script execution and signature verification
    // portion of validation, which is typically the most computationally intensive part.
    ValidateTransactionScripts(tx *bt.Tx, blockHeight uint32, utxoHeights []uint32, validationOptions *Options) error
}
```

#### TxScriptInterpreter Interface

```go
type TxScriptInterpreter interface {
    // VerifyScript implements script verification for a transaction
    VerifyScript(tx *bt.Tx, blockHeight uint32, consensus bool, utxoHeights []uint32) error

    // Interpreter returns the interpreter being used
    Interpreter() TxInterpreter
}
```

The validator supports multiple script interpreters through a factory pattern:

- **GoBT**: Pure Go implementation from the libsv/go-bt library
- **GoSDK**: Bitcoin SV SDK implementation
- **GoBDK**: Bitcoin Development Kit implementation

## Key Functions

### TxValidator Methods

- `ValidateTransaction(tx *bt.Tx, blockHeight uint32, validationOptions *Options) error`: Performs comprehensive validation checks on a transaction. This includes checking input and output presence, transaction size limits, input values and coinbase restrictions, output values and dust limits, lock time requirements, script operation limits, script validation, and fee requirements.
- `ValidateTransactionScripts(tx *bt.Tx, blockHeight uint32, utxoHeights []uint32, validationOptions *Options) error`: Validates transaction scripts using the configured script interpreter.
- `checkOutputs(tx *bt.Tx, blockHeight uint32) error`: Validates transaction outputs, checking for dust values and other output-specific rules.
- `checkInputs(tx *bt.Tx, blockHeight uint32) error`: Validates transaction inputs, checking for proper formatting and sequence values.
- `checkTxSize(txSize int) error`: Checks if the transaction size is within the allowed policy limit.
- `checkFees(tx *bt.Tx, feeQuote *bt.FeeQuote) error`: Verifies if the transaction fee is sufficient according to the fee policy.
- `sigOpsCheck(tx *bt.Tx, validationOptions *Options) error`: Checks the number of signature operations in the transaction against policy limits.
- `pushDataCheck(tx *bt.Tx) error`: Ensures that unlocking scripts only push data onto the stack, enforcing Bitcoin's signature script policy.

### Validator Methods

- `validateInternal(ctx context.Context, tx *bt.Tx, blockHeight uint32, validationOptions *Options) (txMetaData *meta.Data, err error)`: Performs the core validation logic for a transaction. This method contains the detailed step-by-step transaction validation workflow and manages the entire lifecycle of a transaction from initial validation through UTXO updates and optional block assembly integration.
- `validateTransaction(ctx context.Context, tx *bt.Tx, blockHeight uint32, validationOptions *Options) error`: Performs transaction-level validation checks. Ensures transaction is properly extended and meets all validation rules.
- `validateTransactionScripts(ctx context.Context, tx *bt.Tx, blockHeight uint32, utxoHeights []uint32, validationOptions *Options) error`: Performs script validation for a transaction. Returns error if validation fails.
- `spendUtxos(traceSpan tracing.Span, tx *bt.Tx, ignoreLocked bool) ([]*utxo.Spend, error)`: Attempts to spend the UTXOs referenced by transaction inputs. Returns the spent UTXOs and error if spending fails.
- `sendToBlockAssembler(traceSpan tracing.Span, bData *blockassembly.Data, reservedUtxos []*utxo.Spend) error`: Sends validated transaction data to the block assembler. Returns error if block assembly integration fails.
- `extendTransaction(ctx context.Context, tx *bt.Tx) error`: Adds previous output information to transaction inputs. Returns error if required parent transaction data cannot be found.

## Configuration

The TX Validator Service uses various configuration options, including:

- **Kafka settings**: Configuration for Kafka producers and consumers used in transaction processing
- **Policy settings**: Transaction validation policy parameters, including fee rates, size limits, and other enforcement rules
- **Network parameters**: Mainnet, testnet, or regtest configuration affecting consensus rules
- **Block assembly settings**: Configuration for integrating validation with block template generation
- **Script interpreter selection**: Choice of script validation engine (GoBT, GoSDK, GoBDK)

## Error Handling

The service uses custom error types defined in the `errors` package to provide detailed information about validation failures:

- **TxInvalidError**: Indicates that a transaction failed validation rules
- **ScriptVerifyError**: Specific to script verification failures
- **ProcessingError**: General processing errors not directly related to transaction validity
- **StorageError**: Errors related to UTXO storage operations
- **ConfigurationError**: Errors in service configuration

## Metrics

Prometheus metrics are used to monitor various aspects of the validation process, including:

- Transaction validation time (histogram)
- Number of invalid transactions (counter)
- Transaction size distribution (histogram)
- Script verification time (histogram)
- Fee rate distribution (histogram)
- Number of transactions processed (counter)
- Number of UTXOs created/spent (counter)

## Kafka Integration

The service integrates with Kafka for:

- Consuming transactions to be validated from other services
- Producing metadata for validated transactions for downstream consumers
- Producing information about rejected transactions for monitoring and analysis
- Supporting asynchronous processing for high throughput

## Related Documents

- [Validator Topic Guide](../../topics/services/validator.md)
- [Validator Settings](../settings/services/validator_settings.md)
- [Validator Protobuf Reference](../protobuf_docs/validatorProto.md)
- [Prometheus Metrics](../prometheusMetrics.md)
