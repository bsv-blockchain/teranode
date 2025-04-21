# TX Validator Service Reference Documentation

## Overview

The TX Validator Service is responsible for validating transactions in a blockchain system. It ensures that transactions conform to the network's rules and policies before they are added to the blockchain. The service provides both gRPC and HTTP endpoints for transaction validation.

## Core Components

### Server

The `Server` struct is the main component of the TX Validator Service.

```go
type Server struct {
    validator_api.UnsafeValidatorAPIServer
    validator                     Interface
    logger                        ulogger.Logger
    settings                      *settings.Settings
    utxoStore                     utxo.Store
    kafkaSignal                   chan os.Signal
    stats                         *gocore.Stat
    ctx                           context.Context
    blockchainClient              blockchain.ClientI
    consumerClient                kafka.KafkaConsumerGroupI
    txMetaKafkaProducerClient     kafka.KafkaAsyncProducerI
    rejectedTxKafkaProducerClient kafka.KafkaAsyncProducerI
    httpServer                    *echo.Echo
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
) *Server
```

Creates a new `Server` instance with the provided dependencies.

#### Methods

##### gRPC Endpoints
- `Health(ctx context.Context, checkLiveness bool) (int, string, error)`: Performs health checks on the server and its dependencies.
- `HealthGRPC(ctx context.Context, _ *validator_api.EmptyMessage) (*validator_api.HealthResponse, error)`: Implements the gRPC health check endpoint.
- `ValidateTransaction(ctx context.Context, req *validator_api.ValidateTransactionRequest) (*validator_api.ValidateTransactionResponse, error)`: Validates a single transaction.
- `ValidateTransactionBatch(ctx context.Context, req *validator_api.ValidateTransactionBatchRequest) (*validator_api.ValidateTransactionBatchResponse, error)`: Validates a batch of transactions.
- `GetBlockHeight(ctx context.Context, _ *validator_api.EmptyMessage) (*validator_api.GetBlockHeightResponse, error)`: Returns the current block height.
- `GetMedianBlockTime(ctx context.Context, _ *validator_api.EmptyMessage) (*validator_api.GetMedianBlockTimeResponse, error)`: Returns the median block time.

##### HTTP Endpoints
- `handleSingleTx(ctx context.Context) echo.HandlerFunc`: Handles HTTP requests for single transaction validation.
- `handleMultipleTx(ctx context.Context) echo.HandlerFunc`: Handles HTTP requests for validating multiple transactions.
- `startHTTPServer(ctx context.Context, httpAddresses string) error`: Initializes and starts the HTTP server.
- `startAndMonitorHTTPServer(ctx context.Context, httpAddresses string)`: Starts and monitors the HTTP server.

##### Lifecycle Methods
- `Init(ctx context.Context) error`: Initializes the server, setting up the validator and Kafka consumer.
- `Start(ctx context.Context, readyCh chan<- struct{}) error`: Starts the server, including Kafka consumer, gRPC server, and HTTP server.
- `Stop(_ context.Context) error`: Stops the server and its components.

### Validator

The `Validator` struct implements the core validation logic.

```go
type Validator struct {
    logger                        ulogger.Logger
    settings                      *settings.Settings
    txValidator                   TxValidatorI
    utxoStore                     utxo.Store
    blockAssembler                blockassembly.Store
    saveInParallel                bool
    stats                         *gocore.Stat
    txmetaKafkaProducerClient     kafka.KafkaAsyncProducerI
    rejectedTxKafkaProducerClient kafka.KafkaAsyncProducerI
}
```

#### Constructor

```go
func New(ctx context.Context, logger ulogger.Logger, tSettings *settings.Settings, store utxo.Store,
    txMetaKafkaProducerClient kafka.KafkaAsyncProducerI,
    rejectedTxKafkaProducerClient kafka.KafkaAsyncProducerI) (Interface, error)
```

Creates a new `Validator` instance with the provided dependencies.

#### Methods

- `Health(ctx context.Context, checkLiveness bool) (int, string, error)`: Performs health checks on the validator and its dependencies.
- `GetBlockHeight() uint32`: Returns the current block height.
- `GetMedianBlockTime() uint32`: Returns the median block time.
- `Validate(ctx context.Context, tx *bt.Tx, blockHeight uint32, opts ...Option) (*meta.Data, error)`: Validates a transaction with optional settings, returning transaction metadata.
- `ValidateWithOptions(ctx context.Context, tx *bt.Tx, blockHeight uint32, validationOptions *Options) (*meta.Data, error)`: Validates a transaction with explicit options.
- `TriggerBatcher()`: Triggers the batcher (currently a no-op).
- `CreateInUtxoStore(traceSpan tracing.Span, tx *bt.Tx, blockHeight uint32, markAsConflicting bool, markAsUnspendable bool) (*meta.Data, error)`: Stores transaction metadata in the UTXO store.

### TxValidator

The validator package defines multiple interfaces for transaction validation:

#### TxValidatorI Interface

```go
type TxValidatorI interface {
    // ValidateTransaction performs comprehensive validation of a transaction
    ValidateTransaction(tx *bt.Tx, blockHeight uint32, validationOptions *Options) error

    // ValidateTransactionScripts performs script validation for a transaction
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

The validator supports multiple script interpreters (GoBT, GoSDK, GoBDK) through a factory pattern.

## Key Functions

### TxValidator Methods

- `ValidateTransaction(tx *bt.Tx, blockHeight uint32, validationOptions *Options) error`: Performs comprehensive validation checks on a transaction.
- `ValidateTransactionScripts(tx *bt.Tx, blockHeight uint32, utxoHeights []uint32, validationOptions *Options) error`: Validates transaction scripts.
- `checkOutputs(tx *bt.Tx, blockHeight uint32) error`: Validates transaction outputs.
- `checkInputs(tx *bt.Tx, blockHeight uint32) error`: Validates transaction inputs.
- `checkTxSize(txSize int) error`: Checks if the transaction size is within the allowed limit.
- `checkFees(tx *bt.Tx, feeQuote *bt.FeeQuote) error`: Verifies if the transaction fee is sufficient.
- `sigOpsCheck(tx *bt.Tx, validationOptions *Options) error`: Checks the number of signature operations in the transaction.
- `pushDataCheck(tx *bt.Tx) error`: Ensures that unlocking scripts only push data onto the stack.

### Validator Methods

- `validateInternal(ctx context.Context, tx *bt.Tx, blockHeight uint32, validationOptions *Options) (txMetaData *meta.Data, err error)`: Performs the core validation logic for a transaction.
- `validateTransaction(ctx context.Context, tx *bt.Tx, blockHeight uint32, validationOptions *Options) error`: Performs transaction-level validation checks.
- `validateTransactionScripts(ctx context.Context, tx *bt.Tx, blockHeight uint32, utxoHeights []uint32, validationOptions *Options) error`: Performs script validation for a transaction.
- `spendUtxos(traceSpan tracing.Span, tx *bt.Tx, ignoreUnspendable bool) ([]*utxo.Spend, error)`: Attempts to spend the UTXOs referenced by transaction inputs.
- `sendToBlockAssembler(traceSpan tracing.Span, bData *blockassembly.Data, reservedUtxos []*utxo.Spend) error`: Sends validated transaction data to the block assembler.
- `extendTransaction(ctx context.Context, tx *bt.Tx) error`: Adds previous output information to transaction inputs.

## Configuration

The TX Validator Service uses various configuration options, including:

- Kafka settings for transaction processing
- Policy settings for transaction validation
- Network parameters (mainnet, testnet, etc.)

## Error Handling

The service uses custom error types defined in the `errors` package to provide detailed information about validation failures.

## Metrics

Prometheus metrics are used to monitor various aspects of the validation process, including:

- Transaction validation time
- Number of invalid transactions
- Transaction size distribution

## Kafka Integration

The service integrates with Kafka for:

- Consuming transactions to be validated
- Producing metadata for validated transactions
- Producing information about rejected transactions
