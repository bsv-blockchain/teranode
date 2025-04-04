# TX Validator Service Reference Documentation

## Overview

The TX Validator Service is responsible for validating transactions in a blockchain system. It ensures that transactions conform to the network's rules and policies before they are added to the blockchain.

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

- `Health(ctx context.Context, checkLiveness bool) (int, string, error)`: Performs health checks on the server and its dependencies.
- `HealthGRPC(ctx context.Context, _ *validator_api.EmptyMessage) (*validator_api.HealthResponse, error)`: Implements the gRPC health check endpoint.
- `Init(ctx context.Context) error`: Initializes the server, setting up the validator and Kafka consumer.
- `Start(ctx context.Context) error`: Starts the server, including Kafka consumer and gRPC server.
- `Stop(_ context.Context) error`: Stops the server.
- `ValidateTransaction(ctx context.Context, req *validator_api.ValidateTransactionRequest) (*validator_api.ValidateTransactionResponse, error)`: Validates a single transaction.
- `ValidateTransactionBatch(ctx context.Context, req *validator_api.ValidateTransactionBatchRequest) (*validator_api.ValidateTransactionBatchResponse, error)`: Validates a batch of transactions.
- `GetBlockHeight(ctx context.Context, _ *validator_api.EmptyMessage) (*validator_api.GetBlockHeightResponse, error)`: Returns the current block height.
- `GetMedianBlockTime(ctx context.Context, _ *validator_api.EmptyMessage) (*validator_api.GetMedianBlockTimeResponse, error)`: Returns the median block time.

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
func New(ctx context.Context, logger ulogger.Logger, store utxo.Store) (Interface, error)
```

Creates a new `Validator` instance.

#### Methods

- `Health(ctx context.Context, checkLiveness bool) (int, string, error)`: Performs health checks on the validator and its dependencies.
- `GetBlockHeight() uint32`: Returns the current block height.
- `GetMedianBlockTime() uint32`: Returns the median block time.
- `Validate(ctx context.Context, tx *bt.Tx, blockHeight uint32, opts ...Option) error`: Validates a transaction.
- `TriggerBatcher()`: Triggers the batcher (currently a no-op).

### TxValidator

The `TxValidator` interface defines the contract for transaction validation.

```go
type TxValidator interface {
Logger() ulogger.Logger
Params() *chaincfg.Params
PolicySettings() *PolicySettings
VerifyScript(tx *bt.Tx, blockHeight uint32) error
}
```

## Key Functions

- `ValidateTransaction(tv TxValidator, tx *bt.Tx, blockHeight uint32) error`: Performs comprehensive validation checks on a transaction.
- `checkOutputs(tv TxValidator, tx *bt.Tx, blockHeight uint32) error`: Validates transaction outputs.
- `checkInputs(tv TxValidator, tx *bt.Tx, blockHeight uint32) error`: Validates transaction inputs.
- `checkTxSize(txSize int, policy *PolicySettings) error`: Checks if the transaction size is within the allowed limit.
- `checkFees(tx *bt.Tx, feeQuote *bt.FeeQuote) error`: Verifies if the transaction fee is sufficient.
- `sigOpsCheck(tx *bt.Tx, policy *PolicySettings) error`: Checks the number of signature operations in the transaction.
- `pushDataCheck(tx *bt.Tx) error`: Ensures that unlocking scripts only push data onto the stack.

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
