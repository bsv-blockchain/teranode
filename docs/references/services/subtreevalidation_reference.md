# Subtree Validation Reference Documentation

## Overview

The `Server` type implements the core functionality for subtree validation in a blockchain system. It handles subtree and transaction metadata processing, interacts with various data stores, and manages Kafka consumers for distributed processing.

## Type Definition

```go
type Server struct {
    subtreevalidation_api.UnimplementedSubtreeValidationAPIServer
    logger ulogger.Logger
    settings *settings.Settings
    subtreeStore blob.Store
    txStore blob.Store
    utxoStore utxo.Store
    validatorClient validator.Interface
    subtreeCount atomic.Int32
    stats *gocore.Stat
    prioritySubtreeCheckActiveMap map[string]bool
    prioritySubtreeCheckActiveMapLock sync.Mutex
    blockchainClient blockchain.ClientI
    subtreeConsumerClient kafka.KafkaConsumerGroupI
    txmetaConsumerClient kafka.KafkaConsumerGroupI
}
```

## Constructor

### New

```go
func New(ctx context.Context, logger ulogger.Logger, tSettings *settings.Settings,
subtreeStore blob.Store, txStore blob.Store, utxoStore utxo.Store,
validatorClient validator.Interface, blockchainClient blockchain.ClientI,
subtreeConsumerClient kafka.KafkaConsumerGroupI,
txmetaConsumerClient kafka.KafkaConsumerGroupI) (*Server, error)
```

Creates a new `Server` instance with the provided dependencies.

## Methods

### Health

```go
func (u *Server) Health(ctx context.Context, checkLiveness bool) (int, string, error)
```

Performs health checks on the server and its dependencies.

### HealthGRPC

```go
func (u *Server) HealthGRPC(ctx context.Context, _ *subtreevalidation_api.EmptyMessage) (*subtreevalidation_api.HealthResponse, error)
```

Implements the gRPC health check endpoint.

### Init

```go
func (u *Server) Init(ctx context.Context) (err error)
```

Initializes the server, setting up Kafka consumers and other necessary components.

### Start

```go
func (u *Server) Start(ctx context.Context, readyCh chan<- struct{}) error
```

Starts the server, including Kafka consumers and gRPC server. It blocks until the context is canceled or an error occurs, and signals readiness by closing the readyCh channel once initialization is complete.

### Stop

```go
func (u *Server) Stop(_ context.Context) error
```

Gracefully shuts down the server components including Kafka consumers.

### CheckSubtreeFromBlock

```go
func (u *Server) CheckSubtreeFromBlock(ctx context.Context, request *subtreevalidation_api.CheckSubtreeFromBlockRequest) (*subtreevalidation_api.CheckSubtreeFromBlockResponse, error)
```

Validates a subtree and its transactions based on the provided request. It handles both legacy and current validation paths, managing locks to prevent duplicate processing. The function implements a retry mechanism for lock acquisition and supports both legacy and current validation paths.

## Internal Methods

### checkSubtreeFromBlock

```go
func (u *Server) checkSubtreeFromBlock(ctx context.Context, request *subtreevalidation_api.CheckSubtreeFromBlockRequest) (ok bool, err error)
```

Internal implementation of subtree checking logic. This function expects a subtree to have been stored in the subtree store with an extension of .subtreeToCheck compared to the normal .subtree extension, which is done to ensure the subtree validation does not think the subtree has already been checked.

### GetUutxoStore

```go
func (u *Server) GetUutxoStore() utxo.Store
```

Returns the UTXO store instance used by the server.

### SetUutxoStore

```go
func (u *Server) SetUutxoStore(s utxo.Store)
```

Sets the UTXO store instance for the server.

### ValidateSubtreeInternal

```go
func (u *Server) ValidateSubtreeInternal(ctx context.Context, v ValidateSubtree, blockHeight uint32) (err error)
```

Performs internal validation of a subtree.

### getSubtreeTxHashes

```go
func (u *Server) getSubtreeTxHashes(spanCtx context.Context, stat *gocore.Stat, subtreeHash *chainhash.Hash, baseURL string) ([]chainhash.Hash, error)
```

Retrieves transaction hashes for a given subtree.

### processMissingTransactions

```go
func (u *Server) processMissingTransactions(ctx context.Context, subtreeHash *chainhash.Hash, missingTxHashes []utxo.UnresolvedMetaData, baseURL string, txMetaSlice []*meta.Data, blockHeight uint32) (err error)
```

Processes missing transactions for a subtree.

### prepareTxsPerLevel

```go
func (u *Server) prepareTxsPerLevel(ctx context.Context, transactions []missingTx) (uint32, map[uint32][]missingTx)
```

Prepares transactions per level for processing.

### getMissingTransactionsFromFile

```go
func (u *Server) getMissingTransactionsFromFile(ctx context.Context, subtreeHash *chainhash.Hash, missingTxHashes []utxo.UnresolvedMetaData) (missingTxs []missingTx, err error)
```

Retrieves missing transactions from a file.

### getMissingTransactions

```go
func (u *Server) getMissingTransactions(ctx context.Context, subtreeHash *chainhash.Hash, missingTxHashes []utxo.UnresolvedMetaData, baseUrl string) (missingTxs []missingTx, err error)
```

Retrieves missing transactions from the network.

### isPrioritySubtreeCheckActive

```go
func (u *Server) isPrioritySubtreeCheckActive(subtreeHash string) bool
```

Checks if a priority subtree check is active for the given subtree hash.

## Kafka Handlers

### consumerMessageHandler

```go
func (u *Server) consumerMessageHandler(ctx context.Context) func(msg *kafka.KafkaMessage) error
```

Returns a function that processes Kafka messages for subtree validation. It handles both recoverable and unrecoverable errors appropriately. This is used for the subtree consumer.

### subtreesHandler

```go
func (u *Server) subtreesHandler(msg *kafka.KafkaMessage) error
```

The actual implementation that handles incoming subtree messages from Kafka.

### txmetaHandler

```go
func (u *Server) txmetaHandler(msg *kafka.KafkaMessage) error
```

Handles incoming transaction metadata messages from Kafka.
