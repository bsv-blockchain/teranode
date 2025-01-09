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
func (u *Server) Start(ctx context.Context) error
```

Starts the server, including Kafka consumers and gRPC server.

### Stop

```go
func (u *Server) Stop(_ context.Context) error
```

Stops the server (currently a no-op).

### CheckSubtree

```go
func (u *Server) CheckSubtree(ctx context.Context, request *subtreevalidation_api.CheckSubtreeRequest) (*subtreevalidation_api.CheckSubtreeResponse, error)
```

Checks the validity of a subtree based on the provided request.

## Internal Methods

### checkSubtree

```go
func (u *Server) checkSubtree(ctx context.Context, request *subtreevalidation_api.CheckSubtreeRequest) (ok bool, err error)
```

Internal implementation of subtree checking logic.

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

### subtreeHandler

```go
func (u *Server) subtreeHandler(msg *kafka.KafkaMessage) error
```

Handles incoming subtree messages from Kafka.

### txmetaHandler

```go
func (u *Server) txmetaHandler(msg *kafka.KafkaMessage) error
```

Handles incoming transaction metadata messages from Kafka.
