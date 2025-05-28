# Subtree Validation Reference Documentation

## Overview

The `Server` type implements the core functionality for subtree validation in a blockchain system. It handles subtree and transaction metadata processing, interacts with various data stores, and manages Kafka consumers for distributed processing. The service is a critical component in validating transaction subtrees for inclusion in the blockchain.

## Types

### Server

The `Server` is the central component of the subtreevalidation package, managing the complete lifecycle of subtree validation including transaction validation, storage, and integration with other Teranode services.

```go
type Server struct {
    // UnimplementedSubtreeValidationAPIServer embeds the auto-generated gRPC server base
    subtreevalidation_api.UnimplementedSubtreeValidationAPIServer

    // logger handles all logging operations for the service
    logger ulogger.Logger

    // settings contains the configuration parameters for the service
    // including connection details, timeouts, and operational modes
    settings *settings.Settings

    // subtreeStore manages persistent storage of subtrees
    // This blob store is used to save and retrieve complete subtree structures
    subtreeStore blob.Store

    // txStore manages transaction storage
    // This blob store is used to save and retrieve individual transactions
    txStore blob.Store

    // utxoStore manages the Unspent Transaction Output (UTXO) state
    // It's used during transaction validation to verify input spending
    utxoStore utxo.Store

    // validatorClient provides transaction validation services
    // It's used to validate transactions against consensus rules
    validatorClient validator.Interface

    // subtreeCount tracks the number of subtrees processed
    // Uses atomic operations for thread-safe access
    subtreeCount atomic.Int32

    // stats tracks operational statistics for monitoring and diagnostics
    stats *gocore.Stat

    // prioritySubtreeCheckActiveMap tracks active priority subtree checks
    // Maps subtree hash strings to boolean values indicating check status
    prioritySubtreeCheckActiveMap map[string]bool

    // prioritySubtreeCheckActiveMapLock protects concurrent access to the priority map
    prioritySubtreeCheckActiveMapLock sync.Mutex

    // blockchainClient interfaces with the blockchain service
    // Used to retrieve block information and validate chain state
    blockchainClient blockchain.ClientI

    // subtreeConsumerClient consumes subtree-related Kafka messages
    // Handles incoming subtree validation requests from other services
    subtreeConsumerClient kafka.KafkaConsumerGroupI

    // txmetaConsumerClient consumes transaction metadata Kafka messages
    // Processes transaction metadata updates from other services
    txmetaConsumerClient kafka.KafkaConsumerGroupI
}
```

### ValidateSubtree

The `ValidateSubtree` structure encapsulates all the necessary information required to validate a transaction subtree, providing a clean interface for the validation methods.

```go
type ValidateSubtree struct {
    // SubtreeHash is the hash identifier of the subtree to validate
    SubtreeHash chainhash.Hash

    // BaseURL is the source location for retrieving missing transactions
    BaseURL string

    // TxHashes contains the hashes of all transactions in the subtree
    TxHashes []chainhash.Hash

    // AllowFailFast determines whether validation should stop on first error
    // When true, validation will terminate immediately upon encountering an invalid transaction
    // When false, validation will attempt to validate all transactions before returning
    AllowFailFast bool
}
```

### missingTx

This structure pairs a transaction with its index in the original subtree transaction list, allowing the validation process to maintain the correct ordering and relationship of transactions.

```go
type missingTx struct {
    // tx is the actual transaction data that was retrieved
    tx *bt.Tx

    // idx is the original position of this transaction in the subtree's transaction list
    idx int
}
```

## Constructor

### New

```go
func New(
    ctx context.Context,
    logger ulogger.Logger,
    tSettings *settings.Settings,
    subtreeStore blob.Store,
    txStore blob.Store,
    utxoStore utxo.Store,
    validatorClient validator.Interface,
    blockchainClient blockchain.ClientI,
    subtreeConsumerClient kafka.KafkaConsumerGroupI,
    txmetaConsumerClient kafka.KafkaConsumerGroupI,
) (*Server, error)
```

Creates a new `Server` instance with the provided dependencies. This factory function constructs and initializes a fully configured subtree validation service, injecting all required dependencies. It follows the dependency injection pattern to ensure testability and proper separation of concerns.

The method ensures that the service is configured with proper stores, clients, and settings before it's made available for use. It also initializes internal tracking structures and statistics for monitoring.

## Core Methods

### Health

```go
func (u *Server) Health(ctx context.Context, checkLiveness bool) (int, string, error)
```

Checks the health status of the service and its dependencies. This method implements the standard Teranode health check interface used across all services for consistent monitoring, alerting, and orchestration. It provides both readiness and liveness checking capabilities to support different operational scenarios.

The method performs checks appropriate to the service's role, including:
- Verifying store access for subtree, transaction, and UTXO data
- Checking connections to dependent services (validator, blockchain)
- Validating Kafka consumer health
- Ensuring internal state consistency

### HealthGRPC

```go
func (u *Server) HealthGRPC(ctx context.Context, _ *subtreevalidation_api.EmptyMessage) (*subtreevalidation_api.HealthResponse, error)
```

Implements the gRPC health check endpoint, translating the core health check results to the gRPC protocol format.

### Init

```go
func (u *Server) Init(ctx context.Context) (err error)
```

Initializes the server metrics and performs any necessary setup. This method completes the initialization process by setting up components that require runtime initialization rather than construction-time setup. It's called after New() but before Start() to ensure all systems are properly initialized.

The initialization is designed to be idempotent and can be safely called multiple times, though typically it's only called once after construction and before starting the service.

### Start

```go
func (u *Server) Start(ctx context.Context, readyCh chan<- struct{}) error
```

Initializes and starts the server components including Kafka consumers and gRPC server. This method launches all the operational components of the subtree validation service, including:
- Kafka consumers for subtree and transaction metadata messages
- The gRPC server for API access
- Any background workers or timers required for operation

The method implements a safe startup sequence to ensure all components are properly initialized before the service is marked as ready. It also handles proper error propagation if any component fails to start.

Once all components are successfully started, the method signals readiness through the provided channel and then blocks until the context is canceled or an error occurs. This design allows the caller to coordinate the startup of multiple services.

### Stop

```go
func (u *Server) Stop(_ context.Context) error
```

Gracefully shuts down the server components including Kafka consumers. This method ensures a clean and orderly shutdown of all service components, allowing in-progress operations to complete when possible and releasing all resources properly. It follows a consistent shutdown sequence that:
1. Stops accepting new requests
2. Pauses Kafka consumers to prevent new messages from being processed
3. Waits for in-progress operations to complete (with reasonable timeouts)
4. Closes connections and releases resources

The method is designed to be called when the service needs to be terminated, either for normal shutdown or in response to system signals.

### CheckSubtreeFromBlock

```go
func (u *Server) CheckSubtreeFromBlock(ctx context.Context, request *subtreevalidation_api.CheckSubtreeFromBlockRequest) (*subtreevalidation_api.CheckSubtreeFromBlockResponse, error)
```

Validates a subtree and its transactions based on the provided request. This method is the primary gRPC API endpoint for subtree validation, responsible for coordinating the validation process for an entire subtree of interdependent transactions. It ensures that all transactions in the subtree adhere to consensus rules and can be added to the blockchain.

The method implements several important features:
- Distributed locking to prevent duplicate validation of the same subtree
- Retry logic for lock acquisition with exponential backoff
- Support for both legacy and current validation paths for backward compatibility
- Proper resource cleanup even in error conditions
- Structured error responses with appropriate gRPC status codes

Validation includes checking that:
- All transactions in the subtree are valid according to consensus rules
- All transaction inputs refer to unspent outputs or other transactions in the subtree
- No double-spending conflicts exist within the subtree or with existing chain state
- Transactions satisfy all policy rules (fees, standardness, etc.)

The method will retry lock acquisition for up to 20 seconds with exponential backoff, making it resilient to temporary contention when multiple services attempt to validate the same subtree simultaneously.

## Transaction Metadata Management

### GetUutxoStore

```go
func (u *Server) GetUutxoStore() utxo.Store
```

Returns the UTXO store instance used by the server. This method provides access to the store that manages unspent transaction outputs.

### SetUutxoStore

```go
func (u *Server) SetUutxoStore(s utxo.Store)
```

Sets the UTXO store instance for the server. This method allows runtime replacement of the UTXO store, which can be useful for testing or for implementing different storage strategies.

### SetTxMetaCache

```go
func (u *Server) SetTxMetaCache(ctx context.Context, hash *chainhash.Hash, txMeta *meta.Data) error
```

Stores transaction metadata in the cache if caching is enabled. This method optimizes validation performance by caching frequently accessed transaction metadata.

### SetTxMetaCacheFromBytes

```go
func (u *Server) SetTxMetaCacheFromBytes(_ context.Context, key, txMetaBytes []byte) error
```

Stores raw transaction metadata bytes in the cache. This is a lower-level method that can be more efficient when the raw bytes are already available.

### DelTxMetaCache

```go
func (u *Server) DelTxMetaCache(ctx context.Context, hash *chainhash.Hash) error
```

Removes transaction metadata from the cache if caching is enabled. This ensures cache consistency when transactions are modified or invalidated.

### DelTxMetaCacheMulti

```go
func (u *Server) DelTxMetaCacheMulti(ctx context.Context, hash *chainhash.Hash) error
```

Removes multiple transaction metadata entries from the cache. This method is optimized for batch operations when multiple related entries need to be invalidated.

## Internal Validation Methods

### checkSubtreeFromBlock

```go
func (u *Server) checkSubtreeFromBlock(ctx context.Context, request *subtreevalidation_api.CheckSubtreeFromBlockRequest) (ok bool, err error)
```

Internal implementation of subtree validation logic. This method contains the core business logic for validating a subtree, separated from the API-level concerns handled by the public CheckSubtreeFromBlock method. The separation allows for cleaner testing and better separation of concerns.

The method expects the subtree to be stored in the subtree store with a special extension (.subtreeToCheck instead of .subtree) to differentiate between validated and unvalidated subtrees. This prevents the validation service from mistakenly treating an unvalidated subtree as already validated.

The validation process includes:
1. Retrieving the subtree data from storage
2. Parsing the subtree structure
3. Checking for existing transaction metadata
4. Retrieving and validating missing transactions
5. Verifying transaction dependencies and ordering
6. Confirming all transactions meet consensus rules

### ValidateSubtreeInternal

```go
func (u *Server) ValidateSubtreeInternal(ctx context.Context, v ValidateSubtree, blockHeight uint32,
    blockIds map[uint32]bool, validationOptions ...validator.Option) (err error)
```

Performs the actual validation of a subtree. This is the core method of the subtree validation service, responsible for the complete validation process of a transaction subtree. It handles the complex task of verifying that all transactions in a subtree are valid both individually and collectively, ensuring they can be safely added to the blockchain.

The validation process includes several key steps:
1. Retrieving the subtree structure and transaction list
2. Identifying which transactions need validation (missing metadata)
3. Retrieving missing transactions from appropriate sources
4. Validating transaction dependencies and ordering
5. Applying consensus rules to each transaction
6. Managing transaction metadata storage and updates
7. Handling any conflicts or validation failures

The method employs several optimization techniques:
- Batch processing of transaction validations where possible
- Caching of transaction metadata to avoid redundant validation
- Parallel processing of independent transaction validations
- Early termination for invalid subtrees (when AllowFailFast is true)
- Efficient retrieval of missing transactions in batches

### blessMissingTransaction

```go
func (u *Server) blessMissingTransaction(ctx context.Context, subtreeHash chainhash.Hash, tx *bt.Tx, blockHeight uint32,
    blockIds map[uint32]bool, validationOptions *validator.Options) (txMeta *meta.Data, err error)
```

Validates a transaction and retrieves its metadata, performing the core consensus validation operations required for blockchain inclusion. This method applies full validation to a transaction, ensuring it adheres to all Bitcoin consensus rules and can be properly included in the blockchain.

The validation includes:
- Transaction format and structure validation
- Input signature verification
- Input UTXO availability and spending authorization
- Fee calculation and policy enforcement
- Script execution and validation
- Double-spend prevention

Upon successful validation, the transaction's metadata is calculated and stored, making it available for future reference and for validation of dependent transactions.

### checkCounterConflictingOnCurrentChain

```go
func (u *Server) checkCounterConflictingOnCurrentChain(ctx context.Context, txHash chainhash.Hash, blockIds map[uint32]bool) error
```

Checks if the counter-conflicting transactions of a given transaction have already been mined on the current chain. If they have, it returns an error indicating that the transaction is invalid.

## Transaction Retrieval Methods

### getSubtreeTxHashes

```go
func (u *Server) getSubtreeTxHashes(spanCtx context.Context, stat *gocore.Stat, subtreeHash *chainhash.Hash, baseURL string) ([]chainhash.Hash, error)
```

Retrieves transaction hashes for a subtree from a remote source. This method fetches the list of transactions that are part of a given subtree from a network peer or another service.

### processMissingTransactions

```go
func (u *Server) processMissingTransactions(ctx context.Context, subtreeHash chainhash.Hash, subtree *util.Subtree,
    missingTxHashes []utxo.UnresolvedMetaData, allTxs []chainhash.Hash, baseURL string, txMetaSlice []*meta.Data, blockHeight uint32,
    blockIds map[uint32]bool, validationOptions ...validator.Option) (err error)
```

Handles the retrieval and validation of missing transactions in a subtree, coordinating both the retrieval process and the validation workflow. This method is a critical part of the subtree validation process, responsible for:
1. Retrieving transactions that are referenced in the subtree but not available locally
2. Organizing transactions into dependency levels for ordered processing
3. Validating each transaction according to consensus rules
4. Managing parallel processing of independent transaction validations
5. Tracking validation results and updating transaction metadata

The method supports both file-based and network-based transaction retrieval, with fallback mechanisms to ensure maximum resilience. It implements a level-based processing approach where transactions are grouped by dependency level and processed in order, ensuring that parent transactions are validated before their children.

### prepareTxsPerLevel

```go
func (u *Server) prepareTxsPerLevel(ctx context.Context, transactions []missingTx) (uint32, map[uint32][]missingTx)
```

Organizes transactions by their dependency level for ordered processing. This method implements a topological sorting algorithm to organize transactions based on their dependency relationships. Transactions are grouped into levels, where each level contains transactions that can be processed independently of each other, but depend on transactions from previous levels.

### getSubtreeMissingTxs

```go
func (u *Server) getSubtreeMissingTxs(ctx context.Context, subtreeHash chainhash.Hash, subtree *util.Subtree,
    missingTxHashes []utxo.UnresolvedMetaData, allTxs []chainhash.Hash, baseURL string) ([]missingTx, error)
```

Retrieves transactions that are referenced in a subtree but not available locally. This method implements an intelligent retrieval strategy for missing transactions with optimizations for different scenarios. It first checks if a complete subtree data file exists locally, which would contain all transactions. If not available, it makes a decision based on the percentage of missing transactions:

- If a large percentage of transactions are missing (configurable threshold), it attempts to fetch the entire subtree data file from the peer to optimize network usage.
- Otherwise, it retrieves only the specific missing transactions individually.

The method employs fallback mechanisms to ensure maximum resilience, switching between file-based and network-based retrieval methods as needed.

### getMissingTransactionsFromFile

```go
func (u *Server) getMissingTransactionsFromFile(ctx context.Context, subtreeHash chainhash.Hash, missingTxHashes []utxo.UnresolvedMetaData,
    allTxs []chainhash.Hash) (missingTxs []missingTx, err error)
```

Retrieves missing transactions from a file. This method attempts to read transaction data from a locally stored subtree file, which can be more efficient than retrieving individual transactions from the network.

### getMissingTransactionsFromPeer

```go
func (u *Server) getMissingTransactionsFromPeer(ctx context.Context, subtreeHash chainhash.Hash, missingTxHashes []utxo.UnresolvedMetaData,
    baseURL string) (missingTxs []missingTx, err error)
```

Retrieves missing transactions from a peer node. This method handles the network communication required to fetch transactions that are not available locally, organizing the retrieval into batches for efficiency.

### getMissingTransactionsBatch

```go
func (u *Server) getMissingTransactionsBatch(ctx context.Context, subtreeHash chainhash.Hash, txHashes []utxo.UnresolvedMetaData, baseURL string) ([]*bt.Tx, error)
```

Retrieves a batch of transactions from the network. This method optimizes network utilization by fetching multiple transactions in a single request, reducing the overhead of multiple separate requests.

### isPrioritySubtreeCheckActive

```go
func (u *Server) isPrioritySubtreeCheckActive(subtreeHash string) bool
```

Checks if a priority subtree check is active for the given subtree hash. Priority checks get special handling and resource allocation to ensure critical subtrees are validated promptly.

## Kafka Handlers

### consumerMessageHandler

```go
func (u *Server) consumerMessageHandler(ctx context.Context) func(msg *kafka.KafkaMessage) error
```

Returns a function that processes Kafka messages for subtree validation. It handles both recoverable and unrecoverable errors appropriately. The handler includes sophisticated error categorization to determine whether errors should result in message reprocessing or rejection.

Key features include:
- Different handling for recoverable vs. non-recoverable errors
- State-aware processing that considers the current blockchain state
- Proper context cancellation handling
- Idempotent processing to prevent duplicate validation

### subtreesHandler

```go
func (u *Server) subtreesHandler(msg *kafka.KafkaMessage) error
```

Handles incoming subtree messages from Kafka. This method unmarshals the message, extracts the subtree hash and base URL, acquires the appropriate lock, and triggers the validation process. It includes comprehensive error handling and logging for operational visibility.

### txmetaHandler

```go
func (u *Server) txmetaHandler(msg *kafka.KafkaMessage) error
```

Handles incoming transaction metadata messages from Kafka. This method processes updates to transaction metadata that might be required for proper subtree validation, ensuring the metadata store remains consistent with the latest transaction state.
