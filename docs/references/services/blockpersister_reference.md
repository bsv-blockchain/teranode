# Block Persister Service Reference Documentation

## Overview

The Block Persister Service is responsible for taking blocks from the blockchain service and ensuring they are properly stored in persistent storage along with all related data (transactions, UTXOs, etc.). It plays a critical role in the overall blockchain data persistence strategy by:

- Processing and storing complete blocks in the blob store
- Managing subtree processing for efficient transaction handling
- Maintaining UTXO set differences for each block
- Ensuring data consistency and integrity during persistence operations
- Providing resilient error handling and recovery mechanisms

The service integrates with multiple stores (block store, subtree store, UTXO store) and coordinates between them to ensure consistent and reliable block data persistence. It employs concurrency and batching techniques to optimize performance for high transaction volumes.

## Types

### Server

```go
type Server struct {
    // ctx is the context for controlling server lifecycle and handling cancellation signals
    ctx context.Context

    // logger provides structured logging functionality for operational monitoring and debugging
    logger ulogger.Logger

    // settings contains configuration settings for the server, controlling behavior such as
    // concurrency levels, batch sizes, and persistence strategies
    settings *settings.Settings

    // blockStore provides persistent storage for complete blocks
    // This is typically implemented as a blob store capable of handling large block data
    blockStore blob.Store

    // subtreeStore provides storage for block subtrees, which are hierarchical structures
    // containing transaction references that make up parts of a block
    subtreeStore blob.Store

    // utxoStore provides storage for UTXO (Unspent Transaction Output) data
    // Used to track the current state of the UTXO set and process changes
    utxoStore utxo.Store

    // stats tracks operational statistics for monitoring and performance analysis
    stats *gocore.Stat

    // blockchainClient interfaces with the blockchain service to retrieve block data
    // and coordinate persistence operations with blockchain state
    blockchainClient blockchain.ClientI
}
```

The `Server` type is the main structure for the Block Persister Service. It contains components for managing stores and blockchain interactions.

## Functions

### Server Management

#### New

```go
func New(
    ctx context.Context,
    logger ulogger.Logger,
    tSettings *settings.Settings,
    blockStore blob.Store,
    subtreeStore blob.Store,
    utxoStore utxo.Store,
    blockchainClient blockchain.ClientI,
    opts ...func(*Server),
) *Server
```

Creates a new instance of the `Server` with the provided dependencies.

This constructor initializes all components required for block persistence operations, including stores and client connections. It accepts optional configuration functions to customize the server instance after construction.

Parameters:

- ctx: Context for controlling the server lifecycle
- logger: Logger for recording operational events and errors
- tSettings: Configuration settings that control server behavior
- blockStore: Storage interface for blocks
- subtreeStore: Storage interface for block subtrees
- utxoStore: Storage interface for UTXO data
- blockchainClient: Client for interacting with the blockchain service
- opts: Optional configuration functions to apply after construction

Returns a fully constructed and configured Server instance ready for initialization.

#### Health

```go
func (u *Server) Health(ctx context.Context, checkLiveness bool) (int, string, error)
```

Performs health checks on the server and its dependencies. This method implements the health.Check interface and is used by monitoring systems to determine the operational status of the service.

The health check distinguishes between liveness (is the service running?) and readiness (is the service able to handle requests?) checks:

- Liveness checks verify the service process is running and responsive
- Readiness checks verify all dependencies are available and functioning

Parameters:

- ctx: Context for coordinating cancellation or timeouts
- checkLiveness: When true, only liveness checks are performed; when false, both liveness and readiness checks are performed

Returns:

- int: HTTP status code (200 for healthy, 503 for unhealthy)
- string: Human-readable status message
- error: Any error encountered during health checking

Dependency checks include:

- Blockchain client and FSM status
- Block store availability
- Subtree store status
- UTXO store health

#### Init

```go
func (u *Server) Init(ctx context.Context) (err error)
```

Initializes the server, setting up any required resources.

This method is called after construction but before the server starts processing blocks. It performs one-time initialization tasks such as setting up Prometheus metrics.

Parameters:

- ctx: Context for coordinating initialization operations

Returns an error if initialization fails, nil otherwise.

#### Start

```go
func (u *Server) Start(ctx context.Context, readyCh chan<- struct{}) error
```

Initializes and begins the block persister service operations.

This method starts the main processing loop and sets up HTTP services if configured. It waits for the blockchain FSM to transition from IDLE state before beginning block persistence operations to ensure the blockchain is ready.

The method implements the following key operations:

- Waits for blockchain service readiness
- Sets up HTTP blob server if required by configuration
- Starts the main processing loop in a background goroutine
- Signals service readiness through the provided channel

Parameters:

- ctx: Context for controlling the service lifecycle and handling cancellation
- readyCh: Channel used to signal when the service is ready to accept requests

Returns an error if the service fails to start properly, nil otherwise.

#### Stop

```go
func (u *Server) Stop(_ context.Context) error
```

Gracefully shuts down the server.

This method is called when the service is being stopped and provides an opportunity to perform any necessary cleanup operations, such as closing connections, flushing buffers, or persisting state.

Currently, the Server doesn't need to perform any specific cleanup actions during shutdown as resource cleanup is handled by the context cancellation mechanism in the Start method.

Parameters:

- ctx: Context for controlling the shutdown operation (currently unused)

Returns an error if shutdown fails, or nil on successful shutdown.

### Internal Methods

#### getNextBlockToProcess

```go
func (u *Server) getNextBlockToProcess(ctx context.Context) (*model.Block, error)
```

Retrieves the next block that needs to be processed based on the current state and configuration. This method determines the next block to persist by comparing the last persisted block height with the current blockchain tip. It ensures blocks are persisted in sequence without gaps and respects the configured persistence age policy to control how far behind persistence can lag.

#### persistBlock

```go
func (u *Server) persistBlock(ctx context.Context, hash *chainhash.Hash, blockBytes []byte) error
```

Stores a block and its associated data to persistent storage.

This is a core function of the blockpersister service that handles the complete persistence workflow for a single block. It ensures all components of a block (header, transactions, and UTXO changes) are properly stored in a consistent and recoverable manner.

!!! abstract "Processing Steps"
    The function implements a multi-stage persistence process:

    1. **Convert raw block bytes** into a structured block model
    2. **Create a new UTXO difference set** for tracking changes
    3. **Process the coinbase transaction** if no subtrees are present
    4. **For blocks with subtrees**, process each subtree concurrently according to configured limits
    5. **Close and finalize** the UTXO difference set once all transactions are processed
    6. **Write the complete block** to persistent storage

**Parameters:**

- `ctx`: Context for the operation, used for cancellation and tracing
- `hash`: Hash identifier of the block to persist
- `blockBytes`: Raw serialized bytes of the complete block

**Returns** an error if any part of the persistence process fails. The error will be wrapped with appropriate context to identify the specific failure point.

!!! note "Concurrency Management"
    Concurrency is managed through errgroup with configurable parallel processing limits to optimize performance while avoiding resource exhaustion.

!!! warning "Atomicity"
    Block persistence is atomic - if any part fails, the entire operation is considered failed and should be retried after resolving the underlying issue.

#### getNextBlockToProcess

```go
func (u *Server) getNextBlockToProcess(ctx context.Context) (*model.Block, error)
```

Retrieves the next block that needs to be persisted to blob storage.

This method queries the database for blocks that haven't been persisted yet (persisted_at IS NULL) and aren't marked as invalid. The database stores block metadata and tracks persistence status, eliminating the need for external state files.

!!! info "Processing Logic"
    The method follows these steps:

    1. **Query database** for blocks where `persisted_at IS NULL AND invalid = false`
    2. **Retrieve one block** (limit=1) in ascending height order
    3. **Return the block** if found, or nil if no blocks need processing

**Parameters:**

- `ctx`: Context for coordinating the block retrieval operation

**Returns:**

- `*model.Block`: The next block to process, or nil if no block needs processing yet
- `error`: Any error encountered during the operation

#### readSubtree

```go
func (u *Server) readSubtree(ctx context.Context, subtreeHash chainhash.Hash) (*subtreepkg.Subtree, error)
```

Retrieves a subtree from the subtree store and deserializes it.

This function is responsible for loading a subtree structure from persistent storage, which contains the hierarchical organization of transactions within a block. It retrieves the subtree file using the provided hash and deserializes it into a usable subtree object.

!!! abstract "Processing Steps"
    The process includes:

    1. **Attempting to read the subtree** from the store using the provided hash
    2. **If the primary read fails**, it attempts to read from a secondary location (FileTypeSubtreeToCheck)
    3. **Deserializing the retrieved subtree data** into a subtree object

**Parameters:**

- `ctx`: Context for the operation, enabling cancellation and timeout handling
- `subtreeHash`: Hash identifier of the subtree to retrieve and deserialize

**Returns:**

- `*subtreepkg.Subtree`: The deserialized subtree object ready for further processing
- `error`: Any error encountered during retrieval or deserialization

#### readSubtreeData

```go
func (u *Server) readSubtreeData(ctx context.Context, subtreeHash chainhash.Hash) (*subtreepkg.SubtreeData, error)
```

Retrieves and deserializes subtree data from the subtree store.

This internal method handles the two-stage process of loading subtree information: first retrieving the subtree structure itself, then loading the associated subtree data that contains the actual transaction references and metadata.

!!! abstract "Processing Steps"
    The function performs these operations:

    1. **Retrieves the subtree structure** from the subtree store using the provided hash
    2. **Deserializes the subtree** to understand its structure and transaction organization
    3. **Retrieves the corresponding subtree data file** containing transaction references
    4. **Deserializes the subtree data** into a usable format for transaction processing

**Parameters:**

- `ctx`: Context for the operation, enabling cancellation and timeout handling
- `subtreeHash`: Hash identifier of the subtree to retrieve and deserialize

**Returns:**

- `*subtreepkg.SubtreeData`: The deserialized subtree data ready for transaction processing
- `error`: Any error encountered during retrieval or deserialization

### Subtree Processing

Block persistence uses a **two-phase approach** to process subtrees efficiently while maintaining data integrity:

#### Phase 1: CreateSubtreeDataFileStreaming

```go
func (u *Server) CreateSubtreeDataFileStreaming(ctx context.Context, subtreeHash chainhash.Hash, block *model.Block, subtreeIndex int) error
```

Creates subtree data files using streaming writes. This phase runs **in parallel** across all subtrees with configurable concurrency.

!!! note "Processing Steps"
    1. **Check if subtree data already exists** - if it does, just set DAH and skip processing
    2. **Retrieve the subtree** from the subtree store using its hash
    3. **Load transaction metadata** from the UTXO store (batched or individual)
    4. **Stream write** the subtree data file using `SubtreeDataWriter`
    5. **Abort on error** - incomplete files are automatically cleaned up

#### Phase 2: ProcessSubtreeUTXOStreaming

```go
func (u *Server) ProcessSubtreeUTXOStreaming(ctx context.Context, subtreeHash chainhash.Hash, utxoDiff *utxopersister.UTXOSet) error
```

Processes UTXO changes by reading from the subtree data files. This phase runs **sequentially** to maintain UTXO ordering.

!!! note "Processing Steps"
    1. **Open subtree data file** for streaming read
    2. **Process each transaction** through the UTXO diff tracker
    3. **Record additions and deletions** to the UTXO set

#### SubtreeDataWriter

```go
type SubtreeDataWriter struct {
    storer      *filestorer.FileStorer
    nextBatchID int
    // ... additional fields
}
```

The `SubtreeDataWriter` provides **ordered batch writes** for subtree data. It ensures batches are written in the correct order even when produced concurrently.

**Key Methods:**

- `WriteBatch(batchID int, txData [][]byte) error`: Writes a batch of transaction data at the specified position
- `Close() error`: Finalizes the file (aborts if batches are pending)
- `Abort(err error)`: Aborts the write operation without finalizing

!!! tip "Error Safety"
    If `Close()` is called while batches are still pending (gaps in batch sequence), the writer automatically aborts instead of finalizing an incomplete file.

#### Helper Functions

##### readSubtree

```go
func (u *Server) readSubtree(ctx context.Context, subtreeHash chainhash.Hash) (*subtreepkg.Subtree, error)
```

Reads a subtree from the subtree store by its hash. This method attempts to retrieve the subtree from storage, trying both regular subtree files and subtrees marked for checking if the primary lookup fails.

##### processTxMetaUsingStore

```go
func (u *Server) processTxMetaUsingStore(ctx context.Context, subtree *subtreepkg.Subtree, subtreeData *subtreepkg.SubtreeData) error
```

Processes transaction metadata using the UTXO store. This method handles the retrieval and processing of transaction metadata for all transactions in a subtree, with support for both batched and individual transaction processing modes.

#### Legacy: WriteTxs

```go
func WriteTxs(_ context.Context, logger ulogger.Logger, writer *filestorer.FileStorer, txs []*bt.Tx, utxoDiff *utxopersister.UTXOSet) error
```

Writes a series of transactions to storage and processes their UTXO changes.

This function handles the final persistence of transaction data to storage and optionally processes UTXO set changes. It's a critical component in the block persistence pipeline that ensures transactions are properly serialized and stored.

!!! abstract "Processing Steps"
    The function performs the following steps:

    1. **For each transaction** in the provided slice:

        - Check for nil transactions and log errors if found
        - Write the raw transaction bytes to storage (using normal bytes, not extended)
        - If a UTXO diff is provided, process the transaction's UTXO changes
    2. **Report any errors** or validation issues encountered

The function includes safety checks to handle nil transactions, logging errors but continuing processing when possible to maximize resilience.

**Parameters:**

- `_`: Context parameter (currently unused in implementation)
- `logger`: Logger for recording operations, errors, and warnings
- `writer`: FileStorer destination for writing serialized transaction data
- `txs`: Slice of transaction objects to write
- `utxoDiff`: UTXO set difference tracker (optional, can be nil if UTXO tracking not needed)

**Returns** an error if writing fails at any point. Specific error conditions include:

- **Failure to write individual transaction data**
- **Errors during UTXO processing** for transactions

!!! warning "Atomicity Consideration"
    The operation is not fully atomic - some transactions may be written successfully even if others fail. The caller should handle partial success scenarios appropriately.

## Configuration

The service uses settings from the `settings.Settings` structure, primarily focused on the Block section. These settings control various aspects of block persistence behavior, from storage locations to processing strategies.

### Block Settings

#### Storage Configuration

- **`Block.BlockStore`**: Block store URL. Defines the location of the blob store used for block data.

#### Network Configuration

- **`Block.PersisterHTTPListenAddress`**: HTTP listener address for the blob server if enabled. Format should be "host:port".

#### Processing Configuration

- **`Block.BlockPersisterPersistSleep`**: Sleep duration between processing attempts when no blocks are available to process. Specified in milliseconds.
- **`Block.BatchMissingTransactions`**: When true, enables batched retrieval of transaction metadata, which improves performance for high transaction volumes by reducing individual store requests.

### Interaction with Other Components

!!! info "Component Dependencies"
    The BlockPersister service relies on interactions with several other components:

    - **Blockchain Service**: Provides information about the current blockchain state and blocks to be persisted
    - **Block Store**: Persistent storage for complete blocks
    - **Subtree Store**: Storage for block subtrees containing transaction references
    - **UTXO Store**: Storage for the current UTXO set and processing changes

## Error Handling

!!! warning "Error Handling Strategy"
    The service implements comprehensive error handling:

    - **Storage errors**: Trigger retries after delay
    - **Processing errors**: Logged with context for debugging
    - **Configuration errors**: Prevent service startup
    - **Database errors**: Logged and service retries after delay

### Streaming Write Error Recovery

The Block Persister uses streaming writes via `FileStorer` and `SubtreeDataWriter` to efficiently handle large files without loading them entirely into memory. These streaming writers implement a **success-flag pattern** to ensure incomplete files are never finalized:

!!! abstract "Abort Mechanism"
    When an error occurs during streaming writes (e.g., missing transaction metadata, store failures):

    1. **FileStorer.Abort()** is called instead of `Close()`
    2. The underlying `io.PipeWriter` is closed with an error via `CloseWithError()`
    3. The blob store's `SetFromReader` detects the error and **removes the temporary file**
    4. No incomplete file is left in the blob store

**Pattern used in Block Persister:**

```go
storer, err := filestorer.NewFileStorer(ctx, logger, settings, store, key, fileType)
if err != nil {
    return err
}

var writeSucceeded bool
defer func() {
    if writeSucceeded {
        storer.Close(ctx)  // Finalizes the file
    } else {
        storer.Abort(err)  // Removes temp file, no incomplete data saved
    }
}()

// ... streaming write operations ...

writeSucceeded = true
return nil
```

!!! tip "SubtreeDataWriter"
    The `SubtreeDataWriter` wraps `FileStorer` and provides an `Abort()` method that propagates to the underlying storer. If `Close()` is called while batches are still pending (indicating incomplete processing), it automatically aborts instead of finalizing.

### Temporary File Cleanup

The file-based blob store implements automatic cleanup of stale temporary files:

- Temporary files use `.tmp` extension during writes
- On successful write, temp files are atomically renamed to final names
- On error/abort, temp files are immediately deleted
- During store initialization, stale `.tmp` files older than 10 minutes are automatically cleaned up

## Metrics

The service provides Prometheus metrics for monitoring:

- Block persistence timing
- Subtree validation metrics
- Transaction processing stats
- Store health indicators

## Dependencies

Required components:

- Block Store (blob.Store)
- Subtree Store (blob.Store)
- UTXO Store (utxo.Store)
- Blockchain Client (blockchain.ClientI)
- Logger (ulogger.Logger)
- Settings (settings.Settings)

## Processing Flow

### Block Processing Loop

!!! abstract "Block Processing Steps"
    1. **Query database** for blocks not yet persisted
    2. **Retrieve block data** if available
    3. **Persist block data** to storage
    4. **Mark block as persisted** in database via `SetBlockPersistedAt`
    5. **Sleep** if no blocks available or on error

### Subtree Processing Flow (Two-Phase)

!!! abstract "Phase 1: Create Subtree Data Files (Parallel)"
    1. **Check if subtree data exists** - skip if already created
    2. **Retrieve subtree structure** from subtree store
    3. **Load transaction metadata** from UTXO store (batched)
    4. **Stream write subtree data** using `SubtreeDataWriter`
    5. **Abort on error** - incomplete files are cleaned up automatically

!!! abstract "Phase 2: Process UTXO Changes (Sequential)"
    1. **Open subtree data file** for streaming read
    2. **Process each transaction** through UTXO diff tracker
    3. **Record additions and deletions** to UTXO set files

!!! tip "Performance Benefit"
    Phase 1 runs in parallel across all subtrees with configurable concurrency, while Phase 2 runs sequentially to maintain UTXO ordering. This separation allows maximum parallelism for I/O-bound file creation while ensuring correct UTXO state.

## Health Checks

!!! success "Health Check Types"
    The service implements two types of health checks:

    ### Liveness Check
    - **Basic service health validation**
    - **No dependency checks**
    - **Quick response** for kubernetes probes

    ### Readiness Check
    - **Comprehensive dependency validation**
    - **Store connectivity verification**
    - **Service operational status**

## Other Resources

- [Block Persister](../../topics/services/blockPersister.md)
- [Prometheus Metrics](../prometheusMetrics.md)
