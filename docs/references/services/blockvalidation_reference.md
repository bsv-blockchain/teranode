# Block Validation Service Reference Documentation

## Types

### Server

```go
type Server struct {
    // UnimplementedBlockValidationAPIServer provides default implementations of gRPC methods
    blockvalidation_api.UnimplementedBlockValidationAPIServer

    logger                    ulogger.Logger                          // Structured logging with contextual information

    settings                  *settings.Settings                      // Operational parameters and configuration
    blockchainClient          blockchain.ClientI                      // Blockchain state and operations client

    subtreeStore              blob.Store                              // Persistent storage for block subtrees
    txStore                   blob.Store                              // Permanent storage for transactions

    utxoStore                 utxo.Store                              // UTXO set management for transaction validation
    blockAssemblyClient       blockassembly.ClientI                   // Block assembly service client

    blockFoundCh              chan processBlockFound                  // Channel for newly discovered blocks
    blockPriorityQueue        *BlockPriorityQueue                     // Priority queue for block processing
    blockClassifier           *BlockClassifier                        // Block priority classification
    forkManager               *ForkManager                            // Fork processing management
    catchupCh                 chan processBlockCatchup                // Channel for catchup block processing

    blockValidation           *BlockValidation                        // Core validation logic and state
    validatorClient           validator.Interface                     // Transaction validation services

    kafkaConsumerClient       kafka.KafkaConsumerGroupI              // Kafka message consumption client
    processBlockNotify        *ttlcache.Cache[chainhash.Hash, bool]   // Cache for block processing state
    catchupAlternatives       *ttlcache.Cache[chainhash.Hash, []processBlockCatchup] // Alternative peer sources for catchup blocks
    stats                     *gocore.Stat                            // Operational metrics tracking
    peerCircuitBreakers       *catchup.PeerCircuitBreakers            // Circuit breakers for peer management
    headerChainCache          *catchup.HeaderChainCache               // Block header cache for catchup
    p2pClient                 P2PClientI                              // P2P service client for peer registry operations
    isCatchingUp              atomic.Bool                             // Atomic flag for catchup operations
    catchupStatsMu            sync.RWMutex                            // Mutex for catchup statistics
    lastCatchupTime           time.Time                               // Timestamp of last catchup attempt
    lastCatchupResult         bool                                    // Result of last catchup operation
    catchupAttempts           atomic.Int64                            // Total catchup attempts counter
    catchupSuccesses          atomic.Int64                            // Successful catchup operations counter
}
```

The `Server` type implements a high-performance block validation service for Bitcoin SV. It coordinates block validation, subtree management, and transaction metadata processing across multiple subsystems while maintaining chain consistency. The server supports both synchronous and asynchronous validation modes, with automatic catchup capabilities when falling behind the chain tip.

### processBlockFound

```go
type processBlockFound struct {
    hash    *chainhash.Hash // Block identifier using double SHA256 hash
    baseURL string          // Peer URL for additional block data retrieval
    peerID  string          // P2P peer identifier for metrics tracking
    errCh   chan error      // Error channel for validation completion
}
```

The `processBlockFound` type encapsulates information about a newly discovered block that requires validation. It includes both the block identifier and communication channels for handling validation results.

### processBlockCatchup

```go
type processBlockCatchup struct {
    block   *model.Block // Full block data for validation
    baseURL string       // Peer URL for additional data retrieval
    peerID  string       // P2P peer identifier for metrics tracking
}
```

The `processBlockCatchup` type contains information needed to process a block during chain catchup operations when the node has fallen behind the current chain tip.

### BlockValidation

```go
type BlockValidation struct {
    // logger provides structured logging capabilities
    logger ulogger.Logger

    // settings contains operational parameters and feature flags
    settings *settings.Settings

    // blockchainClient interfaces with the blockchain for operations
    blockchainClient blockchain.ClientI

    // subtreeStore provides persistent storage for block subtrees
    subtreeStore blob.Store

    // subtreeBlockHeightRetention specifies how long subtrees should be retained
    subtreeBlockHeightRetention uint32

    // txStore handles permanent storage of transactions
    txStore blob.Store

    // utxoStore manages the UTXO set for transaction validation
    utxoStore utxo.Store

    // validatorClient handles transaction validation operations
    validatorClient validator.Interface

    // recentBlocksBloomFilters maintains bloom filters for recent blocks
    recentBlocksBloomFilters *txmap.SyncedMap[chainhash.Hash, *model.BlockBloomFilter]

    // bloomFilterRetentionSize defines the number of blocks to keep the bloom filter for
    bloomFilterRetentionSize uint32

    // subtreeValidationClient manages subtree validation processes
    subtreeValidationClient subtreevalidation.Interface

    // subtreeDeDuplicator prevents duplicate processing of subtrees
    subtreeDeDuplicator *DeDuplicator

    // lastValidatedBlocks caches recently validated blocks for 2 minutes
    lastValidatedBlocks *expiringmap.ExpiringMap[chainhash.Hash, *model.Block]

    // blockExists tracks validated block hashes for 2 hours
    blockExists *expiringmap.ExpiringMap[chainhash.Hash, bool]

    // subtreeExists tracks validated subtree hashes for 10 minutes
    subtreeExists *expiringmap.ExpiringMap[chainhash.Hash, bool]

    // subtreeCount tracks the number of subtrees being processed
    subtreeCount atomic.Int32

    // blockHashesCurrentlyValidated tracks blocks in validation process
    blockHashesCurrentlyValidated *txmap.SwissMap

    // blocksCurrentlyValidating tracks blocks being validated to prevent concurrent validation
    blocksCurrentlyValidating *txmap.SyncedMap[chainhash.Hash, *validationResult]

    // blockBloomFiltersBeingCreated tracks bloom filters being generated
    blockBloomFiltersBeingCreated *txmap.SwissMap

    // bloomFilterStats collects statistics about bloom filter operations
    bloomFilterStats *model.BloomStats

    // setMinedChan receives block hashes that need to be marked as mined
    setMinedChan chan *chainhash.Hash

    // revalidateBlockChan receives blocks that need revalidation
    revalidateBlockChan chan revalidateBlockData

    // stats tracks operational metrics for monitoring
    stats *gocore.Stat

    // invalidBlockKafkaProducer publishes invalid block events to Kafka
    invalidBlockKafkaProducer kafka.KafkaAsyncProducerI

    // backgroundTasks tracks background goroutines to ensure proper shutdown
    backgroundTasks sync.WaitGroup
}
```

The `BlockValidation` type handles the core validation logic for blocks in Teranode. It manages block validation, subtree processing, and bloom filter creation.

## Functions

### Server

#### New

```go
func New(logger ulogger.Logger, tSettings *settings.Settings, subtreeStore blob.Store, txStore blob.Store, utxoStore utxo.Store, validatorClient validator.Interface, blockchainClient blockchain.ClientI, kafkaConsumerClient kafka.KafkaConsumerGroupI, blockAssemblyClient blockassembly.ClientI, p2pClient P2PClientI) *Server
```

Creates a new instance of the `Server` with:

- Initialization of Prometheus metrics
- Setup of background processors
- Configuration of validation components

#### Health

```go
func (u *Server) Health(ctx context.Context, checkLiveness bool) (int, string, error)
```

Performs comprehensive health checks:

- Liveness checks when checkLiveness is true
- Dependency checks including:

    - Kafka broker status
    - Blockchain client
    - FSM state
    - Subtree store
    - Transaction store
    - UTXO store

#### Init

```go
func (u *Server) Init(ctx context.Context) (err error)
```

Initializes the service:

- Sets up subtree validation client
- Configures background processors
- Initializes Kafka consumer
- Sets up metadata processing queue

#### HealthGRPC

```go
func (u *Server) HealthGRPC(ctx context.Context, _ *blockvalidation_api.EmptyMessage) (*blockvalidation_api.HealthResponse, error)
```

Performs a gRPC health check on the service, including:

- Full dependency verification
- Detailed status reporting
- Timestamp information

#### Start

```go
func (u *Server) Start(ctx context.Context, readyCh chan<- struct{}) error
```

Starts all service components:

- Kafka consumer
- HTTP server (if configured)
- gRPC server
- Background processors

#### Stop

```go
func (u *Server) Stop(_ context.Context) error
```

Gracefully stops the service:

- Closes TTL cache
- Shuts down Kafka consumer
- Cleans up resources

#### BlockFound

```go
func (u *Server) BlockFound(ctx context.Context, req *blockvalidation_api.BlockFoundRequest) (*blockvalidation_api.EmptyMessage, error)
```

Handles notification of new blocks:

- Verifies block doesn't already exist
- Queues block for validation
- Optionally waits for validation completion

#### ProcessBlock

```go
func (u *Server) ProcessBlock(ctx context.Context, request *blockvalidation_api.ProcessBlockRequest) (*blockvalidation_api.EmptyMessage, error)
```

Processes complete blocks:

- Validates block structure
- Handles height calculation
- Integrates with blockchain state

#### ValidateBlock

```go
func (u *Server) ValidateBlock(ctx context.Context, request *blockvalidation_api.ValidateBlockRequest) (*blockvalidation_api.ValidateBlockResponse, error)
```

Validates a block and returns validation results without adding it to the blockchain. This method performs comprehensive block validation including structure checks, transaction validation, and consensus rule verification.

### Internal Methods

#### processBlockFound

```go
func (u *Server) processBlockFound(ctx context.Context, hash *chainhash.Hash, peerID string, baseURL string, useBlock ...*model.Block) error
```

Internal method that processes a newly discovered block. Handles block retrieval, validation, and integration with the blockchain state.

#### checkParentProcessingComplete

```go
func (u *Server) checkParentProcessingComplete(ctx context.Context, block *model.Block, baseURL string)
```

Verifies that a block's parent has completed processing before proceeding with validation. Ensures proper block ordering and chain consistency.

#### startBlockProcessingSystem

```go
func (u *Server) startBlockProcessingSystem(ctx context.Context)
```

Initializes the priority-based block processing system with support for parallel fork processing. Sets up worker goroutines and processing queues.

#### blockProcessingWorker

```go
func (u *Server) blockProcessingWorker(ctx context.Context, workerID int)
```

Worker goroutine that processes blocks from the priority queue. Handles block classification, validation, and error recovery.

#### addBlockToPriorityQueue

```go
func (u *Server) addBlockToPriorityQueue(ctx context.Context, blockFound processBlockFound)
```

Adds a block to the priority queue with appropriate classification based on its relationship to the current chain tip.

#### processBlockWithPriority

```go
func (u *Server) processBlockWithPriority(ctx context.Context, blockFound processBlockFound) error
```

Processes a block based on its assigned priority, handling both normal processing and retry scenarios with alternative peer sources.

### BlockValidation

#### ValidateBlock

```go
func (u *BlockValidation) ValidateBlock(ctx context.Context, block *model.Block, baseURL string, bloomStats *model.BloomStats, disableOptimisticMining ...bool) error
```

Performs comprehensive block validation:

- Size verification
- Parent block validation
- Subtree validation
- Optional optimistic mining

#### GetBlockExists

```go
func (u *BlockValidation) GetBlockExists(ctx context.Context, hash *chainhash.Hash) (bool, error)
```

Checks block existence in validation system.

## Core Features

### Chain Catchup Process

The Chain Catchup system automatically handles situations when the node falls behind the main blockchain. When activated, it:

The system detects missing blocks by comparing the current node's block height with the network height. When it identifies a gap, it initiates the catchup process. The service retrieves blocks in batches of up to 100 blocks at a time to optimize network usage. During catchup, multiple blocks are validated simultaneously using configurable concurrency settings, typically defaulting to 32 concurrent validations. The service manages state transitions through the FSM (Finite State Machine), moving from normal operation to catchup mode and back once synchronization is complete.

### Optimistic Mining Support

Optimistic Mining is a performance optimization technique that allows faster block processing. In this mode:

The system accepts new blocks before completing full validation, provisionally adding them to the chain. While the block is being added, validation continues in the background without blocking the main processing pipeline. This behavior can be configured through settings, allowing operators to balance between performance and validation thoroughness based on their requirements.

### Bloom Filters

The Block Validation Service maintains bloom filters for recent blocks to optimize validation. This mechanism provides a probabilistic way to quickly determine if a transaction has been seen in a previous block without needing to perform extensive searches.

Creates filters dynamically as new blocks are validated, maintaining them in memory for rapid access. The service uses a configurable retention size parameter (default set in settings) to determine how many recent blocks should maintain bloom filters. The service uses a thread-safe SyncedMap to ensure concurrent access to filters is handled properly.

## Service Configuration

The service configuration is managed through several key areas:

### Validation Settings

- Maximum block size limits
- Block validation timeouts
- Transaction verification parameters
- Signature checking requirements

### Performance Controls

- Maximum concurrent validations
- Batch processing sizes
- Queue buffer sizes
- Cache retention periods

### Network Configuration

- Kafka broker addresses and topics
- HTTP server binding and ports
- Connection timeout values
- TLS/SSL settings

### Chain Catchup Parameters

- Maximum blocks per batch
- Catchup trigger thresholds
- Validation concurrency limits
- State transition timeouts

## Error Management

The service implements a structured error handling system:

### Error Categories

Recoverable errors include temporary network issues or resource constraints that can be resolved through retries. Unrecoverable errors indicate fundamental problems like invalid block structures or consensus violations.

### Error Processing

All errors are wrapped with appropriate context for the gRPC interface, maintaining error type information while adding relevant metadata. The system implements exponential backoff for retryable operations, with configurable retry limits and delays.

## Performance Monitoring

The service provides comprehensive performance monitoring through Prometheus metrics:

### Block Processing Metrics

- Time to validate blocks (histogram)
- Number of blocks in processing queue
- Block acceptance/rejection rates
- Chain catchup progress

### Resource Usage Metrics

- Transaction queue lengths
- Memory cache utilization
- Store operation latencies
- Background worker statistics

## Related Documents

- [Block Validation Topic Guide](../../topics/services/blockValidation.md)
- [Block Validation Settings](../settings/services/blockvalidation_settings.md)
- [Block Validation Protobuf Reference](../protobuf_docs/blockvalidationProto.md)
- [Prometheus Metrics](../prometheusMetrics.md)
