# Block Assembly Reference Documentation

## Types

### BlockAssembly

```go
type BlockAssembly struct {
    // UnimplementedBlockAssemblyAPIServer provides default implementations for gRPC methods
    blockassembly_api.UnimplementedBlockAssemblyAPIServer

    // blockAssembler handles the core block assembly logic
    blockAssembler *BlockAssembler

    // logger provides logging functionality
    logger ulogger.Logger

    // stats tracks operational statistics
    stats *gocore.Stat

    // settings contains configuration parameters
    settings *settings.Settings

    // blockchainClient interfaces with the blockchain
    blockchainClient blockchain.ClientI

    // txStore manages transaction storage
    txStore blob.Store

    // utxoStore manages UTXO storage
    utxoStore utxostore.Store

    // subtreeStore manages subtree storage
    subtreeStore blob.Store

    // jobStore caches mining jobs with TTL
    jobStore *ttlcache.Cache[chainhash.Hash, *subtreeprocessor.Job]

    // blockSubmissionChan handles block submission requests
    blockSubmissionChan chan *BlockSubmissionRequest

    // skipWaitForPendingBlocks stores the flag value for tests
    skipWaitForPendingBlocks bool

    // stopOnce ensures Stop() is only executed once
    stopOnce sync.Once
}
```

The `BlockAssembly` type is the main structure for the block assembly service. It implements the `UnimplementedBlockAssemblyAPIServer` and contains various components for managing block assembly, storage, and communication with other services.

### BlockAssembler

```go
type BlockAssembler struct {
    // logger provides logging functionality for the assembler
    logger ulogger.Logger

    // stats tracks operational statistics for monitoring and debugging
    stats *gocore.Stat

    // settings contains configuration parameters for block assembly
    settings *settings.Settings

    // utxoStore manages the UTXO set storage and retrieval
    utxoStore utxo.Store

    // subtreeStore manages persistent storage of transaction subtrees
    subtreeStore blob.Store

    // blockchainClient interfaces with the blockchain for network operations
    blockchainClient blockchain.ClientI

    // subtreeProcessor handles the processing and organization of transaction subtrees
    subtreeProcessor subtreeprocessor.Interface

    // miningCandidateCh coordinates requests for mining candidates
    miningCandidateCh chan chan *miningCandidateResponse

    // bestBlock atomically stores the current best block header and height together
    bestBlock atomic.Pointer[BestBlockInfo]

    // stateChangeCh notifies listeners of state changes
    // Protected by stateChangeMu to prevent race conditions
    stateChangeMu sync.RWMutex
    stateChangeCh chan BestBlockInfo

    // lastPersistedHeight tracks the last block height processed by block persister
    // This is updated via BlockPersisted notifications
    lastPersistedHeight atomic.Uint32

    // currentChainMap maps block hashes to their heights
    currentChainMap map[chainhash.Hash]uint32

    // currentChainMapIDs tracks block IDs in the current chain
    currentChainMapIDs map[uint32]struct{}

    // currentChainMapMu protects access to chain maps
    currentChainMapMu sync.RWMutex

    // blockchainSubscriptionCh receives blockchain notifications
    blockchainSubscriptionCh chan *blockchain.Notification

    // defaultMiningNBits stores the default mining difficulty
    defaultMiningNBits *model.NBit

    // resetCh handles reset requests for the assembler
    resetCh chan resetRequest

    // currentRunningState tracks the current operational state
    currentRunningState atomic.Value

    // Note: Cleanup-related fields (cleanupService, cleanupQueueCh, unminedCleanupTicker)
    // were removed in PR #114 when UTXO pruning was extracted to the standalone Pruner service.
    // See: docs/topics/services/pruner.md

    // cachedCandidate stores the cached mining candidate
    cachedCandidate *CachedMiningCandidate

    // skipWaitForPendingBlocks allows tests to skip waiting for pending blocks during startup
    skipWaitForPendingBlocks bool

    // unminedTransactionsLoading indicates if unmined transactions are currently being loaded
    unminedTransactionsLoading atomic.Bool
}
```

The `BlockAssembler` type is responsible for assembling blocks, managing the current chain state, and handling mining candidates.

### SubtreeProcessor

```go
type SubtreeProcessor struct {
    // settings contains the configuration parameters for the processor
    settings *settings.Settings

    // currentItemsPerFile specifies the maximum number of items per subtree file
    currentItemsPerFile int

    // blockStartTime tracks when the current block started
    blockStartTime time.Time

    // subtreesInBlock tracks number of subtrees created in current block
    subtreesInBlock int

    // blockIntervals tracks recent intervals per subtree in previous blocks
    blockIntervals []time.Duration

    // maxBlockSamples is the number of block samples to keep for averaging
    maxBlockSamples int

    // txChan receives transaction batches for processing
    txChan chan *[]TxIDAndFee

    // getSubtreesChan handles requests to retrieve current subtrees
    getSubtreesChan chan chan []*util.Subtree

    // moveForwardBlockChan receives requests to process new blocks
    moveForwardBlockChan chan moveBlockRequest

    // reorgBlockChan handles blockchain reorganization requests
    reorgBlockChan chan reorgBlocksRequest

    // resetCh handles requests to reset the processor state
    resetCh chan *resetBlocks

    // removeTxCh receives transactions to be removed
    removeTxCh chan chainhash.Hash

    // lengthCh receives requests for the current length of the processor
    lengthCh chan chan int

    // checkSubtreeProcessorCh is used to check the subtree processor state
    checkSubtreeProcessorCh chan chan error

    // newSubtreeChan receives notifications about new subtrees
    newSubtreeChan chan NewSubtreeRequest

    // chainedSubtrees stores the ordered list of completed subtrees
    chainedSubtrees []*util.Subtree

    // chainedSubtreeCount tracks the number of chained subtrees atomically
    chainedSubtreeCount atomic.Int32

    // currentSubtree represents the subtree currently being built
    currentSubtree *util.Subtree

    // currentBlockHeader stores the current block header being processed
    currentBlockHeader *model.BlockHeader

    // Mutex provides thread-safe access to shared resources
    sync.Mutex

    // txCount tracks the total number of transactions processed
    txCount atomic.Uint64

    // batcher manages transaction batching operations
    batcher *TxIDAndFeeBatch

    // queue manages the transaction processing queue
    queue *LockFreeQueue

    // currentTxMap tracks transactions currently held in the subtree processor
    currentTxMap *util.SyncedMap[chainhash.Hash, meta.TxInpoints]

    // removeMap tracks transactions marked for removal
    removeMap *util.SwissMap

    // subtreeStore manages persistent storage of transaction subtrees
    subtreeStore blob.Store

    // utxoStore manages the UTXO set storage and retrieval
    utxoStore utxostore.Store

    // logger provides logging functionality
    logger ulogger.Logger

    // stats tracks operational statistics
    stats *gocore.Stat

    // currentRunningState tracks the current operational state
    currentRunningState atomic.Value
}
```

The `SubtreeProcessor` type manages the processing of transactions into subtrees, handling chain reorganizations, and maintaining the current state of the block assembly process.

### LockFreeQueue

```go
type LockFreeQueue struct {
    head        *TxIDAndFee
    tail        atomic.Pointer[TxIDAndFee]
    queueLength atomic.Int64
}
```

The `LockFreeQueue` type represents a lock-free FIFO queue for managing transactions in the block assembly process.

## Functions

### BlockAssembly

#### New

```go
func New(logger ulogger.Logger, tSettings *settings.Settings, txStore blob.Store, utxoStore utxostore.Store, subtreeStore blob.Store, blockchainClient blockchain.ClientI) *BlockAssembly
```

Creates a new instance of the `BlockAssembly` service with the specified dependencies and initializes Prometheus metrics.

#### Health

```go
func (ba *BlockAssembly) Health(ctx context.Context, checkLiveness bool) (int, string, error)
```

Performs health checks on the block assembly service. If checkLiveness is true, only performs basic liveness checks. Otherwise, performs readiness checks on all dependencies: BlockchainClient, FSM, SubtreeStore, TxStore, and UTXOStore.

#### HealthGRPC

```go
func (ba *BlockAssembly) HealthGRPC(ctx context.Context, _ *blockassembly_api.EmptyMessage) (*blockassembly_api.HealthResponse, error)
```

Performs a gRPC health check on the block assembly service.

#### Init

```go
func (ba *BlockAssembly) Init(ctx context.Context) (err error)
```

Initializes the block assembly service by creating a BlockAssembler instance, subscribing to blockchain notifications, and preparing for transaction processing.

#### Start

```go
func (ba *BlockAssembly) Start(ctx context.Context, readyCh chan<- struct{}) (err error)
```

Starts the block assembly service, launches concurrent processing routines, and begins listening for blockchain notifications. The readyCh channel is closed once the service is ready to receive requests.

#### Stop

```go
func (ba *BlockAssembly) Stop(_ context.Context) error
```

Gracefully shuts down the BlockAssembly service, stopping all processing operations.

#### AddTx

```go
func (ba *BlockAssembly) AddTx(ctx context.Context, req *blockassembly_api.AddTxRequest) (*blockassembly_api.AddTxResponse, error)
```

Adds a transaction to the block assembly. This method accepts a transaction request and forwards it to the block assembler for processing.

#### RemoveTx

```go
func (ba *BlockAssembly) RemoveTx(ctx context.Context, req *blockassembly_api.RemoveTxRequest) (*blockassembly_api.EmptyMessage, error)
```

Removes a transaction from the block assembly by its hash. This prevents the transaction from being included in future blocks.

#### AddTxBatch

```go
func (ba *BlockAssembly) AddTxBatch(ctx context.Context, batch *blockassembly_api.AddTxBatchRequest) (*blockassembly_api.AddTxBatchResponse, error)
```

Processes a batch of transactions for block assembly. This is more efficient than adding transactions individually.

#### TxCount

```go
func (ba *BlockAssembly) TxCount() uint64
```

Returns the total number of transactions processed by the block assembly service.

#### GetMiningCandidate

```go
func (ba *BlockAssembly) GetMiningCandidate(ctx context.Context, req *blockassembly_api.GetMiningCandidateRequest) (*model.MiningCandidate, error)
```

Retrieves a candidate block for mining. This method returns a block template that miners can use to find a valid proof-of-work solution.

#### SubmitMiningSolution

```go
func (ba *BlockAssembly) SubmitMiningSolution(ctx context.Context, req *blockassembly_api.SubmitMiningSolutionRequest) (*blockassembly_api.OKResponse, error)
```

Processes a mining solution submission. It validates the solution, creates a block, and adds it to the blockchain.

#### SubtreeCount

```go
func (ba *BlockAssembly) SubtreeCount() int
```

Returns the total number of subtrees managed by the block assembly service.

#### ResetBlockAssembly

```go
func (ba *BlockAssembly) ResetBlockAssembly(ctx context.Context, _ *blockassembly_api.EmptyMessage) (*blockassembly_api.EmptyMessage, error)
```

Resets the block assembly service to a clean state, removing all transactions and subtrees. This is useful for recovery after errors or when a full reset is needed.

#### ResetBlockAssemblyFully

```go
func (ba *BlockAssembly) ResetBlockAssemblyFully(ctx context.Context, _ *blockassembly_api.EmptyMessage) (*blockassembly_api.EmptyMessage, error)
```

Performs a comprehensive reset of block assembly state, including clearing the mempool and all unmined transactions in addition to the standard reset. This is a more thorough version of `ResetBlockAssembly` intended for complete recovery scenarios.

#### GetBlockAssemblyState

```go
func (ba *BlockAssembly) GetBlockAssemblyState(ctx context.Context, _ *blockassembly_api.EmptyMessage) (*blockassembly_api.StateMessage, error)
```

Retrieves the current operational state of the block assembly service, including transaction and subtree counts, blockchain tip information, and queue metrics. This information is valuable for monitoring, debugging, and ensuring the service is operating correctly.

#### GetCurrentDifficulty

```go
func (ba *BlockAssembly) GetCurrentDifficulty(_ context.Context, _ *blockassembly_api.EmptyMessage) (*blockassembly_api.GetCurrentDifficultyResponse, error)
```

Retrieves the current mining difficulty target required for valid proof-of-work. This value determines how much computational work is required to find a valid block solution.

#### GenerateBlocks

```go
func (ba *BlockAssembly) GenerateBlocks(ctx context.Context, req *blockassembly_api.GenerateBlocksRequest) (*blockassembly_api.EmptyMessage, error)
```

Generates the given number of blocks by mining them. This method is primarily used for testing and development environments. The operation requires the GenerateSupported flag to be enabled in chain configuration.

Parameters:

- `req.Count`: Number of blocks to generate
- `req.Address`: Optional mining address

#### CheckBlockAssembly

```go
func (ba *BlockAssembly) CheckBlockAssembly(_ context.Context, _ *blockassembly_api.EmptyMessage) (*blockassembly_api.OKResponse, error)
```

Checks the block assembly state for integrity and consistency.

#### GetBlockAssemblyBlockCandidate

```go
func (ba *BlockAssembly) GetBlockAssemblyBlockCandidate(ctx context.Context, _ *blockassembly_api.EmptyMessage) (*blockassembly_api.GetBlockAssemblyBlockCandidateResponse, error)
```

Retrieves the current block assembly block candidate, including detailed information about the candidate's construction.

#### GetBlockAssemblyTxs

```go
func (ba *BlockAssembly) GetBlockAssemblyTxs(ctx context.Context, _ *blockassembly_api.EmptyMessage) (*blockassembly_api.GetBlockAssemblyTxsResponse, error)
```

Retrieves all transaction hashes currently held in the block assembly service. Returns both the count and list of transaction hashes for monitoring and debugging purposes.

#### SetSkipWaitForPendingBlocks

```go
func (ba *BlockAssembly) SetSkipWaitForPendingBlocks(skip bool)
```

Sets the flag to skip waiting for pending blocks during startup. This is primarily used in test environments to prevent blocking on pending blocks.

### BlockAssembler

#### NewBlockAssembler

```go
func NewBlockAssembler(ctx context.Context, logger ulogger.Logger, tSettings *settings.Settings, stats *gocore.Stat, utxoStore utxo.Store, subtreeStore blob.Store, blockchainClient blockchain.ClientI, newSubtreeChan chan subtreeprocessor.NewSubtreeRequest) (*BlockAssembler, error)
```

Creates and initializes a new BlockAssembler instance with the specified dependencies.

#### TxCount

```go
func (b *BlockAssembler) TxCount() uint64
```

Returns the total number of transactions in the assembler.

#### QueueLength

```go
func (b *BlockAssembler) QueueLength() int64
```

Returns the current length of the transaction queue.

#### SubtreeCount

```go
func (b *BlockAssembler) SubtreeCount() int
```

Returns the total number of subtrees.

#### Start

```go
func (b *BlockAssembler) Start(ctx context.Context) error
```

Initializes and begins the block assembler operations, setting up channel listeners and initializing the blockchain state.

#### GetState

```go
func (b *BlockAssembler) GetState(ctx context.Context) (*model.BlockHeader, uint32, error)
```

Retrieves the current state of the block assembler from the blockchain, returning the best block header and height.

#### SetState

```go
func (b *BlockAssembler) SetState(ctx context.Context) error
```

Persists the current state of the block assembler to the blockchain.

#### CurrentBlock

```go
func (b *BlockAssembler) CurrentBlock() (*model.BlockHeader, uint32)
```

Returns the current best block header and height.

#### AddTx

```go
func (b *BlockAssembler) AddTx(node util.SubtreeNode, txInpoints meta.TxInpoints)
```

Adds a transaction to the block assembler along with its input points.

#### RemoveTx

```go
func (b *BlockAssembler) RemoveTx(hash chainhash.Hash) error
```

Removes a transaction from the block assembler by its hash.

#### Reset

```go
func (b *BlockAssembler) Reset()
```

Triggers a reset of the block assembler state. This operation runs asynchronously to prevent blocking.

#### GetMiningCandidate

```go
func (b *BlockAssembler) GetMiningCandidate(_ context.Context) (*model.MiningCandidate, []*util.Subtree, error)
```

Retrieves a candidate block for mining along with its associated subtrees.

### SubtreeProcessor

#### NewSubtreeProcessor

```go
func NewSubtreeProcessor(ctx context.Context, logger ulogger.Logger, tSettings *settings.Settings, subtreeStore blob.Store, blockchainClient blockchain.ClientI, utxoStore utxostore.Store, newSubtreeChan chan NewSubtreeRequest, options ...Options) (*SubtreeProcessor, error)
```

Creates and initializes a new SubtreeProcessor instance with the specified dependencies. This is the core component responsible for organizing transactions into hierarchical subtrees for efficient block assembly.

#### TxCount

```go
func (stp *SubtreeProcessor) TxCount() uint64
```

Returns the total number of transactions processed by the subtree processor.

#### QueueLength

```go
func (stp *SubtreeProcessor) QueueLength() int64
```

Returns the current length of the transaction queue in the subtree processor.

#### SubtreeCount

```go
func (stp *SubtreeProcessor) SubtreeCount() int
```

Returns the total number of subtrees managed by the processor. This method is primarily used for prometheus statistics.

#### GetCurrentRunningState

```go
func (stp *SubtreeProcessor) GetCurrentRunningState() State
```

Returns the current operational state of the processor.

#### GetCurrentLength

```go
func (stp *SubtreeProcessor) GetCurrentLength() int
```

Returns the length of the current subtree.

#### Add

```go
func (stp *SubtreeProcessor) Add(node util.SubtreeNode, txInpoints meta.TxInpoints)
```

Adds a transaction node to the processor along with its input points.

#### Remove

```go
func (stp *SubtreeProcessor) Remove(hash chainhash.Hash) error
```

Prevents a transaction from being processed from the queue into a subtree, and removes it if already present. This can only take place before the delay time in the queue has passed.

#### GetCompletedSubtreesForMiningCandidate

```go
func (stp *SubtreeProcessor) GetCompletedSubtreesForMiningCandidate() []*util.Subtree
```

Retrieves all completed subtrees for block mining.

#### MoveForwardBlock

```go
func (stp *SubtreeProcessor) MoveForwardBlock(block *model.Block) error
```

Updates the subtrees when a new block is found.

#### Reorg

```go
func (stp *SubtreeProcessor) Reorg(moveBackBlocks []*model.Block, moveForwardBlocks []*model.Block) error
```

Handles blockchain reorganization by processing moved blocks. This method manages the complex task of reconciling the subtree state with the new blockchain state after a reorganization.

#### Reset

```go
func (stp *SubtreeProcessor) Reset(blockHeader *model.BlockHeader, moveBackBlocks []*model.Block, moveForwardBlocks []*model.Block, isLegacySync bool) ResetResponse
```

Resets the processor to a clean state, removing all subtrees and transactions.

#### CheckSubtreeProcessor

```go
func (stp *SubtreeProcessor) CheckSubtreeProcessor() error
```

Checks the integrity of the subtree processor. It verifies that all transactions in the current transaction map are present in the subtrees and that the size of the current transaction map matches the expected transaction count.

### LockFreeQueue

#### NewLockFreeQueue

```go
func NewLockFreeQueue() *LockFreeQueue
```

Creates a new instance of the LockFreeQueue, which is a lock-free FIFO queue for managing transactions in the block assembly process.

#### Len

```go
func (q *LockFreeQueue) Len() int64
```

Returns the current length of the queue.

#### Enqueue

```go
func (q *LockFreeQueue) Enqueue(v *TxIDAndFee)
```

Adds a transaction to the queue in a thread-safe manner.

#### Dequeue

```go
func (q *LockFreeQueue) Dequeue(validFromMillis int64) *TxIDAndFee
```

Removes and returns a transaction from the queue if its validation time has passed. This method implements the delay mechanism that allows transactions to be properly validated before being included in subtrees.

## Related Documents

- [Block Assembly Topic Guide](../../topics/services/blockAssembly.md)
- [Block Assembly Settings](../settings/services/blockassembly_settings.md)
- [Block Assembly Protobuf Reference](../protobuf_docs/blockassemblyProto.md)
- [Prometheus Metrics](../prometheusMetrics.md)
