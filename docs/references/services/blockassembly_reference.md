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
    subtreeProcessor *subtreeprocessor.SubtreeProcessor

    // miningCandidateCh coordinates requests for mining candidates
    miningCandidateCh chan chan *miningCandidateResponse

    // bestBlockHeader atomically stores the current best block header
    bestBlockHeader atomic.Pointer[model.BlockHeader]

    // bestBlockHeight atomically stores the current best block height
    bestBlockHeight atomic.Uint32

    // currentChain stores the current blockchain state
    currentChain []*model.BlockHeader

    // currentChainMap maps block hashes to their heights
    currentChainMap map[chainhash.Hash]uint32

    // currentChainMapIDs tracks block IDs in the current chain
    currentChainMapIDs map[uint32]struct{}

    // currentChainMapMu protects access to chain maps
    currentChainMapMu sync.RWMutex

    // blockchainSubscriptionCh receives blockchain notifications
    blockchainSubscriptionCh chan *blockchain.Notification

    // currentDifficulty stores the current mining difficulty target
    currentDifficulty atomic.Pointer[model.NBit]

    // defaultMiningNBits stores the default mining difficulty
    defaultMiningNBits *model.NBit

    // resetCh handles reset requests for the assembler
    resetCh chan struct{}

    // resetWaitCount tracks the number of blocks to wait after reset
    resetWaitCount atomic.Int32

    // resetWaitDuration tracks the time to wait after reset
    resetWaitDuration atomic.Int32

    // currentRunningState tracks the current operational state
    currentRunningState atomic.Value
}
```

The `BlockAssembler` type is responsible for assembling blocks, managing the current chain state, and handling mining candidates.

### SubtreeProcessor

```go
type SubtreeProcessor struct {
    currentItemsPerFile       int
    settings                 *settings.Settings
    txChan                    chan *[]TxIDAndFee
    getSubtreesChan           chan chan []*util.Subtree
    moveForwardBlockChan      chan moveBlockRequest
    reorgBlockChan            chan reorgBlocksRequest
    deDuplicateTransactionsCh chan struct{}
    resetCh                   chan *resetBlocks
    newSubtreeChan            chan NewSubtreeRequest
    chainedSubtrees           []*util.Subtree
    chainedSubtreeCount       atomic.Int32
    currentSubtree            *util.Subtree
    currentBlockHeader        *model.BlockHeader
    sync.Mutex
    txCount                   atomic.Uint64
    batcher                   *TxIDAndFeeBatch
    queue                     *LockFreeQueue
    removeMap                 *util.SwissMap
    subtreeStore              blob.Store
    utxoStore                 utxostore.Store
    logger                    ulogger.Logger
    stats                     *gocore.Stat
    currentRunningState       atomic.Value
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

Gracefully shuts down the BlockAssembly service, stopping all internal processes and cleaning up resources.

#### AddTx

```go
func (ba *BlockAssembly) AddTx(ctx context.Context, req *blockassembly_api.AddTxRequest) (resp *blockassembly_api.AddTxResponse, err error)
```

Adds a transaction to the block assembly process.

#### RemoveTx

```go
func (ba *BlockAssembly) RemoveTx(ctx context.Context, req *blockassembly_api.RemoveTxRequest) (*blockassembly_api.EmptyMessage, error)
```

Removes a transaction from the block assembly process.

#### AddTxBatch

```go
func (ba *BlockAssembly) AddTxBatch(ctx context.Context, batch *blockassembly_api.AddTxBatchRequest) (*blockassembly_api.AddTxBatchResponse, error)
```

Adds a batch of transactions to the block assembly process.

#### GetMiningCandidate

```go
func (ba *BlockAssembly) GetMiningCandidate(ctx context.Context, req *blockassembly_api.GetMiningCandidateRequest) (*model.MiningCandidate, error)
```

Retrieves a candidate block for mining, containing all necessary information for miners to begin the mining process.

#### SubmitMiningSolution

```go
func (ba *BlockAssembly) SubmitMiningSolution(ctx context.Context, req *blockassembly_api.SubmitMiningSolutionRequest) (*blockassembly_api.SubmitMiningSolutionResponse, error)
```

Processes a mining solution submission. It validates the solution, creates a block, and adds it to the blockchain.

#### DeDuplicateBlockAssembly

```go
func (ba *BlockAssembly) DeDuplicateBlockAssembly(ctx context.Context, _ *blockassembly_api.EmptyMessage) (*blockassembly_api.EmptyMessage, error)
```

Removes duplicate transactions from the block assembly process. This operation helps maintain block validity by ensuring no transaction is included multiple times.

#### GenerateBlocks

```go
func (ba *BlockAssembly) GenerateBlocks(ctx context.Context, req *blockassembly_api.GenerateBlocksRequest) (*blockassembly_api.EmptyMessage, error)
```

Generates the specified number of blocks, if block generation is supported by the chain configuration. Each block is mined and submitted following standard block assembly rules.

Parameters:
- `req.Count`: Number of blocks to generate
- `req.Address`: Optional mining address

#### GetBlockAssemblyState

```go
func (ba *BlockAssembly) GetBlockAssemblyState(ctx context.Context, _ *blockassembly_api.EmptyMessage) (*blockassembly_api.StateMessage, error)
```

### BlockAssembler

#### NewBlockAssembler

```go
func NewBlockAssembler(ctx context.Context, logger ulogger.Logger, utxoStore utxo.Store, subtreeStore blob.Store, blockchainClient blockchain.ClientI, newSubtreeChan chan subtreeprocessor.NewSubtreeRequest) *BlockAssembler
```

Creates a new instance of the `BlockAssembler`.

#### Start

```go
func (b *BlockAssembler) Start(ctx context.Context) error
```

Starts the block assembler.

#### AddTx

```go
func (b *BlockAssembler) AddTx(node util.SubtreeNode)
```

Adds a transaction to the block assembler.

#### RemoveTx

```go
func (b *BlockAssembler) RemoveTx(hash chainhash.Hash) error
```

Removes a transaction from the block assembler.

#### GetMiningCandidate

```go
func (b *BlockAssembler) GetMiningCandidate(_ context.Context) (*model.MiningCandidate, []*util.Subtree, error)
```

Retrieves a mining candidate from the block assembler.

### SubtreeProcessor

#### NewSubtreeProcessor

```go
func NewSubtreeProcessor(ctx context.Context, logger ulogger.Logger, subtreeStore blob.Store, utxoStore utxostore.Store, newSubtreeChan chan NewSubtreeRequest, options ...Options) (*SubtreeProcessor, error)
```

Creates a new instance of the `SubtreeProcessor`.

#### Add

```go
func (stp *SubtreeProcessor) Add(node util.SubtreeNode)
```

Adds a transaction to the subtree processor.

#### Remove

```go
func (stp *SubtreeProcessor) Remove(hash chainhash.Hash) error
```

Removes a transaction from the subtree processor.

#### GetCompletedSubtreesForMiningCandidate

```go
func (stp *SubtreeProcessor) GetCompletedSubtreesForMiningCandidate() []*util.Subtree
```

Retrieves completed subtrees for mining candidate generation.

#### MoveForwardBlock

```go
func (stp *SubtreeProcessor) MoveForwardBlock(block *model.Block) error
```

Moves the subtree processor state up to a new block.

#### Reorg

```go
func (stp *SubtreeProcessor) Reorg(moveBackBlocks []*model.Block, modeUpBlocks []*model.Block) error
```

Handles chain reorganization in the subtree processor.

### LockFreeQueue

#### NewLockFreeQueue

```go
func NewLockFreeQueue() *LockFreeQueue
```

Creates a new instance of the `LockFreeQueue`.

#### enqueue

```go
func (q *LockFreeQueue) enqueue(v *TxIDAndFee)
```

Adds a transaction to the queue.

#### dequeue

```go
func (q *LockFreeQueue) dequeue(validFromMillis int64) *TxIDAndFee
```

Removes and returns a transaction from the queue.
