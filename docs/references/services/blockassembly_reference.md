# Block Assembly Reference Documentation

## Types

### BlockAssembly

```go
type BlockAssembly struct {
    blockassembly_api.UnimplementedBlockAssemblyAPIServer
    blockAssembler        *BlockAssembler
    logger                ulogger.Logger
    stats                 *gocore.Stat
    blockchainClient      blockchain.ClientI
    txStore               blob.Store
    utxoStore             utxostore.Store
    subtreeStore          blob.Store
    subtreeTTL            time.Duration
    jobStore              *ttlcache.Cache[chainhash.Hash, *subtreeprocessor.Job]
    blockSubmissionChan   chan *BlockSubmissionRequest
}
```

The `BlockAssembly` type is the main structure for the block assembly service. It implements the `UnimplementedBlockAssemblyAPIServer` and contains various components for managing block assembly, storage, and communication with other services.

### BlockAssembler

```go
type BlockAssembler struct {
    logger                 ulogger.Logger
    utxoStore              utxo.Store
    subtreeStore           blob.Store
    blockchainClient       blockchain.ClientI
    subtreeProcessor       *subtreeprocessor.SubtreeProcessor
    miningCandidateCh      chan chan *miningCandidateResponse
    bestBlockHeader        atomic.Pointer[model.BlockHeader]
    bestBlockHeight        atomic.Uint32
    currentChain           []*model.BlockHeader
    currentChainMap        map[chainhash.Hash]uint32
    currentChainMapIDs     map[uint32]struct{}
    currentChainMapMu      sync.RWMutex
    blockchainSubscriptionCh chan *blockchain.Notification
    chainParams            *chaincfg.Params
    difficultyAdjustment   bool
    currentDifficulty      *model.NBit
    defaultMiningNBits     *model.NBit
    resetCh                chan struct{}
    resetWaitCount         atomic.Int32
    resetWaitTime          atomic.Int32
    currentRunningState    atomic.Value
}
```

The `BlockAssembler` type is responsible for assembling blocks, managing the current chain state, and handling mining candidates.

### SubtreeProcessor

```go
type SubtreeProcessor struct {
    currentItemsPerFile       int
    txChan                    chan *[]txIDAndFee
    getSubtreesChan           chan chan []*util.Subtree
    moveUpBlockChan           chan moveBlockRequest
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
    batcher                   *txIDAndFeeBatch
    queue                     *LockFreeQueue
    removeMap                 *util.SwissMap
    doubleSpendWindowDuration time.Duration
    subtreeStore              blob.Store
    utxoStore                 utxostore.Store
    logger                    ulogger.Logger
    stat                      *gocore.Stat
    currentRunningState       atomic.Value
}
```

The `SubtreeProcessor` type manages the processing of transactions into subtrees, handling chain reorganizations, and maintaining the current state of the block assembly process.

### LockFreeQueue

```go
type LockFreeQueue struct {
    head        *txIDAndFee
    tail        atomic.Pointer[txIDAndFee]
    queueLength atomic.Int64
}
```

The `LockFreeQueue` type represents a lock-free FIFO queue for managing transactions in the block assembly process.

## Functions

### BlockAssembly

#### New

```go
func New(logger ulogger.Logger, txStore blob.Store, utxoStore utxostore.Store, subtreeStore blob.Store, blockchainClient blockchain.ClientI) *BlockAssembly
```

Creates a new instance of the `BlockAssembly` service.

#### Health

```go
func (ba *BlockAssembly) Health(ctx context.Context, checkLiveness bool) (int, string, error)
```

Performs health checks on the block assembly service.

#### HealthGRPC

```go
func (ba *BlockAssembly) HealthGRPC(ctx context.Context, _ *blockassembly_api.EmptyMessage) (*blockassembly_api.HealthResponse, error)
```

Performs a gRPC health check on the block assembly service.

#### Init

```go
func (ba *BlockAssembly) Init(ctx context.Context) (err error)
```

Initializes the block assembly service.

#### Start

```go
func (ba *BlockAssembly) Start(ctx context.Context) (err error)
```

Starts the block assembly service.

#### Stop

```go
func (ba *BlockAssembly) Stop(_ context.Context) error
```

Stops the block assembly service.

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
func (ba *BlockAssembly) GetMiningCandidate(ctx context.Context, _ *blockassembly_api.EmptyMessage) (*model.MiningCandidate, error)
```

Retrieves a mining candidate for block assembly.

#### SubmitMiningSolution

```go
func (ba *BlockAssembly) SubmitMiningSolution(ctx context.Context, req *blockassembly_api.SubmitMiningSolutionRequest) (*blockassembly_api.SubmitMiningSolutionResponse, error)
```

Submits a mining solution for a block.

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

#### MoveUpBlock

```go
func (stp *SubtreeProcessor) MoveUpBlock(block *model.Block) error
```

Moves the subtree processor state up to a new block.

#### Reorg

```go
func (stp *SubtreeProcessor) Reorg(moveDownBlocks []*model.Block, modeUpBlocks []*model.Block) error
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
func (q *LockFreeQueue) enqueue(v *txIDAndFee)
```

Adds a transaction to the queue.

#### dequeue

```go
func (q *LockFreeQueue) dequeue(validFromMillis int64) *txIDAndFee
```

Removes and returns a transaction from the queue.
