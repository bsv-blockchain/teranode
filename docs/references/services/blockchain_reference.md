# Blockchain Server Reference Documentation

## Types

### Blockchain

```go
type Blockchain struct {
    blockchain_api.UnimplementedBlockchainAPIServer
    addBlockChan                  chan *blockchain_api.AddBlockRequest // Channel for adding blocks
    store                         blockchain_store.Store               // Storage interface for blockchain data
    logger                        ulogger.Logger                       // Logger instance
    settings                      *settings.Settings                   // Configuration settings
    newSubscriptions              chan subscriber                      // Channel for new subscriptions
    deadSubscriptions             chan subscriber                      // Channel for ended subscriptions
    subscribers                   map[subscriber]bool                  // Active subscribers map
    subscribersMu                 sync.RWMutex                         // Mutex for subscribers map
    notifications                 chan *blockchain_api.Notification    // Channel for notifications
    newBlock                      chan struct{}                        // Channel signaling new block events
    difficulty                    *Difficulty                          // Difficulty calculation instance
    blocksFinalKafkaAsyncProducer kafka.KafkaAsyncProducerI            // Kafka producer for final blocks
    kafkaChan                     chan *kafka.Message                  // Channel for Kafka messages
    stats                         *gocore.Stat                         // Statistics tracking
    finiteStateMachine            *fsm.FSM                             // FSM for blockchain state
    stateChangeTimestamp          time.Time                            // Timestamp of last state change
    AppCtx                        context.Context                      // Application context
    localTestStartState           string                               // Initial state for testing
    subscriptionManagerReady      atomic.Bool                          // Flag indicating subscription manager is ready
    batchTokens                   map[string]*blobDeletionBatchToken   // Active batch tokens for blob deletion
    batchTokensMu                 sync.RWMutex                         // Mutex for batch tokens map
}
```

The `Blockchain` type is the main structure for the blockchain server. It implements the `UnimplementedBlockchainAPIServer` and contains various channels and components for managing the blockchain state, subscribers, and notifications. It uses a finite state machine (FSM) to manage its operational states and provides resilience across service restarts.

### subscriber

```go
type subscriber struct {
    subscription blockchain_api.BlockchainAPI_SubscribeServer // The gRPC subscription server
    source       string                                       // Source identifier of the subscription
    done         chan struct{}                                // Channel to signal when subscription is done
}
```

The `subscriber` type represents a subscriber to the blockchain server, encapsulating the connection to a client interested in blockchain events and providing a mechanism for sending notifications about new blocks, state changes, and other blockchain events.

## Core Functions

### New

```go
func New(ctx context.Context, logger ulogger.Logger, tSettings *settings.Settings, store blockchain_store.Store, blocksFinalKafkaAsyncProducer kafka.KafkaAsyncProducerI, localTestStartFromState ...string) (*Blockchain, error)
```

Creates a new instance of the `Blockchain` server with the provided dependencies. This constructor initializes the core blockchain service with all required components and sets up internal channels for communication between different parts of the service. The optional `localTestStartFromState` parameter allows initializing the blockchain service in a specific FSM state for testing purposes.

### Health

```go
func (b *Blockchain) Health(ctx context.Context, checkLiveness bool) (int, string, error)
```

Performs health checks on the blockchain server. When `checkLiveness` is true, it only verifies the internal service state (used to determine if the service needs to be restarted). When `checkLiveness` is false, it verifies both the service and its dependencies are ready to accept requests.

### HealthGRPC

```go
func (b *Blockchain) HealthGRPC(ctx context.Context, _ *emptypb.Empty) (*blockchain_api.HealthResponse, error)
```

Provides health check information via gRPC, exposing the readiness health check functionality through the gRPC API. This method wraps the `Health` method to provide a standardized gRPC response format.

### Init

```go
func (b *Blockchain) Init(ctx context.Context) error
```

Initializes the blockchain service, setting up the finite state machine (FSM) that governs the service's operational states. It handles three initialization scenarios: test mode, new deployment, and normal operation where it restores the previously persisted state from storage.

### Start

```go
func (b *Blockchain) Start(ctx context.Context, readyCh chan<- struct{}) error
```

Starts the blockchain service operations, initializing and launching all core components: Kafka producer, subscription management, HTTP server for administrative endpoints, and gRPC server for client API access. It uses a synchronized approach to ensure the service is fully operational before signaling readiness.

### Stop

```go
func (b *Blockchain) Stop(_ context.Context) error
```

Gracefully stops the blockchain service, allowing for proper resource cleanup and state persistence before termination.

## Block Management Functions

### AddBlock

```go
func (b *Blockchain) AddBlock(ctx context.Context, request *blockchain_api.AddBlockRequest) (*emptypb.Empty, error)
```

Processes a request to add a new block to the blockchain. This method handles the full lifecycle of adding a new block: validating and parsing the incoming block data, persisting the validated block with configurable options, updating block metadata, publishing the finalized block to Kafka, and notifying subscribers.

The method supports functional options through the request's option fields:

- `OptionMinedSet`: Marks the block as mined when set to true
- `OptionSubtreesSet`: Marks the block's subtrees as processed when set to true
- `OptionInvalid`: Marks the block as invalid when set to true (useful for tracking invalid blocks during catchup)
- `OptionID`: Allows specifying a custom block ID (useful for quick validation with pre-allocated IDs)

Example usage with options:

```go
// Adding a block with pre-allocated ID and marked as mined
request := &blockchain_api.AddBlockRequest{
    Header:           blockHeader,
    CoinbaseTx:       coinbaseTxBytes,
    SubtreeHashes:    subtreeHashBytes,
    TransactionCount: txCount,
    SizeInBytes:      blockSize,
    PeerId:           peerID,
    OptionMinedSet:   true,
    OptionID:         preAllocatedID,
}
```

### GetBlock

```go
func (b *Blockchain) GetBlock(ctx context.Context, request *blockchain_api.GetBlockRequest) (*blockchain_api.GetBlockResponse, error)
```

Retrieves a block from the blockchain by its hash. It validates the requested block hash format, retrieves the block data from storage, and returns the complete block data in API response format.

### GetBlocks

```go
func (b *Blockchain) GetBlocks(ctx context.Context, req *blockchain_api.GetBlocksRequest) (*blockchain_api.GetBlocksResponse, error)
```

Retrieves multiple blocks from the blockchain starting from a specific hash, limiting the number of blocks returned based on the request.

### GetBlockByHeight

```go
func (b *Blockchain) GetBlockByHeight(ctx context.Context, request *blockchain_api.GetBlockByHeightRequest) (*blockchain_api.GetBlockResponse, error)
```

Retrieves a block from the blockchain at a specific height. It fetches the block hash at the requested height and then retrieves the complete block data.

### GetBlockByID

```go
func (b *Blockchain) GetBlockByID(ctx context.Context, request *blockchain_api.GetBlockByIDRequest) (*blockchain_api.GetBlockResponse, error)
```

Retrieves a block from the blockchain by its unique ID. It maps the ID to the corresponding block hash and then retrieves the complete block data.

### GetBlockStats

```go
func (b *Blockchain) GetBlockStats(ctx context.Context, _ *emptypb.Empty) (*model.BlockStats, error)
```

Retrieves statistical information about the blockchain, including block count, transaction count, and other metrics useful for monitoring and analysis.

### GetBlockGraphData

```go
func (b *Blockchain) GetBlockGraphData(ctx context.Context, req *blockchain_api.GetBlockGraphDataRequest) (*model.BlockDataPoints, error)
```

Retrieves data points for blockchain visualization over a specified time period, useful for creating charts and graphs of blockchain activity.

### GetLastNBlocks

```go
func (b *Blockchain) GetLastNBlocks(ctx context.Context, request *blockchain_api.GetLastNBlocksRequest) (*blockchain_api.GetLastNBlocksResponse, error)
```

Retrieves the most recent N blocks from the blockchain, ordered by block height in descending order (newest first).

### GetLastNInvalidBlocks

```go
func (b *Blockchain) GetLastNInvalidBlocks(ctx context.Context, request *blockchain_api.GetLastNInvalidBlocksRequest) (*blockchain_api.GetLastNInvalidBlocksResponse, error)
```

Retrieves the most recent N blocks that have been marked as invalid, useful for monitoring and debugging chain reorganizations or consensus issues.

### GetSuitableBlock

```go
func (b *Blockchain) GetSuitableBlock(ctx context.Context, request *blockchain_api.GetSuitableBlockRequest) (*blockchain_api.GetSuitableBlockResponse, error)
```

Finds a suitable block for mining purposes based on the provided hash, typically used by mining software to determine which block to build upon.

### GetBlockExists

```go
func (b *Blockchain) GetBlockExists(ctx context.Context, request *blockchain_api.GetBlockRequest) (*blockchain_api.GetBlockExistsResponse, error)
```

Checks if a block with the given hash exists in the blockchain, without returning the full block data.

## Block Header Functions

### GetBestBlockHeader

```go
func (b *Blockchain) GetBestBlockHeader(ctx context.Context, empty *emptypb.Empty) (*blockchain_api.GetBlockHeaderResponse, error)
```

Retrieves the header of the current best (most recent) block in the blockchain, which represents the tip of the main chain.

### CheckBlockIsInCurrentChain

```go
func (b *Blockchain) CheckBlockIsInCurrentChain(ctx context.Context, req *blockchain_api.CheckBlockIsCurrentChainRequest) (*blockchain_api.CheckBlockIsCurrentChainResponse, error)
```

Verifies if specified blocks are part of the current main chain, useful for determining if blocks have been orphaned or remain in the active chain.

### GetBlockHeader

```go
func (b *Blockchain) GetBlockHeader(ctx context.Context, req *blockchain_api.GetBlockHeaderRequest) (*blockchain_api.GetBlockHeaderResponse, error)
```

Retrieves the header of a specific block in the blockchain by its hash, without retrieving the full block data.

### GetBlockHeaders

```go
func (b *Blockchain) GetBlockHeaders(ctx context.Context, request *blockchain_api.GetBlockHeadersRequest) (*blockchain_api.GetBlockHeadersResponse, error)
```

Retrieves multiple block headers starting from a specific hash, limiting the number of headers returned based on the request.

### GetBlockHeadersToCommonAncestor

```go
func (b *Blockchain) GetBlockHeadersToCommonAncestor(ctx context.Context, req *blockchain_api.GetBlockHeadersToCommonAncestorRequest) (*blockchain_api.GetBlockHeadersResponse, error)
```

Retrieves block headers to find a common ancestor between two chains, typically used during chain synchronization and reorganization.

### GetBlockHeadersFromTill

```go
func (b *Blockchain) GetBlockHeadersFromTill(ctx context.Context, req *blockchain_api.GetBlockHeadersFromTillRequest) (*blockchain_api.GetBlockHeadersResponse, error)
```

Retrieves block headers between two specified blocks, useful for filling gaps in blockchain data or analyzing specific ranges.

### GetBlockHeadersFromHeight

```go
func (b *Blockchain) GetBlockHeadersFromHeight(ctx context.Context, req *blockchain_api.GetBlockHeadersFromHeightRequest) (*blockchain_api.GetBlockHeadersFromHeightResponse, error)
```

Retrieves block headers starting from a specific height, allowing clients to efficiently fetch headers based on block height rather than hash.

### GetBlocksByHeight

```go
func (b *Blockchain) GetBlocksByHeight(ctx context.Context, req *blockchain_api.GetBlocksByHeightRequest) (*blockchain_api.GetBlocksByHeightResponse, error)
```

Retrieves full blocks within a specified height range. This method implements the gRPC service endpoint for fetching complete blocks between two heights in a single efficient operation. It delegates to the blockchain store's GetBlocksByHeight method and serializes the results for gRPC transmission.

### FindBlocksContainingSubtree

```go
func (b *Blockchain) FindBlocksContainingSubtree(ctx context.Context, req *blockchain_api.FindBlocksContainingSubtreeRequest) (*blockchain_api.FindBlocksContainingSubtreeResponse, error)
```

Finds blocks that contain a specific subtree hash. This method searches through the blockchain to locate all blocks that include the specified subtree hash in their merkle tree structure.

### GetBlockHeadersByHeight

```go
func (b *Blockchain) GetBlockHeadersByHeight(ctx context.Context, req *blockchain_api.GetBlockHeadersByHeightRequest) (*blockchain_api.GetBlockHeadersByHeightResponse, error)
```

Retrieves block headers between two specified heights, providing an efficient way to fetch a range of headers for analysis or synchronization.

### GetBlockHeaderIDs

```go
func (b *Blockchain) GetBlockHeaderIDs(ctx context.Context, request *blockchain_api.GetBlockHeadersRequest) (*blockchain_api.GetBlockHeaderIDsResponse, error)
```

Retrieves block header IDs starting from a specific hash, returning only the identifiers rather than the full header data for efficiency.

## Mining and Difficulty Functions

### GetNextWorkRequired

```go
func (b *Blockchain) GetNextWorkRequired(ctx context.Context, request *blockchain_api.GetNextWorkRequiredRequest) (*blockchain_api.GetNextWorkRequiredResponse, error)
```

Calculates the required proof of work difficulty for the next block based on the difficulty adjustment algorithm, used by miners to determine the target difficulty.

### GetHashOfAncestorBlock

```go
func (b *Blockchain) GetHashOfAncestorBlock(ctx context.Context, request *blockchain_api.GetHashOfAncestorBlockRequest) (*blockchain_api.GetHashOfAncestorBlockResponse, error)
```

Retrieves the hash of an ancestor block at a specified depth from a given block, useful for difficulty calculations and chain traversal.

### GetBlockIsMined

```go
func (b *Blockchain) GetBlockIsMined(ctx context.Context, req *blockchain_api.GetBlockIsMinedRequest) (*blockchain_api.GetBlockIsMinedResponse, error)
```

Checks if a block has been marked as mined in the blockchain, which indicates that the block has been fully processed by the mining subsystem.

### SetBlockMinedSet

```go
func (b *Blockchain) SetBlockMinedSet(ctx context.Context, req *blockchain_api.SetBlockMinedSetRequest) (*emptypb.Empty, error)
```

Marks a block as mined in the blockchain, updating its status to indicate completion of the mining process.

### GetBlocksMinedNotSet

```go
func (b *Blockchain) GetBlocksMinedNotSet(ctx context.Context, _ *emptypb.Empty) (*blockchain_api.GetBlocksMinedNotSetResponse, error)
```

Retrieves blocks that have not been marked as mined.

### SetBlockSubtreesSet

```go
func (b *Blockchain) SetBlockSubtreesSet(ctx context.Context, req *blockchain_api.SetBlockSubtreesSetRequest) (*emptypb.Empty, error)
```

Marks a block's subtrees as set.

### GetBlocksSubtreesNotSet

```go
func (b *Blockchain) GetBlocksSubtreesNotSet(ctx context.Context, _ *emptypb.Empty) (*blockchain_api.GetBlocksSubtreesNotSetResponse, error)
```

Retrieves blocks whose subtrees have not been set.

## Subscription and Notification Functions

### Subscribe

```go
func (b *Blockchain) Subscribe(req *blockchain_api.SubscribeRequest, sub blockchain_api.BlockchainAPI_SubscribeServer) error
```

Handles subscription requests to blockchain notifications. Establishes a persistent gRPC streaming connection for real-time blockchain event notifications.

### SendNotification

```go
func (b *Blockchain) SendNotification(ctx context.Context, req *blockchain_api.Notification) (*emptypb.Empty, error)
```

Broadcasts a notification to all subscribers.

### ReportPeerFailure

```go
func (b *Blockchain) ReportPeerFailure(ctx context.Context, req *blockchain_api.ReportPeerFailureRequest) (*emptypb.Empty, error)
```

Handles reports of peer download failures and broadcasts notifications to subscribers. This method is used by other services to report when a peer fails to provide requested data, allowing the blockchain service to notify subscribers (including P2P services) about peer reliability issues.

## State Management Functions

### GetState

```go
func (b *Blockchain) GetState(ctx context.Context, req *blockchain_api.GetStateRequest) (*blockchain_api.StateResponse, error)
```

Retrieves a value from the blockchain state storage by its key.

### SetState

```go
func (b *Blockchain) SetState(ctx context.Context, req *blockchain_api.SetStateRequest) (*emptypb.Empty, error)
```

Stores a value in the blockchain state storage with the specified key.

## Block Validation Functions

### InvalidateBlock

```go
func (b *Blockchain) InvalidateBlock(ctx context.Context, request *blockchain_api.InvalidateBlockRequest) (*blockchain_api.InvalidateBlockResponse, error)
```

Marks a block as invalid in the blockchain.

### RevalidateBlock

```go
func (b *Blockchain) RevalidateBlock(ctx context.Context, request *blockchain_api.RevalidateBlockRequest) (*emptypb.Empty, error)
```

Restores a previously invalidated block.

## Additional Block Functions

### GetChainTips

```go
func (b *Blockchain) GetChainTips(ctx context.Context, _ *emptypb.Empty) (*blockchain_api.GetChainTipsResponse, error)
```

Retrieves information about all known tips in the block tree.

### GetLatestBlockHeaderFromBlockLocatorRequest

```go
func (b *Blockchain) GetLatestBlockHeaderFromBlockLocatorRequest(ctx context.Context, request *blockchain_api.GetLatestBlockHeaderFromBlockLocatorRequest) (*blockchain_api.GetBlockHeaderResponse, error)
```

Retrieves the latest block header from a block locator request.

### SetBlockProcessedAt

```go
func (b *Blockchain) SetBlockProcessedAt(ctx context.Context, req *blockchain_api.SetBlockProcessedAtRequest) (*emptypb.Empty, error)
```

Sets or clears the processed_at timestamp for a block. This method updates the timestamp indicating when a block was processed by the system.

### GetBlockHeadersFromOldestRequest

```go
func (b *Blockchain) GetBlockHeadersFromOldestRequest(ctx context.Context, request *blockchain_api.GetBlockHeadersFromOldestRequest) (*blockchain_api.GetBlockHeadersResponse, error)
```

Retrieves block headers from the oldest request.

### GetBlockHeadersFromCommonAncestor

```go
func (b *Blockchain) GetBlockHeadersFromCommonAncestor(ctx context.Context, request *blockchain_api.GetBlockHeadersFromCommonAncestorRequest) (*blockchain_api.GetBlockHeadersResponse, error)
```

Retrieves block headers from a common ancestor.

## Finite State Machine (FSM) Related Functions

### GetStoreFSMState

```go
func (b *Blockchain) GetStoreFSMState(ctx context.Context) (string, error)
```

Retrieves the current FSM state from the store. This method provides access to the persisted FSM state directly from the underlying storage, which may differ from the in-memory state of the service's FSM. This is useful for diagnostics and recovery scenarios.

### ResetFSMS

```go
func (b *Blockchain) ResetFSMS()
```

Resets the finite state machine to nil (used for testing). This method is primarily intended for testing purposes to force a clean state. **Note: This method should only be called in test environments.**

### SetSubscriptionManagerReadyForTesting

```go
func (b *Blockchain) SetSubscriptionManagerReadyForTesting(ready bool)
```

Sets the subscription manager ready flag for testing purposes. This method should only be used in tests to simulate subscription manager readiness. **Note: This is a testing-only method.**

### IsFullyReady

```go
func (b *Blockchain) IsFullyReady(ctx context.Context) (bool, error)
```

Checks if the blockchain service is fully operational. This includes both FSM being in a non-IDLE state and subscription infrastructure being ready. Services should use this method to determine if they can safely proceed with blockchain operations.

### GetFSMCurrentState

```go
func (b *Blockchain) GetFSMCurrentState(ctx context.Context, _ *emptypb.Empty) (*blockchain_api.GetFSMStateResponse, error)
```

Retrieves the current state of the finite state machine.

### WaitForFSMtoTransitionToGivenState (Internal Helper)

```go
func (b *Blockchain) WaitForFSMtoTransitionToGivenState(ctx context.Context, targetState blockchain_api.FSMStateType) error
```

Internal helper that polls until the FSM reaches the target state. The corresponding gRPC endpoint is `WaitFSMToTransitionToGivenState`, which wraps this method and accepts a `WaitFSMToTransitionRequest`.

### WaitUntilFSMTransitionFromIdleState

```go
func (b *Blockchain) WaitUntilFSMTransitionFromIdleState(ctx context.Context, _ *emptypb.Empty) (*emptypb.Empty, error)
```

Waits for the FSM to transition from the IDLE state.

### SendFSMEvent

```go
func (b *Blockchain) SendFSMEvent(ctx context.Context, eventReq *blockchain_api.SendFSMEventRequest) (*blockchain_api.GetFSMStateResponse, error)
```

Sends an event to the finite state machine.

### Run

```go
func (b *Blockchain) Run(ctx context.Context, _ *emptypb.Empty) (*emptypb.Empty, error)
```

Transitions the FSM to the RUNNING state.

### CatchUpBlocks

```go
func (b *Blockchain) CatchUpBlocks(ctx context.Context, _ *emptypb.Empty) (*emptypb.Empty, error)
```

Transitions the FSM to the CATCHINGBLOCKS state.

### LegacySync

```go
func (b *Blockchain) LegacySync(ctx context.Context, _ *emptypb.Empty) (*emptypb.Empty, error)
```

Transitions the FSM to the LEGACYSYNCING state.

### Idle

```go
func (b *Blockchain) Idle(ctx context.Context, _ *emptypb.Empty) (*emptypb.Empty, error)
```

Transitions the FSM to the IDLE state.

## Legacy Endpoints

### GetBlockLocator

```go
func (b *Blockchain) GetBlockLocator(ctx context.Context, req *blockchain_api.GetBlockLocatorRequest) (*blockchain_api.GetBlockLocatorResponse, error)
```

Retrieves a block locator for a given block hash and height.

### LocateBlockHeaders

```go
func (b *Blockchain) LocateBlockHeaders(ctx context.Context, request *blockchain_api.LocateBlockHeadersRequest) (*blockchain_api.LocateBlockHeadersResponse, error)
```

Locates block headers based on a given locator and hash stop.

### GetBestHeightAndTime

```go
func (b *Blockchain) GetBestHeightAndTime(ctx context.Context, _ *emptypb.Empty) (*blockchain_api.GetBestHeightAndTimeResponse, error)
```

Retrieves the best height and median time of the blockchain.

## Block ID Management Functions

### GetNextBlockID

```go
func (b *Blockchain) GetNextBlockID(ctx context.Context, _ *emptypb.Empty) (*blockchain_api.GetNextBlockIDResponse, error)
```

Retrieves the next available block ID for pre-allocation purposes. This method provides atomic ID generation that ensures unique block IDs across concurrent operations. It is particularly useful during quick validation scenarios where blocks need to be assigned IDs before full processing.

The implementation varies by database backend:

- **PostgreSQL**: Uses database sequences for atomic ID generation
- **SQLite**: Implements transaction-based increment for thread safety

Returns:

- `next_block_id`: The next available unique block ID that can be used for block storage

This method is commonly used during:

- Quick validation of checkpointed blocks
- Parallel block processing where IDs need to be reserved in advance
- Recovery scenarios where block IDs need to be coordinated across services

## Block Ancestry

### CheckBlockIsAncestorOfBlock

```go
func (b *Blockchain) CheckBlockIsAncestorOfBlock(ctx context.Context, req *blockchain_api.CheckBlockIsAncestorOfBlockRequest) (*blockchain_api.CheckBlockIsAncestorOfBlockResponse, error)
```

Verifies whether specified blocks are ancestors of a given block. Used for double-spend detection on fork blocks where the check is performed against the fork's ancestor chain.

## Block Persistence Tracking

### SetBlockPersistedAt

```go
func (b *Blockchain) SetBlockPersistedAt(ctx context.Context, req *blockchain_api.SetBlockPersistedAtRequest) (*emptypb.Empty, error)
```

Updates the `persisted_at` timestamp for a block, recording when the block was fully persisted to blob storage.

### GetBlocksNotPersisted

```go
func (b *Blockchain) GetBlocksNotPersisted(ctx context.Context, req *blockchain_api.GetBlocksNotPersistedRequest) (*blockchain_api.GetBlocksNotPersistedResponse, error)
```

Retrieves blocks that have not yet been persisted to blob storage. Used by the Block Persister service to discover blocks that still need processing.

## Blob Deletion Management

These gRPC methods manage scheduled blob deletions as part of the pruning workflow. The blockchain service stores deletion schedules in the database, and the pruner service uses these endpoints to coordinate blob cleanup.

### ScheduleBlobDeletion

```go
func (b *Blockchain) ScheduleBlobDeletion(ctx context.Context, req *blockchain_api.ScheduleBlobDeletionRequest) (*blockchain_api.ScheduleBlobDeletionResponse, error)
```

Schedules a blob for deletion at a specific block height.

### CancelBlobDeletion

```go
func (b *Blockchain) CancelBlobDeletion(ctx context.Context, req *blockchain_api.CancelBlobDeletionRequest) (*blockchain_api.CancelBlobDeletionResponse, error)
```

Cancels a previously scheduled blob deletion.

### ListScheduledDeletions

```go
func (b *Blockchain) ListScheduledDeletions(ctx context.Context, req *blockchain_api.ListScheduledDeletionsRequest) (*blockchain_api.ListScheduledDeletionsResponse, error)
```

Lists all scheduled blob deletions with optional filtering.

### GetPendingBlobDeletions

```go
func (b *Blockchain) GetPendingBlobDeletions(ctx context.Context, req *blockchain_api.GetPendingBlobDeletionsRequest) (*blockchain_api.GetPendingBlobDeletionsResponse, error)
```

Retrieves blob deletions that are ready for processing at a specific height.

### RemoveBlobDeletion

```go
func (b *Blockchain) RemoveBlobDeletion(ctx context.Context, req *blockchain_api.RemoveBlobDeletionRequest) (*emptypb.Empty, error)
```

Removes a blob deletion from the schedule after successful deletion.

### IncrementBlobDeletionRetry

```go
func (b *Blockchain) IncrementBlobDeletionRetry(ctx context.Context, req *blockchain_api.IncrementBlobDeletionRetryRequest) (*blockchain_api.IncrementBlobDeletionRetryResponse, error)
```

Increments the retry counter for a failed blob deletion.

### CompleteBlobDeletions

```go
func (b *Blockchain) CompleteBlobDeletions(ctx context.Context, req *blockchain_api.CompleteBlobDeletionsRequest) (*blockchain_api.CompleteBlobDeletionsResponse, error)
```

Completes multiple blob deletions in a single call. More efficient than calling `RemoveBlobDeletion` multiple times.

### AcquireBlobDeletionBatch

```go
func (b *Blockchain) AcquireBlobDeletionBatch(ctx context.Context, req *blockchain_api.AcquireBlobDeletionBatchRequest) (*blockchain_api.AcquireBlobDeletionBatchResponse, error)
```

Acquires a batch of deletions with locking for processing. Uses `SELECT...FOR UPDATE SKIP LOCKED` to ensure only one pruner instance processes each batch.

### CompleteBlobDeletionBatch

```go
func (b *Blockchain) CompleteBlobDeletionBatch(ctx context.Context, req *blockchain_api.CompleteBlobDeletionBatchRequest) (*emptypb.Empty, error)
```

Completes a previously acquired batch, reporting all results (successes and failures) in a single call.

## Related Documents

- [Blockchain Topic Guide](../../topics/services/blockchain.md)
- [Blockchain Settings](../settings/services/blockchain_settings.md)
- [Blockchain Protobuf Reference](../protobuf_docs/blockchainProto.md)
- [Prometheus Metrics](../prometheusMetrics.md)
