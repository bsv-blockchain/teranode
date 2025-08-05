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

Processes a request to add a new block to the blockchain. This method handles the full lifecycle of adding a new block: validating and parsing the incoming block data, persisting the validated block, updating block metadata, publishing the finalized block to Kafka, and notifying subscribers.

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

## Finite State Machine (FSM) Related Functions

### GetFSMCurrentState

```go
func (b *Blockchain) GetFSMCurrentState(ctx context.Context, _ *emptypb.Empty) (*blockchain_api.GetFSMStateResponse, error)
```

Retrieves the current state of the finite state machine.

### WaitForFSMtoTransitionToGivenState (Internal Method)

```go
func (b *Blockchain) WaitForFSMtoTransitionToGivenState(_ context.Context, targetState blockchain_api.FSMStateType) error
```

Waits for the FSM to transition to a given state. **Note: This is an internal helper method and is not exposed as a gRPC endpoint.**

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

### CatchUpTransactions

```go
func (b *Blockchain) CatchUpTransactions(ctx context.Context, _ *emptypb.Empty) (*emptypb.Empty, error)
```

Transitions the FSM to the CATCHINGTXS state.

### Restore

```go
func (b *Blockchain) Restore(ctx context.Context, _ *emptypb.Empty) (*emptypb.Empty, error)
```

Transitions the FSM to the RESTORING state.

### LegacySync

```go
func (b *Blockchain) LegacySync(ctx context.Context, _ *emptypb.Empty) (*emptypb.Empty, error)
```

Transitions the FSM to the LEGACYSYNCING state.

### Unavailable

```go
func (b *Blockchain) Unavailable(ctx context.Context, _ *emptypb.Empty) (*emptypb.Empty, error)
```

Transitions the FSM to the RESOURCE_UNAVAILABLE state.

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
