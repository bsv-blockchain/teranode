# Blockchain Server Reference Documentation

## Types

### Blockchain

```go
type Blockchain struct {
blockchain_api.UnimplementedBlockchainAPIServer
addBlockChan       chan *blockchain_api.AddBlockRequest
store              blockchain_store.Store
logger             ulogger.Logger
newSubscriptions   chan subscriber
deadSubscriptions  chan subscriber
subscribers        map[subscriber]bool
notifications      chan *blockchain_api.Notification
newBlock           chan struct{}
difficulty         *Difficulty
chainParams        *chaincfg.Params
blockKafkaProducer kafka.KafkaProducerI
stats              *gocore.Stat
finiteStateMachine *fsm.FSM
kafkaHealthURL     *url.URL
}
```

The `Blockchain` type is the main structure for the blockchain server. It implements the `UnimplementedBlockchainAPIServer` and contains various channels and components for managing the blockchain state, subscribers, and notifications.

### subscriber

```go
type subscriber struct {
subscription blockchain_api.BlockchainAPI_SubscribeServer
source       string
done         chan struct{}
}
```

The `subscriber` type represents a subscriber to the blockchain server, containing the subscription server, source, and a done channel.

## Functions

### New

```go
func New(ctx context.Context, logger ulogger.Logger, store blockchain_store.Store) (*Blockchain, error)
```

Creates a new instance of the `Blockchain` server with the provided logger and store.

### Health

```go
func (b *Blockchain) Health(ctx context.Context, checkLiveness bool) (int, string, error)
```

Performs health checks on the blockchain server, including liveness and readiness checks.

### HealthGRPC

```go
func (b *Blockchain) HealthGRPC(ctx context.Context, _ *emptypb.Empty) (*blockchain_api.HealthResponse, error)
```

Performs a gRPC health check on the blockchain server.

### Init

```go
func (b *Blockchain) Init(_ context.Context) error
```

Initializes the blockchain server, setting up the finite state machine.

### Start

```go
func (b *Blockchain) Start(ctx context.Context) error
```

Starts the blockchain server, setting up Kafka producers, HTTP servers, and gRPC servers.

### Stop

```go
func (b *Blockchain) Stop(_ context.Context) error
```

Stops the blockchain server.

### AddBlock

```go
func (b *Blockchain) AddBlock(ctx context.Context, request *blockchain_api.AddBlockRequest) (*emptypb.Empty, error)
```

Adds a new block to the blockchain.

### GetBlock

```go
func (b *Blockchain) GetBlock(ctx context.Context, request *blockchain_api.GetBlockRequest) (*blockchain_api.GetBlockResponse, error)
```

Retrieves a block from the blockchain by its hash.

### GetBlocks

```go
func (b *Blockchain) GetBlocks(ctx context.Context, req *blockchain_api.GetBlocksRequest) (*blockchain_api.GetBlocksResponse, error)
```

Retrieves multiple blocks from the blockchain starting from a specific hash.

### GetBlockByHeight

```go
func (b *Blockchain) GetBlockByHeight(ctx context.Context, request *blockchain_api.GetBlockByHeightRequest) (*blockchain_api.GetBlockResponse, error)
```

Retrieves a block from the blockchain by its height.

### GetBlockStats

```go
func (b *Blockchain) GetBlockStats(ctx context.Context, _ *emptypb.Empty) (*model.BlockStats, error)
```

Retrieves statistics about the blockchain.

### GetBlockGraphData

```go
func (b *Blockchain) GetBlockGraphData(ctx context.Context, req *blockchain_api.GetBlockGraphDataRequest) (*model.BlockDataPoints, error)
```

Retrieves graph data for blocks over a specified time period.

### GetLastNBlocks

```go
func (b *Blockchain) GetLastNBlocks(ctx context.Context, request *blockchain_api.GetLastNBlocksRequest) (*blockchain_api.GetLastNBlocksResponse, error)
```

Retrieves the last N blocks from the blockchain.

### GetSuitableBlock

```go
func (b *Blockchain) GetSuitableBlock(ctx context.Context, request *blockchain_api.GetSuitableBlockRequest) (*blockchain_api.GetSuitableBlockResponse, error)
```

Retrieves a suitable block for mining based on the provided hash.

### GetNextWorkRequired

```go
func (b *Blockchain) GetNextWorkRequired(ctx context.Context, request *blockchain_api.GetNextWorkRequiredRequest) (*blockchain_api.GetNextWorkRequiredResponse, error)
```

Calculates the next work required for mining a block.

### GetHashOfAncestorBlock

```go
func (b *Blockchain) GetHashOfAncestorBlock(ctx context.Context, request *blockchain_api.GetHashOfAncestorBlockRequest) (*blockchain_api.GetHashOfAncestorBlockResponse, error)
```

Retrieves the hash of an ancestor block at a specified depth.

### GetBlockExists

```go
func (b *Blockchain) GetBlockExists(ctx context.Context, request *blockchain_api.GetBlockRequest) (*blockchain_api.GetBlockExistsResponse, error)
```

Checks if a block exists in the blockchain.

### GetBestBlockHeader

```go
func (b *Blockchain) GetBestBlockHeader(ctx context.Context, empty *emptypb.Empty) (*blockchain_api.GetBlockHeaderResponse, error)
```

Retrieves the header of the best (most recent) block in the blockchain.

### CheckBlockIsInCurrentChain

```go
func (b *Blockchain) CheckBlockIsInCurrentChain(ctx context.Context, req *blockchain_api.CheckBlockIsCurrentChainRequest) (*blockchain_api.CheckBlockIsCurrentChainResponse, error)
```

Checks if specified blocks are part of the current blockchain.

### GetBlockHeader

```go
func (b *Blockchain) GetBlockHeader(ctx context.Context, req *blockchain_api.GetBlockHeaderRequest) (*blockchain_api.GetBlockHeaderResponse, error)
```

Retrieves a block header by its hash.

### GetBlockHeaders

```go
func (b *Blockchain) GetBlockHeaders(ctx context.Context, req *blockchain_api.GetBlockHeadersRequest) (*blockchain_api.GetBlockHeadersResponse, error)
```

Retrieves multiple block headers starting from a specific hash.

### GetBlockHeadersFromHeight

```go
func (b *Blockchain) GetBlockHeadersFromHeight(ctx context.Context, req *blockchain_api.GetBlockHeadersFromHeightRequest) (*blockchain_api.GetBlockHeadersFromHeightResponse, error)
```

Retrieves block headers starting from a specific height.

### Subscribe

```go
func (b *Blockchain) Subscribe(req *blockchain_api.SubscribeRequest, sub blockchain_api.BlockchainAPI_SubscribeServer) error
```

Handles subscriptions to the blockchain server.

### GetState

```go
func (b *Blockchain) GetState(ctx context.Context, req *blockchain_api.GetStateRequest) (*blockchain_api.StateResponse, error)
```

Retrieves the state for a given key.

### SetState

```go
func (b *Blockchain) SetState(ctx context.Context, req *blockchain_api.SetStateRequest) (*emptypb.Empty, error)
```

Sets the state for a given key.

### GetBlockHeaderIDs

```go
func (b *Blockchain) GetBlockHeaderIDs(ctx context.Context, request *blockchain_api.GetBlockHeadersRequest) (*blockchain_api.GetBlockHeaderIDsResponse, error)
```

Retrieves block header IDs starting from a specific hash.

### InvalidateBlock

```go
func (b *Blockchain) InvalidateBlock(ctx context.Context, request *blockchain_api.InvalidateBlockRequest) (*emptypb.Empty, error)
```

Invalidates a block in the blockchain.

### RevalidateBlock

```go
func (b *Blockchain) RevalidateBlock(ctx context.Context, request *blockchain_api.RevalidateBlockRequest) (*emptypb.Empty, error)
```

Revalidates a previously invalidated block.

### SendNotification

```go
func (b *Blockchain) SendNotification(ctx context.Context, req *blockchain_api.Notification) (*emptypb.Empty, error)
```

Sends a notification to subscribers.

### SetBlockMinedSet

```go
func (b *Blockchain) SetBlockMinedSet(ctx context.Context, req *blockchain_api.SetBlockMinedSetRequest) (*emptypb.Empty, error)
```

Marks a block as mined.

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

### WaitForFSMtoTransitionToGivenState

```go
func (b *Blockchain) WaitForFSMtoTransitionToGivenState(_ context.Context, targetState blockchain_api.FSMStateType) error
```

Waits for the FSM to transition to a given state.

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
