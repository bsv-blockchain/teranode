# Block Persister Service Reference Documentation

## Types

### Server

```go
type Server struct {
    ctx              context.Context
    logger           ulogger.Logger
    settings         *settings.Settings
    blockStore       blob.Store
    subtreeStore     blob.Store
    utxoStore        utxo.Store
    stats            *gocore.Stat
    blockchainClient blockchain.ClientI
    state            *state.State
}
```

The `Server` type is the main structure for the Block Persister Service. It contains components for managing stores, blockchain interactions, and state management.

## Functions

### Server Management

#### New
```go
func New(ctx context.Context, logger ulogger.Logger, tSettings *settings.Settings, blockStore blob.Store, subtreeStore blob.Store, utxoStore utxo.Store, blockchainClient blockchain.ClientI, opts ...func(*Server)) *Server
```

Creates a new instance of the `Server` with optional configuration functions.

#### WithSetInitialState
```go
func WithSetInitialState(height uint32, hash *chainhash.Hash) func(*Server)
```

Optional configuration function that sets the initial state of the block persister server.

#### Health
```go
func (u *Server) Health(ctx context.Context, checkLiveness bool) (int, string, error)
```

Performs health checks on the Block Persister Service, including:
- Blockchain client and FSM status
- Block store availability
- Subtree store status
- UTXO store health

#### Init
```go
func (u *Server) Init(ctx context.Context) (err error)
```

Initializes the Block Persister Service and Prometheus metrics.

#### Start
```go
func (u *Server) Start(ctx context.Context) error
```

Starts the Block Persister Service, including:
- HTTP blob store server (if configured)
- Block processing loop
- State management

### Block Processing

#### getNextBlockToProcess
```go
func (u *Server) getNextBlockToProcess(ctx context.Context) (*model.Block, error)
```

Retrieves the next block to process based on:
- Last persisted block height
- Block persister persist age configuration
- Current best block height

### Subtree Processing

#### ProcessSubtree
```go
func (u *Server) ProcessSubtree(pCtx context.Context, subtreeHash chainhash.Hash, coinbaseTx *bt.Tx, utxoDiff *utxopersister.UTXOSet) error
```

Processes a subtree by:
1. Retrieving subtree data from store
2. Deserializing subtree data
3. Processing transaction metadata
4. Writing transactions to storage
5. Updating UTXO set

#### WriteTxs
```go
func WriteTxs(ctx context.Context, logger ulogger.Logger, writer *filestorer.FileStorer, txMetaSlice []*meta.Data, utxoDiff *utxopersister.UTXOSet) error
```

Writes transactions to storage and processes them for UTXO updates.

## Configuration

The service uses settings from the `settings.Settings` structure:

### Block Settings
- `Block.StateFile`: File path for state storage
- `Block.PersisterHTTPListenAddress`: HTTP listener address
- `Block.BlockStore`: Block store URL
- `Block.BlockPersisterPersistAge`: Age threshold for block persistence
- `Block.BlockPersisterPersistSleep`: Sleep duration between processing attempts
- `Block.BatchMissingTransactions`: Whether to batch missing transaction requests

## State Management

The service maintains persistence state through the `state.State` component:
- Tracks last persisted block height
- Manages block hash records
- Provides atomic state updates

## Error Handling

The service implements comprehensive error handling:
- Storage errors trigger retries after delay
- Processing errors are logged with context
- Configuration errors prevent service startup
- State management errors trigger recovery procedures

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
1. Check for next block to process based on persistence age
2. Retrieve block data if available
3. Persist block data to storage
4. Update state with successful persistence
5. Sleep if no blocks available or on error

### Subtree Processing
1. Retrieve subtree data from store
2. Process transaction metadata
3. Write transactions to storage
4. Update UTXO set
5. Handle errors with appropriate recovery

## Health Checks

The service implements two types of health checks:

### Liveness Check
- Basic service health validation
- No dependency checks
- Quick response for kubernetes probes

### Readiness Check
- Full dependency validation
- Store availability checks
- Blockchain client status
- FSM state validation
