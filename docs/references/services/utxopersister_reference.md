# UTXO Persister Service Reference Documentation

## Overview

The UTXO (Unspent Transaction Output) Persister Service is responsible for managing and persisting UTXO data in a blockchain system. It handles the creation, storage, and retrieval of UTXO sets, additions, and deletions.

## Core Components

### Server

The `Server` struct is the main component of the UTXO Persister Service.

```go
type Server struct {
    logger           ulogger.Logger
    settings         *settings.Settings      // Configuration settings
    blockchainClient blockchain.ClientI      // Interface to blockchain operations
    blockchainStore  blockchain_store.Store  // Direct blockchain storage access
    blockStore       blob.Store             // Binary large object storage
    stats            *gocore.Stat           // Performance statistics
    lastHeight       uint32                 // Last processed block height
    mu               sync.Mutex             // Synchronization mutex
    running          bool                   // Server operational state
    triggerCh        chan string           // Channel for processing triggers
}
```

#### Constructor

The service provides two initialization paths depending on your architectural needs:

```go
// Standard initialization with blockchain client
func New(
    ctx context.Context,
    logger ulogger.Logger,
    tSettings *settings.Settings,
    blockStore blob.Store,
    blockchainClient blockchain.ClientI,
) *Server

// Direct initialization bypassing blockchain client
func NewDirect(
    ctx context.Context,
    logger ulogger.Logger,
    tSettings *settings.Settings,
    blockStore blob.Store,
    blockchainStore blockchain_store.Store,
) (*Server, error)
```

Creates a new `Server` instance with the provided dependencies.

#### Methods

- `Health(ctx context.Context, checkLiveness bool) (int, string, error)`: Performs health checks on the server and its dependencies.
- `Init(ctx context.Context) error`: Initializes the server by reading the last processed height.
- `Start(ctx context.Context) error`: Starts the server, including blockchain event subscription and processing.
- `Stop(ctx context.Context) error`: Stops the server (currently a no-op).
- `trigger(ctx context.Context, source string) error`: Triggers the processing of the next block.
- `processNextBlock(ctx context.Context) (time.Duration, error)`: Processes the next block in the chain.

### UTXOSet

The `UTXOSet` struct represents a set of UTXOs for a specific block.

```go
type UTXOSet struct {
ctx             context.Context
logger          ulogger.Logger
blockHash       chainhash.Hash
blockHeight     uint32
additionsStorer *filestorer.FileStorer
deletionsStorer *filestorer.FileStorer
store           blob.Store
deletionsMap    map[[32]byte][]uint32
txCount         uint64
utxoCount       uint64
deletionCount   uint64
stats           *gocore.Stat
mu              sync.Mutex
}
```

#### Constructors

- `NewUTXOSet(ctx context.Context, logger ulogger.Logger, store blob.Store, blockHash *chainhash.Hash, blockHeight uint32) (*UTXOSet, error)`
- `GetUTXOSet(ctx context.Context, logger ulogger.Logger, store blob.Store, blockHash *chainhash.Hash) (*UTXOSet, error)`
- `GetUTXOSetWithExistCheck(ctx context.Context, logger ulogger.Logger, store blob.Store, blockHash *chainhash.Hash) (*UTXOSet, error)`

#### Methods

- `ProcessTx(tx *bt.Tx) error`: Processes a transaction, adding new UTXOs and marking spent ones for deletion.
- `Close() error`: Finalizes the UTXO set, writing footers and closing file storers.
- `GetUTXOAdditionsReader(ctx context.Context) (io.ReadCloser, error)`: Returns a reader for UTXO additions.
- `GetUTXODeletionsReader(ctx context.Context) (io.ReadCloser, error)`: Returns a reader for UTXO deletions.
- `CreateUTXOSet(ctx context.Context, c *consolidator) error`: Creates a new UTXO set based on the previous set and current additions/deletions.
- `GetUTXOSetReader(optionalBlockHash ...*chainhash.Hash) (io.ReadCloser, error)`: Returns a reader for the UTXO set.

### UTXO

The `UTXO` struct represents an individual Unspent Transaction Output.

```go
type UTXO struct {
Index  uint32
Value  uint64
Script []byte
}
```

#### Methods

- `Bytes() []byte`: Serializes the UTXO to bytes.
- `String() string`: Returns a string representation of the UTXO.

### UTXOWrapper

The `UTXOWrapper` struct wraps multiple UTXOs for a single transaction.

```go
type UTXOWrapper struct {
TxID     chainhash.Hash
Height   uint32
Coinbase bool
UTXOs    []*UTXO
}
```

#### Methods

- `Bytes() []byte`: Serializes the UTXOWrapper to bytes.
- `DeletionBytes(index uint32) [36]byte`: Returns the bytes representation for UTXO deletion.
- `String() string`: Returns a string representation of the UTXOWrapper.

### UTXODeletion

The `UTXODeletion` struct represents a UTXO to be deleted.

```go
type UTXODeletion struct {
TxID  chainhash.Hash
Index uint32
}
```

#### Methods

- `DeletionBytes() []byte`: Returns the bytes representation of the UTXODeletion.
- `String() string`: Returns a string representation of the UTXODeletion.

## File Formats

The UTXO Persister Service uses three types of files:

1. UTXO Additions (extension: `utxo-additions`)

```
- Transaction ID (32 bytes)
- Encoded Height and Coinbase (4 bytes)
  * Height << 1 | coinbase_flag
- Output Count (4 bytes)
For each output:
  - Index (4 bytes)
  - Value (8 bytes)
  - Script Length (4 bytes)
  - Script (variable)
```

2. UTXO Deletions (extension: `utxo-deletions`)

```
- Transaction ID (32 bytes)
- Output Index (4 bytes)
```

3. UTXO Set (extension: `utxo-set`)

```
- EOF Marker (32 zero bytes)
- Transaction Count (8 bytes)
- UTXO/Deletion Count (8 bytes)
```


Each file type has a specific header format and contains serialized UTXO data.

## Helper Functions

- `BuildHeaderBytes(magic string, blockHash *chainhash.Hash, blockHeight uint32, previousBlockHash ...*chainhash.Hash) ([]byte, error)`: Builds the header bytes for UTXO files.
- `GetHeaderFromReader(reader io.Reader) (string, *chainhash.Hash, uint32, error)`: Reads and parses the header from a reader.
- `GetUTXOSetHeaderFromReader(reader io.Reader) (string, *chainhash.Hash, uint32, *chainhash.Hash, error)`: Reads and parses the UTXO set header from a reader.
- `filterUTXOs(utxos []*UTXO, deletions map[UTXODeletion]struct{}, txID *chainhash.Hash) []*UTXO`: Filters UTXOs based on deletions.
- `PadUTXOsWithNil(utxos []*UTXO) []*UTXO`: Pads a UTXO slice with nil values.
- `UnpadSlice[T any](padded []*T) []*T`: Removes nil values from a padded slice.
