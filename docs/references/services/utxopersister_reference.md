# UTXO Persister Service Reference Documentation

## Overview

The UTXO (Unspent Transaction Output) Persister Service is responsible for managing and persisting UTXO data in a blockchain system. It creates and maintains up-to-date UTXO file sets for each block in the Teranode blockchain. Its primary function is to process the output of the Block Persister service (utxo-additions and utxo-deletions) and generate complete UTXO set files. The resulting UTXO set files can be exported and used to initialize the UTXO store in new Teranode instances, enabling fast synchronization of new nodes.

## Core Components

### Server

The `Server` struct is the main component of the UTXO Persister Service. It coordinates the processing of blocks, extraction of UTXOs, and their persistent storage. The server maintains the state of the UTXO set by handling additions and deletions as new blocks are processed.

```go
type Server struct {
    // logger provides logging functionality
    logger ulogger.Logger

    // settings contains configuration settings
    settings *settings.Settings

    // blockchainClient provides access to blockchain operations
    blockchainClient blockchain.ClientI

    // blockchainStore provides access to blockchain storage
    blockchainStore blockchain_store.Store

    // blockStore provides access to block storage
    blockStore blob.Store

    // stats tracks operational statistics
    stats *gocore.Stat

    // lastHeight stores the last processed block height
    lastHeight uint32

    // mu provides mutex locking for thread safety
    mu sync.Mutex

    // running indicates if the server is currently processing
    running bool

    // triggerCh is used to trigger processing operations
    triggerCh chan string
}
```

#### Constructors

The service provides two initialization paths depending on your architectural needs:

```go
// Standard initialization with blockchain client
// This constructor leverages the blockchain client interface for operations,
// which is suitable for distributed setups where components may be on different machines.
func New(
    ctx context.Context,
    logger ulogger.Logger,
    tSettings *settings.Settings,
    blockStore blob.Store,
    blockchainClient blockchain.ClientI,
) *Server

// Direct initialization bypassing blockchain client
// This constructor provides direct access to the blockchain store without using the client interface.
// This can be more efficient when the server is running in the same process as the blockchain store.
func NewDirect(
    ctx context.Context,
    logger ulogger.Logger,
    tSettings *settings.Settings,
    blockStore blob.Store,
    blockchainStore blockchain_store.Store,
) (*Server, error)
```

#### Methods

- `Health(ctx context.Context, checkLiveness bool) (int, string, error)`: Checks the health status of the server and its dependencies. It performs both liveness and readiness checks based on the checkLiveness parameter. Liveness checks verify that the service is running, while readiness checks also verify that dependencies like blockchain client, FSM, blockchain store, and block store are available.

- `Init(ctx context.Context) error`: Initializes the server by reading the last processed height from persistent storage. It retrieves the last block height that was successfully processed and sets it as the starting point for future processing.

- `Start(ctx context.Context, readyCh chan<- struct{}) error`: Starts the server's processing operations. It sets up notification channels, subscribes to blockchain updates, and starts the main processing loop. The loop processes blocks as they are received through the notification channel or on a timer. The readyCh is closed when initialization is complete to signal readiness.

- `Stop(ctx context.Context) error`: Stops the server's processing operations and performs necessary cleanup for graceful termination.

- `trigger(ctx context.Context, source string) error`: Initiates the processing of the next block. It ensures only one processing operation runs at a time and handles various trigger sources. The source parameter indicates what triggered the processing (blockchain, timer, startup, etc.).

- `processNextBlock(ctx context.Context) (time.Duration, error)`: Processes the next block in the chain. It retrieves the next block based on the last processed height, extracts UTXOs, and persists them to storage. It also updates the last processed height. Returns a duration to wait before processing the next block and any error encountered.

- `readLastHeight(ctx context.Context) (uint32, error)`: Reads the last processed block height from storage. It attempts to retrieve the height from a special file in the block store.

- `writeLastHeight(ctx context.Context, height uint32) error`: Writes the current block height to storage for recovery purposes. This allows the server to resume processing from the correct point after a restart.

- `verifyLastSet(ctx context.Context, hash *chainhash.Hash) error`: Verifies the integrity of the last UTXO set. It checks if the UTXO set for the given hash exists and has valid header and footer. This verification ensures that the UTXO set was completely written and is not corrupted.

### UTXOSet

The `UTXOSet` struct represents a set of UTXOs for a specific block. It provides functionality to track, store, and retrieve UTXOs. UTXOSet handles both additions (new outputs) and deletions (spent outputs) for maintaining the UTXO state.

```go
type UTXOSet struct {
    // ctx provides context for operations
    ctx context.Context

    // logger provides logging functionality
    logger ulogger.Logger

    // settings contains configuration settings
    settings *settings.Settings

    // blockHash contains the hash of the current block
    blockHash chainhash.Hash

    // blockHeight represents the height of the current block
    blockHeight uint32

    // additionsStorer manages storage of UTXO additions
    additionsStorer *filestorer.FileStorer

    // deletionsStorer manages storage of UTXO deletions
    deletionsStorer *filestorer.FileStorer

    // store provides blob storage functionality
    store blob.Store

    // deletionsMap tracks deletions by transaction ID
    deletionsMap map[[32]byte][]uint32

    // txCount tracks the number of transactions
    txCount uint64

    // utxoCount tracks the number of UTXOs
    utxoCount uint64

    // deletionCount tracks the number of deletions
    deletionCount uint64

    // stats tracks operational statistics
    stats *gocore.Stat

    // mu provides mutex locking for thread safety
    mu sync.Mutex
}
```

#### Constructors

- `NewUTXOSet(ctx context.Context, logger ulogger.Logger, tSettings *settings.Settings, store blob.Store, blockHash *chainhash.Hash, blockHeight uint32) (*UTXOSet, error)`: Creates a new UTXOSet instance for managing UTXOs. It initializes the additions and deletions storers and writes their headers. This constructor prepares the storage for a new block's UTXO additions and deletions.

- `GetUTXOSet(ctx context.Context, logger ulogger.Logger, tSettings *settings.Settings, store blob.Store, blockHash *chainhash.Hash) (*UTXOSet, error)`: Creates a new UTXOSet instance for an existing block. It's used for reading existing UTXO data rather than creating new data. This method doesn't check if the UTXO set actually exists.

- `GetUTXOSetWithExistCheck(ctx context.Context, logger ulogger.Logger, tSettings *settings.Settings, store blob.Store, blockHash *chainhash.Hash) (*UTXOSet, bool, error)`: Creates a new UTXOSet instance and checks if it exists. Unlike GetUTXOSet, this method also verifies if the UTXO set for the specified block exists in storage. Returns the UTXOSet, a boolean indicating existence, and any error encountered.

#### Methods

- `ProcessTx(tx *bt.Tx) error`: Processes a transaction, updating the UTXO set accordingly. It handles both spending (deletions) and creation (additions) of UTXOs. This method ensures thread-safety with a mutex lock.

- `delete(deletion *UTXODeletion) error`: Records a UTXO deletion. It marks a specific UTXO as spent by writing a deletion record.

- `Close() error`: Finalizes the UTXO set by writing footers and closing storers. It writes EOFMarkers and count information to the addition and deletion files.

- `Abort(err error) error`: Aborts the UTXO set without finalizing files. This method should be called when an error occurs during processing to ensure incomplete files are not saved. It calls `Abort()` on both the additions and deletions storers, which removes any temporary files without renaming them to their final names.

- `GetUTXOAdditionsReader(ctx context.Context) (io.ReadCloser, error)`: Returns a reader for accessing UTXO additions. It creates a reader for the additions file of the current block or a specified block.

- `GetUTXODeletionsReader(ctx context.Context) (io.ReadCloser, error)`: Returns a reader for accessing UTXO deletions. It creates a reader for the deletions file of the current block or a specified block.

- `CreateUTXOSet(ctx context.Context, c *consolidator) error`: Generates the UTXO set for the current block, using the previous block's UTXO set and applying additions and deletions from the consolidator. It handles the creation, serialization, and storage of the complete UTXO state after processing a block or range of blocks.

- `GetUTXOSetReader(optionalBlockHash ...*chainhash.Hash) (io.ReadCloser, error)`: Returns a reader for accessing the UTXO set. It creates a reader for the UTXO set file of the current block or a specified block.

### UTXO

The `UTXO` struct represents an individual Unspent Transaction Output. It contains the essential components of a Bitcoin transaction output: index, value, and script.

```go
type UTXO struct {
    // Index represents the output index in the transaction
    Index uint32

    // Value represents the amount in satoshis
    Value uint64

    // Script contains the locking script
    Script []byte
}
```

#### Methods

- `Bytes() []byte`: Returns the byte representation of the UTXO. The serialized format includes the index (4 bytes), value (8 bytes), script length (4 bytes), and the script itself. All integers are serialized in little-endian format.

- `String() string`: Returns a string representation of the UTXO. It includes the output index, value in satoshis, and a hexadecimal representation of the script.

#### Factory Methods

- `NewUTXOFromReader(r io.Reader) (*UTXO, error)`: Creates a new UTXO from the provided reader. It deserializes a UTXO by reading the index, value, script length, and script bytes.

### UTXOWrapper

The `UTXOWrapper` struct wraps multiple UTXOs for a single transaction. It encapsulates a transaction ID, block height, coinbase flag, and a collection of UTXOs that belong to a single transaction.

```go
type UTXOWrapper struct {
    // TxID contains the transaction ID
    TxID chainhash.Hash

    // Height represents the block height
    Height uint32

    // Coinbase indicates if this is a coinbase transaction
    Coinbase bool

    // UTXOs contains the unspent transaction outputs
    UTXOs []*UTXO

    // UTXOTotalValue is the total value of all UTXOs in this wrapper
    // This field is not serialized but can be used for quick access to the total value
    // without needing to sum the individual UTXOs each time.
    UTXOTotalValue uint64

    // UTXOCount is the number of UTXOs in this wrapper
    // This field is not serialized but can be used for quick access to the count
    // without needing to compute len(UTXOs) each time.
    UTXOCount int

    // Reusable buffer for reading fixed-size fields
    // not concurrently accessed
    b8             [8]byte
    b16            [16]byte
    reusableScript []byte
}
```

#### Methods

- `Bytes() []byte`: Returns the byte representation of the UTXOWrapper. The serialized format includes the transaction ID, encoded height/coinbase flag, number of UTXOs, and the serialized UTXOs themselves.

- `DeletionBytes(index uint32) [36]byte`: Returns the bytes representation for deletion of a specific output. It creates a fixed-size array containing the transaction ID and the output index.

- `String() string`: Returns a string representation of the UTXOWrapper. The string includes the transaction ID, height, coinbase status, number of outputs, and a formatted representation of each UTXO.

#### Factory Methods

- `NewUTXOWrapperFromReader(ctx context.Context, r io.Reader) (*UTXOWrapper, error)`: Creates a new UTXOWrapper from the provided reader. It deserializes the UTXOWrapper data from a byte stream, checking for EOF markers and properly decoding the height, coinbase flag, and UTXOs.

- `NewUTXOWrapperFromBytes(b []byte) (*UTXOWrapper, error)`: Creates a new UTXOWrapper from the provided bytes. It's a convenience wrapper around NewUTXOWrapperFromReader that uses a bytes.Reader.

### EOFMarker

The package defines an `EOFMarker` which is a byte slice of 32 zero bytes used to identify the end of a data stream when reading UTXOs from a file. When serializing UTXO data to storage, this marker is placed at the end to signify a complete and valid write. When reading, encountering this marker indicates that all UTXOs have been successfully read.

```go
var EOFMarker = make([]byte, 32) // 32 zero bytes
```

### UTXODeletion

The `UTXODeletion` struct represents a UTXO to be deleted.

```go
type UTXODeletion struct {
    // TxID contains the transaction ID of the UTXO to delete
    TxID chainhash.Hash
    // Index represents the output index to delete
    Index uint32
}
```

#### Methods

- `DeletionBytes() []byte`: Returns the bytes representation of the UTXODeletion, used when marking a UTXO as spent.
- `String() string`: Returns a string representation of the UTXODeletion for debugging purposes.

### BlockIndex

The `BlockIndex` struct contains metadata about a block for UTXO set validation and linking.

```go
type BlockIndex struct {
    // Hash contains the block hash
    Hash *chainhash.Hash

    // Height represents the block height
    Height uint32

    // TxCount represents the number of transactions
    TxCount uint64

    // BlockHeader contains the block header information
    BlockHeader *model.BlockHeader
}
```

#### Methods

- `Serialise(writer io.Writer) error`: Writes the BlockIndex data to the provided writer for persistent storage.
- `String() string`: Returns a string representation of the BlockIndex including hash, height, and transaction count.

#### Factory Methods

- `NewUTXOHeaderFromReader(reader io.Reader) (*BlockIndex, error)`: Creates a new BlockIndex from the provided reader by deserializing block metadata.

### Consolidator

The `consolidator` manages the consolidation of UTXO additions and deletions across multiple blocks to create accurate UTXO sets.

```go
type consolidator struct {
    logger              ulogger.Logger
    settings            *settings.Settings
    blockchainStore     headerIfc
    blockchainClient    headerIfc
    blockStore          blob.Store
    insertCounter       uint64
    additions           map[UTXODeletion]*Addition
    deletions           map[UTXODeletion]struct{}
    firstBlockHeight    uint32
    lastBlockHeight     uint32
    // Additional fields for block tracking
}
```

#### Methods

- `NewConsolidator(logger, settings, blockchainStore, blockchainClient, blockStore, previousBlockHash) *consolidator`: Creates a new consolidator instance.
- `ConsolidateBlockRange(ctx context.Context, startBlock, endBlock uint32) error`: Consolidates UTXOs across the specified block range, resolving conflicts when outputs are created and spent within the range.
- `getSortedUTXOWrappers() []*UTXOWrapper`: Returns deterministically sorted UTXO wrappers for output.

## File Formats

The UTXO Persister service uses specific binary file formats for storing UTXO data:

### UTXO Additions (extension: `utxo-additions`)

```text
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

### UTXO Deletions (extension: `utxo-deletions`)

```text
- Transaction ID (32 bytes)
- Output Index (4 bytes)
```

### UTXO Set (extension: `utxo-set`)

```text
- EOF Marker (32 zero bytes)
- Transaction Count (8 bytes)
- UTXO/Deletion Count (8 bytes)
```

Each file type has a specific header format and contains serialized UTXO data.

## Helper Functions

- `BuildHeaderBytes(magic string, blockHash *chainhash.Hash, blockHeight uint32, previousBlockHash ...*chainhash.Hash) ([]byte, error)`: Builds the header bytes for UTXO files.
- `GetHeaderFromReader(reader io.Reader) (string, *chainhash.Hash, uint32, error)`: Reads and parses the header from a reader.
- `GetUTXOSetHeaderFromReader(reader io.Reader) (string, *chainhash.Hash, uint32, *chainhash.Hash, error)`: Reads and parses the UTXO set header from a reader.
- `GetFooter(r io.Reader) (uint64, uint64, error)`: Retrieves transaction and UTXO counts from the footer of a UTXO file. Requires a seekable reader and reads the last 16 bytes containing transaction count and UTXO count.
- `filterUTXOs(utxos []*UTXO, deletions map[UTXODeletion]struct{}, txID *chainhash.Hash) []*UTXO`: Filters out UTXOs that are present in the deletions map. It removes any UTXOs that have been spent (present in the deletions map) from the provided list.
- `PadUTXOsWithNil(utxos []*UTXO) []*UTXO`: Pads a slice of UTXOs with nil values to match their indices. It creates a new slice with nil values at positions where no UTXO exists, ensuring that UTXOs are at positions matching their output index.
- `UnpadSlice[T any](padded []*T) []*T`: Removes nil values from a padded slice. It creates a new slice containing only the non-nil elements from the input slice.
- `checkMagic(r io.Reader, magic string) error`: Verifies the magic number in a file header. It reads and validates that the magic identifier in the header matches the expected value.

## Error Handling

### UTXOSet Error Recovery

The `UTXOSet` uses `FileStorer` for streaming writes of UTXO additions and deletions. To prevent incomplete files from being saved when errors occur during processing, callers should use the success-flag pattern:

```go
utxoDiff, err := utxopersister.NewUTXOSet(ctx, logger, settings, store, blockHash, height)
if err != nil {
    return err
}

var writeSucceeded bool
defer func() {
    if writeSucceeded {
        utxoDiff.Close()  // Finalizes files with footers
    } else {
        utxoDiff.Abort(err)  // Removes temp files
    }
}()

// Process transactions...
for _, tx := range txs {
    if err := utxoDiff.ProcessTx(tx); err != nil {
        return err  // Abort will be called via defer
    }
}

writeSucceeded = true
return nil
```

### CreateUTXOSet Error Recovery

The `CreateUTXOSet` method, which generates complete UTXO set files, also implements the success-flag pattern internally. If any error occurs while writing the UTXO set, the temporary file is automatically cleaned up and no incomplete file is left in storage.

### FileStorer Abort Mechanism

The underlying `FileStorer` provides an `Abort()` method that:

1. Closes the `io.PipeWriter` with an error via `CloseWithError()`
2. Signals to the blob store's `SetFromReader` that the write failed
3. Triggers cleanup of the temporary file
4. Ensures no incomplete UTXO data is persisted
