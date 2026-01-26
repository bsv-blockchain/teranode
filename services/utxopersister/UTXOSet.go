// Package utxopersister creates and maintains up-to-date Unspent Transaction Output (UTXO) file sets
// for each block in the Teranode blockchain. Its primary function is to process the output of the
// Block Persister service (utxo-additions and utxo-deletions) and generate complete UTXO set files.
// The resulting UTXO set files can be exported and used to initialize the UTXO store in new Teranode instances.
//
// UTXOSet.go implements the core UTXO set management functionality, including:
// - Creating and initializing UTXO set data structures
// - Processing transactions to extract and track UTXOs
// - Managing additions (new outputs) and deletions (spent outputs)
// - Serializing and deserializing UTXO data to/from storage
// - Constructing complete UTXO sets by consolidating previous sets with new changes
//
// The UTXO set files use a structured binary format with headers, data records, and footers
// to ensure data integrity and efficient processing. This file implements both the data structures
// and the algorithms needed to maintain an accurate UTXO state across blockchain updates.
package utxopersister

import (
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"io"
	"sort"
	"sync"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	safeconversion "github.com/bsv-blockchain/go-safe-conversion"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/services/utxopersister/filestorer"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/stores/blob"
	"github.com/bsv-blockchain/teranode/stores/blob/options"
	blockchain_store "github.com/bsv-blockchain/teranode/stores/blockchain"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/bytesize"
	"github.com/bsv-blockchain/teranode/util/tracing"
	"github.com/ordishs/gocore"
)

// This type is responsible for reading and writing UTXO additions, deletions and sets to and from files.

// The file format is as follows:
// - the header
// - the records
// - the footer

// For UTXO additions and the utxoset, the records are serialized UTXOs in following format:
// - 32 bytes - txID
// - 4 bytes - encoded height and coinbase flag
// - 4 bytes - number of outputs
// - and then for each output:
//   - 4 bytes - index
//	 - 8 bytes - value
//	 - 4 bytes - length of script
//	 - n bytes - script

// UTXOSet manages a set of Unspent Transaction Outputs.
// It provides functionality to track, store, and retrieve UTXOs for a specific block.
// UTXOSet handles both additions (new outputs) and deletions (spent outputs) for maintaining the UTXO state.
//
// UTXOSet is the primary data structure for managing the blockchain's UTXO state. It maintains:
// - A reference to the current block being processed
// - File storers for both additions and deletions
// - In-memory tracking of UTXO operations for efficient processing
// - Statistics for monitoring performance and resource usage
//
// UTXOSet implements thread-safe operations through mutex locking and provides
// methods for transaction processing, serialization, deserialization, and UTXO set creation.
// It serves as the core component for maintaining an accurate representation of all
// unspent transaction outputs at any given block height.
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

// NewUTXOSet creates a new UTXOSet instance for managing UTXOs.
// It initializes the additions and deletions storers and writes their headers.
// This constructor prepares the storage for a new block's UTXO additions and deletions.
// Returns the initialized UTXOSet and any error encountered during setup.
//
// Parameters:
// - ctx: Context for controlling the initialization process
// - logger: Logger interface for recording operational events and errors
// - tSettings: Configuration settings that control behavior
// - store: Blob store instance for persisting UTXO data
// - blockHash: Pointer to the hash of the block being processed
// - blockHeight: Height of the block being processed
//
// Returns:
// - *UTXOSet: The initialized UTXOSet instance
// - error: Any error encountered during setup
//
// This method performs several key initialization steps:
// 1. Creates file storers for both additions and deletions files
// 2. Builds appropriate headers for each file type with magic numbers and block information
// 3. Writes the headers to the respective files
// 4. Initializes the UTXOSet with the necessary references and empty tracking structures
//
// The resulting UTXOSet is ready to process transactions and maintain UTXO state for the block.
func NewUTXOSet(ctx context.Context, logger ulogger.Logger, tSettings *settings.Settings, store blob.Store, blockHash *chainhash.Hash, blockHeight uint32) (*UTXOSet, error) {
	// Now, write the block file
	logger.Infof("[BlockPersister] Persisting utxo additions and deletions for block %s", blockHash.String())

	additionsStorer, err := filestorer.NewFileStorer(ctx, logger, tSettings, store, blockHash[:], fileformat.FileTypeUtxoAdditions)
	if err != nil {
		return nil, errors.NewStorageError("error creating additions file", err)
	}

	deletionsStorer, err := filestorer.NewFileStorer(ctx, logger, tSettings, store, blockHash[:], fileformat.FileTypeUtxoDeletions)
	if err != nil {
		// Abort the additions storer to prevent incomplete file
		additionsStorer.Abort(errors.NewStorageError("error creating deletions file", err))
		return nil, errors.NewStorageError("error creating deletions file", err)
	}

	if err := binary.Write(additionsStorer, binary.LittleEndian, blockHash[:]); err != nil {
		additionsStorer.Abort(err)
		deletionsStorer.Abort(err)
		return nil, errors.NewStorageError("error writing block hash to additions header", err)
	}

	if err := binary.Write(additionsStorer, binary.LittleEndian, blockHeight); err != nil {
		additionsStorer.Abort(err)
		deletionsStorer.Abort(err)
		return nil, errors.NewStorageError("error writing block height to additions header", err)
	}

	if err := binary.Write(deletionsStorer, binary.LittleEndian, blockHash[:]); err != nil {
		additionsStorer.Abort(err)
		deletionsStorer.Abort(err)
		return nil, errors.NewStorageError("error writing block hash to deletions header", err)
	}

	if err := binary.Write(deletionsStorer, binary.LittleEndian, blockHeight); err != nil {
		additionsStorer.Abort(err)
		deletionsStorer.Abort(err)
		return nil, errors.NewStorageError("error writing block height to deletions header", err)
	}

	return &UTXOSet{
		ctx:             ctx,
		logger:          logger,
		settings:        tSettings,
		blockHash:       *blockHash,
		blockHeight:     blockHeight,
		additionsStorer: additionsStorer,
		deletionsStorer: deletionsStorer,
		store:           store,
		stats:           gocore.NewStat("utxopersister"),
	}, nil
}

// GetUTXOSet creates a new UTXOSet instance for an existing block.
// It's used for reading existing UTXO data rather than creating new data.
// This method doesn't check if the UTXO set actually exists.
// Returns the initialized UTXOSet and any error encountered.
//
// Parameters:
// - ctx: Context for controlling the operation
// - logger: Logger interface for recording operational events and errors
// - tSettings: Configuration settings that control behavior
// - store: Blob store instance for accessing persisted UTXO data
// - blockHash: Pointer to the hash of the block whose UTXO set is being accessed
//
// Returns:
// - *UTXOSet: The initialized UTXOSet instance
// - error: Any error encountered during setup
//
// This method is simpler than NewUTXOSet as it doesn't create any new files or write headers.
// It only initializes a UTXOSet instance with the necessary references to access existing data.
// Use this method when you need to read from an existing UTXO set but don't need to
// verify its existence first. For verification, use GetUTXOSetWithExistCheck instead.
func GetUTXOSet(ctx context.Context, logger ulogger.Logger, tSettings *settings.Settings, store blob.Store, blockHash *chainhash.Hash) (*UTXOSet, error) {
	return &UTXOSet{
		ctx:       ctx,
		logger:    logger,
		settings:  tSettings,
		blockHash: *blockHash,
		store:     store,
		stats:     gocore.NewStat("utxopersister"),
	}, nil
}

// GetUTXOSetWithExistCheck creates a new UTXOSet instance and checks if it exists.
// Unlike GetUTXOSet, this method also verifies if the UTXO set for the specified block exists in storage.
// Returns the UTXOSet, a boolean indicating existence, and any error encountered.
//
// Parameters:
// - ctx: Context for controlling the operation
// - logger: Logger interface for recording operational events and errors
// - tSettings: Configuration settings that control behavior
// - store: Blob store instance for accessing persisted UTXO data
// - blockHash: Pointer to the hash of the block whose UTXO set is being accessed
//
// Returns:
// - *UTXOSet: The initialized UTXOSet instance
// - bool: True if the UTXO set exists, false otherwise
// - error: Any error encountered during setup or verification
//
// This method combines the functionality of GetUTXOSet with an existence check.
// It initializes a UTXOSet instance and then verifies whether a UTXO set file
// exists for the specified block hash. This is useful when you need to determine
// if a UTXO set needs to be created before attempting to read from it.
func GetUTXOSetWithExistCheck(ctx context.Context, logger ulogger.Logger, tSettings *settings.Settings, store blob.Store, blockHash *chainhash.Hash) (*UTXOSet, bool, error) {
	us := &UTXOSet{
		ctx:       ctx,
		logger:    logger,
		settings:  tSettings,
		blockHash: *blockHash,
		store:     store,
		stats:     gocore.NewStat("utxopersister"),
	}

	// Check to see if the utxo-set already exists
	exists, err := store.Exists(ctx, blockHash[:], fileformat.FileTypeUtxoSet)
	if err != nil {
		return nil, false, errors.NewStorageError("error checking if %v.%s exists", blockHash, fileformat.FileTypeUtxoSet, err)
	}

	return us, exists, nil
}

// ProcessTx processes a transaction, updating the UTXO set accordingly.
// It handles both spending (deletions) and creation (additions) of UTXOs.
// This method ensures thread-safety with a mutex lock.
// Returns an error if processing fails.
//
// Parameters:
// - tx: Pointer to the transaction to process
//
// Returns:
// - error: Any error encountered during processing
//
// This method performs two main operations:
//  1. Processing inputs: For each input, it creates a UTXODeletion record marking
//     the referenced output as spent and calls the delete method to record it.
//  2. Processing outputs: For all outputs in the transaction, it creates a UTXOWrapper
//     containing the outputs and writes it to the additions file.
//
// The method maintains thread-safety through a mutex lock, ensuring consistent
// state even when processing multiple transactions concurrently. It also
// updates transaction and UTXO count statistics for monitoring purposes.
func (us *UTXOSet) ProcessTx(tx *bt.Tx) error {
	us.mu.Lock()
	defer us.mu.Unlock()

	if !tx.IsCoinbase() {
		for _, input := range tx.Inputs {
			if err := us.delete(&UTXODeletion{*input.PreviousTxIDChainHash(), input.PreviousTxOutIndex}); err != nil {
				return err
			}
		}
	}

	// Create a new UTXOWrapper
	uw := &UTXOWrapper{
		TxID:     *tx.TxIDChainHash(),
		Height:   us.blockHeight,
		Coinbase: tx.IsCoinbase(),
		UTXOs:    make([]*UTXO, 0),
	}

	for i, output := range tx.Outputs {
		if utxo.ShouldStoreOutputAsUTXO(tx.IsCoinbase(), output, us.blockHeight) {
			iUint32, err := safeconversion.IntToUint32(i)
			if err != nil {
				return err
			}

			uw.UTXOs = append(uw.UTXOs, &UTXO{
				iUint32,
				output.Satoshis,
				*output.LockingScript,
			})

			us.utxoCount++
		}
	}

	// Write the UTXOWrapper to the file
	_, err := us.additionsStorer.Write(uw.Bytes())
	if err != nil {
		return err
	}

	us.txCount++

	return nil
}

// delete records a UTXO deletion.
// It marks a specific UTXO as spent by writing a deletion record.
// This is called when an input references a previous output.
// Returns an error if the deletion cannot be recorded.
func (us *UTXOSet) delete(deletion *UTXODeletion) error {
	if _, err := us.deletionsStorer.Write(deletion.DeletionBytes()); err != nil {
		return err
	}

	us.deletionCount++

	return nil
}

// Close finalizes the UTXO set by writing footers and closing storers.
// It writes EOFMarkers and count information to the addition and deletion files.
// This ensures that files are properly terminated and contains metadata about their contents.
// Returns an error if closing operations fail.
func (us *UTXOSet) Close() error {
	// TODO Write the number of transactions and utxos to a meta file
	// us.txCount
	// us.utxoCount

	if err := us.additionsStorer.Close(us.ctx); err != nil {
		return errors.NewStorageError("Error flushing additions writer", err)
	}

	if err := us.deletionsStorer.Close(us.ctx); err != nil {
		return errors.NewStorageError("Error flushing deletions writer:", err)
	}

	us.logger.Infof("[BlockPersister] Persisting utxo additions and deletions for block %s - DONE", us.blockHash.String())

	return nil
}

// Abort aborts both the additions and deletions storers to prevent incomplete files from being finalized.
// This should be called instead of Close() when an error occurs during UTXO processing.
func (us *UTXOSet) Abort(err error) {
	if us.additionsStorer != nil {
		us.additionsStorer.Abort(err)
	}
	if us.deletionsStorer != nil {
		us.deletionsStorer.Abort(err)
	}
}

type readCloserWrapper struct {
	*bufio.Reader
	io.Closer
}

// GetUTXOAdditionsReader returns a reader for accessing UTXO additions.
// It creates a reader for the additions file of the current block or a specified block.
// This reader can be used to iterate through all UTXOs added in the block.
// Returns a ReadCloser interface and any error encountered.
func (us *UTXOSet) GetUTXOAdditionsReader(ctx context.Context) (io.ReadCloser, error) {
	ctx, _, deferFn := tracing.Tracer("utxopersister").Start(ctx, "GetUTXOAdditionsReader",
		tracing.WithDebugLogMessage(us.logger, "[GetUTXOAdditionsReader] called"),
	)
	defer deferFn()

	r, err := us.store.GetIoReader(ctx, us.blockHash[:], fileformat.FileTypeUtxoAdditions, options.WithDeleteAt(0))
	if err != nil {
		return nil, errors.NewStorageError("error getting utxo-additions reader", err)
	}

	// Consume the block hash and block height
	_, err = r.Read(make([]byte, 32))
	if err != nil {
		return nil, errors.NewStorageError("error reading block hash", err)
	}

	_, err = r.Read(make([]byte, 4))
	if err != nil {
		return nil, errors.NewStorageError("error reading block height", err)
	}

	utxopersisterBufferSize := us.settings.Block.UTXOPersisterBufferSize

	bufferSize, err := bytesize.Parse(utxopersisterBufferSize)
	if err != nil {
		us.logger.Warnf("error parsing utxoPersister_buffer_size %q, using default of 4KB", utxopersisterBufferSize)

		bufferSize = 4096
	}

	us.logger.Debugf("Using %s buffer for utxo-additions reader", bufferSize)

	r = &readCloserWrapper{
		Reader: bufio.NewReaderSize(r, bufferSize.Int()),
		Closer: r.(io.Closer),
	}

	return r, nil
}

// GetUTXODeletionsReader returns a reader for accessing UTXO deletions.
// It creates a reader for the deletions file of the current block or a specified block.
// This reader can be used to iterate through all UTXOs deleted (spent) in the block.
// Returns a ReadCloser interface and any error encountered.
func (us *UTXOSet) GetUTXODeletionsReader(ctx context.Context) (io.ReadCloser, error) {
	r, err := us.store.GetIoReader(ctx, us.blockHash[:], fileformat.FileTypeUtxoDeletions, options.WithDeleteAt(0))
	if err != nil {
		return nil, errors.NewStorageError("error getting utxo-deletions reader", err)
	}

	// Consume the block hash and block height
	_, err = r.Read(make([]byte, 32))
	if err != nil {
		return nil, errors.NewStorageError("error reading block hash", err)
	}

	_, err = r.Read(make([]byte, 4))
	if err != nil {
		return nil, errors.NewStorageError("error reading block height", err)
	}

	utxopersisterBufferSize := us.settings.Block.UTXOPersisterBufferSize

	bufferSize, err := bytesize.Parse(utxopersisterBufferSize)
	if err != nil {
		us.logger.Warnf("error parsing utxoPersister_buffer_size %q, using default of 4KB", utxopersisterBufferSize)

		bufferSize = 4096
	}

	us.logger.Debugf("Using %s buffer for utxo-deletions reader", bufferSize)

	r = &readCloserWrapper{
		Reader: bufio.NewReaderSize(r, bufferSize.Int()),
		Closer: r.(io.Closer),
	}

	return r, nil
}

// CreateUTXOSet generates the UTXO set for the current block, using the previous block's UTXO set
// and applying additions and deletions from the consolidator. It handles the creation, serialization,
// and storage of the complete UTXO state after processing a block or range of blocks.
// Returns an error if the operation fails.
//
// Parameters:
// - ctx: Context for controlling the operation
// - c: Pointer to the consolidator containing UTXO additions and deletions
//
// Returns:
// - error: Any error encountered during UTXO set creation
//
// This method performs several key steps:
// 1. Creates and initializes a file storer for the UTXO set output file
// 2. Writes the UTXO set header with magic number and block information
// 3. Processes the previous block's UTXO set, filtering out spent outputs
// 4. Adds new unspent outputs from the current block
// 5. Writes all remaining UTXOs to the UTXO set file
// 6. Finalizes the file with footer information and counts
//
// The method uses error groups to process UTXOs in parallel for better performance,
// with coordinated error handling to ensure data integrity. Tracing is used for
// performance monitoring and diagnostics throughout the operation.
func (us *UTXOSet) CreateUTXOSet(ctx context.Context, c *consolidator) (err error) {
	if us == nil {
		return errors.NewStorageError("UTXOSet is nil")
	}

	createStat := gocore.NewStat("utxopersister.CreateUTXOSet")

	ctx, _, endSpan := tracing.Tracer("utxopersister").Start(ctx, "CreateUTXOSet",
		tracing.WithParentStat(us.stats),
		tracing.WithLogMessage(us.logger, "[CreateUTXOSet] called"),
	)
	defer endSpan()

	us.logger.Infof("[CreateUTXOSet] Creating UTXOSet for block %s height %d", c.lastBlockHash, c.lastBlockHeight)

	if us.store == nil {
		us.logger.Errorf("[CreateUTXOSet] FATAL: store is nil, cannot create UTXO set file")
		return errors.NewStorageError("store is nil, cannot create UTXO set file")
	}

	if c == nil {
		return errors.NewStorageError("consolidator is nil")
	}

	if us.logger == nil {
		return errors.NewStorageError("logger is nil")
	}

	if us.settings == nil {
		return errors.NewStorageError("settings is nil")
	}

	storer, err := filestorer.NewFileStorer(ctx, us.logger, us.settings, us.store, c.lastBlockHash[:], fileformat.FileTypeUtxoSet)
	if err != nil {
		return errors.NewStorageError("error creating utxo-set file", err)
	}

	// Track whether write succeeded to determine whether to close or abort
	var writeSucceeded bool
	defer func() {
		if writeSucceeded {
			// Success - file already closed below
			return
		}
		// Error path - abort to prevent incomplete file from being finalized
		storer.Abort(errors.NewProcessingError("[CreateUTXOSet] write failed for block %s", c.lastBlockHash.String()))
	}()

	_, err = storer.Write(c.lastBlockHash[:])
	if err != nil {
		return errors.NewProcessingError("Couldn't write previous block hash to file", err)
	}

	if err := binary.Write(storer, binary.LittleEndian, c.lastBlockHeight); err != nil {
		return errors.NewProcessingError("Couldn't write previous block height to file", err)
	}

	// With UTXOSets, we also write the previous block hash before we start writing the UTXOs
	_, err = storer.Write(c.previousBlockHash[:])
	if err != nil {
		return errors.NewProcessingError("Couldn't write previous block hash to file", err)
	}

	var (
		readStat   = createStat.NewStat("readTX")
		filterStat = createStat.NewStat("filterUTXOs")
		writeStat  = createStat.NewStat("writeUTXOs")
		ts         = gocore.CurrentTime()
		txCount    uint64
		utxoCount  uint64
	)

	if c.firstPreviousBlockHash.String() != c.settings.ChainCfgParams.GenesisHash.String() {
		// Open the previous UTXOSet for the previous block
		previousUTXOSetReader, err := us.store.GetIoReader(ctx, c.firstPreviousBlockHash[:], fileformat.FileTypeUtxoSet)
		if err != nil {
			return errors.NewStorageError("error getting utxoset reader for previous block %s", c.firstPreviousBlockHash, err)
		}

		utxopersisterBufferSize := us.settings.Block.UTXOPersisterBufferSize

		bufferSize, err := bytesize.Parse(utxopersisterBufferSize)
		if err != nil {
			us.logger.Warnf("error parsing utxoPersister_buffer_size %q, using default of 4KB", utxopersisterBufferSize)

			bufferSize = 4096
		}

		us.logger.Infof("Using %s buffer for previous UTXOSet reader", bufferSize)

		previousUTXOSetReader = &readCloserWrapper{
			Reader: bufio.NewReaderSize(previousUTXOSetReader, bufferSize.Int()),
			Closer: previousUTXOSetReader.(io.Closer),
		}

		defer previousUTXOSetReader.Close()

		previousHeader, err := fileformat.ReadHeader(previousUTXOSetReader)
		if err != nil {
			return errors.NewStorageError("error reading previous utxo-set header", err)
		}

		if previousHeader.FileType() != fileformat.FileTypeUtxoSet {
			return errors.NewStorageError("previous utxo-set header is not a utxo-set header")
		}

	OUTER:
		for {
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
				// Read the next 36 bytes...
				utxoWrapper, err := NewUTXOWrapperFromReader(ctx, previousUTXOSetReader)
				if err != nil {
					if err == io.EOF {
						break OUTER
					}

					return errors.NewStorageError("error reading previous utxo-set (%s.%s) at iteration %d", c.firstPreviousBlockHash.String(), fileformat.FileTypeUtxoSet, txCount, err)
				}

				ts = readStat.AddTime(ts)

				// Filter UTXOs based on the deletions map
				utxoWrapper.UTXOs = filterUTXOs(utxoWrapper.UTXOs, c.deletions, &utxoWrapper.TxID)

				ts = filterStat.AddTime(ts)

				// Only write the UTXOWrapper if there are remaining UTXOs after deletions
				if len(utxoWrapper.UTXOs) > 0 {
					if _, err := storer.Write(utxoWrapper.Bytes()); err != nil {
						return errors.NewStorageError("error writing utxo wrapper", err)
					}

					txCount++

					utxoCount += uint64(len(utxoWrapper.UTXOs))

					ts = writeStat.AddTime(ts)
				}
			}
		}

		us.logger.Infof("Read %d UTXOs from previous block %s", txCount, c.firstPreviousBlockHash)
	}

	sortedAdditions := c.getSortedUTXOWrappers()

	for _, utxoWrapper := range sortedAdditions {
		// Filter UTXOs based on the deletions map
		utxoWrapper.UTXOs = filterUTXOs(utxoWrapper.UTXOs, c.deletions, &utxoWrapper.TxID)

		ts = filterStat.AddTime(ts)

		// Only write the UTXOWrapper if there are remaining UTXOs after deletions
		if len(utxoWrapper.UTXOs) > 0 {
			if _, err := storer.Write(utxoWrapper.Bytes()); err != nil {
				return errors.NewStorageError("error writing utxo wrapper", err)
			}

			txCount++
			utxoCount += uint64(len(utxoWrapper.UTXOs))

			ts = writeStat.AddTime(ts)
		}
	}

	// Write the number of transactions
	b := make([]byte, 8)
	binary.LittleEndian.PutUint64(b, txCount)

	if _, err = storer.Write(b); err != nil {
		return errors.NewStorageError("error writing number of transactions", err)
	}

	// Write the number of utxos
	binary.LittleEndian.PutUint64(b, utxoCount)

	if _, err = storer.Write(b); err != nil {
		return errors.NewStorageError("error writing number of UTXOs", err)
	}

	// Close the storer
	if err = storer.Close(ctx); err != nil {
		return errors.NewStorageError("error flushing utxoset writer", err)
	}

	// Mark as successful so defer doesn't abort
	writeSucceeded = true

	return nil
}

// GetUTXOSetReader returns a reader for accessing the UTXO set.
// It creates a reader for the UTXO set file of the current block or a specified block.
// This reader provides access to the complete state of all unspent transaction outputs at a specific block.
// Optionally accepts a specific block hash to read from.
// Returns a ReadCloser interface and any error encountered.
func (us *UTXOSet) GetUTXOSetReader(optionalBlockHash ...*chainhash.Hash) (io.ReadCloser, error) {
	blockHash := us.blockHash
	if len(optionalBlockHash) > 0 {
		blockHash = *optionalBlockHash[0]
	}

	return us.store.GetIoReader(us.ctx, blockHash[:], fileformat.FileTypeUtxoSet)
}

// filterUTXOs filters out UTXOs that are present in the deletions map.
// It removes any UTXOs that have been spent (present in the deletions map) from the provided list.
// This is used during UTXO set creation to ensure only unspent outputs are included.
// Returns a filtered slice containing only the unspent outputs.
func filterUTXOs(utxos []*UTXO, deletions map[UTXODeletion]struct{}, txID *chainhash.Hash) []*UTXO {
	filteredUTXOs := make([]*UTXO, 0, len(utxos))

	for _, utxo := range utxos {
		outpoint := UTXODeletion{
			TxID:  *txID,
			Index: utxo.Index,
		}

		if _, found := deletions[outpoint]; !found {
			filteredUTXOs = append(filteredUTXOs, utxo)
		}
	}

	return filteredUTXOs
}

// PadUTXOsWithNil pads a slice of UTXOs with nil values to match their indices.
// It creates a new slice with nil values at positions where no UTXO exists,
// ensuring that UTXOs are at positions matching their output index.
// This helps maintain the correct mapping between output indices and their UTXOs.
// Returns the padded slice.
func PadUTXOsWithNil(utxos []*UTXO) []*UTXO {
	// Determine the size of the new slice
	var maxIndex uint32

	for _, utxo := range utxos {
		if utxo.Index > maxIndex {
			maxIndex = utxo.Index
		}
	}

	// Create a slice with nil values of length maxIdx+1
	padded := make([]*UTXO, maxIndex+1)

	// Place each item in its corresponding index position
	for _, utxo := range utxos {
		padded[utxo.Index] = utxo
	}

	return padded
}

// UnpadSlice removes nil values from a padded slice.
// It creates a new slice containing only the non-nil elements from the input slice.
// This is a generic function that works with any type of slice.
// Returns the compacted slice without nil values.
func UnpadSlice[T any](padded []*T) []*T {
	utxos := make([]*T, 0, len(padded))

	for _, utxo := range padded {
		if utxo != nil {
			utxos = append(utxos, utxo)
		}
	}

	return utxos
}

// WriteHeadersToStore writes block headers from the blockchain store to a utxo-headers file.
// This function exports block headers for all blocks from genesis up to the specified tip block.
// It traces back from the tip to genesis to ensure only the active chain blocks are included.
// The headers are written to blob storage with the same format as used by bitcointoutxoset tool.
//
// Parameters:
// - ctx: Context for controlling the operation
// - logger: Logger interface for recording operational events
// - settings: Configuration settings
// - blobStore: Blob store for writing the headers file
// - blockchainStore: Blockchain store for reading block headers
// - tipHash: Hash of the tip block (usually matches the UTXO set block hash)
// - tipHeight: Height of the tip block
//
// Returns:
// - error: Any error encountered during the operation
//
// The file format matches the seeder expectations:
// 1. File header with magic number for FileTypeUtxoHeaders
// 2. Tip block hash (32 bytes)
// 3. Tip block height (4 bytes)
// 4. Serialized BlockIndex entries for each block from genesis to tip
// 5. SHA256 hash file for verification
func WriteHeadersToStore(ctx context.Context, logger ulogger.Logger, settings *settings.Settings, blobStore blob.Store, blockchainStore blockchain_store.Store, tipHash *chainhash.Hash, tipHeight uint32) error {
	logger.Infof("[WriteHeadersToStore] Writing headers for block %s height %d", tipHash.String(), tipHeight)

	allBlocks := make(map[chainhash.Hash]*BlockIndex, tipHeight+1)

	currentHash := tipHash
	var bestBlock *BlockIndex

	for currentHash != nil {
		header, meta, err := blockchainStore.GetBlockHeader(ctx, currentHash)
		if err != nil {
			return errors.NewStorageError("failed to get block header for hash %s", currentHash.String(), err)
		}

		// Try to get the full block to extract the coinbase transaction
		var coinbaseTx *bt.Tx
		block, _, err := blockchainStore.GetBlock(ctx, currentHash)
		if err != nil {
			logger.Warnf("[WriteHeadersToStore] Could not get block %s to extract coinbase tx: %v", currentHash.String(), err)
		} else if block != nil {
			coinbaseTx = block.CoinbaseTx
		}

		blockIndex := &BlockIndex{
			Hash:        currentHash,
			Height:      meta.Height,
			TxCount:     meta.TxCount,
			BlockHeader: header,
			CoinbaseTx:  coinbaseTx,
		}

		allBlocks[*currentHash] = blockIndex

		if bestBlock == nil {
			bestBlock = blockIndex
		}

		if meta.Height == 0 {
			break
		}

		currentHash = header.HashPrevBlock
	}

	blocks := make([]*BlockIndex, 0, len(allBlocks))
	for _, block := range allBlocks {
		blocks = append(blocks, block)
	}

	sort.SliceStable(blocks, func(i, j int) bool {
		return blocks[i].Height < blocks[j].Height
	})

	// Use FileStorer which will automatically use the V2 magic header
	storer, err := filestorer.NewFileStorer(ctx, logger, settings, blobStore, tipHash[:], fileformat.FileTypeUtxoHeaders)
	if err != nil {
		return errors.NewStorageError("error creating utxo-headers file", err)
	}

	hasher := sha256.New()
	multiWriter := io.MultiWriter(storer, hasher)

	if err = binary.Write(multiWriter, binary.LittleEndian, bestBlock.Hash); err != nil {
		return errors.NewProcessingError("couldn't write block hash to file", err)
	}

	if err = binary.Write(multiWriter, binary.LittleEndian, bestBlock.Height); err != nil {
		return errors.NewProcessingError("error writing block height", err)
	}

	var (
		recordCount uint64
		txCount     uint64
	)

	for _, block := range blocks {
		if err = block.Serialise(multiWriter); err != nil {
			return errors.NewProcessingError("couldn't write header to file", err)
		}

		recordCount++
		txCount += block.TxCount
	}

	if err = storer.Close(ctx); err != nil {
		return errors.NewStorageError("error closing utxo-headers file", err)
	}

	logger.Infof("Wrote %d block headers with %d total transactions", recordCount, txCount)

	hashData := fmt.Sprintf("%x  %s\n", hasher.Sum(nil), tipHash.String()+".utxo-headers")

	if err = blobStore.Set(ctx, tipHash[:], fileformat.FileTypeUtxoHeaders+".sha256", []byte(hashData), options.WithAllowOverwrite(true)); err != nil {
		return errors.NewStorageError("error writing hash file", err)
	}

	return nil
}
