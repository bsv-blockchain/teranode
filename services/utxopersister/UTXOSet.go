package utxopersister

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"io"
	"strings"

	"github.com/bitcoin-sv/ubsv/errors"
	"github.com/bitcoin-sv/ubsv/services/utxopersister/filestorer"
	"github.com/bitcoin-sv/ubsv/stores/blob"
	"github.com/bitcoin-sv/ubsv/stores/blob/options"
	"github.com/bitcoin-sv/ubsv/stores/utxo"
	"github.com/bitcoin-sv/ubsv/tracing"
	"github.com/bitcoin-sv/ubsv/ulogger"
	"github.com/bitcoin-sv/ubsv/util/bytesize"
	"github.com/libsv/go-bt/v2"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/ordishs/gocore"
	"golang.org/x/sync/errgroup"
)

// This type is responsible for reading and writing UTXO additions, deletions and sets to and from files.

// The file format is as follows:
// - the header
// - the records
// - the footer

// the header contains at least:
// - an 8-byte magic number to indicate the file type and version: U-A-1.0, U-D-1.0, U-S-1.0 (right padded with 0x00)
// - a 32-byte hash (little endian) of the block that the data is for
// - a 4-byte little endian height of the block
// - for utxo-set files, a 32-byte hash (little endian) of the previous block

// the footer contains at least:
// - an EOF marker of 32 0x00 bytes
// - the number of records in the file (uint64) - always 0 for deletions
// - the number of utxos in the file (uint64)

// For UTXO additions and the utxoset, the records are serialized UTXOs in following format:
// - 32 bytes - txID
// - 4 bytes - encoded height and coinbase flag
// - 4 bytes - number of outputs
// - and then for each output:
//   - 4 bytes - index
//	 - 8 bytes - value
//	 - 4 bytes - length of script
//	 - n bytes - script

const (
	additionsExtension = "utxo-additions"
	deletionsExtension = "utxo-deletions"
	utxosetExtension   = "utxo-set"
)

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
}

func NewUTXOSet(ctx context.Context, logger ulogger.Logger, store blob.Store, blockHash *chainhash.Hash, blockHeight uint32) (*UTXOSet, error) {
	// Now, write the block file
	logger.Infof("[BlockPersister] Persisting utxo additions and deletions for block %s", blockHash.String())

	additionsStorer := filestorer.NewFileStorer(ctx, logger, store, blockHash[:], additionsExtension)
	deletionsStorer := filestorer.NewFileStorer(ctx, logger, store, blockHash[:], deletionsExtension)

	// Write the headers
	additionsHeader, err := BuildHeaderBytes("U-A-1.0", blockHash, blockHeight)
	if err != nil {
		return nil, errors.NewStorageError("error building additions header", err)
	}

	deletionsHeader, err := BuildHeaderBytes("U-D-1.0", blockHash, blockHeight)
	if err != nil {
		return nil, errors.NewStorageError("error building deletions header", err)
	}

	if _, err := additionsStorer.Write(additionsHeader); err != nil {
		return nil, errors.NewStorageError("error writing additions header", err)
	}

	if _, err := deletionsStorer.Write(deletionsHeader); err != nil {
		return nil, errors.NewStorageError("error writing additions header", err)
	}

	return &UTXOSet{
		ctx:             ctx,
		logger:          logger,
		blockHash:       *blockHash,
		blockHeight:     blockHeight,
		additionsStorer: additionsStorer,
		deletionsStorer: deletionsStorer,
		store:           store,
		stats:           gocore.NewStat("utxopersister"),
	}, nil
}

func GetUTXOSet(ctx context.Context, logger ulogger.Logger, store blob.Store, blockHash *chainhash.Hash) (*UTXOSet, error) {
	us := &UTXOSet{
		ctx:       ctx,
		logger:    logger,
		blockHash: *blockHash,
		store:     store,
	}

	// Check to see if the utxo-set already exists
	exists, err := store.Exists(ctx, blockHash[:], options.WithFileExtension(utxosetExtension))
	if err != nil {
		return nil, errors.NewStorageError("error checking if %s.%s exists", blockHash, utxosetExtension, err)
	}

	if exists {
		return nil, nil
	}

	return us, nil
}

func GetUTXOSetWithDeletionsMap(ctx context.Context, logger ulogger.Logger, store blob.Store, blockHash *chainhash.Hash) (*UTXOSet, error) {
	us, err := GetUTXOSet(ctx, logger, store, blockHash)
	if err != nil {
		return nil, err
	}

	deletionsMap, err := us.GetUTXODeletionsMap(ctx)
	if err != nil {
		return nil, err
	}

	us.deletionsMap = deletionsMap

	return us, nil
}

// Build the file header...
// - an 8-byte magic number to indicate the file type and version: U-A-1.0, U-D-1.0, U-S-1.0 (right padded with 0x00)
// - a 32-byte hash (little endian) of the block that the data is for
// - a 4-byte little endian height of the block
func BuildHeaderBytes(magic string, blockHash *chainhash.Hash, blockHeight uint32, previousBlockHash ...*chainhash.Hash) ([]byte, error) {
	if len(magic) > 8 {
		return nil, errors.NewStorageError("magic number is too long")
	}

	size := 44
	if len(previousBlockHash) > 0 {
		size += 32
	}

	b := make([]byte, size)
	copy(b[:8], magic)
	copy(b[8:40], blockHash[:])
	binary.LittleEndian.PutUint32(b[40:44], blockHeight)

	if len(previousBlockHash) > 0 {
		prev := make([]byte, 32)

		if previousBlockHash[0] != nil {
			copy(prev, previousBlockHash[0][:])
		}

		copy(b[44:76], prev)
	}

	return b, nil
}

func GetHeaderFromReader(reader io.Reader) (string, *chainhash.Hash, uint32, error) {
	// - an 8-byte magic number to indicate the file type and version: U-A-1.0, U-D-1.0, U-S-1.0 (right padded with 0x00)
	// - a 32-byte hash (little endian) of the block that the data is for
	// - a 4-byte little endian height of the block
	b := make([]byte, 44)

	if _, err := io.ReadFull(reader, b); err != nil {
		return "", nil, 0, errors.NewStorageError("error reading header", err)
	}

	magic := strings.TrimRight(string(b[:8]), "\x00")

	blockHash, err := chainhash.NewHash(b[8:40])
	if err != nil {
		return "", nil, 0, errors.NewStorageError("error reading block hash", err)
	}

	blockHeight := binary.LittleEndian.Uint32(b[40:44])

	return magic, blockHash, blockHeight, nil
}

func GetUTXOSetHeaderFromReader(reader io.Reader) (string, *chainhash.Hash, uint32, *chainhash.Hash, error) {
	// - an 8-byte magic number to indicate the file type and version: U-A-1.0, U-D-1.0, U-S-1.0 (right padded with 0x00)
	// - a 32-byte hash (little endian) of the block that the data is for
	// - a 4-byte little endian height of the block
	// - a 32-byte hash (little endian) of the previous block
	b := make([]byte, 76)

	if _, err := io.ReadFull(reader, b); err != nil {
		return "", nil, 0, nil, errors.NewStorageError("error reading header", err)
	}

	magic := strings.TrimRight(string(b[:8]), "\x00")

	blockHash, err := chainhash.NewHash(b[8:40])
	if err != nil {
		return "", nil, 0, nil, errors.NewStorageError("error reading block hash", err)
	}

	blockHeight := binary.LittleEndian.Uint32(b[40:44])

	previousBlockHash, err := chainhash.NewHash(b[44:76])
	if err != nil {
		return "", nil, 0, nil, errors.NewStorageError("error reading previous block hash", err)
	}

	return magic, blockHash, blockHeight, previousBlockHash, nil
}

func (us *UTXOSet) ProcessTx(tx *bt.Tx) error {
	if !tx.IsCoinbase() {
		for _, input := range tx.Inputs {
			if err := us.delete(&UTXODeletion{input.PreviousTxIDChainHash(), input.PreviousTxOutIndex}); err != nil {
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
			uw.UTXOs = append(uw.UTXOs, &UTXO{
				uint32(i), // nolint:gosec
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

func (us *UTXOSet) delete(deletion *UTXODeletion) error {
	if _, err := us.deletionsStorer.Write(deletion.DeletionBytes()); err != nil {
		return err
	}

	us.deletionCount++

	return nil
}

func (us *UTXOSet) Close() error {
	g, ctx := errgroup.WithContext(us.ctx)

	g.Go(func() error {
		// Write the EOF marker
		if n, err := us.additionsStorer.Write(EOFMarker); err != nil || n != len(EOFMarker) {
			return errors.NewStorageError("Error writing EOF marker", err)
		}

		// Write the number of transactions
		b := make([]byte, 8)
		binary.LittleEndian.PutUint64(b, us.txCount)

		if n, err := us.additionsStorer.Write(b); err != nil || n != 8 {
			return errors.NewStorageError("Error writing number of transactions", err)
		}

		// Write the number of UTXOs
		binary.LittleEndian.PutUint64(b, us.utxoCount)

		if n, err := us.additionsStorer.Write(b); err != nil || n != 8 {
			return errors.NewStorageError("Error writing number of UTXOs", err)
		}

		if err := us.additionsStorer.Close(ctx); err != nil {
			return errors.NewStorageError("Error flushing additions writer", err)
		}

		return nil
	})

	g.Go(func() error {
		// Write the EOF marker
		if n, err := us.deletionsStorer.Write(EOFMarker); err != nil || n != len(EOFMarker) {
			return errors.NewStorageError("Error writing EOF marker", err)
		}

		// Write the number of transactions - always 0 for deletions
		b := make([]byte, 8)
		if n, err := us.deletionsStorer.Write(b); err != nil || n != 8 {
			return errors.NewStorageError("Error writing 0 tx count", err)
		}

		binary.LittleEndian.PutUint64(b, us.deletionCount)

		if n, err := us.deletionsStorer.Write(b); err != nil || n != 8 {
			return errors.NewStorageError("Error writing number of deletion", err)
		}

		if err := us.deletionsStorer.Close(ctx); err != nil {
			return errors.NewStorageError("Error flushing deletions writer:", err)
		}

		return nil
	})

	if err := g.Wait(); err != nil {
		return err
	}

	us.logger.Infof("[BlockPersister] Persisting utxo additions and deletions for block %s - DONE", us.blockHash.String())

	return nil
}

type readCloserWrapper struct {
	*bufio.Reader
	io.Closer
}

func (us *UTXOSet) GetUTXOAdditionsReader(ctx context.Context) (io.ReadCloser, error) {
	ctx, _, deferFn := tracing.StartTracing(ctx, "GetUTXOAdditionsReader",
		tracing.WithLogMessage(us.logger, "[GetUTXOAdditionsReader] called"),
	)
	defer deferFn()

	r, err := us.store.GetIoReader(ctx, us.blockHash[:], options.WithFileExtension(additionsExtension), options.WithTTL(0))
	if err != nil {
		return nil, errors.NewStorageError("error getting utxo-additions reader", err)
	}

	utxopersisterBufferSize, _ := gocore.Config().Get("utxoPersister_buffer_size", "4KB")

	bufferSize, err := bytesize.Parse(utxopersisterBufferSize)
	if err != nil {
		us.logger.Warnf("error parsing utxoPersister_buffer_size %q, using default of 4KB", utxopersisterBufferSize)

		bufferSize = 4096
	}

	us.logger.Infof("Using %s buffer for utxo-additions reader", bufferSize)

	r = &readCloserWrapper{
		Reader: bufio.NewReaderSize(r, bufferSize.Int()),
		Closer: r.(io.Closer),
	}

	return r, nil
}

func (us *UTXOSet) GetUTXODeletionsReader(ctx context.Context) (io.ReadCloser, error) {
	r, err := us.store.GetIoReader(ctx, us.blockHash[:], options.WithFileExtension(deletionsExtension), options.WithTTL(0))
	if err != nil {
		return nil, errors.NewStorageError("error getting utxo-deletions reader", err)
	}

	utxopersisterBufferSize, _ := gocore.Config().Get("utxoPersister_buffer_size", "4KB")

	bufferSize, err := bytesize.Parse(utxopersisterBufferSize)
	if err != nil {
		us.logger.Warnf("error parsing utxoPersister_buffer_size %q, using default of 4KB", utxopersisterBufferSize)

		bufferSize = 4096
	}

	us.logger.Infof("Using %s buffer for utxo-deletions reader", bufferSize)

	r = &readCloserWrapper{
		Reader: bufio.NewReaderSize(r, bufferSize.Int()),
		Closer: r.(io.Closer),
	}

	return r, nil
}

func getUTXODeletionsMapFromReader(ctx context.Context, r io.Reader) (map[[32]byte][]uint32, error) {
	// Read the header
	magic, _, _, err := GetHeaderFromReader(r)
	if err != nil {
		return nil, errors.NewStorageError("error reading header", err)
	}

	if magic != "U-D-1.0" {
		return nil, errors.NewStorageError("invalid magic number in deletions file: %s", magic)
	}

	m := make(map[[32]byte][]uint32)

	var b [36]byte

	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
			// Read the next 36 bytes...
			_, err := io.ReadFull(r, b[:])
			if err != nil {
				if err == io.EOF {
					return nil, errors.NewStorageError("unexpected EOF before EOFMarker")
				}

				return nil, errors.NewStorageError("error reading deletions", err)
			}

			var hash [32]byte

			copy(hash[:], b[:32])

			if bytes.Equal(hash[:], EOFMarker) {
				return m, nil
			}

			deletedIndices := m[hash]
			deletedIndices = append(deletedIndices, uint32(b[32])|uint32(b[33])<<8|uint32(b[34])<<16|uint32(b[35])<<24)
			m[hash] = deletedIndices
		}
	}
}

func (us *UTXOSet) GetUTXODeletionsMap(ctx context.Context) (map[[32]byte][]uint32, error) {
	ctx, _, deferFn := tracing.StartTracing(ctx, "GetUTXODeletionsMap",
		tracing.WithLogMessage(us.logger, "[GetUTXODeletionsMap] called"),
	)
	defer deferFn()

	r, err := us.GetUTXODeletionsReader(ctx)
	if err != nil {
		return nil, errors.NewStorageError("error getting reader for %s.%s", us.blockHash, deletionsExtension, err)
	}

	defer r.Close()

	m, err := getUTXODeletionsMapFromReader(ctx, r)
	if err != nil {
		return nil, errors.NewStorageError("error loading %s.%s", us.blockHash, deletionsExtension, err)
	}

	return m, nil
}

// CreateUTXOSet generates the UTXO set for the current block, using the previous block's UTXO set
// and applying additions and deletions from the current block. It returns an error if the operation fails.
func (us *UTXOSet) CreateUTXOSet(ctx context.Context, previousBlockHash *chainhash.Hash) (err error) {
	ctx, createStat, deferFn := tracing.StartTracing(ctx, "CreateUTXOSet",
		tracing.WithParentStat(us.stats),
		tracing.WithLogMessage(us.logger, "[CreateUTXOSet] called"),
	)
	defer deferFn()

	deletions := us.deletionsMap

	if deletions == nil && previousBlockHash != nil {
		// Load the deletions file for this block in to a set
		var err error

		deletions, err = us.GetUTXODeletionsMap(ctx)
		if err != nil {
			return errors.NewStorageError("error getting utxo-deletions set", err)
		}
	}

	// Open the additions file for this block and stream each record to the new UTXOSet if not in the deletions set
	additionsReader, err := us.GetUTXOAdditionsReader(ctx)
	if err != nil {
		return errors.NewStorageError("error getting utxo-additions reader", err)
	}
	defer additionsReader.Close()

	magic, _, blockHeight, err := GetHeaderFromReader(additionsReader)
	if err != nil {
		return errors.NewStorageError("error reading header", err)
	}

	if magic != "U-A-1.0" {
		return errors.NewStorageError("invalid magic number in additions file: %s", magic)
	}

	storer := filestorer.NewFileStorer(ctx, us.logger, us.store, us.blockHash[:], utxosetExtension)

	b, err := BuildHeaderBytes("U-S-1.0", &us.blockHash, blockHeight, previousBlockHash)
	if err != nil {
		return errors.NewStorageError("error building utxo-set header", err)
	}

	if _, err = storer.Write(b); err != nil {
		return errors.NewStorageError("error writing utxo-set header", err)
	}

	var (
		readStat   = createStat.NewStat("readTX")
		filterStat = createStat.NewStat("filterUTXOs")
		writeStat  = createStat.NewStat("writeUTXOs")
		ts         = gocore.CurrentTime()
		txCount    uint64
		utxoCount  uint64
	)

	if previousBlockHash != nil {
		// Open the previous UTXOSet for the previous block
		previousUTXOSetReader, err := us.store.GetIoReader(ctx, previousBlockHash[:], options.WithFileExtension(utxosetExtension))
		if err != nil {
			return errors.NewStorageError("error getting utxoset reader for previous block %s", previousBlockHash, err)
		}

		utxopersisterBufferSize, _ := gocore.Config().Get("utxoPersister_buffer_size", "4KB")

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

		magic, _, _, _, err := GetUTXOSetHeaderFromReader(previousUTXOSetReader)
		if err != nil {
			return errors.NewStorageError("error reading header", err)
		}

		if magic != "U-S-1.0" {
			return errors.NewStorageError("invalid magic number in utxo-set file: %s", magic)
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

					return errors.NewStorageError("error reading previous utxo-set (%s.%s) at iteration %d", previousBlockHash.String(), utxosetExtension, txCount, err)
				}

				ts = readStat.AddTime(ts)

				// Filter UTXOs based on the deletions map
				utxoWrapper.UTXOs = filterUTXOs(utxoWrapper.UTXOs, deletions, utxoWrapper.TxID)

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

		us.logger.Infof("Read %d UTXOs from previous block %s", txCount, previousBlockHash.String())
	}

	for {
		// Read the next 36 bytes...
		utxoWrapper, err := NewUTXOWrapperFromReader(ctx, additionsReader)
		if err != nil {
			if err == io.EOF {
				break
			}

			return errors.NewStorageError("error reading utxo-additions", err)
		}

		ts = readStat.AddTime(ts)

		// Filter UTXOs based on the deletions map
		utxoWrapper.UTXOs = filterUTXOs(utxoWrapper.UTXOs, deletions, utxoWrapper.TxID)

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

	// Write the EOF marker
	if _, err = storer.Write(EOFMarker); err != nil {
		return errors.NewStorageError("error writing EOF marker", err)
	}

	// Write the number of transactions
	b = make([]byte, 8)
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

	return nil
}

func (us *UTXOSet) GetUTXOSetReader(optionalBlockHash ...chainhash.Hash) (io.ReadCloser, error) {
	blockHash := us.blockHash
	if len(optionalBlockHash) > 0 {
		blockHash = optionalBlockHash[0]
	}

	return us.store.GetIoReader(us.ctx, blockHash[:], options.WithFileExtension(utxosetExtension))
}

// filterUTXOs filters out UTXOs that are present in the deletions map.
func filterUTXOs(utxos []*UTXO, deletions map[[32]byte][]uint32, txID [32]byte) []*UTXO {
	filteredUTXOs := make([]*UTXO, 0, len(utxos))

	indices, found := deletions[txID]
	if found {
		for _, utxo := range utxos {
			toDelete := false

			for _, index := range indices {
				if utxo.Index == index {
					toDelete = true
					break
				}
			}

			if !toDelete {
				filteredUTXOs = append(filteredUTXOs, utxo)
			}
		}
	} else {
		// No deletions found for this txID, return all UTXOs
		return utxos
	}

	return filteredUTXOs
}

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

func UnpadSlice[T any](padded []*T) []*T {
	utxos := make([]*T, 0, len(padded))

	for _, utxo := range padded {
		if utxo != nil {
			utxos = append(utxos, utxo)
		}
	}

	return utxos
}
