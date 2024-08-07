package utxopersister

import (
	"bufio"
	"context"
	"io"
	"sync"
	"time"

	"github.com/bitcoin-sv/ubsv/errors"
	"github.com/bitcoin-sv/ubsv/util"
	"github.com/ordishs/go-utils"
	"golang.org/x/sync/errgroup"

	"github.com/bitcoin-sv/ubsv/stores/blob"
	"github.com/bitcoin-sv/ubsv/stores/blob/options"
	"github.com/bitcoin-sv/ubsv/ulogger"
	"github.com/libsv/go-bt/v2"
	"github.com/libsv/go-bt/v2/chainhash"
)

const (
	additionsExtension = "utxo-additions"
	deletionsExtension = "utxo-deletions"
	utxosetExtension   = "utxo-set"
)

// UTXODiff is a map of UTXOs.
type UTXODiff struct {
	ctx                     context.Context
	logger                  ulogger.Logger
	blockHash               chainhash.Hash
	additionsWriter         *io.PipeWriter
	additionsBufferedWriter *bufio.Writer
	deletionsWriter         *io.PipeWriter
	deletionsBufferedWriter *bufio.Writer
	store                   blob.Store
	deletionsSet            map[[36]byte]struct{}
}

// NewUTXOMap creates a new UTXODiff.
func NewUTXODiff(ctx context.Context, logger ulogger.Logger, store blob.Store, blockHash *chainhash.Hash) (*UTXODiff, error) {
	// Now, write the block file
	logger.Infof("[BlockPersister] Persisting utxo additions and deletions for block %s", blockHash.String())

	additionsReader, additionsWriter := io.Pipe()
	additionsBufferedWriter := bufio.NewWriter(additionsWriter)

	go func() {
		defer func() {
			if err := additionsReader.Close(); err != nil {
				logger.Errorf("Failed to close additionsReader: %v", err)
			}
		}()

		reader := io.NopCloser(bufio.NewReader(additionsReader))

		if err := store.SetFromReader(ctx, blockHash[:], reader, options.WithFileExtension(additionsExtension), options.WithTTL(24*time.Hour)); err != nil {
			logger.Errorf("%s", errors.NewStorageError("[BlockPersister] error setting additions reader", err))
		}
	}()

	deletionsReader, deletionsWriter := io.Pipe()
	deletionsBufferedWriter := bufio.NewWriter(deletionsWriter)

	go func() {
		defer func() {
			if err := deletionsReader.Close(); err != nil {
				logger.Errorf("Failed to close deletionsReader: %v", err)
			}
		}()

		reader := io.NopCloser(bufio.NewReader(deletionsReader))

		if err := store.SetFromReader(ctx, blockHash[:], reader, options.WithFileExtension(deletionsExtension), options.WithTTL(24*time.Hour)); err != nil {
			logger.Errorf("%s", errors.NewStorageError("[BlockPersister] error setting deletions reader", err))
		}
	}()

	return &UTXODiff{
		ctx:                     ctx,
		logger:                  logger,
		blockHash:               *blockHash,
		additionsWriter:         additionsWriter,
		additionsBufferedWriter: additionsBufferedWriter,
		deletionsWriter:         deletionsWriter,
		deletionsBufferedWriter: deletionsBufferedWriter,
		store:                   store,
	}, nil
}

func GetUTXODiff(ctx context.Context, logger ulogger.Logger, store blob.Store, blockHash *chainhash.Hash) (*UTXODiff, error) {

	ud := &UTXODiff{
		ctx:       ctx,
		logger:    logger,
		blockHash: *blockHash,
		store:     store,
	}

	// Check to see if the utxo-set already exists
	exists, err := store.Exists(ctx, blockHash[:], options.WithFileExtension(utxosetExtension))
	if err != nil {
		return nil, errors.NewStorageError("error checking if utxo-set exists", err)
	}

	if exists {
		return nil, nil
	}

	deletions, err := ud.GetUTXODeletionsSet()
	if err != nil {
		return nil, errors.NewStorageError("error getting utxo-deletions set", err)
	}

	ud.deletionsSet = deletions

	return ud, nil
}

func (ud *UTXODiff) ProcessTx(tx *bt.Tx) error {
	spendingHeight := uint32(0)

	if tx.IsCoinbase() {
		// We can ignore the error if the height is not found, because all old blocks are spendable today
		spendingHeight, _ = util.ExtractCoinbaseHeight(tx)
		spendingHeight += 101 // Height: If a block is mined at height 100,000, the coinbase transaction from this block can be spent only AFTER reaching block height 100,100.
	} else {
		for _, input := range tx.Inputs {
			if err := ud.delete(&UTXODeletion{input.PreviousTxIDChainHash(), input.PreviousTxOutIndex}); err != nil {
				return err
			}
		}
	}

	for i, output := range tx.Outputs {
		if output.LockingScript.IsData() {
			continue
		}

		if err := ud.add(&UTXO{
			tx.TxIDChainHash(),
			uint32(i),
			output.Satoshis,
			spendingHeight,
			*output.LockingScript,
		}); err != nil {
			return err
		}
	}

	return nil
}

// Add adds a UTXO to the map.
func (ud *UTXODiff) add(utxo *UTXO) error {
	_, err := ud.additionsWriter.Write(utxo.Bytes())
	if err != nil {
		return err
	}

	return nil
}

func (ud *UTXODiff) delete(utxoDeletion *UTXODeletion) error {
	_, err := ud.deletionsWriter.Write(utxoDeletion.DeletionBytes())
	if err != nil {
		return err
	}

	return nil
}

func (ud *UTXODiff) Close() error {
	g, ctx := errgroup.WithContext(ud.ctx)

	g.Go(func() error {
		if err := ud.additionsBufferedWriter.Flush(); err != nil {
			return errors.NewStorageError("Error flushing additions writer:", err)
		}

		if err := ud.additionsWriter.Close(); err != nil {
			return errors.NewStorageError("Error closing additions writer:", err)
		}

		if err := ud.store.SetTTL(ctx, ud.blockHash[:], 0, options.WithFileExtension(additionsExtension)); err != nil {
			return errors.NewStorageError("Error setting ttl on additions file", err)
		}

		return nil
	})

	g.Go(func() error {
		if err := ud.deletionsBufferedWriter.Flush(); err != nil {
			return errors.NewStorageError("Error flushing deletions writer:", err)
		}

		if err := ud.deletionsWriter.Close(); err != nil {
			return errors.NewStorageError("Error closing deletions writer: %v", err)
		}

		if err := ud.store.SetTTL(ctx, ud.blockHash[:], 0, options.WithFileExtension(deletionsExtension)); err != nil {
			return errors.NewStorageError("Error setting ttl on deletions file", err)
		}

		return nil
	})

	if err := g.Wait(); err != nil {
		return err
	}

	if err := ud.waitUntilFileIsAvailable(additionsExtension); err != nil {
		ud.logger.Warnf("Error waiting for additions file to be available: %v", err)
	}

	if err := ud.waitUntilFileIsAvailable(deletionsExtension); err != nil {
		ud.logger.Warnf("Error waiting for deletions file to be available: %v", err)
	}

	ud.logger.Infof("[BlockPersister] Persisting utxo additions and deletions for block %s - DONE", ud.blockHash.String())

	return nil
}

func (ud *UTXODiff) GetUTXOAdditionsReader() (io.ReadCloser, error) {
	r, err := ud.store.GetIoReader(ud.ctx, ud.blockHash[:], options.WithFileExtension(additionsExtension))
	if err != nil {
		return nil, errors.NewStorageError("error getting utxo-additions reader", err)
	}

	return r, nil
}

func (ud *UTXODiff) GetUTXODeletionsReader() (io.ReadCloser, error) {
	r, err := ud.store.GetIoReader(ud.ctx, ud.blockHash[:], options.WithFileExtension(deletionsExtension))
	if err != nil {
		return nil, errors.NewStorageError("error getting utxo-deletions reader", err)
	}

	return r, nil
}

func getUTXODeletionsSetFromReader(r io.Reader) (map[[36]byte]struct{}, error) {
	m := make(map[[36]byte]struct{})

	var b [36]byte

	for {
		// Read the next 36 bytes...
		n, err := io.ReadFull(r, b[:])
		if err != nil {
			if err == io.EOF {
				break
			}
			return nil, errors.NewStorageError("error reading deletions", err)
		}

		if n != 36 {
			return nil, errors.NewStorageError("error reading deletions", io.ErrUnexpectedEOF)
		}

		m[b] = struct{}{}
	}

	return m, nil
}
func (ud *UTXODiff) GetUTXODeletionsSet() (map[[36]byte]struct{}, error) {
	r, err := ud.GetUTXODeletionsReader()
	if err != nil {
		return nil, errors.NewStorageError("error getting utxo-deletions reader", err)
	}

	defer r.Close()

	m, err := getUTXODeletionsSetFromReader(r)
	if err != nil {
		return nil, errors.NewStorageError("error getting utxo-deletions set", err)
	}

	return m, nil
}

// CreateUTXOSet generates the UTXO set for the current block, using the previous block's UTXO set
// and applying additions and deletions from the current block. It returns an error if the operation fails.
func (ud *UTXODiff) CreateUTXOSet(ctx context.Context, previousBlockHash *chainhash.Hash) (err error) {
	deletions := ud.deletionsSet

	if deletions == nil {
		// Load the deletions file for this block in to a set
		var err error
		deletions, err = ud.GetUTXODeletionsSet()
		if err != nil {
			return errors.NewStorageError("error getting utxo-deletions set", err)
		}
	}

	reader, writer := io.Pipe()
	bufferedReader := io.NopCloser(bufio.NewReader(reader))
	bufferedWriter := bufio.NewWriter(writer)

	// Use a channel to communicate errors from the goroutine
	errChan := make(chan error, 1)

	// Use WaitGroup to ensure the goroutine finishes before returning
	var wg sync.WaitGroup
	wg.Add(1)

	go func() {
		defer reader.Close()
		defer wg.Done()

		if err := ud.store.SetFromReader(ctx, ud.blockHash[:], bufferedReader, options.WithFileExtension(utxosetExtension), options.WithTTL(24*time.Hour)); err != nil {
			// Send the error to the channel
			errChan <- errors.NewStorageError("[BlockPersister] error setting utxo-set reader", err)
		}
		close(errChan) // Close the channel when done
	}()

	if previousBlockHash != nil {
		// Open the previous UTXOSet for the previous block
		previousUTXOSetReader, err := ud.store.GetIoReader(ctx, previousBlockHash[:], options.WithFileExtension(utxosetExtension))
		if err != nil {
			return errors.NewStorageError("error getting utxoset reader for previous block %s", previousBlockHash, err)
		}
		defer previousUTXOSetReader.Close()

		for {
			// Read the next 36 bytes...
			utxo, err := NewUTXOFromReader(previousUTXOSetReader)
			if err != nil {
				if err == io.EOF {
					break
				}
				return errors.NewStorageError("error reading utxo-set", err)
			}

			// Stream each record and write to new UTXOSet if not in the deletions set
			if _, deleted := deletions[utxo.DeletionBytes()]; !deleted {
				_, err := writer.Write(utxo.Bytes())
				if err != nil {
					return errors.NewStorageError("error writing utxo", err)
				}
			}
		}
	}

	// Open the additions file for this block and stream each record to the new UTXOSet if not in the deletions set
	additionsReader, err := ud.GetUTXOAdditionsReader()
	if err != nil {
		return errors.NewStorageError("error getting utxo-additions reader", err)
	}

	defer additionsReader.Close()

	for {
		// Read the next 36 bytes...
		utxo, err := NewUTXOFromReader(additionsReader)
		if err != nil {
			if err == io.EOF {
				break
			}
			return errors.NewStorageError("error reading utxo-additions", err)
		}

		// Stream each record and write to new UTXOSet if not in the deletions set
		if _, deleted := deletions[utxo.DeletionBytes()]; !deleted {
			_, err := writer.Write(utxo.Bytes())
			if err != nil {
				return errors.NewStorageError("error writing utxo", err)
			}
		}
	}

	if err = bufferedWriter.Flush(); err != nil {
		return errors.NewStorageError("error flushing utxoset writer", err)
	}

	if err = writer.Close(); err != nil {
		return errors.NewStorageError("error closing utxoset writer", err)
	}

	// Wait for the goroutine to finish
	wg.Wait()

	if err = ud.waitUntilFileIsAvailable(utxosetExtension); err != nil {
		ud.logger.Warnf("Error waiting for utxo-set file to be available: %v", err)
	}

	// Check if the goroutine returned an error
	if goroutineErr := <-errChan; goroutineErr != nil {
		return goroutineErr
	}

	if err := ud.store.SetTTL(ctx, ud.blockHash[:], 0, options.WithFileExtension(utxosetExtension)); err != nil {
		return errors.NewStorageError("error setting ttl on utxoset file", err)
	}

	return nil
}

func (ud *UTXODiff) GetUTXOSetReader(optionalBlockHash ...chainhash.Hash) (io.ReadCloser, error) {
	blockHash := ud.blockHash
	if len(optionalBlockHash) > 0 {
		blockHash = optionalBlockHash[0]
	}

	return ud.store.GetIoReader(ud.ctx, blockHash[:], options.WithFileExtension(utxosetExtension))
}

func (ud *UTXODiff) waitUntilFileIsAvailable(extension string) error {
	maxRetries := 10
	retryInterval := 100 * time.Millisecond

	for i := 0; i < maxRetries; i++ {
		exists, err := ud.store.Exists(ud.ctx, ud.blockHash[:], options.WithFileExtension(extension))
		if err != nil {
			return err
		}

		if exists {
			return nil
		}

		time.Sleep(retryInterval)
	}

	return errors.NewStorageError("file %s.%s is not available", utils.ReverseAndHexEncodeSlice(ud.blockHash[:]), extension)
}
