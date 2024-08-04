package utxo

import (
	"context"
	"io"
	"sync"
	"time"

	"github.com/bitcoin-sv/ubsv/errors"
	"github.com/bitcoin-sv/ubsv/util"
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
	ctx             context.Context
	logger          ulogger.Logger
	blockHash       chainhash.Hash
	additionsWriter *io.PipeWriter
	deletionsWriter *io.PipeWriter
	store           blob.Store
}

// NewUTXOMap creates a new UTXODiff.
func NewUTXODiff(ctx context.Context, logger ulogger.Logger, store blob.Store, blockHash *chainhash.Hash) (*UTXODiff, error) {
	// Now, write the block file
	logger.Infof("[BlockPersister] Persisting utxo additions and deletions for block %s", blockHash.String())

	additionsReader, additionsWriter := io.Pipe()

	go func() {
		defer additionsReader.Close()

		if err := store.SetFromReader(ctx, blockHash[:], additionsReader, options.WithFileExtension(additionsExtension), options.WithTTL(24*time.Hour)); err != nil {
			logger.Errorf("%s", errors.NewStorageError("[BlockPersister] error setting additions reader", err))
		}
	}()

	deletionsReader, deletionsWriter := io.Pipe()

	go func() {
		defer deletionsReader.Close()

		if err := store.SetFromReader(ctx, blockHash[:], deletionsReader, options.WithFileExtension(deletionsExtension), options.WithTTL(24*time.Hour)); err != nil {
			logger.Errorf("%s", errors.NewStorageError("[BlockPersister] error setting deletions reader", err))
		}
	}()

	return &UTXODiff{
		ctx:             ctx,
		logger:          logger,
		blockHash:       *blockHash,
		additionsWriter: additionsWriter,
		deletionsWriter: deletionsWriter,
		store:           store,
	}, nil
}

func (ud *UTXODiff) ProcessTx(tx *bt.Tx) error {
	spendingHeight := uint32(0)

	if tx.IsCoinbase() {
		spendingHeight, _ = util.ExtractCoinbaseHeight(tx)
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
	g, _ := errgroup.WithContext(ud.ctx)

	g.Go(func() error {
		if err := ud.additionsWriter.Close(); err != nil {
			return errors.NewStorageError("Error closing additions writer:", err)
		}

		if err := ud.store.SetTTL(ud.ctx, ud.blockHash[:], 0, options.WithFileExtension(additionsExtension)); err != nil {
			return errors.NewStorageError("Error setting ttl on additions file", err)
		}

		return nil
	})

	g.Go(func() error {
		if err := ud.deletionsWriter.Close(); err != nil {
			return errors.NewStorageError("Error closing deletions writer: %v", err)
		}

		if err := ud.store.SetTTL(ud.ctx, ud.blockHash[:], 0, options.WithFileExtension(deletionsExtension)); err != nil {
			return errors.NewStorageError("Error setting ttl on deletions file", err)
		}

		return nil
	})

	if err := g.Wait(); err != nil {
		return err
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

func GetUTXODeletionsSetFromReader(r io.Reader) (map[[36]byte]struct{}, error) {

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

	m, err := GetUTXODeletionsSetFromReader(r)
	if err != nil {
		return nil, errors.NewStorageError("error getting utxo-deletions set", err)
	}

	return m, nil
}

func (ud *UTXODiff) CreateUTXOSet(ctx context.Context, previousBlockHash *chainhash.Hash) (err error) {
	// Load the deletions file for this block in to a set
	deletions, err := ud.GetUTXODeletionsSet()
	if err != nil {
		return errors.NewStorageError("error getting utxo-deletions set", err)
	}

	reader, writer := io.Pipe()

	// Use a channel to communicate errors from the goroutine
	errChan := make(chan error, 1)

	// Use WaitGroup to ensure the goroutine finishes before returning
	var wg sync.WaitGroup
	wg.Add(1)

	go func() {
		defer reader.Close()
		defer wg.Done()

		if err := ud.store.SetFromReader(ctx, ud.blockHash[:], reader, options.WithFileExtension(utxosetExtension), options.WithTTL(24*time.Hour)); err != nil {
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

	err = writer.Close()
	if err != nil {
		return errors.NewStorageError("error closing utxoset writer", err)
	}

	// Wait for the goroutine to finish
	wg.Wait()

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
