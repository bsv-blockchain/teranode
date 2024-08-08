package utxopersister

import (
	"context"
	"io"

	"github.com/bitcoin-sv/ubsv/errors"
	"github.com/bitcoin-sv/ubsv/services/utxopersister/filestorer"
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
	blockHeight     uint32
	additionsStorer *filestorer.FileStorer
	deletionsStorer *filestorer.FileStorer
	store           blob.Store
	deletionsSet    map[[36]byte]struct{}
}

// NewUTXOMap creates a new UTXODiff.
func NewUTXODiff(ctx context.Context, logger ulogger.Logger, store blob.Store, blockHash *chainhash.Hash, blockHeight uint32) (*UTXODiff, error) {
	// Now, write the block file
	logger.Infof("[BlockPersister] Persisting utxo additions and deletions for block %s", blockHash.String())

	additionsStorer := filestorer.NewFileStorer(ctx, logger, store, blockHash[:], additionsExtension)
	deletionsStorer := filestorer.NewFileStorer(ctx, logger, store, blockHash[:], deletionsExtension)

	return &UTXODiff{
		ctx:             ctx,
		logger:          logger,
		blockHash:       *blockHash,
		blockHeight:     blockHeight,
		additionsStorer: additionsStorer,
		deletionsStorer: deletionsStorer,
		store:           store,
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
		spendingHeight = ud.blockHeight + 100
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
	_, err := ud.additionsStorer.Write(utxo.Bytes())
	if err != nil {
		return err
	}

	return nil
}

func (ud *UTXODiff) delete(utxoDeletion *UTXODeletion) error {
	_, err := ud.deletionsStorer.Write(utxoDeletion.DeletionBytes())
	if err != nil {
		return err
	}

	return nil
}

func (ud *UTXODiff) Close() error {
	g, ctx := errgroup.WithContext(ud.ctx)

	g.Go(func() error {
		if err := ud.additionsStorer.Close(ctx); err != nil {
			return errors.NewStorageError("Error flushing additions writer:", err)
		}

		return nil
	})

	g.Go(func() error {
		if err := ud.deletionsStorer.Close(ctx); err != nil {
			return errors.NewStorageError("Error flushing deletions writer:", err)
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
	r, err := ud.store.GetIoReader(ud.ctx, ud.blockHash[:], options.WithFileExtension(additionsExtension), options.WithTTL(0))
	if err != nil {
		return nil, errors.NewStorageError("error getting utxo-additions reader", err)
	}

	return r, nil
}

func (ud *UTXODiff) GetUTXODeletionsReader() (io.ReadCloser, error) {
	r, err := ud.store.GetIoReader(ud.ctx, ud.blockHash[:], options.WithFileExtension(deletionsExtension), options.WithTTL(0))
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

	storer := filestorer.NewFileStorer(ctx, ud.logger, ud.store, ud.blockHash[:], utxosetExtension)

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
				_, err := storer.Write(utxo.Bytes())
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
			_, err := storer.Write(utxo.Bytes())
			if err != nil {
				return errors.NewStorageError("error writing utxo", err)
			}
		}
	}

	if err = storer.Close(ctx); err != nil {
		return errors.NewStorageError("error flushing utxoset writer", err)
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
