// Package blockpersister provides functionality for persisting blockchain blocks and their associated data.
package blockpersister

import (
	"context"
	"encoding/binary"

	"github.com/bitcoin-sv/teranode/errors"
	"github.com/bitcoin-sv/teranode/services/utxopersister"
	"github.com/bitcoin-sv/teranode/services/utxopersister/filestorer"
	"github.com/bitcoin-sv/teranode/stores/blob/options"
	"github.com/bitcoin-sv/teranode/stores/utxo/meta"
	"github.com/bitcoin-sv/teranode/tracing"
	"github.com/bitcoin-sv/teranode/ulogger"
	"github.com/bitcoin-sv/teranode/util"
	"github.com/libsv/go-bt/v2"
	"github.com/libsv/go-bt/v2/chainhash"
)

// ProcessSubtree processes a subtree of transactions, validating and storing them.
// Parameters:
//   - pCtx: parent context for the operation
//   - subtreeHash: hash of the subtree to process
//   - coinbaseTx: the coinbase transaction for this subtree
//   - utxoDiff: the UTXO set differences to track changes
//
// Returns an error if processing fails.
func (u *Server) ProcessSubtree(pCtx context.Context, subtreeHash chainhash.Hash, coinbaseTx *bt.Tx, utxoDiff *utxopersister.UTXOSet) error {
	ctx, _, deferFn := tracing.StartTracing(pCtx, "ProcessSubtree",
		tracing.WithHistogram(prometheusBlockPersisterValidateSubtree),
		tracing.WithDebugLogMessage(u.logger, "[ProcessSubtree] called for subtree %s", subtreeHash.String()),
	)
	defer deferFn()

	// 1. get the subtree from the subtree store
	subtreeReader, err := u.subtreeStore.GetIoReader(ctx, subtreeHash.CloneBytes(), options.WithFileExtension("subtree"))
	if err != nil {
		return errors.NewStorageError("[BlockPersister] error getting subtree %s from store", subtreeHash.String(), err)
	}

	defer func() {
		_ = subtreeReader.Close()
	}()

	subtree := util.Subtree{}

	err = subtree.DeserializeFromReader(subtreeReader)
	if err != nil {
		return errors.NewProcessingError("[BlockPersister] error deserializing subtree", err)
	}

	// Get the subtree hashes if they were passed in (SubtreeFound() passes them in, BlockFound does not)
	// 2. create a slice of MissingTxHashes for all the txs in the subtree
	txHashes := make([]chainhash.Hash, len(subtree.Nodes))

	for i := 0; i < len(subtree.Nodes); i++ {
		txHashes[i] = subtree.Nodes[i].Hash
	}

	// txMetaSlice will be populated with the txMeta data for each txHash
	txMetaSlice := make([]*meta.Data, len(txHashes))

	// The first tx is the coinbase tx
	txMetaSlice[0] = &meta.Data{Tx: coinbaseTx}

	// unlike many other lists, this needs to be a pointer list, because a lot of values could be empty = nil

	batched := u.settings.Block.BatchMissingTransactions

	// 2. ...then attempt to load the txMeta from the store (i.e - aerospike in production)
	missed, err := u.processTxMetaUsingStore(ctx, txHashes, txMetaSlice, batched)
	if err != nil {
		u.logger.Errorf("[ValidateSubtreeInternal][%s] failed to get tx meta from store: %s", subtreeHash.String(), err)
		return errors.NewServiceError("[ValidateSubtreeInternal][%s] failed to get tx meta from store", subtreeHash.String(), err)
	}

	if missed > 0 {
		for i := 0; i < len(txHashes); i++ {
			if util.CoinbasePlaceholderHash.Equal(txHashes[i]) {
				continue
			}

			u.logger.Errorf("[ValidateSubtreeInternal][%s] failed to get tx meta from store for tx %s", subtreeHash.String(), txHashes[i].String())
		}

		return errors.NewServiceError("[ValidateSubtreeInternal][%s] failed to get %d of %d tx meta from store", subtreeHash.String(), missed, len(txHashes))
	}

	storer := filestorer.NewFileStorer(context.Background(), u.logger, u.settings, u.blockStore, subtreeHash[:], "subtree")
	defer storer.Close(context.Background())

	if err := WriteTxs(context.Background(), u.logger, storer, txMetaSlice, utxoDiff); err != nil {
		return errors.NewProcessingError("[BlockPersister] error writing txs", err)
	}

	return nil
}

// WriteTxs writes a series of transactions to storage and processes their UTXO changes.
// Parameters:
//   - ctx: context for the operation
//   - logger: logger for recording operations
//   - writer: destination for writing transaction data
//   - txMetaSlice: slice of transaction metadata to write
//   - utxoDiff: UTXO set to track changes (can be nil)
//
// Returns an error if writing fails.
func WriteTxs(ctx context.Context, logger ulogger.Logger, writer *filestorer.FileStorer, txMetaSlice []*meta.Data, utxoDiff *utxopersister.UTXOSet) error {
	// Write the number of txs in the subtree
	//      this makes it impossible to stream directly from S3 to the client
	if err := binary.Write(writer, binary.LittleEndian, uint32(len(txMetaSlice))); err != nil {
		return errors.NewProcessingError("error writing number of txs", err)
	}

	for i := 0; i < len(txMetaSlice); i++ {
		txMeta := txMetaSlice[i]
		if txMeta == nil {
			logger.Errorf("[WriteTxs] txMeta is nil at index %d", i)
			continue
		}

		if txMeta.Tx == nil {
			logger.Errorf("[WriteTxs] txMeta.Tx is nil at index %d", i)
			continue
		}

		if _, err := writer.Write(txMeta.Tx.Bytes()); err != nil {
			return errors.NewProcessingError("error writing tx", err)
		}

		if utxoDiff != nil {
			// Process the utxo diff...
			if err := utxoDiff.ProcessTx(txMetaSlice[i].Tx); err != nil {
				return errors.NewProcessingError("error processing tx", err)
			}
		}
	}

	return nil
}
