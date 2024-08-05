package blockpersister

import (
	"bufio"
	"context"
	"encoding/binary"
	"io"
	"time"

	"github.com/bitcoin-sv/ubsv/errors"
	"github.com/bitcoin-sv/ubsv/services/utxopersister"
	"github.com/bitcoin-sv/ubsv/stores/blob/options"
	"github.com/bitcoin-sv/ubsv/stores/utxo/meta"
	"github.com/bitcoin-sv/ubsv/tracing"
	"github.com/bitcoin-sv/ubsv/ulogger"
	"github.com/bitcoin-sv/ubsv/util"
	"github.com/libsv/go-bt/v2"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/ordishs/gocore"
)

func (u *Server) ProcessSubtree(ctx context.Context, subtreeHash chainhash.Hash, coinbaseTx *bt.Tx, utxoDiff *utxopersister.UTXODiff) error {
	ctx, _, deferFn := tracing.StartTracing(ctx, "ProcessSubtree",
		tracing.WithHistogram(prometheusBlockPersisterValidateSubtree),
		tracing.WithLogMessage(u.logger, "[ProcessSubtree] called for subtree %s", subtreeHash.String()),
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

	batched := gocore.Config().GetBool("blockvalidation_batchMissingTransactions", true)

	// 2. ...then attempt to load the txMeta from the store (i.e - aerospike in production)
	missed, err := u.processTxMetaUsingStore(ctx, txHashes, txMetaSlice, batched)
	if err != nil {
		return errors.NewServiceError("[validateSubtreeInternal][%s] failed to get tx meta from store", subtreeHash.String(), err)
	}

	if missed > 0 {
		return errors.NewServiceError("[validateSubtreeInternal][%s] failed to get tx meta from store", subtreeHash.String())
	}

	reader, writer := io.Pipe()

	go WriteTxs(u.logger, writer, txMetaSlice, utxoDiff)

	// Items with TTL get written to base folder, so we need to set the TTL here and will remove it when the file is written.
	// With the lustre store, removing the TTL will move the file to the S3 folder which tells lustre to move it to an S3 bucket on AWS.
	if err := u.blockStore.SetFromReader(ctx, subtreeHash[:], reader, options.WithFileExtension("subtree"), options.WithTTL(24*time.Hour)); err != nil {
		return errors.NewStorageError("[BlockPersister] error persisting subtree", err)
	}

	if err = u.blockStore.SetTTL(ctx, subtreeHash[:], 0, options.WithFileExtension("subtree")); err != nil {
		return errors.NewStorageError("[BlockPersister]error persisting subtree %s", subtreeHash.String(), err)
	}

	return nil
}

func WriteTxs(logger ulogger.Logger, writer *io.PipeWriter, txMetaSlice []*meta.Data, utxoDiff *utxopersister.UTXODiff) {
	bufferedWriter := bufio.NewWriter(writer)

	defer func() {
		// Flush the buffer and close the writer with error handling
		if err := bufferedWriter.Flush(); err != nil {
			logger.Errorf("error flushing writer: %v", err)
			_ = writer.CloseWithError(err)
			return
		}

		if err := writer.Close(); err != nil {
			logger.Errorf("error closing writer: %v", err)
		}
	}()

	// Write the number of txs in the subtree
	//      this makes it impossible to stream directly from S3 to the client
	if err := binary.Write(bufferedWriter, binary.LittleEndian, uint32(len(txMetaSlice))); err != nil {
		logger.Errorf("error writing number of txs: %v", err)
		_ = writer.CloseWithError(err)
		return
	}

	for i := 0; i < len(txMetaSlice); i++ {
		if _, err := bufferedWriter.Write(txMetaSlice[i].Tx.Bytes()); err != nil {
			logger.Errorf("error writing tx: %v", err)
			_ = writer.CloseWithError(err)
			return
		}

		if utxoDiff != nil {
			// Process the utxo diff...
			if err := utxoDiff.ProcessTx(txMetaSlice[i].Tx); err != nil {
				logger.Errorf("error processing tx: %v", err)
			}
		}
	}
}
