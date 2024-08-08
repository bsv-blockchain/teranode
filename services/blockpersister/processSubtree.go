package blockpersister

import (
	"context"
	"encoding/binary"

	"github.com/bitcoin-sv/ubsv/errors"
	"github.com/bitcoin-sv/ubsv/services/utxopersister"
	"github.com/bitcoin-sv/ubsv/services/utxopersister/filestorer"
	"github.com/bitcoin-sv/ubsv/stores/blob/options"
	"github.com/bitcoin-sv/ubsv/stores/utxo/meta"
	"github.com/bitcoin-sv/ubsv/tracing"
	"github.com/bitcoin-sv/ubsv/ulogger"
	"github.com/bitcoin-sv/ubsv/util"
	"github.com/libsv/go-bt/v2"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/ordishs/gocore"
)

func (u *Server) ProcessSubtree(pCtx context.Context, subtreeHash chainhash.Hash, coinbaseTx *bt.Tx, utxoDiff *utxopersister.UTXODiff) error {
	ctx, _, deferFn := tracing.StartTracing(pCtx, "ProcessSubtree",
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

	storer := filestorer.NewFileStorer(context.Background(), u.logger, u.blockStore, subtreeHash[:], "subtree")
	defer storer.Close(context.Background())

	if err := WriteTxs(context.Background(), u.logger, storer, txMetaSlice, utxoDiff); err != nil {
		return errors.NewProcessingError("[BlockPersister] error writing txs", err)
	}

	return nil
}

func WriteTxs(ctx context.Context, logger ulogger.Logger, writer *filestorer.FileStorer, txMetaSlice []*meta.Data, utxoDiff *utxopersister.UTXODiff) error {
	// Write the number of txs in the subtree
	//      this makes it impossible to stream directly from S3 to the client
	if err := binary.Write(writer, binary.LittleEndian, uint32(len(txMetaSlice))); err != nil {
		return errors.NewProcessingError("error writing number of txs", err)
	}

	for i := 0; i < len(txMetaSlice); i++ {
		if _, err := writer.Write(txMetaSlice[i].Tx.Bytes()); err != nil {
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
