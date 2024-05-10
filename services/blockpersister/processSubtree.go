package blockpersister

import (
	"bufio"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"time"

	"github.com/bitcoin-sv/ubsv/errors"
	"github.com/bitcoin-sv/ubsv/model"
	utxo_model "github.com/bitcoin-sv/ubsv/services/blockpersister/utxoset/model"
	"github.com/bitcoin-sv/ubsv/stores/blob/options"
	"github.com/bitcoin-sv/ubsv/stores/txmeta"
	"github.com/bitcoin-sv/ubsv/util"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/opentracing/opentracing-go"
	"github.com/ordishs/gocore"
)

func (u *Server) processSubtree(ctx context.Context, subtreeHash chainhash.Hash, utxoDiff *utxo_model.UTXODiff) error {
	startTotal, stat, ctx := util.StartStatFromContext(ctx, "validateSubtreeBlobInternal")
	span, spanCtx := opentracing.StartSpanFromContext(ctx, "BlockValidation:validateSubtree")
	span.LogKV("subtree", subtreeHash.String())
	defer func() {
		span.Finish()
		stat.AddTime(startTotal)
		prometheusBlockPersisterValidateSubtree.Inc()
	}()

	// 1. get the subtree from the subtree store
	subtreeReader, err := u.subtreeStore.GetIoReader(ctx, subtreeHash.CloneBytes())
	if err != nil {
		return fmt.Errorf("[BlockPersister] error getting subtree %s from store: %w", subtreeHash.String(), err)
	}
	defer func() {
		_ = subtreeReader.Close()
	}()

	subtree := util.Subtree{}
	err = subtree.DeserializeFromReader(subtreeReader)
	if err != nil {
		return fmt.Errorf("[BlockPersister] error deserializing subtree: %w", err)
	}

	// Get the subtree hashes if they were passed in (SubtreeFound() passes them in, BlockFound does not)
	// 2. create a slice of MissingTxHashes for all the txs in the subtree
	txHashes := make([]chainhash.Hash, len(subtree.Nodes))

	for i := 0; i < len(subtree.Nodes); i++ {
		txHashes[i] = subtree.Nodes[i].Hash
	}

	// txMetaSlice will be populated with the txMeta data for each txHash
	txMetaSlice := make([]*txmeta.Data, len(txHashes))

	// unlike many other lists, this needs to be a pointer list, because a lot of values could be empty = nil

	batched := gocore.Config().GetBool("blockvalidation_batchMissingTransactions", true)

	// 2. ...then attempt to load the txMeta from the store (i.e - aerospike in production)
	missed, err := u.processTxMetaUsingStore(spanCtx, txHashes, txMetaSlice, batched)
	if err != nil {
		return errors.Join(fmt.Errorf("[validateSubtreeInternal][%s] failed to get tx meta from store", subtreeHash.String()), err)
	}

	if missed > 0 {
		return fmt.Errorf("[validateSubtreeInternal][%s] failed to get tx meta from store", subtreeHash.String())
	}

	reader, writer := io.Pipe()

	bufferedWriter := bufio.NewWriter(writer)

	go func() {
		defer func() {
			// Flush the buffer and close the writer with error handling
			if err := bufferedWriter.Flush(); err != nil {
				u.logger.Errorf("error flushing writer: %v", err)
				writer.CloseWithError(err)
				return
			}

			if err := writer.CloseWithError(nil); err != nil {
				u.logger.Errorf("error closing writer: %v", err)
			}
		}()

		// Write the number of txs in the subtree
		// TODO why is there meta data in the subtree data?
		//      this makes it impossible to stream directly from S3 to the client
		if err := binary.Write(bufferedWriter, binary.LittleEndian, uint32(len(txMetaSlice))); err != nil {
			u.logger.Errorf("error writing number of txs: %v", err)
			writer.CloseWithError(err)
			return
		}

		for i := 0; i < len(txMetaSlice); i++ {
			if model.CoinbasePlaceholderHash.Equal(txHashes[i]) {
				// The coinbase tx is not in the txmeta store so we add in a special coinbase placeholder tx
				if i != 0 {
					err := fmt.Errorf("[BlockPersister] coinbase tx is not first in subtree (%d)", i)
					u.logger.Errorf(err.Error())
					writer.CloseWithError(err)
					return
				}

				if _, err := bufferedWriter.Write(model.CoinbasePlaceholderTx.Bytes()); err != nil {
					u.logger.Errorf("error writing coinbase tx: %v", err)
					writer.CloseWithError(err)
					return
				}
			} else {
				if _, err := bufferedWriter.Write(txMetaSlice[i].Tx.Bytes()); err != nil {
					u.logger.Errorf("error writing tx: %v", err)
					writer.CloseWithError(err)
					return
				}

				if utxoDiff != nil {
					// Process the utxo diff...
					utxoDiff.ProcessTx(txMetaSlice[i].Tx)
				}
			}
		}
	}()

	// Items with TTL get written to base folder, so we need to set the TTL here and will remove it when the file is written.
	// With the lustre store, removing the TTL will move the file to the S3 folder which tells lustre to move it to an S3 bucket on AWS.
	if err := u.blockStore.SetFromReader(ctx, subtreeHash[:], reader, options.WithFileExtension("subtree"), options.WithTTL(24*time.Hour)); err != nil {
		return fmt.Errorf("[BlockPersister] error persisting subtree: %w", err)
	}

	return u.blockStore.SetTTL(ctx, subtreeHash[:], 0, options.WithFileExtension("subtree"))
}
