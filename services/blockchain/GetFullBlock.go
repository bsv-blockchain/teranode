package blockchain

import (
	"bytes"
	"context"
	"encoding/binary"
	"runtime"
	"sync/atomic"

	"github.com/bitcoin-sv/ubsv/errors"
	"github.com/bitcoin-sv/ubsv/model"
	"github.com/bitcoin-sv/ubsv/stores/blob"
	"github.com/bitcoin-sv/ubsv/stores/blockchain"
	"github.com/bitcoin-sv/ubsv/stores/utxo"
	"github.com/bitcoin-sv/ubsv/stores/utxo/meta"
	"github.com/bitcoin-sv/ubsv/tracing"
	"github.com/bitcoin-sv/ubsv/util"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/ordishs/gocore"
	"golang.org/x/sync/errgroup"
)

func GetFullBlockBytes(ctx context.Context, blockHash chainhash.Hash, store blockchain.Store, subtreeStore blob.Store, utxoStore utxo.Store) ([]byte, error) {

	block, _, err := store.GetBlock(ctx, &blockHash)
	if err != nil {
		return nil, errors.New(errors.ERR_PROCESSING, "[GetFullBlockBytes][%s] error getting block from store", block.Hash().String(), err)
	}

	// write the block headers
	buf := bytes.NewBuffer([]byte{})
	buf.Write(block.Header.Bytes())

	// write the transaction count
	b := make([]byte, binary.MaxVarintLen64)
	n := binary.PutUvarint(b, block.TransactionCount)
	if _, err := buf.Write(b[:n]); err != nil {
		return nil, errors.New(errors.ERR_PROCESSING, "[GetFullBlockBytes][%s] error writing transaction count", block.Hash().String(), err)
	}

	for _, subtreeHash := range block.Subtrees {
		if err := processSubtree(ctx, block, *subtreeHash, buf, subtreeStore, utxoStore); err != nil {
			return nil, err
		}
	}

	return buf.Bytes(), nil
}

func processSubtree(ctx context.Context, block *model.Block, subtreeHash chainhash.Hash, buf *bytes.Buffer, subtreeStore blob.Store, utxoStore utxo.Store) error {
	subtreeReader, err := subtreeStore.GetIoReader(ctx, subtreeHash.CloneBytes())
	if err != nil {
		return errors.New(errors.ERR_PROCESSING, "[GetFullBlockBytes] error getting subtree %s from store: %w", subtreeHash.String(), err)
	}
	defer func() {
		_ = subtreeReader.Close()
	}()

	subtree := util.Subtree{}
	err = subtree.DeserializeFromReader(subtreeReader)
	if err != nil {
		return errors.New(errors.ERR_PROCESSING, "[GetFullBlockBytes] error deserializing subtree: %w", err)
	}

	// Get the subtree hashes if they were passed in (SubtreeFound() passes them in, BlockFound does not)
	// 2. create a slice of MissingTxHashes for all the txs in the subtree
	txHashes := make([]chainhash.Hash, len(subtree.Nodes))

	for i := 0; i < len(subtree.Nodes); i++ {
		txHashes[i] = subtree.Nodes[i].Hash
	}

	// txMetaSlice will be populated with the txMeta data for each txHash
	txMetaSlice := make([]*meta.Data, len(txHashes))

	// unlike many other lists, this needs to be a pointer list, because a lot of values could be empty = nil

	// 2. ...then attempt to load the txMeta from the store (i.e - aerospike in production)
	missed, err := processTxMetaUsingStore(ctx, txHashes, txMetaSlice, utxoStore)
	if err != nil {
		return errors.New(errors.ERR_PROCESSING, "[GetFullBlockBytes][%s] failed to get tx meta from store", subtreeHash.String(), err)
	}

	if missed > 0 {
		return errors.New(errors.ERR_PROCESSING, "[GetFullBlockBytes][%s] missing tx meta", subtreeHash.String())
	}

	for i := 0; i < len(txMetaSlice); i++ {
		if model.CoinbasePlaceholderHash.Equal(txHashes[i]) {
			// The coinbase tx is not in the txmeta store so we add in a special coinbase placeholder tx
			if i != 0 {
				return errors.New(errors.ERR_PROCESSING, "[GetFullBlockBytes] coinbase tx is not first in subtree (%d)", i)
			}

			// Write coinbase tx
			if _, err := buf.Write(block.CoinbaseTx.Bytes()); err != nil {
				return errors.New(errors.ERR_PROCESSING, "[GetFullBlockBytes] error writing coinbase tx: %w", err)
			}

		} else {

			// Write regular tx
			if _, err := buf.Write(txMetaSlice[i].Tx.Bytes()); err != nil {
				return errors.New(errors.ERR_PROCESSING, "[GetFullBlockBytes] error writing tx[%d]: %v)", i, err)
			}
		}
	}

	return nil

}

func processTxMetaUsingStore(ctx context.Context, txHashes []chainhash.Hash, txMetaSlice []*meta.Data, utxoStore utxo.Store) (int, error) {
	if len(txHashes) != len(txMetaSlice) {
		return 0, errors.New(errors.ERR_PROCESSING, "[GetFullBlockBytes] txHashes and txMetaSlice must be the same length")
	}

	start, stat, ctx := tracing.StartStatFromContext(ctx, "processTxMetaUsingStore")
	defer stat.AddTime(start)

	batchSize, _ := gocore.Config().GetInt("blockvalidation_processTxMetaUsingStore_BatchSize", 1024)
	processSubtreeConcurrency, _ := gocore.Config().GetInt("blockvalidation_processTxMetaUsingStor_Concurrency", util.Max(4, runtime.NumCPU()/2))

	g, gCtx := errgroup.WithContext(ctx)
	g.SetLimit(processSubtreeConcurrency)

	var missed atomic.Int32

	for i := 0; i < len(txHashes); i += batchSize {
		i := i // capture range variable for goroutine

		g.Go(func() error {
			end := util.Min(i+batchSize, len(txHashes))

			missingTxHashesCompacted := make([]*utxo.UnresolvedMetaData, 0, end-i)

			for j := 0; j < util.Min(batchSize, len(txHashes)-i); j++ {
				select {
				case <-gCtx.Done(): // Listen for cancellation signal
					return gCtx.Err() // Return the error that caused the cancellation

				default:

					if txHashes[i+j].Equal(*model.CoinbasePlaceholderHash) {
						// coinbase placeholder is not in the store
						continue
					}

					if txMetaSlice[i+j] == nil {
						missingTxHashesCompacted = append(missingTxHashesCompacted, &utxo.UnresolvedMetaData{
							Hash: txHashes[i+j],
							Idx:  i + j,
						})
					}
				}
			}

			if err := utxoStore.BatchDecorate(gCtx, missingTxHashesCompacted, "tx"); err != nil {
				return err
			}

			select {
			case <-gCtx.Done(): // Listen for cancellation signal
				return gCtx.Err() // Return the error that caused the cancellation

			default:
				for _, data := range missingTxHashesCompacted {
					if data.Data == nil || data.Err != nil {
						missed.Add(1)
						continue
					}
					txMetaSlice[data.Idx] = data.Data
				}

				return nil
			}
		})
	}

	if err := g.Wait(); err != nil {
		return int(missed.Load()), err
	}

	return int(missed.Load()), nil

}
