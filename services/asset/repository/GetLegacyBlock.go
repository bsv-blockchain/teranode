package repository

import (
	"context"
	"encoding/binary"
	"io"
	"runtime"
	"sync/atomic"

	"github.com/bitcoin-sv/ubsv/errors"
	"github.com/bitcoin-sv/ubsv/model"
	"github.com/bitcoin-sv/ubsv/stores/utxo"
	"github.com/bitcoin-sv/ubsv/stores/utxo/meta"
	"github.com/bitcoin-sv/ubsv/tracing"
	"github.com/bitcoin-sv/ubsv/util"
	"github.com/libsv/go-bt"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/ordishs/gocore"
	"golang.org/x/sync/errgroup"
)

func (repo *Repository) GetLegacyBlockReader(ctx context.Context, hash *chainhash.Hash) (*io.PipeReader, error) {
	block, err := repo.GetBlockByHash(ctx, hash)
	if err != nil {
		return nil, err
	}

	r, w := io.Pipe()

	g, gCtx := errgroup.WithContext(ctx)
	g.Go(func() error {
		if err := repo.writeLegacyBlockHeader(block, w); err != nil {
			_ = w.Close()
			_ = r.CloseWithError(err)
			return err
		}

		for _, subtree := range block.Subtrees {
			err := repo.writeTransactionsViaBlockStore(gCtx, block, subtree, w)
			if err != nil {
				// not available via block-store (BlockPersister), maybe this is a timing issue.
				// try different approach - get the subtree/tx data using the subtree-store and utxo-store
				err = repo.writeTransactionsViaSubtreeStore(gCtx, block, subtree, w)
			}
			if err != nil {
				_ = w.Close()
				_ = r.CloseWithError(err)
				return err
			}
		}

		// close the writer after all subtrees have been streamed
		_ = w.Close()
		_ = r.Close()

		return nil
	})

	return r, nil
}

func (repo *Repository) writeLegacyBlockHeader(block *model.Block, w io.Writer) error {
	// write bitcoin block magic number
	if _, err := w.Write([]byte{0xf9, 0xbe, 0xb4, 0xd9}); err != nil {
		return err
	}

	// write the block size
	sizeInBytes := make([]byte, 4)
	binary.LittleEndian.PutUint32(sizeInBytes, uint32(block.SizeInBytes))
	if _, err := w.Write(sizeInBytes); err != nil {
		return err
	}

	// write the 80 byte block header
	if _, err := w.Write(block.Header.Bytes()); err != nil {
		return err
	}

	// write number of transactions
	if _, err := w.Write(bt.VarInt(block.TransactionCount)); err != nil {
		return err
	}

	return nil
}

func (repo *Repository) writeTransactionsViaBlockStore(ctx context.Context, _ *model.Block, subtreeHash *chainhash.Hash, w *io.PipeWriter) error {
	if subtreeReader, err := repo.GetSubtreeDataReader(ctx, subtreeHash); err != nil {

		return err

	} else {

		// skip the subtree tx size
		_, _ = subtreeReader.Read(make([]byte, 4))

		if _, err := io.Copy(w, subtreeReader); err != nil {
			return err
		}

		_ = subtreeReader.Close()
	}

	return nil
}

func (repo *Repository) writeTransactionsViaSubtreeStore(ctx context.Context, block *model.Block, subtreeHash *chainhash.Hash, w *io.PipeWriter) error {
	subtreeReader, err := repo.SubtreeStore.GetIoReader(ctx, subtreeHash.CloneBytes())
	if err != nil {
		return errors.New(errors.ERR_PROCESSING, "[writeTransactionsViaSubtreeStore] error getting subtree %s from store: %w", subtreeHash.String(), err)
	}
	defer func() {
		_ = subtreeReader.Close()
	}()

	subtree := util.Subtree{}
	err = subtree.DeserializeFromReader(subtreeReader)
	if err != nil {
		return errors.New(errors.ERR_PROCESSING, "[writeTransactionsViaSubtreeStore] error deserializing subtree: %w", err)
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
	missed, err := repo.getTxs(ctx, txHashes, txMetaSlice)
	if err != nil {
		return errors.New(errors.ERR_PROCESSING, "[writeTransactionsViaSubtreeStore][%s] failed to get tx meta from store", subtreeHash.String(), err)
	}

	if missed > 0 {
		return errors.New(errors.ERR_PROCESSING, "[writeTransactionsViaSubtreeStore][%s] missing tx meta", subtreeHash.String())
	}

	for i := 0; i < len(txMetaSlice); i++ {
		if model.CoinbasePlaceholderHash.Equal(txHashes[i]) {
			// The coinbase tx is not in the txmeta store so we add in a special coinbase placeholder tx
			if i != 0 {
				return errors.New(errors.ERR_PROCESSING, "[writeTransactionsViaSubtreeStore] coinbase tx is not first in subtree (%d)", i)
			}

			// Write coinbase tx
			if _, err := w.Write(block.CoinbaseTx.Bytes()); err != nil {
				return errors.New(errors.ERR_PROCESSING, "[writeTransactionsViaSubtreeStore] error writing coinbase tx: %w", err)
			}

		} else {

			// Write regular tx
			if _, err := w.Write(txMetaSlice[i].Tx.Bytes()); err != nil {
				return errors.New(errors.ERR_PROCESSING, "[writeTransactionsViaSubtreeStore] error writing tx[%d]: %v)", i, err)
			}
		}
	}

	return nil
}

func (repo *Repository) getTxs(ctx context.Context, txHashes []chainhash.Hash, txMetaSlice []*meta.Data) (int, error) {
	if len(txHashes) != len(txMetaSlice) {
		return 0, errors.New(errors.ERR_PROCESSING, "[processTxMetaUsingStore] txHashes and txMetaSlice must be the same length")
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

			if err := repo.UtxoStore.BatchDecorate(gCtx, missingTxHashesCompacted, "tx"); err != nil {
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
