// Package repository provides access to blockchain data storage and retrieval operations.
// It implements the necessary interfaces to interact with various data stores and
// blockchain clients.
package repository

import (
	"bufio"
	"context"
	"encoding/binary"
	"io"
	"sync/atomic"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	safeconversion "github.com/bsv-blockchain/go-safe-conversion"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/meta"
	"github.com/bsv-blockchain/teranode/util"
	"github.com/bsv-blockchain/teranode/util/tracing"
	"golang.org/x/sync/errgroup"
)

// GetLegacyBlockReader provides a reader interface for retrieving block data in legacy format.
// It streams block data including header, transactions, and subtrees.
//
// Parameters:
//   - ctx: Context for the operation
//   - hash: Hash of the block to retrieve
//
// Returns:
//   - *io.PipeReader: Reader for streaming block data
//   - error: Any error encountered during retrieval
func (repo *Repository) GetLegacyBlockReader(ctx context.Context, hash *chainhash.Hash, wireBlock ...bool) (*io.PipeReader, error) {
	if err := acquireSemaphorePermit(ctx, repo.semGetLegacyBlockReader, "GetLegacyBlockReader"); err != nil {
		return nil, err
	}
	// Note: semaphore will be released when the returned reader is closed

	returnWireBlock := len(wireBlock) > 0 && wireBlock[0]

	block, err := repo.GetBlockByHash(ctx, hash)
	if err != nil {
		releaseSemaphorePermit(repo.semGetLegacyBlockReader)
		return nil, err
	}

	r, w := io.Pipe()

	g, gCtx := errgroup.WithContext(ctx)
	g.Go(func() (err error) {
		// Release semaphore when goroutine completes (after all Aerospike reads are done)
		defer releaseSemaphorePermit(repo.semGetLegacyBlockReader)

		if err = repo.writeLegacyBlockHeader(w, block, returnWireBlock); err != nil {
			_ = w.CloseWithError(io.ErrClosedPipe)
			_ = r.CloseWithError(err)

			return err
		}

		if len(block.Subtrees) == 0 {
			// Write the coinbase tx
			if _, err = w.Write(block.CoinbaseTx.Bytes()); err != nil {
				_ = w.CloseWithError(io.ErrClosedPipe)
				_ = r.CloseWithError(err)

				return err
			}

			// close the writer after the coinbase tx has been streamed
			_ = w.CloseWithError(io.ErrClosedPipe)

			return nil
		}

		var (
			subtreeDataExists bool
			subtreeDataReader io.ReadCloser
		)

		for subtreeIdx, subtreeHash := range block.Subtrees {
			subtreeDataExists, err = repo.SubtreeStore.Exists(ctx, subtreeHash[:], fileformat.FileTypeSubtreeData)
			if err == nil && subtreeDataExists {
				subtreeDataReader, err = repo.SubtreeStore.GetIoReader(ctx, subtreeHash[:], fileformat.FileTypeSubtreeData)
				if err != nil {
					_ = w.CloseWithError(io.ErrClosedPipe)
					_ = r.CloseWithError(err)

					return errors.NewProcessingError("[GetLegacyBlockReader] error getting subtree %s from store", subtreeHash.String(), err)
				}

				// make sure we include the coinbase tx in the first subtree
				if subtreeIdx == 0 && block.CoinbaseTx != nil {
					// Write the coinbase tx first
					if _, err = w.Write(block.CoinbaseTx.Bytes()); err != nil {
						_ = w.CloseWithError(io.ErrClosedPipe)
						_ = r.CloseWithError(err)

						return errors.NewProcessingError("error writing coinbase transaction to writer: %s", err)
					}
				}

				// create a buffered reader to read the subtree data
				// Using 32KB buffer for optimal sequential I/O with minimal memory overhead
				bufferedReader := bufio.NewReaderSize(subtreeDataReader, 32*1024)

				// process the subtree data streaming to the writer
				for {
					tx := &bt.Tx{}

					// this will read the transaction into the tx object
					if _, err = tx.ReadFrom(bufferedReader); err != nil {
						if err == io.EOF {
							break
						}

						return errors.NewProcessingError("error reading transaction: %s", err)
					}

					// Write the normal transaction bytes to the writer
					if _, err = w.Write(tx.Bytes()); err != nil {
						_ = w.CloseWithError(io.ErrClosedPipe)
						_ = r.CloseWithError(err)

						return errors.NewProcessingError("error writing transaction to writer: %s", err)
					}
				}

				// close the subtree data reader after processing all transactions
				_ = subtreeDataReader.Close()

				// move to the next subtree
				continue
			}

			if err = repo.writeTransactionsViaSubtreeStore(gCtx, w, block, subtreeHash); err != nil {
				_ = w.CloseWithError(io.ErrClosedPipe)
				_ = r.CloseWithError(err)

				return err
			}
		}

		// close the writer after all subtrees have been streamed
		_ = w.CloseWithError(io.ErrClosedPipe)

		return nil
	})

	return r, nil
}

// writeLegacyBlockHeader writes a block header in legacy format to the provided writer.
//
// Parameters:
//   - block: Block containing the header to write
//   - w: Writer to write the header to
//
// Returns:
//   - error: Any error encountered during writing
func (repo *Repository) writeLegacyBlockHeader(w io.Writer, block *model.Block, returnWireBlock bool) error {
	txCountVarInt := bt.VarInt(block.TransactionCount)
	txCountVarIntLen := txCountVarInt.Length()

	if !returnWireBlock {
		// write bitcoin block magic number
		if _, err := w.Write([]byte{0xf9, 0xbe, 0xb4, 0xd9}); err != nil {
			return err
		}

		// write the block size
		sizeInBytes := make([]byte, 4)

		blockHeaderTransactionCountSizeUint64, err := safeconversion.IntToUint64(model.BlockHeaderSize + txCountVarIntLen)
		if err != nil {
			return err
		}

		sizeUint32, err := safeconversion.Uint64ToUint32(block.SizeInBytes + blockHeaderTransactionCountSizeUint64)
		if err != nil {
			return err
		}

		binary.LittleEndian.PutUint32(sizeInBytes, sizeUint32)

		if _, err := w.Write(sizeInBytes); err != nil {
			return err
		}
	}

	// write the 80 byte block header
	if _, err := w.Write(block.Header.Bytes()); err != nil {
		return err
	}

	// write number of transactions
	if _, err := w.Write(txCountVarInt.Bytes()); err != nil {
		return err
	}

	return nil
}

// writeTransactionsViaSubtreeStore writes transactions from the subtree store to the provided writer.
// This is used as a fallback when transactions are not available in the block store.
//
// Parameters:
//   - ctx: Context for the operation
//   - block: Block containing the coinbase transaction (optional)
//   - subtreeHash: Hash of the subtree containing transaction information
//   - w: Writer to write the transactions to
//
// Returns:
//   - error: Any error encountered during writing
func (repo *Repository) writeTransactionsViaSubtreeStore(ctx context.Context, w *io.PipeWriter, block *model.Block,
	subtreeHash *chainhash.Hash) error {
	subtreeReader, err := repo.SubtreeStore.GetIoReader(ctx, subtreeHash.CloneBytes(), fileformat.FileTypeSubtree)
	if err != nil {
		subtreeReader, err = repo.SubtreeStore.GetIoReader(ctx, subtreeHash.CloneBytes(), fileformat.FileTypeSubtreeToCheck)
		if err != nil {
			return errors.NewProcessingError("[writeTransactionsViaSubtreeStore] error getting subtree %s from store", subtreeHash.String(), err)
		}
	}

	defer func() {
		_ = subtreeReader.Close()
	}()

	subtree := subtreepkg.Subtree{}

	if err = subtree.DeserializeFromReader(subtreeReader); err != nil {
		return errors.NewProcessingError("[writeTransactionsViaSubtreeStore] error deserializing subtree", err)
	}

	// 1. create a slice of MissingTxHashes for all the txs in the subtree
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
		return errors.NewProcessingError("[writeTransactionsViaSubtreeStore][%s] failed to get tx meta from store", subtreeHash.String(), err)
	}

	if missed > 0 {
		for i := 0; i < len(txHashes); i++ {
			if subtreepkg.CoinbasePlaceholderHash.Equal(txHashes[i]) {
				continue
			}

			if txMetaSlice[i] == nil || txMetaSlice[i].Tx == nil {
				repo.logger.Errorf("[writeTransactionsViaSubtreeStore][%s] failed to get tx meta from store for tx %s", subtreeHash.String(), txHashes[i].String())
			}
		}

		return errors.NewProcessingError("[writeTransactionsViaSubtreeStore][%s] failed to get %d of %d tx meta from store", subtreeHash.String(), missed, len(txHashes))
	}

	for i := 0; i < len(txMetaSlice); i++ {
		if subtreepkg.CoinbasePlaceholderHash.Equal(txHashes[i]) {
			if block != nil {
				// The coinbase tx is not in the txmeta store, so we add in a special coinbase placeholder tx
				if i != 0 {
					return errors.NewProcessingError("[writeTransactionsViaSubtreeStore] coinbase tx is not first in subtree (%d)", i)
				}

				// Write coinbase tx
				if _, err = w.Write(block.CoinbaseTx.Bytes()); err != nil {
					return errors.NewProcessingError("[writeTransactionsViaSubtreeStore] error writing coinbase tx", err)
				}
			}
		} else {
			// always write the non-extended normal bytes to the subtree data file !
			// our peer node should extend the transactions if needed
			if _, err = w.Write(txMetaSlice[i].Tx.Bytes()); err != nil {
				return errors.NewProcessingError("[writeTransactionsViaSubtreeStore] error writing tx[%d])", i, err)
			}
		}
	}

	return nil
}

// getTxs retrieves transaction metadata for a batch of transactions.
// It supports concurrent retrieval of transaction data and handles missing transactions.
//
// Parameters:
//   - ctx: Context for the operation
//   - txHashes: Array of transaction hashes to retrieve
//   - txMetaSlice: Slice to store retrieved transaction metadata
//
// Returns:
//   - int: Number of missing transactions
//   - error: Any error encountered during retrieval
func (repo *Repository) getTxs(ctx context.Context, txHashes []chainhash.Hash, txMetaSlice []*meta.Data) (int, error) {
	if len(txHashes) != len(txMetaSlice) {
		return 0, errors.NewProcessingError("[processTxMetaUsingStore] txHashes and txMetaSlice must be the same length")
	}

	ctx, _, deferFn := tracing.Tracer("repository").Start(ctx, "getTxs")
	defer deferFn()

	batchSize := repo.settings.BlockValidation.ProcessTxMetaUsingStoreBatchSize
	processSubtreeConcurrency := repo.settings.BlockValidation.ProcessTxMetaUsingStoreConcurrency

	g, gCtx := errgroup.WithContext(ctx)
	util.SafeSetLimit(g, processSubtreeConcurrency)

	var missed atomic.Int32

	for i := 0; i < len(txHashes); i += batchSize {
		i := i // capture range variable for goroutine

		g.Go(func() error {
			end := subtreepkg.Min(i+batchSize, len(txHashes))

			missingTxHashesCompacted := make([]*utxo.UnresolvedMetaData, 0, end-i)

			for j := 0; j < subtreepkg.Min(batchSize, len(txHashes)-i); j++ {
				select {
				case <-gCtx.Done(): // Listen for cancellation signal
					return gCtx.Err() // Return the error that caused the cancellation

				default:
					if txHashes[i+j].Equal(*subtreepkg.CoinbasePlaceholderHash) {
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
