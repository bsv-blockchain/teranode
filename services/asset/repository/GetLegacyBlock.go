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

// chunkResult holds the result of fetching a chunk of transactions from the UTXO store.
// Used for ordered fan-in: chunks are fetched in parallel but written in order.
type chunkResult struct {
	chunkIdx       int              // Index of this chunk for ordered reassembly
	chunkOffset    int              // Global offset of first tx in this chunk
	chunkHashes    []chainhash.Hash // Transaction hashes for this chunk
	chunkMetaSlice []*meta.Data     // Fetched transaction metadata
	err            error            // Error from fetching, if any
}

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

	returnWireBlock := len(wireBlock) > 0 && wireBlock[0]

	block, err := repo.GetBlockByHash(ctx, hash)
	if err != nil {
		releaseSemaphorePermit(repo.semGetLegacyBlockReader)
		return nil, err
	}

	r, w := io.Pipe()

	// Release semaphore after initial setup is complete but before streaming begins.
	// The semaphore protects the database query (GetBlockByHash) and pipe creation,
	// but streaming is I/O-bound and doesn't need CPU-based concurrency limiting.
	// File operations have their own semaphore protection (readSemaphore with 768 slots).
	// This allows unlimited concurrent streams while protecting database/initialization.
	releaseSemaphorePermit(repo.semGetLegacyBlockReader)

	g, gCtx := errgroup.WithContext(ctx)
	g.Go(func() (err error) {
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

		// Write the coinbase first before processing subtrees
		if _, err = w.Write(block.CoinbaseTx.Bytes()); err != nil {
			_ = w.CloseWithError(io.ErrClosedPipe)
			_ = r.CloseWithError(err)
			return errors.NewProcessingError("[GetLegacyBlockReader] error writing coinbase tx", err)
		}

		for subtreeIdx, subtreeHash := range block.Subtrees {
			subtreeDataExists, err = repo.SubtreeStore.Exists(ctx, subtreeHash[:], fileformat.FileTypeSubtreeData)
			if err == nil && subtreeDataExists {
				subtreeDataReader, err = repo.SubtreeStore.GetIoReader(ctx, subtreeHash[:], fileformat.FileTypeSubtreeData)
				if err != nil {
					_ = w.CloseWithError(io.ErrClosedPipe)
					_ = r.CloseWithError(err)

					return errors.NewProcessingError("[GetLegacyBlockReader] error getting subtree %s from store", subtreeHash.String(), err)
				}

				// create a buffered reader to read the subtree data
				// Using 32KB buffer for optimal sequential I/O with minimal memory overhead
				bufferedReader := bufio.NewReaderSize(subtreeDataReader, 32*1024)

				// process the subtree data streaming to the writer (non-coinbase transactions)
				for {
					tx := &bt.Tx{}

					// this will read the transaction into the tx object
					if _, err = tx.ReadFrom(bufferedReader); err != nil {
						if err == io.EOF {
							break
						}

						return errors.NewProcessingError("error reading transaction: %s", err)
					}

					// Skip if this is the coinbase transaction
					// Include the subtreeIdx check to avoid needing to do string comparison every iteration
					if subtreeIdx == 0 && tx.IsCoinbase() {
						continue
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

			// Use streaming method to minimize memory usage for large subtrees
			// Pass nil for block since coinbase was already written above
			if err = repo.writeTransactionsViaSubtreeStoreStreaming(gCtx, w, nil, subtreeHash); err != nil {
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

// writeTransactionsViaSubtreeStoreStreaming writes transactions from a subtree to a pipe writer in chunks
// to minimize memory usage. This method processes transactions in configurable chunk sizes rather than
// loading all transaction metadata into memory at once.
//
// This method is designed to handle large subtrees (1M+ transactions) efficiently by:
// 1. Loading only the lightweight subtree structure (transaction hashes)
// 2. Processing transactions in chunks (default: 10K per chunk)
// 3. Fetching each chunk from Aerospike/store and immediately writing to pipe
// 4. Releasing chunk memory before processing the next chunk
//
// This approach maintains constant memory usage (~5MB per chunk) regardless of subtree size,
// compared to the original method which would use ~500MB for 1M transactions.
//
// Parameters:
//   - ctx: Context for the operation
//   - w: Writer to stream transaction data to
//   - block: Optional block containing coinbase transaction (can be nil)
//   - subtreeHash: Hash of the subtree to process
//
// Returns:
//   - error: Any error encountered during processing
func (repo *Repository) writeTransactionsViaSubtreeStoreStreaming(ctx context.Context, w io.Writer, block *model.Block,
	subtreeHash *chainhash.Hash) error {
	// 1. Load subtree structure (lightweight - just hashes and tree structure)
	subtreeReader, err := repo.SubtreeStore.GetIoReader(ctx, subtreeHash.CloneBytes(), fileformat.FileTypeSubtree)
	if err != nil {
		subtreeReader, err = repo.SubtreeStore.GetIoReader(ctx, subtreeHash.CloneBytes(), fileformat.FileTypeSubtreeToCheck)
		if err != nil {
			return errors.NewProcessingError("[writeTransactionsViaSubtreeStoreStreaming] error getting subtree %s from store", subtreeHash.String(), err)
		}
	}

	defer func() {
		_ = subtreeReader.Close()
	}()

	subtree := subtreepkg.Subtree{}

	if err = subtree.DeserializeFromReader(subtreeReader); err != nil {
		return errors.NewProcessingError("[writeTransactionsViaSubtreeStoreStreaming] error deserializing subtree", err)
	}

	totalTxs := len(subtree.Nodes)
	chunkSize := repo.settings.Asset.SubtreeDataStreamingChunkSize
	concurrency := repo.settings.Asset.SubtreeDataStreamingConcurrency
	if concurrency <= 0 {
		concurrency = 4
	}

	// 2. Calculate number of chunks
	numChunks := (totalTxs + chunkSize - 1) / chunkSize
	if numChunks == 0 {
		return nil
	}

	// 3. Create buffered results channel for fan-in
	resultsChan := make(chan chunkResult, numChunks)

	// 4. Launch chunk fetch goroutines with limited concurrency
	g, gCtx := errgroup.WithContext(ctx)
	util.SafeSetLimit(g, concurrency)

	for chunkIdx := 0; chunkIdx < numChunks; chunkIdx++ {
		chunkIdx := chunkIdx // capture for goroutine
		offset := chunkIdx * chunkSize

		g.Go(func() error {
			// Check for context cancellation
			select {
			case <-gCtx.Done():
				return gCtx.Err()
			default:
			}

			// Calculate chunk boundaries
			end := offset + chunkSize
			if end > totalTxs {
				end = totalTxs
			}
			currentChunkSize := end - offset

			// Extract chunk of transaction hashes from subtree
			chunkHashes := make([]chainhash.Hash, currentChunkSize)
			for i := 0; i < currentChunkSize; i++ {
				chunkHashes[i] = subtree.Nodes[offset+i].Hash
			}

			// Allocate memory for chunk only
			chunkMetaSlice := make([]*meta.Data, currentChunkSize)

			// Fetch chunk from store
			missed, fetchErr := repo.getTxs(gCtx, chunkHashes, chunkMetaSlice)
			if fetchErr != nil {
				resultsChan <- chunkResult{chunkIdx: chunkIdx, err: errors.NewProcessingError("[writeTransactionsViaSubtreeStoreStreaming][%s] failed to get tx meta from store for chunk at offset %d", subtreeHash.String(), offset, fetchErr)}
				return fetchErr
			}

			if missed > 0 {
				// Log which transactions are missing in this chunk
				ctxLogger := repo.logger.WithTraceContext(gCtx)
				for i := 0; i < currentChunkSize; i++ {
					if subtreepkg.CoinbasePlaceholderHash.Equal(chunkHashes[i]) {
						continue
					}
					if chunkMetaSlice[i] == nil || chunkMetaSlice[i].Tx == nil {
						ctxLogger.Errorf("[writeTransactionsViaSubtreeStoreStreaming][%s] failed to get tx meta from store for tx %s at offset %d", subtreeHash.String(), chunkHashes[i].String(), offset+i)
					}
				}
				missErr := errors.NewProcessingError("[writeTransactionsViaSubtreeStoreStreaming][%s] failed to get %d of %d tx meta from store in chunk at offset %d", subtreeHash.String(), missed, currentChunkSize, offset)
				resultsChan <- chunkResult{chunkIdx: chunkIdx, err: missErr}
				return missErr
			}

			// Send successful result
			resultsChan <- chunkResult{
				chunkIdx:       chunkIdx,
				chunkOffset:    offset,
				chunkHashes:    chunkHashes,
				chunkMetaSlice: chunkMetaSlice,
			}
			return nil
		})
	}

	// 5. Close results channel when all fetches complete
	go func() {
		_ = g.Wait()
		close(resultsChan)
	}()

	// 6. Ordered consumer: collect results and write in chunk order (0, 1, 2, ...)
	pending := make(map[int]chunkResult)
	nextChunk := 0

	for result := range resultsChan {
		// Check for fetch error
		if result.err != nil {
			// Drain remaining results and return error
			for range resultsChan {
			}
			return result.err
		}

		pending[result.chunkIdx] = result

		// Drain all consecutive chunks starting from nextChunk
		for {
			chunk, ok := pending[nextChunk]
			if !ok {
				break
			}

			// Write this chunk to the pipe
			if err = repo.writeChunkToWriter(w, block, chunk.chunkHashes, chunk.chunkMetaSlice, chunk.chunkOffset); err != nil {
				// Drain remaining results and return error
				for range resultsChan {
					// do nothing, just drain the channel
				}
				return err
			}

			delete(pending, nextChunk)
			nextChunk++
		}
	}

	// 7. Check for errgroup errors after channel is drained
	if err = g.Wait(); err != nil {
		return err
	}

	return nil
}

// writeChunkToWriter writes a chunk of transactions to the writer in order.
func (repo *Repository) writeChunkToWriter(w io.Writer, block *model.Block,
	chunkHashes []chainhash.Hash, chunkMetaSlice []*meta.Data, chunkOffset int) error {
	for i := 0; i < len(chunkHashes); i++ {
		if subtreepkg.CoinbasePlaceholderHash.Equal(chunkHashes[i]) {
			if block != nil {
				// The coinbase tx is not in the txmeta store, so we add in a special coinbase placeholder tx
				if chunkOffset+i != 0 {
					return errors.NewProcessingError("[writeChunkToWriter] coinbase tx is not first in subtree (index %d)", chunkOffset+i)
				}

				// Write coinbase tx
				if _, err := w.Write(block.CoinbaseTx.Bytes()); err != nil {
					return errors.NewProcessingError("[writeChunkToWriter] error writing coinbase tx", err)
				}
			}
			continue
		} else {
			// always write the non-extended normal bytes to the subtree data file !
			// our peer node should extend the transactions if needed
			if _, err := w.Write(chunkMetaSlice[i].Tx.Bytes()); err != nil {
				return errors.NewProcessingError("[writeChunkToWriter] error writing tx at offset %d", chunkOffset+i, err)
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
