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
			if _, err = block.CoinbaseTx.WriteTo(w); err != nil {
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
		if _, err = block.CoinbaseTx.WriteTo(w); err != nil {
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
					if _, err = tx.WriteTo(w); err != nil {
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
// to minimize memory usage. This method streams subtree node records from storage and keeps only a bounded
// number of transaction chunks in flight.
//
// This method is designed to handle large subtrees (1M+ transactions) efficiently by:
// 1. Reading only one chunk of subtree node hashes at a time
// 2. Fetching chunks from Aerospike/store with bounded concurrency
// 3. Writing completed chunks in subtree order
// 4. Releasing chunk memory as soon as the chunk is written
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
	subtreeReader, err := repo.GetSubtreeTxIDsReader(ctx, subtreeHash)
	if err != nil {
		return errors.NewProcessingError("[writeTransactionsViaSubtreeStoreStreaming] error getting subtree %s from store", subtreeHash.String(), err)
	}

	defer func() {
		_ = subtreeReader.Close()
	}()

	bufferedReader := bufio.NewReaderSize(subtreeReader, subtreeStreamBufferSize)
	header, err := readSubtreeStreamHeader(bufferedReader)
	if err != nil {
		return errors.NewProcessingError("[writeTransactionsViaSubtreeStoreStreaming] error reading subtree header", err)
	}

	totalTxs, err := safeconversion.Uint64ToInt(header.numLeaves)
	if err != nil {
		return errors.NewProcessingError("[writeTransactionsViaSubtreeStoreStreaming] error converting subtree node count", err)
	}
	if totalTxs == 0 {
		return nil
	}

	chunkSize := repo.settings.Asset.SubtreeDataStreamingChunkSize
	if chunkSize <= 0 {
		chunkSize = 10000
	}

	concurrency := repo.settings.Asset.SubtreeDataStreamingConcurrency
	if concurrency <= 0 {
		concurrency = 4
	}

	streamCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	resultsChan := make(chan chunkResult, concurrency)
	g, gCtx := errgroup.WithContext(streamCtx)
	g.Go(func() error {
		defer close(resultsChan)
		return repo.scheduleSubtreeChunkFetches(gCtx, bufferedReader, subtreeHash, totalTxs, chunkSize, concurrency, resultsChan)
	})

	pending := make(map[int]chunkResult)
	nextChunk := 0

	// Defensive cap on out-of-order chunks held in memory. With the current scheduler
	// this should never exceed `concurrency`, but a future change to the scheduler could
	// silently grow it. Aborting beats OOM. 2x leaves headroom for transient races.
	pendingCap := 2 * concurrency

	for result := range resultsChan {
		if result.err != nil {
			cancel()
			drainChunkResults(resultsChan)
			_ = g.Wait()
			return result.err
		}

		pending[result.chunkIdx] = result
		if len(pending) > pendingCap {
			cancel()
			drainChunkResults(resultsChan)
			_ = g.Wait()
			return errors.NewProcessingError("[writeTransactionsViaSubtreeStoreStreaming][%s] pending chunk buffer exceeded cap %d (likely scheduler regression)",
				subtreeHash.String(), pendingCap)
		}

		for {
			chunk, ok := pending[nextChunk]
			if !ok {
				break
			}

			if err = repo.writeChunkToWriter(streamCtx, w, block, chunk.chunkHashes, chunk.chunkMetaSlice, chunk.chunkOffset); err != nil {
				cancel()
				drainChunkResults(resultsChan)
				_ = g.Wait()
				return err
			}

			delete(pending, nextChunk)
			nextChunk++
		}
	}

	if err = g.Wait(); err != nil {
		return err
	}

	return nil
}

func (repo *Repository) scheduleSubtreeChunkFetches(ctx context.Context, subtreeReader *bufio.Reader, subtreeHash *chainhash.Hash,
	totalTxs, chunkSize, concurrency int, resultsChan chan<- chunkResult) error {
	fetchCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	g, gCtx := errgroup.WithContext(fetchCtx)
	util.SafeSetLimit(g, concurrency)

	var readErr error
	for chunkIdx, offset := 0, 0; offset < totalTxs; chunkIdx, offset = chunkIdx+1, offset+chunkSize {
		if readErr = gCtx.Err(); readErr != nil {
			cancel()
			break
		}

		currentChunkSize := min(chunkSize, totalTxs-offset)
		chunkHashes, err := readSubtreeHashChunk(gCtx, subtreeReader, currentChunkSize)
		if err != nil {
			readErr = err
			cancel()
			break
		}

		chunkIdx := chunkIdx
		offset := offset
		chunkHashesForWorker := chunkHashes
		g.Go(func() error {
			return repo.fetchSubtreeChunk(gCtx, subtreeHash, chunkIdx, offset, chunkHashesForWorker, resultsChan)
		})
	}

	waitErr := g.Wait()
	// When a worker fails, the errgroup cancels gCtx, which makes the next
	// loop iteration observe context.Canceled and store it in readErr. The
	// real failure is the worker's error returned by g.Wait(); prefer it so
	// callers see the cause rather than the cancellation signal it triggered.
	if waitErr != nil && !errors.Is(waitErr, context.Canceled) {
		return waitErr
	}
	if readErr != nil {
		return readErr
	}

	return waitErr
}

func (repo *Repository) fetchSubtreeChunk(ctx context.Context, subtreeHash *chainhash.Hash, chunkIdx, offset int,
	chunkHashes []chainhash.Hash, resultsChan chan<- chunkResult) error {
	chunkMetaSlice := make([]*meta.Data, len(chunkHashes))

	missed, fetchErr := repo.getTxs(ctx, chunkHashes, chunkMetaSlice)
	if fetchErr != nil {
		resultErr := errors.NewProcessingError("[writeTransactionsViaSubtreeStoreStreaming][%s] failed to get tx meta from store for chunk at offset %d", subtreeHash.String(), offset, fetchErr)
		return sendChunkResult(ctx, resultsChan, chunkResult{chunkIdx: chunkIdx, err: resultErr}, fetchErr)
	}

	if missed > 0 {
		ctxLogger := repo.logger.WithTraceContext(ctx)
		for i := 0; i < len(chunkHashes); i++ {
			if subtreepkg.CoinbasePlaceholderHash.Equal(chunkHashes[i]) {
				continue
			}
			if chunkMetaSlice[i] == nil || chunkMetaSlice[i].Tx == nil {
				ctxLogger.Errorf("[writeTransactionsViaSubtreeStoreStreaming][%s] failed to get tx meta from store for tx %s at offset %d", subtreeHash.String(), chunkHashes[i].String(), offset+i)
			}
		}

		missErr := errors.NewProcessingError("[writeTransactionsViaSubtreeStoreStreaming][%s] failed to get %d of %d tx meta from store in chunk at offset %d", subtreeHash.String(), missed, len(chunkHashes), offset)
		return sendChunkResult(ctx, resultsChan, chunkResult{chunkIdx: chunkIdx, err: missErr}, missErr)
	}

	return sendChunkResult(ctx, resultsChan, chunkResult{
		chunkIdx:       chunkIdx,
		chunkOffset:    offset,
		chunkHashes:    chunkHashes,
		chunkMetaSlice: chunkMetaSlice,
	}, nil)
}

func sendChunkResult(ctx context.Context, resultsChan chan<- chunkResult, result chunkResult, returnErr error) error {
	select {
	case resultsChan <- result:
		return returnErr
	case <-ctx.Done():
		return ctx.Err()
	}
}

func drainChunkResults(resultsChan <-chan chunkResult) {
	for range resultsChan {
	}
}

// writeChunkToWriter writes a chunk of transactions to the writer in order.
//
// The ctx check between writes lets the loop abort promptly when the caller's context is
// cancelled (e.g. HTTP client disconnect). io.MultiWriter / pipe writes are not ctx-aware
// on their own, so without this check we'd keep producing tx bytes until the first write
// happens to land on a closed pipe — meanwhile retaining chunkMetaSlice in heap.
func (repo *Repository) writeChunkToWriter(ctx context.Context, w io.Writer, block *model.Block,
	chunkHashes []chainhash.Hash, chunkMetaSlice []*meta.Data, chunkOffset int) error {
	const ctxCheckEvery = 256

	for i := 0; i < len(chunkHashes); i++ {
		if i%ctxCheckEvery == 0 {
			if err := ctx.Err(); err != nil {
				return err
			}
		}
		if subtreepkg.CoinbasePlaceholderHash.Equal(chunkHashes[i]) {
			if block != nil {
				// The coinbase tx is not in the txmeta store, so we add in a special coinbase placeholder tx
				if chunkOffset+i != 0 {
					return errors.NewProcessingError("[writeChunkToWriter] coinbase tx is not first in subtree (index %d)", chunkOffset+i)
				}

				// Write coinbase tx
				if _, err := block.CoinbaseTx.WriteTo(w); err != nil {
					return errors.NewProcessingError("[writeChunkToWriter] error writing coinbase tx", err)
				}
			}
			continue
		} else {
			// always write the non-extended normal bytes to the subtree data file !
			// our peer node should extend the transactions if needed
			if _, err := chunkMetaSlice[i].Tx.WriteTo(w); err != nil {
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
