// Package blockpersister provides comprehensive functionality for persisting blockchain blocks and their associated data.
// It handles block persistence, transaction processing, and UTXO set management across multiple storage backends.
package blockpersister

import (
	"bufio"
	"context"
	"runtime"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/services/utxopersister"
	"github.com/bsv-blockchain/teranode/services/utxopersister/filestorer"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/bsv-blockchain/teranode/util"
	"github.com/bsv-blockchain/teranode/util/bytesize"
	"github.com/bsv-blockchain/teranode/util/tracing"
	"golang.org/x/sync/errgroup"
)

// CreateSubtreeDataFileStreaming creates a subtreeData file using streaming writes.
// It fetches transactions in parallel batches and writes them to disk in order without
// loading all transactions into memory at once. This significantly reduces memory usage
// for large subtrees.
//
// The function implements a fan-in pattern:
//  1. Check if subtreeData file already exists - skip if yes
//  2. Read the subtree structure to get transaction hashes
//  3. Create a SubtreeDataWriter backed by FileStorer
//  4. Launch parallel batch fetchers that retrieve tx data via BatchDecorate
//  5. Each fetcher serializes transactions and sends to SubtreeDataWriter
//  6. SubtreeDataWriter ensures transactions are written in correct order
//  7. Close writer to flush buffers and finalize file
//
// Parameters:
//   - ctx: Context for the operation, used for cancellation and tracing
//   - subtreeHash: Hash identifier of the subtree to process
//   - coinbaseTx: The coinbase transaction for the block (used if first tx is placeholder)
//
// Returns an error if any part of the process fails.
func (u *Server) CreateSubtreeDataFileStreaming(ctx context.Context, subtreeHash chainhash.Hash, block *model.Block, n int) error {
	ctx, _, deferFn := tracing.Tracer("blockpersister").Start(ctx, "CreateSubtreeDataFileStreaming",
		tracing.WithHistogram(prometheusBlockPersisterSubtrees),
		tracing.WithLogMessage(u.logger, "[CreateSubtreeDataFileStreaming][%s] creating subtreeData %d / %d for [%s]", block.String(), n, len(block.Subtrees), subtreeHash.String()),
	)
	defer deferFn()

	// 1. Check if subtreeData file already exists
	subtreeDataExists, err := u.subtreeStore.Exists(ctx, subtreeHash.CloneBytes(), fileformat.FileTypeSubtreeData)
	if err != nil {
		return errors.NewStorageError("[BlockPersister] error checking if subtree data exists for %s", subtreeHash.String(), err)
	}

	// 2. Read the subtree structure
	subtree, err := u.readSubtree(ctx, subtreeHash)
	if err != nil {
		return err
	}

	if subtreeDataExists {
		// verify that the subtreeData file is valid (non-zero size) and all transactions can be read
		subtreeDataReader, err := u.subtreeStore.GetIoReader(ctx, subtreeHash.CloneBytes(), fileformat.FileTypeSubtreeData)
		if err != nil {
			return errors.NewStorageError("[BlockPersister] error getting existing subtree data for %s", subtreeHash.String(), err)
		}
		defer subtreeDataReader.Close()

		subtreeDataBufferedReader := bufio.NewReaderSize(subtreeDataReader, 32*1024) // 32KB buffer

		if _, err = subtreepkg.NewSubtreeDataFromReader(subtree, subtreeDataBufferedReader); err != nil {
			// something failed reading the subtreeData, we need to recreate it
			u.logger.Warnf("[BlockPersister] existing subtree data for %s is invalid, recreating: %v", subtreeHash.String(), err)

			// delete the invalid file
			if err = u.subtreeStore.Del(ctx, subtreeHash.CloneBytes(), fileformat.FileTypeSubtreeData); err != nil {
				return errors.NewStorageError("[BlockPersister] error deleting invalid subtree data for %s", subtreeHash.String(), err)
			}
		} else {
			// File exists and is correct, just update DAH and return
			err = u.subtreeStore.SetDAH(ctx, subtreeHash.CloneBytes(), fileformat.FileTypeSubtreeData, 0)
			if err != nil {
				return errors.NewStorageError("[BlockPersister] error setting subtree data DAH for %s", subtreeHash.String(), err)
			}

			// Also promote .subtree to permanent alongside .subtreeData
			if err = u.subtreeStore.SetDAH(ctx, subtreeHash.CloneBytes(), fileformat.FileTypeSubtree, 0); err != nil {
				return errors.NewStorageError("[BlockPersister] error setting subtree DAH for %s", subtreeHash.String(), err)
			}

			u.logger.Debugf("[BlockPersister] Subtree data for %s already exists, skipping creation", subtreeHash.String())

			return nil
		}
	}

	// 3. Create FileStorer for streaming writes
	storer, err := filestorer.NewFileStorer(ctx, u.logger, u.settings, u.subtreeStore, subtreeHash.CloneBytes(), fileformat.FileTypeSubtreeData)
	if err != nil {
		return errors.NewStorageError("[BlockPersister] error creating file storer for %s", subtreeHash.String(), err)
	}

	// Create SubtreeDataWriter for ordered writes
	writer := NewSubtreeDataWriter(storer)

	// Track whether write succeeded to determine whether to close or abort
	var writeSucceeded bool
	defer func() {
		if writeSucceeded {
			// Success path handled by writer.Close() below, storer already closed
			return
		}
		// Error path - abort to prevent incomplete file from being finalized
		writer.Abort(errors.NewProcessingError("[CreateSubtreeDataFileStreaming] write failed for subtree %s", subtreeHash.String()))
	}()

	// 4. Launch parallel batch fetchers with errgroup
	batchSize := u.settings.Block.ProcessTxMetaUsingStoreBatchSize
	concurrency := subtreepkg.Max(4, runtime.NumCPU()/2)

	g, gCtx := errgroup.WithContext(ctx)
	util.SafeSetLimit(g, concurrency)

	subtreeLen := subtree.Length()

	// Process batches in parallel
	for batchIndex := 0; batchIndex < (subtreeLen+batchSize-1)/batchSize; batchIndex++ {
		batchIndex := batchIndex
		batchStartIdx := batchIndex * batchSize

		g.Go(func() error {
			batchEndIdx := subtreepkg.Min(batchStartIdx+batchSize, subtreeLen)

			// Collect transaction hashes that need fetching
			missingTxHashesCompacted := make([]*utxo.UnresolvedMetaData, 0, batchEndIdx-batchStartIdx)

			// Create map for O(1) lookup
			txDataByIdx := make(map[int]*utxo.UnresolvedMetaData, batchEndIdx-batchStartIdx)

			for j := batchStartIdx; j < batchEndIdx; j++ {
				if subtree.Nodes[j].Hash.Equal(*subtreepkg.CoinbasePlaceholderHash) {
					// Coinbase placeholder - we'll handle this specially
					continue
				}

				txDataByIdx[j] = &utxo.UnresolvedMetaData{
					Hash: subtree.Nodes[j].Hash,
					Idx:  j,
				}

				missingTxHashesCompacted = append(missingTxHashesCompacted, txDataByIdx[j])
			}

			// Fetch transaction metadata in batch
			if len(missingTxHashesCompacted) > 0 {
				if err := u.utxoStore.BatchDecorate(gCtx, missingTxHashesCompacted, fields.Tx); err != nil {
					return errors.NewStorageError("[CreateSubtreeDataFileStreaming] failed to batch decorate for subtree %s", subtreeHash.String(), err)
				}
			}

			// Serialize transactions in order for this batch
			txBytes := make([][]byte, 0, batchEndIdx-batchStartIdx)

			var txData []byte

			for j := batchStartIdx; j < batchEndIdx; j++ {
				if subtree.Nodes[j].Hash.Equal(*subtreepkg.CoinbasePlaceholderHash) {
					// Use coinbase transaction
					if block.CoinbaseTx == nil {
						return errors.NewProcessingError("[CreateSubtreeDataFileStreaming] coinbase placeholder at index %d but no coinbase tx provided", j)
					}
					txData = block.CoinbaseTx.Bytes()
				} else {
					// Find the transaction in our fetched results
					data, found := txDataByIdx[j]
					if !found {
						return errors.NewProcessingError("[CreateSubtreeDataFileStreaming] transaction not found for index %d", j)
					}

					if data.Data == nil || data.Err != nil {
						return errors.NewProcessingError("[CreateSubtreeDataFileStreaming] failed to retrieve transaction metadata for hash %s at index %d", data.Hash, j, data.Err)
					}

					if data.Data.Tx == nil {
						return errors.NewProcessingError("[CreateSubtreeDataFileStreaming] transaction is nil for hash %s at index %d", data.Hash, j)
					}

					txData = data.Data.Tx.Bytes()
				}

				txBytes = append(txBytes, txData)
			}

			// Write this batch to the SubtreeDataWriter
			// The writer will ensure batches are written in order
			return writer.WriteBatch(batchIndex, txBytes)
		})
	}

	// Wait for all batches to complete
	if err = g.Wait(); err != nil {
		return err
	}

	// 7. Close writer to flush buffers and finalize file
	if err = writer.Close(ctx); err != nil {
		return errors.NewStorageError("[BlockPersister] error closing subtree data writer for %s", subtreeHash.String(), err)
	}

	// Mark as successful so defer doesn't abort
	writeSucceeded = true

	// Promote .subtree to permanent now that the block is confirmed on the main chain
	if err = u.subtreeStore.SetDAH(ctx, subtreeHash.CloneBytes(), fileformat.FileTypeSubtree, 0); err != nil {
		return errors.NewStorageError("[BlockPersister] error setting subtree DAH for %s", subtreeHash.String(), err)
	}

	return nil
}

// ProcessSubtreeUTXOStreaming processes UTXO changes by streaming through an existing subtreeData file.
// Instead of loading all transactions into memory, it reads them one by one from disk and
// processes each through the UTXO diff tracker.
//
// The function:
//  1. Opens the subtreeData file as a reader with buffering
//  2. Reads the subtree structure (to handle coinbase)
//  3. Streams through transactions using bt.Tx.ReadFrom
//  4. Processes each transaction through utxoDiff.ProcessTx
//  5. Closes reader when done
//
// Memory usage is minimal - only one transaction in memory at a time.
//
// Parameters:
//   - ctx: Context for the operation
//   - subtreeHash: Hash identifier of the subtree to process
//   - utxoDiff: UTXO set difference tracker to record changes
//
// Returns an error if any part of the process fails.
func (u *Server) ProcessSubtreeUTXOStreaming(ctx context.Context, subtreeHash chainhash.Hash, utxoDiff *utxopersister.UTXOSet) error {
	ctx, _, deferFn := tracing.Tracer("blockpersister").Start(ctx, "ProcessSubtreeUTXOStreaming",
		tracing.WithDebugLogMessage(u.logger, "[ProcessSubtreeUTXOStreaming] called for subtree %s", subtreeHash.String()),
	)
	defer deferFn()

	// 1. Open subtreeData file as reader
	subtreeDataReader, err := u.subtreeStore.GetIoReader(ctx, subtreeHash.CloneBytes(), fileformat.FileTypeSubtreeData)
	if err != nil {
		return errors.NewStorageError("[BlockPersister] error getting subtree data for %s from store", subtreeHash.String(), err)
	}
	defer subtreeDataReader.Close()

	// Wrap in buffered reader for efficient I/O
	utxopersisterBufferSize := u.settings.Block.UTXOPersisterBufferSize
	bufferSize, err := bytesize.Parse(utxopersisterBufferSize)
	if err != nil {
		u.logger.Errorf("error parsing utxoPersister_buffer_size %q: %v", utxopersisterBufferSize, err)
		bufferSize = 1024 * 128 // default to 128KB
	}

	bufferedReader := bufio.NewReaderSize(subtreeDataReader, bufferSize.Int())

	// 2. Read the subtree structure (needed to know how many transactions)
	subtree, err := u.readSubtree(ctx, subtreeHash)
	if err != nil {
		return err
	}

	// 3. Stream through transactions and process UTXO changes
	subtreeLen := subtree.Length()

	for i := 0; i < subtreeLen; i++ {
		tx := &bt.Tx{}

		// Read transaction from buffered reader
		if _, err = tx.ReadFrom(bufferedReader); err != nil {
			return errors.NewProcessingError("[BlockPersister] error reading transaction at index %d from subtree %s", i, subtreeHash.String(), err)
		}

		// 4. Process this transaction through UTXO diff
		if err = utxoDiff.ProcessTx(tx); err != nil {
			return errors.NewProcessingError("[BlockPersister] error processing tx for UTXO at index %d", i, err)
		}

		if subtree.Nodes[i].Hash.Equal(*subtreepkg.CoinbasePlaceholderHash) {
			// check whether the first transaction was actually the coinbase, otherwise we should skip that and move i forward
			if !tx.IsCoinbase() {
				i++
			}
		}

		// check the tx hash matches the expected hash (from subtree), except for coinbase
		if !tx.IsCoinbase() {
			txHash := tx.TxIDChainHash()
			if !txHash.Equal(subtree.Nodes[i].Hash) {
				return errors.NewProcessingError("[BlockPersister] transaction hash mismatch at index %d: expected %s, got %s", i, subtree.Nodes[i].Hash.String(), txHash.String())
			}
		}
	}

	return nil
}

// readSubtree retrieves a subtree from the subtree store and deserializes it.
//
// This function is responsible for loading a subtree structure from persistent storage,
// which contains the hierarchical organization of transactions within a block.
// It retrieves the subtree file using the provided hash and deserializes it into a
// usable subtree object.
// The process includes:
//  1. Attempting to read the subtree from the store using the provided hash
//  2. If the primary read fails, it attempts to read from a secondary location (e.g., a backup store)
//  3. Deserializing the retrieved subtree data into a subtree object
//
// Parameters:
//   - ctx: Context for the operation, enabling cancellation and timeout handling
//   - subtreeHash: Hash identifier of the subtree to retrieve and deserialize
//
// Returns:
//   - *subtreepkg.Subtree: The deserialized subtree object ready for further processing
//   - error: Any error encountered during retrieval or deserialization
//
// Possible errors include storage access failures, file not found errors, or deserialization issues.
// All errors are wrapped with appropriate context for debugging.
func (u *Server) readSubtree(ctx context.Context, subtreeHash chainhash.Hash) (*subtreepkg.Subtree, error) {
	subtreeReader, err := u.subtreeStore.GetIoReader(ctx, subtreeHash.CloneBytes(), fileformat.FileTypeSubtree)
	if err != nil {
		subtreeReader, err = u.subtreeStore.GetIoReader(ctx, subtreeHash.CloneBytes(), fileformat.FileTypeSubtreeToCheck)
		if err != nil {
			return nil, errors.NewStorageError("[BlockPersister] failed to get subtree from store", err)
		}
	}

	defer subtreeReader.Close()

	subtree := &subtreepkg.Subtree{}
	if err = subtree.DeserializeFromReader(subtreeReader); err != nil {
		return nil, errors.NewProcessingError("[BlockPersister] failed to deserialize subtree", err)
	}

	return subtree, nil
}
