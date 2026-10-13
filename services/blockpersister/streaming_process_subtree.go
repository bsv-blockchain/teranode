// Package blockpersister provides comprehensive functionality for persisting blockchain blocks and their associated data.
// It handles block persistence, transaction processing, and UTXO set management across multiple storage backends.
package blockpersister

import (
	"bufio"
	"context"
	"io"
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
		tracing.WithDebugLogMessage(u.logger, "[CreateSubtreeDataFileStreaming][%s] creating subtreeData %d / %d for [%s]", block.String(), n, len(block.Subtrees), subtreeHash.String()),
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
		// verify that the existing subtreeData file holds every node's transaction, each decoding and matching its node hash
		subtreeDataReader, err := u.subtreeStore.GetIoReader(ctx, subtreeHash.CloneBytes(), fileformat.FileTypeSubtreeData)
		if err != nil {
			return errors.NewStorageError("[BlockPersister] error getting existing subtree data for %s", subtreeHash.String(), err)
		}
		defer subtreeDataReader.Close()

		subtreeDataBufferedReader := bufio.NewReaderSize(subtreeDataReader, 32*1024) // 32KB buffer

		if err = validateSubtreeData(subtree, subtreeDataBufferedReader); err != nil {
			// A read error or a cancelled context says nothing about the file, and deleting it would
			// throw away what may be the only copy: the rebuild needs the block's transactions, which
			// the UTXO store may already have pruned. Only a file whose contents are wrong is
			// deleted and recreated.
			if ctxErr := ctx.Err(); ctxErr != nil {
				return errors.NewContextCanceledError("[BlockPersister] cancelled while checking existing subtree data for %s", subtreeHash.String(), ctxErr)
			}

			if errors.Is(err, errors.ErrStorageError) {
				return errors.NewStorageError("[BlockPersister] error reading existing subtree data for %s", subtreeHash.String(), err)
			}

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
	util.SafeSetLimit(u.logger, g, concurrency)

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

					// A transaction stored from a UTXO-set snapshot comes back as its
					// live outputs only. Bytes() below panics on a nil output hole —
					// and there is no recover on this path, so it takes the process
					// down — or serializes cleanly into a short, 0-input transaction
					// that does not hash to the node's own hash, which would land in
					// the data file as if it were the real one. Same gate as the asset
					// boundary (services/asset/repository.isRequestedTransaction); the
					// predicate must come first, because TxIDChainHash() dereferences
					// the same nil *bt.Output.
					if !data.Data.TxIsSerializable() || !data.Data.Tx.TxIDChainHash().IsEqual(&data.Hash) {
						return errors.NewProcessingError("[CreateSubtreeDataFileStreaming] transaction is not retained in full for hash %s at index %d", data.Hash, j)
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
//  3. Processes the block's coinbase if node 0 is the coinbase placeholder
//  4. Streams through transactions using bt.Tx.ReadFromWithArena, resetting the arena per transaction
//  5. Checks each transaction against its node, then processes it through utxoDiff.ProcessTx
//  6. Closes reader when done
//
// Memory usage is minimal - only one transaction in memory at a time.
//
// Parameters:
//   - ctx: Context for the operation
//   - subtreeHash: Hash identifier of the subtree to process
//   - coinbaseTx: The block's coinbase, processed for a coinbase placeholder at node 0. May be nil
//     for a block started from a seed. A coinbase opening the file is then used instead, but only
//     when the subtree has a node after the placeholder: otherwise nothing is read.
//   - utxoDiff: UTXO set difference tracker to record changes
//
// Returns an error if any part of the process fails.
func (u *Server) ProcessSubtreeUTXOStreaming(ctx context.Context, subtreeHash chainhash.Hash, coinbaseTx *bt.Tx, utxoDiff *utxopersister.UTXOSet) error {
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

	// The buffer is frame-local: never stored, never returned, read by this goroutine only
	// and dead before the return below, so a deferred release is the whole ownership story.
	bufferedReader := filestorer.AcquireReader(subtreeDataReader, bufferSize.Int())
	defer filestorer.ReleaseReader(bufferedReader)

	// 2. Read the subtree structure (needed to know how many transactions)
	subtree, err := u.readSubtree(ctx, subtreeHash)
	if err != nil {
		return err
	}

	// One read of the nodes slice, for the reason model.MissingSubtreeDataTxs sets out.
	nodes := subtree.Nodes
	txIndex := 0

	// 3. A coinbase placeholder at node 0 stands for the block's coinbase. The file may open with a
	// coinbase or leave it out, and a coinbase it holds need not be this block's: the file is keyed
	// by subtree hash, and competing blocks can share their first subtree. So the block's own
	// coinbase is processed, and a coinbase opening the file is read and dropped. Only a block
	// without one, started from a seed, falls back to the file's, and only when the subtree has a
	// node after the placeholder. A placeholder-only subtree reads nothing, so its file's coinbase
	// is never seen. That needs a coinbase-only block below the seed, and a coinbase-only block
	// has no subtrees.
	coinbaseMayOpen := len(nodes) > 0 && nodes[0].Hash.Equal(subtreepkg.CoinbasePlaceholderHashValue)
	if coinbaseMayOpen {
		txIndex = 1

		if coinbaseTx != nil {
			if err = utxoDiff.ProcessTx(coinbaseTx); err != nil {
				return errors.NewProcessingError("[BlockPersister] error processing the block's coinbase for subtree %s", subtreeHash.String(), err)
			}
		}
	}

	// Arena-backed tx decode is safe here because utxoDiff is
	// *utxopersister.UTXOSet (NOT services/blockpersister/utxoset/model.UTXODiff,
	// which retains script slices in an in-memory map). UTXOSet.ProcessTx
	// serialises via UTXOWrapper.Bytes -> UTXO.Bytes, which does
	// `append(b, u.Script...)` — a heap copy — before anything is written. No
	// arena-backed slice survives the iteration that decoded it, so the arena is
	// reset per tx and holds one transaction's scripts at a time (plus, for a tx
	// that grows it mid-decode, the older slabs its earlier scripts point into).
	// Without the reset it holds every script in the subtree, and its doubling
	// growth needs up to 3x the subtreeData size at the moment it copies into a
	// new slab.
	arena := getBlockpersisterArena()
	defer putBlockpersisterArena(arena)
	var hashScratch []byte

	// 4. Stream one transaction per remaining node. The loop is driven by the nodes, as
	// validateSubtreeData's is, so a file phase 1 accepts is one this loop reads to the end.
	for txIndex < len(nodes) {
		arena.Reset()

		tx := &bt.Tx{}

		if _, err = tx.ReadFromWithArena(bufferedReader, arena); err != nil {
			return errors.NewProcessingError("[BlockPersister] error reading transaction for node %d from subtree %s", txIndex, subtreeHash.String(), err)
		}

		if coinbaseMayOpen {
			coinbaseMayOpen = false

			if tx.IsCoinbase() {
				if coinbaseTx == nil {
					if err = utxoDiff.ProcessTx(tx); err != nil {
						return errors.NewProcessingError("[BlockPersister] error processing the file's coinbase for subtree %s", subtreeHash.String(), err)
					}
				}

				continue
			}
		}

		// 5. Check the transaction against its node before it reaches the UTXO diff, so a wrong
		// one writes nothing. A coinbase anywhere but opening the file is checked like any other
		// transaction.
		var txHash chainhash.Hash

		txHash, hashScratch = tx.HashTxIDInto(hashScratch)
		if !txHash.Equal(nodes[txIndex].Hash) {
			return errors.NewProcessingError("[BlockPersister] transaction hash mismatch at index %d: expected %s, got %s", txIndex, nodes[txIndex].Hash.String(), txHash.String())
		}

		if err = utxoDiff.ProcessTx(tx); err != nil {
			return errors.NewProcessingError("[BlockPersister] error processing tx for UTXO at index %d", txIndex, err)
		}

		txIndex++
	}

	return nil
}

// validateSubtreeData checks that r holds complete subtreeData for subtree, without keeping the
// transactions. subtreepkg.NewSubtreeDataFromReader, which this replaces here, decodes
// the whole subtree into memory, which for one multi-GiB subtree is enough to OOM the persister
// just to learn that the file parses. Here each transaction is decoded into a pooled arena,
// checked against its node hash and dropped, so memory follows the largest transaction instead.
//
// It accepts only a file that phase 2 can read to the end: one transaction per node, in node
// order, each hashing to its node. When node 0 is the coinbase placeholder, the file may open
// with a coinbase or leave it out. Phase 2 processes the block's own coinbase either way and drops
// one opening the file, deciding by the node's hash rather than by the subtree's position in the
// block. So a subtree holding only the placeholder is complete with an empty file. The decoder is
// the one NewSubtreeDataFromReader uses: go-bt's Tx.ReadFrom is ReadFromWithArena(r, nil), and
// the arena only changes where script bytes are allocated, after the same length checks.
//
// Three kinds of file that go-subtree's serializeFromReader accepts are rejected here. Reused,
// each gets DAH 0 and is served to peers as it is, and nothing else deletes it:
//   - A file that ends on a transaction or field boundary before the last node, an empty file
//     included when a node needs a transaction. go-bt returns a clean io.EOF there, and phase 2
//     reads every node, so it fails on such a file on every attempt (issue 1917).
//   - A file that ends partway through a transaction on a field boundary after the last node,
//     including a coinbase cut short under a placeholder-only subtree. go-bt returns a clean
//     io.EOF there too; the bytes it consumed first tell it apart from the end of the file.
//   - A coinbase anywhere but first under the placeholder. serializeFromReader skips any coinbase
//     it decodes while standing at node index 1, without a hash check. Phase 2 checks it like any
//     other transaction.
//
// An error from r itself, rather than from decoding what r returned, is reported as a storage
// error: the store failed, and the file's contents are unknown rather than wrong, so the caller
// keeps it. Since every node must be seen, how a decode error is classified no longer decides
// whether an incomplete file passes, so teranode's errors.Is is enough for the end-of-file test.
// TestValidateSubtreeData_AgainstGoSubtreeAndCatchup runs this, go-subtree's reader and the
// catchup fetcher's checks against one table.
func validateSubtreeData(subtree *subtreepkg.Subtree, r io.Reader) error {
	if subtree == nil || len(subtree.Nodes) == 0 {
		return errors.NewProcessingError("subtree has no nodes", subtreepkg.ErrSubtreeNodesEmpty)
	}

	txIndex := 0

	// Under the placeholder, the file may open with the coinbase. Nowhere else may it appear.
	coinbaseMayOpen := subtree.Nodes[0].Hash.Equal(subtreepkg.CoinbasePlaceholderHashValue)
	if coinbaseMayOpen {
		txIndex = 1
	}

	arena := getBlockpersisterArena()
	defer putBlockpersisterArena(arena)

	var hashScratch []byte

	reader := &readErrRecorder{r: r}

	for {
		arena.Reset()

		tx := &bt.Tx{}

		if n, err := tx.ReadFromWithArena(reader, arena); err != nil {
			// The store failed, so the file's contents are unknown rather than wrong.
			if reader.err != nil {
				return errors.NewStorageError("error reading subtreeData for node %d", txIndex, reader.err)
			}

			if errors.Is(err, io.EOF) {
				// go-bt counts the bytes it consumed before every error, so a clean end of file is
				// one that falls exactly between transactions. Anything else is a partial transaction,
				// after the last node as much as before it.
				if n > 0 {
					return errors.NewProcessingError("subtreeData ends inside the transaction for node %d", txIndex)
				}

				if txIndex < len(subtree.Nodes) {
					return errors.NewProcessingError("subtreeData is incomplete (%d of %d txs missing)", len(subtree.Nodes)-txIndex, len(subtree.Nodes))
				}

				return nil
			}

			return errors.NewProcessingError("error reading transaction for node %d", txIndex, err)
		}

		if coinbaseMayOpen {
			coinbaseMayOpen = false

			if tx.IsCoinbase() {
				continue
			}
		}

		if txIndex >= len(subtree.Nodes) {
			return errors.NewProcessingError("transaction for node %d is beyond the subtree's %d nodes", txIndex, len(subtree.Nodes), subtreepkg.ErrTxIndexOutOfBounds)
		}

		var txHash chainhash.Hash

		txHash, hashScratch = tx.HashTxIDInto(hashScratch)
		if !txHash.Equal(subtree.Nodes[txIndex].Hash) {
			return errors.NewProcessingError("transaction hash mismatch for node %d: expected %s, got %s", txIndex, subtree.Nodes[txIndex].Hash.String(), txHash.String())
		}

		txIndex++
	}
}

// readErrRecorder passes reads through and keeps the first error the underlying reader returns
// other than io.EOF. A decode failure on its own cannot tell a file that is wrong from a store that
// failed partway through it; this can.
type readErrRecorder struct {
	r   io.Reader
	err error
}

func (e *readErrRecorder) Read(p []byte) (int, error) {
	n, err := e.r.Read(p)
	if err != nil && err != io.EOF && e.err == nil { //nolint:errorlint // io.EOF is returned unwrapped by contract
		e.err = err
	}

	return n, err
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
