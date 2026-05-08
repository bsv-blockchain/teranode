// Package repository provides access to blockchain data storage and retrieval operations.
// It implements the necessary interfaces to interact with various data stores and
// blockchain clients.
package repository

import (
	"context"
	"io"
	"sync"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/services/subtreevalidation"
	"github.com/bsv-blockchain/teranode/services/utxopersister/filestorer"
	"github.com/bsv-blockchain/teranode/stores/blob/options"
	"golang.org/x/sync/errgroup"
	"golang.org/x/sync/semaphore"
)

// Singleton quorum for distributed locking across asset service instances.
// Created lazily on first use when quorum path is configured.
var (
	assetQuorumOnce sync.Once
	assetQuorum     *subtreevalidation.Quorum
	assetQuorumMu   sync.Mutex // Protects quorum reset (for tests)
)

// resetQuorumForTests resets the singleton quorum. Only used in tests.
func resetQuorumForTests() {
	assetQuorumMu.Lock()
	defer assetQuorumMu.Unlock()
	assetQuorumOnce = sync.Once{}
	assetQuorum = nil
}

// semaphoreReadCloser wraps an io.ReadCloser and releases a semaphore permit when closed.
type semaphoreReadCloser struct {
	io.ReadCloser
	sem  *semaphore.Weighted
	once sync.Once
}

func (sr *semaphoreReadCloser) Close() error {
	err := sr.ReadCloser.Close()
	sr.once.Do(func() {
		releaseSemaphorePermit(sr.sem)
	})
	return err
}

// GetSubtreeDataReader retrieves the subtree data associated with the given subtree hash.
// It returns a PipeReader that can be used to read the subtree data as it is being streamed.
// The data is either retrieved from the block store or the subtree store, depending on availability.
//
// Concurrency model:
//   - File-exists fast path: gated by semGetSubtreeDataReader (waits for permit). Cheap — only
//     holds an open file descriptor while the client streams.
//   - On-demand creation slow path: gated by semSubtreeDataCreate via non-blocking TryAcquire.
//     Returns ErrServiceUnavailable (→ HTTP 503) if the cap is reached. The create path holds
//     chunk-sized batches of transaction metadata in memory and is the dominant memory consumer
//     under load, so it has its own hard cap and fails fast rather than queueing.
//
// Parameters:
// - ctx: The context for managing cancellation and timeouts.
// - subtreeHash: The hash of the subtree to retrieve.
//
// Returns:
// - io.ReadCloser: Reader for the subtree data; release semaphore permits via Close().
// - error: errors.ErrServiceUnavailable when the create path is at capacity; other errors otherwise.
func (repo *Repository) GetSubtreeDataReader(ctx context.Context, subtreeHash *chainhash.Hash) (io.ReadCloser, error) {
	// Cheap existence check first, without holding any reader-permit. If the file exists we
	// only need an FD-bound permit to stream it; if it doesn't we need an admission slot in
	// the (much smaller) create-path semaphore.
	subtreeDataExists, existsErr := repo.SubtreeStore.Exists(ctx, subtreeHash[:], fileformat.FileTypeSubtreeData)

	if existsErr == nil && subtreeDataExists {
		if err := acquireSemaphorePermit(ctx, repo.semGetSubtreeDataReader, "GetSubtreeDataReader"); err != nil {
			return nil, err
		}

		reader, err := repo.SubtreeStore.GetIoReader(ctx, subtreeHash[:], fileformat.FileTypeSubtreeData)
		if err != nil {
			releaseSemaphorePermit(repo.semGetSubtreeDataReader)
			return nil, err
		}
		// Wrap reader to release semaphore when closed
		return &semaphoreReadCloser{
			ReadCloser: reader,
			sem:        repo.semGetSubtreeDataReader,
		}, nil
	}

	// File doesn't exist (or Exists errored) — on-demand creation path. Fail fast under load
	// rather than queueing: each in-flight creation holds chunk-sized tx metadata in memory.
	if !tryAcquireSemaphorePermit(repo.semSubtreeDataCreate) {
		return nil, errors.NewServiceUnavailableError(
			"[GetSubtreeDataReader] subtree data create concurrency limit reached for %s; retry later",
			subtreeHash.String())
	}

	// Hand off to dualStreamWithFileCreation. It owns the create permit lifecycle from here:
	// released either on early-return below or by the producer goroutine when streaming finishes.
	return repo.dualStreamWithFileCreation(ctx, subtreeHash)
}

// getOrCreateQuorum returns the singleton quorum instance for distributed locking.
// Returns nil if quorum path is not configured.
// Thread-safe: uses sync.Once to ensure single initialization.
func (repo *Repository) getOrCreateQuorum() *subtreevalidation.Quorum {
	quorumPath := repo.settings.SubtreeValidation.QuorumPath
	if quorumPath == "" {
		return nil
	}

	assetQuorumOnce.Do(func() {
		var err error
		assetQuorum, err = subtreevalidation.NewQuorum(
			repo.logger,
			repo.SubtreeStore,
			quorumPath,
			subtreevalidation.WithAbsoluteTimeout(repo.settings.SubtreeValidation.QuorumAbsoluteTimeout),
		)
		if err != nil {
			repo.logger.Warnf("[Asset] Failed to create quorum for on-demand subtreeData creation: %v - distributed locking disabled", err)
			assetQuorum = nil
		} else {
			repo.logger.Infof("[Asset] Quorum initialized for on-demand subtreeData creation (path: %s, timeout: %s)",
				quorumPath, repo.settings.SubtreeValidation.QuorumAbsoluteTimeout)
		}
	})

	return assetQuorum
}

// dualStreamWithFileCreation creates a subtreeData file while simultaneously streaming to HTTP response.
// If quorum is configured, uses distributed locking to ensure only one instance creates the file.
//
// Ownership: callers MUST have already acquired a permit on repo.semSubtreeDataCreate. This function
// owns the permit lifecycle from here on:
//   - Released synchronously on early-return paths (file appeared, error before goroutine spawn).
//   - Released by the producer goroutine when streaming completes (success or write error).
//
// On the file-appeared path the returned reader uses the reader semaphore (a fresh permit acquired
// here) so the caller doesn't accidentally double-count the create permit while the client streams.
func (repo *Repository) dualStreamWithFileCreation(ctx context.Context, subtreeHash *chainhash.Hash) (io.ReadCloser, error) {
	// Initialize metrics (safe to call multiple times due to sync.Once)
	initPrometheusMetrics()

	// fileAppearedReadback returns a reader for an already-existing file, releases the create
	// permit, and acquires a reader permit instead. Used when another instance produced the file
	// while we were waiting/setting up.
	fileAppearedReadback := func(metricStatus, metricLabel string) (io.ReadCloser, error) {
		releaseSemaphorePermit(repo.semSubtreeDataCreate)

		if err := acquireSemaphorePermit(ctx, repo.semGetSubtreeDataReader, "GetSubtreeDataReader"); err != nil {
			return nil, err
		}
		reader, err := repo.SubtreeStore.GetIoReader(ctx, subtreeHash[:], fileformat.FileTypeSubtreeData)
		if err != nil {
			releaseSemaphorePermit(repo.semGetSubtreeDataReader)
			return nil, err
		}
		repo.logger.Debugf("[GetSubtreeDataReader] SubtreeData file for %s already exists, reading from file", subtreeHash.String())
		prometheusAssetSubtreeDataCreated.WithLabelValues(metricStatus, metricLabel).Inc()
		return &semaphoreReadCloser{
			ReadCloser: reader,
			sem:        repo.semGetSubtreeDataReader,
		}, nil
	}

	// On-demand subtreeData files are created with a finite DAH so they expire naturally
	// on pruned nodes. Only the block persister promotes files to permanent (DAH=0).

	// If quorum is available, use distributed locking
	var release func()
	quorum := repo.getOrCreateQuorum()
	if quorum != nil {
		locked, exists, releaseFunc, err := quorum.TryLockIfNotExistsWithTimeout(ctx, subtreeHash, fileformat.FileTypeSubtreeData)
		if err != nil {
			// Quorum error - log and continue without locking
			repo.logger.Warnf("[GetSubtreeDataReader] Quorum lock error for %s: %v, continuing without lock", subtreeHash.String(), err)
			prometheusAssetSubtreeDataCreated.WithLabelValues("error", "quorum_lock_failed").Inc()
		} else if exists {
			// File was created by another instance while we waited - just read it
			return fileAppearedReadback("success", "waited_for_other")
		} else if locked {
			// We acquired the lock - will release after file creation
			repo.logger.Debugf("[GetSubtreeDataReader] Acquired quorum lock for %s", subtreeHash.String())
			release = releaseFunc
		}
	}

	// Compute DAH before creating FileStorer so it is set atomically during file creation.
	// This ensures the file always has a finite DAH even if the process crashes after creation.
	dah := repo.UtxoStore.GetBlockHeight() + repo.settings.GetSubtreeValidationBlockHeightRetention()

	// Create FileStorer (with or without quorum lock)
	storer, err := filestorer.NewFileStorer(ctx, repo.logger, repo.settings,
		repo.SubtreeStore, subtreeHash[:], fileformat.FileTypeSubtreeData,
		options.WithDeleteAt(dah))
	if err != nil {
		if release != nil {
			release() // Release quorum lock on error
		}
		if errors.Is(err, errors.NewBlobAlreadyExistsError("")) {
			// File appeared between check and creation - read from it
			return fileAppearedReadback("success", "file_existed")
		}
		// Other error
		releaseSemaphorePermit(repo.semSubtreeDataCreate)
		prometheusAssetSubtreeDataCreated.WithLabelValues("error", "creation_failed").Inc()
		return nil, err
	}

	// Create pipe for HTTP response
	httpReader, httpWriter := io.Pipe()

	// Use MultiWriter to write to both file storage and HTTP pipe simultaneously
	multiWriter := io.MultiWriter(storer, httpWriter)

	// Background goroutine: generate data and write to both destinations
	g, gCtx := errgroup.WithContext(ctx)
	g.Go(func() error {
		defer releaseSemaphorePermit(repo.semSubtreeDataCreate)
		if release != nil {
			defer release() // Release quorum lock when done
		}

		// Write all transactions to both destinations
		err := repo.writeTransactionsViaSubtreeStoreStreaming(gCtx, multiWriter, nil, subtreeHash)
		if err != nil {
			repo.logger.Warnf("[GetSubtreeDataReader] Error writing subtreeData for %s: %v", subtreeHash.String(), err)
			storer.Abort(err)
			_ = httpWriter.CloseWithError(err)
			prometheusAssetSubtreeDataCreated.WithLabelValues("error", "write_failed").Inc()
			return err
		}

		// Close the file storer successfully
		if closeErr := storer.Close(gCtx); closeErr != nil {
			repo.logger.Warnf("[GetSubtreeDataReader] Error closing subtreeData file for %s: %v", subtreeHash.String(), closeErr)
			_ = httpWriter.CloseWithError(closeErr)
			prometheusAssetSubtreeDataCreated.WithLabelValues("error", "close_failed").Inc()
			return closeErr
		}

		// Success - close HTTP pipe
		metricLabel := "on_demand_created"
		if release != nil {
			metricLabel = "on_demand_created_locked"
		}
		repo.logger.Infof("[GetSubtreeDataReader] Successfully created subtreeData file on-demand for %s", subtreeHash.String())
		_ = httpWriter.Close()
		prometheusAssetSubtreeDataCreated.WithLabelValues("success", metricLabel).Inc()
		return nil
	})

	return httpReader, nil
}
