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
// Parameters:
// - ctx: The context for managing cancellation and timeouts.
// - subtreeHash: The hash of the subtree to retrieve.
//
// Returns:
// - *io.PipeReader: A PipeReader that can be used to read the subtree data.
// - error: An error if the retrieval fails, or nil if successful.
func (repo *Repository) GetSubtreeDataReader(ctx context.Context, subtreeHash *chainhash.Hash) (io.ReadCloser, error) {
	if err := acquireSemaphorePermit(ctx, repo.semGetSubtreeDataReader, "GetSubtreeDataReader"); err != nil {
		return nil, err
	}
	// Note: semaphore will be released when the returned reader is closed

	subtreeDataExists, err := repo.SubtreeStore.Exists(ctx, subtreeHash[:], fileformat.FileTypeSubtreeData)
	if err == nil && subtreeDataExists {
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

	// File doesn't exist - create it on-demand while streaming to HTTP response
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
func (repo *Repository) dualStreamWithFileCreation(ctx context.Context, subtreeHash *chainhash.Hash) (io.ReadCloser, error) {
	// Initialize metrics (safe to call multiple times due to sync.Once)
	initPrometheusMetrics()

	// Note: subtreeData files have DAH=0 (no expiration), set by FileStorer.Close()
	// This is consistent with blockpersister behavior.

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
			repo.logger.Debugf("[GetSubtreeDataReader] SubtreeData file for %s created by another instance, reading from file", subtreeHash.String())
			prometheusAssetSubtreeDataCreated.WithLabelValues("success", "waited_for_other").Inc()

			reader, err := repo.SubtreeStore.GetIoReader(ctx, subtreeHash[:], fileformat.FileTypeSubtreeData)
			if err != nil {
				releaseSemaphorePermit(repo.semGetSubtreeDataReader)
				return nil, err
			}
			return &semaphoreReadCloser{
				ReadCloser: reader,
				sem:        repo.semGetSubtreeDataReader,
			}, nil
		} else if locked {
			// We acquired the lock - will release after file creation
			repo.logger.Debugf("[GetSubtreeDataReader] Acquired quorum lock for %s", subtreeHash.String())
			release = releaseFunc
		}
	}

	// Create FileStorer (with or without quorum lock)
	storer, err := filestorer.NewFileStorer(ctx, repo.logger, repo.settings,
		repo.SubtreeStore, subtreeHash[:], fileformat.FileTypeSubtreeData)
	if err != nil {
		if release != nil {
			release() // Release quorum lock on error
		}
		if errors.Is(err, errors.NewBlobAlreadyExistsError("")) {
			// File appeared between check and creation - read from it
			repo.logger.Debugf("[GetSubtreeDataReader] SubtreeData file for %s already exists, reading from file", subtreeHash.String())
			prometheusAssetSubtreeDataCreated.WithLabelValues("success", "file_existed").Inc()

			reader, readErr := repo.SubtreeStore.GetIoReader(ctx, subtreeHash[:], fileformat.FileTypeSubtreeData)
			if readErr != nil {
				releaseSemaphorePermit(repo.semGetSubtreeDataReader)
				return nil, readErr
			}
			return &semaphoreReadCloser{
				ReadCloser: reader,
				sem:        repo.semGetSubtreeDataReader,
			}, nil
		}
		// Other error
		releaseSemaphorePermit(repo.semGetSubtreeDataReader)
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
		defer releaseSemaphorePermit(repo.semGetSubtreeDataReader)
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
