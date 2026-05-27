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
	if err != nil {
		// Surface storage errors instead of falling through to the NotFound /
		// on-demand path — otherwise an IO failure here would be silently
		// reclassified as 404 or trigger an unnecessary regeneration attempt.
		releaseSemaphorePermit(repo.semGetSubtreeDataReader)
		return nil, err
	}
	if subtreeDataExists {
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

	// SubtreeData doesn't exist. The on-demand fallback (dualStreamWithFileCreation)
	// regenerates the data from the underlying subtree file in a goroutine *after* the
	// HTTP handler has committed to 200 OK. If neither the subtree nor the
	// subtreeToCheck file is present we cannot regenerate, and the handler would
	// otherwise emit "200 OK + empty body", which peers report as
	// ErrSubtreeLengthMismatch. Surface a NotFound instead so the handler returns 404
	// and callers can attempt another peer.
	subtreeExists, existsErr := repo.SubtreeStore.Exists(ctx, subtreeHash[:], fileformat.FileTypeSubtree)
	if existsErr != nil {
		releaseSemaphorePermit(repo.semGetSubtreeDataReader)
		return nil, existsErr
	}
	if !subtreeExists {
		toCheckExists, toCheckErr := repo.SubtreeStore.Exists(ctx, subtreeHash[:], fileformat.FileTypeSubtreeToCheck)
		if toCheckErr != nil {
			releaseSemaphorePermit(repo.semGetSubtreeDataReader)
			return nil, toCheckErr
		}
		if !toCheckExists {
			releaseSemaphorePermit(repo.semGetSubtreeDataReader)
			return nil, errors.NewNotFoundError("subtree %s not found", subtreeHash.String())
		}
	}

	// File doesn't exist — on-demand creation path. Apply non-blocking admission
	// control before doing any allocation-heavy work: each in-flight creation
	// holds chunk-sized tx metadata in memory plus pipe buffers, so a runaway
	// queue here is the difference between graceful 503 and OOM. The peer-side
	// retry helper (DoHTTPRequestBodyReaderWithRetry) handles the resulting
	// ErrServiceUnavailable with backoff.
	if !tryAcquireSemaphorePermit(repo.semSubtreeDataCreate) {
		releaseSemaphorePermit(repo.semGetSubtreeDataReader)
		return nil, errors.NewServiceUnavailableError(
			"[GetSubtreeDataReader] on-demand subtree-data creation at capacity for %s; retry later",
			subtreeHash.String())
	}

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
			// File was created by another instance while we waited - just read it.
			// The create permit is released because we are not going to run the
			// on-demand pipeline.
			releaseSemaphorePermit(repo.semSubtreeDataCreate)
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
			// File appeared between check and creation - read from it.
			// We never ran the on-demand pipeline so the create permit is freed.
			releaseSemaphorePermit(repo.semSubtreeDataCreate)
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
		// Other error — pipeline never started, free both permits.
		releaseSemaphorePermit(repo.semSubtreeDataCreate)
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
		defer releaseSemaphorePermit(repo.semSubtreeDataCreate)
		if release != nil {
			defer release() // Release quorum lock when done
		}

		// Write all transactions to both destinations
		err := repo.writeTransactionsViaSubtreeStoreStreaming(gCtx, multiWriter, nil, subtreeHash)
		if err != nil {
			storer.Abort(err)
			_ = httpWriter.CloseWithError(err)
			// "Client gone" — the HTTP client (or proxy) disconnected mid-stream, which
			// surfaces as either io.ErrClosedPipe on the next pipe write or context
			// cancellation observed by writeChunkToWriter. This is not a server fault
			// and dominates under catchup load (one cancelled batch can cascade tens of
			// in-flight streams). Log at debug and segregate the metric so the operational
			// signal stays clean.
			if errors.IsContextError(err) || errors.Is(err, io.ErrClosedPipe) {
				repo.logger.Debugf("[GetSubtreeDataReader] Client gone while writing subtreeData for %s: %v", subtreeHash.String(), err)
				prometheusAssetSubtreeDataCreated.WithLabelValues("error", "client_gone").Inc()
				return err
			}
			repo.logger.Warnf("[GetSubtreeDataReader] Error writing subtreeData for %s: %v", subtreeHash.String(), err)
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
