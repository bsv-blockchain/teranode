package blockvalidation

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/stores/blob"
	"github.com/bsv-blockchain/teranode/stores/blob/options"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util"
	"golang.org/x/sync/errgroup"
)

type catchupArtifactsKey struct{}

type catchupArtifactKey struct {
	hash chainhash.Hash
	kind fileformat.FileType
}

type catchupArtifact struct {
	mu      sync.Mutex
	created bool
	corrupt bool
}

// catchupArtifacts tracks ownership of pending files created by one catchup
// attempt and identifies corrupt cached files for repair. Validation continues
// to use the shared store and can promote accepted subtrees.
// Cleanup must run after all fetchers and async validation writers have joined.
type catchupArtifacts struct {
	blob.Store
	mu                 sync.Mutex
	files              map[catchupArtifactKey]*catchupArtifact
	cleanupConcurrency int
	cleanupFileTimeout time.Duration
	cleanupBudget      time.Duration
}

// catchupArtifactError distinguishes a bad download in this attempt from corrupt
// cached bytes. FileTypeSubtreeToCheck alone is not provenance: transient failures
// retain those files for later attempts. Outside catchup, preserve the caller's
// validation error; the production catchup path always installs this tracker.
func catchupArtifactError(ctx context.Context, hash chainhash.Hash, kind fileformat.FileType, invalid error) error {
	s, ok := ctx.Value(catchupArtifactsKey{}).(*catchupArtifacts)
	if !ok {
		return invalid
	}
	if ctx.Err() != nil {
		return errors.NewServiceError("reading catchup artifact was canceled", errors.NewStorageError("artifact read canceled", ctx.Err()))
	}
	s.mu.Lock()
	key := catchupArtifactKey{hash: hash, kind: kind}
	entry := s.files[key]
	if entry == nil {
		entry = &catchupArtifact{}
		s.files[key] = entry
	}
	s.mu.Unlock()
	entry.mu.Lock()
	defer entry.mu.Unlock()
	if entry.created {
		return invalid
	}
	entry.corrupt = true
	// Do not wrap ErrBlockInvalid: callers must neither penalize the current
	// peer nor fall back into UTXO mutation using these cached bytes. Eviction
	// happens after all attempt workers join, so the next attempt can refetch.
	return errors.NewServiceError("corrupt cached catchup artifact", errors.NewStorageError("%s %s: %s", hash.String(), kind, invalid.Error()))
}

func (u *Server) catchupSubtreeStore(ctx context.Context) blob.Store {
	if store, ok := ctx.Value(catchupArtifactsKey{}).(*catchupArtifacts); ok {
		return store
	}
	return u.subtreeStore
}

// Set always preserves existing files, regardless of caller overwrite options.
// Overwriting would prevent cleanup from distinguishing new files from shared data.
func (s *catchupArtifacts) Set(ctx context.Context, key []byte, kind fileformat.FileType, value []byte, opts ...options.FileOption) error {
	hash, err := chainhash.NewHash(key)
	if err != nil {
		return err
	}
	file := catchupArtifactKey{hash: *hash, kind: kind}
	s.mu.Lock()
	entry := s.files[file]
	if entry == nil {
		entry = &catchupArtifact{}
		s.files[file] = entry
	}
	s.mu.Unlock()
	// Multiple prefetched blocks can share a subtree. Serialize only writes to
	// that file, leaving independent subtree downloads and writes concurrent.
	entry.mu.Lock()
	defer entry.mu.Unlock()
	opts = append(opts, options.WithAllowOverwrite(false))
	if err := s.Store.Set(ctx, key, kind, value, opts...); err != nil {
		if errors.Is(err, errors.ErrBlobAlreadyExists) {
			return nil // preserve existing data, and never claim ownership of it
		}
		return err
	}
	entry.created = true
	return nil
}

func (s *catchupArtifacts) cleanup(logger ulogger.Logger) {
	s.removeFiles(logger, true)
}

func (s *catchupArtifacts) repair(logger ulogger.Logger) {
	s.removeFiles(logger, false)
}

func (s *catchupArtifacts) removeFiles(logger ulogger.Logger, discardCreated bool) {
	// The fetch errgroup's context is canceled on failure (and after Wait).
	budget := s.cleanupBudget
	if budget <= 0 {
		budget = time.Minute
	}
	fileTimeout := s.cleanupFileTimeout
	if fileTimeout <= 0 {
		fileTimeout = 5 * time.Second
	}
	concurrency := s.cleanupConcurrency
	if concurrency <= 0 {
		concurrency = 8
	}
	ctx, cancel := context.WithTimeout(context.Background(), budget)
	defer cancel()
	var workers errgroup.Group
	util.SafeSetLimit(&workers, concurrency)
	var failed atomic.Int64
	for file, entry := range s.files {
		if !entry.corrupt && (!discardCreated || !entry.created) {
			continue
		}
		if ctx.Err() != nil {
			break
		}
		workers.Go(func() error {
			fileCtx, fileCancel := context.WithTimeout(ctx, fileTimeout)
			defer fileCancel()
			if !entry.corrupt {
				// Preserve accepted subtrees and their shared transaction data.
				validated, err := s.Store.Exists(fileCtx, file.hash[:], fileformat.FileTypeSubtree)
				if err != nil {
					failed.Add(1)
					return errors.NewStorageError("checking %s before artifact cleanup", file.hash.String(), err)
				}
				if validated {
					return nil
				}
			}
			if err := s.Store.Del(fileCtx, file.hash[:], file.kind); err != nil && !errors.Is(err, errors.ErrNotFound) {
				failed.Add(1)
				return errors.NewStorageError("removing %s %s during artifact cleanup", file.hash.String(), file.kind, err)
			}
			return nil
		})
	}
	cleanupErr := workers.Wait()
	if failed.Load() != 0 || ctx.Err() != nil {
		logger.Warnf("[catchup] artifact cleanup incomplete: %d operations failed (first error: %v; budget error: %v); remaining files retained", failed.Load(), cleanupErr, ctx.Err())
	}
}
