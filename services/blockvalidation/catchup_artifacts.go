package blockvalidation

import (
	"context"
	"sync"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/stores/blob"
	"github.com/bsv-blockchain/teranode/stores/blob/options"
	"github.com/bsv-blockchain/teranode/ulogger"
)

type catchupArtifactsKey struct{}

type catchupArtifactKey struct {
	hash chainhash.Hash
	kind fileformat.FileType
}

type catchupArtifact struct {
	mu      sync.Mutex
	created bool
}

// catchupArtifacts owns only pending files created by one catchup attempt.
// Validation continues to use the shared store and can promote accepted subtrees.
// Cleanup must run after all fetchers and async validation writers have joined.
type catchupArtifacts struct {
	blob.Store
	mu    sync.Mutex
	files map[catchupArtifactKey]*catchupArtifact
}

func (u *Server) catchupSubtreeStore(ctx context.Context) blob.Store {
	if store, ok := ctx.Value(catchupArtifactsKey{}).(*catchupArtifacts); ok {
		return store
	}
	return u.subtreeStore
}

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
	// The fetch errgroup's context is canceled on failure (and after Wait).
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	for file, entry := range s.files {
		if !entry.created {
			continue
		}
		// A previous block in this attempt, or another validator, may already
		// have accepted this subtree. Keep its shared transaction data intact.
		validated, err := s.Store.Exists(ctx, file.hash[:], fileformat.FileTypeSubtree)
		if err != nil {
			logger.Warnf("[catchup] cannot check artifact %s before cleanup: %v", file.hash.String(), err)
			continue
		}
		if validated {
			continue
		}
		if err := s.Store.Del(ctx, file.hash[:], file.kind); err != nil && !errors.Is(err, errors.ErrNotFound) {
			logger.Warnf("[catchup] failed to remove pending %s for %s: %v", file.kind, file.hash.String(), err)
		}
	}
}
