package subtreevalidation

import (
	"context"
	"os"
	"path"
	"time"

	"github.com/bitcoin-sv/teranode/errors"
	"github.com/bitcoin-sv/teranode/pkg/fileformat"
	"github.com/bitcoin-sv/teranode/stores/blob/options"
	"github.com/bitcoin-sv/teranode/ulogger"
	"github.com/libsv/go-bt/v2/chainhash"
)

type existerIfc interface {
	Exists(ctx context.Context, key []byte, fileType fileformat.FileType, opts ...options.FileOption) (bool, error)
}

type QuorumOption func(*Quorum)

func WithTimeout(timeout time.Duration) QuorumOption {
	return func(q *Quorum) {
		q.timeout = timeout
	}
}

func WithAbsoluteTimeout(timeout time.Duration) QuorumOption {
	return func(q *Quorum) {
		q.absoluteTimeout = timeout
	}
}

var noopFunc = func() {
	// NOOP
}

type Quorum struct {
	logger          ulogger.Logger
	path            string
	timeout         time.Duration
	absoluteTimeout time.Duration
	fileType        fileformat.FileType
	exister         existerIfc
	existerOpts     []options.FileOption
}

func NewQuorum(logger ulogger.Logger, exister existerIfc, path string, quorumOpts ...QuorumOption) (*Quorum, error) {
	if path == "" {
		return nil, errors.NewConfigurationError("Path is required")
	}

	q := &Quorum{
		logger:  logger,
		path:    path,
		exister: exister,
		timeout: 10 * time.Second,
	}

	for _, option := range quorumOpts {
		option(q)
	}

	logger.Infof("Creating subtree quorum path: %s", q.path)

	if err := os.MkdirAll(q.path, 0755); err != nil {
		return nil, errors.NewStorageError("Failed to create quorum path %s", q.path, err)
	}

	return q, nil
}

func (q *Quorum) TryLockIfNotExists(ctx context.Context, hash *chainhash.Hash, fileType fileformat.FileType) (locked bool, exists bool, release func(), err error) {
	b, err := q.exister.Exists(ctx, hash[:], fileType)
	if err != nil {
		return false, false, noopFunc, err
	}

	if b {
		return false, true, noopFunc, nil
	}

	lockFile := path.Join(q.path, hash.String()) + ".lock"

	q.expireLockIfOld(lockFile)

	// Attempt to acquire lock by atomically creating the lock file
	// The O_CREATE|O_EXCL|O_WRONLY flags ensure the file is created only if it does not already exist
	file, err := os.OpenFile(lockFile, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0644)
	if err != nil {
		if errors.Is(err, os.ErrExist) {
			q.logger.Debugf("[TryLockIfNotExists] lock file %s already exists", lockFile)

			return false, false, noopFunc, nil // Lock held by someone else
		}

		q.logger.Errorf("[TryLockIfNotExists] error creating lock file %s: %v", lockFile, err)

		return false, false, noopFunc, err
	}

	// Close the file immediately after creating it
	if err := file.Close(); err != nil {
		q.logger.Warnf("failed to close lock file %q: %v", lockFile, err)
	}

	// Set up automatic lock release
	ctx, cancel := context.WithCancel(ctx)

	go q.autoReleaseLock(ctx, cancel, lockFile)

	return true, false, func() {
		cancel()
		releaseLock(q.logger, lockFile)
	}, nil
}

// If the lock file already exists, the item is being processed by another node. However, the lock may be stale.
// If the lock file mtime is more than quorumTimeout old it is considered stale and can be removed.
func (q *Quorum) expireLockIfOld(lockFile string) {
	if info, err := os.Stat(lockFile); err == nil {
		t := q.timeout
		if q.absoluteTimeout > t {
			t = q.absoluteTimeout
		}

		if time.Since(info.ModTime()) > t {
			q.logger.Warnf("removing stale lock file %q", lockFile)

			releaseLock(q.logger, lockFile)
		}
	}
}

func releaseLock(logger ulogger.Logger, lockFile string) {
	if err := os.Remove(lockFile); err != nil {
		if !errors.Is(err, os.ErrNotExist) {
			logger.Warnf("failed to remove lock file %q: %v", lockFile, err)
		}
	}
}

func (q *Quorum) autoReleaseLock(ctx context.Context, cancel context.CancelFunc, lockFile string) {
	if q.absoluteTimeout == 0 {
		// Initialize ticker to update the lock file twice every quorumTimeout
		ticker := time.NewTicker(q.timeout / 2)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				releaseLock(q.logger, lockFile)

				return
			case <-ticker.C:
				// Touch the lock file by updating its access and modification times to the current time
				now := time.Now()
				if err := os.Chtimes(lockFile, now, now); err != nil {
					q.logger.Warnf("failed to update lock file %q: %v", lockFile, err)
				}
			}
		}
	} else {
		// If absoluteTimeout is set, we use a timer to release the lock after the timeout
		// Release the lock after timeout or when context is cancelled
		select {
		case <-ctx.Done():
		case <-time.After(q.absoluteTimeout):
		}
		releaseLock(q.logger, lockFile)
	}
}

// TryLockIfNotExistsWithTimeout attempts to acquire a lock for the given hash.
// Knowing a lock has a timeout, it keeps retrying just beyond the lock timeout or until the file exists.
// In theory it will always succeed in getting a lock or returning that the file exists.
func (q *Quorum) TryLockIfNotExistsWithTimeout(ctx context.Context, hash *chainhash.Hash, fileType fileformat.FileType) (locked bool, exists bool, release func(), err error) {
	t := q.timeout
	if q.absoluteTimeout > t {
		t = q.absoluteTimeout
	}

	t += 1 * time.Second

	cancelCtx, cancel := context.WithTimeout(ctx, t)
	defer cancel()

	const retryDelay = 10 * time.Millisecond // How long to wait between checks

	retryCount := 0

	for {
		locked, exists, release, err = q.TryLockIfNotExists(ctx, hash, fileType)
		if err != nil || locked || exists {
			if retryCount > 1 && locked {
				q.logger.Warnf("[TryLockIfNotExistsWithTimeout][%s] New lock acquired after waiting for previous lock to expire. Two potential issues. 1) Whatever acquired the previous lock is taking longer than lock timeout to process therefore we are about to duplicate effort or 2) whatever acquired the previous lock is stuck", hash)
			}

			return locked, exists, release, err
		}

		retryCount++

		// Lock not acquired and item doesn't exist yet, wait briefly before retrying.
		// Use a context-aware delay to exit promptly if context is cancelled during the wait.
		select {
		case <-time.After(retryDelay):
			// Sleep finished normally, continue loop.
		case <-cancelCtx.Done(): // This channel will be closed if ctx is done OR if timeout t is reached
			// Check if the parent context (ctx) is the reason cancelCtx is done.
			// ctx.Err() is non-nil if ctx is cancelled.
			if ctx.Err() != nil {
				// Parent context was cancelled.
				return locked, exists, release, errors.NewStorageError("[TryLockIfNotExistsWithTimeout][%s] context done during retry delay", hash)
			}
			// If ctx.Err() is nil, it means cancelCtx timed out on its own (due to 't').
			return locked, exists, release, errors.NewStorageError("[TryLockIfNotExistsWithTimeout][%s] timeout waiting %s for lock to free up during retry delay", hash, t)
		}
	}
}
