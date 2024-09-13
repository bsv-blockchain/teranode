package quorum

import (
	"context"
	"os"
	"path"
	"time"

	"github.com/bitcoin-sv/ubsv/errors"
	"github.com/bitcoin-sv/ubsv/stores/blob/options"
	"github.com/bitcoin-sv/ubsv/ulogger"
	"github.com/libsv/go-bt/v2/chainhash"
)

type existerIfc interface {
	Exists(ctx context.Context, key []byte, opts ...options.Options) (bool, error)
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

func WithExtension(extension string) QuorumOption {
	return func(q *Quorum) {
		q.extension = extension
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
	extension       string
	exister         existerIfc
	existerOpts     []options.Options
}

func New(logger ulogger.Logger, exister existerIfc, path string, quorumOpts ...QuorumOption) (*Quorum, error) {
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

	if q.extension != "" {
		q.existerOpts = append(q.existerOpts, options.WithFileExtension(q.extension))
	}

	logger.Infof("Creating subtree quorum path: %s", q.path)

	if err := os.MkdirAll(q.path, 0755); err != nil {
		return nil, errors.NewStorageError("Failed to create quorum path %s", q.path, err)
	}

	return q, nil
}

// nolint:gocognit
func (q *Quorum) TryLockIfNotExists(ctx context.Context, hash *chainhash.Hash) (bool, bool, func(), error) {
	b, err := q.exister.Exists(ctx, hash[:], q.existerOpts...)
	if err != nil {
		return false, false, noopFunc, err
	}

	if b {
		return false, true, noopFunc, nil
	}

	lockFile := path.Join(q.path, hash.String()) + ".lock"

	// If the lock file already exists, the item is being processed by another node. However, the lock may be stale.
	// If the lock file mtime is more than quorumTimeout old it is considered stale and can be removed.
	if info, err := os.Stat(lockFile); err == nil {
		t := q.timeout
		if q.absoluteTimeout > t {
			t = q.absoluteTimeout
		}

		if time.Since(info.ModTime()) > t {
			if err := os.Remove(lockFile); err != nil {
				q.logger.Warnf("failed to remove stale lock file %q: %v", lockFile, err)
			}
		}
	}

	// Attempt to acquire lock by atomically creating the lock file
	// The O_CREATE|O_EXCL|O_WRONLY flags ensure the file is created only if it does not already exist
	file, err := os.OpenFile(lockFile, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0644)
	if err != nil {
		if errors.Is(err, os.ErrExist) {
			// Failed to acquire lock (file already exists or other error)
			return false, false, noopFunc, nil
		}

		return false, false, noopFunc, err
	}

	// Close the file immediately after creating it
	if err := file.Close(); err != nil {
		q.logger.Warnf("failed to close lock file %q: %v", lockFile, err)
	}

	// Set up automatic lock release
	ctx, cancel := context.WithCancel(ctx)

	go func() {
		if q.absoluteTimeout == 0 {
			// Initialize ticker to update the lock file twice every quorumTimeout
			ticker := time.NewTicker(q.timeout / 2)
			defer ticker.Stop()

			for {
				select {
				case <-ctx.Done():
					if err := os.Remove(lockFile); err != nil && !os.IsNotExist(err) {
						q.logger.Warnf("failed to remove lock file %q: %v", lockFile, err)
					}

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
	}()

	return true, false, func() {
		cancel()
		releaseLock(q.logger, lockFile)
	}, nil
}

func releaseLock(logger ulogger.Logger, lockFile string) {
	if err := os.Remove(lockFile); err != nil {
		if !errors.Is(err, os.ErrNotExist) {
			logger.Warnf("failed to remove lock file %q: %v", lockFile, err)
		}
	}
}
