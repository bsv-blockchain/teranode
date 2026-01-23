// Package tempstore provides temporary disk-based storage using BadgerDB.
// It is designed for scenarios where data needs to be temporarily stored on disk
// to reduce memory usage, such as sorting large datasets.
package tempstore

import (
	"fmt"
	"os"
	"path/filepath"
	"sync/atomic"
	"time"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/dgraph-io/badger/v4"
)

// BadgerTempStore provides temporary disk-based storage using BadgerDB.
// It automatically creates a temporary directory and cleans it up on Close.
type BadgerTempStore struct {
	db   *badger.DB
	path string
}

// Options configures the temporary store.
type Options struct {
	// BasePath is the parent directory for the temporary store.
	// If empty, os.TempDir() is used.
	BasePath string

	// Prefix is used to create a unique subdirectory name.
	Prefix string

	// SyncWrites enables synchronous writes for durability.
	// Default is false for better performance (data is temporary anyway).
	SyncWrites bool
}

// New creates a new temporary BadgerDB store.
// The store is created in a unique subdirectory under the specified base path.
// Call Close() to clean up the temporary directory when done.
func New(opts Options) (*BadgerTempStore, error) {
	basePath := opts.BasePath
	if basePath == "" {
		basePath = os.TempDir()
	}

	// Create unique directory name
	dirName := fmt.Sprintf("%s-%d-%d", opts.Prefix, time.Now().UnixNano(), os.Getpid())
	path := filepath.Join(basePath, dirName)

	if err := os.MkdirAll(path, 0700); err != nil {
		return nil, errors.NewStorageError("failed to create temp directory", err)
	}

	// Configure BadgerDB for temporary storage
	badgerOpts := badger.DefaultOptions(path)
	badgerOpts.SyncWrites = opts.SyncWrites
	badgerOpts.Logger = nil // Disable logging
	badgerOpts.CompactL0OnClose = false
	badgerOpts.NumVersionsToKeep = 1

	// Optimize for write-heavy then read-heavy workload
	badgerOpts.NumMemtables = 2
	badgerOpts.NumLevelZeroTables = 2
	badgerOpts.NumLevelZeroTablesStall = 4
	badgerOpts.ValueLogFileSize = 256 << 20 // 256MB

	db, err := badger.Open(badgerOpts)
	if err != nil {
		// Clean up directory on failure
		_ = os.RemoveAll(path)
		return nil, errors.NewServiceError("failed to open badger: %w", err)
	}

	return &BadgerTempStore{
		db:   db,
		path: path,
	}, nil
}

// Put stores a key-value pair.
func (s *BadgerTempStore) Put(key, value []byte) error {
	return s.db.Update(func(txn *badger.Txn) error {
		return txn.Set(key, value)
	})
}

// Get retrieves a value by key.
// Returns nil, nil if the key does not exist.
func (s *BadgerTempStore) Get(key []byte) ([]byte, error) {
	var result []byte
	err := s.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(key)
		if err == badger.ErrKeyNotFound {
			return nil
		}
		if err != nil {
			return err
		}
		result, err = item.ValueCopy(nil)
		return err
	})
	return result, err
}

// WriteBatch provides efficient batch write operations.
type WriteBatch struct {
	wb    *badger.WriteBatch
	store *BadgerTempStore
	count atomic.Int64
}

// NewWriteBatch creates a new write batch for efficient bulk inserts.
func (s *BadgerTempStore) NewWriteBatch() *WriteBatch {
	return &WriteBatch{
		wb:    s.db.NewWriteBatch(),
		store: s,
	}
}

// Set adds a key-value pair to the batch.
func (b *WriteBatch) Set(key, value []byte) error {
	b.count.Add(1)
	return b.wb.Set(key, value)
}

// Flush writes all pending entries to disk.
func (b *WriteBatch) Flush() error {
	if b.count.Load() == 0 {
		return nil
	}
	err := b.wb.Flush()
	if err != nil {
		return err
	}
	// Create new batch for subsequent writes
	b.wb = b.store.db.NewWriteBatch()
	b.count.Store(0)
	return nil
}

// Cancel discards the batch without writing.
func (b *WriteBatch) Cancel() {
	b.wb.Cancel()
}

// Count returns the number of entries in the current batch.
func (b *WriteBatch) Count() int64 {
	return b.count.Load()
}

// Iterate iterates over all key-value pairs in lexicographic key order.
// The callback receives copies of the key and value that are safe to retain.
func (s *BadgerTempStore) Iterate(fn func(key, value []byte) error) error {
	return s.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchSize = 100
		opts.PrefetchValues = true

		it := txn.NewIterator(opts)
		defer it.Close()

		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()
			key := item.KeyCopy(nil)
			value, err := item.ValueCopy(nil)
			if err != nil {
				return err
			}
			if err := fn(key, value); err != nil {
				return err
			}
		}
		return nil
	})
}

// IterateKeys iterates over all keys in lexicographic order without fetching values.
// This is more efficient when only keys are needed.
func (s *BadgerTempStore) IterateKeys(fn func(key []byte) error) error {
	return s.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false

		it := txn.NewIterator(opts)
		defer it.Close()

		for it.Rewind(); it.Valid(); it.Next() {
			key := it.Item().KeyCopy(nil)
			if err := fn(key); err != nil {
				return err
			}
		}
		return nil
	})
}

// Count returns the approximate number of keys in the store.
func (s *BadgerTempStore) Count() int64 {
	var count int64
	_ = s.IterateKeys(func(key []byte) error {
		count++
		return nil
	})
	return count
}

// Path returns the path to the temporary directory.
func (s *BadgerTempStore) Path() string {
	return s.path
}

// Close closes the database and removes the temporary directory.
func (s *BadgerTempStore) Close() error {
	if s.db != nil {
		if err := s.db.Close(); err != nil {
			// Still try to clean up
			_ = os.RemoveAll(s.path)
			return errors.NewServiceError("failed to close badger: %w", err)
		}
	}

	return os.RemoveAll(s.path)
}
