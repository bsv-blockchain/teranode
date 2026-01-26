// Package logger provides a debugging wrapper for blob.Store implementations.
//
// The logger package implements a transparent logging wrapper that intercepts
// all blob store operations and logs detailed debug information including
// operation parameters, results, and call stack traces. This is primarily
// used for development, debugging, and troubleshooting blob storage issues.
//
// Key features:
//   - Transparent wrapper preserving all blob.Store interface behavior
//   - Detailed debug logging with operation parameters and results
//   - Call stack tracing to identify operation origins
//   - Configurable via store URL query parameter: logger=true
//   - Zero performance impact when debug logging is disabled
//
// The wrapper is typically applied automatically by the blob store factory
// when the logger=true parameter is specified in the store URL.
package logger

import (
	"context"
	"fmt"
	"io"
	"path/filepath"
	"runtime"
	"strings"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/stores/blob/options"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/debugflags"
	"github.com/ordishs/gocore"
)

// blobStore defines the interface contract for blob storage backends.
// This interface mirrors the main blob.Store interface to enable transparent wrapping.
type blobStore interface {
	Health(ctx context.Context, checkLiveness bool) (int, string, error)
	Exists(ctx context.Context, key []byte, fileType fileformat.FileType, opts ...options.FileOption) (bool, error)
	Get(ctx context.Context, key []byte, fileType fileformat.FileType, opts ...options.FileOption) ([]byte, error)
	GetIoReader(ctx context.Context, key []byte, fileType fileformat.FileType, opts ...options.FileOption) (io.ReadCloser, error)
	Set(ctx context.Context, key []byte, fileType fileformat.FileType, value []byte, opts ...options.FileOption) error
	SetFromReader(ctx context.Context, key []byte, fileType fileformat.FileType, value io.ReadCloser, opts ...options.FileOption) error
	SetDAH(ctx context.Context, key []byte, fileType fileformat.FileType, newDAH uint32, opts ...options.FileOption) error
	GetDAH(ctx context.Context, key []byte, fileType fileformat.FileType, opts ...options.FileOption) (uint32, error)
	Del(ctx context.Context, key []byte, fileType fileformat.FileType, opts ...options.FileOption) error
	Close(ctx context.Context) error
	SetCurrentBlockHeight(height uint32)
}

// Logger is a debugging wrapper that logs all blob store operations.
//
// It implements the blobStore interface by wrapping an underlying store
// and logging detailed information about each operation including parameters,
// results, errors, and call stack traces. The wrapper is transparent and
// preserves all original functionality while adding comprehensive logging.
//
// All log messages are emitted at DEBUG level with structured formatting
// that includes operation type, key (reversed for readability), file type,
// and relevant parameters or results.
type Logger struct {
	logger ulogger.Logger
	store  blobStore
}

func (s *Logger) debugEnabled() bool {
	if !debugflags.BlobstoreEnabled() || s == nil || s.logger == nil {
		return false
	}

	return s.logger.LogLevel() <= int(gocore.DEBUG)
}

func (s *Logger) debugf(format string, args ...interface{}) {
	if !s.debugEnabled() {
		return
	}

	s.logger.Debugf(format, args...)
}

// New creates a new Logger wrapper that adds debug logging to blob store operations.
//
// The returned wrapper implements the blobStore interface and logs all operations
// with detailed parameter and result information. Logging is performed at DEBUG
// level, so it can be controlled via logger configuration.
//
// Parameters:
//   - logger: Logger instance for debug output
//   - store: Underlying blob store to wrap
//
// Returns:
//   - blobStore: Logger wrapper implementing the blob store interface
func New(logger ulogger.Logger, store blobStore) blobStore {
	s := &Logger{
		logger: logger,
		store:  store,
	}

	return s
}

// caller generates a formatted call stack trace for debugging purposes.
//
// It walks up the call stack to identify the origin of blob store operations,
// providing context about which code triggered each operation. The trace
// includes function names, file paths, and line numbers formatted for readability.
//
// Returns:
//   - string: Formatted call stack trace with up to 5 levels of depth
func caller() string {
	var callers []string

	depth := 5

	for i := 0; i < depth; i++ {
		pc, file, line, ok := runtime.Caller(2 + i)
		if !ok {
			break
		}

		folders := strings.Split(file, string(filepath.Separator))
		if len(folders) > 0 {
			if folders[0] == "github.com" {
				folders = folders[1:]
			}

			if folders[0] == "bitcoin-sv" {
				folders = folders[1:]
			}

			if folders[0] == "teranode" {
				folders = folders[1:]
			}
		}

		file = filepath.Join(folders...)

		funcName := runtime.FuncForPC(pc).Name()
		funcPaths := strings.Split(funcName, "/")
		funcName = funcPaths[len(funcPaths)-1]

		callers = append(callers, fmt.Sprintf("called from %s: %s:%d", funcName, file, line))
	}

	return strings.Join(callers, ",")
}

func (s *Logger) Health(ctx context.Context, checkLiveness bool) (int, string, error) {
	s.debugf("[BlobStore][logger][Health] : %s", caller())
	return s.store.Health(ctx, checkLiveness)
}

func (s *Logger) Exists(ctx context.Context, key []byte, fileType fileformat.FileType, opts ...options.FileOption) (bool, error) {
	exists, err := s.store.Exists(ctx, key, fileType, opts...)
	k := bt.ReverseBytes(key)
	s.debugf("[BlobStore][logger][Exists] key %x, fileType %s, exists %t, err %v : %s", k, fileType, exists, err, caller())

	return exists, err
}

func (s *Logger) Get(ctx context.Context, key []byte, fileType fileformat.FileType, opts ...options.FileOption) ([]byte, error) {
	value, err := s.store.Get(ctx, key, fileType, opts...)
	k := bt.ReverseBytes(key)
	s.debugf("[BlobStore][logger][Get] key %x, fileType %s, err %v : %s", k, fileType, err, caller())

	return value, err
}

func (s *Logger) GetIoReader(ctx context.Context, key []byte, fileType fileformat.FileType, opts ...options.FileOption) (io.ReadCloser, error) {
	reader, err := s.store.GetIoReader(ctx, key, fileType, opts...)
	k := bt.ReverseBytes(key)
	s.debugf("[BlobStore][logger][GetIoReader] key %x, fileType %s, err %v : %s", k, fileType, err, caller())

	return reader, err
}

func (s *Logger) Set(ctx context.Context, key []byte, fileType fileformat.FileType, value []byte, opts ...options.FileOption) error {
	err := s.store.Set(ctx, key, fileType, value, opts...)
	k := bt.ReverseBytes(key)
	s.debugf("[BlobStore][logger][Set] key %x, fileType %s, err %v : %s", k, fileType, err, caller())

	return err
}

func (s *Logger) SetFromReader(ctx context.Context, key []byte, fileType fileformat.FileType, reader io.ReadCloser, opts ...options.FileOption) error {
	err := s.store.SetFromReader(ctx, key, fileType, reader, opts...)
	k := bt.ReverseBytes(key)
	s.debugf("[BlobStore][logger][SetFromReader] key %x, fileType %s, err %v : %s", k, fileType, err, caller())

	return err
}

func (s *Logger) SetDAH(ctx context.Context, key []byte, fileType fileformat.FileType, dah uint32, opts ...options.FileOption) error {
	err := s.store.SetDAH(ctx, key, fileType, dah, opts...)
	k := bt.ReverseBytes(key)
	s.debugf("[BlobStore][logger][SetDAH] key %x, fileType %s, dah %d, err %v : %s", k, fileType, dah, err, caller())

	return err
}

func (s *Logger) GetDAH(ctx context.Context, key []byte, fileType fileformat.FileType, opts ...options.FileOption) (uint32, error) {
	dah, err := s.store.GetDAH(ctx, key, fileType, opts...)
	k := bt.ReverseBytes(key)
	s.debugf("[BlobStore][logger][GetDAH] key %x, fileType %s, dah %d, err %v : %s", k, fileType, dah, err, caller())

	return dah, err
}

func (s *Logger) Del(ctx context.Context, key []byte, fileType fileformat.FileType, opts ...options.FileOption) error {
	err := s.store.Del(ctx, key, fileType, opts...)
	k := bt.ReverseBytes(key)
	s.debugf("[BlobStore][logger][Del] key %x, fileType %s, err %v : %s", k, fileType, err, caller())

	return err
}

func (s *Logger) Close(ctx context.Context) error {
	err := s.store.Close(ctx)
	s.debugf("[BlobStore][logger][Close] err %v : %s", err, caller())

	return err
}

func (s *Logger) SetCurrentBlockHeight(height uint32) {
	s.store.SetCurrentBlockHeight(height)
	s.debugf("[BlobStore][logger][SetCurrentBlockHeight] height %d : %s", height, caller())
}
