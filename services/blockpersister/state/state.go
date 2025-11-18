// Package state provides persistent state management for the block persister service.
//
// This package handles the storage and retrieval of block processing state information,
// enabling the block persister to track which blocks have been successfully persisted
// and maintain consistency across service restarts. The state is stored in a local file
// system using a simple append-only format for reliability and crash recovery.
//
// Key features:
//   - Thread-safe file-based state persistence using file locking
//   - Atomic block state updates with proper error handling
//   - Recovery from incomplete operations through file validation
//   - Efficient retrieval of the last persisted block height
//
// The state file format stores block height and hash pairs, allowing the service to
// resume processing from the correct point after restarts or failures. File locking
// ensures concurrent access safety when multiple processes might access the state.
//
// Usage:
//
//	state := state.New(logger, "/path/to/state/file")
//	height, err := state.GetLastPersistedBlockHeight()
//	err = state.AddBlock(height+1, blockHash)
package state

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"syscall"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	safeconversion "github.com/bsv-blockchain/go-safe-conversion"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/ulogger"
)

const maxInt = int(^uint(0) >> 1)

// State manages the persistence of block processing state
type State struct {
	// logger provides logging functionality
	logger ulogger.Logger

	// filePath is the path to the state file
	filePath string

	// fileLock provides synchronization for file operations
	fileLock sync.Mutex
}

// New creates a new State instance
// Parameters:
//   - logger: for logging operations
//   - blocksFilePath: path to the state file
func New(logger ulogger.Logger, blocksFilePath string) *State {
	return &State{
		logger:   logger,
		filePath: blocksFilePath,
	}
}

// GetLastPersistedBlock retrieves the height and hash of the last persisted block
// Returns:
//   - uint32: the block height (0 if no blocks persisted)
//   - *chainhash.Hash: the block hash (nil if no blocks persisted)
//   - error: any error encountered
func (s *State) GetLastPersistedBlock() (uint32, *chainhash.Hash, error) {
	s.fileLock.Lock()
	defer s.fileLock.Unlock()

	// Open file for reading
	file, err := os.Open(s.filePath)
	if err != nil {
		if os.IsNotExist(err) {
			return 0, nil, nil // Return zero values if file doesn't exist yet
		}

		return 0, nil, errors.NewProcessingError("failed to open blocks file", err)
	}
	defer file.Close()

	deferFn, err := s.lockFile(file, syscall.LOCK_SH)
	if err != nil {
		return 0, nil, errors.NewProcessingError("could not lock file", err)
	}
	defer deferFn()

	const maxLineLength = 80 // 8 (height) + 1 (comma) + 64 (hash) + 1 (newline) = 74 bytes
	buf := make([]byte, maxLineLength)

	// Seek from end of file
	offset, err := file.Seek(-maxLineLength, io.SeekEnd)
	if err != nil {
		if errors.Is(err, syscall.EINVAL) { // File is smaller than maxLineLength
			offset = 0

			if _, err := file.Seek(0, io.SeekStart); err != nil {
				return 0, nil, errors.NewProcessingError("failed to seek to start", err)
			}
		} else {
			return 0, nil, errors.NewProcessingError("failed to seek from end", err)
		}
	}

	n, err := file.Read(buf)
	if err != nil && err != io.EOF {
		return 0, nil, errors.NewProcessingError("failed to read file", err)
	}

	if n == 0 {
		return 0, nil, nil
	}

	buf = buf[:n] // Trim buffer to actual read size

	var lastIndex int
	// If there's only one newline and we're not at the start, it must be at the end
	if offset == 0 && bytes.Count(buf, []byte{'\n'}) == 1 {
		lastIndex = 0
	} else {
		// Find the second last newline by finding the last newline before the final one
		lastNewline := bytes.LastIndex(buf, []byte{'\n'})
		if lastNewline == -1 {
			return 0, nil, errors.NewProcessingError("no newline found in last chunk", nil)
		}

		// Find the second last newline
		lastIndex = bytes.LastIndex(buf[:lastNewline], []byte{'\n'})
		if lastIndex == -1 {
			return 0, nil, errors.NewProcessingError("no second newline found in last chunk", nil)
		}
	}

	// Extract the second last line
	var lastLine []byte
	if lastIndex == 0 {
		lastLine = buf
	} else {
		lastLine = buf[lastIndex+1:]
	}

	// Parse height and hash from the line (format: height,hash)
	commaIndex := bytes.IndexByte(lastLine, ',')
	if commaIndex == -1 {
		return 0, nil, errors.NewProcessingError("invalid line format: no comma found", nil)
	}

	// Parse height
	height, err := strconv.ParseUint(string(lastLine[:commaIndex]), 10, 32)
	if err != nil {
		return 0, nil, errors.NewProcessingError("failed to parse block height", err)
	}

	heightUint32, err := safeconversion.Uint64ToUint32(height)
	if err != nil {
		return 0, nil, err
	}

	// Extract hash (everything after comma, trimming newline)
	hashBytes := lastLine[commaIndex+1:]
	hashStr := string(bytes.TrimSpace(hashBytes))

	// Parse the hash string into a chainhash.Hash
	hash, err := chainhash.NewHashFromStr(hashStr)
	if err != nil {
		return 0, nil, errors.NewProcessingError("failed to parse block hash", err)
	}

	return heightUint32, hash, nil
}

// GetLastPersistedBlockHeight retrieves the height of the last persisted block
// This is a convenience wrapper around GetLastPersistedBlock for backwards compatibility
// Returns:
//   - uint32: the block height
//   - error: any error encountered
func (s *State) GetLastPersistedBlockHeight() (uint32, error) {
	height, _, err := s.GetLastPersistedBlock()
	return height, err
}

// AddBlock records a new block in the state file.
// It safely appends a block's height and hash to the blocks file.
// Parameters:
//   - height: block height
//   - hash: block hash as string
//
// Returns error if the operation fails
func (s *State) AddBlock(height uint32, hash string) error {
	s.fileLock.Lock()
	defer s.fileLock.Unlock()

	// Ensure directory exists
	dir := filepath.Dir(s.filePath)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return errors.NewProcessingError("failed to create directory", err)
	}

	// Open file with O_APPEND and O_CREATE flags
	file, err := os.OpenFile(s.filePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return errors.NewProcessingError("failed to open blocks file", err)
	}
	defer file.Close()

	deferFn, err := s.lockFile(file, syscall.LOCK_EX)
	if err != nil {
		return errors.NewProcessingError("could not lock file", err)
	}
	defer deferFn()

	// Write the block info with a newline
	line := fmt.Sprintf("%d,%s\n", height, hash)

	// Write in a single operation for atomicity
	if _, err := file.WriteString(line); err != nil {
		return errors.NewProcessingError("failed to write to blocks file", err)
	}

	// Force sync to disk
	if err := file.Sync(); err != nil {
		return errors.NewProcessingError("failed to sync blocks file", err)
	}

	return nil
}

// lockFile implements file locking for concurrent access
// Parameters:
//   - file: file to lock
//   - lockType: type of lock to acquire
//
// Returns:
//   - func(): unlock function
//   - error: any error encountered
func (s *State) lockFile(file *os.File, lockType int) (func(), error) {
	// Get shared lock on the file
	fd := file.Fd()
	// Check if fd can fit in an int without overflow
	if fd > uintptr(maxInt) {
		return nil, errors.NewProcessingError("file descriptor too large", nil)
	}

	fdi, err := safeconversion.UintptrToInt(fd)
	if err != nil {
		return nil, err
	}

	if err := syscall.Flock(fdi, lockType); err != nil {
		return nil, errors.NewProcessingError("failed to lock file", err)
	}

	return func() {
		if err := syscall.Flock(fdi, syscall.LOCK_UN); err != nil {
			// Log error but can't return it from deferred function
			s.logger.Errorf("failed to unlock file: %w", err)
		}
	}, nil
}
