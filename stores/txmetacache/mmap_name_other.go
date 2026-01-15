//go:build !linux

package txmetacache

import (
	"sync/atomic"

	"github.com/bsv-blockchain/teranode/errors"
	"golang.org/x/sys/unix"
)

// globalChunkCounter tracks the total number of mmap allocations across all buckets
var globalChunkCounter atomic.Uint64

// allocateNamedMmap performs an mmap allocation
// On non-Linux platforms, naming is not supported so this is a simple wrapper around unix.Mmap
//
// Parameters:
//   - size: Number of bytes to allocate
//
// Returns:
//   - Allocated byte slice backed by mmap'd memory
//   - Error if allocation fails
func allocateNamedMmap(size int) ([]byte, error) {
	data, err := unix.Mmap(-1, 0, size, unix.PROT_READ|unix.PROT_WRITE, unix.MAP_ANON|unix.MAP_PRIVATE)
	if err != nil {
		return nil, errors.NewProcessingError("cannot allocate %d bytes via mmap", size, err)
	}

	// Increment counter for consistency with Linux version, even though we don't name
	globalChunkCounter.Add(1)

	return data, nil
}
