//go:build linux

package txmetacache

import (
	"fmt"
	"sync/atomic"
	"unsafe"

	"github.com/bsv-blockchain/teranode/errors"
	"golang.org/x/sys/unix"
)

const (
	PR_SET_VMA           = 0x53564d41
	PR_SET_VMA_ANON_NAME = 0
)

// globalChunkCounter tracks the total number of mmap allocations across all buckets
// This ensures unique names for each chunk: TNC-0, TNC-1, TNC-2, etc.
var globalChunkCounter atomic.Uint64

// nameMmapRegion names an anonymous memory region using PR_SET_VMA_ANON_NAME
// This makes it easy to identify teranode cache chunks in /proc/pid/maps
//
// Parameters:
//   - addr: Starting address of the mmap'd region
//   - length: Size of the region in bytes
//   - chunkNum: Chunk number for naming (TNC-{chunkNum})
//
// Returns:
//   - Error if the prctl syscall fails (ignored in production as it's not critical)
//
// The naming is visible in:
//   - /proc/[pid]/maps
//   - /proc/[pid]/smaps
//   - Core dumps
func nameMmapRegion(addr unsafe.Pointer, length uintptr, chunkNum uint64) error {
	name := fmt.Sprintf("TNC-%d", chunkNum)
	nameBytes := append([]byte(name), 0) // null-terminated

	_, _, errno := unix.Syscall6(
		unix.SYS_PRCTL,
		PR_SET_VMA,
		PR_SET_VMA_ANON_NAME,
		uintptr(addr),
		length,
		uintptr(unsafe.Pointer(&nameBytes[0])),
		0,
	)

	if errno != 0 {
		return errors.NewProcessingError("prctl PR_SET_VMA_ANON_NAME failed for %s", name, errno)
	}

	return nil
}

// allocateNamedMmap performs an mmap allocation and names the resulting region
// This is a convenience wrapper around unix.Mmap and nameMmapRegion
//
// Parameters:
//   - size: Number of bytes to allocate
//
// Returns:
//   - Allocated byte slice backed by mmap'd memory
//   - Error if allocation or naming fails
func allocateNamedMmap(size int) ([]byte, error) {
	data, err := unix.Mmap(-1, 0, size, unix.PROT_READ|unix.PROT_WRITE, unix.MAP_ANON|unix.MAP_PRIVATE)
	if err != nil {
		return nil, errors.NewProcessingError("cannot allocate %d bytes via mmap", size, err)
	}

	// Get unique chunk number and name the region
	chunkNum := globalChunkCounter.Add(1) - 1
	if len(data) > 0 {
		// Naming failures are logged but don't fail the allocation
		// This ensures the cache still works on older kernels without PR_SET_VMA_ANON_NAME
		_ = nameMmapRegion(unsafe.Pointer(&data[0]), uintptr(len(data)), chunkNum)
	}

	return data, nil
}
