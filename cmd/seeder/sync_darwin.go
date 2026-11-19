package seeder

import (
	"os"
	"syscall"

	"github.com/bsv-blockchain/teranode/errors"
)

// syncFilesystem flushes filesystem buffers. macOS has no syncfs, so this is
// sync(2), which may return before writeback completes; macOS is a dev
// platform for the seeder, not a production one.
func syncFilesystem(path string) error {
	if _, err := os.Stat(path); err != nil {
		return errors.NewStorageError("cannot sync %s", path, err)
	}

	return syscall.Sync()
}
