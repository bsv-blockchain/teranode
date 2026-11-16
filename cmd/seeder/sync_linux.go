package seeder

import (
	"os"

	"github.com/bsv-blockchain/teranode/errors"
	"golang.org/x/sys/unix"
)

// syncFilesystem flushes the filesystem containing path to stable storage with
// syncfs(2), which (unlike sync(2)) reports writeback errors (Linux 5.8+).
func syncFilesystem(path string) error {
	f, err := os.Open(path)
	if err != nil {
		return errors.NewStorageError("failed to open %s for syncfs", path, err)
	}

	defer func() {
		_ = f.Close()
	}()

	if err = unix.Syncfs(int(f.Fd())); err != nil { //nolint:gosec // fd fits in int
		return errors.NewStorageError("syncfs failed for %s", path, err)
	}

	return nil
}
