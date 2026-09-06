package util

import (
	"os"

	"github.com/bsv-blockchain/teranode/errors"
)

// ValidateWritableDir checks that dir exists, is a directory, and is writable.
// An empty dir is treated as "feature disabled" and always returns nil, so
// callers can invoke this unconditionally on optional off-heap storage
// settings without special-casing the default (off) value.
//
// Writability is verified by actually creating and removing a temporary file,
// rather than inspecting permission bits, so the check also catches read-only
// filesystems and other cases plain stat-based permission checks would miss.
func ValidateWritableDir(dir string) error {
	if dir == "" {
		return nil
	}

	info, err := os.Stat(dir)
	if err != nil {
		return errors.NewConfigurationError("directory %s is not accessible", dir, err)
	}

	if !info.IsDir() {
		return errors.NewConfigurationError("path %s is not a directory", dir)
	}

	f, err := os.CreateTemp(dir, ".teranode-writable-check-*")
	if err != nil {
		return errors.NewConfigurationError("directory %s is not writable", dir, err)
	}

	name := f.Name()

	if err := f.Close(); err != nil {
		_ = os.Remove(name)
		return errors.NewConfigurationError("failed to close writability check file in %s", dir, err)
	}

	if err := os.Remove(name); err != nil {
		return errors.NewConfigurationError("failed to clean up writability check file in %s", dir, err)
	}

	return nil
}
