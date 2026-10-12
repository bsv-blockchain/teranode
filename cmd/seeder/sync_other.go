//go:build !linux && !darwin

package seeder

import "github.com/bsv-blockchain/teranode/errors"

// syncFilesystem is not implemented on this platform. The seeder only relaxes
// external-store fsync where it can sync afterwards, so set
// seeder_externalStoreFsyncMode=full here.
func syncFilesystem(_ string) error {
	return errors.NewConfigurationError("filesystem sync is not supported on this platform; set seeder_externalStoreFsyncMode=full")
}
