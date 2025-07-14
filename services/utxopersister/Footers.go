// Package utxopersister creates and maintains up-to-date Unspent Transaction Output (UTXO) file sets
// for each block in the Teranode blockchain. Its primary function is to process the output of the
// Block Persister service (utxo-additions and utxo-deletions) and generate complete UTXO set files.
// The resulting UTXO set files can be exported and used to initialize the UTXO store in new Teranode instances.
//
// Footers.go implements functionality for reading footer metadata from UTXO files. The footer
// contains essential statistics about the file contents, including transaction counts and UTXO counts.
// This metadata is crucial for validating file integrity and providing summary information about
// the UTXO set without requiring a full file scan.

package utxopersister

import (
	"encoding/binary"
	"io"
	"os"

	"github.com/bitcoin-sv/teranode/errors"
)

// GetFooter retrieves the transaction and UTXO counts from the footer of a UTXO file.
// It reads the footer metadata located at the end of the file, which contains statistical
// information about the file contents. This function requires a seekable reader (os.File)
// to efficiently access the footer without reading the entire file.
//
// The footer format consists of:
// - Last 16 bytes of the file contain two uint64 values in little-endian format
// - Bytes -16 to -9: Transaction count (8 bytes)
// - Bytes -8 to -1: UTXO count (8 bytes)
// - This follows after a 32-byte EOF marker
//
// Parameters:
//   - r: io.Reader that must be an *os.File to support seeking operations
//
// Returns:
//   - uint64: Transaction count from the footer
//   - uint64: UTXO count from the footer
//   - error: Error if seeking is not supported, seek operation fails, or reading fails
//
// This function is essential for quickly obtaining file statistics without scanning
// the entire UTXO file, enabling efficient validation and reporting operations.
func GetFooter(r io.Reader) (uint64, uint64, error) {
	f, ok := r.(*os.File)
	if !ok {
		return 0, 0, errors.NewProcessingError("seek is not supported")
	}

	// The end of the file should have the EOF marker at the end (32 bytes)
	// and the txCount uint64 and the utxoCount uint64 (each 8 bytes)
	_, err := f.Seek(-16, io.SeekEnd)
	if err != nil {
		return 0, 0, errors.NewProcessingError("error seeking to EOF marker", err)
	}

	b := make([]byte, 16)
	if _, err := io.ReadFull(f, b); err != nil {
		return 0, 0, errors.NewProcessingError("error reading EOF marker", err)
	}

	txCount := binary.LittleEndian.Uint64(b[0:8])
	utxoCount := binary.LittleEndian.Uint64(b[8:16])

	return txCount, utxoCount, nil
}
