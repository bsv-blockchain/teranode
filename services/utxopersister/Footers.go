// Package utxopersister provides functionality for managing UTXO (Unspent Transaction Output) persistence.
package utxopersister

import (
	"encoding/binary"
	"io"
	"os"

	"github.com/bitcoin-sv/teranode/errors"
)

// GetFooter retrieves the transaction and UTXO counts from the footer of a file.
// It returns the transaction count, UTXO count, and any error encountered.
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
