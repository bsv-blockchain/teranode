// Package utxopersister provides functionality for managing UTXO (Unspent Transaction Output) persistence.
package utxopersister

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"strings"

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
	_, err := f.Seek(-48, io.SeekEnd)
	if err != nil {
		return 0, 0, errors.NewProcessingError("error seeking to EOF marker", err)
	}

	b := make([]byte, 48)
	if _, err := io.ReadFull(f, b); err != nil {
		return 0, 0, errors.NewProcessingError("error reading EOF marker", err)
	}

	if !bytes.Equal(b[0:32], EOFMarker) {
		return 0, 0, errors.NewProcessingError("EOF marker not found")
	}

	txCount := binary.LittleEndian.Uint64(b[32:40])
	utxoCount := binary.LittleEndian.Uint64(b[40:48])

	return txCount, utxoCount, nil
}

// PrintFooter prints the footer information with formatted numbers.
// It displays the transaction count and UTXO count with the provided label.
func PrintFooter(r io.Reader, label string) error {
	txCount, utxoCount, err := GetFooter(r)
	if err != nil {
		return err
	}

	fmt.Printf("EOF marker found\n")

	fmt.Printf("record count: %16s\n", formatNumber(txCount))
	fmt.Printf("%s count:   %16s\n", label, formatNumber(utxoCount))

	return nil
}

// formatNumber formats a number with comma separators for better readability.
// For example: 1000000 becomes "1,000,000"
func formatNumber(n uint64) string {
	in := fmt.Sprintf("%d", n)
	out := make([]string, 0, len(in)+(len(in)-1)/3)

	for i, c := range in {
		if i > 0 && (len(in)-i)%3 == 0 {
			out = append(out, ",")
		}

		out = append(out, string(c))
	}

	return strings.Join(out, "")
}
