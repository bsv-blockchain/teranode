// Package utxovalidator provides functionality to validate UTXO sets against expected Bitcoin supply.
// It reads UTXO files, extracts the block height and total satoshi values, then compares against
// the expected Bitcoin supply at that height according to the halving schedule.
package utxovalidator

import (
	"bufio"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-chaincfg"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/services/utxopersister"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/stores/blob"
	"github.com/bsv-blockchain/teranode/stores/blob/options"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util"
)

// UTXOValidationResult contains the results of UTXO set validation.
type UTXOValidationResult struct {
	BlockHeight      uint32
	BlockHash        chainhash.Hash
	PreviousHash     chainhash.Hash
	ActualSatoshis   uint64
	ExpectedSatoshis uint64
	IsValid          bool
	UTXOCount        int
}

// ValidateUTXOFile validates a UTXO set file against the expected Bitcoin supply.
// It reads the UTXO file, extracts the block height, sums all UTXO values,
// and compares against the expected supply for that height.
//
// Parameters:
//   - ctx: Context for cancellation and timeouts
//   - path: Path to the UTXO file
//   - logger: Logger for output and errors
//   - settings: Application settings
//   - verbose: Whether to print detailed information
//
// Returns:
//   - *UTXOValidationResult: Validation results including actual vs expected satoshis
//   - error: Any error encountered during validation
func ValidateUTXOFile(ctx context.Context, path string, logger ulogger.Logger, settings *settings.Settings, verbose bool) (*UTXOValidationResult, error) {
	// Get reader for the UTXO file
	r, err := getUTXOFileReader(path, logger, settings)
	if err != nil {
		return nil, errors.NewProcessingError("error getting UTXO file reader", err)
	}

	defer func() {
		if err := r.Close(); err != nil {
			logger.Errorf("error closing UTXO file reader: %v", err)
		}
	}()

	// Read and validate the UTXO set
	return validateUTXOSet(ctx, r, verbose)
}

// validateUTXOSet reads a UTXO set from the reader and validates the total satoshi amount.
func validateUTXOSet(ctx context.Context, r io.Reader, verbose bool) (*UTXOValidationResult, error) {
	result := &UTXOValidationResult{}

	br := bufio.NewReaderSize(r, 64*1024) // 64KB buffer - sufficient for CLI tool validation

	// Read file header to verify this is a UTXO set file
	header, err := fileformat.ReadHeader(br)
	if err != nil {
		return nil, errors.NewProcessingError("error reading file header", err)
	}

	if header.FileType() != fileformat.FileTypeUtxoSet {
		return nil, errors.NewProcessingError("file is not a UTXO set file, got: %s", header.FileType())
	}

	// Read current block hash (32 bytes) - this matches what filereader calls "previous block hash"
	// but it's actually the current block hash based on the filereader output
	blockHashBytes := make([]byte, 32)
	if _, err := io.ReadFull(br, blockHashBytes); err != nil {
		return nil, errors.NewProcessingError("error reading current block hash", err)
	}

	blockHash, err := chainhash.NewHash(blockHashBytes)
	if err != nil {
		return nil, errors.NewProcessingError("error parsing current block hash", err)
	}
	result.BlockHash = *blockHash

	// Read block height (4 bytes)
	if err = binary.Read(br, binary.LittleEndian, &result.BlockHeight); err != nil {
		return nil, errors.NewProcessingError("error reading block height", err)
	}

	// Read previous block hash (32 bytes)
	previousHashBytes := make([]byte, 32)
	if _, err := io.ReadFull(br, previousHashBytes); err != nil {
		return nil, errors.NewProcessingError("error reading previous block hash", err)
	}

	previousHash, err := chainhash.NewHash(previousHashBytes)
	if err != nil {
		return nil, errors.NewProcessingError("error parsing previous block hash", err)
	}
	result.PreviousHash = *previousHash

	// Sum all UTXO values
	var (
		processedWrappers int
		utxoWrapper       = &utxopersister.UTXOWrapper{}
	)

	for {
		if err = utxoWrapper.FromReader(ctx, br, false); err != nil {
			if err == io.EOF {
				break // End of file reached
			}
			// Handle unexpected EOF more gracefully - this can happen at the end of large files
			errStr := err.Error()
			if strings.Contains(errStr, "failed to read txid") || strings.Contains(errStr, "unexpected EOF") {
				fmt.Printf("Reached end of file after processing %d transactions\n", processedWrappers)
				break
			}

			return nil, errors.NewProcessingError("error reading UTXO wrapper", err)
		}

		result.ActualSatoshis += utxoWrapper.UTXOTotalValue
		result.UTXOCount += utxoWrapper.UTXOCount

		processedWrappers++

		// Show progress every 100,000 transactions
		if processedWrappers%100000 == 0 {
			fmt.Printf("Processed %d transactions, %d UTXOs, %s satoshis so far...\n",
				processedWrappers, result.UTXOCount, formatSatoshis(result.ActualSatoshis))
		}

		if verbose {
			fmt.Printf("UTXO Tx: %s (height %d, coinbase: %t) - %d outputs\n",
				utxoWrapper.TxID.String(), utxoWrapper.Height, utxoWrapper.Coinbase, len(utxoWrapper.UTXOs))
		}
	}

	// Calculate expected satoshis at this height
	result.ExpectedSatoshis = util.CalculateExpectedSupplyAtHeight(result.BlockHeight, &chaincfg.MainNetParams)

	// Determine if validation passed
	result.IsValid = result.ActualSatoshis == result.ExpectedSatoshis

	return result, nil
}

// getUTXOFileReader returns a reader for the UTXO file, handling both local files and blob store.
func getUTXOFileReader(path string, logger ulogger.Logger, settings *settings.Settings) (io.ReadCloser, error) {
	// For simplicity, this implementation assumes local file access
	// The original filereader code handles both local files and blob store via useStore flag
	// We can extend this later if needed for blob store access

	// Try to determine if this is a hash (for blob store) or file path
	if len(path) == 64 {
		// Looks like a hash, try blob store
		hash, err := chainhash.NewHashFromStr(path)
		if err == nil {
			return getBlobStoreReader(hash[:], logger, settings)
		}
	}

	// Fallback to local file
	return getLocalFileReader(path)
}

// getLocalFileReader returns a reader for a local file.
func getLocalFileReader(path string) (io.ReadCloser, error) {
	fileReader, err := os.Open(path)
	if err != nil {
		return nil, errors.NewProcessingError("error opening file", err)
	}

	return fileReader, nil
}

// getBlobStoreReader returns a reader for a file in the blob store.
func getBlobStoreReader(hash []byte, logger ulogger.Logger, settings *settings.Settings) (io.ReadCloser, error) {
	blockStoreURL := settings.Block.BlockStore
	if blockStoreURL == nil {
		return nil, errors.NewProcessingError("blockstore config not found")
	}

	var err error

	hashPrefix := -2

	if blockStoreURL.Query().Get("hashPrefix") != "" {
		hashPrefix, err = strconv.Atoi(blockStoreURL.Query().Get("hashPrefix"))
		if err != nil {
			return nil, errors.NewProcessingError("error parsing hashPrefix", err)
		}
	}

	blockStore, err := blob.NewStore(logger, blockStoreURL, options.WithHashPrefix(hashPrefix))
	if err != nil {
		return nil, errors.NewProcessingError("error creating blob store", err)
	}

	reader, err := blockStore.GetIoReader(context.Background(), hash, fileformat.FileTypeUtxoSet)
	if err != nil {
		return nil, errors.NewProcessingError("error getting reader from blob store", err)
	}

	return reader, nil
}

// formatSatoshis formats a satoshi amount with thousand separators for better readability.
func formatSatoshis(satoshis uint64) string {
	str := fmt.Sprintf("%d", satoshis)

	// Add thousand separators
	n := len(str)
	if n <= 3 {
		return str
	}

	// Calculate how many commas we need
	commas := (n - 1) / 3
	result := make([]byte, n+commas)

	// Fill from right to left
	resultPos := len(result) - 1
	strPos := n - 1
	digitCount := 0

	for strPos >= 0 {
		if digitCount == 3 {
			result[resultPos] = ','
			resultPos--
			digitCount = 0
		}

		result[resultPos] = str[strPos]
		resultPos--
		strPos--
		digitCount++
	}

	return string(result)
}
