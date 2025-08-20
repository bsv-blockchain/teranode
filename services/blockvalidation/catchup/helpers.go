// Package blockvalidation provides helper functions for block catchup operations.
// These helpers handle low-level details like byte validation, header parsing,
// and network request retry logic.
package catchup

import (
	"context"
	"os"
	"strings"
	"time"

	"github.com/bitcoin-sv/teranode/errors"
	"github.com/bitcoin-sv/teranode/model"
	"github.com/bitcoin-sv/teranode/ulogger"
	"github.com/bitcoin-sv/teranode/util"
	"github.com/bitcoin-sv/teranode/util/retry"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
)

// ValidateBlockHeaderBytes validates that the byte array can be properly parsed as block headers.
//
// Parameters:
//   - headerBytes: Raw bytes to validate
//
// Returns:
//   - error: If bytes are invalid (wrong length or not divisible by header size)
//
// Ensures the byte array length is a multiple of BlockHeaderSize (80 bytes).
func ValidateBlockHeaderBytes(headerBytes []byte) error {
	if len(headerBytes) == 0 {
		return nil // Empty response is valid
	}

	if len(headerBytes)%model.BlockHeaderSize != 0 {
		return errors.NewNetworkInvalidResponseError("invalid header bytes length %d, must be divisible by %d", len(headerBytes), model.BlockHeaderSize)
	}

	return nil
}

// ParseBlockHeaders parses a byte array into block headers with validation.
//
// Parameters:
//   - headerBytes: Raw header bytes to parse
//
// Returns:
//   - []*model.BlockHeader: Successfully parsed headers (nil if error)
//   - error: First error encountered during parsing/validation
//
// Returns immediately on first error to simplify debugging.
// Validates proof of work, merkle root, and timestamp for each header.
func ParseBlockHeaders(headerBytes []byte) ([]*model.BlockHeader, error) {
	if len(headerBytes) == 0 {
		return nil, nil
	}

	// Validate that the byte array length is a multiple of BlockHeaderSize
	if len(headerBytes)%model.BlockHeaderSize != 0 {
		return nil, errors.NewNetworkInvalidResponseError("invalid header bytes length %d, must be divisible by %d", len(headerBytes), model.BlockHeaderSize)
	}

	numHeaders := len(headerBytes) / model.BlockHeaderSize
	headers := make([]*model.BlockHeader, 0, numHeaders)

	for i := 0; i < len(headerBytes); i += model.BlockHeaderSize {
		headerData := headerBytes[i : i+model.BlockHeaderSize]
		header, err := model.NewBlockHeaderFromBytes(headerData)
		if err != nil {
			// Return immediately on first parse error
			return nil, errors.NewNetworkInvalidResponseError("failed to parse header at offset %d: %w", i, err)
		}

		// Perform basic header validation
		if err = ValidateHeaderProofOfWork(header); err != nil {
			// Return immediately on first validation error
			return nil, err
		}

		if err = ValidateHeaderMerkleRoot(header); err != nil {
			// Return immediately on first validation error
			return nil, err
		}

		if err = ValidateHeaderTimestamp(header); err != nil {
			// Return immediately on first validation error
			return nil, err
		}

		headers = append(headers, header)
	}

	return headers, nil
}

// BuildBlockLocatorString creates a concatenated string of block locator hashes.
//
// Parameters:
//   - locatorHashes: Block locator hashes
//
// Returns:
//   - string: Concatenated hash strings for URL construction
//
// Pre-allocates string builder capacity for efficiency with many hashes.
func BuildBlockLocatorString(locatorHashes []*chainhash.Hash) string {
	if len(locatorHashes) == 0 {
		return ""
	}

	// Pre-calculate capacity: each hash is 64 characters
	var builder strings.Builder
	builder.Grow(len(locatorHashes) * 64)

	for _, locatorHash := range locatorHashes {
		builder.WriteString(locatorHash.String())
	}

	return builder.String()
}

// FetchHeadersWithRetry fetches block headers with exponential backoff retry.
//
// Parameters:
//   - ctx: Context for cancellation
//   - logger: Logger for retry attempts
//   - url: URL to fetch headers from
//   - maxRetries: Maximum retry attempts
//
// Returns:
//   - []byte: Raw header bytes
//   - error: If all retries exhausted
//
// Uses exponential backoff with 2x factor, max 30s between retries.
// Categorizes errors (timeout, connection refused, generic network) for proper handling.
func FetchHeadersWithRetry(ctx context.Context, logger ulogger.Logger, url string, maxRetries int) ([]byte, error) {
	return retry.Retry(ctx, logger, func() ([]byte, error) {
		headerBytes, err := util.DoHTTPRequest(ctx, url)
		if err != nil {
			// Categorize the error for better handling

			// Check for network timeout errors
			if errors.Is(err, os.ErrDeadlineExceeded) {
				return nil, errors.NewNetworkTimeoutError("request timed out: %w", err)
			}

			// Check for connection refused errors
			if strings.Contains(err.Error(), "connection refused") {
				return nil, errors.NewNetworkConnectionRefusedError("peer unavailable: %w", err)
			}

			if errors.IsNetworkError(err) {
				return nil, errors.NewNetworkError("network error: %w", err)
			}

			return nil, err
		}
		return headerBytes, nil
	},
		retry.WithRetryCount(maxRetries),
		retry.WithExponentialBackoff(),
		retry.WithBackoffFactor(2.0),
		retry.WithMaxBackoff(30*time.Second),
		retry.WithMessage("Failed to fetch block headers"),
	)
}

// CheckContextCancellation checks if the context has been cancelled.
//
// Parameters:
//   - ctx: Context to check
//
// Returns:
//   - error: Context cancellation error if cancelled, nil otherwise
//
// Non-blocking check using select with default case.
func CheckContextCancellation(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return errors.NewContextCanceledError("operation cancelled: %w", ctx.Err())
	default:
		return nil
	}
}

// CreateCatchupResult creates a CatchupResult from the current state.
//
// Parameters:
//   - headers: Retrieved headers
//   - targetHash: Target block hash
//   - startHash: Starting block hash
//   - startHeight: Starting block height
//   - startTime: Operation start time
//   - peerURL: Peer URL
//   - iterations: Number of request iterations
//   - failedIterations: Failed iteration details
//   - reachedTarget: Whether target was reached
//   - stopReason: Reason for stopping
//
// Returns:
//   - *CatchupResult: Complete result with metrics and status
//
// Delegates to createCatchupResultWithLocator with nil locator for compatibility.
func CreateCatchupResult(
	headers []*model.BlockHeader,
	targetHash, startHash *chainhash.Hash,
	startHeight uint32,
	startTime time.Time,
	peerURL string,
	iterations int,
	failedIterations []IterationError,
	reachedTarget bool,
	stopReason string,
) *Result {
	return CreateCatchupResultWithLocator(headers, targetHash, startHash, startHeight, startTime, peerURL, iterations, failedIterations, reachedTarget, stopReason, nil)
}

// CreateCatchupResultWithLocator creates a CatchupResult with locator hashes.
//
// Parameters:
//   - headers: Retrieved headers
//   - targetHash: Target block hash
//   - startHash: Starting block hash
//   - startHeight: Starting block height
//   - startTime: Operation start time
//   - peerURL: Peer URL
//   - iterations: Number of request iterations
//   - failedIterations: Failed iteration details
//   - reachedTarget: Whether target was reached
//   - stopReason: Reason for stopping
//   - locatorHashes: Block locator used for efficient search
//
// Returns:
//   - *CatchupResult: Complete result with metrics, status, and locator
//
// Calculates duration, sets partial success flags, and includes backward compatibility fields.
func CreateCatchupResultWithLocator(
	headers []*model.BlockHeader,
	targetHash, startHash *chainhash.Hash,
	startHeight uint32,
	startTime time.Time,
	peerURL string,
	iterations int,
	failedIterations []IterationError,
	reachedTarget bool,
	stopReason string,
	locatorHashes []*chainhash.Hash,
) *Result {
	endTime := time.Now()
	result := &Result{
		Headers:              headers,
		PartialSuccess:       len(headers) > 0 && !reachedTarget,
		TargetHash:           targetHash,
		StartHash:            startHash,
		StartHeight:          startHeight,
		TotalIterations:      iterations,
		TotalHeadersReceived: len(headers),
		FailedIterations:     failedIterations,
		StartTime:            startTime,
		EndTime:              endTime,
		Duration:             endTime.Sub(startTime),
		PeerURL:              peerURL,
		ReachedTarget:        reachedTarget,
		StoppedEarly:         !reachedTarget && stopReason != "",
		StopReason:           stopReason,
		LocatorHashes:        locatorHashes,
	}

	// Set last processed hash if we have headers
	if len(headers) > 0 {
		result.LastProcessedHash = headers[len(headers)-1].Hash()
	}

	return result
}
