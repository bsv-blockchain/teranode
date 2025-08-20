// Package catchup provides structured types for block catchup operations.
// These types capture comprehensive metrics and state for monitoring and debugging.
package catchup

import (
	"time"

	"github.com/bitcoin-sv/teranode/model"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
)

// Result captures comprehensive metrics and results from a catchup operation.
// This structure provides detailed information for monitoring, debugging, and analysis.
type Result struct {
	// Headers retrieved during catchup, ordered from oldest to newest
	Headers []*model.BlockHeader

	// Operation metadata
	TargetHash        *chainhash.Hash   // Hash of the target block we're catching up to
	StartHash         *chainhash.Hash   // Hash of the block we started from
	StartHeight       uint32            // Height of the starting block
	LastProcessedHash *chainhash.Hash   // Last successfully processed block
	LocatorHashes     []*chainhash.Hash // Block locator hashes used

	// Performance metrics
	Duration              time.Duration    // Total time taken for the catchup operation
	HeadersRetrieved      int              // Number of headers successfully retrieved
	HeadersPerSecond      float64          // Average retrieval rate
	BytesDownloaded       int64            // Total bytes downloaded
	ThroughputMBps        float64          // Average throughput in MB/s
	IterationCount        int              // Number of iterations performed
	TotalIterations       int              // Number of iterations performed
	TotalHeaders          uint64           // Total headers retrieved
	TotalHeadersReceived  int              // Total headers received
	TotalHeadersRequested int              // Maximum headers requested
	FailedIterations      []IterationError // Details of any failed iterations

	// Network metrics
	PeerURL             string        // URL of the peer used for catchup
	NetworkLatency      time.Duration // Average network latency
	RetryCount          int           // Number of retries performed
	CircuitBreakerTrips int           // Number of times circuit breaker was triggered

	// Validation metrics
	ValidationErrors   int           // Number of validation errors encountered
	InvalidHeaders     int           // Number of invalid headers rejected
	DuplicateHeaders   int           // Number of duplicate headers skipped
	ValidationDuration time.Duration // Time spent on header validation

	// Resource metrics
	MemoryUsedMB      float64 // Peak memory usage during operation
	GoroutinesCreated int     // Number of goroutines created
	DatabaseQueries   int     // Number of database queries performed

	// Success indicators
	Success           bool    // Whether the catchup completed successfully
	PartialSuccess    bool    // Whether some headers were retrieved despite failure
	ReachedTarget     bool    // Whether we reached the target block
	CompletionPercent float64 // Percentage of catchup completed (0-100)
	StoppedEarly      bool    // Catchup stopped before reaching target
	StopReason        string  // Reason for early stop

	// Timing
	StartTime time.Time // When catchup started
	EndTime   time.Time // When catchup ended

	// Error information
	Error        error  // Primary error if operation failed
	ErrorContext string // Additional context about the error
}

// IterationError captures details about a failed catchup iteration
type IterationError struct {
	Iteration  int           // Iteration number (1-based)
	Error      error         // Error that occurred
	Timestamp  time.Time     // When the error occurred
	PeerURL    string        // Peer that caused the error
	RetryCount int           // Number of retries for this iteration
	Duration   time.Duration // How long this iteration took
}

// NewResult creates a new catchup result with basic information
func NewResult(targetHash, startHash *chainhash.Hash, startHeight uint32, peerURL string) *Result {
	return &Result{
		TargetHash:       targetHash,
		StartHash:        startHash,
		StartHeight:      startHeight,
		PeerURL:          peerURL,
		Headers:          make([]*model.BlockHeader, 0),
		FailedIterations: make([]IterationError, 0),
	}
}

// SetHeaders sets the retrieved headers and updates related metrics
func (r *Result) SetHeaders(headers []*model.BlockHeader) {
	r.Headers = headers
	r.HeadersRetrieved = len(headers)
	if r.Duration > 0 {
		r.HeadersPerSecond = float64(len(headers)) / r.Duration.Seconds()
	}
}

// SetDuration sets the operation duration and updates rate metrics
func (r *Result) SetDuration(duration time.Duration) {
	r.Duration = duration
	if r.HeadersRetrieved > 0 && duration > 0 {
		r.HeadersPerSecond = float64(r.HeadersRetrieved) / duration.Seconds()
	}
	if r.BytesDownloaded > 0 && duration > 0 {
		r.ThroughputMBps = float64(r.BytesDownloaded) / (1024 * 1024) / duration.Seconds()
	}
}

// AddFailedIteration records a failed iteration
func (r *Result) AddFailedIteration(iteration int, err error, peerURL string, retryCount int, duration time.Duration) {
	r.FailedIterations = append(r.FailedIterations, IterationError{
		Iteration:  iteration,
		Error:      err,
		Timestamp:  time.Now(),
		PeerURL:    peerURL,
		RetryCount: retryCount,
		Duration:   duration,
	})
}

// SetError sets the error information for a failed catchup
func (r *Result) SetError(err error, context string) {
	r.Error = err
	r.ErrorContext = context
	r.Success = false
}

// MarkSuccess marks the catchup as successful
func (r *Result) MarkSuccess() {
	r.Success = true
	r.CompletionPercent = 100.0
	if len(r.Headers) > 0 {
		lastHeader := r.Headers[len(r.Headers)-1]
		r.ReachedTarget = lastHeader.Hash().IsEqual(r.TargetHash)
	}
}

// MarkPartialSuccess marks the catchup as partially successful
func (r *Result) MarkPartialSuccess(percentComplete float64) {
	r.Success = false
	r.PartialSuccess = true
	r.CompletionPercent = percentComplete
}

// IsSuccess returns true if the catchup was fully or partially successful
func (r *Result) IsSuccess() bool {
	return r.Success || r.PartialSuccess
}

// GetEffectiveHeaders returns headers that were actually new (not duplicates)
func (r *Result) GetEffectiveHeaders() []*model.BlockHeader {
	if r.DuplicateHeaders == 0 {
		return r.Headers
	}

	effectiveCount := len(r.Headers) - r.DuplicateHeaders
	if effectiveCount <= 0 {
		return []*model.BlockHeader{}
	}

	// Return the last N headers as they're most likely to be new
	return r.Headers[r.DuplicateHeaders:]
}

// GetMetrics returns a map of key metrics for monitoring
func (r *Result) GetMetrics() map[string]interface{} {
	return map[string]interface{}{
		"success":            r.Success,
		"headers_retrieved":  r.HeadersRetrieved,
		"duration_ms":        r.Duration.Milliseconds(),
		"headers_per_second": r.HeadersPerSecond,
		"throughput_mbps":    r.ThroughputMBps,
		"retry_count":        r.RetryCount,
		"validation_errors":  r.ValidationErrors,
		"completion_percent": r.CompletionPercent,
		"peer_url":           r.PeerURL,
		"iteration_count":    r.IterationCount,
		"failed_iterations":  len(r.FailedIterations),
	}
}

// GetHeaderCount returns the number of headers retrieved
func (r *Result) GetHeaderCount() int {
	return len(r.Headers)
}

// HasErrors returns true if there were any errors during catchup
func (r *Result) HasErrors() bool {
	return len(r.FailedIterations) > 0
}
