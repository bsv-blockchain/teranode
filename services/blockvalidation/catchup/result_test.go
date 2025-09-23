package catchup

import (
	"testing"
	"time"

	"github.com/bitcoin-sv/teranode/errors"
	"github.com/bitcoin-sv/teranode/model"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestNewResult tests creating a new Result with basic information
func TestNewResult(t *testing.T) {
	targetHash := &chainhash.Hash{1, 2, 3}
	startHash := &chainhash.Hash{4, 5, 6}
	startHeight := uint32(100)
	peerURL := "peer.example.com:8333"

	result := NewResult(targetHash, startHash, startHeight, peerURL)

	require.NotNil(t, result)
	assert.Equal(t, targetHash, result.TargetHash)
	assert.Equal(t, startHash, result.StartHash)
	assert.Equal(t, startHeight, result.StartHeight)
	assert.Equal(t, peerURL, result.PeerURL)

	// Check that collections are initialized properly
	assert.NotNil(t, result.Headers)
	assert.Len(t, result.Headers, 0)
	assert.NotNil(t, result.FailedIterations)
	assert.Len(t, result.FailedIterations, 0)
}

// TestSetHeaders tests setting headers and updating related metrics
func TestSetHeaders(t *testing.T) {
	result := NewResult(nil, nil, 0, "")

	// Create test headers
	header1 := createResultTestHeader(t, 1, &chainhash.Hash{0})
	header2 := createResultTestHeader(t, 2, header1.Hash())
	headers := []*model.BlockHeader{header1, header2}

	result.SetHeaders(headers)

	assert.Equal(t, headers, result.Headers)
	assert.Equal(t, 2, result.HeadersRetrieved)

	// Headers per second should be 0 when duration is not set
	assert.Equal(t, float64(0), result.HeadersPerSecond)
}

// TestSetHeaders_WithDuration tests SetHeaders when duration is already set
func TestSetHeaders_WithDuration(t *testing.T) {
	result := NewResult(nil, nil, 0, "")
	result.Duration = 2 * time.Second

	header1 := createResultTestHeader(t, 1, &chainhash.Hash{0})
	header2 := createResultTestHeader(t, 2, header1.Hash())
	headers := []*model.BlockHeader{header1, header2}

	result.SetHeaders(headers)

	assert.Equal(t, headers, result.Headers)
	assert.Equal(t, 2, result.HeadersRetrieved)
	assert.Equal(t, float64(1), result.HeadersPerSecond) // 2 headers / 2 seconds = 1 header/sec
}

// TestSetDuration tests setting duration and updating rate metrics
func TestSetDuration(t *testing.T) {
	result := NewResult(nil, nil, 0, "")

	// Set up initial data
	result.HeadersRetrieved = 5
	result.BytesDownloaded = 2 * 1024 * 1024 // 2 MB

	duration := 5 * time.Second
	result.SetDuration(duration)

	assert.Equal(t, duration, result.Duration)
	assert.Equal(t, float64(1), result.HeadersPerSecond) // 5 headers / 5 seconds = 1 header/sec
	assert.Equal(t, 0.4, result.ThroughputMBps)          // 2 MB / 5 seconds = 0.4 MB/s
}

// TestSetDuration_ZeroValues tests SetDuration with zero values
func TestSetDuration_ZeroValues(t *testing.T) {
	result := NewResult(nil, nil, 0, "")

	// Test with zero duration
	result.HeadersRetrieved = 5
	result.SetDuration(0)
	assert.Equal(t, float64(0), result.HeadersPerSecond)

	// Test with zero headers
	result.HeadersRetrieved = 0
	result.SetDuration(time.Second)
	assert.Equal(t, float64(0), result.HeadersPerSecond)
}

// TestAddFailedIteration tests recording failed iterations
func TestAddFailedIteration(t *testing.T) {
	result := NewResult(nil, nil, 0, "")

	testErr := errors.NewError("network timeout")
	peerURL := "failed.peer.com:8333"
	iteration := 3
	retryCount := 2
	duration := 30 * time.Second

	// Record the time before adding the failed iteration
	beforeTime := time.Now()

	result.AddFailedIteration(iteration, testErr, peerURL, retryCount, duration)

	afterTime := time.Now()

	require.Len(t, result.FailedIterations, 1)

	failedIteration := result.FailedIterations[0]
	assert.Equal(t, iteration, failedIteration.Iteration)
	assert.Equal(t, testErr, failedIteration.Error)
	assert.Equal(t, peerURL, failedIteration.PeerURL)
	assert.Equal(t, retryCount, failedIteration.RetryCount)
	assert.Equal(t, duration, failedIteration.Duration)

	// Timestamp should be between beforeTime and afterTime
	assert.True(t, failedIteration.Timestamp.After(beforeTime) || failedIteration.Timestamp.Equal(beforeTime))
	assert.True(t, failedIteration.Timestamp.Before(afterTime) || failedIteration.Timestamp.Equal(afterTime))
}

// TestAddFailedIteration_Multiple tests recording multiple failed iterations
func TestAddFailedIteration_Multiple(t *testing.T) {
	result := NewResult(nil, nil, 0, "")

	// Add first failed iteration
	result.AddFailedIteration(1, errors.NewError("error 1"), "peer1", 1, time.Second)

	// Add second failed iteration
	result.AddFailedIteration(2, errors.NewError("error 2"), "peer2", 2, 2*time.Second)

	assert.Len(t, result.FailedIterations, 2)
	assert.Equal(t, 1, result.FailedIterations[0].Iteration)
	assert.Equal(t, 2, result.FailedIterations[1].Iteration)
}

// TestSetError tests setting error information
func TestSetError(t *testing.T) {
	result := NewResult(nil, nil, 0, "")
	result.Success = true // Initially set to true to test it gets set to false

	testErr := errors.NewError("validation failed")
	context := "header validation failed at height 12345"

	result.SetError(testErr, context)

	assert.Equal(t, testErr, result.Error)
	assert.Equal(t, context, result.ErrorContext)
	assert.False(t, result.Success)
}

// TestMarkSuccess tests marking catchup as successful
func TestMarkSuccess(t *testing.T) {
	// Create a header first, then use its actual hash as the target
	header1 := createResultTestHeader(t, 1, &chainhash.Hash{0})
	targetHash := header1.Hash() // Use the actual computed hash

	result := NewResult(targetHash, nil, 0, "")

	// Test with no headers
	result.MarkSuccess()
	assert.True(t, result.Success)
	assert.Equal(t, 100.0, result.CompletionPercent)
	assert.False(t, result.ReachedTarget) // No headers means didn't reach target

	// Test with headers that don't match target
	header2 := createResultTestHeader(t, 2, &chainhash.Hash{0}) // Different header
	result.Headers = []*model.BlockHeader{header2}
	result.MarkSuccess()
	assert.False(t, result.ReachedTarget) // Header hash doesn't match target

	// Test with headers that match target
	result.Headers = []*model.BlockHeader{header1} // Use the header that matches target
	result.MarkSuccess()
	assert.True(t, result.ReachedTarget) // Last header hash matches target
}

// TestMarkPartialSuccess tests marking catchup as partially successful
func TestMarkPartialSuccess(t *testing.T) {
	result := NewResult(nil, nil, 0, "")

	percentComplete := 75.5
	result.MarkPartialSuccess(percentComplete)

	assert.False(t, result.Success)
	assert.True(t, result.PartialSuccess)
	assert.Equal(t, percentComplete, result.CompletionPercent)
}

// TestIsSuccess tests the success check function
func TestIsSuccess(t *testing.T) {
	result := NewResult(nil, nil, 0, "")

	// Initially not successful
	assert.False(t, result.IsSuccess())

	// Test full success
	result.Success = true
	assert.True(t, result.IsSuccess())

	// Test partial success
	result.Success = false
	result.PartialSuccess = true
	assert.True(t, result.IsSuccess())

	// Test both success flags
	result.Success = true
	result.PartialSuccess = true
	assert.True(t, result.IsSuccess())
}

// TestGetEffectiveHeaders tests getting non-duplicate headers
func TestGetEffectiveHeaders(t *testing.T) {
	result := NewResult(nil, nil, 0, "")

	header1 := createResultTestHeader(t, 1, &chainhash.Hash{0})
	header2 := createResultTestHeader(t, 2, header1.Hash())
	header3 := createResultTestHeader(t, 3, header2.Hash())
	header4 := createResultTestHeader(t, 4, header3.Hash())

	result.Headers = []*model.BlockHeader{header1, header2, header3, header4}

	// Test with no duplicates
	result.DuplicateHeaders = 0
	effective := result.GetEffectiveHeaders()
	assert.Len(t, effective, 4)
	assert.Equal(t, result.Headers, effective)

	// Test with some duplicates
	result.DuplicateHeaders = 2
	effective = result.GetEffectiveHeaders()
	assert.Len(t, effective, 2)
	assert.Equal(t, header3, effective[0])
	assert.Equal(t, header4, effective[1])

	// Test with all headers being duplicates
	result.DuplicateHeaders = 4
	effective = result.GetEffectiveHeaders()
	assert.Len(t, effective, 0)

	// Test with more duplicates than headers (edge case)
	result.DuplicateHeaders = 6
	effective = result.GetEffectiveHeaders()
	assert.Len(t, effective, 0)
}

// TestGetMetrics tests getting metrics map
func TestGetMetrics(t *testing.T) {
	result := NewResult(nil, nil, 0, "peer.example.com")

	// Set up some test data
	result.Success = true
	result.HeadersRetrieved = 10
	result.Duration = 5 * time.Second
	result.HeadersPerSecond = 2.0
	result.ThroughputMBps = 1.5
	result.RetryCount = 3
	result.ValidationErrors = 2
	result.CompletionPercent = 95.0
	result.IterationCount = 4
	result.AddFailedIteration(1, errors.NewError("test"), "peer", 1, time.Second)

	metrics := result.GetMetrics()

	require.NotNil(t, metrics)
	assert.Equal(t, true, metrics["success"])
	assert.Equal(t, 10, metrics["headers_retrieved"])
	assert.Equal(t, int64(5000), metrics["duration_ms"]) // 5 seconds = 5000 ms
	assert.Equal(t, 2.0, metrics["headers_per_second"])
	assert.Equal(t, 1.5, metrics["throughput_mbps"])
	assert.Equal(t, 3, metrics["retry_count"])
	assert.Equal(t, 2, metrics["validation_errors"])
	assert.Equal(t, 95.0, metrics["completion_percent"])
	assert.Equal(t, "peer.example.com", metrics["peer_url"])
	assert.Equal(t, 4, metrics["iteration_count"])
	assert.Equal(t, 1, metrics["failed_iterations"])
}

// TestGetHeaderCount tests getting header count
func TestGetHeaderCount(t *testing.T) {
	result := NewResult(nil, nil, 0, "")

	// Empty headers
	assert.Equal(t, 0, result.GetHeaderCount())

	// Add some headers
	header1 := createResultTestHeader(t, 1, &chainhash.Hash{0})
	header2 := createResultTestHeader(t, 2, header1.Hash())
	result.Headers = []*model.BlockHeader{header1, header2}

	assert.Equal(t, 2, result.GetHeaderCount())
}

// TestHasErrors tests checking for errors
func TestHasErrors(t *testing.T) {
	result := NewResult(nil, nil, 0, "")

	// No errors initially
	assert.False(t, result.HasErrors())

	// Add a failed iteration
	result.AddFailedIteration(1, errors.NewError("test error"), "peer", 0, time.Second)
	assert.True(t, result.HasErrors())
}

// TestResultIntegration tests the interaction between multiple methods
func TestResultIntegration(t *testing.T) {
	startHash := &chainhash.Hash{1, 1, 1}
	startHeight := uint32(50)
	peerURL := "integration.test.com:8333"

	// Create headers first, then use the last one's hash as target
	header1 := createResultTestHeader(t, 1, startHash)
	header2 := createResultTestHeader(t, 2, header1.Hash())
	finalHeader := createResultTestHeader(t, 3, header2.Hash())
	targetHash := finalHeader.Hash() // Use actual computed hash as target

	// Create result
	result := NewResult(targetHash, startHash, startHeight, peerURL)

	// Simulate a catchup operation
	headers := []*model.BlockHeader{header1, header2, finalHeader}

	result.SetHeaders(headers)
	result.BytesDownloaded = 6 * 1024 * 1024 // 6 MB
	result.SetDuration(3 * time.Second)      // Must call after setting BytesDownloaded
	result.ValidationErrors = 1
	result.RetryCount = 2
	result.DuplicateHeaders = 1

	// Add a failed iteration
	result.AddFailedIteration(1, errors.NewError("temporary network error"), peerURL, 1, time.Second)

	// Mark as successful
	result.MarkSuccess()

	// Verify all the computed values
	assert.True(t, result.Success)
	assert.True(t, result.ReachedTarget)
	assert.Equal(t, 100.0, result.CompletionPercent)
	assert.Equal(t, 3, result.HeadersRetrieved)
	assert.Equal(t, float64(1), result.HeadersPerSecond) // 3 headers / 3 seconds
	assert.Equal(t, float64(2), result.ThroughputMBps)   // 6 MB / 3 seconds
	assert.True(t, result.IsSuccess())
	assert.True(t, result.HasErrors()) // Has failed iterations

	// Check effective headers (excluding 1 duplicate)
	effective := result.GetEffectiveHeaders()
	assert.Len(t, effective, 2) // 3 total - 1 duplicate

	// Check metrics
	metrics := result.GetMetrics()
	assert.Equal(t, true, metrics["success"])
	assert.Equal(t, 3, metrics["headers_retrieved"])
	assert.Equal(t, int64(3000), metrics["duration_ms"])
	assert.Equal(t, 1, metrics["failed_iterations"])
}

// TestIterationError tests the IterationError struct behavior
func TestIterationError(t *testing.T) {
	iteration := 5
	err := errors.NewError("connection timeout")
	peerURL := "slow.peer.com:8333"
	retryCount := 3
	duration := 45 * time.Second

	// Create through AddFailedIteration
	result := NewResult(nil, nil, 0, "")
	result.AddFailedIteration(iteration, err, peerURL, retryCount, duration)

	require.Len(t, result.FailedIterations, 1)
	iterErr := result.FailedIterations[0]

	assert.Equal(t, iteration, iterErr.Iteration)
	assert.Equal(t, err, iterErr.Error)
	assert.Equal(t, peerURL, iterErr.PeerURL)
	assert.Equal(t, retryCount, iterErr.RetryCount)
	assert.Equal(t, duration, iterErr.Duration)
	assert.False(t, iterErr.Timestamp.IsZero())
}

// TestEdgeCases tests various edge cases and boundary conditions
func TestEdgeCases(t *testing.T) {
	t.Run("NilHashes", func(t *testing.T) {
		result := NewResult(nil, nil, 0, "")
		result.MarkSuccess()
		assert.False(t, result.ReachedTarget) // Can't reach nil target
	})

	t.Run("EmptyPeerURL", func(t *testing.T) {
		result := NewResult(nil, nil, 0, "")
		assert.Equal(t, "", result.PeerURL)

		metrics := result.GetMetrics()
		assert.Equal(t, "", metrics["peer_url"])
	})

	t.Run("ZeroDuration", func(t *testing.T) {
		result := NewResult(nil, nil, 0, "")
		result.HeadersRetrieved = 10
		result.BytesDownloaded = 1024
		result.SetDuration(0)

		assert.Equal(t, float64(0), result.HeadersPerSecond)
		assert.Equal(t, float64(0), result.ThroughputMBps)
	})

	t.Run("LargeNumbers", func(t *testing.T) {
		result := NewResult(nil, nil, 0, "")
		result.BytesDownloaded = 1024 * 1024 * 1024 // 1 GB
		result.SetDuration(time.Second)

		assert.Equal(t, float64(1024), result.ThroughputMBps) // 1 GB/s = 1024 MB/s
	})
}

// Helper function to create test headers for result tests
func createResultTestHeader(_ *testing.T, nonce uint32, prevHash *chainhash.Hash) *model.BlockHeader {
	return &model.BlockHeader{
		Version:        1,
		HashPrevBlock:  prevHash,
		HashMerkleRoot: &chainhash.Hash{byte(nonce)}, // Use nonce to make unique
		Timestamp:      1000 + nonce,
		Bits:           model.NBit{},
		Nonce:          nonce,
	}
}
