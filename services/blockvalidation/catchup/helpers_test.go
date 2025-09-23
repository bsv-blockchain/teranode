package catchup

import (
	"context"
	"encoding/hex"
	"fmt"
	"testing"
	"time"

	"github.com/bitcoin-sv/teranode/errors"
	"github.com/bitcoin-sv/teranode/model"
	"github.com/bitcoin-sv/teranode/ulogger"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseBlockHeaders(t *testing.T) {
	type args struct {
		headerHex string
	}

	hashPrevBlock, err := chainhash.NewHashFromStr("00000000000000000a10035612d303b96fa7528b30c7e8d81df76f4095f627e5")
	assert.NoError(t, err)
	hashMerkleRoot, err := chainhash.NewHashFromStr("d093e016213aceed8c5ebd46bfae2a40158d725c38b559bfffa8fcfddae9a8cd")
	assert.NoError(t, err)
	bits, err := hex.DecodeString("ebc22118")
	assert.NoError(t, err)

	// Example valid header
	var validSingleHeader = &model.BlockHeader{
		Version:        973078528,
		HashPrevBlock:  hashPrevBlock,
		HashMerkleRoot: hashMerkleRoot,
		Timestamp:      1756169335,
		Bits:           model.NBit(bits),
		Nonce:          600846451,
	}

	tests := []struct {
		name    string
		args    args
		want    []*model.BlockHeader
		wantErr assert.ErrorAssertionFunc
	}{
		{
			name: "Valid single header",
			args: args{
				headerHex: "0000003a" + // Version
					"e527f695406ff71dd8e8c7308b52a76fb903d3125603100a0000000000000000" + // Previous block
					"cda8e9dafdfca8ffbf59b5385c728d15402aaebf46bd5e8cedce3a2116e093d0" + // Merkle root
					"7704ad68" + // Timestamp
					"ebc22118" + // Bits
					"7330d023", // Nonce

			},
			want: []*model.BlockHeader{
				validSingleHeader,
			},
			wantErr: assert.NoError,
		},
		{
			name: "Valid multiple headers",
			args: args{
				headerHex: "0000003ae527f695406ff71dd8e8c7308b52a76fb903d3125603100a0000000000000000cda8e9dafdfca8ffbf59b5385c728d15402aaebf46bd5e8cedce3a2116e093d07704ad68ebc221187330d0230000003ae527f695406ff71dd8e8c7308b52a76fb903d3125603100a0000000000000000cda8e9dafdfca8ffbf59b5385c728d15402aaebf46bd5e8cedce3a2116e093d07704ad68ebc221187330d023",
			},
			want: []*model.BlockHeader{
				validSingleHeader,
				validSingleHeader,
			},
			wantErr: assert.NoError,
		},
		{
			name: "invalid multiple header length",
			args: args{
				headerHex: "0000003ae7f695406ff71dd8e8c7308b52a76fb903d3125603100a0000000000000000cda8e9dafdfca8ffbf59b5385c728d15402aaebf46bd5e8cedce3a2116e093d07704ad68ebc221187330d0230000003ae527f695406ff71dd8e8c7308b52a76fb903d3125603100a0000000000000000cda8e9dafdfca8ffbf59b5385c728d15402aaebf46bd5e8cedce3a2116e093d07704ad68ebc221187330d023",
			},
			want:    nil,
			wantErr: assert.Error,
		},
		{
			name: "invalid single header length",
			args: args{
				headerHex: "00000033edce3a2116e093d07704ad68ebc221187330d023",
			},
			want:    nil,
			wantErr: assert.Error,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			headerBytes, err := hex.DecodeString(tt.args.headerHex)
			if err != nil {
				t.Fatalf("Failed to decode header hex: %v", err)
			}
			got, err := ParseBlockHeaders(headerBytes)
			if !tt.wantErr(t, err, fmt.Sprintf("ParseBlockHeaders(%x)", tt.args.headerHex)) {
				return
			}
			assert.Equalf(t, tt.want, got, "ParseBlockHeaders(%x)", tt.args.headerHex)
		})
	}
}

// Helper function to create test headers
func createHelpersTestHeader(t *testing.T, nonce uint32, prevHash *chainhash.Hash) *model.BlockHeader {
	t.Helper()

	// Create a valid previous hash if not provided
	if prevHash == nil {
		prevHash = &chainhash.Hash{}
	}

	// Create a valid merkle root hash
	merkleRoot := &chainhash.Hash{}

	header := &model.BlockHeader{
		Version:        1,
		HashPrevBlock:  prevHash,
		HashMerkleRoot: merkleRoot,
		Timestamp:      uint32(time.Now().Unix()),
		Bits:           model.NBit([]byte{0x20, 0x7f, 0xff, 0xff}),
		Nonce:          nonce,
	}
	return header
}

func TestValidateBlockHeaderBytes(t *testing.T) {
	tests := []struct {
		name        string
		headerBytes []byte
		wantErr     bool
		errorMsg    string
	}{
		{
			name:        "Empty bytes - valid",
			headerBytes: []byte{},
			wantErr:     false,
		},
		{
			name:        "Valid single header size",
			headerBytes: make([]byte, 80), // BlockHeaderSize
			wantErr:     false,
		},
		{
			name:        "Valid double header size",
			headerBytes: make([]byte, 160), // 2 * BlockHeaderSize
			wantErr:     false,
		},
		{
			name:        "Invalid size - not divisible by 80",
			headerBytes: make([]byte, 79),
			wantErr:     true,
			errorMsg:    "invalid header bytes length 79, must be divisible by 80",
		},
		{
			name:        "Invalid size - 81 bytes",
			headerBytes: make([]byte, 81),
			wantErr:     true,
			errorMsg:    "invalid header bytes length 81, must be divisible by 80",
		},
		{
			name:        "Invalid size - 159 bytes",
			headerBytes: make([]byte, 159),
			wantErr:     true,
			errorMsg:    "invalid header bytes length 159, must be divisible by 80",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateBlockHeaderBytes(tt.headerBytes)
			if tt.wantErr {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.errorMsg)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestParseBlockHeaders_AdditionalCoverage(t *testing.T) {
	// Test empty bytes - already covered by existing test but adding for completeness
	headers, err := ParseBlockHeaders([]byte{})
	require.NoError(t, err)
	assert.Nil(t, headers)

	// Test invalid header that will fail validation (not parsing)
	// Create a valid-looking header structure but with invalid proof of work
	invalidHeaderBytes := make([]byte, 80)
	// Set version to 1 (little endian)
	invalidHeaderBytes[0] = 0x01
	invalidHeaderBytes[1] = 0x00
	invalidHeaderBytes[2] = 0x00
	invalidHeaderBytes[3] = 0x00
	// Leave the rest as zeros, which will fail proof of work validation

	headers, err = ParseBlockHeaders(invalidHeaderBytes)
	require.Error(t, err)
	// The error should be related to validation, not parsing
	assert.Contains(t, err.Error(), "block header fails proof of work")
	assert.Nil(t, headers)

	// Test header with truly invalid structure that would fail parsing
	// Unfortunately, model.NewBlockHeaderFromBytes is quite robust,
	// so we test with the validation errors instead
}

func TestBuildBlockLocatorString(t *testing.T) {
	tests := []struct {
		name    string
		hashes  []*chainhash.Hash
		want    string
		wantLen int
	}{
		{
			name:    "Empty locator hashes",
			hashes:  []*chainhash.Hash{},
			want:    "",
			wantLen: 0,
		},
		{
			name:    "Nil locator hashes",
			hashes:  nil,
			want:    "",
			wantLen: 0,
		},
		{
			name: "Single hash",
			hashes: []*chainhash.Hash{
				{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0x10,
					0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18, 0x19, 0x1a, 0x1b, 0x1c, 0x1d, 0x1e, 0x1f, 0x20},
			},
			wantLen: 64, // Each hash string is 64 characters
		},
		{
			name: "Multiple hashes",
			hashes: []*chainhash.Hash{
				{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0x10,
					0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18, 0x19, 0x1a, 0x1b, 0x1c, 0x1d, 0x1e, 0x1f, 0x20},
				{0x21, 0x22, 0x23, 0x24, 0x25, 0x26, 0x27, 0x28, 0x29, 0x2a, 0x2b, 0x2c, 0x2d, 0x2e, 0x2f, 0x30,
					0x31, 0x32, 0x33, 0x34, 0x35, 0x36, 0x37, 0x38, 0x39, 0x3a, 0x3b, 0x3c, 0x3d, 0x3e, 0x3f, 0x40},
			},
			wantLen: 128, // 2 hashes * 64 characters each
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := BuildBlockLocatorString(tt.hashes)
			if tt.want != "" {
				assert.Equal(t, tt.want, result)
			}
			assert.Equal(t, tt.wantLen, len(result))

			// Verify that each hash appears in the result if we have hashes
			if len(tt.hashes) > 0 {
				for _, hash := range tt.hashes {
					hashStr := hash.String()
					assert.Contains(t, result, hashStr)
				}
			}
		})
	}
}

func TestCheckContextCancellation(t *testing.T) {
	tests := []struct {
		name    string
		ctx     context.Context
		wantErr bool
	}{
		{
			name:    "Context not cancelled",
			ctx:     context.Background(),
			wantErr: false,
		},
		{
			name: "Context cancelled",
			ctx: func() context.Context {
				ctx, cancel := context.WithCancel(context.Background())
				cancel() // Cancel immediately
				return ctx
			}(),
			wantErr: true,
		},
		{
			name: "Context with timeout - not expired",
			ctx: func() context.Context {
				ctx, cancel := context.WithTimeout(context.Background(), time.Hour)
				t.Cleanup(cancel) // Clean up to avoid leaks
				return ctx
			}(),
			wantErr: false,
		},
		{
			name: "Context with timeout - expired",
			ctx: func() context.Context {
				ctx, cancel := context.WithTimeout(context.Background(), time.Nanosecond)
				t.Cleanup(cancel)
				time.Sleep(time.Millisecond) // Ensure timeout
				return ctx
			}(),
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := CheckContextCancellation(tt.ctx)
			if tt.wantErr {
				require.Error(t, err)
				assert.Contains(t, err.Error(), "operation cancelled")
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestCreateCatchupResult(t *testing.T) {
	// Create test data
	targetHash := &chainhash.Hash{0x01, 0x02, 0x03}
	startHash := &chainhash.Hash{0x04, 0x05, 0x06}
	startHeight := uint32(100)
	startTime := time.Now().Add(-time.Hour)
	peerURL := "http://peer.example.com"
	iterations := 5
	failedIterations := []IterationError{
		{
			Iteration:  1,
			Error:      errors.NewError("test error"),
			Timestamp:  time.Now(),
			PeerURL:    peerURL,
			RetryCount: 2,
			Duration:   time.Second,
		},
	}
	reachedTarget := true
	stopReason := "completed"

	// Create some test headers
	headers := []*model.BlockHeader{
		createHelpersTestHeader(t, 1, startHash),
		createHelpersTestHeader(t, 2, nil),
	}

	// Test the function
	result := CreateCatchupResult(
		headers, targetHash, startHash, startHeight, startTime,
		peerURL, iterations, failedIterations, reachedTarget, stopReason,
	)

	// Verify the result
	require.NotNil(t, result)
	assert.Equal(t, headers, result.Headers)
	assert.Equal(t, targetHash, result.TargetHash)
	assert.Equal(t, startHash, result.StartHash)
	assert.Equal(t, startHeight, result.StartHeight)
	assert.Equal(t, startTime, result.StartTime)
	assert.Equal(t, peerURL, result.PeerURL)
	assert.Equal(t, iterations, result.TotalIterations)
	assert.Equal(t, len(headers), result.TotalHeadersReceived)
	assert.Equal(t, failedIterations, result.FailedIterations)
	assert.Equal(t, reachedTarget, result.ReachedTarget)
	assert.Equal(t, stopReason, result.StopReason)
	assert.True(t, result.EndTime.After(result.StartTime))
	assert.True(t, result.Duration > 0)

	// Verify partial success calculation
	assert.False(t, result.PartialSuccess) // reachedTarget is true, so partial success should be false

	// Verify last processed hash is set
	assert.NotNil(t, result.LastProcessedHash)
	assert.Equal(t, headers[len(headers)-1].Hash(), result.LastProcessedHash)
}

func TestCreateCatchupResultWithLocator(t *testing.T) {
	// Test data
	targetHash := &chainhash.Hash{0x01, 0x02, 0x03}
	startHash := &chainhash.Hash{0x04, 0x05, 0x06}
	startHeight := uint32(200)
	startTime := time.Now().Add(-2 * time.Hour)
	peerURL := "http://peer2.example.com"
	iterations := 3
	failedIterations := []IterationError{}
	reachedTarget := false
	stopReason := "timeout"
	locatorHashes := []*chainhash.Hash{
		{0x07, 0x08, 0x09},
		{0x0a, 0x0b, 0x0c},
	}

	headers := []*model.BlockHeader{
		createHelpersTestHeader(t, 10, startHash),
	}

	// Test the function
	result := CreateCatchupResultWithLocator(
		headers, targetHash, startHash, startHeight, startTime,
		peerURL, iterations, failedIterations, reachedTarget, stopReason, locatorHashes,
	)

	// Verify the result
	require.NotNil(t, result)
	assert.Equal(t, headers, result.Headers)
	assert.Equal(t, targetHash, result.TargetHash)
	assert.Equal(t, startHash, result.StartHash)
	assert.Equal(t, startHeight, result.StartHeight)
	assert.Equal(t, peerURL, result.PeerURL)
	assert.Equal(t, iterations, result.TotalIterations)
	assert.Equal(t, len(headers), result.TotalHeadersReceived)
	assert.Equal(t, failedIterations, result.FailedIterations)
	assert.Equal(t, reachedTarget, result.ReachedTarget)
	assert.Equal(t, stopReason, result.StopReason)
	assert.Equal(t, locatorHashes, result.LocatorHashes)

	// Verify partial success calculation (headers > 0 && !reachedTarget)
	assert.True(t, result.PartialSuccess)
	assert.True(t, result.StoppedEarly) // !reachedTarget && stopReason != ""

	// Test with no headers
	resultNoHeaders := CreateCatchupResultWithLocator(
		nil, targetHash, startHash, startHeight, startTime,
		peerURL, iterations, failedIterations, reachedTarget, stopReason, locatorHashes,
	)
	require.NotNil(t, resultNoHeaders)
	assert.Nil(t, resultNoHeaders.LastProcessedHash) // No headers means no last processed hash
	assert.False(t, resultNoHeaders.PartialSuccess)  // len(headers) == 0

	// Test with reached target and empty stop reason
	resultReached := CreateCatchupResultWithLocator(
		headers, targetHash, startHash, startHeight, startTime,
		peerURL, iterations, failedIterations, true, "", locatorHashes,
	)
	require.NotNil(t, resultReached)
	assert.False(t, resultReached.StoppedEarly) // reachedTarget is true, so no early stop
}

func TestFetchHeadersWithRetry(t *testing.T) {
	// This function calls retry.Retry and util.DoHTTPRequest which are external dependencies
	// We'll test the basic structure and error categorization logic

	ctx := context.Background()
	logger := ulogger.TestLogger{}
	url := "http://test.example.com/headers"
	maxRetries := 3

	// Note: This test will fail because util.DoHTTPRequest will likely fail
	// But it will test the function structure and error handling paths
	_, err := FetchHeadersWithRetry(ctx, logger, url, maxRetries)

	// The function should return an error since the URL doesn't exist
	// but we can verify the function can be called without panicking
	require.Error(t, err)

	// Test with cancelled context
	cancelledCtx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err = FetchHeadersWithRetry(cancelledCtx, logger, url, maxRetries)
	require.Error(t, err)
}

func TestFetchHeadersWithRetry_ErrorCategorization(t *testing.T) {
	// We can't easily mock util.DoHTTPRequest, but we can test that the function
	// handles different error types properly by examining the error messages
	// This is more of an integration test

	ctx := context.Background()
	logger := ulogger.TestLogger{}

	// Test with invalid URL that should cause a connection error
	invalidURL := "http://192.0.2.0:12345/headers" // RFC 3330 test address
	_, err := FetchHeadersWithRetry(ctx, logger, invalidURL, 1)
	require.Error(t, err)

	// The error should be wrapped appropriately by the retry mechanism
	// We can't test specific error types without mocking, but we can ensure no panic
	assert.NotEmpty(t, err.Error())

	// Test with context that times out quickly
	timeoutCtx, cancel := context.WithTimeout(context.Background(), time.Nanosecond)
	defer cancel()
	time.Sleep(time.Millisecond) // Ensure timeout

	_, err = FetchHeadersWithRetry(timeoutCtx, logger, invalidURL, 1)
	require.Error(t, err)
}

func TestCreateCatchupResultEdgeCases(t *testing.T) {
	// Test with nil values
	result := CreateCatchupResult(nil, nil, nil, 0, time.Time{}, "", 0, nil, false, "")
	require.NotNil(t, result)
	assert.Nil(t, result.Headers)
	assert.Nil(t, result.TargetHash)
	assert.Nil(t, result.StartHash)
	assert.Equal(t, uint32(0), result.StartHeight)
	assert.Equal(t, 0, result.TotalIterations)
	assert.Equal(t, 0, result.TotalHeadersReceived)
	assert.False(t, result.ReachedTarget)
	assert.False(t, result.PartialSuccess)
	assert.Nil(t, result.LastProcessedHash)

	// Test CreateCatchupResultWithLocator with nil locator
	result2 := CreateCatchupResultWithLocator(nil, nil, nil, 0, time.Time{}, "", 0, nil, false, "", nil)
	require.NotNil(t, result2)
	assert.Nil(t, result2.LocatorHashes)
}

func TestHelpersFunctionIntegration(t *testing.T) {
	// Integration test showing how the helper functions work together

	// Create some test header bytes (valid 80-byte header)
	headerBytes := make([]byte, 80)
	// Set version to 1 (little endian)
	headerBytes[0] = 0x01
	headerBytes[1] = 0x00
	headerBytes[2] = 0x00
	headerBytes[3] = 0x00

	// Validate the bytes first
	err := ValidateBlockHeaderBytes(headerBytes)
	require.NoError(t, err)

	// Create some locator hashes
	locatorHashes := []*chainhash.Hash{
		{0x01, 0x02, 0x03},
		{0x04, 0x05, 0x06},
	}

	// Build locator string
	locatorString := BuildBlockLocatorString(locatorHashes)
	assert.Len(t, locatorString, 128) // 2 hashes * 64 chars each

	// Check context cancellation
	ctx := context.Background()
	err = CheckContextCancellation(ctx)
	require.NoError(t, err)

	// Create a result using the helper
	targetHash := &chainhash.Hash{0x07, 0x08, 0x09}
	startHash := &chainhash.Hash{0x0a, 0x0b, 0x0c}
	startTime := time.Now().Add(-time.Minute)

	result := CreateCatchupResult(nil, targetHash, startHash, 100, startTime, "http://test", 1, nil, false, "test")
	require.NotNil(t, result)
	assert.Equal(t, targetHash, result.TargetHash)
	assert.Equal(t, startHash, result.StartHash)
}
