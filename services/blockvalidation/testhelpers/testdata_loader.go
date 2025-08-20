package testhelpers

import (
	"encoding/json"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/bitcoin-sv/teranode/errors"
	"github.com/bitcoin-sv/teranode/model"
	"github.com/stretchr/testify/require"
)

// TestHeaderData stores the serialized header data loaded from file
type TestHeaderData struct {
	Headers    [][]byte  `json:"headers"`
	Count      int       `json:"count"`
	Generated  time.Time `json:"generated"`
	Difficulty string    `json:"difficulty"`
}

var (
	// Cache for loaded test headers to avoid repeated file reads
	cachedTestHeaders     []*model.BlockHeader
	cachedTestHeadersOnce sync.Once
	cachedTestHeadersErr  error

	// Cache for small test headers
	cachedSmallTestHeaders     []*model.BlockHeader
	cachedSmallTestHeadersOnce sync.Once
	cachedSmallTestHeadersErr  error
)

// loadTestHeadersFromFile loads pre-generated test headers from a JSON file.
// The headers are cached after first load to improve test performance.
//
// Parameters:
//   - filename: Name of the file in testdata directory (e.g., "test_headers_small.json")
//   - cache: Pointer to cache slice to store loaded headers
//   - once: Sync.Once for ensuring single load
//   - cacheErr: Pointer to error cache
//
// Returns:
//   - []*model.BlockHeader: Loaded headers
//   - error: If loading or parsing fails
func loadTestHeadersFromFile(filename string, cache *[]*model.BlockHeader, once *sync.Once, cacheErr *error) ([]*model.BlockHeader, error) {
	once.Do(func() {
		// Build path to testdata file
		testdataPath := filepath.Join("testdata", filename)

		// Read the file
		data, err := os.ReadFile(testdataPath)
		if err != nil {
			*cacheErr = errors.NewProcessingError("failed to read test headers file %s: %w", testdataPath, err)
			return
		}

		// Parse JSON
		var testData TestHeaderData
		if err := json.Unmarshal(data, &testData); err != nil {
			*cacheErr = errors.NewProcessingError("failed to parse test headers JSON: %w", err)
			return
		}

		// Convert byte arrays back to headers
		headers := make([]*model.BlockHeader, len(testData.Headers))
		for i, headerBytes := range testData.Headers {
			header, err := model.NewBlockHeaderFromBytes(headerBytes)
			if err != nil {
				*cacheErr = errors.NewProcessingError("failed to parse header %d: %w", i, err)
				return
			}
			headers[i] = header
		}

		*cache = headers
	})

	if *cacheErr != nil {
		return nil, *cacheErr
	}

	return *cache, nil
}

// LoadTestHeaders loads the full set of pre-generated test headers (15000 headers).
// These headers have valid proof of work and can be used for large tests.
// The headers are cached after first load.
//
// Returns:
//   - []*model.BlockHeader: All test headers
//   - error: If loading fails
func LoadTestHeaders() ([]*model.BlockHeader, error) {
	return loadTestHeadersFromFile("test_headers.json", &cachedTestHeaders, &cachedTestHeadersOnce, &cachedTestHeadersErr)
}

// LoadSmallTestHeaders loads a smaller set of pre-generated test headers (1500 headers).
// These headers have valid proof of work and are suitable for most tests.
// The headers are cached after first load.
//
// Returns:
//   - []*model.BlockHeader: Small set of test headers
//   - error: If loading fails
func LoadSmallTestHeaders() ([]*model.BlockHeader, error) {
	return loadTestHeadersFromFile("test_headers_small.json", &cachedSmallTestHeaders, &cachedSmallTestHeadersOnce, &cachedSmallTestHeadersErr)
}

// GetTestHeaders returns a slice of pre-generated test headers with valid proof of work.
// It automatically chooses between small and large header sets based on count.
// All headers are from pre-mined chains with proper parent-child relationships.
//
// Parameters:
//   - t: Testing instance for assertions
//   - count: Number of headers needed
//
// Returns:
//   - []*model.BlockHeader: Slice of headers with length == count
func GetTestHeaders(t *testing.T, count int) []*model.BlockHeader {
	t.Helper()

	var headers []*model.BlockHeader
	var err error

	// Choose appropriate header set based on count
	if count <= 1500 {
		headers, err = LoadSmallTestHeaders()
	} else {
		headers, err = LoadTestHeaders()
	}

	require.NoError(t, err, "Failed to load test headers")
	require.GreaterOrEqual(t, len(headers), count, "Not enough test headers available (need %d, have %d)", count, len(headers))

	// Return the requested number of headers
	return headers[:count]
}

// GetTestHeadersRange returns a slice of pre-generated test headers within a specific range.
// Useful for simulating partial chains or specific block ranges.
//
// Parameters:
//   - t: Testing instance for assertions
//   - start: Starting index (inclusive)
//   - end: Ending index (exclusive)
//
// Returns:
//   - []*model.BlockHeader: Slice of headers from start to end
func GetTestHeadersRange(t *testing.T, start, end int) []*model.BlockHeader {
	t.Helper()

	require.Less(t, start, end, "Start index must be less than end index")

	var headers []*model.BlockHeader
	var err error

	// Choose appropriate header set based on end index
	if end <= 1500 {
		headers, err = LoadSmallTestHeaders()
	} else {
		headers, err = LoadTestHeaders()
	}

	require.NoError(t, err, "Failed to load test headers")
	require.GreaterOrEqual(t, len(headers), end, "Not enough test headers available (need up to index %d, have %d)", end, len(headers))

	return headers[start:end]
}

// GetTestBlocks creates test blocks from pre-generated headers.
// The blocks will have the headers with valid proof of work and proper heights.
//
// Parameters:
//   - t: Testing instance for assertions
//   - count: Number of blocks needed
//   - startHeight: Height of the first block
//
// Returns:
//   - []*model.Block: Slice of blocks with valid headers
func GetTestBlocks(t *testing.T, count int, startHeight uint32) []*model.Block {
	t.Helper()

	headers := GetTestHeaders(t, count)
	blocks := make([]*model.Block, count)

	for i, header := range headers {
		blocks[i] = &model.Block{
			Header: header,
			Height: startHeight + uint32(i),
		}
	}

	return blocks
}

// GetTestBlocksRange creates test blocks from pre-generated headers within a range.
//
// Parameters:
//   - t: Testing instance for assertions
//   - start: Starting header index
//   - end: Ending header index (exclusive)
//   - startHeight: Height of the first block
//
// Returns:
//   - []*model.Block: Slice of blocks with valid headers
func GetTestBlocksRange(t *testing.T, start, end int, startHeight uint32) []*model.Block {
	t.Helper()

	headers := GetTestHeadersRange(t, start, end)
	blocks := make([]*model.Block, len(headers))

	for i, header := range headers {
		blocks[i] = &model.Block{
			Header: header,
			Height: startHeight + uint32(i),
		}
	}

	return blocks
}
