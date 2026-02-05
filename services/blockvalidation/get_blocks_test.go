package blockvalidation

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/services/blockvalidation/testhelpers"
	"github.com/bsv-blockchain/teranode/stores/blob"
	"github.com/bsv-blockchain/teranode/stores/blob/memory"
	"github.com/bsv-blockchain/teranode/test/utils/transactions"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/jarcoal/httpmock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// TestFetchBlocksConcurrently_CurrentImplementation tests the existing fetchBlocksConcurrently function behavior
func TestFetchBlocksConcurrently_CurrentImplementation(t *testing.T) {
	t.Run("Single Block Fetch", func(t *testing.T) {
		suite := NewCatchupTestSuite(t)
		defer suite.Cleanup()

		// Create test blocks
		blocks := testhelpers.CreateTestBlockChain(t, 2)
		targetBlock := blocks[1]
		headers := []*model.BlockHeader{blocks[1].Header}

		// Set up HTTP mock for block fetch
		httpmock.ActivateNonDefault(util.HTTPClient())
		defer httpmock.DeactivateAndReset()

		httpmock.RegisterResponder(
			"GET",
			fmt.Sprintf("http://test-peer/blocks/%s?n=1", blocks[1].Header.Hash().String()),
			httpmock.NewBytesResponder(200, func() []byte {
				blockBytes, _ := blocks[1].Bytes()
				return blockBytes
			}()),
		)

		// Create channels and counters
		var size atomic.Int64
		size.Store(1)
		validateBlocksChan := make(chan *model.Block, 1)

		catchupCtx := &CatchupContext{
			blockUpTo:    targetBlock,
			baseURL:      "http://test-peer",
			blockHeaders: headers,
			commonAncestorMeta: &model.BlockHeaderMeta{
				Height: 0,
			},
		}

		// Call fetchBlocksConcurrently
		err := suite.Server.fetchBlocksConcurrently(suite.Ctx, catchupCtx, validateBlocksChan, &size)
		require.NoError(t, err)

		// Wait for goroutines to complete
		// err = errorGroup.Wait()
		// require.NoError(t, err)

		// Collect blocks from channel with timeout
		var receivedBlocks []*model.Block
		for i := 0; i < 1; i++ {
			select {
			case block := <-validateBlocksChan:
				receivedBlocks = append(receivedBlocks, block)
			case <-time.After(time.Second):
				t.Fatalf("Timeout waiting for block %d/%d", i+1, 1)
			}
		}

		// Verify block was sent to channel
		assert.Len(t, receivedBlocks, 1)
		assert.Equal(t, blocks[1].Header.Hash(), receivedBlocks[0].Header.Hash())
	})

	t.Run("Multiple_Blocks_Fetch_-_Ordering_Test", func(t *testing.T) {
		suite := NewCatchupTestSuite(t)
		defer suite.Cleanup()

		numBlocks := 5
		blocks := testhelpers.CreateTestBlockChain(t, numBlocks+1)
		targetBlock := blocks[numBlocks]

		var headers []*model.BlockHeader
		for i := 1; i <= numBlocks; i++ {
			headers = append(headers, blocks[i].Header)
		}

		// Set up HTTP mocks for batch fetching (current implementation uses large batches)
		httpmock.ActivateNonDefault(util.HTTPClient())
		defer httpmock.DeactivateAndReset()

		// Mock batch request for all 5 blocks in one request
		// The request will be for the LAST block hash, and blocks should be returned in reverse order
		batchData := bytes.Buffer{}
		for i := numBlocks; i >= 1; i-- { // Reverse order
			blockBytes, err := blocks[i].Bytes()
			require.NoError(t, err)
			batchData.Write(blockBytes)
		}

		httpmock.RegisterResponder("GET", fmt.Sprintf("http://test-peer/blocks/%s?n=%d", blocks[numBlocks].Header.Hash().String(), numBlocks),
			httpmock.NewBytesResponder(200, batchData.Bytes()))

		// Create channels and counters
		var size atomic.Int64
		size.Store(int64(numBlocks))
		validateBlocksChan := make(chan *model.Block, numBlocks)

		catchupCtx := &CatchupContext{
			blockUpTo:    targetBlock,
			baseURL:      "http://test-peer",
			blockHeaders: headers,
			commonAncestorMeta: &model.BlockHeaderMeta{
				Height: 0,
			},
		}

		// Call fetchBlocksConcurrently
		err := suite.Server.fetchBlocksConcurrently(suite.Ctx, catchupCtx, validateBlocksChan, &size)
		require.NoError(t, err)

		// Wait for goroutines to complete
		// err = errorGroup.Wait()
		// require.NoError(t, err)

		// Collect blocks from channel with timeout
		var receivedBlocks []*model.Block
		for i := 0; i < numBlocks; i++ {
			select {
			case block := <-validateBlocksChan:
				receivedBlocks = append(receivedBlocks, block)
			case <-time.After(time.Second):
				t.Fatalf("Timeout waiting for block %d/%d", i+1, numBlocks)
			}
		}

		// Verify we received all blocks
		assert.Len(t, receivedBlocks, numBlocks)

		// Verify blocks are delivered in correct order (worker pool architecture ensures this)
		receivedHashes := make([]string, len(receivedBlocks))
		expectedHashes := make([]string, len(headers))

		for i, block := range receivedBlocks {
			receivedHashes[i] = block.Header.Hash().String()
		}
		for i, header := range headers {
			expectedHashes[i] = header.Hash().String()
		}

		assert.Equal(t, expectedHashes, receivedHashes, "Blocks should be delivered in correct chain order")
	})

	t.Run("HTTP Error Handling", func(t *testing.T) {
		suite := NewCatchupTestSuite(t)
		defer suite.Cleanup()

		numBlocks := 3
		blocks := testhelpers.CreateTestBlockChain(t, numBlocks+1)
		targetBlock := blocks[numBlocks]

		var headers []*model.BlockHeader
		for i := 1; i <= numBlocks; i++ {
			headers = append(headers, blocks[i].Header)
		}

		// Set up HTTP mock to return error for batch request
		httpmock.ActivateNonDefault(util.HTTPClient())
		defer httpmock.DeactivateAndReset()

		httpmock.RegisterResponder("GET", fmt.Sprintf("http://test-peer/blocks/%s?n=%d", blocks[1].Header.Hash().String(), numBlocks),
			httpmock.NewStringResponder(500, "Internal Server Error"))

		// Create channels and counters
		var size atomic.Int64
		size.Store(int64(numBlocks))
		validateBlocksChan := make(chan *model.Block, numBlocks)

		catchupCtx := &CatchupContext{
			blockUpTo:    targetBlock,
			baseURL:      "http://test-peer",
			blockHeaders: headers,
			commonAncestorMeta: &model.BlockHeaderMeta{
				Height: 0,
			},
		}

		// Call fetchBlocksConcurrently - it now handles its own error group internally
		err := suite.Server.fetchBlocksConcurrently(suite.Ctx, catchupCtx, validateBlocksChan, &size)

		// The function should return an error when HTTP request fails
		require.Error(t, err)
		assert.Contains(t, err.Error(), "failed to fetch batch")

		// Channel should be closed with no blocks - don't try to read since error occurred
		// The channel will be closed by orderedDelivery but may be empty due to error
		select {
		case <-validateBlocksChan:
			// May receive some blocks before error, that's ok
		default:
			// Or may receive no blocks, that's also ok
		}
	})

	t.Run("Context Cancellation", func(t *testing.T) {
		suite := NewCatchupTestSuite(t)
		defer suite.Cleanup()

		blocks := testhelpers.CreateTestBlockChain(t, 2)
		targetBlock := blocks[1]
		headers := []*model.BlockHeader{blocks[1].Header}

		// Set up HTTP mock with delay
		httpmock.ActivateNonDefault(util.HTTPClient())
		defer httpmock.DeactivateAndReset()

		httpmock.RegisterResponder(
			"GET",
			fmt.Sprintf("http://test-peer/blocks/%s?n=1", blocks[1].Header.Hash().String()),
			func(req *http.Request) (*http.Response, error) {
				time.Sleep(200 * time.Millisecond)
				blockBytes, err := blocks[1].Bytes()
				if err != nil {
					return nil, err
				}
				return httpmock.NewBytesResponse(200, blockBytes), nil
			},
		)

		// Create channels and counters
		var size atomic.Int64
		size.Store(1)
		validateBlocksChan := make(chan *model.Block, 1)

		// Create cancellable context
		ctx, cancel := context.WithCancel(suite.Ctx)
		// errorGroup, gCtx := errgroup.WithContext(ctx)

		catchupCtx := &CatchupContext{
			blockUpTo:    targetBlock,
			baseURL:      "http://test-peer",
			blockHeaders: headers,
			commonAncestorMeta: &model.BlockHeaderMeta{
				Height: 0,
			},
		}

		// Call fetchBlocksConcurrently
		err := suite.Server.fetchBlocksConcurrently(ctx, catchupCtx, validateBlocksChan, &size)
		require.NoError(t, err)

		// Cancel context after a short delay
		go func() {
			time.Sleep(50 * time.Millisecond)
			cancel()
		}()

		// Wait for goroutines to complete - should return context cancelled error
		// err = errorGroup.Wait()
		// assert.Error(t, err)
		// assert.Contains(t, err.Error(), "context canceled")
	})

	t.Run("Empty_Block_Response", func(t *testing.T) {
		suite := NewCatchupTestSuite(t)
		defer suite.Cleanup()

		numBlocks := 2
		blocks := testhelpers.CreateTestBlockChain(t, numBlocks+1)
		targetBlock := blocks[numBlocks]

		var headers []*model.BlockHeader
		for i := 1; i <= numBlocks; i++ {
			headers = append(headers, blocks[i].Header)
		}

		// Set up HTTP mock to return empty response for batch request
		httpmock.ActivateNonDefault(util.HTTPClient())
		defer httpmock.DeactivateAndReset()

		httpmock.RegisterResponder("GET", fmt.Sprintf("http://test-peer/blocks/%s?n=%d", blocks[numBlocks].Header.Hash().String(), numBlocks),
			httpmock.NewBytesResponder(200, []byte{}))

		// Create channels and counters
		var size atomic.Int64
		size.Store(int64(numBlocks))
		validateBlocksChan := make(chan *model.Block, numBlocks)

		catchupCtx := &CatchupContext{
			blockUpTo:    targetBlock,
			baseURL:      "http://test-peer",
			blockHeaders: headers,
			commonAncestorMeta: &model.BlockHeaderMeta{
				Height: 0,
			},
		}

		// Call fetchBlocksConcurrently - it now handles its own error group internally
		err := suite.Server.fetchBlocksConcurrently(suite.Ctx, catchupCtx, validateBlocksChan, &size)

		// The function should return an error when response is empty (expected blocks but got none)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "expected 2 blocks, got 0")

		// Channel should be closed with no blocks - don't try to read since error occurred
		// The channel will be closed by orderedDelivery but may be empty due to error
		select {
		case <-validateBlocksChan:
			// May receive some blocks before error, that's ok
		default:
			// Or may receive no blocks, that's also ok
		}
	})

	t.Run("Concurrent_Block_Fetching_Behavior", func(t *testing.T) {
		suite := NewCatchupTestSuite(t)
		defer suite.Cleanup()

		numBlocks := 10
		blocks := testhelpers.CreateTestBlockChain(t, numBlocks+1)
		targetBlock := blocks[numBlocks]

		var headers []*model.BlockHeader
		for i := 1; i <= numBlocks; i++ {
			headers = append(headers, blocks[i].Header)
		}

		// Track request timing to verify concurrent behavior
		var requestTimes []time.Time
		var timeMutex sync.Mutex

		httpmock.ActivateNonDefault(util.HTTPClient())
		defer httpmock.DeactivateAndReset()

		// Mock batch request for all blocks - blocks returned in reverse order
		batchData := bytes.Buffer{}
		for i := numBlocks; i >= 1; i-- { // Reverse order
			blockBytes, _ := blocks[i].Bytes()
			batchData.Write(blockBytes)
		}
		httpmock.RegisterResponder("GET", fmt.Sprintf("http://test-peer/blocks/%s?n=%d", blocks[numBlocks].Hash().String(), numBlocks),
			func(req *http.Request) (*http.Response, error) {
				timeMutex.Lock()
				requestTimes = append(requestTimes, time.Now())
				timeMutex.Unlock()

				// Small delay to simulate network
				time.Sleep(10 * time.Millisecond)
				return httpmock.NewBytesResponse(200, batchData.Bytes()), nil
			})

		// Create channels and counters
		var size atomic.Int64
		size.Store(int64(numBlocks))
		validateBlocksChan := make(chan *model.Block, numBlocks)

		// Create error group and measure total time
		// errorGroup, gCtx := errgroup.WithContext(suite.Ctx)
		startTime := time.Now()

		catchupCtx := &CatchupContext{
			blockUpTo:    targetBlock,
			baseURL:      "http://test-peer",
			blockHeaders: headers,
			commonAncestorMeta: &model.BlockHeaderMeta{
				Height: 0,
			},
		}

		// Call fetchBlocksConcurrently
		err := suite.Server.fetchBlocksConcurrently(suite.Ctx, catchupCtx, validateBlocksChan, &size)
		assert.NoError(t, err)

		// Wait for completion
		// err = errorGroup.Wait()
		// assert.NoError(t, err)
		totalTime := time.Since(startTime)

		// Collect blocks from channel with timeout
		var receivedBlocks []*model.Block
		for i := 0; i < numBlocks; i++ {
			select {
			case block := <-validateBlocksChan:
				receivedBlocks = append(receivedBlocks, block)
			case <-time.After(time.Second):
				t.Fatalf("Timeout waiting for block %d/%d", i+1, numBlocks)
			}
		}

		assert.Len(t, receivedBlocks, numBlocks)

		// Verify timing characteristics of batch fetching
		timeMutex.Lock()
		defer timeMutex.Unlock()

		// Should make only 1 batch request instead of multiple individual requests
		assert.Equal(t, 1, len(requestTimes), "Should make 1 batch request")
		assert.Equal(t, 1, httpmock.GetTotalCallCount(), "Should make 1 HTTP call total")

		// Total time should be efficient (batch + worker processing)
		t.Logf("Total processing time: %v", totalTime)
		t.Logf("Batch fetching with worker pool architecture is efficient")
	})

	t.Run("No Headers", func(t *testing.T) {
		suite := NewCatchupTestSuite(t)
		defer suite.Cleanup()

		blocks := testhelpers.CreateTestBlockChain(t, 1)
		targetBlock := blocks[0]
		headers := []*model.BlockHeader{} // Empty headers

		var size atomic.Int64
		size.Store(0)
		validateBlocksChan := make(chan *model.Block, 1)

		catchupCtx := &CatchupContext{
			blockUpTo:    targetBlock,
			baseURL:      "http://test-peer",
			blockHeaders: headers,
			commonAncestorMeta: &model.BlockHeaderMeta{
				Height: 0,
			},
		}

		// Call fetchBlocksConcurrently
		err := suite.Server.fetchBlocksConcurrently(suite.Ctx, catchupCtx, validateBlocksChan, &size)
		require.NoError(t, err)

		// Wait for goroutines to complete
		// err = errorGroup.Wait()
		// require.NoError(t, err)

		// Channel should be empty when no headers are provided
		var receivedBlocks []*model.Block

		// Try to read from channel with timeout - should get nothing
		select {
		case block := <-validateBlocksChan:
			if block != nil {
				receivedBlocks = append(receivedBlocks, block)
			}
		case <-time.After(100 * time.Millisecond):
			// Timeout is expected when no headers to process
		}
		assert.Len(t, receivedBlocks, 0)
	})

	t.Run("Nil Headers", func(t *testing.T) {
		suite := NewCatchupTestSuite(t)
		defer suite.Cleanup()

		blocks := testhelpers.CreateTestBlockChain(t, 1)
		targetBlock := blocks[0]
		var headers []*model.BlockHeader // nil slice

		var size atomic.Int64
		size.Store(0)
		validateBlocksChan := make(chan *model.Block, 1)

		catchupCtx := &CatchupContext{
			blockUpTo:    targetBlock,
			baseURL:      "http://test-peer",
			blockHeaders: headers,
			commonAncestorMeta: &model.BlockHeaderMeta{
				Height: 0,
			},
		}

		// Call fetchBlocksConcurrently
		err := suite.Server.fetchBlocksConcurrently(suite.Ctx, catchupCtx, validateBlocksChan, &size)
		require.NoError(t, err)

		// Wait for goroutines to complete
		// err = errorGroup.Wait()
		// require.NoError(t, err)
	})

	t.Run("Fetch Blocks Batch", func(t *testing.T) {
		suite := NewCatchupTestSuite(t)
		defer suite.Cleanup()

		// Create test block
		blocks := testhelpers.CreateTestBlockChain(t, 2)
		targetHash := blocks[1].Header.Hash()

		// Set up HTTP mock
		httpmock.ActivateNonDefault(util.HTTPClient())
		defer httpmock.DeactivateAndReset()

		httpmock.RegisterResponder(
			"GET",
			fmt.Sprintf("http://test-peer/blocks/%s?n=1", blocks[1].Header.Hash().String()),
			httpmock.NewBytesResponder(200, func() []byte {
				blockBytes, _ := blocks[1].Bytes()
				return blockBytes
			}()),
		)

		// Call fetchBlocksBatch
		fetchedBlocks, err := suite.Server.fetchBlocksBatch(suite.Ctx, targetHash, 1, "test-peer-id", "http://test-peer")
		require.NoError(t, err)
		require.Len(t, fetchedBlocks, 1)
		assert.Equal(t, targetHash, fetchedBlocks[0].Header.Hash())
	})

	t.Run("Fetch Single Block", func(t *testing.T) {
		suite := NewCatchupTestSuite(t)
		defer suite.Cleanup()

		// Create test block
		blocks := testhelpers.CreateTestBlockChain(t, 2)
		targetHash := blocks[1].Header.Hash()

		// Set up HTTP mock
		httpmock.ActivateNonDefault(util.HTTPClient())
		defer httpmock.DeactivateAndReset()

		httpmock.RegisterResponder(
			"GET",
			fmt.Sprintf("http://test-peer/block/%s", targetHash.String()),
			httpmock.NewBytesResponder(200, func() []byte {
				blockBytes, _ := blocks[1].Bytes()
				return blockBytes
			}()),
		)

		// Call fetchSingleBlock
		fetchedBlock, err := suite.Server.fetchSingleBlock(suite.Ctx, targetHash, "12D3KooWL1NF6fdTJ9cucEuwvuX8V8KtpJZZnUE4umdLBuK15eUZ", "http://test-peer")
		require.NoError(t, err)
		require.NotNil(t, fetchedBlock)
		assert.Equal(t, targetHash, fetchedBlock.Header.Hash())
	})
}

// TestFetchBlocksConcurrently_PerformanceCharacteristics documents current performance characteristics
func TestFetchBlocksConcurrently_PerformanceCharacteristics(t *testing.T) {
	t.Run("Memory_Usage_Pattern", func(t *testing.T) {
		suite := NewCatchupTestSuite(t)
		defer suite.Cleanup()

		numBlocks := 100
		blocks := testhelpers.CreateTestBlockChain(t, numBlocks+1)
		targetBlock := blocks[numBlocks]

		var headers []*model.BlockHeader
		for i := 1; i <= numBlocks; i++ {
			headers = append(headers, blocks[i].Header)
		}

		// Mock batch request for large batch processing
		httpmock.ActivateNonDefault(util.HTTPClient())
		defer httpmock.DeactivateAndReset()

		// Mock single large batch request - blocks returned in reverse order
		batchData := bytes.Buffer{}
		for i := numBlocks; i >= 1; i-- { // Reverse order
			blockBytes, err := blocks[i].Bytes()
			require.NoError(t, err)
			batchData.Write(blockBytes)
		}

		httpmock.RegisterResponder("GET", fmt.Sprintf("http://test-peer/blocks/%s?n=%d", blocks[numBlocks].Header.Hash().String(), numBlocks),
			httpmock.NewBytesResponder(200, batchData.Bytes()))

		// Create channels and counters
		var size atomic.Int64
		size.Store(int64(numBlocks))
		validateBlocksChan := make(chan *model.Block, numBlocks)

		catchupCtx := &CatchupContext{
			blockUpTo:    targetBlock,
			baseURL:      "http://test-peer",
			blockHeaders: headers,
			commonAncestorMeta: &model.BlockHeaderMeta{
				Height: 0,
			},
		}

		// Call fetchBlocksConcurrently
		err := suite.Server.fetchBlocksConcurrently(suite.Ctx, catchupCtx, validateBlocksChan, &size)
		require.NoError(t, err)

		// Wait for goroutines to complete
		// err = errorGroup.Wait()
		// require.NoError(t, err)

		// Collect blocks from channel with timeout
		var receivedBlocks []*model.Block
		for i := 0; i < numBlocks; i++ {
			select {
			case block := <-validateBlocksChan:
				receivedBlocks = append(receivedBlocks, block)
			case <-time.After(time.Second):
				t.Fatalf("Timeout waiting for block %d/%d", i+1, numBlocks)
			}
		}

		// Verify all blocks were received
		assert.Len(t, receivedBlocks, numBlocks)

		// Document current behavior:
		// - Large batch fetching with worker pool architecture
		// - Efficient memory usage with controlled worker concurrency
		// - Blocks are delivered in strict order despite parallel processing
		t.Logf("Worker pool architecture efficiently processes %d blocks", numBlocks)
		t.Logf("Memory usage is controlled with fixed worker pool size")
	})
}

// TestFetchBlocksConcurrently_EdgeCases tests edge cases and error conditions
func TestFetchBlocksConcurrently_EdgeCases(t *testing.T) {
	t.Run("Nil Headers", func(t *testing.T) {
		suite := NewCatchupTestSuite(t)
		defer suite.Cleanup()

		blocks := testhelpers.CreateTestBlockChain(t, 1)
		targetBlock := blocks[0]
		var headers []*model.BlockHeader // nil slice

		var size atomic.Int64
		size.Store(0)
		validateBlocksChan := make(chan *model.Block, 1)

		catchupCtx := &CatchupContext{
			blockUpTo:    targetBlock,
			baseURL:      "http://test-peer",
			blockHeaders: headers,
			commonAncestorMeta: &model.BlockHeaderMeta{
				Height: 0,
			},
		}

		// Call fetchBlocksConcurrently
		err := suite.Server.fetchBlocksConcurrently(suite.Ctx, catchupCtx, validateBlocksChan, &size)
		require.NoError(t, err)

		// Wait for goroutines to complete
		// err = errorGroup.Wait()
		// require.NoError(t, err)
	})

	t.Run("Fetch Blocks Batch", func(t *testing.T) {
		suite := NewCatchupTestSuite(t)
		defer suite.Cleanup()

		// Create test block
		blocks := testhelpers.CreateTestBlockChain(t, 2)
		targetHash := blocks[1].Header.Hash()

		// Set up HTTP mock
		httpmock.ActivateNonDefault(util.HTTPClient())
		defer httpmock.DeactivateAndReset()

		httpmock.RegisterResponder(
			"GET",
			fmt.Sprintf("http://test-peer/blocks/%s?n=1", blocks[1].Header.Hash().String()),
			httpmock.NewBytesResponder(200, func() []byte {
				blockBytes, _ := blocks[1].Bytes()
				return blockBytes
			}()),
		)

		// Call fetchBlocksBatch
		fetchedBlocks, err := suite.Server.fetchBlocksBatch(suite.Ctx, targetHash, 1, "test-peer-id", "http://test-peer")
		require.NoError(t, err)
		require.Len(t, fetchedBlocks, 1)
		assert.Equal(t, targetHash, fetchedBlocks[0].Header.Hash())
	})

	t.Run("Fetch Single Block", func(t *testing.T) {
		suite := NewCatchupTestSuite(t)
		defer suite.Cleanup()

		// Create test block
		blocks := testhelpers.CreateTestBlockChain(t, 2)
		targetHash := blocks[1].Header.Hash()

		// Set up HTTP mock
		httpmock.ActivateNonDefault(util.HTTPClient())
		defer httpmock.DeactivateAndReset()

		httpmock.RegisterResponder(
			"GET",
			fmt.Sprintf("http://test-peer/block/%s", targetHash.String()),
			httpmock.NewBytesResponder(200, func() []byte {
				blockBytes, _ := blocks[1].Bytes()
				return blockBytes
			}()),
		)

		// Call fetchSingleBlock
		fetchedBlock, err := suite.Server.fetchSingleBlock(suite.Ctx, targetHash, "12D3KooWL1NF6fdTJ9cucEuwvuX8V8KtpJZZnUE4umdLBuK15eUZ", "http://test-peer")
		require.NoError(t, err)
		require.NotNil(t, fetchedBlock)
		assert.Equal(t, targetHash, fetchedBlock.Header.Hash())
	})
}

// TestFetchBlocksBatch_CurrentBehavior documents the current behavior of fetchBlocksBatch function
func TestFetchBlocksBatch_CurrentBehavior(t *testing.T) {
	t.Run("Single Block Fetch", func(t *testing.T) {
		suite := NewCatchupTestSuite(t)
		defer suite.Cleanup()

		// Create test block
		blocks := testhelpers.CreateTestBlockChain(t, 2)
		targetHash := blocks[1].Header.Hash()

		// Set up HTTP mock
		httpmock.ActivateNonDefault(util.HTTPClient())
		defer httpmock.DeactivateAndReset()

		httpmock.RegisterResponder(
			"GET",
			fmt.Sprintf("http://test-peer/blocks/%s?n=1", blocks[1].Header.Hash().String()),
			httpmock.NewBytesResponder(200, func() []byte {
				blockBytes, _ := blocks[1].Bytes()
				return blockBytes
			}()),
		)

		// Call fetchBlocksBatch
		fetchedBlocks, err := suite.Server.fetchBlocksBatch(suite.Ctx, targetHash, 1, "test-peer-id", "http://test-peer")
		require.NoError(t, err)
		require.Len(t, fetchedBlocks, 1)
		assert.Equal(t, targetHash, fetchedBlocks[0].Header.Hash())
	})

	t.Run("Multiple Blocks Fetch", func(t *testing.T) {
		suite := NewCatchupTestSuite(t)
		defer suite.Cleanup()

		// Create test blocks
		blocks := testhelpers.CreateTestBlockChain(t, 4)
		targetHash := blocks[1].Header.Hash()

		// Set up HTTP mock to return multiple blocks
		httpmock.ActivateNonDefault(util.HTTPClient())
		defer httpmock.DeactivateAndReset()

		httpmock.RegisterResponder(
			"GET",
			fmt.Sprintf("http://test-peer/blocks/%s?n=3", targetHash.String()),
			httpmock.NewBytesResponder(200, func() []byte {
				// Concatenate multiple block bytes
				var allBytes []byte
				for i := 1; i <= 3; i++ {
					blockBytes, _ := blocks[i].Bytes()
					allBytes = append(allBytes, blockBytes...)
				}
				return allBytes
			}()),
		)

		// Call fetchBlocksBatch
		fetchedBlocks, err := suite.Server.fetchBlocksBatch(suite.Ctx, targetHash, 3, "test-peer-id", "http://test-peer")
		require.NoError(t, err)
		require.Len(t, fetchedBlocks, 3)

		// Verify blocks are returned in order
		for i, block := range fetchedBlocks {
			assert.Equal(t, blocks[i+1].Header.Hash(), block.Header.Hash())
		}
	})

	t.Run("Network Error Handling", func(t *testing.T) {
		suite := NewCatchupTestSuite(t)
		defer suite.Cleanup()

		blocks := testhelpers.CreateTestBlockChain(t, 2)
		targetHash := blocks[1].Header.Hash()

		httpmock.RegisterResponder(
			"GET",
			fmt.Sprintf("http://test-peer/blocks/%s?n=1", targetHash.String()),
			httpmock.NewErrorResponder(errors.NewNetworkError("network timeout")),
		)

		// Set up HTTP mock to return error
		httpmock.ActivateNonDefault(util.HTTPClient())
		defer httpmock.DeactivateAndReset()

		// Call fetchBlocksBatch - should return error
		fetchedBlocks, err := suite.Server.fetchBlocksBatch(suite.Ctx, targetHash, 1, "test-peer-id", "http://test-peer")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "failed to get blocks from peer")
		require.Nil(t, fetchedBlocks)
	})
}

// TestFetchSingleBlock_CurrentBehavior documents the current behavior of fetchSingleBlock function
func TestFetchSingleBlock_CurrentBehavior(t *testing.T) {
	t.Run("Successful Fetch", func(t *testing.T) {
		suite := NewCatchupTestSuite(t)
		defer suite.Cleanup()

		// Create test block
		blocks := testhelpers.CreateTestBlockChain(t, 2)
		targetHash := blocks[1].Header.Hash()

		// Set up HTTP mock
		httpmock.ActivateNonDefault(util.HTTPClient())
		defer httpmock.DeactivateAndReset()

		httpmock.RegisterResponder(
			"GET",
			fmt.Sprintf("http://test-peer/block/%s", targetHash.String()),
			httpmock.NewBytesResponder(200, func() []byte {
				blockBytes, _ := blocks[1].Bytes()
				return blockBytes
			}()),
		)

		// Call fetchSingleBlock
		fetchedBlock, err := suite.Server.fetchSingleBlock(suite.Ctx, targetHash, "12D3KooWL1NF6fdTJ9cucEuwvuX8V8KtpJZZnUE4umdLBuK15eUZ", "http://test-peer")
		require.NoError(t, err)
		require.NotNil(t, fetchedBlock)
		assert.Equal(t, targetHash, fetchedBlock.Header.Hash())
	})

	t.Run("Network Error Handling", func(t *testing.T) {
		suite := NewCatchupTestSuite(t)
		defer suite.Cleanup()

		blocks := testhelpers.CreateTestBlockChain(t, 2)
		targetHash := blocks[1].Header.Hash()

		// Set up HTTP mock to return error
		httpmock.ActivateNonDefault(util.HTTPClient())
		defer httpmock.DeactivateAndReset()

		httpmock.RegisterResponder(
			"GET",
			fmt.Sprintf("http://test-peer/block/%s", targetHash.String()),
			httpmock.NewErrorResponder(errors.NewNetworkError("connection refused")),
		)

		// Call fetchSingleBlock - should return error
		fetchedBlock, err := suite.Server.fetchSingleBlock(suite.Ctx, targetHash, "12D3KooWL1NF6fdTJ9cucEuwvuX8V8KtpJZZnUE4umdLBuK15eUZ", "http://test-peer")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "failed to get block from peer")
		require.Nil(t, fetchedBlock)
	})

	t.Run("Invalid Block Data", func(t *testing.T) {
		suite := NewCatchupTestSuite(t)
		defer suite.Cleanup()

		blocks := testhelpers.CreateTestBlockChain(t, 2)
		targetHash := blocks[1].Header.Hash()

		// Set up HTTP mock to return invalid data
		httpmock.ActivateNonDefault(util.HTTPClient())
		defer httpmock.DeactivateAndReset()

		httpmock.RegisterResponder(
			"GET",
			fmt.Sprintf("http://test-peer/block/%s", targetHash.String()),
			httpmock.NewBytesResponder(200, []byte("invalid block data")),
		)

		// Call fetchSingleBlock - should return error
		fetchedBlock, err := suite.Server.fetchSingleBlock(suite.Ctx, targetHash, "12D3KooWL1NF6fdTJ9cucEuwvuX8V8KtpJZZnUE4umdLBuK15eUZ", "http://test-peer")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "failed to create block from bytes")
		require.Nil(t, fetchedBlock)
	})
}

// Phase 2: Tests for optimized batch fetching and ordered delivery
func TestFetchBlocksConcurrently_OptimizedBehavior(t *testing.T) {
	t.Run("Ordered_Delivery_With_Batching", func(t *testing.T) {
		suite := NewCatchupTestSuite(t)
		defer suite.Cleanup()

		// Create test blockchain with 10 blocks
		blocks := testhelpers.CreateTestBlockChain(t, 10)

		// Create headers for blocks 1-5 (skip genesis)
		var blockHeaders []*model.BlockHeader
		for i := 1; i <= 5; i++ {
			blockHeaders = append(blockHeaders, blocks[i].Header)
		}

		// Mock HTTP responses - simulate batch fetching
		httpmock.ActivateNonDefault(util.HTTPClient())
		defer httpmock.DeactivateAndReset()

		// The optimized function uses batch size of 5, so it will make 1 request for all 5 blocks
		// The request will be for the LAST block in the batch (blocks[5]), and blocks should be returned in reverse order
		batchData := bytes.Buffer{}
		for i := 5; i >= 1; i-- { // Reverse order: 5, 4, 3, 2, 1
			blockBytes, _ := blocks[i].Bytes()
			batchData.Write(blockBytes)
		}
		httpmock.RegisterResponder("GET", fmt.Sprintf("http://peer/blocks/%s?n=5", blocks[5].Hash().String()),
			httpmock.NewBytesResponder(200, batchData.Bytes()))

		// Test optimized fetching
		ctx := context.Background()
		validateBlocksChan := make(chan *model.Block, 10)
		size := &atomic.Int64{}

		catchupCtx := &CatchupContext{
			blockUpTo:    blocks[5],
			baseURL:      "http://peer",
			blockHeaders: blockHeaders,
			commonAncestorMeta: &model.BlockHeaderMeta{
				Height: 0,
			},
		}

		err := suite.Server.fetchBlocksConcurrently(ctx, catchupCtx, validateBlocksChan, size)
		assert.NoError(t, err)

		// Wait for completion
		// err = errorGroup.Wait()
		// assert.NoError(t, err)

		// Collect blocks from channel with timeout
		var receivedBlocks []*model.Block
		for i := 0; i < 5; i++ {
			select {
			case block := <-validateBlocksChan:
				receivedBlocks = append(receivedBlocks, block)
			case <-time.After(time.Second):
				t.Fatalf("Timeout waiting for block %d/%d", i+1, 5)
			}
		}

		// Verify all blocks received
		assert.Len(t, receivedBlocks, 5)

		// Verify blocks are in correct order
		for i, block := range receivedBlocks {
			expectedHash := blocks[i+1].Hash().String()
			actualHash := block.Hash().String()
			assert.Equal(t, expectedHash, actualHash, "Block %d should be in correct order", i+1)
		}

		// Verify we made 1 batch request instead of 5 individual requests
		assert.Equal(t, 1, httpmock.GetTotalCallCount(), "Should make 1 batch request instead of 5 individual requests")
	})

	t.Run("Efficient_Batching_Strategy", func(t *testing.T) {
		suite := NewCatchupTestSuite(t)
		defer suite.Cleanup()

		// Create test blockchain with 20 blocks
		blocks := testhelpers.CreateTestBlockChain(t, 20)

		// Create headers for blocks 1-15 (skip genesis)
		var blockHeaders []*model.BlockHeader
		for i := 1; i <= 15; i++ {
			blockHeaders = append(blockHeaders, blocks[i].Header)
		}

		httpmock.ActivateNonDefault(util.HTTPClient())
		defer httpmock.DeactivateAndReset()

		// Mock batch responses using regex pattern to match any batch request
		expectedBatches := 1

		httpmock.RegisterResponder("GET", `=~^http://peer/blocks/[a-f0-9]+\?n=\d+$`,
			func(req *http.Request) (*http.Response, error) {
				// Parse the batch size from the URL
				n := req.URL.Query().Get("n")
				// Accept the actual batch size used by implementation (15 for 15 blocks)
				if n != "15" {
					return httpmock.NewStringResponse(400, fmt.Sprintf("Invalid batch size: expected 15, got %s", n)), nil
				}

				// Create mock response with all 15 blocks in one batch - blocks returned in reverse order
				batchData := bytes.Buffer{}
				for i := 15; i >= 1; i-- { // Reverse order
					blockBytes, _ := blocks[i].Bytes()
					batchData.Write(blockBytes)
				}

				return httpmock.NewBytesResponse(200, batchData.Bytes()), nil
			})

		// Test optimized fetching
		ctx := context.Background()
		validateBlocksChan := make(chan *model.Block, 20)
		size := &atomic.Int64{}

		catchupCtx := &CatchupContext{
			blockUpTo:    blocks[15],
			baseURL:      "http://peer",
			blockHeaders: blockHeaders,
			commonAncestorMeta: &model.BlockHeaderMeta{
				Height: 0,
			},
		}

		err := suite.Server.fetchBlocksConcurrently(ctx, catchupCtx, validateBlocksChan, size)
		assert.NoError(t, err)

		// Wait for completion
		// err = errorGroup.Wait()
		// assert.NoError(t, err)

		// Collect blocks from channel with timeout
		var receivedBlocks []*model.Block
		for i := 0; i < 15; i++ {
			select {
			case block := <-validateBlocksChan:
				receivedBlocks = append(receivedBlocks, block)
			case <-time.After(time.Second):
				t.Fatalf("Timeout waiting for block %d/%d", i+1, 15)
			}
		}

		assert.Len(t, receivedBlocks, 15)

		// Verify blocks are in correct order
		for i, block := range receivedBlocks {
			expectedHash := blocks[i+1].Hash().String()
			actualHash := block.Hash().String()
			assert.Equal(t, expectedHash, actualHash, "Block %d should be in correct order", i+1)
		}

		// Verify we made 1 batch request instead of 15 individual requests
		assert.Equal(t, expectedBatches, httpmock.GetTotalCallCount(), "Should make 1 batch request for optimal efficiency")
	})

	t.Run("Large_Batch_Fetching", func(t *testing.T) {
		suite := NewCatchupTestSuite(t)
		defer suite.Cleanup()

		// Create test blockchain with 100 blocks
		blocks := testhelpers.CreateTestBlockChain(t, 101) // +1 for genesis

		// Create headers for blocks 1-100 (skip genesis)
		var blockHeaders []*model.BlockHeader
		for i := 1; i <= 100; i++ {
			blockHeaders = append(blockHeaders, blocks[i].Header)
		}

		// Mock HTTP responses for large batch requests
		httpmock.ActivateNonDefault(util.HTTPClient())
		defer httpmock.DeactivateAndReset()

		// Mock single large batch request (100 blocks) - blocks returned in reverse order
		batchData := bytes.Buffer{}
		for i := 100; i >= 1; i-- { // Reverse order
			blockBytes, _ := blocks[i].Bytes()
			batchData.Write(blockBytes)
		}
		httpmock.RegisterResponder("GET", fmt.Sprintf("http://peer/blocks/%s?n=100", blocks[100].Hash().String()),
			httpmock.NewBytesResponder(200, batchData.Bytes()))

		// Test high-performance fetching
		ctx := context.Background()
		validateBlocksChan := make(chan *model.Block, 200)
		size := &atomic.Int64{}

		catchupCtx := &CatchupContext{
			blockUpTo:    blocks[100],
			baseURL:      "http://peer",
			blockHeaders: blockHeaders,
			commonAncestorMeta: &model.BlockHeaderMeta{
				Height: 0,
			},
		}

		err := suite.Server.fetchBlocksConcurrently(ctx, catchupCtx, validateBlocksChan, size)
		assert.NoError(t, err)

		// Wait for completion
		// err = errorGroup.Wait()
		// assert.NoError(t, err)

		// Collect blocks from channel with timeout
		var receivedBlocks []*model.Block
		for i := 0; i < 100; i++ {
			select {
			case block := <-validateBlocksChan:
				receivedBlocks = append(receivedBlocks, block)
			case <-time.After(time.Second):
				t.Fatalf("Timeout waiting for block %d/%d", i+1, 100)
			}
		}

		// Verify all blocks received
		assert.Len(t, receivedBlocks, 100)

		// Verify blocks are in strict order (critical requirement)
		for i, block := range receivedBlocks {
			expectedHash := blocks[i+1].Hash().String()
			actualHash := block.Hash().String()
			assert.Equal(t, expectedHash, actualHash, "Block %d should be in strict chain order", i+1)
		}

		// Verify we made only 1 HTTP request for 100 blocks (maximum efficiency)
		assert.Equal(t, 1, httpmock.GetTotalCallCount(), "Should make 1 large batch request for 100 blocks")
	})

	t.Run("Multiple_Large_Batches_250_Blocks", func(t *testing.T) {
		suite := NewCatchupTestSuite(t)
		defer suite.Cleanup()

		// Create test blockchain with 250 blocks
		blocks := testhelpers.CreateTestBlockChain(t, 251) // +1 for genesis

		// Create headers for blocks 1-250 (skip genesis)
		var blockHeaders []*model.BlockHeader
		for i := 1; i <= 250; i++ {
			blockHeaders = append(blockHeaders, blocks[i].Header)
		}

		httpmock.ActivateNonDefault(util.HTTPClient())
		defer httpmock.DeactivateAndReset()

		// Mock 3 large batch requests (100, 100, 50)
		// Since the server requests blocks in batches and expects them in reverse order,
		// we need to mock the responses properly
		batches := []struct{ start, end, count int }{
			{1, 100, 100},   // Batch 1: blocks 1-100
			{101, 200, 100}, // Batch 2: blocks 101-200
			{201, 250, 50},  // Batch 3: blocks 201-250
		}

		for _, batch := range batches {
			batchData := bytes.Buffer{}
			// Return blocks in reverse order within the batch
			for i := batch.end; i >= batch.start; i-- {
				blockBytes, _ := blocks[i].Bytes()
				batchData.Write(blockBytes)
			}
			// Request uses the LAST block hash in the batch
			httpmock.RegisterResponder("GET", fmt.Sprintf("http://peer/blocks/%s?n=%d", blocks[batch.end].Hash().String(), batch.count),
				httpmock.NewBytesResponder(200, batchData.Bytes()))
		}

		// Test high-performance fetching
		ctx := context.Background()
		validateBlocksChan := make(chan *model.Block, 300)
		size := &atomic.Int64{}

		catchupCtx := &CatchupContext{
			blockUpTo:    blocks[250],
			baseURL:      "http://peer",
			blockHeaders: blockHeaders,
			commonAncestorMeta: &model.BlockHeaderMeta{
				Height: 0,
			},
		}

		err := suite.Server.fetchBlocksConcurrently(ctx, catchupCtx, validateBlocksChan, size)
		assert.NoError(t, err)

		// Wait for completion
		// err = errorGroup.Wait()
		// assert.NoError(t, err)

		// Collect blocks from channel with timeout
		var receivedBlocks []*model.Block
		for i := 0; i < 250; i++ {
			select {
			case block := <-validateBlocksChan:
				receivedBlocks = append(receivedBlocks, block)
			case <-time.After(time.Second):
				t.Fatalf("Timeout waiting for block %d/%d", i+1, 250)
			}
		}

		// Verify all blocks received
		assert.Len(t, receivedBlocks, 250)

		// Verify strict ordering (critical for validation pipeline)
		for i, block := range receivedBlocks {
			expectedHash := blocks[i+1].Hash().String()
			actualHash := block.Hash().String()
			assert.Equal(t, expectedHash, actualHash, "Block %d should be in strict chain order", i+1)
		}

		// Verify efficient batching - 3 large requests instead of 250 individual requests
		assert.Equal(t, 3, httpmock.GetTotalCallCount(), "Should make 3 large batch requests for maximum efficiency")
	})

	t.Run("Worker_Pool_Parallel_Processing", func(t *testing.T) {
		suite := NewCatchupTestSuite(t)
		defer suite.Cleanup()

		// Create test blockchain with 50 blocks
		blocks := testhelpers.CreateTestBlockChain(t, 51) // +1 for genesis

		// Create headers for blocks 1-50 (skip genesis)
		var blockHeaders []*model.BlockHeader
		for i := 1; i <= 50; i++ {
			blockHeaders = append(blockHeaders, blocks[i].Header)
		}

		httpmock.ActivateNonDefault(util.HTTPClient())
		defer httpmock.DeactivateAndReset()

		// Mock batch request with artificial delay to test parallel processing
		batchData := bytes.Buffer{}
		// Return blocks in reverse order (newest first)
		for i := 50; i >= 1; i-- {
			blockBytes, _ := blocks[i].Bytes()
			batchData.Write(blockBytes)
		}

		// Add delay to simulate network latency and verify parallel processing
		// Request uses last block's hash since we fetch in reverse
		httpmock.RegisterResponder("GET", fmt.Sprintf("http://peer/blocks/%s?n=50", blocks[50].Hash().String()),
			func(req *http.Request) (*http.Response, error) {
				time.Sleep(100 * time.Millisecond) // Simulate network delay
				return httpmock.NewBytesResponse(200, batchData.Bytes()), nil
			})

		// Measure processing time to verify parallel worker efficiency
		startTime := time.Now()

		// Test high-performance fetching
		ctx := context.Background()
		validateBlocksChan := make(chan *model.Block, 100)
		size := &atomic.Int64{}

		catchupCtx := &CatchupContext{
			blockUpTo:    blocks[50],
			baseURL:      "http://peer",
			blockHeaders: blockHeaders,
			commonAncestorMeta: &model.BlockHeaderMeta{
				Height: 0,
			},
		}

		err := suite.Server.fetchBlocksConcurrently(ctx, catchupCtx, validateBlocksChan, size)
		assert.NoError(t, err)

		// Wait for completion
		// err = errorGroup.Wait()
		// assert.NoError(t, err)

		processingTime := time.Since(startTime)

		// Collect blocks from channel with timeout
		var receivedBlocks []*model.Block
		for i := 0; i < 50; i++ {
			select {
			case block := <-validateBlocksChan:
				receivedBlocks = append(receivedBlocks, block)
			case <-time.After(time.Second):
				t.Fatalf("Timeout waiting for block %d/%d", i+1, 50)
			}
		}

		// Verify all blocks received in order
		assert.Len(t, receivedBlocks, 50)
		for i, block := range receivedBlocks {
			expectedHash := blocks[i+1].Hash().String()
			actualHash := block.Hash().String()
			assert.Equal(t, expectedHash, actualHash, "Block %d should be in strict chain order", i+1)
		}

		// Verify parallel processing efficiency
		// With 8 workers and 10ms subtree processing per block, 50 blocks should complete much faster than sequential
		// Sequential: 50 * 10ms = 500ms, Parallel with 8 workers: ~100ms + network delay
		assert.Less(t, processingTime, 300*time.Millisecond, "Parallel worker processing should be significantly faster than sequential")

		t.Logf("Processed 50 blocks with worker pool in %v (demonstrates parallel efficiency)", processingTime)
	})

	t.Run("Error_Handling_In_Worker_Pipeline", func(t *testing.T) {
		suite := NewCatchupTestSuite(t)
		defer suite.Cleanup()

		// Create test blockchain
		blocks := testhelpers.CreateTestBlockChain(t, 6)

		// Create headers for blocks 1-5
		var blockHeaders []*model.BlockHeader
		for i := 1; i <= 5; i++ {
			blockHeaders = append(blockHeaders, blocks[i].Header)
		}

		httpmock.ActivateNonDefault(util.HTTPClient())
		defer httpmock.DeactivateAndReset()

		// Mock HTTP error response
		// Request uses last block's hash since we fetch in reverse
		httpmock.RegisterResponder("GET", fmt.Sprintf("http://peer/blocks/%s?n=5", blocks[5].Hash().String()),
			httpmock.NewErrorResponder(errors.NewNetworkError("network error")))

		// Test error handling
		ctx := context.Background()
		validateBlocksChan := make(chan *model.Block, 10)
		size := &atomic.Int64{}

		catchupCtx := &CatchupContext{
			blockUpTo:    blocks[5],
			baseURL:      "http://peer",
			blockHeaders: blockHeaders,
			commonAncestorMeta: &model.BlockHeaderMeta{
				Height: 0,
			},
		}

		err := suite.Server.fetchBlocksConcurrently(ctx, catchupCtx, validateBlocksChan, size)

		// The function should return an error when HTTP request fails
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "failed to fetch batch")

		// Channel should be closed with no blocks - don't try to read since error occurred
		// The channel will be closed by orderedDelivery but may be empty due to error
		select {
		case <-validateBlocksChan:
			// May receive some blocks before error, that's ok
		default:
			// Or may receive no blocks, that's also ok
		}
	})
}

// Phase 3: Tests for high-performance worker pool architecture
func TestFetchBlocksConcurrently_WorkerPoolArchitecture(t *testing.T) {
	t.Run("Large_Batch_Processing_100_Blocks", func(t *testing.T) {
		suite := NewCatchupTestSuite(t)
		defer suite.Cleanup()

		// Create test blockchain with 100 blocks
		blocks := testhelpers.CreateTestBlockChain(t, 101) // +1 for genesis

		// Create headers for blocks 1-100 (skip genesis)
		var blockHeaders []*model.BlockHeader
		for i := 1; i <= 100; i++ {
			blockHeaders = append(blockHeaders, blocks[i].Header)
		}

		// Mock HTTP responses for large batch requests
		httpmock.ActivateNonDefault(util.HTTPClient())
		defer httpmock.DeactivateAndReset()

		// Mock single large batch request (100 blocks) - blocks returned in reverse order
		batchData := bytes.Buffer{}
		for i := 100; i >= 1; i-- { // Reverse order
			blockBytes, _ := blocks[i].Bytes()
			batchData.Write(blockBytes)
		}
		httpmock.RegisterResponder("GET", fmt.Sprintf("http://peer/blocks/%s?n=100", blocks[100].Hash().String()),
			httpmock.NewBytesResponder(200, batchData.Bytes()))

		// Test high-performance fetching
		ctx := context.Background()
		validateBlocksChan := make(chan *model.Block, 200)
		size := &atomic.Int64{}

		catchupCtx := &CatchupContext{
			blockUpTo:    blocks[100],
			baseURL:      "http://peer",
			blockHeaders: blockHeaders,
			commonAncestorMeta: &model.BlockHeaderMeta{
				Height: 0,
			},
		}

		err := suite.Server.fetchBlocksConcurrently(ctx, catchupCtx, validateBlocksChan, size)
		assert.NoError(t, err)

		// Wait for completion
		// err = errorGroup.Wait()
		// assert.NoError(t, err)

		// Collect blocks from channel with timeout
		var receivedBlocks []*model.Block
		for i := 0; i < 100; i++ {
			select {
			case block := <-validateBlocksChan:
				receivedBlocks = append(receivedBlocks, block)
			case <-time.After(time.Second):
				t.Fatalf("Timeout waiting for block %d/%d", i+1, 100)
			}
		}

		// Verify all blocks received
		assert.Len(t, receivedBlocks, 100)

		// Verify blocks are in strict order (critical requirement)
		for i, block := range receivedBlocks {
			expectedHash := blocks[i+1].Hash().String()
			actualHash := block.Hash().String()
			assert.Equal(t, expectedHash, actualHash, "Block %d should be in strict chain order", i+1)
		}

		// Verify we made only 1 HTTP request for 100 blocks (maximum efficiency)
		assert.Equal(t, 1, httpmock.GetTotalCallCount(), "Should make 1 large batch request for 100 blocks")
	})
}

// TestSubtreeFunctions tests all subtree-related functions for complete coverage
func TestSubtreeFunctions(t *testing.T) {
	txs := transactions.CreateTestTransactionChainWithCount(t, 5)

	subtree, err := subtreepkg.NewIncompleteTreeByLeafCount(4)
	assert.NoError(t, err)

	require.NoError(t, subtree.AddCoinbaseNode())
	require.NoError(t, subtree.AddNode(*txs[1].TxIDChainHash(), 1, 11))
	require.NoError(t, subtree.AddNode(*txs[2].TxIDChainHash(), 2, 12))
	require.NoError(t, subtree.AddNode(*txs[3].TxIDChainHash(), 3, 13))

	subtreeHash := subtree.RootHash()

	subtreeData := subtreepkg.NewSubtreeData(subtree)
	require.NoError(t, subtreeData.AddTx(txs[0], 0))
	require.NoError(t, subtreeData.AddTx(txs[1], 1))
	require.NoError(t, subtreeData.AddTx(txs[2], 2))
	require.NoError(t, subtreeData.AddTx(txs[3], 3))

	subtreeDataBytes, err := subtreeData.Serialize()
	require.NoError(t, err)

	t.Run("fetchSubtreeFromPeer_Success", func(t *testing.T) {
		suite := NewCatchupTestSuite(t)
		defer suite.Cleanup()

		subtreeHash := &chainhash.Hash{0x01, 0x02, 0x03}
		expectedData := []byte("mock subtree data")

		httpmock.ActivateNonDefault(util.HTTPClient())
		defer httpmock.DeactivateAndReset()

		httpmock.RegisterResponder("GET",
			fmt.Sprintf("http://test-peer/subtree/%s", subtreeHash.String()),
			httpmock.NewBytesResponder(200, expectedData))

		result, err := suite.Server.fetchSubtreeFromPeer(suite.Ctx, subtreeHash, "test-peer-id", "http://test-peer")
		assert.NoError(t, err)
		assert.Equal(t, expectedData, result)
	})

	t.Run("fetchSubtreeFromPeer_HTTPError", func(t *testing.T) {
		suite := NewCatchupTestSuite(t)
		defer suite.Cleanup()

		subtreeHash := &chainhash.Hash{0x01, 0x02, 0x03}

		httpmock.ActivateNonDefault(util.HTTPClient())
		defer httpmock.DeactivateAndReset()

		httpmock.RegisterResponder("GET",
			fmt.Sprintf("http://test-peer/subtree/%s", subtreeHash.String()),
			httpmock.NewStringResponder(500, "Internal Server Error"))

		result, err := suite.Server.fetchSubtreeFromPeer(suite.Ctx, subtreeHash, "test-peer-id", "http://test-peer")
		assert.Error(t, err)
		assert.Nil(t, result)
		assert.Contains(t, err.Error(), "failed to fetch subtree")
	})

	t.Run("fetchSubtreeFromPeer_EmptyResponse", func(t *testing.T) {
		suite := NewCatchupTestSuite(t)
		defer suite.Cleanup()

		subtreeHash := &chainhash.Hash{0x01, 0x02, 0x03}

		httpmock.ActivateNonDefault(util.HTTPClient())
		defer httpmock.DeactivateAndReset()

		httpmock.RegisterResponder("GET",
			fmt.Sprintf("http://test-peer/subtree/%s", subtreeHash.String()),
			httpmock.NewBytesResponder(200, []byte{}))

		result, err := suite.Server.fetchSubtreeFromPeer(suite.Ctx, subtreeHash, "test-peer-id", "http://test-peer")
		assert.Error(t, err)
		assert.Nil(t, result)
		assert.Contains(t, err.Error(), "empty subtree received")
	})

	t.Run("fetchSubtreeDataFromPeer_Success", func(t *testing.T) {
		suite := NewCatchupTestSuite(t)
		defer suite.Cleanup()

		subtreeHash := &chainhash.Hash{0x01, 0x02, 0x03}
		expectedData := []byte("mock subtree data content")

		httpmock.ActivateNonDefault(util.HTTPClient())
		defer httpmock.DeactivateAndReset()

		httpmock.RegisterResponder("GET",
			fmt.Sprintf("http://test-peer/subtree_data/%s", subtreeHash.String()),
			httpmock.NewBytesResponder(200, expectedData))

		reader, err := suite.Server.fetchSubtreeDataFromPeer(suite.Ctx, subtreeHash, "test-peer-id", "http://test-peer")
		assert.NoError(t, err)
		assert.NotNil(t, reader)
		defer reader.Close()

		// Read the data from the reader
		data, err := io.ReadAll(reader)
		assert.NoError(t, err)
		assert.Equal(t, expectedData, data)
	})

	t.Run("fetchSubtreeDataFromPeer_HTTPError", func(t *testing.T) {
		suite := NewCatchupTestSuite(t)
		defer suite.Cleanup()

		subtreeHash := &chainhash.Hash{0x01, 0x02, 0x03}

		httpmock.ActivateNonDefault(util.HTTPClient())
		defer httpmock.DeactivateAndReset()

		httpmock.RegisterResponder("GET",
			fmt.Sprintf("http://test-peer/subtree_data/%s", subtreeHash.String()),
			httpmock.NewStringResponder(404, "Not Found"))

		result, err := suite.Server.fetchSubtreeDataFromPeer(suite.Ctx, subtreeHash, "test-peer-id", "http://test-peer")
		assert.Error(t, err)
		assert.Nil(t, result)
		assert.Contains(t, err.Error(), "failed to fetch subtree data from")
	})

	t.Run("fetchSubtreeDataFromPeer_EmptyResponse", func(t *testing.T) {
		suite := NewCatchupTestSuite(t)
		defer suite.Cleanup()

		subtreeHash := &chainhash.Hash{0x01, 0x02, 0x03}

		httpmock.ActivateNonDefault(util.HTTPClient())
		defer httpmock.DeactivateAndReset()

		httpmock.RegisterResponder("GET",
			fmt.Sprintf("http://test-peer/subtree_data/%s", subtreeHash.String()),
			httpmock.NewBytesResponder(200, []byte{}))

		reader, err := suite.Server.fetchSubtreeDataFromPeer(suite.Ctx, subtreeHash, "test-peer-id", "http://test-peer")
		// Empty response is not an error for the fetcher - it just returns an empty reader
		assert.NoError(t, err)
		assert.NotNil(t, reader)
		defer reader.Close()

		// Read the data from the reader - should be empty
		data, err := io.ReadAll(reader)
		assert.NoError(t, err)
		assert.Empty(t, data)
	})

	t.Run("fetchAndStoreSubtreeAndSubtreeData_Success", func(t *testing.T) {
		suite := NewCatchupTestSuite(t)
		defer suite.Cleanup()

		suite.Server.subtreeStore = memory.New()

		httpmock.ActivateNonDefault(util.HTTPClient())
		defer httpmock.DeactivateAndReset()

		// Create node hashes for the subtree endpoint (raw hashes, not serialized subtree)
		var nodeHashes []byte
		// First is coinbase placeholder
		nodeHashes = append(nodeHashes, subtreepkg.CoinbasePlaceholderHashValue[:]...)
		// Then the transaction hashes
		nodeHashes = append(nodeHashes, txs[1].TxIDChainHash()[:]...)
		nodeHashes = append(nodeHashes, txs[2].TxIDChainHash()[:]...)
		nodeHashes = append(nodeHashes, txs[3].TxIDChainHash()[:]...)

		// Mock both subtree and subtree_data endpoints
		httpmock.RegisterResponder("GET",
			fmt.Sprintf("http://test-peer/subtree/%s", subtreeHash.String()),
			httpmock.NewBytesResponder(200, nodeHashes))

		httpmock.RegisterResponder("GET",
			fmt.Sprintf("http://test-peer/subtree_data/%s", subtreeHash.String()),
			httpmock.NewBytesResponder(200, subtreeDataBytes))

		// Create a dummy block for the test
		testBlock := &model.Block{
			Height: 100,
		}
		err = suite.Server.fetchAndStoreSubtreeAndSubtreeData(suite.Ctx, testBlock, subtreeHash, "12D3KooWL1NF6fdTJ9cucEuwvuX8V8KtpJZZnUE4umdLBuK15eUZ", "http://test-peer")
		assert.NoError(t, err)

		// Verify both were stored in subtreeStore
		storedSubtreeBytes, err := suite.Server.subtreeStore.Get(suite.Ctx, subtreeHash[:], fileformat.FileTypeSubtreeToCheck)
		assert.NoError(t, err)

		subtreeFromStore := &subtreepkg.Subtree{}
		err = subtreeFromStore.Deserialize(storedSubtreeBytes)
		assert.NoError(t, err)
		assert.Equal(t, subtreeFromStore.RootHash(), subtreeHash)

		storedSubtreeDataBytes, err := suite.Server.subtreeStore.Get(suite.Ctx, subtreeHash[:], fileformat.FileTypeSubtreeData)
		assert.NoError(t, err)

		storedSubtreeData, err := subtreepkg.NewSubtreeDataFromBytes(subtree, storedSubtreeDataBytes)
		assert.NoError(t, err)

		// check that all the transactions are still in there
		assert.Equal(t, 4, len(storedSubtreeData.Txs))
		assert.Nil(t, storedSubtreeData.Txs[0]) // coinbase tx is not stored in subtree data
		assert.Equal(t, txs[1].TxIDChainHash(), storedSubtreeData.Txs[1].TxIDChainHash())
		assert.Equal(t, txs[2].TxIDChainHash(), storedSubtreeData.Txs[2].TxIDChainHash())
		assert.Equal(t, txs[3].TxIDChainHash(), storedSubtreeData.Txs[3].TxIDChainHash())
	})

	t.Run("fetchAndStoreSubtreeAndSubtreeData_SubtreeError", func(t *testing.T) {
		suite := NewCatchupTestSuite(t)
		defer suite.Cleanup()

		suite.Server.subtreeStore = memory.New()

		subtreeHash := &chainhash.Hash{0x01, 0x02, 0x03}

		httpmock.ActivateNonDefault(util.HTTPClient())
		defer httpmock.DeactivateAndReset()

		// Mock subtree endpoint to fail
		httpmock.RegisterResponder("GET",
			fmt.Sprintf("http://test-peer/subtree/%s", subtreeHash.String()),
			httpmock.NewStringResponder(500, "Internal Server Error"))

		// Mock subtree_data endpoint to succeed
		httpmock.RegisterResponder("GET",
			fmt.Sprintf("http://test-peer/subtree_data/%s", subtreeHash.String()),
			httpmock.NewBytesResponder(200, []byte("data")))

		// Create a dummy block for the test
		testBlock := &model.Block{
			Height: 100,
		}

		err := suite.Server.fetchAndStoreSubtreeAndSubtreeData(suite.Ctx, testBlock, subtreeHash, "12D3KooWL1NF6fdTJ9cucEuwvuX8V8KtpJZZnUE4umdLBuK15eUZ", "http://test-peer")
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "failed to fetch subtree from")
	})

	t.Run("fetchAndStoreSubtreeAndSubtreeData_SubtreeDataError", func(t *testing.T) {
		suite := NewCatchupTestSuite(t)
		defer suite.Cleanup()

		suite.Server.subtreeStore = memory.New()

		subtreeHash := &chainhash.Hash{0x01, 0x02, 0x03}

		httpmock.ActivateNonDefault(util.HTTPClient())
		defer httpmock.DeactivateAndReset()

		// Create node hashes for the subtree endpoint (raw hashes, not serialized subtree)
		var nodeHashes []byte
		// First is coinbase placeholder
		nodeHashes = append(nodeHashes, subtreepkg.CoinbasePlaceholderHashValue[:]...)
		// Then the transaction hashes
		nodeHashes = append(nodeHashes, txs[1].TxIDChainHash()[:]...)
		nodeHashes = append(nodeHashes, txs[2].TxIDChainHash()[:]...)
		nodeHashes = append(nodeHashes, txs[3].TxIDChainHash()[:]...)

		// Mock subtree endpoint to succeed
		httpmock.RegisterResponder("GET",
			fmt.Sprintf("http://test-peer/subtree/%s", subtreeHash.String()),
			httpmock.NewBytesResponder(200, nodeHashes))

		// Mock subtree_data endpoint to fail
		httpmock.RegisterResponder("GET",
			fmt.Sprintf("http://test-peer/subtree_data/%s", subtreeHash.String()),
			httpmock.NewStringResponder(404, "Not Found"))

		// Create a dummy block for the test
		testBlock := &model.Block{
			Height: 100,
		}
		err := suite.Server.fetchAndStoreSubtreeAndSubtreeData(suite.Ctx, testBlock, subtreeHash, "12D3KooWL1NF6fdTJ9cucEuwvuX8V8KtpJZZnUE4umdLBuK15eUZ", "http://test-peer")
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "failed to fetch subtree data from")
	})

	t.Run("fetchSubtreeDataForBlock_NoSubtrees", func(t *testing.T) {
		suite := NewCatchupTestSuite(t)
		defer suite.Cleanup()

		// Create block with no subtrees
		block := &model.Block{
			Subtrees: []*chainhash.Hash{}, // Empty subtrees
		}

		err := suite.Server.fetchSubtreeDataForBlock(suite.Ctx, block, "12D3KooWL1NF6fdTJ9cucEuwvuX8V8KtpJZZnUE4umdLBuK15eUZ", "http://test-peer")
		assert.NoError(t, err) // Should return early with no error
	})

	t.Run("fetchSubtreeDataForBlock_SubtreeError", func(t *testing.T) {
		suite := NewCatchupTestSuite(t)
		defer suite.Cleanup()

		suite.Server.subtreeStore = memory.New()

		// Create block with subtree
		subtreeHash := createTestHash("error-subtree")

		block := &model.Block{
			Subtrees: []*chainhash.Hash{subtreeHash},
		}

		httpmock.ActivateNonDefault(util.HTTPClient())
		defer httpmock.DeactivateAndReset()

		// Mock subtree endpoint to fail
		httpmock.RegisterResponder("GET",
			fmt.Sprintf("http://test-peer/subtree/%s", subtreeHash.String()),
			httpmock.NewStringResponder(500, "Internal Server Error"))

		err := suite.Server.fetchSubtreeDataForBlock(suite.Ctx, block, "12D3KooWL1NF6fdTJ9cucEuwvuX8V8KtpJZZnUE4umdLBuK15eUZ", "http://test-peer")
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "Failed to fetch subtree data for block")
	})

	t.Run("fetchSubtreeDataForBlock_SubtreeDataError", func(t *testing.T) {
		suite := NewCatchupTestSuite(t)
		defer suite.Cleanup()

		suite.Server.subtreeStore = memory.New()

		// Create block with subtree
		subtreeHash := createTestHash("data-error-subtree")

		block := &model.Block{
			Subtrees: []*chainhash.Hash{subtreeHash},
		}

		httpmock.ActivateNonDefault(util.HTTPClient())
		defer httpmock.DeactivateAndReset()

		// Create minimal valid node hashes for the subtree endpoint
		// Just one hash to make it valid
		nodeHashes := make([]byte, chainhash.HashSize)

		// Mock subtree endpoint to succeed
		httpmock.RegisterResponder("GET",
			fmt.Sprintf("http://test-peer/subtree/%s", subtreeHash.String()),
			httpmock.NewBytesResponder(200, nodeHashes))

		// Mock subtree_data endpoint to fail
		httpmock.RegisterResponder("GET",
			fmt.Sprintf("http://test-peer/subtree_data/%s", subtreeHash.String()),
			httpmock.NewStringResponder(404, "Not Found"))

		err := suite.Server.fetchSubtreeDataForBlock(suite.Ctx, block, "12D3KooWL1NF6fdTJ9cucEuwvuX8V8KtpJZZnUE4umdLBuK15eUZ", "http://test-peer")
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "Failed to fetch subtree data for block")
	})
}

// TestFetchBlocksConcurrentlyOptimized tests the deprecated alias function
func TestFetchBlocksConcurrentlyOptimized(t *testing.T) {
	t.Run("BackwardCompatibilityAlias", func(t *testing.T) {
		suite := NewCatchupTestSuite(t)
		defer suite.Cleanup()

		// Create test blocks
		blocks := testhelpers.CreateTestBlockChain(t, 3)
		targetBlock := blocks[2]
		headers := []*model.BlockHeader{blocks[1].Header, blocks[2].Header}

		// Set up HTTP mock for block fetching
		httpmock.ActivateNonDefault(util.HTTPClient())
		defer httpmock.DeactivateAndReset()

		// Mock batch request - return blocks in reverse order (newest first)
		var batchData bytes.Buffer
		// Write blocks 2, 1 (reverse order)
		blockBytes2, _ := blocks[2].Bytes()
		batchData.Write(blockBytes2)
		blockBytes1, _ := blocks[1].Bytes()
		batchData.Write(blockBytes1)

		// Request uses last block's hash since we fetch in reverse
		httpmock.RegisterResponder("GET",
			fmt.Sprintf("http://test-peer/blocks/%s?n=2", blocks[2].Header.Hash().String()),
			httpmock.NewBytesResponder(200, batchData.Bytes()))

		// Create channels and size counter
		var size atomic.Int64
		size.Store(2)
		validateBlocksChan := make(chan *model.Block, 2)

		catchupCtx := &CatchupContext{
			blockUpTo:    targetBlock,
			baseURL:      "http://test-peer",
			blockHeaders: headers,
			commonAncestorMeta: &model.BlockHeaderMeta{
				Height: 0,
			},
		}

		// Call the deprecated alias function
		err := suite.Server.fetchBlocksConcurrently(suite.Ctx, catchupCtx, validateBlocksChan, &size)
		assert.NoError(t, err)

		// Wait for completion
		// err = errorGroup.Wait()
		// assert.NoError(t, err)

		// Verify blocks were processed
		var receivedBlocks []*model.Block
		for i := 0; i < 2; i++ {
			select {
			case block := <-validateBlocksChan:
				receivedBlocks = append(receivedBlocks, block)
			case <-time.After(time.Second):
				t.Fatalf("Timeout waiting for block %d", i+1)
			}
		}

		assert.Len(t, receivedBlocks, 2)
		// Verify blocks are in correct order
		assert.Equal(t, blocks[1].Header.Hash(), receivedBlocks[0].Header.Hash())
		assert.Equal(t, blocks[2].Header.Hash(), receivedBlocks[1].Header.Hash())
	})
}

// TestFetchSubtreeDataForBlock tests the fetchSubtreeDataForBlock function comprehensively
func TestFetchSubtreeDataForBlock(t *testing.T) {
	httpmock.ActivateNonDefault(util.HTTPClient())
	defer httpmock.DeactivateAndReset()

	logger := ulogger.TestLogger{}
	mockSubtreeStore := memory.New()
	settings := test.CreateBaseTestSettings(t)
	server := &Server{
		logger:       logger,
		subtreeStore: mockSubtreeStore,
		settings:     settings,
	}

	baseURL := "http://test-peer:8080"
	ctx := context.Background()

	txs := transactions.CreateTestTransactionChainWithCount(t, 5)

	subtree, err := subtreepkg.NewIncompleteTreeByLeafCount(4)
	assert.NoError(t, err)

	require.NoError(t, subtree.AddCoinbaseNode())
	require.NoError(t, subtree.AddNode(*txs[1].TxIDChainHash(), 1, 11))
	require.NoError(t, subtree.AddNode(*txs[2].TxIDChainHash(), 2, 12))
	require.NoError(t, subtree.AddNode(*txs[3].TxIDChainHash(), 3, 13))

	subtreeHash := subtree.RootHash()

	// subtreeBytes not needed - we use raw node hashes instead
	// subtreeBytes, err := subtree.Serialize()
	// require.NoError(t, err)

	subtreeData := subtreepkg.NewSubtreeData(subtree)
	require.NoError(t, subtreeData.AddTx(txs[0], 0))
	require.NoError(t, subtreeData.AddTx(txs[1], 1))
	require.NoError(t, subtreeData.AddTx(txs[2], 2))
	require.NoError(t, subtreeData.AddTx(txs[3], 3))

	subtreeDataBytes, err := subtreeData.Serialize()
	require.NoError(t, err)

	t.Run("NoSubtrees", func(t *testing.T) {
		// Test block with no subtrees
		block := &model.Block{
			Subtrees: []*chainhash.Hash{}, // Empty subtrees
		}

		err := server.fetchSubtreeDataForBlock(ctx, block, "12D3KooWL1NF6fdTJ9cucEuwvuX8V8KtpJZZnUE4umdLBuK15eUZ", baseURL)
		assert.NoError(t, err)
	})

	t.Run("SingleSubtree", func(t *testing.T) {
		block := &model.Block{
			Subtrees: []*chainhash.Hash{subtreeHash},
		}

		// Mock HTTP responses for subtree and subtreeData
		subtreeURL := fmt.Sprintf("%s/subtree/%s", baseURL, subtreeHash.String())
		subtreeDataURL := fmt.Sprintf("%s/subtree_data/%s", baseURL, subtreeHash.String())

		// Create node hashes for the subtree endpoint (raw hashes, not serialized subtree)
		var nodeHashes []byte
		// First is coinbase placeholder
		nodeHashes = append(nodeHashes, subtreepkg.CoinbasePlaceholderHashValue[:]...)
		// Then the transaction hashes
		nodeHashes = append(nodeHashes, txs[1].TxIDChainHash()[:]...)
		nodeHashes = append(nodeHashes, txs[2].TxIDChainHash()[:]...)
		nodeHashes = append(nodeHashes, txs[3].TxIDChainHash()[:]...)

		httpmock.RegisterResponder("GET", subtreeURL,
			httpmock.NewBytesResponder(200, nodeHashes))
		httpmock.RegisterResponder("GET", subtreeDataURL,
			httpmock.NewBytesResponder(200, subtreeDataBytes))

		err := server.fetchSubtreeDataForBlock(ctx, block, "12D3KooWL1NF6fdTJ9cucEuwvuX8V8KtpJZZnUE4umdLBuK15eUZ", baseURL)
		assert.NoError(t, err)
	})

	t.Run("MultipleSubtrees", func(t *testing.T) {
		subtreeHash1 := createTestHash("subtree1")
		subtreeHash2 := createTestHash("subtree2")
		subtreeHash3 := createTestHash("subtree3")

		block := &model.Block{
			Subtrees: []*chainhash.Hash{subtreeHash1, subtreeHash2, subtreeHash3},
		}

		// Create node hashes for the subtree endpoint (raw hashes, not serialized subtree)
		var nodeHashes []byte
		// First is coinbase placeholder
		nodeHashes = append(nodeHashes, subtreepkg.CoinbasePlaceholderHashValue[:]...)
		// Then the transaction hashes - just use the first 3 for simplicity
		nodeHashes = append(nodeHashes, txs[1].TxIDChainHash()[:]...)
		nodeHashes = append(nodeHashes, txs[2].TxIDChainHash()[:]...)
		nodeHashes = append(nodeHashes, txs[3].TxIDChainHash()[:]...)

		// Mock HTTP responses for all subtrees
		for _, hash := range block.Subtrees {
			subtreeURL := fmt.Sprintf("%s/subtree/%s", baseURL, hash.String())
			subtreeDataURL := fmt.Sprintf("%s/subtree_data/%s", baseURL, hash.String())

			httpmock.RegisterResponder("GET", subtreeURL,
				httpmock.NewBytesResponder(200, nodeHashes))
			httpmock.RegisterResponder("GET", subtreeDataURL,
				httpmock.NewBytesResponder(200, subtreeDataBytes))
		}

		err := server.fetchSubtreeDataForBlock(ctx, block, "12D3KooWL1NF6fdTJ9cucEuwvuX8V8KtpJZZnUE4umdLBuK15eUZ", baseURL)
		assert.NoError(t, err)
	})

	t.Run("SubtreeFetchError", func(t *testing.T) {
		subtreeHash := createTestHash("error-subtree")
		block := &model.Block{
			Subtrees: []*chainhash.Hash{subtreeHash},
		}

		// Mock error response
		subtreeURL := fmt.Sprintf("%s/subtree/%s", baseURL, subtreeHash.String())
		httpmock.RegisterResponder("GET", subtreeURL,
			httpmock.NewErrorResponder(errors.NewNetworkError("subtree fetch error")))

		err := server.fetchSubtreeDataForBlock(ctx, block, "12D3KooWL1NF6fdTJ9cucEuwvuX8V8KtpJZZnUE4umdLBuK15eUZ", baseURL)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "Failed to fetch subtree data for block")
	})

	t.Run("ContextCancellation", func(t *testing.T) {
		subtreeHash := createTestHash("cancel-subtree")
		block := &model.Block{
			Subtrees: []*chainhash.Hash{subtreeHash},
		}

		// Set up HTTP mocks that will be cancelled
		httpmock.RegisterResponder("GET", fmt.Sprintf("%s/subtree/%s", baseURL, subtreeHash.String()),
			func(req *http.Request) (*http.Response, error) {
				// Check if context is cancelled
				select {
				case <-req.Context().Done():
					return nil, req.Context().Err()
				default:
					return httpmock.NewStringResponse(500, "error"), nil
				}
			},
		)
		httpmock.RegisterResponder("GET", fmt.Sprintf("%s/subtree_data/%s", baseURL, subtreeHash.String()),
			func(req *http.Request) (*http.Response, error) {
				// Check if context is cancelled
				select {
				case <-req.Context().Done():
					return nil, req.Context().Err()
				default:
					return httpmock.NewStringResponse(500, "error"), nil
				}
			},
		)

		// Create cancelled context
		cancelCtx, cancel := context.WithCancel(ctx)
		cancel() // Cancel immediately

		err := server.fetchSubtreeDataForBlock(cancelCtx, block, "12D3KooWL1NF6fdTJ9cucEuwvuX8V8KtpJZZnUE4umdLBuK15eUZ", baseURL)
		assert.Error(t, err)
		// Check for either context canceled or the wrapped error containing context cancellation
		assert.True(t,
			strings.Contains(err.Error(), "context canceled") ||
				strings.Contains(err.Error(), "context cancelled") ||
				strings.Contains(err.Error(), "Failed to fetch subtree data for block"),
			"Expected error to contain context cancellation or fetch failure, got: %s", err.Error())
	})
}

// TestFetchAndStoreSubtreeAndSubtreeData tests the fetchAndStoreSubtreeAndSubtreeData function comprehensively
func TestFetchAndStoreSubtreeData(t *testing.T) {
	baseURL := "http://test-peer:8080"
	ctx := context.Background()

	txs := transactions.CreateTestTransactionChainWithCount(t, 5)

	subtree, err := subtreepkg.NewIncompleteTreeByLeafCount(4)
	assert.NoError(t, err)

	require.NoError(t, subtree.AddCoinbaseNode())
	require.NoError(t, subtree.AddNode(*txs[1].TxIDChainHash(), 1, 11))
	require.NoError(t, subtree.AddNode(*txs[2].TxIDChainHash(), 2, 12))
	require.NoError(t, subtree.AddNode(*txs[3].TxIDChainHash(), 3, 13))

	subtreeHash := subtree.RootHash()

	// subtreeBytes not needed - we use raw node hashes instead
	// subtreeBytes, err := subtree.Serialize()
	// require.NoError(t, err)

	subtreeData := subtreepkg.NewSubtreeData(subtree)
	require.NoError(t, subtreeData.AddTx(txs[0], 0))
	require.NoError(t, subtreeData.AddTx(txs[1], 1))
	require.NoError(t, subtreeData.AddTx(txs[2], 2))
	require.NoError(t, subtreeData.AddTx(txs[3], 3))

	subtreeDataBytes, err := subtreeData.Serialize()
	require.NoError(t, err)

	t.Run("SuccessfulFetch", func(t *testing.T) {
		// Create a fresh server instance for this test
		logger := ulogger.TestLogger{}
		mockSubtreeStore := memory.New()
		settings := test.CreateBaseTestSettings(t)
		server := &Server{
			logger:       logger,
			subtreeStore: mockSubtreeStore,
			settings:     settings,
		}
		httpmock.ActivateNonDefault(util.HTTPClient())
		defer func() {
			httpmock.DeactivateAndReset()
		}()

		// Create node hashes for the subtree endpoint (raw hashes, not serialized subtree)
		var nodeHashes []byte
		// First is coinbase placeholder
		nodeHashes = append(nodeHashes, subtreepkg.CoinbasePlaceholderHashValue[:]...)
		// Then the transaction hashes
		nodeHashes = append(nodeHashes, txs[1].TxIDChainHash()[:]...)
		nodeHashes = append(nodeHashes, txs[2].TxIDChainHash()[:]...)
		nodeHashes = append(nodeHashes, txs[3].TxIDChainHash()[:]...)

		// Mock HTTP responses
		subtreeURL := fmt.Sprintf("%s/subtree/%s", baseURL, subtreeHash.String())
		subtreeDataURL := fmt.Sprintf("%s/subtree_data/%s", baseURL, subtreeHash.String())

		httpmock.RegisterResponder("GET", subtreeURL,
			httpmock.NewBytesResponder(200, nodeHashes))
		httpmock.RegisterResponder("GET", subtreeDataURL,
			httpmock.NewBytesResponder(200, subtreeDataBytes))

		// Create a dummy block for the test
		testBlock := &model.Block{
			Height: 100,
		}
		err := server.fetchAndStoreSubtreeAndSubtreeData(ctx, testBlock, subtreeHash, "12D3KooWL1NF6fdTJ9cucEuwvuX8V8KtpJZZnUE4umdLBuK15eUZ", baseURL)
		assert.NoError(t, err)
	})

	t.Run("SubtreeFetchError", func(t *testing.T) {
		// Create a fresh server instance for this test
		logger := ulogger.TestLogger{}
		mockSubtreeStore := memory.New()
		settings := test.CreateBaseTestSettings(t)
		server := &Server{
			logger:       logger,
			subtreeStore: mockSubtreeStore,
			settings:     settings,
		}
		httpmock.ActivateNonDefault(util.HTTPClient())
		defer func() {
			httpmock.DeactivateAndReset()
		}()

		// Mock error for subtree fetch
		subtreeURL := fmt.Sprintf("%s/subtree/%s", baseURL, subtreeHash.String())
		httpmock.RegisterResponder("GET", subtreeURL,
			httpmock.NewErrorResponder(errors.NewNetworkError("subtree fetch failed")))

		// Mock error for subtree fetch
		subtreeURL = fmt.Sprintf("%s/subtree_data/%s", baseURL, subtreeHash.String())
		httpmock.RegisterResponder("GET", subtreeURL,
			httpmock.NewErrorResponder(errors.NewNetworkError("subtree fetch failed")))

		// Create a dummy block for the test
		testBlock := &model.Block{
			Height: 100,
		}
		err := server.fetchAndStoreSubtreeAndSubtreeData(ctx, testBlock, subtreeHash, "12D3KooWL1NF6fdTJ9cucEuwvuX8V8KtpJZZnUE4umdLBuK15eUZ", baseURL)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "failed to fetch subtree")
	})

	t.Run("SubtreeDataFetchError", func(t *testing.T) {
		// Create a fresh server instance for this test
		logger := ulogger.TestLogger{}
		mockSubtreeStore := memory.New()
		settings := test.CreateBaseTestSettings(t)
		server := &Server{
			logger:       logger,
			subtreeStore: mockSubtreeStore,
			settings:     settings,
		}
		httpmock.ActivateNonDefault(util.HTTPClient())
		defer func() {
			httpmock.DeactivateAndReset()
		}()

		// Create node hashes for the subtree endpoint (raw hashes, not serialized subtree)
		var nodeHashes []byte
		// First is coinbase placeholder
		nodeHashes = append(nodeHashes, subtreepkg.CoinbasePlaceholderHashValue[:]...)
		// Then the transaction hashes
		nodeHashes = append(nodeHashes, txs[1].TxIDChainHash()[:]...)
		nodeHashes = append(nodeHashes, txs[2].TxIDChainHash()[:]...)
		nodeHashes = append(nodeHashes, txs[3].TxIDChainHash()[:]...)

		// Mock successful subtree but error for subtreeData
		subtreeURL := fmt.Sprintf("%s/subtree/%s", baseURL, subtreeHash.String())
		subtreeDataURL := fmt.Sprintf("%s/subtree_data/%s", baseURL, subtreeHash.String())

		httpmock.RegisterResponder("GET", subtreeURL,
			httpmock.NewBytesResponder(200, nodeHashes))
		httpmock.RegisterResponder("GET", subtreeDataURL,
			httpmock.NewErrorResponder(errors.NewNetworkError("subtree data fetch failed")))

		// Create a dummy block for the test
		testBlock := &model.Block{
			Height: 100,
		}
		err := server.fetchAndStoreSubtreeAndSubtreeData(ctx, testBlock, subtreeHash, "12D3KooWL1NF6fdTJ9cucEuwvuX8V8KtpJZZnUE4umdLBuK15eUZ", baseURL)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "failed to fetch subtree data from")
	})

	t.Run("StoreError", func(t *testing.T) {
		// Create a fresh server instance for this test
		logger := ulogger.TestLogger{}
		settings := test.CreateBaseTestSettings(t)
		blobStore := &blob.MockStore{}
		server := &Server{
			logger:       logger,
			subtreeStore: blobStore,
			settings:     settings,
		}
		httpmock.ActivateNonDefault(util.HTTPClient())
		defer func() {
			httpmock.DeactivateAndReset()
		}()

		// Mock Exists to return false (subtree doesn't exist) for any file type
		blobStore.On("Exists", mock.Anything, mock.Anything, mock.Anything, mock.Anything).
			Return(false, nil)

		// Mock SetFromReader for subtreeData
		blobStore.On("SetFromReader", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
			Return(nil).Maybe()

		// Mock Set to return error for storing subtree
		blobStore.On("Set", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
			Return(errors.NewStorageError("failed to store subtree data")).Maybe()

		// Create node hashes for the subtree endpoint (raw hashes, not serialized subtree)
		var nodeHashes []byte
		// First is coinbase placeholder
		nodeHashes = append(nodeHashes, subtreepkg.CoinbasePlaceholderHashValue[:]...)
		// Then the transaction hashes
		nodeHashes = append(nodeHashes, txs[1].TxIDChainHash()[:]...)
		nodeHashes = append(nodeHashes, txs[2].TxIDChainHash()[:]...)
		nodeHashes = append(nodeHashes, txs[3].TxIDChainHash()[:]...)

		// Mock successful HTTP responses
		subtreeURL := fmt.Sprintf("%s/subtree/%s", baseURL, subtreeHash.String())
		subtreeDataURL := fmt.Sprintf("%s/subtree_data/%s", baseURL, subtreeHash.String())

		httpmock.RegisterResponder("GET", subtreeURL,
			httpmock.NewBytesResponder(200, nodeHashes))
		httpmock.RegisterResponder("GET", subtreeDataURL,
			httpmock.NewBytesResponder(200, subtreeDataBytes))

		// Create a dummy block for the test
		testBlock := &model.Block{
			Height: 100,
		}
		err := server.fetchAndStoreSubtreeAndSubtreeData(ctx, testBlock, subtreeHash, "12D3KooWL1NF6fdTJ9cucEuwvuX8V8KtpJZZnUE4umdLBuK15eUZ", baseURL)
		assert.Error(t, err)
	})

	t.Run("ContextCancellation", func(t *testing.T) {
		// Create a fresh server instance for this test
		logger := ulogger.TestLogger{}
		mockSubtreeStore := memory.New()
		settings := test.CreateBaseTestSettings(t)
		server := &Server{
			logger:       logger,
			subtreeStore: mockSubtreeStore,
			settings:     settings,
		}
		httpmock.ActivateNonDefault(util.HTTPClient())
		defer func() {
			httpmock.DeactivateAndReset()
		}()

		// Set up HTTP mocks that will be cancelled
		httpmock.RegisterResponder("GET", fmt.Sprintf("%s/subtree/%s", baseURL, subtreeHash.String()),
			func(req *http.Request) (*http.Response, error) {
				// Check if context is cancelled
				select {
				case <-req.Context().Done():
					return nil, req.Context().Err()
				default:
					return httpmock.NewStringResponse(500, "error"), nil
				}
			},
		)
		httpmock.RegisterResponder("GET", fmt.Sprintf("%s/subtree_data/%s", baseURL, subtreeHash.String()),
			func(req *http.Request) (*http.Response, error) {
				// Check if context is cancelled
				select {
				case <-req.Context().Done():
					return nil, req.Context().Err()
				default:
					return httpmock.NewStringResponse(500, "error"), nil
				}
			},
		)

		// Create cancelled context
		cancelCtx, cancel := context.WithCancel(ctx)
		cancel() // Cancel immediately

		// Create a dummy block for the test
		testBlock := &model.Block{
			Height: 100,
		}
		err := server.fetchAndStoreSubtreeAndSubtreeData(cancelCtx, testBlock, subtreeHash, "12D3KooWL1NF6fdTJ9cucEuwvuX8V8KtpJZZnUE4umdLBuK15eUZ", baseURL)
		assert.Error(t, err)
		// Check for either context canceled or the wrapped error containing context cancellation
		assert.True(t,
			strings.Contains(err.Error(), "context canceled") ||
				strings.Contains(err.Error(), "context cancelled") ||
				strings.Contains(err.Error(), "Failed to fetch data for subtree"),
			"Expected error to contain context cancellation or fetch failure, got: %s", err.Error())
	})
}

// TestFetchSubtreeFromPeer tests the fetchSubtreeFromPeer function comprehensively
func TestFetchSubtreeFromPeer(t *testing.T) {
	httpmock.ActivateNonDefault(util.HTTPClient())
	defer httpmock.DeactivateAndReset()

	logger := ulogger.TestLogger{}
	server := &Server{
		logger: logger,
	}

	baseURL := "http://test-peer:8080"
	ctx := context.Background()

	t.Run("SuccessfulFetch", func(t *testing.T) {
		subtreeHash := createTestHash("test-subtree")
		expectedData := []byte("subtree-content-data")

		subtreeURL := fmt.Sprintf("%s/subtree/%s", baseURL, subtreeHash.String())
		httpmock.RegisterResponder("GET", subtreeURL,
			httpmock.NewBytesResponder(200, expectedData))

		data, err := server.fetchSubtreeFromPeer(ctx, subtreeHash, "test-peer-id", baseURL)
		assert.NoError(t, err)
		assert.Equal(t, expectedData, data)
	})

	t.Run("HTTPError", func(t *testing.T) {
		subtreeHash := createTestHash("error-subtree")

		subtreeURL := fmt.Sprintf("%s/subtree/%s", baseURL, subtreeHash.String())
		httpmock.RegisterResponder("GET", subtreeURL,
			httpmock.NewErrorResponder(errors.NewNetworkError("HTTP request failed")))

		data, err := server.fetchSubtreeFromPeer(ctx, subtreeHash, "test-peer-id", baseURL)
		assert.Error(t, err)
		assert.Nil(t, data)
		assert.Contains(t, err.Error(), "failed to fetch subtree")
	})

	t.Run("EmptyResponse", func(t *testing.T) {
		subtreeHash := createTestHash("empty-subtree")

		subtreeURL := fmt.Sprintf("%s/subtree/%s", baseURL, subtreeHash.String())
		httpmock.RegisterResponder("GET", subtreeURL,
			httpmock.NewBytesResponder(200, []byte{})) // Empty response

		data, err := server.fetchSubtreeFromPeer(ctx, subtreeHash, "test-peer-id", baseURL)
		assert.Error(t, err)
		assert.Nil(t, data)
		assert.Contains(t, err.Error(), "empty subtree received")
	})

	t.Run("ContextCancellation", func(t *testing.T) {
		subtreeHash := createTestHash("cancel-subtree")

		// Set up HTTP mock that will be cancelled
		httpmock.RegisterResponder("GET", fmt.Sprintf("%s/subtree/%s", baseURL, subtreeHash.String()),
			func(req *http.Request) (*http.Response, error) {
				// Check if context is cancelled
				select {
				case <-req.Context().Done():
					return nil, req.Context().Err()
				default:
					return httpmock.NewStringResponse(500, "error"), nil
				}
			},
		)

		// Create cancelled context
		cancelCtx, cancel := context.WithCancel(ctx)
		cancel() // Cancel immediately

		data, err := server.fetchSubtreeFromPeer(cancelCtx, subtreeHash, "test-peer-id", baseURL)
		assert.Error(t, err)
		assert.Nil(t, data)
		// Check for either context canceled or the wrapped error containing context cancellation
		assert.True(t,
			strings.Contains(err.Error(), "context canceled") ||
				strings.Contains(err.Error(), "context cancelled") ||
				strings.Contains(err.Error(), "failed to fetch subtree"),
			"Expected error to contain context cancellation or fetch failure, got: %s", err.Error())
	})
}

// TestFetchSubtreeDataFromPeer tests the fetchSubtreeDataFromPeer function comprehensively
func TestFetchSubtreeDataFromPeer(t *testing.T) {
	httpmock.ActivateNonDefault(util.HTTPClient())
	defer httpmock.DeactivateAndReset()

	logger := ulogger.TestLogger{}
	settings := test.CreateBaseTestSettings(t)
	server := &Server{
		logger:   logger,
		settings: settings,
	}

	baseURL := "http://test-peer:8080"
	ctx := context.Background()

	t.Run("SuccessfulFetch", func(t *testing.T) {
		subtreeHash := createTestHash("test-subtree-data")
		expectedData := []byte("subtree-raw-data-content")

		subtreeDataURL := fmt.Sprintf("%s/subtree_data/%s", baseURL, subtreeHash.String())
		httpmock.RegisterResponder("GET", subtreeDataURL,
			httpmock.NewBytesResponder(200, expectedData))

		reader, err := server.fetchSubtreeDataFromPeer(ctx, subtreeHash, "test-peer-id", baseURL)
		assert.NoError(t, err)
		assert.NotNil(t, reader)
		defer reader.Close()

		// Read the data from the reader
		data, err := io.ReadAll(reader)
		assert.NoError(t, err)
		assert.Equal(t, expectedData, data)
	})

	t.Run("HTTPError", func(t *testing.T) {
		subtreeHash := createTestHash("error-subtree-data")

		subtreeDataURL := fmt.Sprintf("%s/subtree_data/%s", baseURL, subtreeHash.String())
		httpmock.RegisterResponder("GET", subtreeDataURL,
			httpmock.NewErrorResponder(errors.NewNetworkError("HTTP request failed")))

		data, err := server.fetchSubtreeDataFromPeer(ctx, subtreeHash, "test-peer-id", baseURL)
		assert.Error(t, err)
		assert.Nil(t, data)
		assert.Contains(t, err.Error(), "failed to fetch subtree data from")
	})

	t.Run("EmptyResponse", func(t *testing.T) {
		subtreeHash := createTestHash("empty-subtree-data")

		subtreeDataURL := fmt.Sprintf("%s/subtree_data/%s", baseURL, subtreeHash.String())
		httpmock.RegisterResponder("GET", subtreeDataURL,
			httpmock.NewBytesResponder(200, []byte{})) // Empty response

		reader, err := server.fetchSubtreeDataFromPeer(ctx, subtreeHash, "test-peer-id", baseURL)
		// Empty response is not an error for the fetcher - it just returns an empty reader
		assert.NoError(t, err)
		assert.NotNil(t, reader)
		defer reader.Close()

		// Read the data from the reader - should be empty
		data, err := io.ReadAll(reader)
		assert.NoError(t, err)
		assert.Empty(t, data)
	})

	t.Run("ContextCancellation", func(t *testing.T) {
		subtreeHash := createTestHash("cancel-subtree-data")

		// Set up HTTP mock that will be cancelled
		httpmock.RegisterResponder("GET", fmt.Sprintf("%s/subtree_data/%s", baseURL, subtreeHash.String()),
			func(req *http.Request) (*http.Response, error) {
				// Check if context is cancelled
				select {
				case <-req.Context().Done():
					return nil, req.Context().Err()
				default:
					return httpmock.NewStringResponse(500, "error"), nil
				}
			},
		)

		// Create cancelled context
		cancelCtx, cancel := context.WithCancel(ctx)
		cancel() // Cancel immediately

		data, err := server.fetchSubtreeDataFromPeer(cancelCtx, subtreeHash, "test-peer-id", baseURL)
		assert.Error(t, err)
		assert.Nil(t, data)
		// Check for either context canceled or the wrapped error containing context cancellation
		assert.True(t,
			strings.Contains(err.Error(), "context canceled") ||
				strings.Contains(err.Error(), "context cancelled") ||
				strings.Contains(err.Error(), "Failed to fetch subtree data"),
			"Expected error to contain context cancellation or fetch failure, got: %s", err.Error())
	})
}

// TestBlockWorker tests the blockWorker function more comprehensively
func TestBlockWorker(t *testing.T) {
	httpmock.ActivateNonDefault(util.HTTPClient())
	defer httpmock.DeactivateAndReset()

	logger := ulogger.TestLogger{}
	mockSubtreeStore := memory.New()
	settings := test.CreateBaseTestSettings(t)
	server := &Server{
		logger:       logger,
		subtreeStore: mockSubtreeStore,
		settings:     settings,
	}

	baseURL := "http://test-peer:8080"
	ctx := context.Background()

	txs := transactions.CreateTestTransactionChainWithCount(t, 5)

	subtree, err := subtreepkg.NewIncompleteTreeByLeafCount(4)
	assert.NoError(t, err)

	require.NoError(t, subtree.AddCoinbaseNode())
	require.NoError(t, subtree.AddNode(*txs[1].TxIDChainHash(), 1, 11))
	require.NoError(t, subtree.AddNode(*txs[2].TxIDChainHash(), 2, 12))
	require.NoError(t, subtree.AddNode(*txs[3].TxIDChainHash(), 3, 13))

	subtreeHash := subtree.RootHash()

	// subtreeBytes not needed - we use raw node hashes instead
	// subtreeBytes, err := subtree.Serialize()
	// require.NoError(t, err)

	subtreeData := subtreepkg.NewSubtreeData(subtree)
	require.NoError(t, subtreeData.AddTx(txs[0], 0))
	require.NoError(t, subtreeData.AddTx(txs[1], 1))
	require.NoError(t, subtreeData.AddTx(txs[2], 2))
	require.NoError(t, subtreeData.AddTx(txs[3], 3))

	subtreeDataBytes, err := subtreeData.Serialize()
	require.NoError(t, err)

	t.Run("WorkerProcessesBlocksWithSubtrees", func(t *testing.T) {
		// Create test blocks with subtrees
		subtreeHash2 := createTestHash("subtree2")

		block1 := &model.Block{
			Subtrees: []*chainhash.Hash{subtreeHash},
		}
		block2 := &model.Block{
			Subtrees: []*chainhash.Hash{subtreeHash2},
		}

		// Create node hashes for the subtree endpoint (raw hashes, not serialized subtree)
		var nodeHashes []byte
		// First is coinbase placeholder
		nodeHashes = append(nodeHashes, subtreepkg.CoinbasePlaceholderHashValue[:]...)
		// Then the transaction hashes
		nodeHashes = append(nodeHashes, txs[1].TxIDChainHash()[:]...)
		nodeHashes = append(nodeHashes, txs[2].TxIDChainHash()[:]...)
		nodeHashes = append(nodeHashes, txs[3].TxIDChainHash()[:]...)

		// Mock HTTP responses for all subtrees
		for _, hash := range []*chainhash.Hash{subtreeHash, subtreeHash2} {
			subtreeURL := fmt.Sprintf("%s/subtree/%s", baseURL, hash.String())
			subtreeDataURL := fmt.Sprintf("%s/subtree_data/%s", baseURL, hash.String())

			httpmock.RegisterResponder("GET", subtreeURL,
				httpmock.NewBytesResponder(200, nodeHashes))
			httpmock.RegisterResponder("GET", subtreeDataURL,
				httpmock.NewBytesResponder(200, subtreeDataBytes))
		}

		// Create channels
		workQueue := make(chan workItem, 2)
		resultQueue := make(chan resultItem, 2)

		// Send work items (using correct lowercase field names)
		workQueue <- workItem{block: block1, index: 0}
		workQueue <- workItem{block: block2, index: 1}
		close(workQueue)

		// Create a dummy blockUpTo for the worker
		blockUpTo := &model.Block{}

		// Start worker (using correct function signature)
		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = server.blockWorker(ctx, 1, workQueue, resultQueue, "12D3KooWL1NF6fdTJ9cucEuwvuX8V8KtpJZZnUE4umdLBuK15eUZ", baseURL, blockUpTo)
		}()

		// Wait for worker to finish
		wg.Wait()
		close(resultQueue)

		// Collect results
		var results []resultItem
		for result := range resultQueue {
			results = append(results, result)
		}

		assert.Len(t, results, 2, "Should process both blocks")

		// Check that all results are successful (using correct lowercase field name)
		for _, result := range results {
			assert.NoError(t, result.err, "All blocks should be processed successfully")
		}
	})

	t.Run("WorkerHandlesSubtreeError", func(t *testing.T) {
		subtreeHash := createTestHash("error-subtree")
		block := &model.Block{
			Subtrees: []*chainhash.Hash{subtreeHash},
		}

		// Mock error response
		subtreeURL := fmt.Sprintf("%s/subtree/%s", baseURL, subtreeHash.String())
		httpmock.RegisterResponder("GET", subtreeURL,
			httpmock.NewErrorResponder(errors.NewNetworkError("subtree fetch error")))

		// Create channels
		workQueue := make(chan workItem, 1)
		resultQueue := make(chan resultItem, 1)

		workQueue <- workItem{block: block, index: 0}
		close(workQueue)

		// Create a dummy blockUpTo for the worker
		blockUpTo := &model.Block{}

		// Start worker
		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = server.blockWorker(ctx, 1, workQueue, resultQueue, "12D3KooWL1NF6fdTJ9cucEuwvuX8V8KtpJZZnUE4umdLBuK15eUZ", baseURL, blockUpTo)
		}()

		// Wait for worker to finish
		wg.Wait()
		close(resultQueue)

		// Check result
		result := <-resultQueue
		assert.Error(t, result.err, "Should propagate subtree fetch error")
		assert.Contains(t, result.err.Error(), "Failed to fetch subtree data for block")
	})

	t.Run("WorkerHandlesEmptyQueue", func(t *testing.T) {
		// Create empty work queue
		workQueue := make(chan workItem)
		resultQueue := make(chan resultItem, 1)
		close(workQueue) // Close immediately

		// Create a dummy blockUpTo for the worker
		blockUpTo := &model.Block{}

		// Start worker
		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = server.blockWorker(ctx, 1, workQueue, resultQueue, "12D3KooWL1NF6fdTJ9cucEuwvuX8V8KtpJZZnUE4umdLBuK15eUZ", baseURL, blockUpTo)
		}()

		// Wait for worker to finish
		wg.Wait()
		close(resultQueue)

		// Should have no results
		results := make([]resultItem, 0)
		for result := range resultQueue {
			results = append(results, result)
		}
		assert.Len(t, results, 0, "Should have no results for empty queue")
	})
}

// TestFetchBlocksConcurrently_ErrorHandling tests improved error handling and cancellation
func TestFetchBlocksConcurrently_ErrorHandling(t *testing.T) {
	t.Run("Context Cancellation Propagates", func(t *testing.T) {
		suite := NewCatchupTestSuite(t)
		defer suite.Cleanup()

		// Create test blocks
		blocks := testhelpers.CreateTestBlockChain(t, 5)
		headers := make([]*model.BlockHeader, 4)
		for i := 0; i < 4; i++ {
			headers[i] = blocks[i+1].Header
		}

		// Set up HTTP mock with delay to allow cancellation
		httpmock.ActivateNonDefault(util.HTTPClient())
		defer httpmock.DeactivateAndReset()

		httpmock.RegisterResponder(
			"GET",
			`=~^http://test-peer/blocks/.*\?n=\d+$`,
			func(req *http.Request) (*http.Response, error) {
				time.Sleep(100 * time.Millisecond) // Delay to allow cancellation
				return httpmock.NewStringResponse(500, "Server Error"), nil
			},
		)

		// Create channels and counters
		var size atomic.Int64
		size.Store(int64(len(headers)))
		validateBlocksChan := make(chan *model.Block, 10)

		// Create context that will be cancelled
		ctx, cancel := context.WithCancel(context.Background())

		catchupCtx := &CatchupContext{
			blockUpTo:    blocks[4],
			baseURL:      "http://test-peer",
			blockHeaders: headers,
			commonAncestorMeta: &model.BlockHeaderMeta{
				Height: 0,
			},
		}

		// Start the function in a goroutine
		errChan := make(chan error, 1)
		go func() {
			err := suite.Server.fetchBlocksConcurrently(ctx, catchupCtx, validateBlocksChan, &size)
			errChan <- err
		}()

		// Cancel the context after a short delay
		time.Sleep(50 * time.Millisecond)
		cancel()

		// Wait for the function to return with cancellation error
		select {
		case err := <-errChan:
			assert.Error(t, err)
			assert.Contains(t, err.Error(), "context canceled")
		case <-time.After(2 * time.Second):
			t.Fatal("Function did not return within timeout after cancellation")
		}
	})

	t.Run("Hash Integrity Verification", func(t *testing.T) {
		suite := NewCatchupTestSuite(t)
		defer suite.Cleanup()

		// Create test blocks
		blocks := testhelpers.CreateTestBlockChain(t, 3)
		headers := []*model.BlockHeader{blocks[1].Header, blocks[2].Header}

		// Set up HTTP mock that returns wrong block for second request
		httpmock.ActivateNonDefault(util.HTTPClient())
		defer httpmock.DeactivateAndReset()

		httpmock.RegisterResponder(
			"GET",
			fmt.Sprintf("http://test-peer/blocks/%s?n=2", blocks[2].Header.Hash().String()),
			func(req *http.Request) (*http.Response, error) {
				// Return block2 twice (wrong) instead of block2 and block1
				// This will cause a hash mismatch when checking against headers
				block2Bytes, _ := blocks[2].Bytes()
				block2Bytes2, _ := blocks[2].Bytes()

				var buffer bytes.Buffer
				buffer.Write(block2Bytes)
				buffer.Write(block2Bytes2)

				return httpmock.NewBytesResponse(200, buffer.Bytes()), nil
			})

		// Create channels and counters
		var size atomic.Int64
		size.Store(2)
		validateBlocksChan := make(chan *model.Block, 2)

		catchupCtx := &CatchupContext{
			blockUpTo:    blocks[2],
			baseURL:      "http://test-peer",
			blockHeaders: headers,
			commonAncestorMeta: &model.BlockHeaderMeta{
				Height: 0,
			},
		}

		// Call fetchBlocksConcurrently
		err := suite.Server.fetchBlocksConcurrently(context.Background(), catchupCtx, validateBlocksChan, &size)

		// Should fail with hash mismatch error
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "block hash mismatch")
	})

	t.Run("EOF Handling with errors.Is", func(t *testing.T) {
		suite := NewCatchupTestSuite(t)
		defer suite.Cleanup()

		// Create test blocks
		blocks := testhelpers.CreateTestBlockChain(t, 2)

		// Set up HTTP mock that returns partial block data
		httpmock.ActivateNonDefault(util.HTTPClient())
		defer httpmock.DeactivateAndReset()

		httpmock.RegisterResponder(
			"GET",
			fmt.Sprintf("http://test-peer/blocks/%s?n=2", blocks[1].Header.Hash().String()),
			func(req *http.Request) (*http.Response, error) {
				// Return only first block's data (incomplete batch)
				block1Bytes, _ := blocks[1].Bytes()
				return httpmock.NewBytesResponse(200, block1Bytes), nil
			},
		)

		// Call fetchBlocksBatch directly to test EOF handling
		fetchedBlocks, err := suite.Server.fetchBlocksBatch(context.Background(), blocks[1].Header.Hash(), 2, "test-peer-id", "http://test-peer")

		// Should succeed and return only the first block (EOF handled gracefully)
		assert.NoError(t, err)
		assert.Len(t, fetchedBlocks, 1)
		assert.Equal(t, blocks[1].Header.Hash().String(), fetchedBlocks[0].Header.Hash().String())
	})
}

// TestOrderedDelivery_StrictOrdering tests that blocks are delivered in correct order despite worker completion order
func TestOrderedDelivery_StrictOrdering(t *testing.T) {
	suite := NewCatchupTestSuite(t)
	defer suite.Cleanup()

	// Create test blocks
	blocks := testhelpers.CreateTestBlockChain(t, 6)
	headers := make([]*model.BlockHeader, 5)
	for i := 0; i < 5; i++ {
		headers[i] = blocks[i+1].Header
	}

	// Set up HTTP mock
	httpmock.ActivateNonDefault(util.HTTPClient())
	defer httpmock.DeactivateAndReset()

	httpmock.RegisterResponder(
		"GET",
		`=~^http://test-peer/blocks/.*\?n=\d+$`,
		func(req *http.Request) (*http.Response, error) {
			// Return all blocks in reverse order (newest first)
			var buffer bytes.Buffer
			for i := 5; i >= 1; i-- {
				blockBytes, _ := blocks[i].Bytes()
				buffer.Write(blockBytes)
			}
			return httpmock.NewBytesResponse(200, buffer.Bytes()), nil
		},
	)

	// Mock subtree endpoints (empty responses for simplicity)
	httpmock.RegisterResponder("GET", `=~^http://test-peer/subtree/.*$`, httpmock.NewStringResponder(200, ""))
	httpmock.RegisterResponder("GET", `=~^http://test-peer/subtree_data/.*$`, httpmock.NewStringResponder(200, ""))

	// Create channels and counters
	var size atomic.Int64
	size.Store(int64(len(headers)))
	validateBlocksChan := make(chan *model.Block, 10)

	catchupCtx := &CatchupContext{
		blockUpTo:    blocks[5],
		baseURL:      "http://test-peer",
		blockHeaders: headers,
		commonAncestorMeta: &model.BlockHeaderMeta{
			Height: 0,
		},
	}

	// Call fetchBlocksConcurrently
	err := suite.Server.fetchBlocksConcurrently(context.Background(), catchupCtx, validateBlocksChan, &size)
	assert.NoError(t, err)

	// Collect delivered blocks
	var deliveredBlocks []*model.Block
	for i := 0; i < len(headers); i++ {
		select {
		case block := <-validateBlocksChan:
			deliveredBlocks = append(deliveredBlocks, block)
		case <-time.After(5 * time.Second):
			t.Fatal("Timeout waiting for block delivery")
		}
	}

	// Verify strict ordering: blocks should be delivered in chain order
	assert.Len(t, deliveredBlocks, 5)
	for i, block := range deliveredBlocks {
		expectedHash := blocks[i+1].Header.Hash().String()
		actualHash := block.Header.Hash().String()
		assert.Equal(t, expectedHash, actualHash, "Block %d should be delivered in correct order", i)
	}
}

// TestFetchSingleBlock_ImprovedErrorHandling tests improved error handling in fetchSingleBlock
func TestFetchSingleBlock_ImprovedErrorHandling(t *testing.T) {
	logger := ulogger.TestLogger{}
	server := &Server{
		logger:   logger,
		settings: test.CreateBaseTestSettings(t),
	}

	t.Run("Block Creation Failure with Better Context", func(t *testing.T) {
		httpmock.ActivateNonDefault(util.HTTPClient())
		defer httpmock.DeactivateAndReset()

		hash := createTestHash("test")

		// Mock HTTP response with invalid block data
		httpmock.RegisterResponder(
			"GET",
			fmt.Sprintf("http://test-peer/block/%s", hash.String()),
			httpmock.NewBytesResponder(200, []byte("invalid_block_data")),
		)

		block, err := server.fetchSingleBlock(context.Background(), hash, "12D3KooWL1NF6fdTJ9cucEuwvuX8V8KtpJZZnUE4umdLBuK15eUZ", "http://test-peer")

		// Should fail with better error context
		assert.Error(t, err)
		assert.Nil(t, block)
		assert.Contains(t, err.Error(), "failed to create block from bytes")
		// Should not contain raw bytes in error message
		assert.NotContains(t, err.Error(), "invalid_block_data")
	})
}

// Helper function to create test hashes
func createTestHash(input string) *chainhash.Hash {
	hash := chainhash.DoubleHashH([]byte(input))
	return &hash
}

// TestFetchAndStoreSubtree tests the fetchAndStoreSubtree function comprehensively
func TestFetchAndStoreSubtree(t *testing.T) {
	t.Run("SubtreeAlreadyExists", func(t *testing.T) {
		suite := NewCatchupTestSuite(t)
		defer suite.Cleanup()

		// Create a test subtree
		subtree, err := subtreepkg.NewIncompleteTreeByLeafCount(4)
		require.NoError(t, err)

		// Add some nodes
		hash1 := chainhash.DoubleHashH([]byte("tx1"))
		hash2 := chainhash.DoubleHashH([]byte("tx2"))
		hash3 := chainhash.DoubleHashH([]byte("tx3"))
		hash4 := chainhash.DoubleHashH([]byte("tx4"))

		require.NoError(t, subtree.AddNode(hash1, 100, 250))
		require.NoError(t, subtree.AddNode(hash2, 200, 350))
		require.NoError(t, subtree.AddNode(hash3, 150, 300))
		require.NoError(t, subtree.AddNode(hash4, 180, 400))

		subtreeBytes, err := subtree.Serialize()
		require.NoError(t, err)

		subtreeHash := chainhash.DoubleHashH(subtreeBytes)

		// Pre-store the subtree
		err = suite.Server.subtreeStore.Set(suite.Ctx, subtreeHash[:], fileformat.FileTypeSubtreeToCheck, subtreeBytes)
		require.NoError(t, err)

		// Create test block
		testBlock := &model.Block{
			Height: 100,
		}

		// Fetch the subtree (should load from store, not network)
		result, err := suite.Server.fetchAndStoreSubtree(suite.Ctx, testBlock, &subtreeHash, "12D3KooWL1NF6fdTJ9cucEuwvuX8V8KtpJZZnUE4umdLBuK15eUZ", "http://test-peer")

		assert.NoError(t, err)
		assert.NotNil(t, result)
	})

	t.Run("SubtreeDoesNotExist_FetchFromPeer", func(t *testing.T) {
		suite := NewCatchupTestSuite(t)
		defer suite.Cleanup()

		// Set up HTTP mock
		httpmock.ActivateNonDefault(util.HTTPClient())
		defer httpmock.DeactivateAndReset()

		subtreeHash := createTestHash("subtree1")

		// Create subtree node bytes (4 hashes)
		nodeBytes := make([]byte, 0)
		hash1 := chainhash.DoubleHashH([]byte("tx1"))
		hash2 := chainhash.DoubleHashH([]byte("tx2"))
		hash3 := chainhash.DoubleHashH([]byte("tx3"))
		hash4 := chainhash.DoubleHashH([]byte("tx4"))

		nodeBytes = append(nodeBytes, hash1[:]...)
		nodeBytes = append(nodeBytes, hash2[:]...)
		nodeBytes = append(nodeBytes, hash3[:]...)
		nodeBytes = append(nodeBytes, hash4[:]...)

		httpmock.RegisterResponder(
			"GET",
			fmt.Sprintf("http://test-peer/subtree/%s", subtreeHash.String()),
			httpmock.NewBytesResponder(200, nodeBytes),
		)

		testBlock := &model.Block{
			Height: 100,
		}

		result, err := suite.Server.fetchAndStoreSubtree(suite.Ctx, testBlock, subtreeHash, "12D3KooWL1NF6fdTJ9cucEuwvuX8V8KtpJZZnUE4umdLBuK15eUZ", "http://test-peer")

		assert.NoError(t, err)
		assert.NotNil(t, result)

		// Verify subtree was stored
		exists, err := suite.Server.subtreeStore.Exists(suite.Ctx, subtreeHash[:], fileformat.FileTypeSubtreeToCheck)
		assert.NoError(t, err)
		assert.True(t, exists)
	})

	t.Run("SubtreeWithCoinbaseNode", func(t *testing.T) {
		suite := NewCatchupTestSuite(t)
		defer suite.Cleanup()

		httpmock.ActivateNonDefault(util.HTTPClient())
		defer httpmock.DeactivateAndReset()

		subtreeHash := createTestHash("subtree-coinbase")

		// Create subtree node bytes with coinbase placeholder as first node
		nodeBytes := make([]byte, 0)
		nodeBytes = append(nodeBytes, subtreepkg.CoinbasePlaceholderHashValue[:]...)

		hash2 := chainhash.DoubleHashH([]byte("tx2"))
		hash3 := chainhash.DoubleHashH([]byte("tx3"))
		hash4 := chainhash.DoubleHashH([]byte("tx4"))

		nodeBytes = append(nodeBytes, hash2[:]...)
		nodeBytes = append(nodeBytes, hash3[:]...)
		nodeBytes = append(nodeBytes, hash4[:]...)

		httpmock.RegisterResponder(
			"GET",
			fmt.Sprintf("http://test-peer/subtree/%s", subtreeHash.String()),
			httpmock.NewBytesResponder(200, nodeBytes),
		)

		testBlock := &model.Block{
			Height: 100,
		}

		result, err := suite.Server.fetchAndStoreSubtree(suite.Ctx, testBlock, subtreeHash, "12D3KooWL1NF6fdTJ9cucEuwvuX8V8KtpJZZnUE4umdLBuK15eUZ", "http://test-peer")

		assert.NoError(t, err)
		assert.NotNil(t, result)
	})

	t.Run("FetchFromPeerFails", func(t *testing.T) {
		suite := NewCatchupTestSuite(t)
		defer suite.Cleanup()

		httpmock.ActivateNonDefault(util.HTTPClient())
		defer httpmock.DeactivateAndReset()

		subtreeHash := createTestHash("subtree-fail")

		httpmock.RegisterResponder(
			"GET",
			fmt.Sprintf("http://test-peer/subtree/%s", subtreeHash.String()),
			httpmock.NewErrorResponder(errors.NewNetworkError("network error")),
		)

		testBlock := &model.Block{
			Height: 100,
		}

		result, err := suite.Server.fetchAndStoreSubtree(suite.Ctx, testBlock, subtreeHash, "12D3KooWL1NF6fdTJ9cucEuwvuX8V8KtpJZZnUE4umdLBuK15eUZ", "http://test-peer")

		assert.Error(t, err)
		assert.Nil(t, result)
		assert.Contains(t, err.Error(), "Failed to fetch subtree")
	})

	t.Run("EmptySubtreeError", func(t *testing.T) {
		suite := NewCatchupTestSuite(t)
		defer suite.Cleanup()

		httpmock.ActivateNonDefault(util.HTTPClient())
		defer httpmock.DeactivateAndReset()

		subtreeHash := createTestHash("empty-subtree")

		// Return empty bytes
		httpmock.RegisterResponder(
			"GET",
			fmt.Sprintf("http://test-peer/subtree/%s", subtreeHash.String()),
			httpmock.NewBytesResponder(200, []byte{}),
		)

		testBlock := &model.Block{
			Height: 100,
		}

		result, err := suite.Server.fetchAndStoreSubtree(suite.Ctx, testBlock, subtreeHash, "12D3KooWL1NF6fdTJ9cucEuwvuX8V8KtpJZZnUE4umdLBuK15eUZ", "http://test-peer")

		assert.Error(t, err)
		assert.Nil(t, result)
		// The error is actually "empty subtree received" not "has zero nodes"
		assert.Contains(t, err.Error(), "empty subtree received")
	})

	t.Run("SubtreeExistsButFailsToLoad", func(t *testing.T) {
		suite := NewCatchupTestSuite(t)
		defer suite.Cleanup()

		subtreeHash := createTestHash("corrupt-subtree")

		// Store corrupt data
		err := suite.Server.subtreeStore.Set(suite.Ctx, subtreeHash[:], fileformat.FileTypeSubtreeToCheck, []byte("corrupt"))
		require.NoError(t, err)

		testBlock := &model.Block{
			Height: 100,
		}

		result, err := suite.Server.fetchAndStoreSubtree(suite.Ctx, testBlock, subtreeHash, "12D3KooWL1NF6fdTJ9cucEuwvuX8V8KtpJZZnUE4umdLBuK15eUZ", "http://test-peer")

		assert.Error(t, err)
		assert.Nil(t, result)
		assert.Contains(t, err.Error(), "Failed to deserialize existing subtree")
	})
}

// TestFetchAndStoreSubtreeDataEdgeCases tests edge cases in fetchAndStoreSubtreeData
func TestFetchAndStoreSubtreeDataEdgeCases(t *testing.T) {
	t.Run("SubtreeDataAlreadyExists", func(t *testing.T) {
		suite := NewCatchupTestSuite(t)
		defer suite.Cleanup()

		// Create a test subtree
		subtree, err := subtreepkg.NewIncompleteTreeByLeafCount(2)
		require.NoError(t, err)

		hash1 := chainhash.DoubleHashH([]byte("tx1"))
		hash2 := chainhash.DoubleHashH([]byte("tx2"))

		require.NoError(t, subtree.AddNode(hash1, 100, 250))
		require.NoError(t, subtree.AddNode(hash2, 200, 350))

		subtreeBytes, err := subtree.Serialize()
		require.NoError(t, err)
		subtreeHash := chainhash.DoubleHashH(subtreeBytes)

		// Pre-store subtree data
		subtreeData := []byte("existing_subtree_data")
		err = suite.Server.subtreeStore.Set(suite.Ctx, subtreeHash[:], fileformat.FileTypeSubtreeData, subtreeData)
		require.NoError(t, err)

		testBlock := &model.Block{
			Height: 100,
		}

		// This should skip fetching since data already exists
		err = suite.Server.fetchAndStoreSubtreeData(suite.Ctx, testBlock, &subtreeHash, subtree, "12D3KooWL1NF6fdTJ9cucEuwvuX8V8KtpJZZnUE4umdLBuK15eUZ", "http://test-peer")
		assert.NoError(t, err)
	})
}

// TestFetchBlocksConcurrently_BlockHeightIsSet verifies that block.Height is set correctly
// during catchup block fetching (Issue #4464)
func TestFetchBlocksConcurrently_BlockHeightIsSet(t *testing.T) {
	t.Run("Block_Height_Should_Be_Set_During_Catchup", func(t *testing.T) {
		suite := NewCatchupTestSuite(t)
		defer suite.Cleanup()

		// Create test blocks at specific heights
		numBlocks := 5
		startingHeight := uint32(100) // Common ancestor at height 99
		blocks := testhelpers.CreateTestBlockChain(t, numBlocks+1)
		targetBlock := blocks[numBlocks]

		// Set up HTTP mocks for batch fetching
		httpmock.ActivateNonDefault(util.HTTPClient())
		defer httpmock.DeactivateAndReset()

		// Mock batch request
		batchData := bytes.Buffer{}
		for i := numBlocks; i >= 1; i-- {
			blockBytes, err := blocks[i].Bytes()
			require.NoError(t, err)
			batchData.Write(blockBytes)
		}

		httpmock.RegisterResponder("GET",
			fmt.Sprintf("http://test-peer/blocks/%s?n=%d",
				blocks[numBlocks].Header.Hash().String(), numBlocks),
			httpmock.NewBytesResponder(200, batchData.Bytes()))

		var headers []*model.BlockHeader
		for i := 1; i <= numBlocks; i++ {
			headers = append(headers, blocks[i].Header)
		}

		var size atomic.Int64
		size.Store(int64(numBlocks))
		validateBlocksChan := make(chan *model.Block, numBlocks)

		catchupCtx := &CatchupContext{
			blockUpTo:    targetBlock,
			baseURL:      "http://test-peer",
			blockHeaders: headers,
			commonAncestorMeta: &model.BlockHeaderMeta{
				Height: startingHeight - 1, // Height 99
			},
		}

		err := suite.Server.fetchBlocksConcurrently(suite.Ctx, catchupCtx,
			validateBlocksChan, &size)
		require.NoError(t, err)

		// Collect blocks and verify heights
		for i := 0; i < numBlocks; i++ {
			select {
			case block := <-validateBlocksChan:
				expectedHeight := startingHeight + uint32(i)
				assert.Equal(t, expectedHeight, block.Height,
					"Block %d should have height %d, got %d",
					i, expectedHeight, block.Height)
			case <-time.After(time.Second):
				t.Fatalf("Timeout waiting for block %d/%d", i+1, numBlocks)
			}
		}
	})
}
