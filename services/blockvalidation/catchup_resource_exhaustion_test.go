package blockvalidation

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/services/blockvalidation/testhelpers"
	"github.com/bsv-blockchain/teranode/util"
	"github.com/jarcoal/httpmock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// TestCatchup_MemoryExhaustionAttack tests protection against peers sending massive amounts of headers
func TestCatchup_MemoryExhaustionAttack(t *testing.T) {
	t.Run("RejectMassiveHeaderPayload", func(t *testing.T) {
		ctx, cancel := testhelpers.CreateTestContext(t, 30*time.Second)
		defer cancel()
		server, mockBlockchainClient, _, cleanup := setupTestCatchupServer(t)
		defer cleanup()

		// Track memory before test
		var memStatsBefore runtime.MemStats
		runtime.ReadMemStats(&memStatsBefore)

		nBits, err := model.NewNBitFromString("207fffff") // minimum difficulty
		require.NoError(t, err, "Failed to create NBit")

		// Create target block far in the future
		targetBlock := &model.Block{
			Header: &model.BlockHeader{
				Version:        1,
				HashPrevBlock:  &chainhash.Hash{1},
				HashMerkleRoot: &chainhash.Hash{2},
				Timestamp:      uint32(time.Now().Unix()),
				Bits:           *nBits,
				Nonce:          12345,
			},
			Height: 10000000, // 10 million blocks ahead
		}
		targetHash := targetBlock.Hash()

		testhelpers.MineHeader(targetBlock.Header)

		// Mock GetBlockExists - we don't have this block
		mockBlockchainClient.On("GetBlockExists", mock.Anything, targetHash).
			Return(false, nil)

		// Mock best block (we're at block 1000)
		bestBlockHeader := &model.BlockHeader{
			Version:        1,
			HashPrevBlock:  &chainhash.Hash{},
			HashMerkleRoot: &chainhash.Hash{},
			Timestamp:      uint32(time.Now().Unix()),
			Bits:           model.NBit{},
			Nonce:          0,
		}
		mockBlockchainClient.On("GetBestBlockHeader", mock.Anything).
			Return(bestBlockHeader, &model.BlockHeaderMeta{Height: 1000}, nil)

		// Mock block locator
		mockBlockchainClient.On("GetBlockLocator", mock.Anything, bestBlockHeader.Hash(), mock.Anything).
			Return([]*chainhash.Hash{bestBlockHeader.Hash()}, nil)

		// Mock GetBlockHeaders for common ancestor finding
		mockBlockchainClient.On("GetBlockHeaders", mock.Anything, mock.Anything, mock.Anything).
			Return([]*model.BlockHeader{bestBlockHeader}, []*model.BlockHeaderMeta{{Height: 1000, ID: 1}}, nil).Maybe()

		// Mock GetBlockHeader for common ancestor finding
		mockBlockchainClient.On("GetBlockHeader", mock.Anything, bestBlockHeader.Hash()).
			Return(bestBlockHeader, &model.BlockHeaderMeta{Height: 1000, ID: 1}, nil).Maybe()

		// Mock GetBlockExists for best block header (it already exists)
		mockBlockchainClient.On("GetBlockExists", mock.Anything, bestBlockHeader.Hash()).
			Return(true, nil).Maybe()

		// Setup HTTP mock
		httpmock.ActivateNonDefault(util.HTTPClient())
		defer httpmock.DeactivateAndReset()

		requestCount := 0
		httpmock.RegisterResponder(
			"GET",
			`=~^http://malicious-peer/headers_from_common_ancestor/.*`,
			func(req *http.Request) (*http.Response, error) {
				requestCount++

				// Simulate malicious peer trying to send 10 million headers
				// Each header is 80 bytes, 10M headers = 800MB
				// But we'll simulate the attack by starting to send them
				const headersToSend = 10000000
				const headerSize = 80

				// Create a massive header response (but limit for test practicality)
				// In production, this would be 800MB+
				testHeaders := make([]byte, 0, headerSize*1000) // Start with 1000 headers
				prevBlockHash := chainhash.HashH([]byte("prev-block-hash"))

				for i := 0; i < 1000; i++ {
					// Create a fake header (80 bytes)
					header := &model.BlockHeader{
						Version:        1,
						HashPrevBlock:  &prevBlockHash,
						HashMerkleRoot: &chainhash.Hash{byte(i + 1), byte(i >> 8)},
						Timestamp:      uint32(time.Now().Unix() + int64(i)),
						Bits:           *nBits,
						Nonce:          uint32(i),
					}

					testhelpers.MineHeader(header)

					prevBlockHash = *header.Hash()

					testHeaders = append(testHeaders, header.Bytes()...)
				}

				// Add a header indicating there are millions more coming
				resp := httpmock.NewBytesResponse(200, testHeaders)
				resp.Header.Set("X-Total-Headers", fmt.Sprintf("%d", headersToSend))
				return resp, nil
			},
		)

		// Execute catchup - should fail due to invalid headers
		err = server.catchup(ctx, targetBlock, "", "http://malicious-peer")
		require.Error(t, err, "Catchup should fail with invalid headers")

		// Check memory after test - should not have grown excessively
		var memStatsAfter runtime.MemStats
		runtime.ReadMemStats(&memStatsAfter)

		// Calculate memory growth (handle potential overflow)
		var memGrowth uint64
		if memStatsAfter.Alloc > memStatsBefore.Alloc {
			memGrowth = memStatsAfter.Alloc - memStatsBefore.Alloc
			// Memory growth should be limited (less than 100MB for safety)
			assert.Less(t, memGrowth, uint64(100*1024*1024),
				"Memory grew by %d bytes, should be limited", memGrowth)
		}

		// Note: peerMetrics field has been removed from Server struct
		// (malicious peer metrics logging disabled)

		// Circuit breaker might not be initialized in this test setup
		if server.peerCircuitBreakers != nil {
			state := server.peerCircuitBreakers.GetPeerState("peer-malicious-002")
			t.Logf("Circuit breaker state: %v", state)
		}
	})

	t.Run("HandleLargeButValidHeaderChain", func(t *testing.T) {
		ctx, cancel := testhelpers.CreateTestContext(t, 30*time.Second)
		defer cancel()
		server, mockBlockchainClient, _, cleanup := setupTestCatchupServer(t)
		defer cleanup()

		// Test with a large but reasonable number of headers (5000)
		targetBlock := &model.Block{
			Header: &model.BlockHeader{
				Version:        1,
				HashPrevBlock:  &chainhash.Hash{1},
				HashMerkleRoot: &chainhash.Hash{2},
				Timestamp:      uint32(time.Now().Unix()),
				Bits:           model.NBit{},
				Nonce:          12345,
			},
			Height: 6000,
		}
		targetHash := targetBlock.Hash()

		mockBlockchainClient.On("GetBlockExists", mock.Anything, targetHash).
			Return(false, nil)

		bestBlockHeader := &model.BlockHeader{
			Version:        1,
			HashPrevBlock:  &chainhash.Hash{},
			HashMerkleRoot: &chainhash.Hash{},
			Timestamp:      uint32(time.Now().Unix()),
			Bits:           model.NBit{},
			Nonce:          0,
		}
		mockBlockchainClient.On("GetBestBlockHeader", mock.Anything).
			Return(bestBlockHeader, &model.BlockHeaderMeta{Height: 1000}, nil)

		mockBlockchainClient.On("GetBlockLocator", mock.Anything, mock.Anything, mock.Anything).
			Return([]*chainhash.Hash{bestBlockHeader.Hash()}, nil)

		// Mock GetBlockHeader for common ancestor lookup
		mockBlockchainClient.On("GetBlockHeader", mock.Anything, mock.Anything).
			Return(&model.BlockHeader{}, &model.BlockHeaderMeta{Height: 1000}, nil).Maybe()

		httpmock.ActivateNonDefault(util.HTTPClient())
		defer httpmock.DeactivateAndReset()

		// Create a large but valid chain of headers
		httpmock.RegisterResponder(
			"GET",
			`=~^http://honest-peer/headers_from_common_ancestor/.*`,
			func(req *http.Request) (*http.Response, error) {
				// Return headers in chunks to simulate normal operation
				headers := make([]byte, 0, 80*2000) // 2000 headers per request
				for i := 0; i < 2000; i++ {
					header := &model.BlockHeader{
						Version:        1,
						HashPrevBlock:  &chainhash.Hash{byte(i), byte(i >> 8)},
						HashMerkleRoot: &chainhash.Hash{byte(i + 1), byte(i >> 8)},
						Timestamp:      uint32(time.Now().Unix() + int64(i)),
						Bits:           model.NBit{},
						Nonce:          uint32(i),
					}
					headers = append(headers, header.Bytes()...)

					// Mock these headers don't exist
					mockBlockchainClient.On("GetBlockExists", mock.Anything, header.Hash()).
						Return(false, nil).Maybe()
				}
				return httpmock.NewBytesResponse(200, headers), nil
			},
		)

		// Should handle large valid chains without issues
		err := server.catchup(ctx, targetBlock, "", "http://honest-peer")

		// May have other errors but should not be memory-related
		if err != nil {
			assert.NotContains(t, err.Error(), "memory")
			assert.NotContains(t, err.Error(), "exhaustion")
		}
	})
}

// TestCatchup_CPUExhaustion tests protection against CPU exhaustion from concurrent catchups
func TestCatchup_CPUExhaustion(t *testing.T) {
	t.Run("Handle100ConcurrentCatchups", func(t *testing.T) {
		ctx, cancel := testhelpers.CreateTestContext(t, 30*time.Second)
		defer cancel()
		server, mockBlockchainClient, mockUTXOStore, cleanup := setupTestCatchupServer(t)
		defer cleanup()

		// Mock UTXO store block height
		mockUTXOStore.On("GetBlockHeight").Return(uint32(1000))

		// Track CPU usage before test
		initialCPU := runtime.NumGoroutine()

		nBits, err := model.NewNBitFromString("207fffff") // minimum difficulty
		require.NoError(t, err, "Failed to create NBit")

		// Create 100 different target blocks
		targetBlocks := make([]*model.Block, 100)
		for i := 0; i < 100; i++ {
			targetBlocks[i] = &model.Block{
				Header: &model.BlockHeader{
					Version:        1,
					HashPrevBlock:  &chainhash.Hash{byte(i)},
					HashMerkleRoot: &chainhash.Hash{byte(i + 1)},
					Timestamp:      uint32(time.Now().Unix() + int64(i)),
					Bits:           *nBits,
					Nonce:          uint32(i),
				},
				Height: uint32(2000 + i),
			}

			testhelpers.MineHeader(targetBlocks[i].Header)
		}

		mockBlockchainClient.On("GetBlockExists", mock.Anything, mock.Anything).
			Return(false, nil).Maybe()

		mockBlockchainClient.On("GetBlockHeader", mock.Anything, mock.Anything).
			Return(&model.BlockHeader{}, &model.BlockHeaderMeta{}, errors.NewBlockNotFoundError("not found")).Maybe()

		// Mock best block
		bestBlockHeader := &model.BlockHeader{
			Version:        1,
			HashPrevBlock:  &chainhash.Hash{},
			HashMerkleRoot: &chainhash.Hash{},
			Timestamp:      uint32(time.Now().Unix()),
			Bits:           model.NBit{},
			Nonce:          0,
		}
		mockBlockchainClient.On("GetBestBlockHeader", mock.Anything).
			Return(bestBlockHeader, &model.BlockHeaderMeta{Height: 1000}, nil).Maybe()

		mockBlockchainClient.On("GetBlockLocator", mock.Anything, mock.Anything, mock.Anything).
			Return([]*chainhash.Hash{bestBlockHeader.Hash()}, nil).Maybe()

		mockBlockchainClient.On("GetBlockExists", mock.Anything, mock.Anything).
			Return(false, nil).Maybe()

		// Setup HTTP mock
		httpmock.ActivateNonDefault(util.HTTPClient())
		defer httpmock.DeactivateAndReset()

		// Track active requests
		var activeRequests int32
		var maxConcurrent int32

		httpmock.RegisterResponder(
			"GET",
			`=~^http://peer-.*`,
			func(req *http.Request) (*http.Response, error) {
				// Track concurrent requests
				current := atomic.AddInt32(&activeRequests, 1)
				if current > atomic.LoadInt32(&maxConcurrent) {
					atomic.StoreInt32(&maxConcurrent, current)
				}

				// Simulate some processing time
				time.Sleep(10 * time.Millisecond)

				nBits, err := model.NewNBitFromString("207fffff") // minimum difficulty
				require.NoError(t, err)

				// Create a simple header response
				header := &model.BlockHeader{
					Version:        1,
					HashPrevBlock:  &chainhash.Hash{1},
					HashMerkleRoot: &chainhash.Hash{2},
					Timestamp:      uint32(time.Now().Unix()),
					Bits:           *nBits,
					Nonce:          1,
				}

				testhelpers.MineHeader(header)

				atomic.AddInt32(&activeRequests, -1)
				return httpmock.NewBytesResponse(200, header.Bytes()), nil
			},
		)

		// Launch 100 concurrent catchup operations
		var wg sync.WaitGroup
		successCount := int32(0)
		errorCount := int32(0)
		rejectedCount := int32(0)

		for i := 0; i < 100; i++ {
			wg.Add(1)
			go func(idx int) {
				defer wg.Done()

				peerURL := fmt.Sprintf("http://peer-%d", idx)
				if err := server.catchup(ctx, targetBlocks[idx], "", peerURL); err != nil {
					// Check if error indicates resource exhaustion
					if strings.Contains(err.Error(), "another catchup is currently in progress") {
						atomic.AddInt32(&rejectedCount, 1)
					} else {
						atomic.AddInt32(&errorCount, 1)
					}
				} else {
					atomic.AddInt32(&successCount, 1)
				}
			}(i)
		}

		// Wait for all to complete
		wg.Wait()

		// Check resource limits were enforced
		t.Logf("Success: %d,er  Errors: %d, Rejected: %d, Max Concurrent: %d",
			successCount, errorCount, rejectedCount, maxConcurrent)

		// System should have protected itself
		assert.Greater(t, rejectedCount, int32(0),
			"Some requests should be rejected to prevent exhaustion")

		// Concurrent requests should be limited
		assert.LessOrEqual(t, maxConcurrent, int32(10),
			"Should limit concurrent catchup operations")

		// Check goroutine count didn't explode
		finalCPU := runtime.NumGoroutine()
		goroutineGrowth := finalCPU - initialCPU
		assert.Less(t, goroutineGrowth, 200,
			"Goroutine count should be controlled (grew by %d)", goroutineGrowth)
	})
}

// TestCatchup_SlowLorisAttack tests protection against slow drip-feed attacks
func TestCatchup_SlowLorisAttack(t *testing.T) {
	t.Run("DetectAndTimeoutSlowPeer", func(t *testing.T) {
		ctx, cancel := testhelpers.CreateTestContext(t, 30*time.Second)
		defer cancel()

		server, mockBlockchainClient, mockUTXOStore, cleanup := setupTestCatchupServer(t)
		defer cleanup()

		// Mock UTXO store block height
		mockUTXOStore.On("GetBlockHeight").Return(uint32(1000))

		// Set aggressive timeout for testing
		server.settings.BlockValidation.CatchupIterationTimeout = 1 // 1 second

		targetBlock := &model.Block{
			Header: &model.BlockHeader{
				Version:        1,
				HashPrevBlock:  &chainhash.Hash{1},
				HashMerkleRoot: &chainhash.Hash{2},
				Timestamp:      uint32(time.Now().Unix()),
				Bits:           model.NBit{},
				Nonce:          12345,
			},
			Height: 2000,
		}
		targetHash := targetBlock.Hash()

		mockBlockchainClient.On("GetBlockExists", mock.Anything, targetHash).
			Return(false, nil)

		bestBlockHeader := &model.BlockHeader{
			Version:        1,
			HashPrevBlock:  &chainhash.Hash{},
			HashMerkleRoot: &chainhash.Hash{},
			Timestamp:      uint32(time.Now().Unix()),
			Bits:           model.NBit{},
			Nonce:          0,
		}
		mockBlockchainClient.On("GetBestBlockHeader", mock.Anything).
			Return(bestBlockHeader, &model.BlockHeaderMeta{Height: 1000}, nil)

		mockBlockchainClient.On("GetBlockLocator", mock.Anything, mock.Anything, mock.Anything).
			Return([]*chainhash.Hash{bestBlockHeader.Hash()}, nil)

		httpmock.ActivateNonDefault(util.HTTPClient())
		defer httpmock.DeactivateAndReset()

		// Mock GetBlockHeader for common ancestor finding
		mockBlockchainClient.On("GetBlockHeader", mock.Anything, bestBlockHeader.Hash()).
			Return(bestBlockHeader, &model.BlockHeaderMeta{Height: 1000, ID: 1}, nil).Maybe()

		// Mock GetBlockExists for best block header (it already exists)
		mockBlockchainClient.On("GetBlockExists", mock.Anything, bestBlockHeader.Hash()).
			Return(true, nil).Maybe()

		// Mock GetBlockExists for any other hash (new headers from slow peer)
		// This is needed because FilterNewHeaders will check if headers exist
		mockBlockchainClient.On("GetBlockExists", mock.Anything, mock.Anything).
			Return(false, nil).Maybe()

		// Simulate slow loris attack - drip feed bytes slowly
		httpmock.RegisterResponder(
			"GET",
			`=~^http://slow-peer/headers_from_common_ancestor/.*`,
			func(req *http.Request) (*http.Response, error) {
				nBits, err := model.NewNBitFromString("207fffff") // minimum difficulty
				require.NoError(t, err)

				// Create a header
				header := &model.BlockHeader{
					Version:        1,
					HashPrevBlock:  &chainhash.Hash{1},
					HashMerkleRoot: &chainhash.Hash{2},
					Timestamp:      uint32(time.Now().Unix()),
					Bits:           *nBits,
					Nonce:          1,
				}

				testhelpers.MineHeader(header)
				headerBytes := header.Bytes()

				// Return response that will drip-feed data
				// In a real slow loris attack, this would send 1 byte per second
				resp := &http.Response{
					StatusCode: 200,
					Body: &slowReader{
						data:         headerBytes,
						delay:        200 * time.Millisecond, // Delay per read to simulate slow attack
						bytesPerRead: 1,                      // Send 1 byte at a time
					},
					Header: make(http.Header),
				}
				return resp, nil
			},
		)

		start := time.Now()
		err := server.catchup(ctx, targetBlock, "", "http://slow-peer")
		duration := time.Since(start)

		// Should timeout quickly
		assert.Error(t, err)
		assert.Less(t, duration, 5*time.Second, "Should timeout within 5 seconds, took %v", duration)

		// Verify peer was marked as problematic if circuit breaker is configured
		if server.peerCircuitBreakers != nil {
			peerState := server.peerCircuitBreakers.GetPeerState("peer-slow-001")
			// Circuit breaker might be open or half-open for slow peer
			// Note: The exact state depends on the circuit breaker configuration
			t.Logf("Circuit breaker state for slow peer: %v", peerState)
		}
	})

	t.Run("HandleNormalSlowConnection", func(t *testing.T) {
		ctx, cancel := testhelpers.CreateTestContext(t, 30*time.Second)
		defer cancel()
		server, mockBlockchainClient, mockUTXOStore, cleanup := setupTestCatchupServer(t)
		defer cleanup()

		// Mock UTXO store block height
		mockUTXOStore.On("GetBlockHeight").Return(uint32(1000))

		// Set reasonable timeout
		server.settings.BlockValidation.CatchupIterationTimeout = 5

		targetBlock := &model.Block{
			Header: &model.BlockHeader{
				Version:        1,
				HashPrevBlock:  &chainhash.Hash{1},
				HashMerkleRoot: &chainhash.Hash{2},
				Timestamp:      uint32(time.Now().Unix()),
				Bits:           model.NBit{},
				Nonce:          12345,
			},
			Height: 1100,
		}
		targetHash := targetBlock.Hash()

		mockBlockchainClient.On("GetBlockExists", mock.Anything, targetHash).
			Return(false, nil)

		bestBlockHeader := &model.BlockHeader{
			Version:        1,
			HashPrevBlock:  &chainhash.Hash{},
			HashMerkleRoot: &chainhash.Hash{},
			Timestamp:      uint32(time.Now().Unix()),
			Bits:           model.NBit{},
			Nonce:          0,
		}
		mockBlockchainClient.On("GetBestBlockHeader", mock.Anything).
			Return(bestBlockHeader, &model.BlockHeaderMeta{Height: 1000}, nil)

		mockBlockchainClient.On("GetBlockLocator", mock.Anything, mock.Anything, mock.Anything).
			Return([]*chainhash.Hash{bestBlockHeader.Hash()}, nil)

		// Mock GetBlockHeader for validation
		mockBlockchainClient.On("GetBlockHeader", mock.Anything, mock.Anything).
			Return(&model.BlockHeader{}, &model.BlockHeaderMeta{Height: 1000}, nil).Maybe()

		httpmock.ActivateNonDefault(util.HTTPClient())
		defer httpmock.DeactivateAndReset()

		// Simulate normally slow but legitimate peer
		httpmock.RegisterResponder(
			"GET",
			`=~^http://legitimate-slow-peer/headers_from_common_ancestor/.*`,
			func(req *http.Request) (*http.Response, error) {
				// Small delay to simulate slow connection
				time.Sleep(100 * time.Millisecond)

				// Return 10 headers
				headers := make([]byte, 0)
				for i := 0; i < 10; i++ {
					header := &model.BlockHeader{
						Version:        1,
						HashPrevBlock:  &chainhash.Hash{byte(i)},
						HashMerkleRoot: &chainhash.Hash{byte(i + 1)},
						Timestamp:      uint32(time.Now().Unix() + int64(i)),
						Bits:           model.NBit{},
						Nonce:          uint32(i),
					}
					headers = append(headers, header.Bytes()...)

					// Mock that these headers don't exist
					mockBlockchainClient.On("GetBlockExists", mock.Anything, header.Hash()).
						Return(false, nil).Maybe()
				}

				return httpmock.NewBytesResponse(200, headers), nil
			},
		)

		start := time.Now()
		err := server.catchup(ctx, targetBlock, "", "http://legitimate-slow-peer")
		duration := time.Since(start)

		// Should complete successfully despite being slow
		if err != nil {
			// Check it's not a timeout error
			assert.NotContains(t, err.Error(), "timeout")
			assert.NotContains(t, err.Error(), "deadline")
		}

		// Should complete within reasonable time
		assert.Less(t, duration, 10*time.Second,
			"Should complete within 10 seconds for legitimate slow peer")
	})
}

// slowReader simulates a slow network connection that drip-feeds data
type slowReader struct {
	data         []byte
	offset       int
	delay        time.Duration
	bytesPerRead int
}

func (r *slowReader) Read(p []byte) (n int, err error) {
	if r.offset >= len(r.data) {
		return 0, io.EOF
	}

	// Delay to simulate slow connection
	time.Sleep(r.delay)

	// Read limited bytes
	toRead := r.bytesPerRead
	if toRead > len(p) {
		toRead = len(p)
	}
	if r.offset+toRead > len(r.data) {
		toRead = len(r.data) - r.offset
	}

	copy(p[:toRead], r.data[r.offset:r.offset+toRead])
	r.offset += toRead
	return toRead, nil
}

func (r *slowReader) Close() error {
	return nil
}

// TestCatchup_MemoryMonitoring tests that memory usage is properly tracked during catchup
func TestCatchup_MemoryMonitoring(t *testing.T) {
	t.Run("TrackMemoryDuringNormalOperation", func(t *testing.T) {
		ctx, cancel := testhelpers.CreateTestContext(t, 30*time.Second)
		defer cancel()
		server, mockBlockchainClient, _, cleanup := setupTestCatchupServer(t)
		defer cleanup()

		// Force garbage collection to get baseline
		runtime.GC()
		var baselineStats runtime.MemStats
		runtime.ReadMemStats(&baselineStats)

		// Create test scenario with 1000 headers
		targetBlock := &model.Block{
			Header: &model.BlockHeader{
				Version:        1,
				HashPrevBlock:  &chainhash.Hash{1},
				HashMerkleRoot: &chainhash.Hash{2},
				Timestamp:      uint32(time.Now().Unix()),
				Bits:           model.NBit{},
				Nonce:          12345,
			},
			Height: 2000,
		}
		targetHash := targetBlock.Hash()

		mockBlockchainClient.On("GetBlockExists", mock.Anything, targetHash).
			Return(false, nil)

		bestBlockHeader := &model.BlockHeader{
			Version:        1,
			HashPrevBlock:  &chainhash.Hash{},
			HashMerkleRoot: &chainhash.Hash{},
			Timestamp:      uint32(time.Now().Unix()),
			Bits:           model.NBit{},
			Nonce:          0,
		}
		mockBlockchainClient.On("GetBestBlockHeader", mock.Anything).
			Return(bestBlockHeader, &model.BlockHeaderMeta{Height: 1000}, nil)

		mockBlockchainClient.On("GetBlockLocator", mock.Anything, mock.Anything, mock.Anything).
			Return([]*chainhash.Hash{bestBlockHeader.Hash()}, nil)

		mockBlockchainClient.On("GetBlockHeader", mock.Anything, mock.Anything).
			Return(&model.BlockHeader{}, &model.BlockHeaderMeta{Height: 1000}, nil).Maybe()

		// Mock GetBlockExists for best block header (it already exists)
		mockBlockchainClient.On("GetBlockExists", mock.Anything, bestBlockHeader.Hash()).
			Return(true, nil).Maybe()

		httpmock.ActivateNonDefault(util.HTTPClient())
		defer httpmock.DeactivateAndReset()

		// Track peak memory during request using atomic operations
		var peakMemory uint64
		atomic.StoreUint64(&peakMemory, baselineStats.Alloc)
		memoryCheckDone := make(chan bool)

		// Start memory monitoring goroutine
		go func() {
			ticker := time.NewTicker(10 * time.Millisecond)
			defer ticker.Stop()

			for {
				select {
				case <-ticker.C:
					var stats runtime.MemStats
					runtime.ReadMemStats(&stats)
					current := atomic.LoadUint64(&peakMemory)
					if stats.Alloc > current {
						atomic.StoreUint64(&peakMemory, stats.Alloc)
					}
				case <-memoryCheckDone:
					return
				}
			}
		}()

		httpmock.RegisterResponder(
			"GET",
			`=~^http://test-peer/headers_from_common_ancestor/.*`,
			func(req *http.Request) (*http.Response, error) {
				nBits, err := model.NewNBitFromString("207fffff") // minimum difficulty
				require.NoError(t, err)

				// Create 1000 headers (80KB)
				headers := make([]byte, 0, 80*1000)
				for i := 0; i < 1000; i++ {
					header := &model.BlockHeader{
						Version:        1,
						HashPrevBlock:  &chainhash.Hash{byte(i), byte(i >> 8)},
						HashMerkleRoot: &chainhash.Hash{byte(i + 1), byte(i >> 8)},
						Timestamp:      uint32(time.Now().Unix() + int64(i)),
						Bits:           *nBits,
						Nonce:          uint32(i),
					}
					testhelpers.MineHeader(header) // Mine the header to make it valid
					headers = append(headers, header.Bytes()...)

					mockBlockchainClient.On("GetBlockExists", mock.Anything, header.Hash()).
						Return(false, nil).Maybe()
				}
				return httpmock.NewBytesResponse(200, headers), nil
			},
		)

		// Execute catchup
		_ = server.catchup(ctx, targetBlock, "", "http://test-peer")

		// Stop memory monitoring
		close(memoryCheckDone)

		// Check memory growth (handle potential underflow)
		var memoryGrowth int64
		finalPeakMemory := atomic.LoadUint64(&peakMemory)
		if finalPeakMemory > baselineStats.Alloc {
			memoryGrowth = int64(finalPeakMemory - baselineStats.Alloc)
		} else {
			// If peak is less than baseline, there was no growth (GC may have run)
			memoryGrowth = 0
		}
		t.Logf("Memory growth during catchup: %d bytes (%.2f MB)",
			memoryGrowth, float64(memoryGrowth)/(1024*1024))

		// For 1000 headers (80KB), memory growth should be reasonable
		// Allow for overhead but should be less than 10MB
		assert.Less(t, memoryGrowth, int64(10*1024*1024),
			"Memory growth should be reasonable for 1000 headers")
	})
}

// TestCatchup_ResourceCleanup tests that resources are properly cleaned up after catchup
func TestCatchup_ResourceCleanup(t *testing.T) {
	t.Run("CleanupAfterSuccessfulCatchup", func(t *testing.T) {
		ctx, cancel := testhelpers.CreateTestContext(t, 30*time.Second)
		defer cancel()
		server, mockBlockchainClient, _, cleanup := setupTestCatchupServer(t)
		defer cleanup()

		// Track goroutines before
		goroutinesBefore := runtime.NumGoroutine()

		targetBlock := &model.Block{
			Header: &model.BlockHeader{
				Version:        1,
				HashPrevBlock:  &chainhash.Hash{1},
				HashMerkleRoot: &chainhash.Hash{2},
				Timestamp:      uint32(time.Now().Unix()),
				Bits:           model.NBit{},
				Nonce:          12345,
			},
			Height: 1001,
		}
		targetHash := targetBlock.Hash()

		mockBlockchainClient.On("GetBlockExists", mock.Anything, targetHash).
			Return(true, nil) // Already have this block

		// Execute catchup (should return immediately)
		err := server.catchup(ctx, targetBlock, "", "http://test-peer")
		require.NoError(t, err)

		// Give time for goroutines to cleanup
		time.Sleep(100 * time.Millisecond)

		// Check goroutines after
		goroutinesAfter := runtime.NumGoroutine()
		goroutineLeakage := goroutinesAfter - goroutinesBefore

		assert.LessOrEqual(t, goroutineLeakage, 2,
			"Should not leak goroutines (leaked %d)", goroutineLeakage)
	})

	t.Run("CleanupAfterFailedCatchup", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		server, mockBlockchainClient, _, cleanup := setupTestCatchupServer(t)
		defer cleanup()

		goroutinesBefore := runtime.NumGoroutine()

		targetBlock := &model.Block{
			Header: &model.BlockHeader{
				Version:        1,
				HashPrevBlock:  &chainhash.Hash{1},
				HashMerkleRoot: &chainhash.Hash{2},
				Timestamp:      uint32(time.Now().Unix()),
				Bits:           model.NBit{},
				Nonce:          12345,
			},
			Height: 2000,
		}
		targetHash := targetBlock.Hash()

		mockBlockchainClient.On("GetBlockExists", mock.Anything, targetHash).
			Return(false, nil)

		bestBlockHeader := &model.BlockHeader{
			Version:        1,
			HashPrevBlock:  &chainhash.Hash{},
			HashMerkleRoot: &chainhash.Hash{},
			Timestamp:      uint32(time.Now().Unix()),
			Bits:           model.NBit{},
			Nonce:          0,
		}
		mockBlockchainClient.On("GetBestBlockHeader", mock.Anything).
			Return(bestBlockHeader, &model.BlockHeaderMeta{Height: 1000}, nil)

		mockBlockchainClient.On("GetBlockLocator", mock.Anything, mock.Anything, mock.Anything).
			Return([]*chainhash.Hash{bestBlockHeader.Hash()}, nil)

		httpmock.ActivateNonDefault(util.HTTPClient())
		defer httpmock.DeactivateAndReset()

		httpmock.RegisterResponder(
			"GET",
			`=~^http://test-peer/headers_from_common_ancestor/.*`,
			func(req *http.Request) (*http.Response, error) {
				// Cancel context to simulate failure
				cancel()
				return nil, errors.New(errors.ERR_NETWORK_ERROR, "network error simulation")
			},
		)

		// Execute catchup (should fail)
		err := server.catchup(ctx, targetBlock, "", "http://test-peer")
		assert.Error(t, err)

		// Give time for cleanup
		time.Sleep(100 * time.Millisecond)

		goroutinesAfter := runtime.NumGoroutine()
		goroutineLeakage := goroutinesAfter - goroutinesBefore

		assert.LessOrEqual(t, goroutineLeakage, 2,
			"Should cleanup goroutines even after failure (leaked %d)", goroutineLeakage)
	})
}
