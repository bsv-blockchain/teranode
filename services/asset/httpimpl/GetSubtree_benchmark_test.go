package httpimpl

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"runtime"
	"runtime/pprof"
	"sync"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/services/asset/repository"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/stores/blob/memory"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/labstack/echo/v4"
	"github.com/stretchr/testify/require"
)

const (
	benchmarkSubtreeSize = 1024 * 1024 // 1 million items
	defaultIterations    = 10
)

// benchmarkSetup holds the common setup for subtree benchmarks.
type benchmarkSetup struct {
	httpServer   *HTTP
	rootHash     *chainhash.Hash
	subtreeBytes []byte
	benchStart   time.Time
	subtreeSize  int
}

// skipIfNotEnabled skips the test if RUN_SUBTREE_BENCHMARK is not set.
func skipIfNotEnabled(t *testing.T) {
	t.Skip("Skipping benchmark test. Set RUN_SUBTREE_BENCHMARK=true to run.")
}

// printBenchmarkHeader prints the benchmark header with system info.
func printBenchmarkHeader(name string, subtreeSize int) {
	fmt.Printf("%s\n", name)
	for range len(name) {
		fmt.Print("=")
	}
	fmt.Println()
	fmt.Printf("Subtree Size:     %d items\n", subtreeSize)
	fmt.Printf("CPU Cores:        %d\n", runtime.NumCPU())
	fmt.Printf("GOMAXPROCS:       %d\n", runtime.GOMAXPROCS(0))
	fmt.Println()
}

// createLargeSubtree creates a subtree with the specified number of items.
func createLargeSubtree(t *testing.T, size int, benchStart time.Time) (*subtreepkg.Subtree, []chainhash.Hash) {
	fmt.Printf("[%s] Creating subtree with %d items...\n", time.Since(benchStart), size)
	subtree, err := subtreepkg.NewTreeByLeafCount(size)
	require.NoError(t, err)

	// Pre-generate all hashes in parallel for faster setup
	fmt.Printf("[%s] Pre-generating %d transaction hashes using %d workers...\n",
		time.Since(benchStart), size, runtime.NumCPU())

	txHashes := make([]chainhash.Hash, size)
	numWorkers := runtime.NumCPU()
	itemsPerWorker := size / numWorkers

	var wg sync.WaitGroup
	for w := 0; w < numWorkers; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			start := workerID * itemsPerWorker
			end := start + itemsPerWorker
			if workerID == numWorkers-1 {
				end = size
			}
			for i := start; i < end; i++ {
				txHashes[i] = chainhash.HashH([]byte(fmt.Sprintf("tx-%d", i)))
			}
		}(w)
	}
	wg.Wait()
	fmt.Printf("[%s] Hash pre-generation complete.\n", time.Since(benchStart))

	// Add nodes to subtree
	fmt.Printf("[%s] Adding %d nodes to subtree...\n", time.Since(benchStart), size)
	addStartTime := time.Now()
	progressInterval := size / 10
	if progressInterval < 1 {
		progressInterval = 1
	}

	for i := 0; i < size; i++ {
		err := subtree.AddNode(txHashes[i], uint64(i%10000), uint64(250))
		require.NoError(t, err)

		if (i+1)%progressInterval == 0 {
			fmt.Printf("  [%s] Added %d/%d nodes (%.1f%%)\n",
				time.Since(benchStart), i+1, size,
				100.0*float64(i+1)/float64(size))
		}
	}
	addDuration := time.Since(addStartTime)
	fmt.Printf("[%s] Node addition complete in %.2fs (%.0f nodes/sec)\n",
		time.Since(benchStart), addDuration.Seconds(), float64(size)/addDuration.Seconds())

	return subtree, txHashes
}

// serializeSubtree serializes the subtree and returns the bytes.
func serializeSubtree(t *testing.T, subtree *subtreepkg.Subtree, benchStart time.Time) []byte {
	fmt.Printf("[%s] Serializing subtree...\n", time.Since(benchStart))
	serializeStartTime := time.Now()
	subtreeBytes, err := subtree.Serialize()
	require.NoError(t, err)
	serializeDuration := time.Since(serializeStartTime)
	fmt.Printf("[%s] Serialization complete in %.2fs (%.2f MB, %.2f MB/sec)\n",
		time.Since(benchStart), serializeDuration.Seconds(),
		float64(len(subtreeBytes))/(1024*1024),
		float64(len(subtreeBytes))/(1024*1024)/serializeDuration.Seconds())
	return subtreeBytes
}

// setupBinaryBenchmark creates the full setup for BINARY_STREAM and HEX benchmarks.
func setupBinaryBenchmark(t *testing.T, subtreeSize int) *benchmarkSetup {
	initPrometheusMetrics()

	benchStart := time.Now()
	fmt.Printf("[%s] Setting up in-memory blob store...\n", time.Since(benchStart))
	subtreeStore := memory.New()

	subtree, _ := createLargeSubtree(t, subtreeSize, benchStart)
	subtreeBytes := serializeSubtree(t, subtree, benchStart)

	rootHash := subtree.RootHash()
	require.NotNil(t, rootHash)
	fmt.Printf("[%s] Subtree root hash: %s\n", time.Since(benchStart), rootHash.String())

	// Store the subtree in the blob store
	fmt.Printf("[%s] Writing subtree to blob store...\n", time.Since(benchStart))
	storeStartTime := time.Now()
	err := subtreeStore.Set(context.Background(), rootHash.CloneBytes(), fileformat.FileTypeSubtree, subtreeBytes)
	require.NoError(t, err)
	storeDuration := time.Since(storeStartTime)
	fmt.Printf("[%s] Store write complete in %.2fs\n", time.Since(benchStart), storeDuration.Seconds())

	// Create a repository with the subtree store
	repo := &repository.Repository{
		SubtreeStore: subtreeStore,
	}

	httpServer := &HTTP{
		logger:     ulogger.TestLogger{},
		settings:   &settings.Settings{},
		repository: repo,
		e:          echo.New(),
		startTime:  time.Now(),
	}

	fmt.Printf("[%s] Setup complete.\n\n", time.Since(benchStart))

	return &benchmarkSetup{
		httpServer:   httpServer,
		rootHash:     rootHash,
		subtreeBytes: subtreeBytes,
		benchStart:   benchStart,
		subtreeSize:  subtreeSize,
	}
}

// setupJSONBenchmark creates the setup for JSON benchmarks using a mock repository.
func setupJSONBenchmark(t *testing.T, subtreeSize int) *benchmarkSetup {
	initPrometheusMetrics()

	benchStart := time.Now()

	subtree, _ := createLargeSubtree(t, subtreeSize, benchStart)
	subtreeBytes := serializeSubtree(t, subtree, benchStart)

	rootHash := subtree.RootHash()
	require.NotNil(t, rootHash)
	fmt.Printf("[%s] Subtree root hash: %s\n", time.Since(benchStart), rootHash.String())

	// Create a mock that returns the subtree for JSON mode
	// Note: Mock's GetSubtree ignores context, only uses the hash parameter
	mockRepo := &repository.Mock{}
	mockRepo.On("GetSubtree", rootHash).Return(subtree, nil)
	mockRepo.On("GetSubtreeTxIDsReader", rootHash).Return(
		io.NopCloser(bytes.NewReader(subtreeBytes)), nil)

	httpServer := &HTTP{
		logger:     ulogger.TestLogger{},
		settings:   &settings.Settings{},
		repository: mockRepo,
		e:          echo.New(),
		startTime:  time.Now(),
	}

	fmt.Printf("[%s] Setup complete.\n\n", time.Since(benchStart))

	return &benchmarkSetup{
		httpServer:   httpServer,
		rootHash:     rootHash,
		subtreeBytes: subtreeBytes,
		benchStart:   benchStart,
		subtreeSize:  subtreeSize,
	}
}

// benchmarkResult holds the results of a benchmark run.
type benchmarkResult struct {
	avgDuration   time.Duration
	avgBytes      int64
	totalDuration time.Duration
}

// runBenchmarkIterations runs the benchmark iterations for a given mode.
func runBenchmarkIterations(t *testing.T, setup *benchmarkSetup, mode ReadMode, iterations int) benchmarkResult {
	modeName := mode.String()
	fmt.Printf("[%s] Running %d iterations of GetSubtree (%s mode)...\n",
		time.Since(setup.benchStart), iterations, modeName)

	var totalDuration time.Duration
	var totalBytes int64

	for i := 0; i < iterations; i++ {
		req := httptest.NewRequest(http.MethodGet, "/subtree/"+setup.rootHash.String(), nil)
		rec := httptest.NewRecorder()
		c := setup.httpServer.e.NewContext(req, rec)
		c.SetPath("/subtree/:hash")
		c.SetParamNames("hash")
		c.SetParamValues(setup.rootHash.String())

		iterStart := time.Now()
		err := setup.httpServer.GetSubtree(mode)(c)
		iterDuration := time.Since(iterStart)

		require.NoError(t, err)
		require.Equal(t, http.StatusOK, rec.Code)

		responseBytes := rec.Body.Len()
		totalDuration += iterDuration
		totalBytes += int64(responseBytes)

		fmt.Printf("  [%s] Iteration %d: %.2fs, %d bytes (%.2f MB/sec)\n",
			time.Since(setup.benchStart), i+1, iterDuration.Seconds(), responseBytes,
			float64(responseBytes)/(1024*1024)/iterDuration.Seconds())
	}

	avgDuration := totalDuration / time.Duration(iterations)
	avgBytes := totalBytes / int64(iterations)

	fmt.Printf("\n[%s] %s Results:\n", time.Since(setup.benchStart), modeName)
	fmt.Printf("  Average Duration: %.3fs\n", avgDuration.Seconds())
	fmt.Printf("  Average Response Size: %.2f MB\n", float64(avgBytes)/(1024*1024))
	fmt.Printf("  Average Throughput: %.2f MB/sec\n", float64(avgBytes)/(1024*1024)/avgDuration.Seconds())

	return benchmarkResult{
		avgDuration:   avgDuration,
		avgBytes:      avgBytes,
		totalDuration: totalDuration,
	}
}

// startCPUProfile starts CPU profiling to the given file.
func startCPUProfile(t *testing.T, filename string) *os.File {
	cpuFile, err := os.Create(filename)
	require.NoError(t, err)
	err = pprof.StartCPUProfile(cpuFile)
	require.NoError(t, err)
	return cpuFile
}

// stopCPUProfile stops CPU profiling and closes the file.
func stopCPUProfile(t *testing.T, cpuFile *os.File) {
	pprof.StopCPUProfile()
	err := cpuFile.Close()
	require.NoError(t, err)
}

// writeMemoryProfile writes a heap profile to the given file.
func writeMemoryProfile(t *testing.T, filename string) {
	runtime.GC()
	memFile, err := os.Create(filename)
	require.NoError(t, err)
	err = pprof.WriteHeapProfile(memFile)
	require.NoError(t, err)
	err = memFile.Close()
	require.NoError(t, err)
}

// printProfileInfo prints information about the generated profiles.
func printProfileInfo(cpuProfile, memProfile string) {
	fmt.Printf("\nProfiles written to:\n")
	fmt.Printf("  CPU:    %s\n", cpuProfile)
	fmt.Printf("  Memory: %s\n", memProfile)
	fmt.Printf("\nTo analyze profiles:\n")
	fmt.Printf("  go tool pprof -http=:8080 %s\n", cpuProfile)
	fmt.Printf("  go tool pprof -http=:8081 %s\n", memProfile)
}

// printBenchmarkSummary prints the benchmark summary.
func printBenchmarkSummary(setup *benchmarkSetup, result benchmarkResult) {
	fmt.Printf("\n========================================\n")
	fmt.Printf("Benchmark Summary\n")
	fmt.Printf("========================================\n")
	fmt.Printf("Subtree Size:       %d items\n", setup.subtreeSize)
	fmt.Printf("Serialized Size:    %.2f MB\n", float64(len(setup.subtreeBytes))/(1024*1024))
	fmt.Printf("Average Duration:   %.3fs\n", result.avgDuration.Seconds())
	fmt.Printf("Average Throughput: %.2f MB/sec\n", float64(result.avgBytes)/(1024*1024)/result.avgDuration.Seconds())
}

// TestGetSubtreeBinaryBenchmark benchmarks the GetSubtree HTTP handler in BINARY_STREAM mode.
// This test is skipped by default and can be run with:
//
//	RUN_SUBTREE_BENCHMARK=true go test -v -run TestGetSubtreeBinaryBenchmark ./services/asset/httpimpl/...
//
// It creates a subtree with 1 million items, writes it to an in-memory store, and benchmarks
// the GetSubtree handler in BINARY_STREAM mode. CPU and memory profiles are written to /tmp.
func TestGetSubtreeBinaryBenchmark(t *testing.T) {
	skipIfNotEnabled(t)

	cpuProfile := "/tmp/getsubtree_binary_cpu.prof"
	memProfile := "/tmp/getsubtree_binary_mem.prof"

	printBenchmarkHeader("GetSubtree BINARY_STREAM Benchmark", benchmarkSubtreeSize)

	setup := setupBinaryBenchmark(t, benchmarkSubtreeSize)

	fmt.Printf("Starting profiled benchmark...\n\n")
	cpuFile := startCPUProfile(t, cpuProfile)

	result := runBenchmarkIterations(t, setup, BINARY_STREAM, defaultIterations)

	stopCPUProfile(t, cpuFile)
	writeMemoryProfile(t, memProfile)

	printBenchmarkSummary(setup, result)
	printProfileInfo(cpuProfile, memProfile)
}

// TestGetSubtreeHexBenchmark benchmarks the GetSubtree HTTP handler in HEX mode.
// This test is skipped by default and can be run with:
//
//	RUN_SUBTREE_BENCHMARK=true go test -v -run TestGetSubtreeHexBenchmark ./services/asset/httpimpl/...
//
// It creates a subtree with 1 million items, writes it to an in-memory store, and benchmarks
// the GetSubtree handler in HEX mode. CPU and memory profiles are written to /tmp.
func TestGetSubtreeHexBenchmark(t *testing.T) {
	skipIfNotEnabled(t)

	cpuProfile := "/tmp/getsubtree_hex_cpu.prof"
	memProfile := "/tmp/getsubtree_hex_mem.prof"

	printBenchmarkHeader("GetSubtree HEX Benchmark", benchmarkSubtreeSize)

	setup := setupBinaryBenchmark(t, benchmarkSubtreeSize)

	fmt.Printf("Starting profiled benchmark...\n\n")
	cpuFile := startCPUProfile(t, cpuProfile)

	result := runBenchmarkIterations(t, setup, HEX, defaultIterations)

	stopCPUProfile(t, cpuFile)
	writeMemoryProfile(t, memProfile)

	printBenchmarkSummary(setup, result)
	printProfileInfo(cpuProfile, memProfile)
}

// TestGetSubtreeJSONBenchmark benchmarks the GetSubtree JSON handler specifically.
// This test is skipped by default and can be run with:
//
//	RUN_SUBTREE_BENCHMARK=true go test -v -run TestGetSubtreeJSONBenchmark ./services/asset/httpimpl/...
//
// JSON mode requires full deserialization, so it uses a smaller subtree size (128K items).
// CPU and memory profiles are written to /tmp.
func TestGetSubtreeJSONBenchmark(t *testing.T) {
	skipIfNotEnabled(t)

	// Use a smaller size for JSON since it's much slower due to full deserialization
	// Must be a power of 2 for subtree creation
	const jsonSubtreeSize = 131072 // 128K items for JSON mode (2^17)
	const jsonIterations = 5

	cpuProfile := "/tmp/getsubtree_json_cpu.prof"
	memProfile := "/tmp/getsubtree_json_mem.prof"

	printBenchmarkHeader("GetSubtree JSON Benchmark", jsonSubtreeSize)

	setup := setupJSONBenchmark(t, jsonSubtreeSize)

	fmt.Printf("Starting profiled benchmark...\n\n")
	cpuFile := startCPUProfile(t, cpuProfile)

	result := runBenchmarkIterations(t, setup, JSON, jsonIterations)

	stopCPUProfile(t, cpuFile)
	writeMemoryProfile(t, memProfile)

	printBenchmarkSummary(setup, result)
	printProfileInfo(cpuProfile, memProfile)
}
