package netsync

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"runtime"
	"runtime/pprof"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-chaincfg"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/services/blockassembly"
	"github.com/bsv-blockchain/teranode/services/blockassembly/blockassembly_api"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/services/blockchain/blockchain_api"
	"github.com/bsv-blockchain/teranode/services/blockvalidation"
	"github.com/bsv-blockchain/teranode/services/legacy/peer"
	"github.com/bsv-blockchain/teranode/services/legacy/testdata"
	"github.com/bsv-blockchain/teranode/services/subtreevalidation"
	"github.com/bsv-blockchain/teranode/services/validator"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/stores/blob/memory"
	"github.com/bsv-blockchain/teranode/stores/utxo/nullstore"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/ordishs/go-bitcoin"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

const (
	testnetBlockHeight = 1681787
	cpuProfileFile     = "handleblockdirect_testnet_cpu.prof"
	memProfileFile     = "handleblockdirect_testnet_mem.prof"
)

func getEnvOrDefault(key, defaultValue string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return defaultValue
}

// downloadAndCacheBlock downloads the block at the given height and saves raw
// bytes to a .bin file in testdata/. If the file already exists, it returns
// the path immediately.
//
// Download strategies (in order of preference):
//  1. SSH remote download — if TESTNET_SSH_HOST is set, runs curl on the remote
//     server and streams the raw binary over SSH. This avoids SSH tunnel
//     timeouts that kill multi-GB transfers through port-forwarded connections.
//  2. Direct REST download — uses the local REST endpoint directly.
//
// Environment variables:
//
//	TESTNET_SSH_HOST     — SSH host to download from (e.g., "bsva-ovh-seed-testnet-us-1")
//	TESTNET_NODE_PORT    — REST port on the remote node (default: same as TESTNET_RPC_PORT)
//	TESTNET_RPC_HOST     — RPC host (default: "localhost")
//	TESTNET_RPC_PORT     — RPC port (default: "18332")
//	TESTNET_RPC_USER     — RPC user (default: "bitcoin")
//	TESTNET_RPC_PASS     — RPC password
func downloadAndCacheBlock(t *testing.T, blockHeight int) string {
	t.Helper()

	rpcHost := getEnvOrDefault("TESTNET_RPC_HOST", "localhost")
	rpcPortStr := getEnvOrDefault("TESTNET_RPC_PORT", "18332")
	rpcUser := getEnvOrDefault("TESTNET_RPC_USER", "bitcoin")
	rpcPass := getEnvOrDefault("TESTNET_RPC_PASS", "bitcoin")

	rpcPort, err := strconv.Atoi(rpcPortStr)
	require.NoError(t, err)

	b, err := bitcoin.New(rpcHost, rpcPort, rpcUser, rpcPass, false)
	require.NoError(t, err, "failed to connect to BSV node at %s:%d", rpcHost, rpcPort)

	t.Logf("Getting block hash for height %d from %s:%d", blockHeight, rpcHost, rpcPort)
	blockHash, err := b.GetBlockHash(blockHeight)
	require.NoError(t, err, "failed to get block hash for height %d", blockHeight)
	t.Logf("Block hash: %s", blockHash)

	filePath := fmt.Sprintf("../testdata/%s.bin", blockHash)

	if _, err := os.Stat(filePath); err == nil {
		t.Logf("Using cached block file: %s", filePath)
		return filePath
	}

	sshHost := os.Getenv("TESTNET_SSH_HOST")
	if sshHost != "" {
		downloadBlockViaSSH(t, sshHost, rpcPortStr, blockHash, filePath)
	} else {
		downloadBlockViaREST(t, rpcHost, rpcPort, blockHash, filePath)
	}

	return filePath
}

// downloadBlockViaSSH downloads the block in two steps to avoid node-side
// HTTP timeouts that kill large transfers:
//  1. SSH into the remote host and run curl to save the block to a temp file
//     on the remote server (localhost download, very fast, no backpressure).
//  2. SCP the temp file to the local machine.
func downloadBlockViaSSH(t *testing.T, sshHost, rpcPort, blockHash, filePath string) {
	t.Helper()

	nodePort := getEnvOrDefault("TESTNET_NODE_PORT", rpcPort)
	remoteURL := fmt.Sprintf("http://127.0.0.1:%s/rest/block/%s.bin", nodePort, blockHash)
	remoteTmp := fmt.Sprintf("/tmp/block_%s.bin", blockHash)

	// Step 1: Download on the remote server (curl localhost → disk)
	t.Logf("Step 1: Downloading block on remote server %s ...", sshHost)
	t.Logf("  Remote URL: %s", remoteURL)
	t.Logf("  Remote file: %s", remoteTmp)
	startTime := time.Now()

	cmd := exec.Command("ssh",
		"-o", "ServerAliveInterval=30",
		"-o", "ServerAliveCountMax=10",
		sshHost,
		fmt.Sprintf("curl -sf -o '%s' '%s' && stat -c '%%s' '%s'", remoteTmp, remoteURL, remoteTmp),
	)
	output, err := cmd.CombinedOutput()
	if err != nil {
		// Try macOS stat syntax on remote
		cmd2 := exec.Command("ssh", sshHost, fmt.Sprintf("stat -f '%%z' '%s' 2>/dev/null || echo 0", remoteTmp))
		sizeOut, _ := cmd2.Output()
		t.Logf("  Remote download may have failed: %v (output: %s, remote file size: %s)", err, string(output), string(sizeOut))
		require.NoError(t, err, "failed to download block on remote server")
	}
	t.Logf("  Remote download complete: %s bytes (%s elapsed)", strings.TrimSpace(string(output)), time.Since(startTime))

	// Step 2: SCP the file to local machine
	t.Logf("Step 2: Copying block from remote server via SCP ...")
	scpStart := time.Now()

	scpCmd := exec.Command("scp", fmt.Sprintf("%s:%s", sshHost, remoteTmp), filePath)
	scpCmd.Stderr = os.Stderr

	scpDone := make(chan error, 1)
	err = scpCmd.Start()
	require.NoError(t, err, "failed to start scp")
	go func() { scpDone <- scpCmd.Wait() }()

	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case err := <-scpDone:
			require.NoError(t, err, "scp failed")
			fi, err := os.Stat(filePath)
			require.NoError(t, err)
			require.Greater(t, fi.Size(), int64(0), "downloaded file is empty")

			t.Logf("  SCP complete: %d bytes (%.2f GB) in %s", fi.Size(), float64(fi.Size())/(1024*1024*1024), time.Since(scpStart))
			t.Logf("Total download time: %s", time.Since(startTime))
			t.Logf("Cached block to %s", filePath)

			// Clean up remote temp file
			cleanCmd := exec.Command("ssh", sshHost, fmt.Sprintf("rm -f '%s'", remoteTmp))
			_ = cleanCmd.Run()
			return

		case <-ticker.C:
			fi, _ := os.Stat(filePath)
			if fi != nil {
				t.Logf("  ... SCP progress: %.2f GB (%s elapsed)", float64(fi.Size())/(1024*1024*1024), time.Since(scpStart))
			}
		}
	}
}

// downloadBlockViaREST downloads a block directly from the REST endpoint.
// This works for local nodes but may fail for SSH-tunneled connections on
// multi-GB blocks due to tunnel timeouts.
func downloadBlockViaREST(t *testing.T, rpcHost string, rpcPort int, blockHash, filePath string) {
	t.Helper()

	t.Logf("Downloading block %s via REST API (streaming to disk)...", blockHash)
	startTime := time.Now()

	restURL := fmt.Sprintf("http://%s:%d/rest/block/%s.bin", rpcHost, rpcPort, blockHash)

	req, err := http.NewRequest("GET", restURL, nil)
	require.NoError(t, err)
	req.Header.Set("Accept-Encoding", "identity")
	req.Header.Set("Connection", "keep-alive")

	restClient := &http.Client{
		Timeout: 0,
		Transport: &http.Transport{
			DialContext: (&net.Dialer{
				Timeout:   30 * time.Second,
				KeepAlive: 60 * time.Second,
			}).DialContext,
			IdleConnTimeout:       0,
			ResponseHeaderTimeout: 120 * time.Second,
			WriteBufferSize:       4 * 1024 * 1024,
			ReadBufferSize:        4 * 1024 * 1024,
		},
	}

	resp, err := restClient.Do(req)
	require.NoError(t, err, "failed to start block download (is REST enabled on the node?)")
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode, "REST endpoint returned status %d", resp.StatusCode)

	if resp.ContentLength > 0 {
		t.Logf("Content-Length: %d bytes (%.2f GB)", resp.ContentLength, float64(resp.ContentLength)/(1024*1024*1024))
	}

	outFile, err := os.Create(filePath)
	require.NoError(t, err, "failed to create cache file %s", filePath)

	buf := make([]byte, 4*1024*1024)
	var written int64
	lastLog := time.Now()
	for {
		n, readErr := resp.Body.Read(buf)
		if n > 0 {
			nw, writeErr := outFile.Write(buf[:n])
			written += int64(nw)
			if writeErr != nil {
				outFile.Close()
				os.Remove(filePath)
				require.NoError(t, writeErr, "failed to write to cache file")
			}
			if time.Since(lastLog) > 10*time.Second {
				t.Logf("  ... downloaded %.2f GB so far (%s elapsed)", float64(written)/(1024*1024*1024), time.Since(startTime))
				lastLog = time.Now()
			}
		}
		if readErr == io.EOF {
			break
		}
		if readErr != nil {
			outFile.Close()
			os.Remove(filePath)
			require.NoError(t, readErr, "failed to stream block to disk (got %d bytes / %.2f GB before error)", written, float64(written)/(1024*1024*1024))
		}
	}
	outFile.Close()

	elapsed := time.Since(startTime)
	t.Logf("Downloaded %d bytes (%.2f GB) in %s", written, float64(written)/(1024*1024*1024), elapsed)
	t.Logf("Cached block to %s", filePath)
}

func logMemStats(t *testing.T, label string, stats *runtime.MemStats) {
	t.Helper()
	t.Logf("[MemStats][%s] HeapAlloc=%.2f MB, HeapInuse=%.2f MB, HeapSys=%.2f MB, TotalAlloc=%.2f MB, Sys=%.2f MB, NumGC=%d",
		label,
		float64(stats.HeapAlloc)/(1024*1024),
		float64(stats.HeapInuse)/(1024*1024),
		float64(stats.HeapSys)/(1024*1024),
		float64(stats.TotalAlloc)/(1024*1024),
		float64(stats.Sys)/(1024*1024),
		stats.NumGC,
	)
}

// TestHandleBlockDirect_TestnetLargeBlock downloads testnet block 1681787 (3.9GB),
// processes it through HandleBlockDirect, and generates CPU + memory profiles
// to investigate the excessive memory usage reported in issue #490.
//
// Run with:
//
//	TESTNET_RPC_HOST=<host> TESTNET_RPC_PORT=<port> \
//	  go test -v -run TestHandleBlockDirect_TestnetLargeBlock \
//	  -timeout 30m ./services/legacy/netsync/
//
// Analyze profiles:
//
//	go tool pprof -http=:8080 handleblockdirect_testnet_cpu.prof
//	go tool pprof -http=:8081 handleblockdirect_testnet_mem.prof
func TestHandleBlockDirect_TestnetLargeBlock(t *testing.T) {
	t.Skip("Skipping: requires a running BSV testnet node with RPC enabled. Set TESTNET_RPC_HOST/PORT/USER/PASS env vars and remove this skip to run.")

	benchStartTime := time.Now()

	// === Download / cache the block ===
	filePath := downloadAndCacheBlock(t, testnetBlockHeight)

	t.Logf("[%s] Loading block from disk...", time.Since(benchStartTime))
	block, err := testdata.ReadBlockFromFile(filePath)
	require.NoError(t, err)

	blockHash := block.Hash()
	txCount := len(block.Transactions())
	blockSize := block.MsgBlock().SerializeSize()
	t.Logf("[%s] Block %s loaded: %d txs, %.2f GB",
		time.Since(benchStartTime), blockHash, txCount, float64(blockSize)/(1024*1024*1024))

	// === Set up mocks (following HandleBlockDirect_test.go pattern) ===
	var (
		ctx                 = context.Background()
		logger              = ulogger.TestLogger{}
		blockchainClient    = &blockchain.Mock{}
		validatorClient     = &validator.MockValidator{}
		utxoStore           = &nullstore.NullStore{}
		subtreeStore        = memory.New()
		subtreeValidation   = &subtreevalidation.MockSubtreeValidation{}
		blockValidation     = &blockvalidation.MockBlockValidation{}
		blockAssemblyClient = blockassembly.NewMock()
		config              = &Config{
			ChainParams: &chaincfg.TestNetParams,
		}
	)

	nowUint32 := uint32(time.Now().Unix()) //nolint:gosec

	mockBlockHeaderMeta := &model.BlockHeaderMeta{
		ID:        1,
		Height:    uint32(testnetBlockHeight) - 1, //nolint:gosec
		Miner:     "test",
		BlockTime: nowUint32,
		Timestamp: nowUint32,
	}

	blockchainClient.On("GetBlockExists", mock.Anything, mock.Anything).Return(false, nil)
	blockchainClient.On("GetBlockHeader", mock.Anything, mock.Anything).Return((*model.BlockHeader)(nil), mockBlockHeaderMeta, nil)
	blockchainClient.On("GetBestBlockHeader", mock.Anything).Return((*model.BlockHeader)(nil), mockBlockHeaderMeta, nil)
	blockchainClient.On("IsFSMCurrentState", mock.Anything, mock.Anything).Return(true, nil)
	blockchainClient.On("AddBlock", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	blockchainClient.On("Subscribe", mock.Anything, mock.Anything).Return(make(chan *blockchain_api.Notification), nil)

	blockAssemblyClient.On("GetBlockAssemblyState", mock.Anything).Return(&blockassembly_api.StateMessage{
		CurrentHeight: uint32(testnetBlockHeight), //nolint:gosec
	}, nil)

	subtreeValidation.On("CheckSubtreeFromBlock", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)

	tSettings := &settings.Settings{
		GlobalBlockHeightRetention: 100,
		BlockValidation: settings.BlockValidationSettings{
			MaxBlocksBehindBlockAssembly: 20,
		},
		Legacy: settings.LegacySettings{
			StoreBatcherSize:           1024,
			StoreBatcherConcurrency:    32,
			SpendBatcherSize:           1024,
			SpendBatcherConcurrency:    32,
			OutpointBatcherSize:        1024,
			OutpointBatcherConcurrency: 32,
		},
	}

	sm, err := New(
		ctx,
		logger,
		tSettings,
		blockchainClient,
		validatorClient,
		utxoStore,
		subtreeStore,
		subtreeValidation,
		blockValidation,
		blockAssemblyClient,
		config,
	)
	require.NoError(t, err)

	// === Memory baseline ===
	runtime.GC()
	var memBefore runtime.MemStats
	runtime.ReadMemStats(&memBefore)
	logMemStats(t, "before HandleBlockDirect", &memBefore)

	// === Start CPU profiling ===
	cpuFile, err := os.Create(cpuProfileFile)
	require.NoError(t, err)
	err = pprof.StartCPUProfile(cpuFile)
	require.NoError(t, err)

	// === Run HandleBlockDirect ===
	t.Logf("[%s] Starting HandleBlockDirect...", time.Since(benchStartTime))
	startTime := time.Now()

	err = sm.HandleBlockDirect(ctx, &peer.Peer{}, *blockHash, block.MsgBlock())
	elapsed := time.Since(startTime)

	// === Stop CPU profiling ===
	pprof.StopCPUProfile()
	cpuFile.Close()

	if err != nil {
		t.Logf("[%s] HandleBlockDirect returned error: %v", time.Since(benchStartTime), err)
	}
	require.NoError(t, err)

	// === Memory after ===
	runtime.GC()
	var memAfter runtime.MemStats
	runtime.ReadMemStats(&memAfter)
	logMemStats(t, "after HandleBlockDirect", &memAfter)

	// === Write memory profile ===
	memFile, err := os.Create(memProfileFile)
	require.NoError(t, err)
	err = pprof.WriteHeapProfile(memFile)
	require.NoError(t, err)
	memFile.Close()

	// === Print summary ===
	heapGrowthMB := float64(memAfter.HeapAlloc-memBefore.HeapAlloc) / (1024 * 1024)
	totalAllocMB := float64(memAfter.TotalAlloc-memBefore.TotalAlloc) / (1024 * 1024)

	fmt.Printf("\n")
	fmt.Printf("HandleBlockDirect Memory Profile Results\n")
	fmt.Printf("=========================================\n")
	fmt.Printf("Block:              %s\n", blockHash)
	fmt.Printf("Block Height:       %d\n", testnetBlockHeight)
	fmt.Printf("Block Size:         %.2f GB\n", float64(blockSize)/(1024*1024*1024))
	fmt.Printf("Transaction Count:  %d\n", txCount)
	fmt.Printf("Elapsed Time:       %s\n", elapsed)
	fmt.Printf("\n")
	fmt.Printf("Heap Before:        %.2f MB\n", float64(memBefore.HeapAlloc)/(1024*1024))
	fmt.Printf("Heap After:         %.2f MB\n", float64(memAfter.HeapAlloc)/(1024*1024))
	fmt.Printf("Heap Growth:        %.2f MB (%.1fx block size)\n", heapGrowthMB, heapGrowthMB/(float64(blockSize)/(1024*1024)))
	fmt.Printf("Total Allocated:    %.2f MB\n", totalAllocMB)
	fmt.Printf("Sys Memory:         %.2f MB\n", float64(memAfter.Sys)/(1024*1024))
	fmt.Printf("GC Cycles:          %d\n", memAfter.NumGC-memBefore.NumGC)
	fmt.Printf("\n")
	fmt.Printf("Profiles written to:\n")
	fmt.Printf("  CPU:    %s\n", cpuProfileFile)
	fmt.Printf("  Memory: %s\n", memProfileFile)
	fmt.Printf("\n")
	fmt.Printf("Analyze with:\n")
	fmt.Printf("  go tool pprof -http=:8080 %s\n", cpuProfileFile)
	fmt.Printf("  go tool pprof -http=:8081 %s\n", memProfileFile)
}
