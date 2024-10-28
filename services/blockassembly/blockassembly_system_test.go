//go:build system

package blockassembly

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log"
	"net/http"
	"os"
	"os/exec"
	"testing"
	"time"

	"github.com/bitcoin-sv/ubsv/errors"
	"github.com/bitcoin-sv/ubsv/services/blockchain"
	"github.com/bitcoin-sv/ubsv/stores/blob/memory"
	utxostore "github.com/bitcoin-sv/ubsv/stores/utxo/memory"
	"github.com/bitcoin-sv/ubsv/ulogger"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var kafkaCmd *exec.Cmd
var appCmd *exec.Cmd
var appPID int

func startKafka(logFile string) error {
	kafkaCmd = exec.Command("../../deploy/dev/kafka.sh")
	kafkaLog, err := os.Create(logFile)
	if err != nil {
		return err
	}
	defer kafkaLog.Close()

	kafkaCmd.Stdout = kafkaLog
	kafkaCmd.Stderr = kafkaLog
	return kafkaCmd.Start()
}

func startApp(logFile string) error {
	appCmd := exec.Command("go", "run", "../../.")
	appCmd.Env = append(os.Environ(), "SETTINGS_CONTEXT=dev.system.test.ba")

	appLog, err := os.Create(logFile)
	if err != nil {
		return err
	}
	defer appLog.Close()

	appCmd.Stdout = appLog
	appCmd.Stderr = appLog

	log.Println("Starting app in the background...")
	if err := appCmd.Start(); err != nil {
		return err
	}

	appPID = appCmd.Process.Pid

	// Wait for the app to be ready (consider implementing a health check here)
	time.Sleep(30 * time.Second) // Adjust this as needed for your app's startup time

	return nil
}

func stopKafka() {
	log.Println("Stopping Kafka...")
	cmd := exec.Command("docker", "stop", "kafka-server")
	if err := cmd.Run(); err != nil {
		log.Printf("Failed to stop Kafka: %v\n", err)
	} else {
		log.Println("Kafka stopped successfully")
	}
}

func stopUbsv() {
	log.Println("Stopping UBSV...")
	cmd := exec.Command("pkill", "-f", "ubsv")
	if err := cmd.Run(); err != nil {
		log.Printf("Failed to stop UBSV: %v\n", err)
	} else {
		log.Println("UBSV stopped successfully")
	}
}

// TestMain runs setup before tests and teardown after tests
func TestMain(m *testing.M) {
	// Start Kafka
	if err := startKafka("kafka.log"); err != nil {
		log.Fatalf("Failed to start Kafka: %v", err)
	}

	// Ensure Kafka has time to initialize
	time.Sleep(5 * time.Second)

	// Start the app
	if err := startApp("app.log"); err != nil {
		log.Fatalf("Failed to start app: %v", err)
	}

	// Run tests
	code := m.Run()

	// Cleanup processes
	stopKafka()
	stopUbsv()

	os.Exit(code)
}

// Health check
func TestHealth(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	memStore := memory.New()
	blobStore := memory.New()
	utxoStore := utxostore.New(ulogger.TestLogger{})
	blockchainClient, err := blockchain.NewClient(ctx, ulogger.TestLogger{}, "test")
	require.NoError(t, err)
	ba := New(ulogger.TestLogger{}, memStore, utxoStore, blobStore, blockchainClient)
	require.NotNil(t, ba)

	err = ba.Init(ctx)
	require.NoError(t, err)

	err = blockchainClient.Run(ctx)
	require.NoError(t, err, "Blockchain client failed to start")

	status, message, err := ba.Health(ctx, true)
	require.NoError(t, err, "Liveness check should not return an error")
	assert.Equal(t, http.StatusOK, status, "Liveness check should return OK status")
	assert.Equal(t, "OK", message, "Liveness check should return 'OK' message")
}

// Test coinbase value
func TestCoinbaseSubsidyHeight(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	memStore := memory.New()
	blobStore := memory.New()
	utxoStore := utxostore.New(ulogger.TestLogger{})
	blockchainClient, err := blockchain.NewClient(ctx, ulogger.TestLogger{}, "test")
	require.NoError(t, err)
	ba := New(ulogger.TestLogger{}, memStore, utxoStore, blobStore, blockchainClient)
	require.NotNil(t, ba)

	err = ba.Init(ctx)
	require.NoError(t, err)

	err = blockchainClient.Run(ctx)
	require.NoError(t, err, "Blockchain client failed to start")

	// Generate blocks
	_, err = CallRPC("http://localhost:9292", "generate", []interface{}{"[200]"})
	require.NoError(t, err, "Failed to generate blocks")
	time.Sleep(5 * time.Second)

	// Get the block height
	err = ba.blockAssembler.blockchainClient.Run(ctx)
	require.NoError(t, err, "Failed to start block assembler")
	time.Sleep(5 * time.Second)

	h, m, _ := ba.blockchainClient.GetBestBlockHeader(ctx)
	assert.NotNil(t, h, "Best block header should not be nil")
	assert.NotNil(t, m, "Best block metadata should not be nil")
	t.Logf("Best block header: %v", h)

	ba.blockAssembler.bestBlockHeader.Store(h)
	ba.blockAssembler.bestBlockHeight.Store(m.Height)
	ba.blockAssembler.subtreeProcessor.SetCurrentBlockHeader(ba.blockAssembler.bestBlockHeader.Load())
	mc, st, err := ba.blockAssembler.getMiningCandidate()
	require.NoError(t, err, "Failed to get mining candidate")
	assert.NotNil(t, mc, "Mining candidate should not be nil")
	assert.NotNil(t, st, "Subtree should not be nil")
	t.Logf("Coinbase: %v", mc.CoinbaseValue)
	//TODO: Add an assertion for height
}

func TestDifficultyAdjustment(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	memStore := memory.New()
	blobStore := memory.New()
	utxoStore := utxostore.New(ulogger.TestLogger{})
	blockchainClient, err := blockchain.NewClient(ctx, ulogger.TestLogger{}, "test")
	require.NoError(t, err)
	ba := New(ulogger.TestLogger{}, memStore, utxoStore, blobStore, blockchainClient)
	require.NotNil(t, ba)

	err = ba.Init(ctx)
	require.NoError(t, err)

	err = blockchainClient.Run(ctx)
	require.NoError(t, err, "Blockchain client failed to start")

	ba.blockAssembler.chainParams.NoDifficultyAdjustment = false
	ba.blockAssembler.chainParams.TargetTimePerBlock = 30 * time.Second

	// Generate initial blocks
	_, err = CallRPC("http://localhost:9292", "generate", []interface{}{"[100]"})
	require.NoError(t, err, "Failed to generate initial blocks")
	time.Sleep(5 * time.Second)

	// Get initial block height
	_, initialMetadata, err := ba.blockchainClient.GetBestBlockHeader(ctx)
	require.NoError(t, err, "Failed to get initial block header")
	initialHeight := initialMetadata.Height
	t.Logf("Initial block height: %d", initialHeight)

	// Monitor block generation for 60 seconds
	startTime := time.Now()
	endTime := startTime.Add(60 * time.Second)
	lastHeight := initialHeight

	for time.Now().Before(endTime) {
		_, m, err := ba.blockchainClient.GetBestBlockHeader(ctx)
		if err != nil {
			t.Logf("Error getting block header: %v", err)
			continue
		}
		if m.Height > lastHeight {
			t.Logf("New block at height %d, time since start: %v", m.Height, time.Since(startTime))
			lastHeight = m.Height
		}
		time.Sleep(5 * time.Second)
	}

	blocksGenerated := lastHeight - initialHeight
	t.Logf("Total blocks generated: %d", blocksGenerated)

	// Assert that some blocks were generated
	assert.True(t, blocksGenerated > 0, "Expected some blocks to be generated")

	// Calculate blocks per minute
	blocksPerMinute := float64(blocksGenerated) / time.Since(startTime).Minutes()
	t.Logf("Blocks per minute: %.2f", blocksPerMinute)

	// Assert that the block generation rate is within an acceptable range
	// Expecting roughly 2 blocks per minute (30 seconds per block)
	assert.InDelta(t, 1.0, blocksPerMinute, 1.0, "Block generation rate should be roughly 2 blocks per minute")
}

func CallRPC(url string, method string, params []interface{}) (string, error) {

	// Create the request payload
	requestBody, err := json.Marshal(map[string]interface{}{
		"method": method,
		"params": params,
	})
	if err != nil {
		return "", errors.NewProcessingError("failed to marshal request body", err)
	}

	// Create the HTTP request
	req, err := http.NewRequest("POST", url, bytes.NewBuffer(requestBody))
	if err != nil {
		return "", errors.NewProcessingError("failed to create request", err)
	}

	// Set the appropriate headers
	req.SetBasicAuth("bitcoin", "bitcoin")
	req.Header.Set("Content-Type", "application/json")

	// Perform the request
	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return "", errors.NewProcessingError("failed to perform request", err)
	}
	defer resp.Body.Close()

	// Check the status code
	if resp.StatusCode != http.StatusOK {
		return "", errors.NewProcessingError("expected status code 200, got %v", resp.StatusCode)
	}

	// Read the response body
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", errors.NewProcessingError("failed to read response body", err)
	}

	// Return the response as a string
	return string(body), nil
}
