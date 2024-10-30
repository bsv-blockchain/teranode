//go:build system

package blockassembly

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"testing"
	"time"

	"github.com/bitcoin-sv/ubsv/chaincfg"
	"github.com/bitcoin-sv/ubsv/errors"
	"github.com/bitcoin-sv/ubsv/model"
	"github.com/bitcoin-sv/ubsv/services/blockchain"
	"github.com/bitcoin-sv/ubsv/stores/blob/memory"
	blockchainstore "github.com/bitcoin-sv/ubsv/stores/blockchain/sql"
	utxostore "github.com/bitcoin-sv/ubsv/stores/utxo/memory"
	"github.com/bitcoin-sv/ubsv/ulogger"
	"github.com/libsv/go-bt/v2"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/ordishs/gocore"
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

	err = blockchainClient.Run(ctx, "test")
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

	err = blockchainClient.Run(ctx, "test")
	require.NoError(t, err, "Blockchain client failed to start")

	// Get the block height
	err = ba.blockAssembler.blockchainClient.Run(ctx, "test")
	require.NoError(t, err, "Failed to start block assembler")
	time.Sleep(5 * time.Second)

	// Generate blocks
	_, err = CallRPC("http://localhost:9292", "generate", []interface{}{"[200]"})
	require.NoError(t, err, "Failed to generate blocks")
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

	err = blockchainClient.Run(ctx, "test")
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

func TestShouldFollowLongerChain(t *testing.T) {

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	memStore := memory.New()
	blobStore := memory.New()
	utxoStore := utxostore.New(ulogger.TestLogger{})
	blockchainClient, err := blockchain.NewClient(ctx, ulogger.TestLogger{}, "test")
	require.NoError(t, err)
	blockchainStoreUrlstr, ok := gocore.Config().Get("blockchain_store")
	require.True(t, ok)
	blockchainStoreUrl, err := url.Parse(blockchainStoreUrlstr)
	blockchainStore, err := blockchainstore.New(ulogger.TestLogger{}, blockchainStoreUrl)
	paramsA := &chaincfg.MainNetParams


	dA, err := blockchain.NewDifficulty(blockchainStore, ulogger.TestLogger{}, paramsA)
	t.Logf("difficulty: %v", )
	require.NoError(t, err)

	paramsB = &chaincfg.RegressionNetParams

	dB, err := blockchain.NewDifficulty(blockchainStore, ulogger.TestLogger{}, paramsB)
	t.Logf("difficulty: %v", *dB.nBits)
	require.NoError(t, err)

	ba := New(ulogger.TestLogger{}, memStore, utxoStore, blobStore, blockchainClient)
	require.NotNil(t, ba)

	err = ba.Init(ctx)
	require.NoError(t, err)

	err = blockchainClient.Run(ctx, "test")
	require.NoError(t, err, "Blockchain client failed to start")

	// Generate initial 101 blocks
	_, err = CallRPC("http://localhost:9292", "generate", []interface{}{"[101]"})
	require.NoError(t, err, "Failed to generate initial blocks")
	time.Sleep(5 * time.Second)

	// Get initial block height and hash
	initialHeader, initialMetadata, err := ba.blockchainClient.GetBestBlockHeader(ctx)
	require.NoError(t, err, "Failed to get initial block header")
	initialHeight := initialMetadata.Height
	t.Logf("Initial block height: %d", initialHeight)

	// Create chain A (higher difficulty)
	chainABits, _ := model.NewNBitFromString("1d00ffff")
	chainAHeader1 := &model.BlockHeader{
		Version:        1,
		HashPrevBlock:  initialHeader.Hash(),
		HashMerkleRoot: &chainhash.Hash{},
		Nonce:          1,
		Bits:           *chainABits,
		Timestamp:      uint32(time.Now().Unix()),
	}

	// Create chain B (lower difficulty)
	chainBBits, _ := model.NewNBitFromString("1d00ffaa") // Lower difficulty
	chainBHeader1 := &model.BlockHeader{
		Version:        1,
		HashPrevBlock:  initialHeader.Hash(),
		HashMerkleRoot: &chainhash.Hash{},
		Nonce:          2,
		Bits:           *chainBBits,
		Timestamp:      uint32(time.Now().Unix()),
	}

	// Store blocks from both chains
	coinbaseTx, _ := bt.NewTxFromString("01000000010000000000000000000000000000000000000000000000000000000000000000ffffffff17030200002f6d312d65752f605f77009f74384816a31807ffffffff03ac505763000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88acaa505763000000001976a9143c22b6d9ba7b50b6d6e615c69d11ecb2ba3db14588acaa505763000000001976a914b7177c7deb43f3869eabc25cfd9f618215f34d5588ac00000000")

	// Store Chain A block
	blockA := &model.Block{
		Header:           chainAHeader1,
		CoinbaseTx:       coinbaseTx,
		TransactionCount: 1,
		Subtrees:         []*chainhash.Hash{},
	}
	err = ba.blockchainClient.AddBlock(ctx, blockA, "")
	require.NoError(t, err, "Failed to add Chain A block")

	// Store Chain B block
	blockB := &model.Block{
		Header:           chainBHeader1,
		CoinbaseTx:       coinbaseTx,
		TransactionCount: 1,
		Subtrees:         []*chainhash.Hash{},
	}
	err = ba.blockchainClient.AddBlock(ctx, blockB, "")
	require.NoError(t, err, "Failed to add Chain B block")

	// Create new block assembler to check which chain it follows
	newBA := New(ulogger.TestLogger{}, memStore, utxoStore, blobStore, blockchainClient)
	require.NotNil(t, newBA)

	err = newBA.Init(ctx)
	require.NoError(t, err)

	// Get the best block header from the new block assembler
	bestHeader, _, err := ba.blockchainClient.GetBestBlockHeader(ctx)
	require.NoError(t, err)
	require.NotNil(t, bestHeader)

	// Assert that it followed chain A (higher difficulty)
	assert.Equal(t, chainAHeader1.Hash(), bestHeader.Hash(), "Block assembler should follow the chain with higher difficulty")
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
