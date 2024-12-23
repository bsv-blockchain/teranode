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

	"github.com/bitcoin-sv/teranode/errors"
	"github.com/bitcoin-sv/teranode/model"
	"github.com/bitcoin-sv/teranode/services/blockchain"
	"github.com/bitcoin-sv/teranode/stores/blob/memory"
	utxostore "github.com/bitcoin-sv/teranode/stores/utxo/memory"
	"github.com/bitcoin-sv/teranode/ulogger"
	"github.com/bitcoin-sv/teranode/util"
	"github.com/bitcoin-sv/teranode/util/test"
	"github.com/libsv/go-bt/v2"
	"github.com/libsv/go-bt/v2/chainhash"
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

	appCmd.Env = append(os.Environ(), "SETTINGS_CONTEXT=dev.system.test.blockassembly")

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

func stopTeranode() {
	log.Println("Stopping TERANODE...")
	cmd := exec.Command("pkill", "-f", "teranode")
	if err := cmd.Run(); err != nil {
		log.Printf("Failed to stop TERANODE: %v\n", err)
	} else {
		log.Println("TERANODE stopped successfully")
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
	stopTeranode()

	os.Exit(code)
}

// Health check
func TestHealth(t *testing.T) {
	tSettings := test.CreateBaseTestSettings()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	memStore := memory.New()
	blobStore := memory.New()
	utxoStore := utxostore.New(ulogger.TestLogger{})
	blockchainClient, err := blockchain.NewClient(ctx, ulogger.TestLogger{}, tSettings, "test")
	require.NoError(t, err)
	ba := New(ulogger.TestLogger{}, tSettings, memStore, utxoStore, blobStore, blockchainClient)
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
	tSettings := test.CreateBaseTestSettings()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	memStore := memory.New()
	blobStore := memory.New()
	utxoStore := utxostore.New(ulogger.TestLogger{})
	blockchainClient, err := blockchain.NewClient(ctx, ulogger.TestLogger{}, tSettings, "test")
	require.NoError(t, err)
	ba := New(ulogger.TestLogger{}, tSettings, memStore, utxoStore, blobStore, blockchainClient)
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
	tSettings := test.CreateBaseTestSettings()
	t.Skip("Skipping difficulty adjustment test")
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	memStore := memory.New()
	blobStore := memory.New()
	utxoStore := utxostore.New(ulogger.TestLogger{})
	blockchainClient, err := blockchain.NewClient(ctx, ulogger.TestLogger{}, tSettings, "test")
	require.NoError(t, err)
	ba := New(ulogger.TestLogger{}, tSettings, memStore, utxoStore, blobStore, blockchainClient)
	require.NotNil(t, ba)

	err = ba.Init(ctx)
	require.NoError(t, err)

	err = blockchainClient.Run(ctx, "test")
	require.NoError(t, err, "Blockchain client failed to start")

	ba.blockAssembler.settings.ChainCfgParams.NoDifficultyAdjustment = false
	ba.blockAssembler.settings.ChainCfgParams.TargetTimePerBlock = 30 * time.Second

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
	tSettings := test.CreateBaseTestSettings()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	memStore := memory.New()
	blobStore := memory.New()
	utxoStore := utxostore.New(ulogger.TestLogger{})
	blockchainClient, err := blockchain.NewClient(ctx, ulogger.TestLogger{}, tSettings, "test")
	require.NoError(t, err)

	ba := New(ulogger.TestLogger{}, tSettings, memStore, utxoStore, blobStore, blockchainClient)
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
	chainBBits, _ := model.NewNBitFromString("207fffff") // Lower difficulty
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
	newBA := New(ulogger.TestLogger{}, tSettings, memStore, utxoStore, blobStore, blockchainClient)
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

func TestShouldFollowChainWithMoreChainwork(t *testing.T) {
	tSettings := test.CreateBaseTestSettings()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	memStore := memory.New()
	blobStore := memory.New()
	utxoStore := utxostore.New(ulogger.TestLogger{})
	blockchainClient, err := blockchain.NewClient(ctx, ulogger.TestLogger{}, tSettings, "test")
	require.NoError(t, err)

	ba := New(ulogger.TestLogger{}, tSettings, memStore, utxoStore, blobStore, blockchainClient)
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

	// Create chain B (lower difficulty) with multiple blocks
	chainBBits, _ := model.NewNBitFromString("207fffff") // Much lower difficulty
	chainBHeader1 := &model.BlockHeader{
		Version:        1,
		HashPrevBlock:  initialHeader.Hash(),
		HashMerkleRoot: &chainhash.Hash{},
		Nonce:          2,
		Bits:           *chainBBits,
		Timestamp:      uint32(time.Now().Unix()),
	}

	chainBHeader2 := &model.BlockHeader{
		Version:        1,
		HashPrevBlock:  chainBHeader1.Hash(),
		HashMerkleRoot: &chainhash.Hash{},
		Nonce:          3,
		Bits:           *chainBBits,
		Timestamp:      uint32(time.Now().Unix()),
	}

	chainBHeader3 := &model.BlockHeader{
		Version:        1,
		HashPrevBlock:  chainBHeader2.Hash(),
		HashMerkleRoot: &chainhash.Hash{},
		Nonce:          4,
		Bits:           *chainBBits,
		Timestamp:      uint32(time.Now().Unix()),
	}

	// Store blocks from both chains
	coinbaseTx, _ := bt.NewTxFromString("01000000010000000000000000000000000000000000000000000000000000000000000000ffffffff17030200002f6d312d65752f605f77009f74384816a31807ffffffff03ac505763000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88acaa505763000000001976a9143c22b6d9ba7b50b6d6e615c69d11ecb2ba3db14588acaa505763000000001976a914b7177c7deb43f3869eabc25cfd9f618215f34d5588ac00000000")

	// Store Chain A block (higher difficulty)
	blockA := &model.Block{
		Header:           chainAHeader1,
		CoinbaseTx:       coinbaseTx,
		TransactionCount: 1,
		Subtrees:         []*chainhash.Hash{},
	}
	err = ba.blockchainClient.AddBlock(ctx, blockA, "")
	require.NoError(t, err, "Failed to add Chain A block")

	// Store Chain B blocks (lower difficulty but longer)
	blockB1 := &model.Block{
		Header:           chainBHeader1,
		CoinbaseTx:       coinbaseTx,
		TransactionCount: 1,
		Subtrees:         []*chainhash.Hash{},
	}
	err = ba.blockchainClient.AddBlock(ctx, blockB1, "")
	require.NoError(t, err, "Failed to add Chain B block 1")

	blockB2 := &model.Block{
		Header:           chainBHeader2,
		CoinbaseTx:       coinbaseTx,
		TransactionCount: 1,
		Subtrees:         []*chainhash.Hash{},
	}
	err = ba.blockchainClient.AddBlock(ctx, blockB2, "")
	require.NoError(t, err, "Failed to add Chain B block 2")

	blockB3 := &model.Block{
		Header:           chainBHeader3,
		CoinbaseTx:       coinbaseTx,
		TransactionCount: 1,
		Subtrees:         []*chainhash.Hash{},
	}
	err = ba.blockchainClient.AddBlock(ctx, blockB3, "")
	require.NoError(t, err, "Failed to add Chain B block 3")

	t.Logf("Chain A: 1 block with difficulty %s", chainABits.String())
	t.Logf("Chain B: 3 blocks with difficulty %s", chainBBits.String())

	// Create new block assembler to check which chain it follows
	newBA := New(ulogger.TestLogger{}, tSettings, memStore, utxoStore, blobStore, blockchainClient)
	require.NotNil(t, newBA)

	err = newBA.Init(ctx)
	require.NoError(t, err)

	// Get the best block header from the new block assembler
	bestHeader, _, err := ba.blockchainClient.GetBestBlockHeader(ctx)
	require.NoError(t, err)
	require.NotNil(t, bestHeader)

	// Assert that it followed chain A (higher difficulty) despite chain B being longer
	assert.Equal(t, chainAHeader1.Hash(), bestHeader.Hash(),
		"Block assembler should follow Chain A (more chainwork) despite Chain B being longer (3 blocks)")
}

func TestShouldAddSubtreesToLongerChain(t *testing.T) {
	tSettings := test.CreateBaseTestSettings()
	tSettings.Block.StoreCacheEnabled = false
	// Create a cancellable context
	ctx := context.Background()
	done := make(chan struct{})

	blobStore := memory.New()
	utxoStore := utxostore.New(ulogger.TestLogger{})
	blockchainClient, err := blockchain.NewClient(ctx, ulogger.TestLogger{}, tSettings, "test")
	require.NoError(t, err)

	err = blockchainClient.Run(ctx, "test")
	require.NoError(t, err, "Blockchain client failed to start")

	t.Logf("Creating block assembler...")

	baService := New(ulogger.TestLogger{}, tSettings, blobStore, utxoStore, blobStore, blockchainClient)
	require.NotNil(t, baService)
	err = baService.Init(ctx)
	require.NoError(t, err)

	t.Logf("Block assembler created")

	go func() {
		defer close(done)
		err = baService.Start(ctx)
		if err != nil {
			t.Errorf("Error starting service: %v", err)
		}
	}()

	//wait for the block assembler to start
	time.Sleep(5 * time.Second)
	ba := baService.blockAssembler

	// Get initial state
	initialHeader, initialMetadata, err := ba.blockchainClient.GetBestBlockHeader(ctx)
	require.NoError(t, err)
	t.Logf("Initial block height: %d", initialMetadata.Height)

	// Create test transactions
	t.Log("Creating test transactions...")
	testTx1 := newTx(1)
	testTx2 := newTx(2)
	testTx3 := newTx(3)

	testHash1 := testTx1.TxIDChainHash()
	testHash2 := testTx2.TxIDChainHash()
	testHash3 := testTx3.TxIDChainHash()

	// Create and add Chain B block (lower difficulty)
	t.Log("Creating Chain B block...")
	chainBBits, _ := model.NewNBitFromString("207fffff")
	chainBHeader1 := &model.BlockHeader{
		Version:        1,
		HashPrevBlock:  initialHeader.Hash(),
		HashMerkleRoot: &chainhash.Hash{},
		Nonce:          2,
		Bits:           *chainBBits,
		Timestamp:      uint32(time.Now().Unix()),
	}

	coinbaseTx, _ := bt.NewTxFromString("01000000010000000000000000000000000000000000000000000000000000000000000000ffffffff17030200002f6d312d65752f605f77009f74384816a31807ffffffff03ac505763000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88acaa505763000000001976a9143c22b6d9ba7b50b6d6e615c69d11ecb2ba3db14588acaa505763000000001976a914b7177c7deb43f3869eabc25cfd9f618215f34d5588ac00000000") // Dummy coinbase
	blockB := &model.Block{
		Header:           chainBHeader1,
		CoinbaseTx:       coinbaseTx,
		TransactionCount: 1,
		Subtrees:         []*chainhash.Hash{},
	}

	// Create and add Chain A block (higher difficulty)
	t.Log("Creating Chain A block...")
	chainABits, _ := model.NewNBitFromString("1d00ffff")
	chainAHeader1 := &model.BlockHeader{
		Version:        1,
		HashPrevBlock:  initialHeader.Hash(),
		HashMerkleRoot: &chainhash.Hash{},
		Nonce:          1,
		Bits:           *chainABits,
		Timestamp:      uint32(time.Now().Unix()),
	}

	blockA := &model.Block{
		Header:           chainAHeader1,
		CoinbaseTx:       coinbaseTx,
		TransactionCount: 1,
		Subtrees:         []*chainhash.Hash{},
	}

	t.Log("Adding Chain A block...")
	err = ba.blockchainClient.AddBlock(ctx, blockA, "")
	require.NoError(t, err)
	time.Sleep(2 * time.Second)

	t.Log("Adding Chain B block...")
	err = ba.blockchainClient.AddBlock(ctx, blockB, "")
	require.NoError(t, err)
	time.Sleep(1 * time.Second)

	// Add transactions
	t.Log("Adding transactions...")
	_, err = ba.utxoStore.Create(ctx, testTx1, 0)
	require.NoError(t, err)
	ba.AddTx(util.SubtreeNode{Hash: *testHash1, Fee: 111})

	_, err = ba.utxoStore.Create(ctx, testTx2, 0)
	require.NoError(t, err)
	ba.AddTx(util.SubtreeNode{Hash: *testHash2, Fee: 222})

	_, err = ba.utxoStore.Create(ctx, testTx3, 0)
	require.NoError(t, err)
	ba.AddTx(util.SubtreeNode{Hash: *testHash3, Fee: 333})

	t.Log("Waiting for transactions to be processed...")
	time.Sleep(2 * time.Second)

	// Get mining candidate with timeout context
	t.Log("Getting mining candidate...")
	var miningCandidate *model.MiningCandidate
	var s []*util.Subtree
	miningCandidate, s, mcErr := ba.getMiningCandidate()
	require.NoError(t, mcErr)

	// Verify the mining candidate is built on Chain A
	prevHash, _ := chainhash.NewHash(miningCandidate.PreviousHash)
	t.Logf("Mining candidate built on block with previous hash: %s", prevHash.String())
	t.Logf("Chain A block hash: %s", chainAHeader1.Hash().String())
	assert.Equal(t, chainAHeader1.Hash(), prevHash,
		"Mining candidate should be built on Chain A (higher difficulty)")

	// Verify transactions were carried over
	var foundTxs int
	for _, subtree := range s {
		for _, node := range subtree.Nodes {
			if node.Hash.Equal(*testHash1) || node.Hash.Equal(*testHash2) || node.Hash.Equal(*testHash3) {
				foundTxs++
			}
		}
	}
	t.Logf("Found %d transactions in subtrees", foundTxs)
	assert.Equal(t, 3, foundTxs, "All transactions should be included in the mining candidate")
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

func TestShouldHandleReorg(t *testing.T) {
	tSettings := test.CreateBaseTestSettings()
	tSettings.Block.StoreCacheEnabled = false

	ctx := context.Background()
	done := make(chan struct{})

	blobStore := memory.New()
	utxoStore := utxostore.New(ulogger.TestLogger{})
	blockchainClient, err := blockchain.NewClient(ctx, ulogger.TestLogger{}, tSettings, "test")
	require.NoError(t, err)

	err = blockchainClient.Run(ctx, "test")
	require.NoError(t, err, "Blockchain client failed to start")

	t.Log("Creating block assembler...")
	baService := New(ulogger.TestLogger{}, tSettings, blobStore, utxoStore, blobStore, blockchainClient)
	require.NotNil(t, baService)
	err = baService.Init(ctx)
	require.NoError(t, err)

	// Start the service
	go func() {
		defer close(done)
		err = baService.Start(ctx)
		if err != nil {
			t.Errorf("Error starting service: %v", err)
		}
	}()

	time.Sleep(5 * time.Second)
	ba := baService.blockAssembler

	// Get initial state
	initialHeader, initialMetadata, err := ba.blockchainClient.GetBestBlockHeader(ctx)
	require.NoError(t, err)
	t.Logf("Initial block height: %d", initialMetadata.Height)

	// Create test transactions
	t.Log("Creating test transactions...")
	testTx1 := newTx(1)
	testTx2 := newTx(2)
	testTx3 := newTx(3)

	testHash1 := testTx1.TxIDChainHash()
	testHash2 := testTx2.TxIDChainHash()
	testHash3 := testTx3.TxIDChainHash()

	// Create chain A (original chain) with lower difficulty
	t.Log("Creating Chain A (lower difficulty)...")
	chainABits, _ := model.NewNBitFromString("207fffff") // Lower difficulty
	chainAHeader1 := &model.BlockHeader{
		Version:        1,
		HashPrevBlock:  initialHeader.Hash(),
		HashMerkleRoot: &chainhash.Hash{},
		Nonce:          1,
		Bits:           *chainABits,
		Timestamp:      uint32(time.Now().Unix()),
	}

	// Create chain B (competing chain) with higher difficulty
	t.Log("Creating Chain B (higher difficulty)...")
	chainBBits, _ := model.NewNBitFromString("1d00ffff") // Higher difficulty
	chainBHeader1 := &model.BlockHeader{
		Version:        1,
		HashPrevBlock:  initialHeader.Hash(),
		HashMerkleRoot: &chainhash.Hash{},
		Nonce:          3,
		Bits:           *chainBBits,
		Timestamp:      uint32(time.Now().Unix()),
	}

	// Add transactions
	t.Log("Adding transactions...")
	_, err = ba.utxoStore.Create(ctx, testTx1, 0)
	require.NoError(t, err)
	ba.AddTx(util.SubtreeNode{Hash: *testHash1, Fee: 111})

	_, err = ba.utxoStore.Create(ctx, testTx2, 0)
	require.NoError(t, err)
	ba.AddTx(util.SubtreeNode{Hash: *testHash2, Fee: 222})

	_, err = ba.utxoStore.Create(ctx, testTx3, 0)
	require.NoError(t, err)
	ba.AddTx(util.SubtreeNode{Hash: *testHash3, Fee: 333})

	// Add Chain A block (lower difficulty)
	t.Log("Adding Chain A block...")
	coinbaseTx, _ := bt.NewTxFromString("01000000010000000000000000000000000000000000000000000000000000000000000000ffffffff17030200002f6d312d65752f605f77009f74384816a31807ffffffff03ac505763000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88acaa505763000000001976a9143c22b6d9ba7b50b6d6e615c69d11ecb2ba3db14588acaa505763000000001976a914b7177c7deb43f3869eabc25cfd9f618215f34d5588ac00000000")

	blockA := &model.Block{
		Header:           chainAHeader1,
		CoinbaseTx:       coinbaseTx,
		TransactionCount: 1,
		Subtrees:         []*chainhash.Hash{},
	}
	err = ba.blockchainClient.AddBlock(ctx, blockA, "")
	require.NoError(t, err)
	time.Sleep(2 * time.Second)

	// Verify transactions in original chain
	mc1, subtrees1, err := ba.getMiningCandidate()
	require.NoError(t, err)
	require.NotNil(t, mc1)
	require.NotEmpty(t, subtrees1)

	//check the previous hash of the mining candidate
	prevHash := chainhash.Hash(mc1.PreviousHash)
	t.Logf("Mining candidate built on block with previous hash: %s", prevHash)
	assert.Equal(t, chainAHeader1.Hash().String(), prevHash.String(), "Mining candidate should be built on Chain A")

	// Now trigger reorg by adding Chain B block with higher difficulty
	t.Log("Triggering reorg with Chain B block (higher difficulty)...")
	blockB := &model.Block{
		Header:           chainBHeader1,
		CoinbaseTx:       coinbaseTx,
		TransactionCount: 1,
		Subtrees:         []*chainhash.Hash{},
	}
	err = ba.blockchainClient.AddBlock(ctx, blockB, "")
	require.NoError(t, err)
	time.Sleep(2 * time.Second)

	// Verify transactions are still present after reorg
	mc2, subtrees2, err := ba.getMiningCandidate()
	require.NoError(t, err)
	require.NotNil(t, mc2)
	require.NotEmpty(t, subtrees2)

	//check the previous hash of the mining candidate
	prevHash = chainhash.Hash(mc2.PreviousHash)
	t.Logf("Mining candidate built on block with previous hash: %s", prevHash)
	assert.Equal(t, chainBHeader1.Hash().String(), prevHash.String(), "Mining candidate should be built on Chain B")

	// Verify transaction count is maintained
	var foundTxsAfterReorg int
	for _, subtree := range subtrees2 {
		for _, node := range subtree.Nodes {
			if node.Hash.Equal(*testHash1) || node.Hash.Equal(*testHash2) || node.Hash.Equal(*testHash3) {
				foundTxsAfterReorg++
			}
		}
	}
	t.Logf("Found %d transactions after reorg", foundTxsAfterReorg)
	assert.Equal(t, 3, foundTxsAfterReorg, "All transactions should be preserved after reorg")

	// Verify we're on Chain B (higher difficulty chain)
	bestHeader, _, err := ba.blockchainClient.GetBestBlockHeader(ctx)
	require.NoError(t, err)
	assert.Equal(t, chainBHeader1.Hash(), bestHeader.Hash(),
		"Block assembler should follow Chain B due to higher difficulty")
}

func TestShouldHandleReorgWithLongerChain(t *testing.T) {
	tSettings := test.CreateBaseTestSettings()
	tSettings.Block.StoreCacheEnabled = false
	ctx := context.Background()
	done := make(chan struct{})

	blobStore := memory.New()
	utxoStore := utxostore.New(ulogger.TestLogger{})
	blockchainClient, err := blockchain.NewClient(ctx, ulogger.TestLogger{}, tSettings, "test")
	require.NoError(t, err)

	err = blockchainClient.Run(ctx, "test")
	require.NoError(t, err, "Blockchain client failed to start")

	t.Log("Creating block assembler...")
	baService := New(ulogger.TestLogger{}, tSettings, blobStore, utxoStore, blobStore, blockchainClient)
	require.NotNil(t, baService)
	err = baService.Init(ctx)
	require.NoError(t, err)

	// Start the service
	go func() {
		defer close(done)
		err = baService.Start(ctx)
		if err != nil {
			t.Errorf("Error starting service: %v", err)
		}
	}()

	time.Sleep(5 * time.Second)
	ba := baService.blockAssembler

	// Get initial state
	initialHeader, initialMetadata, err := ba.blockchainClient.GetBestBlockHeader(ctx)
	require.NoError(t, err)
	t.Logf("Initial block height: %d", initialMetadata.Height)

	// Create test transactions
	t.Log("Creating test transactions...")
	testTx1 := newTx(1)
	testTx2 := newTx(2)
	testTx3 := newTx(3)

	testHash1 := testTx1.TxIDChainHash()
	testHash2 := testTx2.TxIDChainHash()
	testHash3 := testTx3.TxIDChainHash()

	// Create chain A (original chain) with lower difficulty
	t.Log("Creating Chain A (lower difficulty)...")
	chainABits, _ := model.NewNBitFromString("207fffff") // Lower difficulty

	// Create multiple blocks for Chain A
	chainAHeader1 := &model.BlockHeader{
		Version:        1,
		HashPrevBlock:  initialHeader.Hash(),
		HashMerkleRoot: &chainhash.Hash{},
		Nonce:          1,
		Bits:           *chainABits,
		Timestamp:      uint32(time.Now().Unix()),
	}

	chainAHeader2 := &model.BlockHeader{
		Version:        1,
		HashPrevBlock:  chainAHeader1.Hash(),
		HashMerkleRoot: &chainhash.Hash{},
		Nonce:          2,
		Bits:           *chainABits,
		Timestamp:      uint32(time.Now().Unix()),
	}

	chainAHeader3 := &model.BlockHeader{
		Version:        1,
		HashPrevBlock:  chainAHeader2.Hash(),
		HashMerkleRoot: &chainhash.Hash{},
		Nonce:          3,
		Bits:           *chainABits,
		Timestamp:      uint32(time.Now().Unix()),
	}

	chainAHeader4 := &model.BlockHeader{
		Version:        1,
		HashPrevBlock:  chainAHeader3.Hash(),
		HashMerkleRoot: &chainhash.Hash{},
		Nonce:          4,
		Bits:           *chainABits,
		Timestamp:      uint32(time.Now().Unix()),
	}

	// Create chain B (competing chain) with higher difficulty
	t.Log("Creating Chain B (higher difficulty)...")
	chainBBits, _ := model.NewNBitFromString("1d00ffff") // Higher difficulty
	chainBHeader1 := &model.BlockHeader{
		Version:        1,
		HashPrevBlock:  initialHeader.Hash(),
		HashMerkleRoot: &chainhash.Hash{},
		Nonce:          10,
		Bits:           *chainBBits,
		Timestamp:      uint32(time.Now().Unix()),
	}

	// Add transactions
	t.Log("Adding transactions...")
	_, err = ba.utxoStore.Create(ctx, testTx1, 0)
	require.NoError(t, err)
	ba.AddTx(util.SubtreeNode{Hash: *testHash1, Fee: 111})

	_, err = ba.utxoStore.Create(ctx, testTx2, 0)
	require.NoError(t, err)
	ba.AddTx(util.SubtreeNode{Hash: *testHash2, Fee: 222})

	_, err = ba.utxoStore.Create(ctx, testTx3, 0)
	require.NoError(t, err)
	ba.AddTx(util.SubtreeNode{Hash: *testHash3, Fee: 333})

	// Add Chain A blocks (lower difficulty)
	t.Log("Adding Chain A blocks...")
	coinbaseTx, _ := bt.NewTxFromString("01000000010000000000000000000000000000000000000000000000000000000000000000ffffffff17030200002f6d312d65752f605f77009f74384816a31807ffffffff03ac505763000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88acaa505763000000001976a9143c22b6d9ba7b50b6d6e615c69d11ecb2ba3db14588acaa505763000000001976a914b7177c7deb43f3869eabc25cfd9f618215f34d5588ac00000000")

	// Add all 4 blocks from Chain A
	blockA1 := &model.Block{
		Header:           chainAHeader1,
		CoinbaseTx:       coinbaseTx,
		TransactionCount: 1,
		Subtrees:         []*chainhash.Hash{},
	}
	err = ba.blockchainClient.AddBlock(ctx, blockA1, "")
	require.NoError(t, err)

	blockA2 := &model.Block{
		Header:           chainAHeader2,
		CoinbaseTx:       coinbaseTx,
		TransactionCount: 1,
		Subtrees:         []*chainhash.Hash{},
	}
	err = ba.blockchainClient.AddBlock(ctx, blockA2, "")
	require.NoError(t, err)

	blockA3 := &model.Block{
		Header:           chainAHeader3,
		CoinbaseTx:       coinbaseTx,
		TransactionCount: 1,
		Subtrees:         []*chainhash.Hash{},
	}
	err = ba.blockchainClient.AddBlock(ctx, blockA3, "")
	require.NoError(t, err)

	blockA4 := &model.Block{
		Header:           chainAHeader4,
		CoinbaseTx:       coinbaseTx,
		TransactionCount: 1,
		Subtrees:         []*chainhash.Hash{},
	}
	err = ba.blockchainClient.AddBlock(ctx, blockA4, "")
	require.NoError(t, err)

	time.Sleep(2 * time.Second)

	// Get mining candidate while on Chain A
	t.Log("Getting mining candidate on Chain A...")
	mc1, subtrees1, err := ba.getMiningCandidate()
	require.NoError(t, err)
	require.NotNil(t, mc1)
	require.NotEmpty(t, subtrees1)

	// Verify we're on Chain A
	bestHeaderBeforeReorg, _, err := ba.blockchainClient.GetBestBlockHeader(ctx)
	require.NoError(t, err)
	assert.Equal(t, chainAHeader4.Hash(), bestHeaderBeforeReorg.Hash(),
		"Should be on Chain A before reorg")

	// Now trigger reorg by adding single Chain B block with higher difficulty
	t.Log("Triggering reorg with single Chain B block (higher difficulty)...")
	blockB := &model.Block{
		Header:           chainBHeader1,
		CoinbaseTx:       coinbaseTx,
		TransactionCount: 1,
		Subtrees:         []*chainhash.Hash{},
	}
	err = ba.blockchainClient.AddBlock(ctx, blockB, "")
	require.NoError(t, err)
	time.Sleep(2 * time.Second)

	// Verify transactions are still present after reorg
	mc2, subtrees2, err := ba.getMiningCandidate()
	require.NoError(t, err)
	require.NotNil(t, mc2)
	require.NotEmpty(t, subtrees2)

	// Verify transaction count is maintained
	var foundTxsAfterReorg int
	for _, subtree := range subtrees2 {
		for _, node := range subtree.Nodes {
			if node.Hash.Equal(*testHash1) || node.Hash.Equal(*testHash2) || node.Hash.Equal(*testHash3) {
				foundTxsAfterReorg++
			}
		}
	}
	t.Logf("Found %d transactions after reorg", foundTxsAfterReorg)
	assert.Equal(t, 3, foundTxsAfterReorg, "All transactions should be preserved after reorg")

	// Verify we're on Chain B (higher difficulty chain)
	bestHeaderAfterReorg, _, err := ba.blockchainClient.GetBestBlockHeader(ctx)
	require.NoError(t, err)
	assert.Equal(t, chainBHeader1.Hash(), bestHeaderAfterReorg.Hash(),
		"Block assembler should follow Chain B due to higher difficulty despite Chain A being longer")

	t.Logf("Chain A length: 4 blocks, Chain B length: 1 block")
	t.Logf("Chain A difficulty: %s", chainABits.String())
	t.Logf("Chain B difficulty: %s", chainBBits.String())
}
