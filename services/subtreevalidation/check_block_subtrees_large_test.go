package subtreevalidation

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"runtime/pprof"
	"sync"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-bt/v2/unlocker"
	bec "github.com/bsv-blockchain/go-sdk/primitives/ec"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/services/blockvalidation/testhelpers"
	"github.com/bsv-blockchain/teranode/services/subtreevalidation/subtreevalidation_api"
	"github.com/bsv-blockchain/teranode/services/validator"
	"github.com/bsv-blockchain/teranode/stores/blob"
	"github.com/bsv-blockchain/teranode/stores/blob/file"
	"github.com/bsv-blockchain/teranode/stores/utxo/meta"
	"github.com/bsv-blockchain/teranode/stores/utxo/nullstore"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// Large block test configuration constants
const (
	// numSubtreesLarge is the number of subtrees to generate
	numSubtreesLarge = 10

	// txPerSubtreeLarge is the number of transactions per subtree (1024 * 1024 = 1,048,576)
	txPerSubtreeLarge = 512 * 1024

	// numWorkersLarge is the number of parallel workers generating transaction chains
	numWorkersLarge = 20000

	// totalTxLarge is the total number of transactions (10 * 1,048,576 = 10,485,760)
	totalTxLarge = numSubtreesLarge * txPerSubtreeLarge

	// txPerChainLarge is the approximate number of transactions per chain
	// 10,485,760 / 20,000 = ~524 transactions per chain
	txPerChainLarge = totalTxLarge / numWorkersLarge

	// satoshisPerOutput is the satoshis for each coinbase output
	satoshisPerOutput = uint64(50_000_000_000) // 500 BSV per worker

	// satoshisPerTx is the satoshis for each regular transaction output
	satoshisPerTx = uint64(1000)
)

// metadataKey is a fixed key used to store test metadata in the blob store
// (sha256 hash of "large_block_test_metadata")
var metadataKey = sha256.Sum256([]byte("large_block_test_metadata"))

// LargeBlockFixture contains all test data for large block tests
type LargeBlockFixture struct {
	Block         *model.Block
	CoinbaseTxs   []*bt.Tx // Multiple coinbase transactions to avoid hot keys
	SubtreeHashes []*chainhash.Hash
	TotalTxCount  int
}

// BlockMetadata for JSON cache file
type BlockMetadata struct {
	BlockHash      string   `json:"block_hash"`
	MerkleRoot     string   `json:"merkle_root"`
	SubtreeHashes  []string `json:"subtree_hashes"`
	TotalTxCount   int      `json:"total_tx_count"`
	NumSubtrees    int      `json:"num_subtrees"`
	TxPerSubtree   int      `json:"tx_per_subtree"`
	GeneratedAt    string   `json:"generated_at"`
	CoinbaseTxsHex []string `json:"coinbase_txs_hex"`
}

// TopologicalOrderValidator is a mock validator that verifies topological ordering.
// It tracks validated transaction hashes and ensures parents are validated before children.
// This directly tests the fix for preserving topological order in the streaming processor.
type TopologicalOrderValidator struct {
	validated sync.Map // map[chainhash.Hash]struct{} - tracks validated tx hashes
	t         *testing.T
}

// NewTopologicalOrderValidator creates a new validator that checks topological ordering.
func NewTopologicalOrderValidator(t *testing.T) *TopologicalOrderValidator {
	return &TopologicalOrderValidator{t: t}
}

// SeedCoinbase marks coinbase transactions as validated (they have no parents).
func (v *TopologicalOrderValidator) SeedCoinbase(txs []*bt.Tx) {
	for _, tx := range txs {
		txHash := tx.TxIDChainHash()
		v.validated.Store(*txHash, struct{}{})
	}
}

// Health implements validator.Interface.
func (v *TopologicalOrderValidator) Health(ctx context.Context, checkLiveness bool) (int, string, error) {
	return 0, "TopologicalOrderValidator", nil
}

// Validate checks that all parent transactions were validated before this transaction.
// If a parent is missing, it returns an error indicating broken topological order.
func (v *TopologicalOrderValidator) Validate(ctx context.Context, tx *bt.Tx, blockHeight uint32, opts ...validator.Option) (*meta.Data, error) {
	return v.ValidateWithOptions(ctx, tx, blockHeight, nil)
}

// ValidateWithOptions checks that all parent transactions were validated before this transaction.
func (v *TopologicalOrderValidator) ValidateWithOptions(ctx context.Context, tx *bt.Tx, blockHeight uint32, validationOptions *validator.Options) (*meta.Data, error) {
	txHash := tx.TxIDChainHash()

	// Skip coinbase transactions (they have no parents)
	if tx.IsCoinbase() {
		v.validated.Store(*txHash, struct{}{})
		return v.createMinimalMetadata(tx), nil
	}

	// Check that all input parent txids are already validated
	for i, input := range tx.Inputs {
		if input == nil {
			continue
		}
		parentTxID := input.PreviousTxID()
		if parentTxID == nil {
			continue
		}

		parentHash, err := chainhash.NewHash(parentTxID)
		if err != nil {
			return nil, errors.NewProcessingError("invalid parent txid at input %d: %v", i, err)
		}

		if _, found := v.validated.Load(*parentHash); !found {
			return nil, errors.NewProcessingError("TOPOLOGICAL ORDER VIOLATION: parent tx %s was not validated before child tx %s (input %d)", parentHash.String(), txHash.String(), i)
		}
	}

	// All parents validated, add this tx to validated set
	v.validated.Store(*txHash, struct{}{})

	// Return minimal metadata without fee calculation
	return v.createMinimalMetadata(tx), nil
}

// createMinimalMetadata returns a minimal meta.Data without fee calculation
func (v *TopologicalOrderValidator) createMinimalMetadata(tx *bt.Tx) *meta.Data {
	// Extract TxInpoints from the transaction - needed for subtree serialization
	txInpoints, _ := subtreepkg.NewTxInpointsFromTx(tx)
	return &meta.Data{
		Tx:         tx,
		TxInpoints: txInpoints,
		BlockIDs:   make([]uint32, 0),
		IsCoinbase: tx.IsCoinbase(),
		LockTime:   tx.LockTime,
	}
}

// GetBlockHeight implements validator.Interface.
func (v *TopologicalOrderValidator) GetBlockHeight() uint32 {
	return 100
}

// GetMedianBlockTime implements validator.Interface.
func (v *TopologicalOrderValidator) GetMedianBlockTime() uint32 {
	return uint32(time.Now().Unix())
}

// TriggerBatcher implements validator.Interface (no-op).
func (v *TopologicalOrderValidator) TriggerBatcher() {}

// TestCheckBlockSubtreesLevelBasedLargeBlock benchmarks CheckBlockSubtrees with level-based processor
// using 10 million transactions across 10 subtrees.
func TestCheckBlockSubtreesLevelBasedLargeBlock(t *testing.T) {
	t.Skip()
	if testing.Short() {
		t.Skip("Skipping large block test in short mode")
	}

	runLargeBlockTest(t)
}

// runLargeBlockTest is the implementation for large block tests.
func runLargeBlockTest(t *testing.T) {
	t.Logf("=== Large Block Test ===")
	t.Logf("Configuration: %d subtrees, %d tx/subtree, %d total tx", numSubtreesLarge, txPerSubtreeLarge, totalTxLarge)
	t.Logf("Workers: %d, ~%d tx/chain", numWorkersLarge, txPerChainLarge)

	// Get cache directory, from the current working directory
	cacheDir, _ := os.Getwd()
	cacheDir += "/data/large_block_test_cache"
	t.Logf("Cache directory: %s", cacheDir)

	// Create cache directory if not exists
	err := os.MkdirAll(cacheDir, 0755)
	require.NoError(t, err)

	// Create blob store early - all helpers will use this same instance
	store := createBlobStore(t, cacheDir)

	// Load or generate test data
	fixture := loadOrGenerateLargeBlockData(t, store)
	t.Logf("Test data ready: %d transactions, %d subtrees", fixture.TotalTxCount, len(fixture.SubtreeHashes))

	// Clean up any existing .subtree validation files (keep .subtreeToCheck and .subtreeData for cache)
	// This ensures the test actually validates the subtrees instead of finding them already validated
	t.Logf("Cleaning up existing .subtree validation files...")
	cleanupValidatedSubtrees(t, store, fixture.SubtreeHashes)

	// Setup server using the same blob store and cache directory
	server, cleanup := setupLargeTestServer(t, cacheDir, store, fixture)
	defer cleanup()

	// Prepare request
	blockBytes, err := fixture.Block.Bytes()
	require.NoError(t, err)

	request := &subtreevalidation_api.CheckBlockSubtreesRequest{
		Block:   blockBytes,
		BaseUrl: "legacy", // Use legacy mode to load from store
	}

	// Run with profiling
	profileDir := filepath.Join(cacheDir, "profiles")
	profileName := "subtree_processor"

	t.Logf("Starting CheckBlockSubtrees test in profileDir: %s", profileDir)

	elapsed := runWithProfiling(t, profileDir, profileName, func() {
		server.logger = ulogger.New("CheckBlockSubtreesLargeBlockTest", ulogger.WithLevel("info"))
		_, err := server.CheckBlockSubtrees(t.Context(), request)
		require.NoError(t, err, errors.UnwrapGRPC(err).Error())
		server.logger = ulogger.TestLogger{}
	})

	// Report results
	throughput := float64(fixture.TotalTxCount) / elapsed.Seconds()
	t.Logf("=== Results ===")
	t.Logf("Elapsed: %.2fs", elapsed.Seconds())
	t.Logf("Throughput: %.2f tx/sec", throughput)
	t.Logf("CPU profile: %s/%s_cpu.prof", profileDir, profileName)
	t.Logf("Mem profile: %s/%s_mem.prof", profileDir, profileName)
}

// cleanupValidatedSubtrees deletes validated .subtree files while keeping .subtreeToCheck and .subtreeData.
// This ensures the test actually validates subtrees instead of finding them already validated.
func cleanupValidatedSubtrees(t *testing.T, store blob.Store, subtreeHashes []*chainhash.Hash) {
	t.Helper()

	ctx := context.Background()
	for _, hash := range subtreeHashes {
		// Delete validated subtree file if it exists
		err := store.Del(ctx, hash[:], fileformat.FileTypeSubtree)
		if err != nil && !errors.Is(err, errors.ErrNotFound) {
			t.Logf("Warning: failed to delete validated subtree %s: %v", hash.String(), err)
		}

		// Also delete subtree meta file if it exists
		if err = store.Del(ctx, hash[:], fileformat.FileTypeSubtreeMeta); err != nil && !errors.Is(err, errors.ErrNotFound) {
			t.Logf("Warning: failed to delete validated subtree %s: %v", hash.String(), err)
		}
	}
}

// createBlobStore creates a file-based blob store for the given cache directory.
func createBlobStore(t *testing.T, cacheDir string) blob.Store {
	t.Helper()

	logger := ulogger.TestLogger{}
	storeURL, err := url.Parse("file://" + cacheDir)
	require.NoError(t, err)

	store, err := file.New(logger, storeURL)
	require.NoError(t, err)

	return store
}

// loadOrGenerateLargeBlockData loads test data from cache or generates it.
func loadOrGenerateLargeBlockData(t *testing.T, store blob.Store) *LargeBlockFixture {
	t.Helper()

	// Try to load from cache
	fixture, err := loadFromCache(t, store)
	if err == nil {
		t.Logf("Loaded test data from cache")
		return fixture
	}

	t.Logf("Cache not found or invalid (%v), generating test data...", err)
	t.Logf("This may take 10-30 minutes for first run")

	// Generate new test data, writing directly to blob store
	fixture = generateLargeBlockData(t, store)

	// Save metadata to cache
	err = saveToCache(t, store, fixture)
	require.NoError(t, err)

	t.Logf("Test data generated and cached")
	return fixture
}

// loadFromCache attempts to load test data from cache.
// Uses the blob store to load metadata and verify subtree data exists.
func loadFromCache(t *testing.T, store blob.Store) (*LargeBlockFixture, error) {
	t.Helper()

	ctx := context.Background()

	// Read metadata from blob store using FileTypeDat
	data, err := store.Get(ctx, metadataKey[:], fileformat.FileTypeDat)
	if err != nil {
		return nil, errors.NewStorageError("failed to read metadata", err)
	}

	var metadata BlockMetadata
	if err := json.Unmarshal(data, &metadata); err != nil {
		return nil, errors.NewProcessingError("failed to unmarshal metadata", err)
	}

	// Verify all subtree files exist via blob store
	for _, hashStr := range metadata.SubtreeHashes {
		hash, err := chainhash.NewHashFromStr(hashStr)
		if err != nil {
			return nil, errors.NewProcessingError("invalid subtree hash", err)
		}

		// Check SubtreeToCheck exists
		_, err = store.Get(ctx, hash[:], fileformat.FileTypeSubtreeToCheck)
		if err != nil {
			return nil, errors.NewStorageError("missing subtree to check: %s", hashStr, err)
		}

		// Check SubtreeData exists
		_, err = store.Get(ctx, hash[:], fileformat.FileTypeSubtreeData)
		if err != nil {
			return nil, errors.NewStorageError("missing subtree data: %s", hashStr, err)
		}
	}

	// Reconstruct fixture
	subtreeHashes := make([]*chainhash.Hash, len(metadata.SubtreeHashes))
	for i, hashStr := range metadata.SubtreeHashes {
		hash, err := chainhash.NewHashFromStr(hashStr)
		if err != nil {
			return nil, errors.NewProcessingError("invalid subtree hash", err)
		}
		subtreeHashes[i] = hash
	}

	// Decode coinbase transactions
	coinbaseTxs := make([]*bt.Tx, len(metadata.CoinbaseTxsHex))
	for i, hexStr := range metadata.CoinbaseTxsHex {
		coinbaseTxBytes, err := hexDecode(hexStr)
		if err != nil {
			return nil, errors.NewProcessingError("failed to decode coinbase %d", i, err)
		}
		coinbaseTx, err := bt.NewTxFromBytes(coinbaseTxBytes)
		if err != nil {
			return nil, errors.NewProcessingError("failed to parse coinbase %d", i, err)
		}
		coinbaseTxs[i] = coinbaseTx
	}

	// Reconstruct block
	merkleRoot, err := chainhash.NewHashFromStr(metadata.MerkleRoot)
	if err != nil {
		return nil, errors.NewProcessingError("invalid merkle root", err)
	}

	nBits, _ := model.NewNBitFromString("2000ffff")
	hashPrevBlock, _ := chainhash.NewHashFromStr("0f9188f13cb7b2c71f2a335e3a4fc328bf5beb436012afca590b1a11466e2206")

	blockHeader := &model.BlockHeader{
		Version:        1,
		HashPrevBlock:  hashPrevBlock,
		HashMerkleRoot: merkleRoot,
		Timestamp:      uint32(time.Now().Unix()),
		Bits:           *nBits,
		Nonce:          0,
	}

	// Use first coinbase as block's coinbase (for merkle root calculation)
	block := &model.Block{
		Header:           blockHeader,
		CoinbaseTx:       coinbaseTxs[0],
		TransactionCount: uint64(metadata.TotalTxCount),
		Subtrees:         subtreeHashes,
		Height:           100,
	}

	return &LargeBlockFixture{
		Block:         block,
		CoinbaseTxs:   coinbaseTxs,
		SubtreeHashes: subtreeHashes,
		TotalTxCount:  metadata.TotalTxCount,
	}, nil
}

// saveToCache saves test data to cache.
// Uses the blob store to save metadata.
func saveToCache(t *testing.T, store blob.Store, fixture *LargeBlockFixture) error {
	t.Helper()

	ctx := context.Background()

	subtreeHashStrs := make([]string, len(fixture.SubtreeHashes))
	for i, hash := range fixture.SubtreeHashes {
		subtreeHashStrs[i] = hash.String()
	}

	merkleRoot := fixture.Block.Header.HashMerkleRoot
	blockHash := fixture.Block.Header.Hash()

	coinbaseTxsHex := make([]string, len(fixture.CoinbaseTxs))
	for i, coinbaseTx := range fixture.CoinbaseTxs {
		coinbaseTxsHex[i] = hexEncode(coinbaseTx.Bytes())
	}

	metadata := BlockMetadata{
		BlockHash:      blockHash.String(),
		MerkleRoot:     merkleRoot.String(),
		SubtreeHashes:  subtreeHashStrs,
		TotalTxCount:   fixture.TotalTxCount,
		NumSubtrees:    len(fixture.SubtreeHashes),
		TxPerSubtree:   txPerSubtreeLarge,
		GeneratedAt:    time.Now().Format(time.RFC3339),
		CoinbaseTxsHex: coinbaseTxsHex,
	}

	data, err := json.MarshalIndent(metadata, "", "  ")
	if err != nil {
		return errors.NewProcessingError("failed to marshal metadata", err)
	}

	// Write metadata to blob store using FileTypeDat
	if err := store.Set(ctx, metadataKey[:], fileformat.FileTypeDat, data); err != nil {
		return errors.NewStorageError("failed to write metadata", err)
	}

	return nil
}

// generateLargeBlockData generates test data with 10M transactions using 20K workers.
// Workers generate transactions directly onto a channel, which is consumed in real-time
// to build subtrees, minimizing memory usage.
func generateLargeBlockData(t *testing.T, store blob.Store) *LargeBlockFixture {
	t.Helper()

	startTime := time.Now()

	// Step 1: Create 20,000 separate coinbase transactions (one per worker to avoid hot keys)
	t.Logf("[%v] Creating %d separate coinbase transactions...", time.Since(startTime), numWorkersLarge)
	coinbaseTxs, workerKeys := createSeparateCoinbases(t, numWorkersLarge, satoshisPerOutput)
	t.Logf("[%v] Created %d coinbase transactions", time.Since(startTime), len(coinbaseTxs))

	// Step 2: Generate transactions directly to channel and stream into subtrees
	t.Logf("[%v] Generating transactions with %d workers streaming to subtrees...", time.Since(startTime), numWorkersLarge)

	// Channel for workers to send transactions directly
	txChan := make(chan *bt.Tx, 100000) // Large buffer for throughput
	var wg sync.WaitGroup

	// Launch workers - each generates a full chain (no early termination with zero fees)
	for workerID := 0; workerID < numWorkersLarge; workerID++ {
		wg.Add(1)
		go func(wID int) {
			defer wg.Done()
			generateChainToChannel(t, coinbaseTxs[wID], workerKeys[wID], txPerChainLarge, txChan)
		}(workerID)
	}

	// Close channel when all workers done
	go func() {
		wg.Wait()
		close(txChan)
	}()

	// Consume transactions from channel and build subtrees in real-time
	subtreeHashes, totalTxCount := streamTransactionsToSubtrees(t, store, txChan, startTime)

	t.Logf("[%v] Total transactions streamed: %d", time.Since(startTime), totalTxCount)

	// Calculate merkle root from subtree hashes
	merkleRoot := calculateMerkleRootFromSubtreeHashes(t, subtreeHashes)

	// Create block
	nBits, _ := model.NewNBitFromString("2000ffff")
	hashPrevBlock, _ := chainhash.NewHashFromStr("0f9188f13cb7b2c71f2a335e3a4fc328bf5beb436012afca590b1a11466e2206")

	blockHeader := &model.BlockHeader{
		Version:        1,
		HashPrevBlock:  hashPrevBlock,
		HashMerkleRoot: merkleRoot,
		Timestamp:      uint32(time.Now().Unix()),
		Bits:           *nBits,
		Nonce:          0,
	}

	// Use first coinbase as block's coinbase (for merkle root calculation)
	block := &model.Block{
		Header:           blockHeader,
		CoinbaseTx:       coinbaseTxs[0],
		TransactionCount: uint64(totalTxCount),
		Subtrees:         subtreeHashes,
		Height:           100,
	}

	t.Logf("[%v] Block created with %d subtrees", time.Since(startTime), len(subtreeHashes))

	return &LargeBlockFixture{
		Block:         block,
		CoinbaseTxs:   coinbaseTxs,
		SubtreeHashes: subtreeHashes,
		TotalTxCount:  totalTxCount,
	}
}

// generateChainToChannel generates a chain of transactions and sends each directly to the channel.
// Creates exactly chainLength transactions by maintaining a circular output pattern.
func generateChainToChannel(t *testing.T, coinbaseTx *bt.Tx, privKey *bec.PrivateKey, chainLength int, txChan chan<- *bt.Tx) {
	t.Helper()

	// First transaction spends coinbase output 0
	parentTx := coinbaseTx
	vout := uint32(0)
	parentSatoshis := parentTx.Outputs[vout].Satoshis

	for i := 0; i < chainLength; i++ {
		tx := bt.NewTx()

		// Add input from parent
		err := tx.FromUTXOs(&bt.UTXO{
			TxIDHash:      parentTx.TxIDChainHash(),
			Vout:          vout,
			LockingScript: parentTx.Outputs[vout].LockingScript,
			Satoshis:      parentSatoshis,
		})
		if err != nil {
			t.Logf("Worker chain generation error at tx %d: %v", i, err)
			return
		}

		// No fees needed for block validation testing - use all parent satoshis
		outputAmount := parentSatoshis

		script, err := bscript.NewP2PKHFromPubKeyBytes(privKey.PubKey().Compressed())
		if err != nil {
			t.Logf("Worker chain generation error creating script: %v", err)
			return
		}

		tx.AddOutput(&bt.Output{
			Satoshis:      outputAmount,
			LockingScript: script,
		})

		// Sign
		err = tx.FillAllInputs(context.Background(), &unlocker.Getter{PrivateKey: privKey})
		if err != nil {
			t.Logf("Worker chain generation error signing: %v", err)
			return
		}

		// Send transaction directly to channel
		txChan <- tx

		// Next transaction spends this output
		parentTx = tx
		vout = 0
		parentSatoshis = outputAmount
	}
}

// streamTransactionsToSubtrees consumes transactions from channel and builds subtrees in real-time.
func streamTransactionsToSubtrees(t *testing.T, store blob.Store, txChan <-chan *bt.Tx, startTime time.Time) ([]*chainhash.Hash, int) {
	t.Helper()

	ctx := context.Background()
	subtreeHashes := make([]*chainhash.Hash, 0)
	totalTxCount := 1 // Start with 1 for coinbase

	// Current subtree being built
	var currentSubtree *subtreepkg.Subtree
	var currentTxData []byte
	subtreeIdx := 0
	txInCurrentSubtree := 0

	// Create first subtree with coinbase placeholder
	var err error
	currentSubtree, err = subtreepkg.NewTreeByLeafCount(txPerSubtreeLarge)
	require.NoError(t, err)
	err = currentSubtree.AddCoinbaseNode()
	require.NoError(t, err)
	txInCurrentSubtree = 1

	lastLogTime := time.Now()

	// Consume transactions from channel
	for tx := range txChan {
		// Check if current subtree is full
		if txInCurrentSubtree >= txPerSubtreeLarge {
			// Finalize and write current subtree to blob store
			subtreeHash := writeSubtreeToStore(t, ctx, store, currentSubtree, currentTxData)
			subtreeHashes = append(subtreeHashes, subtreeHash)

			t.Logf("[%v] Wrote subtree %d with %d transactions", time.Since(startTime), subtreeIdx, txInCurrentSubtree)
			subtreeIdx++

			// Start new subtree
			currentSubtree, err = subtreepkg.NewTreeByLeafCount(txPerSubtreeLarge)
			require.NoError(t, err)
			currentTxData = nil
			txInCurrentSubtree = 0
		}

		// Add transaction to subtree
		txHash := tx.TxIDChainHash()
		fee := calculateTxFeeLarge(tx)
		size := uint64(len(tx.Bytes()))

		err = currentSubtree.AddNode(*txHash, fee, size)
		require.NoError(t, err)

		currentTxData = append(currentTxData, tx.Bytes()...)
		txInCurrentSubtree++
		totalTxCount++

		// Log progress periodically
		if time.Since(lastLogTime) > 5*time.Second {
			t.Logf("[%v] Streamed %d transactions, %d subtrees complete", time.Since(startTime), totalTxCount, subtreeIdx)
			lastLogTime = time.Now()
		}
	}

	// Write final subtree if not empty
	if txInCurrentSubtree > 0 {
		subtreeHash := writeSubtreeToStore(t, ctx, store, currentSubtree, currentTxData)
		subtreeHashes = append(subtreeHashes, subtreeHash)
		t.Logf("[%v] Wrote final subtree %d with %d transactions", time.Since(startTime), subtreeIdx, txInCurrentSubtree)
	}

	return subtreeHashes, totalTxCount
}

// createSeparateCoinbases creates separate coinbase transactions (one per worker) to avoid hot keys.
func createSeparateCoinbases(t *testing.T, numWorkers int, satoshisPerOutput uint64) ([]*bt.Tx, []*bec.PrivateKey) {
	t.Helper()

	coinbaseTxs := make([]*bt.Tx, numWorkers)
	workerKeys := make([]*bec.PrivateKey, numWorkers)

	for workerID := 0; workerID < numWorkers; workerID++ {
		tx := bt.NewTx()

		// Add coinbase input
		blockHeightBytes := make([]byte, 4)
		binary.LittleEndian.PutUint32(blockHeightBytes, 100)

		arbitraryData := make([]byte, 0)
		arbitraryData = append(arbitraryData, 0x03)
		arbitraryData = append(arbitraryData, blockHeightBytes[:3]...)
		arbitraryData = append(arbitraryData, []byte(fmt.Sprintf("/Worker %d/", workerID))...)

		input := &bt.Input{
			SequenceNumber:     0xFFFFFFFF,
			UnlockingScript:    bscript.NewFromBytes(arbitraryData),
			PreviousTxSatoshis: 0,
			PreviousTxOutIndex: 0xFFFFFFFF,
		}
		zeroHash := new(chainhash.Hash)
		err := input.PreviousTxIDAdd(zeroHash)
		require.NoError(t, err)
		tx.Inputs = append(tx.Inputs, input)

		// Generate key and create single output
		privKey := deriveKeyForWorker(workerID)
		workerKeys[workerID] = privKey

		script, err := bscript.NewP2PKHFromPubKeyBytes(privKey.PubKey().Compressed())
		require.NoError(t, err)

		tx.AddOutput(&bt.Output{
			Satoshis:      satoshisPerOutput,
			LockingScript: script,
		})

		coinbaseTxs[workerID] = tx
	}

	return coinbaseTxs, workerKeys
}

// deriveKeyForWorker derives a deterministic private key for a worker.
func deriveKeyForWorker(workerID int) *bec.PrivateKey {
	seed := fmt.Sprintf("worker_key_%d_large_block_test", workerID)
	hash := sha256.Sum256([]byte(seed))
	privKey, _ := bec.PrivateKeyFromBytes(hash[:])
	return privKey
}

// writeSubtreeToStore writes a subtree and its data to the blob store.
func writeSubtreeToStore(t *testing.T, ctx context.Context, store blob.Store, subtree *subtreepkg.Subtree, txData []byte) *chainhash.Hash {
	t.Helper()

	rootHash := subtree.RootHash()

	// Serialize subtree structure
	subtreeBytes, err := subtree.Serialize()
	require.NoError(t, err)

	// Write subtree to check file using blob store
	err = store.Set(ctx, rootHash[:], fileformat.FileTypeSubtreeToCheck, subtreeBytes)
	require.NoError(t, err)

	// Write subtree data using blob store
	err = store.Set(ctx, rootHash[:], fileformat.FileTypeSubtreeData, txData)
	require.NoError(t, err)

	return rootHash
}

// calculateTxFeeLarge calculates the fee for a transaction.
func calculateTxFeeLarge(tx *bt.Tx) uint64 {
	inputTotal := tx.TotalInputSatoshis()
	outputTotal := tx.TotalOutputSatoshis()
	if inputTotal > outputTotal {
		return inputTotal - outputTotal
	}
	return 0
}

// calculateMerkleRootFromSubtreeHashes computes merkle root from subtree hashes.
func calculateMerkleRootFromSubtreeHashes(t *testing.T, subtreeHashes []*chainhash.Hash) *chainhash.Hash {
	t.Helper()

	if len(subtreeHashes) == 0 {
		return &chainhash.Hash{}
	}

	if len(subtreeHashes) == 1 {
		return subtreeHashes[0]
	}

	// Create merkle tree from subtree hashes
	st, err := subtreepkg.NewIncompleteTreeByLeafCount(len(subtreeHashes))
	require.NoError(t, err)

	for _, hash := range subtreeHashes {
		err := st.AddNode(*hash, 1, 0)
		require.NoError(t, err)
	}

	rootHash := st.RootHash()
	result, err := chainhash.NewHash(rootHash[:])
	require.NoError(t, err)

	return result
}

// setupLargeTestServer creates a server for large block testing.
// Uses the provided blob store directly - data was written there during generation.
func setupLargeTestServer(t *testing.T, cacheDir string, subtreeStore blob.Store, fixture *LargeBlockFixture) (*Server, func()) {
	t.Helper()

	logger := ulogger.TestLogger{}

	tSettings := test.CreateBaseTestSettings(t)
	// Override DataFolder to use our cache directory instead of temp
	tSettings.DataFolder = cacheDir
	tSettings.SubtreeValidation.SpendBatcherSize = 1000
	tSettings.BlockAssembly.Disabled = true

	// Use NullStore for UTXO - we're testing the subtree validation logic, not the UTXO store
	utxoStore, err := nullstore.NewNullStore()
	require.NoError(t, err)

	// Create mock blockchain client
	mockBlockchainClient := &blockchain.Mock{}

	// Create TopologicalOrderValidator - this checks that parents are validated before children
	// If topological order is broken, validation will fail with a clear error
	topologicalValidator := NewTopologicalOrderValidator(t)

	// Seed coinbase transactions into the mock validator
	// Coinbase txs have no parents, so they are pre-seeded as "already validated"
	topologicalValidator.SeedCoinbase(fixture.CoinbaseTxs)
	t.Logf("Seeded %d coinbase transactions into topological validator", len(fixture.CoinbaseTxs))

	// Setup blockchain mocks
	testHeaders := testhelpers.CreateTestHeaders(t, 1)
	genesisHash, _ := chainhash.NewHashFromStr("0f9188f13cb7b2c71f2a335e3a4fc328bf5beb436012afca590b1a11466e2206")

	mockBlockchainClient.On("GetBestBlockHeader", mock.Anything).
		Return(testHeaders[0], &model.BlockHeaderMeta{ID: 100}, nil).Maybe()
	mockBlockchainClient.On("GetBlockHeaderIDs", mock.Anything, mock.Anything, mock.Anything).
		Return([]uint32{100, 99, 98}, nil).Maybe()
	mockBlockchainClient.On("IsFSMCurrentState", mock.Anything, blockchain.FSMStateRUNNING).
		Return(true, nil).Maybe()
	runningState := blockchain.FSMStateRUNNING
	mockBlockchainClient.On("GetFSMCurrentState", mock.Anything).
		Return(&runningState, nil).Maybe()
	mockBlockchainClient.On("GetBlockExists", mock.Anything, mock.Anything).
		Return(true, nil).Maybe()
	mockBlockchainClient.On("GetBlockHeader", mock.Anything, genesisHash).
		Return(testHeaders[0], &model.BlockHeaderMeta{ID: 99}, nil).Maybe()
	mockBlockchainClient.On("CheckBlockIsInCurrentChain", mock.Anything, mock.Anything).
		Return(true, nil).Maybe()

	// Create server with the topological order validator
	server := &Server{
		logger:           logger,
		settings:         tSettings,
		subtreeStore:     subtreeStore,
		txStore:          subtreeStore,
		utxoStore:        utxoStore,
		validatorClient:  topologicalValidator,
		blockchainClient: mockBlockchainClient,
	}

	// Initialize orphanage
	orphanage, err := NewOrphanage(time.Minute*10, 100, logger)
	require.NoError(t, err)
	server.orphanage = orphanage

	// Container cleanup is handled by t.Cleanup above
	return server, func() {}
}

// runWithProfiling runs a function with CPU and memory profiling.
func runWithProfiling(t *testing.T, profileDir, name string, fn func()) time.Duration {
	t.Helper()

	err := os.MkdirAll(profileDir, 0755)
	require.NoError(t, err)

	// Start CPU profiling
	cpuProfilePath := filepath.Join(profileDir, name+"_cpu.prof")
	cpuFile, err := os.Create(cpuProfilePath)
	require.NoError(t, err)
	defer cpuFile.Close()

	err = pprof.StartCPUProfile(cpuFile)
	require.NoError(t, err)

	// Run the function
	start := time.Now()
	fn()
	elapsed := time.Since(start)

	// Stop CPU profiling
	pprof.StopCPUProfile()

	// Write memory profile
	memProfilePath := filepath.Join(profileDir, name+"_mem.prof")
	memFile, err := os.Create(memProfilePath)
	require.NoError(t, err)
	defer memFile.Close()

	runtime.GC() // Force GC before memory profile
	err = pprof.WriteHeapProfile(memFile)
	require.NoError(t, err)

	return elapsed
}

// Helper functions for hex encoding/decoding
func hexEncode(data []byte) string {
	return fmt.Sprintf("%x", data)
}

func hexDecode(s string) ([]byte, error) {
	result := make([]byte, len(s)/2)
	for i := 0; i < len(s)/2; i++ {
		_, err := fmt.Sscanf(s[i*2:i*2+2], "%02x", &result[i])
		if err != nil {
			return nil, err
		}
	}
	return result, nil
}
