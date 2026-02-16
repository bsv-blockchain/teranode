// Package blockassembly provides functionality for assembling Bitcoin blocks in Teranode.
package blockassembly

import (
	"context"
	"crypto/rand"
	"fmt"
	"math"
	"net"
	"net/http"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	primitives "github.com/bsv-blockchain/go-sdk/primitives/ec"
	"github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/services/blockassembly/blockassembly_api"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/stores/blob/memory"
	"github.com/bsv-blockchain/teranode/stores/blockchain/options"
	"github.com/bsv-blockchain/teranode/stores/utxo/sql"
	nodehelpers "github.com/bsv-blockchain/teranode/test/nodeHelpers"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// getFreePort returns a free port number on localhost
func getFreePort(t *testing.T) int {
	listener, err := net.Listen("tcp", "localhost:0")
	require.NoError(t, err)
	defer listener.Close()
	return listener.Addr().(*net.TCPAddr).Port
}

// setupTest initializes the test environment and returns a cleanup function.
// The cleanup function should be deferred by the calling test.
func setupTest(t *testing.T) (*nodehelpers.BlockchainDaemon, *BlockAssembly, context.Context, context.CancelFunc, func()) {
	err := os.RemoveAll("./data")
	require.NoError(t, err)

	blockchainDaemon, err := nodehelpers.NewBlockchainDaemon(t)
	require.NoError(t, err)
	err = blockchainDaemon.StartBlockchainService()
	require.NoError(t, err, "Failed to start blockchain service")

	// Setup block assembly service
	tSettings := blockchainDaemon.Settings

	// Use a dynamic port for BlockAssembly to avoid conflicts
	baPort := getFreePort(t)
	tSettings.BlockAssembly.GRPCListenAddress = fmt.Sprintf("localhost:%d", baPort)
	tSettings.BlockAssembly.GRPCAddress = fmt.Sprintf("localhost:%d", baPort)
	// Set DoubleSpendWindow to 0 so transactions are dequeued immediately in tests
	tSettings.BlockAssembly.DoubleSpendWindow = 0

	ctx, cancel := context.WithCancel(context.Background())
	memStore := memory.New()
	blobStore := memory.New()

	logger := ulogger.NewErrorTestLogger(t)
	settings := test.CreateBaseTestSettings(t)

	utxoStoreURL, err := url.Parse("sqlitememory:///test")
	require.NoError(t, err)

	utxoStore, err := sql.New(ctx, logger, settings, utxoStoreURL)
	require.NoError(t, err)

	// Use the blockchain client from the daemon which is already connected and running
	ba := New(ulogger.TestLogger{}, tSettings, memStore, utxoStore, blobStore, blockchainDaemon.BlockchainClient)
	require.NotNil(t, ba)

	// Skip waiting for pending blocks in tests to prevent hanging
	ba.SetSkipWaitForPendingBlocks(true)

	// Log the gRPC addresses
	t.Logf("BlockAssembly GRPCListenAddress: %s", tSettings.BlockAssembly.GRPCListenAddress)
	t.Logf("BlockAssembly GRPCAddress: %s", tSettings.BlockAssembly.GRPCAddress)

	err = ba.Init(ctx)
	require.NoError(t, err)

	readyCh := make(chan struct{}, 1)
	startErrCh := make(chan error, 1)
	go func() {
		defer func() {
			// Recover from any panic that might occur when the test has already completed
			if r := recover(); r != nil {
				// Log to stderr instead of using t.Logf since test may be done
				fmt.Fprintf(os.Stderr, "Recovered from panic in ba.Start goroutine: %v\n", r)
			}
		}()

		err := ba.Start(ctx, readyCh)
		// Send error to channel instead of calling t.Errorf directly
		// This avoids calling test methods after the test completes
		startErrCh <- err
	}()

	<-readyCh // Wait for service to be ready

	// Check for startup errors in cleanup, not in the goroutine
	cleanup := func() {
		// First cancel the context to stop ba.Start
		cancel()

		// Then check if there was a startup error
		select {
		case err := <-startErrCh:
			// Only report errors if context wasn't cancelled
			if err != nil && ctx.Err() == nil {
				t.Errorf("Error starting block assembly service: %v", err)
			}
		case <-time.After(100 * time.Millisecond):
			// ba.Start is still running, that's okay
		}

		if err := ba.Stop(ctx); err != nil {
			t.Logf("Error stopping block assembly service: %v", err)
		}

		blockchainDaemon.Stop()
	}

	return blockchainDaemon, ba, ctx, cancel, cleanup
}

// TestHealth verifies the health check functionality of the block assembly service.
func TestHealth(t *testing.T) {
	_, ba, ctx, cancel, cleanup := setupTest(t)
	defer cancel()
	defer cleanup()

	status, message, err := ba.Health(ctx, true)
	require.NoError(t, err, "Liveness check should not return an error")
	assert.Equal(t, http.StatusOK, status, "Liveness check should return OK status")
	assert.Equal(t, "OK", message, "Liveness check should return 'OK' message")
}

// TestCoinbaseSubsidyHeight verifies correct coinbase subsidy calculation at different heights.
func Test_CoinbaseSubsidyHeight(t *testing.T) {
	_, ba, ctx, cancel, cleanup := setupTest(t)

	defer cancel()

	defer cleanup()

	baClient, err := NewClient(ctx, ulogger.TestLogger{}, ba.settings)
	require.NoError(t, err)

	// Get initial mining candidate and verify coinbase value
	miningCandidate, err := baClient.GetMiningCandidate(ctx)
	require.NoError(t, err, "Failed to get mining candidate")
	t.Logf("Initial coinbase value at height %d: %d", miningCandidate.Height, miningCandidate.CoinbaseValue)

	// Verify coinbase value is set (should be block subsidy for this height)
	assert.Greater(t, miningCandidate.CoinbaseValue, uint64(0), "Coinbase value should be greater than 0")

	coinbase, err := CreateCoinbaseTxCandidate(t, miningCandidate)
	require.NoError(t, err, "Failed to create coinbase tx")

	blockHeaderFromMC, err := model.NewBlockHeaderFromMiningCandidate(miningCandidate, coinbase)
	require.NoError(t, err, "Failed to create block header from mining candidate")

	var nonce uint32

	for ; nonce < math.MaxUint32; nonce++ {
		blockHeaderFromMC.Nonce = nonce

		headerValid, hash, err := blockHeaderFromMC.HasMetTargetDifficulty()
		if err != nil && !strings.Contains(err.Error(), "block header does not meet target") {
			t.Error(err)
			t.FailNow()
		}

		if headerValid {
			t.Logf("Found valid nonce: %d, hash: %s", nonce, hash)
			break
		}
	}

	solution := model.MiningSolution{
		Id:       miningCandidate.Id,
		Nonce:    nonce,
		Time:     &blockHeaderFromMC.Timestamp,
		Version:  &blockHeaderFromMC.Version,
		Coinbase: coinbase.Bytes(),
	}

	err = baClient.SubmitMiningSolution(ctx, &solution)
	require.NoError(t, err, "Failed to submit mining solution")

	// Verify the block was added to the blockchain
	h, m, err := ba.blockchainClient.GetBestBlockHeader(ctx)
	require.NoError(t, err, "Failed to get best block header")
	assert.NotNil(t, h, "Best block header should not be nil")
	assert.NotNil(t, m, "Best block metadata should not be nil")
	t.Logf("Best block header after mining: %v at height %d", h, m.Height)

	// Wait for block assembly to process the new block
	time.Sleep(100 * time.Millisecond)

	// Get a new mining candidate at the new height via the public API
	newMiningCandidate, err := baClient.GetMiningCandidate(ctx)
	require.NoError(t, err, "Failed to get new mining candidate")

	t.Logf("New coinbase value at height %d: %d", newMiningCandidate.Height, newMiningCandidate.CoinbaseValue)

	// Verify coinbase value is still valid at the new height
	assert.Greater(t, newMiningCandidate.CoinbaseValue, uint64(0), "Coinbase value should be greater than 0 at new height")
}

// This testcase tests TNA-3: Teranode must work on finding a difficult proof-of-work for its block
// This testcase tests TNA-6: Teranode must express its acceptance of the block by working on creating the next block in the chain, using the hash of the accepted block as previous hash.
func TestShouldAddSubtreesToLongerChain(t *testing.T) {
	_, ba, ctx, cancel, cleanup := setupTest(t)
	defer cancel()
	defer cleanup()

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

	parents1, _ := subtree.NewTxInpointsFromTx(testTx1)
	parents2, _ := subtree.NewTxInpointsFromTx(testTx2)
	parents3, _ := subtree.NewTxInpointsFromTx(testTx3)

	// Create and add Chain B block (lower difficulty)
	t.Log("Creating Chain B block...")

	chainBBits, _ := model.NewNBitFromString("207fffff")

	chainBHeader1 := &model.BlockHeader{
		Version:        1,
		HashPrevBlock:  initialHeader.Hash(),
		HashMerkleRoot: &chainhash.Hash{},
		Nonce:          2,
		Bits:           *chainBBits,
		Timestamp:      uint32(time.Now().Unix()), //nolint:gosec
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
		Timestamp:      uint32(time.Now().Unix()), //nolint:gosec
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

	t.Log("Adding Chain B block...")

	err = ba.blockchainClient.AddBlock(ctx, blockB, "")
	require.NoError(t, err)

	// Add transactions
	t.Log("Adding transactions...")

	_, err = ba.utxoStore.Create(ctx, testTx1, 0)
	require.NoError(t, err)

	ba.blockAssembler.AddTxBatch([]subtree.Node{{Hash: *testHash1, Fee: 111}}, []*subtree.TxInpoints{&parents1})

	_, err = ba.utxoStore.Create(ctx, testTx2, 0)
	require.NoError(t, err)

	ba.blockAssembler.AddTxBatch([]subtree.Node{{Hash: *testHash2, Fee: 222}}, []*subtree.TxInpoints{&parents2})

	_, err = ba.utxoStore.Create(ctx, testTx3, 0)
	require.NoError(t, err)

	ba.blockAssembler.AddTxBatch([]subtree.Node{{Hash: *testHash3, Fee: 333}}, []*subtree.TxInpoints{&parents3})

	t.Log("Waiting for transactions to be processed...")

	// Wait for transactions to be processed by the subtree processor.
	// Transactions must age for at least DoubleSpendWindow before being dequeued.
	var s []*subtree.Subtree
	require.Eventually(t, func() bool {
		// Use internal method to get subtrees directly (gRPC client doesn't return SubtreeSlices)
		_, subtrees, err := ba.blockAssembler.GetMiningCandidate(context.Background())
		if err != nil {
			return false
		}

		// Count transactions in subtrees
		foundTxs := 0
		for _, st := range subtrees {
			for _, node := range st.Nodes {
				if node.Hash.Equal(*testHash1) || node.Hash.Equal(*testHash2) || node.Hash.Equal(*testHash3) {
					foundTxs++
				}
			}
		}

		if foundTxs == 3 {
			s = subtrees
			return true
		}
		return false
	}, 5*time.Second, 100*time.Millisecond, "Timeout waiting for transactions to be processed")

	// Get mining candidate with timeout context
	t.Log("Getting mining candidate...")

	baClient, err := NewClient(ctx, ulogger.TestLogger{}, ba.settings)
	require.NoError(t, err)

	var miningCandidate *model.MiningCandidate

	// Get mining candidate with subtree hashes included
	miningCandidate, mcErr := baClient.GetMiningCandidate(ctx, true)
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

// TestShouldHandleReorg verifies blockchain reorganization handling.
func TestShouldHandleReorg(t *testing.T) {
	_, ba, ctx, cancel, cleanup := setupTest(t)
	defer cancel()
	defer cleanup()

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

	parents1, err := subtree.NewTxInpointsFromTx(testTx1)
	require.NoError(t, err)
	parents2, err := subtree.NewTxInpointsFromTx(testTx2)
	require.NoError(t, err)
	parents3, err := subtree.NewTxInpointsFromTx(testTx3)
	require.NoError(t, err)

	// Create chain A (original chain) with lower difficulty
	t.Log("Creating Chain A (lower difficulty)...")

	chainABits, _ := model.NewNBitFromString("207fffff") // Lower difficulty
	chainAHeader1 := &model.BlockHeader{
		Version:        1,
		HashPrevBlock:  initialHeader.Hash(),
		HashMerkleRoot: &chainhash.Hash{},
		Nonce:          1,
		Bits:           *chainABits,
		Timestamp:      uint32(time.Now().Unix()), //nolint:gosec
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
		Timestamp:      uint32(time.Now().Unix()), //nolint:gosec
	}

	// Add transactions
	t.Log("Adding transactions...")

	_, err = ba.utxoStore.Create(ctx, testTx1, 0)
	require.NoError(t, err)

	ba.blockAssembler.AddTxBatch([]subtree.Node{{Hash: *testHash1, Fee: 111}}, []*subtree.TxInpoints{&parents1})

	_, err = ba.utxoStore.Create(ctx, testTx2, 0)
	require.NoError(t, err)
	ba.blockAssembler.AddTxBatch([]subtree.Node{{Hash: *testHash2, Fee: 222}}, []*subtree.TxInpoints{&parents2})

	_, err = ba.utxoStore.Create(ctx, testTx3, 0)
	require.NoError(t, err)
	ba.blockAssembler.AddTxBatch([]subtree.Node{{Hash: *testHash3, Fee: 333}}, []*subtree.TxInpoints{&parents3})

	// Add Chain A block (lower difficulty)
	t.Log("Adding Chain A block...")

	coinbaseTx, _ := bt.NewTxFromString("01000000010000000000000000000000000000000000000000000000000000000000000000ffffffff17030200002f6d312d65752f605f77009f74384816a31807ffffffff03ac505763000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88acaa505763000000001976a9143c22b6d9ba7b50b6d6e615c69d11ecb2ba3db14588acaa505763000000001976a914b7177c7deb43f3869eabc25cfd9f618215f34d5588ac00000000")

	blockA := &model.Block{
		Header:           chainAHeader1,
		CoinbaseTx:       coinbaseTx,
		TransactionCount: 1,
		Subtrees:         []*chainhash.Hash{},
	}
	err = ba.blockchainClient.AddBlock(ctx, blockA, "", options.WithMinedSet(true))
	require.NoError(t, err)

	// Wait for the block to be processed
	err = waitForBestBlockHash(ctx, ba.blockchainClient, chainAHeader1.Hash(), 10*time.Second)
	require.NoError(t, err, "Timeout waiting for Chain A block to be processed")

	// Wait for the block assembly service to process the block AND for transactions to be available
	var mc1 *model.MiningCandidate
	var st1 []*subtree.Subtree
	require.Eventually(t, func() bool {
		var err error
		mc1, st1, err = ba.blockAssembler.GetMiningCandidate(context.Background())
		if err != nil || mc1 == nil {
			return false
		}
		prevHash := chainhash.Hash(mc1.PreviousHash)
		// Wait for both correct previous hash AND subtrees to be available
		return prevHash.String() == chainAHeader1.Hash().String() && len(st1) > 0
	}, 5*time.Second, 100*time.Millisecond, "Timeout waiting for block assembly to process the block")

	// Verify transactions in original chain
	require.NotNil(t, mc1)
	require.NotEmpty(t, st1)

	// check the previous hash of the mining candidate
	prevHash := chainhash.Hash(mc1.PreviousHash)
	t.Logf("Mining candidate built on block with previous hash: %s", prevHash.String())
	assert.Equal(t, chainAHeader1.Hash().String(), prevHash.String(), "Mining candidate should be built on Chain A")

	// Now trigger reorg by adding Chain B block with higher difficulty
	t.Log("Triggering reorg with Chain B block (higher difficulty)...")

	blockB := &model.Block{
		Header:           chainBHeader1,
		CoinbaseTx:       coinbaseTx,
		TransactionCount: 1,
		Subtrees:         []*chainhash.Hash{},
	}

	err = ba.blockchainClient.AddBlock(ctx, blockB, "", options.WithMinedSet(true))
	require.NoError(t, err)

	// Wait for the reorganization to complete
	err = waitForBestBlockHash(ctx, ba.blockchainClient, chainBHeader1.Hash(), 10*time.Second)
	require.NoError(t, err, "Timeout waiting for reorganization to Chain B")

	// Additional wait to ensure block assembly has processed the reorg
	time.Sleep(500 * time.Millisecond)

	// Verify transactions are still present after reorg
	mc2, st2, err := ba.blockAssembler.GetMiningCandidate(context.Background())
	require.NoError(t, err)
	require.NotNil(t, mc2)
	require.NotEmpty(t, st2)

	// check the previous hash of the mining candidate
	prevHash = chainhash.Hash(mc2.PreviousHash)
	t.Logf("Mining candidate built on block with previous hash: %s", prevHash)
	assert.Equal(t, chainBHeader1.Hash().String(), prevHash.String(), "Mining candidate should be built on Chain B")

	// Verify transaction count is maintained
	var foundTxsAfterReorg int

	for _, subtree := range st2 {
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

// waitForBestBlockHash waits for the best block to match the expected hash
func waitForBestBlockHash(ctx context.Context, blockchainClient blockchain.ClientI, expectedHash *chainhash.Hash, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)

	for time.Now().Before(deadline) {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			bestHeader, _, err := blockchainClient.GetBestBlockHeader(ctx)
			if err != nil {
				return err
			}

			if bestHeader.Hash().IsEqual(expectedHash) {
				return nil
			}

			time.Sleep(100 * time.Millisecond)
		}
	}

	return errors.NewProcessingError("timeout waiting for best block hash %s", expectedHash)
}

// TestShouldHandleReorgWithLongerChain verifies reorganization with extended chains.
func TestShouldHandleReorgWithLongerChain(t *testing.T) {
	_, ba, ctx, cancel, cleanup := setupTest(t)
	defer cancel()
	defer cleanup()

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

	parents1, _ := subtree.NewTxInpointsFromTx(testTx1)
	parents2, _ := subtree.NewTxInpointsFromTx(testTx2)
	parents3, _ := subtree.NewTxInpointsFromTx(testTx3)

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
		Timestamp:      uint32(time.Now().Unix()), //nolint:gosec
	}

	chainAHeader2 := &model.BlockHeader{
		Version:        1,
		HashPrevBlock:  chainAHeader1.Hash(),
		HashMerkleRoot: &chainhash.Hash{},
		Nonce:          2,
		Bits:           *chainABits,
		Timestamp:      uint32(time.Now().Unix()), //nolint:gosec
	}

	chainAHeader3 := &model.BlockHeader{
		Version:        1,
		HashPrevBlock:  chainAHeader2.Hash(),
		HashMerkleRoot: &chainhash.Hash{},
		Nonce:          3,
		Bits:           *chainABits,
		Timestamp:      uint32(time.Now().Unix()), //nolint:gosec
	}

	chainAHeader4 := &model.BlockHeader{
		Version:        1,
		HashPrevBlock:  chainAHeader3.Hash(),
		HashMerkleRoot: &chainhash.Hash{},
		Nonce:          4,
		Bits:           *chainABits,
		Timestamp:      uint32(time.Now().Unix()), //nolint:gosec
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
		Timestamp:      uint32(time.Now().Unix()), //nolint:gosec
	}

	// Add transactions
	t.Log("Adding transactions...")

	_, err = ba.utxoStore.Create(ctx, testTx1, 0)
	require.NoError(t, err)

	ba.blockAssembler.AddTxBatch([]subtree.Node{{Hash: *testHash1, Fee: 111}}, []*subtree.TxInpoints{&parents1})

	_, err = ba.utxoStore.Create(ctx, testTx2, 0)
	require.NoError(t, err)
	ba.blockAssembler.AddTxBatch([]subtree.Node{{Hash: *testHash2, Fee: 222}}, []*subtree.TxInpoints{&parents2})

	_, err = ba.utxoStore.Create(ctx, testTx3, 0)
	require.NoError(t, err)
	ba.blockAssembler.AddTxBatch([]subtree.Node{{Hash: *testHash3, Fee: 333}}, []*subtree.TxInpoints{&parents3})

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
	err = ba.blockchainClient.AddBlock(ctx, blockA1, "", options.WithMinedSet(true))
	require.NoError(t, err)

	blockA2 := &model.Block{
		Header:           chainAHeader2,
		CoinbaseTx:       coinbaseTx,
		TransactionCount: 1,
		Subtrees:         []*chainhash.Hash{},
	}
	err = ba.blockchainClient.AddBlock(ctx, blockA2, "", options.WithMinedSet(true))
	require.NoError(t, err)

	blockA3 := &model.Block{
		Header:           chainAHeader3,
		CoinbaseTx:       coinbaseTx,
		TransactionCount: 1,
		Subtrees:         []*chainhash.Hash{},
	}
	err = ba.blockchainClient.AddBlock(ctx, blockA3, "", options.WithMinedSet(true))
	require.NoError(t, err)

	blockA4 := &model.Block{
		Header:           chainAHeader4,
		CoinbaseTx:       coinbaseTx,
		TransactionCount: 1,
		Subtrees:         []*chainhash.Hash{},
	}
	err = ba.blockchainClient.AddBlock(ctx, blockA4, "", options.WithMinedSet(true))
	require.NoError(t, err)

	// Wait for the block to be processed
	err = waitForBestBlockHash(ctx, ba.blockchainClient, chainAHeader4.Hash(), 10*time.Second)
	require.NoError(t, err, "Timeout waiting for Chain A block 4 to be processed")

	// Additional wait to ensure block assembly has processed the block
	time.Sleep(500 * time.Millisecond)

	// Get mining candidate while on Chain A
	t.Log("Getting mining candidate on Chain A...")

	mc1, subtrees1, err := ba.blockAssembler.GetMiningCandidate(context.Background())
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
	err = ba.blockchainClient.AddBlock(ctx, blockB, "", options.WithMinedSet(true))
	require.NoError(t, err)

	// Wait for the reorganization to complete by checking for the expected best block
	err = waitForBestBlockHash(ctx, ba.blockchainClient, chainBHeader1.Hash(), 10*time.Second)
	require.NoError(t, err, "Timeout waiting for reorganization to complete")

	// Additional wait to ensure block assembly has processed the reorg
	time.Sleep(500 * time.Millisecond)

	// Verify transactions are still present after reorg
	mc2, subtrees2, err := ba.blockAssembler.GetMiningCandidate(context.Background())
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

// TestShouldFailCoinbaseArbitraryTextTooLong verifies max coinbase size policy.
func TestShouldFailCoinbaseArbitraryTextTooLong(t *testing.T) {
	t.Skip("Skipping coinbase arbitrary text too long test")
	_, ba, ctx, cancel, cleanup := setupTest(t)

	defer cancel()

	defer cleanup()

	tSettings := ba.settings
	tSettings.Coinbase.ArbitraryText = "too long"
	tSettings.ChainCfgParams.MaxCoinbaseScriptSigSize = 5

	_, err := ba.GenerateBlocks(ctx, &blockassembly_api.GenerateBlocksRequest{Count: 1})
	require.Error(t, err, "Should return error for bad coinbase length")
	require.Contains(t, err.Error(), "bad coinbase length")
}

func CreateCoinbaseTxCandidate(t *testing.T, m *model.MiningCandidate) (*bt.Tx, error) {
	tSettings := test.CreateBaseTestSettings(t)

	arbitraryText := tSettings.Coinbase.ArbitraryText

	coinbasePrivKeys := tSettings.BlockAssembly.MinerWalletPrivateKeys
	if len(coinbasePrivKeys) == 0 {
		return nil, errors.NewConfigurationError("miner_wallet_private_keys not found in config")
	}

	walletAddresses := make([]string, len(coinbasePrivKeys))

	for i, coinbasePrivKey := range coinbasePrivKeys {
		privateKey, err := primitives.PrivateKeyFromWif(coinbasePrivKey)
		if err != nil {
			return nil, errors.NewProcessingError("can't decode coinbase priv key", err)
		}

		walletAddress, err := bscript.NewAddressFromPublicKey(privateKey.PubKey(), true)
		if err != nil {
			return nil, errors.NewProcessingError("can't create coinbase address", err)
		}

		walletAddresses[i] = walletAddress.AddressString
	}

	a, b, err := model.GetCoinbaseParts(m.Height, m.CoinbaseValue, arbitraryText, walletAddresses)
	if err != nil {
		return nil, errors.NewProcessingError("error creating coinbase transaction", err)
	}

	extranonce := make([]byte, 12)
	_, _ = rand.Read(extranonce)
	a = append(a, extranonce...)
	a = append(a, b...)

	coinbaseTx, err := bt.NewTxFromBytes(a)
	if err != nil {
		return nil, errors.NewProcessingError("error decoding coinbase transaction", err)
	}

	return coinbaseTx, nil
}
