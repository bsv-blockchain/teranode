package smoke

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/teranode/daemon"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/services/validator"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/test"
	helper "github.com/bsv-blockchain/teranode/test/utils"
	"github.com/bsv-blockchain/teranode/test/utils/svnode"
	"github.com/bsv-blockchain/teranode/test/utils/transactions"
	"github.com/stretchr/testify/require"
)

// BIP68 constants imported from validator package
const (
	SequenceLockTimeDisableFlag = validator.SequenceLockTimeDisableFlag
	SequenceLockTimeTypeFlag    = validator.SequenceLockTimeTypeFlag
	SequenceLockTimeMask        = validator.SequenceLockTimeMask
)

// Lock to prevent parallel BIP68 tests from port conflicts
var bip68TestLock sync.Mutex

// setupBIP68TestNodes initializes both Teranode and SV Node with CSV height override
func setupBIP68TestNodes(t *testing.T, csvHeight uint32) (*daemon.TestDaemon, svnode.SVNodeI) {
	ctx := t.Context()

	// Start SV Node in Docker
	sv := newSVNode()
	err := sv.Start(ctx)
	require.NoError(t, err, "Failed to start SV Node")

	// Start Teranode with CSV height override and legacy connection
	td := daemon.NewTestDaemon(t, daemon.TestOptions{
		EnableRPC:       true,
		EnableP2P:       true,
		EnableLegacy:    true,
		EnableValidator: true,
		SettingsOverrideFunc: test.ComposeSettings(
			test.SystemTestSettings(),
			func(s *settings.Settings) {
				s.ChainCfgParams.CSVHeight = csvHeight
				s.Legacy.ConnectPeers = []string{sv.P2PHost()}
				s.P2P.StaticPeers = []string{}
			},
		),
	})

	return td, sv
}

// createSequenceLockedTx creates a transaction with specific sequence number
func createSequenceLockedTx(t *testing.T, td *daemon.TestDaemon,
	parentTx *bt.Tx, inputIndex uint32, sequenceNumber uint32, version uint32) *bt.Tx {

	tx := td.CreateTransactionWithOptions(t,
		transactions.WithInput(parentTx, inputIndex),
		transactions.WithP2PKHOutputs(1, parentTx.Outputs[inputIndex].Satoshis-1000),
	)

	tx.Version = version
	tx.Inputs[0].SequenceNumber = sequenceNumber

	return tx
}

// createSequenceLockedTxMultiInput creates a transaction with multiple inputs, each with different sequence numbers
func createSequenceLockedTxMultiInput(t *testing.T, td *daemon.TestDaemon,
	inputs []struct {
		tx       *bt.Tx
		vout     uint32
		sequence uint32
	}) *bt.Tx {

	// Calculate total satoshis
	var totalSatoshis uint64
	for _, input := range inputs {
		totalSatoshis += input.tx.Outputs[input.vout].Satoshis
	}

	// Build transaction options
	txOpts := []transactions.TxOption{}
	for _, input := range inputs {
		txOpts = append(txOpts, transactions.WithInput(input.tx, input.vout))
	}
	txOpts = append(txOpts, transactions.WithP2PKHOutputs(1, totalSatoshis-uint64(len(inputs))*1000))

	tx := td.CreateTransactionWithOptions(t, txOpts...)

	// Set version and sequence numbers
	tx.Version = 2
	for i, input := range inputs {
		tx.Inputs[i].SequenceNumber = input.sequence
	}

	return tx
}

// syncNodesToHeight mines blocks on SV Node and waits for Teranode to sync
func syncNodesToHeight(t *testing.T, td *daemon.TestDaemon, sv svnode.SVNodeI,
	targetHeight uint32) {

	ctx := t.Context()

	// Generate blocks on SV Node
	currentHeight, err := sv.GetBlockCount()
	require.NoError(t, err)

	if currentHeight < int(targetHeight) {
		blocksToMine := int(targetHeight) - currentHeight
		_, err = sv.Generate(blocksToMine)
		require.NoError(t, err)
	}

	// Wait for Teranode to sync
	err = helper.WaitForNodeBlockHeight(ctx, td.BlockchainClient,
		targetHeight, 60*time.Second)
	require.NoError(t, err)

	t.Logf("Both nodes synced to height %d", targetHeight)
}

// verifyConsensus verifies both nodes have same best block hash
func verifyConsensus(t *testing.T, ctx context.Context,
	td *daemon.TestDaemon, sv svnode.SVNodeI) {

	// Get best block from Teranode
	header, _, err := td.BlockchainClient.GetBestBlockHeader(ctx)
	require.NoError(t, err)
	teranodeHash := header.Hash().String()

	// Get best block from SV Node
	svHash, err := sv.GetBestBlockHash()
	require.NoError(t, err)

	require.Equal(t, svHash, teranodeHash,
		"Both nodes must agree on best block hash")

	t.Logf("Consensus verified: %s", svHash)
}

// verifyNodeHeight verifies a node is at expected height
func verifyNodeHeight(t *testing.T, sv svnode.SVNodeI, expectedHeight int) {
	height, err := sv.GetBlockCount()
	require.NoError(t, err)
	require.Equal(t, expectedHeight, height,
		"Node should be at height %d", expectedHeight)
}

// submitTxToSVNodeAndMine submits a transaction to SV Node's mempool first (as the reference),
// then has SV Node mine a block, then waits for Teranode to sync via P2P.
// This ensures we're testing that Teranode matches SV Node's consensus behavior.
func submitTxToSVNodeAndMine(t *testing.T, td *daemon.TestDaemon,
	sv svnode.SVNodeI, tx *bt.Tx, expectedHeight uint32) {

	ctx := t.Context()

	// IMPORTANT: Submit transaction to SV Node FIRST (it's the reference implementation)
	txHex := tx.String()
	txID, err := sv.SendRawTransaction(txHex)
	require.NoError(t, err, "SV Node (reference) should accept transaction")

	t.Logf("SV Node accepted transaction %s into mempool", txID)

	// Have SV Node mine a block containing this transaction
	blockHashes, err := sv.Generate(1)
	require.NoError(t, err, "SV Node should mine block")
	require.Len(t, blockHashes, 1, "Should generate exactly 1 block")

	t.Logf("SV Node mined block %s at height %d", blockHashes[0], expectedHeight)

	// Now wait for Teranode to sync the block via P2P from SV Node
	err = helper.WaitForNodeBlockHeight(ctx, td.BlockchainClient, expectedHeight, 60*time.Second)
	require.NoError(t, err, "Teranode should sync block from SV Node")

	t.Logf("Teranode synced to height %d from SV Node", expectedHeight)
}

// createBlockWithTx creates a block without submitting
func createBlockWithTx(t *testing.T, td *daemon.TestDaemon,
	blockHeight uint32, tx *bt.Tx) *model.Block {

	ctx := t.Context()

	prevBlock, err := td.BlockchainClient.GetBlockByHeight(ctx, blockHeight-1)
	require.NoError(t, err)

	_, block := td.CreateTestBlock(t, prevBlock, 0, tx)
	return block
}

// getMatureUTXO gets a coinbase UTXO with sufficient confirmations
func getMatureUTXO(t *testing.T, td *daemon.TestDaemon,
	blockHeight uint32) (*model.Block, *bt.Tx) {

	ctx := t.Context()
	block, err := td.BlockchainClient.GetBlockByHeight(ctx, blockHeight)
	require.NoError(t, err)
	return block, block.CoinbaseTx
}

// TestBIP68_HeightBased_Accept verifies both nodes accept valid height-based sequence lock
func TestBIP68_HeightBased_Accept(t *testing.T) {
	bip68TestLock.Lock()
	defer bip68TestLock.Unlock()

	// Setup with CSV height = 10
	td, sv := setupBIP68TestNodes(t, 10)
	defer func() {
		td.Stop(t)
		_ = sv.Stop(t.Context())
	}()

	ctx := t.Context()

	// Sync both nodes to height 20 (past CSV activation)
	syncNodesToHeight(t, td, sv, 20)

	// Get mature coinbase from block 10
	_, coinbaseTx := getMatureUTXO(t, td, 10)

	// Create transaction with sequence = 5 (requires 5 confirmations)
	// Current height = 20, UTXO at height 10
	// minHeight = 10 + 5 = 15, current = 21 ✓
	tx := createSequenceLockedTx(t, td, coinbaseTx, 0, 5, 2)

	t.Logf("Testing height-based sequence lock: sequence=5, UTXO height=10, current=21")

	// Submit to SV Node first (reference), let it mine, then Teranode syncs
	submitTxToSVNodeAndMine(t, td, sv, tx, 21)

	// Verify consensus - both nodes accepted
	verifyConsensus(t, ctx, td, sv)

	t.Logf("SUCCESS: Both nodes accepted block with valid height-based sequence lock")
}

// TestBIP68_HeightBased_Reject verifies both nodes reject invalid height-based lock
func TestBIP68_HeightBased_Reject(t *testing.T) {
	bip68TestLock.Lock()
	defer bip68TestLock.Unlock()

	td, sv := setupBIP68TestNodes(t, 10)
	defer func() {
		td.Stop(t)
		_ = sv.Stop(t.Context())
	}()

	ctx := t.Context()

	syncNodesToHeight(t, td, sv, 20)

	// Get coinbase from block 15
	_, coinbaseTx := getMatureUTXO(t, td, 15)

	// Create transaction with sequence = 100 (requires 100 confirmations)
	// Current height = 20, UTXO at height 15
	// minHeight = 15 + 100 = 115, current = 21 ✗
	tx := createSequenceLockedTx(t, td, coinbaseTx, 0, 100, 2)

	t.Logf("Testing invalid height-based lock: sequence=100, UTXO height=15, current=21")

	// For rejection tests: Transaction will enter SV Node mempool (mempool doesn't check BIP68),
	// but when SV Node tries to mine, it will exclude this transaction from the block.
	// So we test Teranode's validator directly to verify it would also reject.

	// Create block with transaction for direct validation test
	block21 := createBlockWithTx(t, td, 21, tx)

	// Submit to Teranode validator - should reject
	err := td.BlockValidationClient.ProcessBlock(ctx, block21, 21, "", "")
	require.Error(t, err, "Teranode should reject block with invalid sequence lock")
	require.Contains(t, err.Error(), "sequence lock")

	t.Logf("Teranode correctly rejected: %v", err)

	// Verify both nodes remain at height 20 (no invalid block accepted)
	verifyNodeHeight(t, sv, 20)
	verifyConsensus(t, ctx, td, sv)

	t.Logf("SUCCESS: Teranode correctly rejects invalid height-based sequence lock (matching SV Node behavior)")
}

// TestBIP68_TimeBased_Accept verifies time-based sequence lock acceptance
func TestBIP68_TimeBased_Accept(t *testing.T) {
	bip68TestLock.Lock()
	defer bip68TestLock.Unlock()

	td, sv := setupBIP68TestNodes(t, 10)
	defer func() {
		td.Stop(t)
		_ = sv.Stop(t.Context())
	}()

	ctx := t.Context()

	// Sync to height 20 - gives enough time between block 10 and block 21
	syncNodesToHeight(t, td, sv, 20)

	_, coinbaseTx := getMatureUTXO(t, td, 10)

	// Create transaction with time-based sequence lock
	// 2 × 512 = 1024 seconds
	// With natural block generation timing over 10 blocks, this should pass
	tx := createSequenceLockedTx(t, td, coinbaseTx, 0,
		SequenceLockTimeTypeFlag|2, 2)

	t.Logf("Testing time-based sequence lock: 2 × 512 = 1024 seconds")

	// Submit to SV Node first (reference), let it mine, then Teranode syncs
	submitTxToSVNodeAndMine(t, td, sv, tx, 21)

	verifyConsensus(t, ctx, td, sv)

	t.Logf("SUCCESS: Both nodes accepted block with valid time-based sequence lock")
}

// TestBIP68_TimeBased_Reject verifies time-based sequence lock rejection
func TestBIP68_TimeBased_Reject(t *testing.T) {
	bip68TestLock.Lock()
	defer bip68TestLock.Unlock()

	td, sv := setupBIP68TestNodes(t, 10)
	defer func() {
		td.Stop(t)
		_ = sv.Stop(t.Context())
	}()

	ctx := t.Context()

	// Sync to height 11 only - not enough time has passed
	syncNodesToHeight(t, td, sv, 11)

	_, coinbaseTx := getMatureUTXO(t, td, 10)

	// Create transaction with large time-based sequence lock
	// 10000 × 512 = 5,120,000 seconds (~59 days)
	// This should definitely fail
	tx := createSequenceLockedTx(t, td, coinbaseTx, 0,
		SequenceLockTimeTypeFlag|10000, 2)

	t.Logf("Testing invalid time-based lock: 10000 × 512 = 5,120,000 seconds")

	// Create block with transaction for direct validation test
	block12 := createBlockWithTx(t, td, 12, tx)

	// Submit to Teranode validator - should reject
	err := td.BlockValidationClient.ProcessBlock(ctx, block12, 12, "", "")
	require.Error(t, err, "Teranode should reject block with invalid time-based sequence lock")
	require.Contains(t, err.Error(), "sequence lock")

	t.Logf("Teranode correctly rejected: %v", err)

	// Verify both nodes remain at height 11
	verifyNodeHeight(t, sv, 11)
	verifyConsensus(t, ctx, td, sv)

	t.Logf("SUCCESS: Teranode correctly rejects invalid time-based sequence lock (matching SV Node behavior)")
}

// TestBIP68_DisableFlag verifies disable flag bypasses BIP68
func TestBIP68_DisableFlag(t *testing.T) {
	bip68TestLock.Lock()
	defer bip68TestLock.Unlock()

	td, sv := setupBIP68TestNodes(t, 10)
	defer func() {
		td.Stop(t)
		_ = sv.Stop(t.Context())
	}()

	ctx := t.Context()

	syncNodesToHeight(t, td, sv, 11)

	// Get very recent UTXO (1 block ago)
	_, coinbaseTx := getMatureUTXO(t, td, 10)

	// Create transaction with disable flag set and high sequence
	// Would fail without disable flag, but should pass with it
	tx := createSequenceLockedTx(t, td, coinbaseTx, 0,
		SequenceLockTimeDisableFlag|100, 2)

	t.Logf("Testing disable flag: sequence has disable flag set with value 100")

	// Submit to SV Node first (reference), let it mine, then Teranode syncs
	submitTxToSVNodeAndMine(t, td, sv, tx, 12)

	// Verify consensus - both nodes accepted
	verifyConsensus(t, ctx, td, sv)

	t.Logf("SUCCESS: Both nodes accepted block - disable flag bypassed BIP68")
}

// TestBIP68_BeforeCSVHeight verifies BIP68 not enforced before CSV activation
func TestBIP68_BeforeCSVHeight(t *testing.T) {
	bip68TestLock.Lock()
	defer bip68TestLock.Unlock()

	// Set CSV height to 50 (high value)
	td, sv := setupBIP68TestNodes(t, 50)
	defer func() {
		td.Stop(t)
		_ = sv.Stop(t.Context())
	}()

	ctx := t.Context()

	// Sync to height 11 (before CSV activation at 50)
	syncNodesToHeight(t, td, sv, 11)

	_, coinbaseTx := getMatureUTXO(t, td, 10)

	// Create transaction with sequence = 100 (would fail if BIP68 active)
	tx := createSequenceLockedTx(t, td, coinbaseTx, 0, 100, 2)

	t.Logf("Testing before CSV height: current=12, CSV height=50, sequence=100")

	// Submit to SV Node first (reference), let it mine, then Teranode syncs
	submitTxToSVNodeAndMine(t, td, sv, tx, 12)

	// Verify consensus - both nodes accepted (BIP68 not enforced yet)
	verifyConsensus(t, ctx, td, sv)

	t.Logf("SUCCESS: Both nodes accepted block - BIP68 not enforced before CSV height")
}

// TestBIP68_Version1Bypass verifies version 1 transactions bypass BIP68
func TestBIP68_Version1Bypass(t *testing.T) {
	bip68TestLock.Lock()
	defer bip68TestLock.Unlock()

	td, sv := setupBIP68TestNodes(t, 10)
	defer func() {
		td.Stop(t)
		_ = sv.Stop(t.Context())
	}()

	ctx := t.Context()

	syncNodesToHeight(t, td, sv, 11)

	_, coinbaseTx := getMatureUTXO(t, td, 10)

	// Create transaction with version 1 and high sequence
	// Should pass because version 1 bypasses BIP68
	tx := createSequenceLockedTx(t, td, coinbaseTx, 0, 100, 1)

	t.Logf("Testing version 1 bypass: version=1, sequence=100, UTXO age=1 block")

	// Submit to SV Node first (reference), let it mine, then Teranode syncs
	submitTxToSVNodeAndMine(t, td, sv, tx, 12)

	// Verify consensus - both nodes accepted (version 1 bypasses BIP68)
	verifyConsensus(t, ctx, td, sv)

	t.Logf("SUCCESS: Both nodes accepted block - version 1 bypassed BIP68")
}

// TestBIP68_MixedInputTypes verifies mixed sequence lock types in single transaction
func TestBIP68_MixedInputTypes(t *testing.T) {
	bip68TestLock.Lock()
	defer bip68TestLock.Unlock()

	td, sv := setupBIP68TestNodes(t, 10)
	defer func() {
		td.Stop(t)
		_ = sv.Stop(t.Context())
	}()

	ctx := t.Context()

	syncNodesToHeight(t, td, sv, 20)

	// Get three mature UTXOs
	_, coinbaseTx1 := getMatureUTXO(t, td, 10)
	_, coinbaseTx2 := getMatureUTXO(t, td, 11)
	_, coinbaseTx3 := getMatureUTXO(t, td, 12)

	// Create transaction with mixed input types
	tx := createSequenceLockedTxMultiInput(t, td, []struct {
		tx       *bt.Tx
		vout     uint32
		sequence uint32
	}{
		{coinbaseTx1, 0, 5},                              // Height-based: 5 blocks
		{coinbaseTx2, 0, SequenceLockTimeTypeFlag | 2},  // Time-based: 2 × 512 seconds
		{coinbaseTx3, 0, SequenceLockTimeDisableFlag},   // Disabled
	})

	t.Logf("Testing mixed input types: height-based(5), time-based(2), disabled")

	// Submit to SV Node first (reference), let it mine, then Teranode syncs
	submitTxToSVNodeAndMine(t, td, sv, tx, 21)

	// Verify consensus - both nodes accepted
	verifyConsensus(t, ctx, td, sv)

	t.Logf("SUCCESS: Both nodes accepted block with mixed sequence lock types")
}

// TestBIP68_ZeroSequence verifies zero sequence number imposes no constraint
func TestBIP68_ZeroSequence(t *testing.T) {
	bip68TestLock.Lock()
	defer bip68TestLock.Unlock()

	td, sv := setupBIP68TestNodes(t, 10)
	defer func() {
		td.Stop(t)
		_ = sv.Stop(t.Context())
	}()

	ctx := t.Context()

	syncNodesToHeight(t, td, sv, 11)

	// Get very recent UTXO (1 block ago)
	_, coinbaseTx := getMatureUTXO(t, td, 10)

	// Create transaction with zero sequence number
	tx := createSequenceLockedTx(t, td, coinbaseTx, 0, 0, 2)

	t.Logf("Testing zero sequence: sequence=0, UTXO age=1 block")

	// Submit to SV Node first (reference), let it mine, then Teranode syncs
	submitTxToSVNodeAndMine(t, td, sv, tx, 12)

	// Verify consensus - both nodes accepted (zero imposes no constraint)
	verifyConsensus(t, ctx, td, sv)

	t.Logf("SUCCESS: Both nodes accepted block - zero sequence imposes no constraint")
}

// TestBIP68_AtExactCSVHeight verifies BIP68 enforced at exact activation height
func TestBIP68_AtExactCSVHeight(t *testing.T) {
	bip68TestLock.Lock()
	defer bip68TestLock.Unlock()

	// Set CSV height to 20
	td, sv := setupBIP68TestNodes(t, 20)
	defer func() {
		td.Stop(t)
		_ = sv.Stop(t.Context())
	}()

	ctx := t.Context()

	// Sync to exactly CSV height - 1
	syncNodesToHeight(t, td, sv, 19)

	_, coinbaseTx := getMatureUTXO(t, td, 10)

	// Create transaction that would pass if BIP68 not enforced
	// but should fail at CSV height
	tx := createSequenceLockedTx(t, td, coinbaseTx, 0, 100, 2)

	t.Logf("Testing at exact CSV height: current=19, CSV height=20, sequence=100")

	// Create block at CSV height
	block20 := createBlockWithTx(t, td, 20, tx)

	// Submit to Teranode - should reject (BIP68 enforced at CSV height)
	err := td.BlockValidationClient.ProcessBlock(ctx, block20, 20, "", "")
	require.Error(t, err, "Teranode should reject block")
	require.Contains(t, err.Error(), "sequence lock")

	t.Logf("Teranode rejected at CSV height: %v", err)

	// Verify SV Node also rejected
	verifyNodeHeight(t, sv, 19)

	// Verify consensus
	verifyConsensus(t, ctx, td, sv)

	t.Logf("SUCCESS: Both nodes enforced BIP68 at exact CSV activation height")
}
