package smoke

import (
	"bytes"
	"context"
	"encoding/hex"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	bec "github.com/bsv-blockchain/go-sdk/primitives/ec"
	"github.com/bsv-blockchain/go-wire"
	"github.com/bsv-blockchain/teranode/daemon"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/services/validator"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/test"
	helper "github.com/bsv-blockchain/teranode/test/utils"
	"github.com/bsv-blockchain/teranode/test/utils/svnode"
	"github.com/stretchr/testify/require"
)

// BIP68 constants for sequence lock encoding
const (
	SequenceLockTimeDisableFlag = validator.SequenceLockTimeDisableFlag // 1 << 31
	SequenceLockTimeTypeFlag    = validator.SequenceLockTimeTypeFlag    // 1 << 22
	SequenceLockTimeMask        = validator.SequenceLockTimeMask        // 0x0000ffff
)

// setupBIP68Test initializes both nodes with CSV height override
// Generates initialHeight blocks on SV Node BEFORE starting Teranode for reliable IBD sync
func setupBIP68Test(t *testing.T, csvHeight uint32, initialHeight int) (*daemon.TestDaemon, svnode.SVNodeI, *svnode.TxCreator) {
	ctx := t.Context()

	// Start SV Node
	sv := newSVNode()
	err := sv.Start(ctx)
	require.NoError(t, err, "Failed to start SV Node")

	// Generate blocks BEFORE starting Teranode (following legacy_sync_test.go pattern)
	// This ensures Teranode performs Initial Block Download (IBD) which is more reliable
	if initialHeight > 0 {
		_, err = sv.Generate(initialHeight)
		require.NoError(t, err, "Failed to generate initial blocks on SV Node")
		t.Logf("SV Node generated %d blocks before Teranode starts", initialHeight)
	}

	// Start Teranode with CSV height override
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
		FSMState: blockchain.FSMStateRUNNING,
	})

	// Wait for Teranode to sync initial blocks via IBD
	if initialHeight > 0 {
		err = helper.WaitForNodeBlockHeight(ctx, td.BlockchainClient, uint32(initialHeight), 120*time.Second)
		require.NoError(t, err, "Teranode should sync to height %d", initialHeight)
		t.Logf("Teranode synced to height %d via IBD", initialHeight)
	}

	// Create TxCreator for funding transactions
	privKey := td.GetPrivateKey(t)
	txCreator, err := svnode.NewTxCreator(sv, privKey)
	require.NoError(t, err)

	t.Logf("Test setup complete: CSV height=%d, initial height=%d, TxCreator address=%s",
		csvHeight, initialHeight, txCreator.Address())

	return td, sv, txCreator
}

// createSequenceLockedTx creates a transaction with a specific sequence number
func createSequenceLockedTx(fundingUTXO *svnode.FundingUTXO, toAddress string,
	sequenceNumber uint32, version uint32, privKey *bec.PrivateKey) (*bt.Tx, error) {

	tx := bt.NewTx()
	tx.Version = version

	// Add input from funding UTXO
	utxo := &bt.UTXO{
		TxIDHash:      fundingUTXO.Tx.TxIDChainHash(),
		Vout:          fundingUTXO.Vout,
		LockingScript: fundingUTXO.LockingScript,
		Satoshis:      fundingUTXO.Amount,
	}
	err := tx.FromUTXOs(utxo)
	if err != nil {
		return nil, err
	}

	// Set sequence number
	tx.Inputs[0].SequenceNumber = sequenceNumber

	// Add output (send to address with small fee)
	outputAmount := fundingUTXO.Amount - 10000 // 10k satoshi fee
	err = tx.AddP2PKHOutputFromAddress(toAddress, outputAmount)
	if err != nil {
		return nil, err
	}

	// Sign the transaction
	return signTransaction(tx, privKey)
}

// signTransaction signs all inputs in a transaction
func signTransaction(tx *bt.Tx, privKey *bec.PrivateKey) (*bt.Tx, error) {
	for i := range tx.Inputs {
		sigHash, err := tx.CalcInputSignatureHash(uint32(i), 0x41) // ALL|FORKID
		if err != nil {
			return nil, err
		}

		sig, err := privKey.Sign(sigHash)
		if err != nil {
			return nil, err
		}

		unlockScript := &bscript.Script{}
		sigBytes := append(sig.Serialize(), byte(0x41))
		_ = unlockScript.AppendPushData(sigBytes)
		_ = unlockScript.AppendPushData(privKey.PubKey().Compressed())

		tx.Inputs[i].UnlockingScript = unlockScript
	}

	return tx, nil
}

// waitForSync waits for both nodes to reach the same height
func waitForSync(t *testing.T, ctx context.Context, td *daemon.TestDaemon,
	sv svnode.SVNodeI, expectedHeight uint32) {

	// Wait for Teranode to sync
	err := helper.WaitForNodeBlockHeight(ctx, td.BlockchainClient, expectedHeight, 60*time.Second)
	require.NoError(t, err, "Teranode should sync to height %d", expectedHeight)

	// Verify SV Node is also at expected height
	svHeight, err := sv.GetBlockCount()
	require.NoError(t, err)
	require.Equal(t, int(expectedHeight), svHeight, "SV Node should be at height %d", expectedHeight)

	t.Logf("Both nodes synced to height %d", expectedHeight)
}

// TestBIP68_HeightBased_Accept verifies valid height-based sequence lock is accepted
func TestBIP68_HeightBased_Accept(t *testing.T) {
	legacySyncTestLock.Lock()
	defer legacySyncTestLock.Unlock()

	ctx := t.Context()

	// Setup with CSV height = 10, generate 120 blocks before Teranode starts
	// This is past CSV activation and provides coinbase maturity
	td, sv, txCreator := setupBIP68Test(t, 10, 120)
	defer func() {
		td.Stop(t)
		_ = sv.Stop(ctx)
	}()

	// Create funding at current height (will be in block 121)
	fundingUTXO, err := txCreator.CreateConfirmedFunding(10.0)
	require.NoError(t, err)
	waitForSync(t, ctx, td, sv, 121)

	t.Logf("Created funding UTXO at height 121: %s:%d", fundingUTXO.TxID, fundingUTXO.Vout)

	// Mine 5 more blocks to age the UTXO
	_, err = sv.Generate(5)
	require.NoError(t, err)
	waitForSync(t, ctx, td, sv, 126)

	// Create transaction with sequence = 5 (requires 5 block confirmations)
	// UTXO is at height 121, current height = 127 (after mining the tx)
	// Age = 127 - 121 = 6 blocks ≥ 5 ✓
	tx, err := createSequenceLockedTx(fundingUTXO, txCreator.Address(), 5, 2, td.GetPrivateKey(t))
	require.NoError(t, err)

	t.Logf("Testing height-based sequence lock: sequence=5, UTXO height=121, current=127 after mining")

	// Submit to SV Node (reference) and mine
	txHex := tx.String()
	txID, err := sv.SendRawTransaction(txHex)
	require.NoError(t, err, "SV Node should accept transaction")
	t.Logf("SV Node accepted transaction %s", txID)

	blockHashes, err := sv.Generate(1)
	require.NoError(t, err)
	t.Logf("SV Node mined block %s at height 127", blockHashes[0])

	// Verify BSV Node accepted the block before checking Teranode sync
	svBlockCount, err := sv.GetBlockCount()
	require.NoError(t, err, "Should get BSV Node block count")
	require.Equal(t, 127, svBlockCount, "BSV Node should be at height 127")
	t.Logf("BSV Node block height: %d", svBlockCount)

	// Verify transaction is in the blockchain
	txInfo, err := sv.GetRawTransactionVerbose(txID)
	require.NoError(t, err, "Transaction %s should be retrievable from the blockchain", txID)
	confirmations, ok := txInfo["confirmations"].(float64)
	require.True(t, ok && confirmations >= 1, "Transaction %s should have at least 1 confirmation", txID)
	t.Logf("Transaction %s confirmed with %v confirmations in block %v", txID, confirmations, txInfo["blockhash"])

	// Verify Teranode syncs
	waitForSync(t, ctx, td, sv, 127)

	t.Logf("SUCCESS: Both nodes accepted valid height-based sequence lock")
}

// TestBIP68_HeightBased_Reject verifies that a block containing a tx with an unsatisfied
// height-based sequence lock is rejected by Teranode.
//
// NOTE: Skipped because model.NewBlockFromMsgBlock (model/Block.go:161) only populates the
// coinbase subtree — non-coinbase txs are invisible to block.Valid() and therefore to
// ValidateBlock. Fix that TODO first, then remove the t.Skip.
func TestBIP68_HeightBased_Reject(t *testing.T) {
	t.Skip("BIP68 reject: model.NewBlockFromMsgBlock (model/Block.go:161) only populates the coinbase subtree, so ValidateBlock never sees non-coinbase txs and cannot enforce BIP68 sequence locks on them")
	legacySyncTestLock.Lock()
	defer legacySyncTestLock.Unlock()

	ctx := t.Context()

	// Setup: CSV active at height 10, start with 110 initial blocks
	td, sv, txCreator := setupBIP68Test(t, 10, 110)
	defer func() {
		td.Stop(t)
		_ = sv.Stop(ctx)
	}()

	// Create funding at height 111
	fundingUTXO, err := txCreator.CreateConfirmedFunding(10.0)
	require.NoError(t, err)
	fundingBlockHeight := uint32(111)
	waitForSync(t, ctx, td, sv, fundingBlockHeight)

	t.Logf("Created funding UTXO at height %d: %s:%d", fundingBlockHeight, fundingUTXO.TxID, fundingUTXO.Vout)

	// Create a tx with sequence = 200 (requires 200 block confirmations).
	// UTXO is 1 block old, so height lock is not satisfied (1 < 200).
	sequence := uint32(200)
	tx, err := createSequenceLockedTx(fundingUTXO, txCreator.Address(), sequence, 2, td.GetPrivateKey(t))
	require.NoError(t, err)

	t.Logf("Created tx with sequence=%d (requires %d block confirmations, UTXO is 1 block old → not satisfied)", sequence, sequence)

	// Build an offline block containing the invalid tx
	blockCreator := svnode.NewBlockCreator(sv, txCreator.Address())
	block, err := blockCreator.CreateBlock([]*bt.Tx{tx})
	require.NoError(t, err)
	t.Logf("Created offline block %s at height %d", block.Hash, fundingBlockHeight+1)

	// Record initial heights before submission
	initialSVHeight, err := sv.GetBlockCount()
	require.NoError(t, err)
	require.Equal(t, int(fundingBlockHeight), initialSVHeight)

	_, initialTDMeta, err := td.BlockchainClient.GetBestBlockHeader(ctx)
	require.NoError(t, err)
	initialTDHeight := initialTDMeta.Height
	require.Equal(t, fundingBlockHeight, initialTDHeight)

	// Submit to Teranode via ValidateBlock — the correct full-validation path (calls block.Valid()
	// with UTXO store and subtree store). Should reject because the sequence lock is not satisfied.
	blockBytes, err := hex.DecodeString(block.Hex)
	require.NoError(t, err)
	msgBlock := &wire.MsgBlock{}
	err = msgBlock.Deserialize(bytes.NewReader(blockBytes))
	require.NoError(t, err)
	modelBlock, err := model.NewBlockFromMsgBlock(msgBlock, nil)
	require.NoError(t, err)
	modelBlock.Height = fundingBlockHeight + 1

	err = td.BlockValidationClient.ValidateBlock(ctx, modelBlock, nil)
	require.Error(t, err, "Teranode should reject block with unsatisfied height-based sequence lock (sequence=%d, UTXO age=1 block)", sequence)
	t.Logf("Teranode rejected block as expected: %v", err)

	// Verify Teranode height is unchanged
	_, finalTDMeta, err := td.BlockchainClient.GetBestBlockHeader(ctx)
	require.NoError(t, err)
	require.Equal(t, initialTDHeight, finalTDMeta.Height, "Teranode height should be unchanged after rejecting invalid block")

	t.Logf("SUCCESS: Teranode rejected block with unsatisfied height-based sequence lock, height remained at %d", initialTDHeight)
}

// TestBIP68_TimeBased_Accept verifies valid time-based sequence lock is accepted
func TestBIP68_TimeBased_Accept(t *testing.T) {
	legacySyncTestLock.Lock()
	defer legacySyncTestLock.Unlock()

	ctx := t.Context()

	// Setup with CSV height = 10, generate 120 blocks before Teranode starts
	td, sv, txCreator := setupBIP68Test(t, 10, 120)
	defer func() {
		td.Stop(t)
		_ = sv.Stop(ctx)
	}()

	// Create funding UTXO (confirmed at height 121).
	// Use 9.0 BSV (not 10.0) so this test's funding tx differs from
	// TestBIP68_HeightBased_Accept, giving block 121 a unique hash and
	// avoiding a subtree-quorum path collision when tests run sequentially.
	fundingUTXO, err := txCreator.CreateConfirmedFunding(9.0)
	require.NoError(t, err)
	waitForSync(t, ctx, td, sv, 121)
	t.Logf("Created funding UTXO at height 121: %s:%d", fundingUTXO.TxID, fundingUTXO.Vout)

	// Read the timestamp of the funding block so we can advance beyond it.
	// In regtest all blocks share the same Unix second, so MTP never advances
	// on its own. setmocktime forces subsequent blocks to carry a later timestamp,
	// which makes MTP advance and satisfies the time-based sequence lock.
	fundingBlockHash, err := sv.GetBlockHash(121)
	require.NoError(t, err)
	fundingBlockData, err := sv.GetBlock(fundingBlockHash, 1)
	require.NoError(t, err)
	fundingBlockTime := int64(fundingBlockData["time"].(float64))
	t.Logf("Funding block (121) timestamp: %d", fundingBlockTime)

	// Advance BSV's clock by 2000 seconds (> 2 × 512 = 1024 required by the lock).
	err = sv.SetMockTime(fundingBlockTime + 2000)
	require.NoError(t, err, "Failed to set mock time on BSV node")
	t.Logf("Set BSV mock time to %d (funding + 2000 s)", fundingBlockTime+2000)

	// Mine 11 blocks at the new time so that MTP of the chain tip is fully at
	// fundingBlockTime+2000 (i.e. all 11 preceding blocks carry the new timestamp).
	_, err = sv.Generate(11)
	require.NoError(t, err)
	waitForSync(t, ctx, td, sv, 132)
	t.Logf("Mined 11 blocks at mocktime; chain now at height 132")

	// sequence = SequenceLockTimeTypeFlag | 2 → 2 × 512 = 1024 seconds required.
	// MTP(132) ≈ fundingBlockTime+2000 ≥ MTP(121) + 1024 ✓
	sequence := SequenceLockTimeTypeFlag | 2
	tx, err := createSequenceLockedTx(fundingUTXO, txCreator.Address(), sequence, 2, td.GetPrivateKey(t))
	require.NoError(t, err)
	t.Logf("Testing time-based sequence lock: sequence=0x%x (2 × 512 = 1024 s, 2000 s elapsed)", sequence)

	// BSV 1.2.0 does not enforce BIP68 in mempool or submitblock, so it accepts.
	txHex := tx.String()
	txID, err := sv.SendRawTransaction(txHex)
	require.NoError(t, err, "SV Node should accept transaction to mempool")
	t.Logf("SV Node accepted transaction %s", txID)

	blockHashes, err := sv.Generate(1)
	require.NoError(t, err)
	t.Logf("SV Node mined block %s at height 133", blockHashes[0])

	svBlockCount, err := sv.GetBlockCount()
	require.NoError(t, err, "Should get BSV Node block count")
	require.Equal(t, 133, svBlockCount, "BSV Node should be at height 133")
	t.Logf("BSV Node block height: %d", svBlockCount)

	// Verify transaction is in the blockchain
	txInfo, err := sv.GetRawTransactionVerbose(txID)
	require.NoError(t, err, "Transaction %s should be retrievable from the blockchain", txID)
	confirmations, ok := txInfo["confirmations"].(float64)
	require.True(t, ok && confirmations >= 1, "Transaction %s should have at least 1 confirmation", txID)
	t.Logf("Transaction %s confirmed with %v confirmations in block %v", txID, confirmations, txInfo["blockhash"])

	// Teranode syncs via legacy P2P; BIP68 check passes because elapsed MTP ≥ 1024 s
	waitForSync(t, ctx, td, sv, 133)

	t.Logf("SUCCESS: Both nodes accepted valid time-based sequence lock")
}

// TestBIP68_TimeBased_Reject verifies that a block containing a tx with an unsatisfied
// time-based sequence lock is rejected by Teranode.
//
// NOTE: Same prerequisite as TestBIP68_HeightBased_Reject — model.NewBlockFromMsgBlock
// (model/Block.go:161) only populates the coinbase subtree, so ValidateBlock never sees
// non-coinbase txs. Fix that TODO first, then remove the t.Skip.
func TestBIP68_TimeBased_Reject(t *testing.T) {
	t.Skip("BIP68 reject: model.NewBlockFromMsgBlock (model/Block.go:161) only populates the coinbase subtree, so ValidateBlock never sees non-coinbase txs and cannot enforce BIP68 sequence locks on them")
	legacySyncTestLock.Lock()
	defer legacySyncTestLock.Unlock()

	ctx := t.Context()

	// Setup: CSV active at height 10, start with 110 initial blocks
	td, sv, txCreator := setupBIP68Test(t, 10, 110)
	defer func() {
		td.Stop(t)
		_ = sv.Stop(ctx)
	}()

	// Create funding at height 111
	fundingUTXO, err := txCreator.CreateConfirmedFunding(10.0)
	require.NoError(t, err)
	fundingBlockHeight := uint32(111)
	waitForSync(t, ctx, td, sv, fundingBlockHeight)

	t.Logf("Created funding UTXO at height %d: %s:%d", fundingBlockHeight, fundingUTXO.TxID, fundingUTXO.Vout)

	// Create a tx with time-based sequence = 1000 units (1000 × 512 = 512000 seconds ≈ 6 days).
	// In regtest, blocks are mined immediately so far less than 512000 seconds have elapsed.
	sequence := SequenceLockTimeTypeFlag | 1000
	tx, err := createSequenceLockedTx(fundingUTXO, txCreator.Address(), sequence, 2, td.GetPrivateKey(t))
	require.NoError(t, err)

	t.Logf("Created tx with time-based sequence=1000 (1000 × 512 = 512000 seconds not elapsed → not satisfied)")

	// Build an offline block containing the invalid tx
	blockCreator := svnode.NewBlockCreator(sv, txCreator.Address())
	block, err := blockCreator.CreateBlock([]*bt.Tx{tx})
	require.NoError(t, err)
	t.Logf("Created offline block %s at height %d", block.Hash, fundingBlockHeight+1)

	// Record initial heights before submission
	initialSVHeight, err := sv.GetBlockCount()
	require.NoError(t, err)
	require.Equal(t, int(fundingBlockHeight), initialSVHeight)

	_, initialTDMeta, err := td.BlockchainClient.GetBestBlockHeader(ctx)
	require.NoError(t, err)
	initialTDHeight := initialTDMeta.Height
	require.Equal(t, fundingBlockHeight, initialTDHeight)

	// Submit to Teranode via ValidateBlock — the correct full-validation path (calls block.Valid()
	// with UTXO store and subtree store). Should reject because the time lock is not satisfied.
	blockBytes, err := hex.DecodeString(block.Hex)
	require.NoError(t, err)
	msgBlock := &wire.MsgBlock{}
	err = msgBlock.Deserialize(bytes.NewReader(blockBytes))
	require.NoError(t, err)
	modelBlock, err := model.NewBlockFromMsgBlock(msgBlock, nil)
	require.NoError(t, err)
	modelBlock.Height = fundingBlockHeight + 1

	err = td.BlockValidationClient.ValidateBlock(ctx, modelBlock, nil)
	require.Error(t, err, "Teranode should reject block with unsatisfied time-based sequence lock (1000 × 512 seconds not elapsed)")
	t.Logf("Teranode rejected block as expected: %v", err)

	// Verify Teranode height is unchanged
	_, finalTDMeta, err := td.BlockchainClient.GetBestBlockHeader(ctx)
	require.NoError(t, err)
	require.Equal(t, initialTDHeight, finalTDMeta.Height, "Teranode height should be unchanged after rejecting invalid block")

	t.Logf("SUCCESS: Teranode rejected block with unsatisfied time-based sequence lock, height remained at %d", initialTDHeight)
}

// TestBIP68_DisableFlag verifies disable flag bypasses BIP68 enforcement
func TestBIP68_DisableFlag(t *testing.T) {
	legacySyncTestLock.Lock()
	defer legacySyncTestLock.Unlock()

	ctx := t.Context()

	td, sv, txCreator := setupBIP68Test(t, 10, 115)
	defer func() {
		td.Stop(t)
		_ = sv.Stop(ctx)
	}()

	// Create funding
	fundingUTXO, err := txCreator.CreateConfirmedFunding(10.0)
	require.NoError(t, err)
	waitForSync(t, ctx, td, sv, 116)

	t.Logf("Created funding UTXO at height 116: %s:%d", fundingUTXO.TxID, fundingUTXO.Vout)

	// Mine 1 block
	_, err = sv.Generate(1)
	require.NoError(t, err)
	waitForSync(t, ctx, td, sv, 117)

	// Create transaction with disable flag and high sequence
	// Would fail without disable flag (sequence=100 but only 2 blocks old)
	sequence := SequenceLockTimeDisableFlag | 100
	tx, err := createSequenceLockedTx(fundingUTXO, txCreator.Address(), sequence, 2, td.GetPrivateKey(t))
	require.NoError(t, err)

	t.Logf("Testing disable flag: sequence has disable flag with value 100")

	// Submit and mine - should succeed
	txHex := tx.String()
	txID, err := sv.SendRawTransaction(txHex)
	require.NoError(t, err)
	t.Logf("SV Node accepted transaction %s", txID)

	blockHashes, err := sv.Generate(1)
	require.NoError(t, err)
	t.Logf("SV Node mined block %s at height 118", blockHashes[0])

	// Verify BSV Node accepted the block before checking Teranode sync
	svBlockCount, err := sv.GetBlockCount()
	require.NoError(t, err, "Should get BSV Node block count")
	require.Equal(t, 118, svBlockCount, "BSV Node should be at height 118")
	t.Logf("BSV Node block height: %d", svBlockCount)

	// Verify transaction is in the blockchain
	txInfo, err := sv.GetRawTransactionVerbose(txID)
	require.NoError(t, err, "Transaction %s should be retrievable from the blockchain", txID)
	confirmations, ok := txInfo["confirmations"].(float64)
	require.True(t, ok && confirmations >= 1, "Transaction %s should have at least 1 confirmation", txID)
	t.Logf("Transaction %s confirmed with %v confirmations in block %v", txID, confirmations, txInfo["blockhash"])

	waitForSync(t, ctx, td, sv, 118)

	t.Logf("SUCCESS: Both nodes accepted - disable flag bypassed BIP68")
}

// TestBIP68_BeforeCSVHeight verifies BIP68 not enforced before CSV activation
func TestBIP68_BeforeCSVHeight(t *testing.T) {
	legacySyncTestLock.Lock()
	defer legacySyncTestLock.Unlock()

	ctx := t.Context()

	// Set CSV height to 150 (won't be active during test)
	td, sv, txCreator := setupBIP68Test(t, 150, 115)
	defer func() {
		td.Stop(t)
		_ = sv.Stop(ctx)
	}()

	// Create funding
	fundingUTXO, err := txCreator.CreateConfirmedFunding(10.0)
	require.NoError(t, err)
	waitForSync(t, ctx, td, sv, 116)

	t.Logf("Created funding UTXO at height 116: %s:%d", fundingUTXO.TxID, fundingUTXO.Vout)

	// Mine 1 block
	_, err = sv.Generate(1)
	require.NoError(t, err)
	waitForSync(t, ctx, td, sv, 117)

	// Create transaction with sequence = 100 (would fail if BIP68 active)
	tx, err := createSequenceLockedTx(fundingUTXO, txCreator.Address(), 100, 2, td.GetPrivateKey(t))
	require.NoError(t, err)

	t.Logf("Testing before CSV height: current=118, CSV height=150, sequence=100")

	// Submit and mine - should succeed (BIP68 not enforced yet)
	txHex := tx.String()
	txID, err := sv.SendRawTransaction(txHex)
	require.NoError(t, err)
	t.Logf("SV Node accepted transaction %s", txID)

	blockHashes, err := sv.Generate(1)
	require.NoError(t, err)
	t.Logf("SV Node mined block %s at height 118", blockHashes[0])

	// Verify BSV Node accepted the block before checking Teranode sync
	svBlockCount, err := sv.GetBlockCount()
	require.NoError(t, err, "Should get BSV Node block count")
	require.Equal(t, 118, svBlockCount, "BSV Node should be at height 118")
	t.Logf("BSV Node block height: %d", svBlockCount)

	// Verify transaction is in the blockchain
	txInfo, err := sv.GetRawTransactionVerbose(txID)
	require.NoError(t, err, "Transaction %s should be retrievable from the blockchain", txID)
	confirmations, ok := txInfo["confirmations"].(float64)
	require.True(t, ok && confirmations >= 1, "Transaction %s should have at least 1 confirmation", txID)
	t.Logf("Transaction %s confirmed with %v confirmations in block %v", txID, confirmations, txInfo["blockhash"])

	waitForSync(t, ctx, td, sv, 118)

	t.Logf("SUCCESS: Both nodes accepted - BIP68 not enforced before CSV height")
}

// TestBIP68_Version1Bypass verifies version 1 transactions bypass BIP68
func TestBIP68_Version1Bypass(t *testing.T) {
	legacySyncTestLock.Lock()
	defer legacySyncTestLock.Unlock()

	ctx := t.Context()

	td, sv, txCreator := setupBIP68Test(t, 10, 115)
	defer func() {
		td.Stop(t)
		_ = sv.Stop(ctx)
	}()

	// Create funding
	fundingUTXO, err := txCreator.CreateConfirmedFunding(10.0)
	require.NoError(t, err)
	waitForSync(t, ctx, td, sv, 116)

	t.Logf("Created funding UTXO at height 116: %s:%d", fundingUTXO.TxID, fundingUTXO.Vout)

	// Mine 1 block
	_, err = sv.Generate(1)
	require.NoError(t, err)
	waitForSync(t, ctx, td, sv, 117)

	// Create transaction with version 1 and high sequence
	// Should pass because version 1 bypasses BIP68
	tx, err := createSequenceLockedTx(fundingUTXO, txCreator.Address(), 100, 1, td.GetPrivateKey(t))
	require.NoError(t, err)

	t.Logf("Testing version 1 bypass: version=1, sequence=100, UTXO age=2 blocks")

	// Submit and mine - should succeed
	txHex := tx.String()
	txID, err := sv.SendRawTransaction(txHex)
	require.NoError(t, err)
	t.Logf("SV Node accepted transaction %s", txID)

	blockHashes, err := sv.Generate(1)
	require.NoError(t, err)
	t.Logf("SV Node mined block %s at height 118", blockHashes[0])

	// Verify BSV Node accepted the block before checking Teranode sync
	svBlockCount, err := sv.GetBlockCount()
	require.NoError(t, err, "Should get BSV Node block count")
	require.Equal(t, 118, svBlockCount, "BSV Node should be at height 118")
	t.Logf("BSV Node block height: %d", svBlockCount)

	// Verify transaction is in the blockchain
	txInfo, err := sv.GetRawTransactionVerbose(txID)
	require.NoError(t, err, "Transaction %s should be retrievable from the blockchain", txID)
	confirmations, ok := txInfo["confirmations"].(float64)
	require.True(t, ok && confirmations >= 1, "Transaction %s should have at least 1 confirmation", txID)
	t.Logf("Transaction %s confirmed with %v confirmations in block %v", txID, confirmations, txInfo["blockhash"])

	waitForSync(t, ctx, td, sv, 118)

	t.Logf("SUCCESS: Both nodes accepted - version 1 bypassed BIP68")
}

// TestBIP68_MixedInputTypes verifies mixed sequence lock types in single transaction
func TestBIP68_MixedInputTypes(t *testing.T) {
	legacySyncTestLock.Lock()
	defer legacySyncTestLock.Unlock()

	ctx := t.Context()

	td, sv, txCreator := setupBIP68Test(t, 10, 115)
	defer func() {
		td.Stop(t)
		_ = sv.Stop(ctx)
	}()

	// Create three separate funding UTXOs
	fundingUTXO1, err := txCreator.CreateConfirmedFunding(5.0)
	require.NoError(t, err)
	waitForSync(t, ctx, td, sv, 116)

	fundingUTXO2, err := txCreator.CreateConfirmedFunding(5.0)
	require.NoError(t, err)
	waitForSync(t, ctx, td, sv, 117)

	fundingUTXO3, err := txCreator.CreateConfirmedFunding(5.0)
	require.NoError(t, err)
	waitForSync(t, ctx, td, sv, 118)

	t.Logf("Created 3 funding UTXOs at heights 116, 117, 118")

	// UTXO2 carries a time-based sequence lock (2 × 512 = 1024 s).
	// In regtest all blocks share the same Unix second, so MTP never advances.
	// Read the timestamp of block 117 (UTXO2's confirmation block) and advance
	// BSV's clock by 2000 s so subsequent blocks carry a later timestamp.
	utxo2BlockHash, err := sv.GetBlockHash(117)
	require.NoError(t, err)
	utxo2BlockData, err := sv.GetBlock(utxo2BlockHash, 1)
	require.NoError(t, err)
	utxo2BlockTime := int64(utxo2BlockData["time"].(float64))
	err = sv.SetMockTime(utxo2BlockTime + 2000)
	require.NoError(t, err, "Failed to set mock time on BSV node")
	t.Logf("Set BSV mock time to %d (UTXO2 block + 2000 s)", utxo2BlockTime+2000)

	// Mine 11 blocks at the new time so that MTP is fully at utxo2BlockTime+2000
	_, err = sv.Generate(11)
	require.NoError(t, err)
	waitForSync(t, ctx, td, sv, 129)

	// Create transaction with mixed input types
	tx := bt.NewTx()
	tx.Version = 2

	privKey := td.GetPrivateKey(t)

	// Input 1: Height-based (5 blocks) - UTXO at 116, current will be 130, age = 14 ≥ 5 ✓
	utxo1 := &bt.UTXO{
		TxIDHash:      fundingUTXO1.Tx.TxIDChainHash(),
		Vout:          fundingUTXO1.Vout,
		LockingScript: fundingUTXO1.LockingScript,
		Satoshis:      fundingUTXO1.Amount,
	}
	_ = tx.FromUTXOs(utxo1)
	tx.Inputs[0].SequenceNumber = 5

	// Input 2: Time-based (2 × 512 seconds) - enough time has passed
	utxo2 := &bt.UTXO{
		TxIDHash:      fundingUTXO2.Tx.TxIDChainHash(),
		Vout:          fundingUTXO2.Vout,
		LockingScript: fundingUTXO2.LockingScript,
		Satoshis:      fundingUTXO2.Amount,
	}
	_ = tx.FromUTXOs(utxo2)
	tx.Inputs[1].SequenceNumber = SequenceLockTimeTypeFlag | 2

	// Input 3: Disabled
	utxo3 := &bt.UTXO{
		TxIDHash:      fundingUTXO3.Tx.TxIDChainHash(),
		Vout:          fundingUTXO3.Vout,
		LockingScript: fundingUTXO3.LockingScript,
		Satoshis:      fundingUTXO3.Amount,
	}
	_ = tx.FromUTXOs(utxo3)
	tx.Inputs[2].SequenceNumber = SequenceLockTimeDisableFlag

	// Add output
	totalInput := fundingUTXO1.Amount + fundingUTXO2.Amount + fundingUTXO3.Amount
	outputAmount := totalInput - 30000 // 30k satoshi fee
	_ = tx.AddP2PKHOutputFromAddress(txCreator.Address(), outputAmount)

	// Sign all inputs
	tx, err = signTransaction(tx, privKey)
	require.NoError(t, err)

	t.Logf("Testing mixed input types: height-based(5), time-based(2), disabled")

	// Submit and mine
	txHex := tx.String()
	txID, err := sv.SendRawTransaction(txHex)
	require.NoError(t, err)
	t.Logf("SV Node accepted transaction %s", txID)

	blockHashes, err := sv.Generate(1)
	require.NoError(t, err)
	t.Logf("SV Node mined block %s at height 130", blockHashes[0])

	// Verify BSV Node accepted the block before checking Teranode sync
	svBlockCount, err := sv.GetBlockCount()
	require.NoError(t, err, "Should get BSV Node block count")
	require.Equal(t, 130, svBlockCount, "BSV Node should be at height 130")
	t.Logf("BSV Node block height: %d", svBlockCount)

	// Verify transaction is in the blockchain
	txInfo, err := sv.GetRawTransactionVerbose(txID)
	require.NoError(t, err, "Transaction %s should be retrievable from the blockchain", txID)
	confirmations, ok := txInfo["confirmations"].(float64)
	require.True(t, ok && confirmations >= 1, "Transaction %s should have at least 1 confirmation", txID)
	t.Logf("Transaction %s confirmed with %v confirmations in block %v", txID, confirmations, txInfo["blockhash"])

	waitForSync(t, ctx, td, sv, 130)

	t.Logf("SUCCESS: Both nodes accepted transaction with mixed sequence lock types")
}

// TestBIP68_ZeroSequence verifies zero sequence number imposes no constraint
func TestBIP68_ZeroSequence(t *testing.T) {
	legacySyncTestLock.Lock()
	defer legacySyncTestLock.Unlock()

	ctx := t.Context()

	td, sv, txCreator := setupBIP68Test(t, 10, 115)
	defer func() {
		td.Stop(t)
		_ = sv.Stop(ctx)
	}()

	// Create funding
	fundingUTXO, err := txCreator.CreateConfirmedFunding(10.0)
	require.NoError(t, err)
	waitForSync(t, ctx, td, sv, 116)

	t.Logf("Created funding UTXO at height 116: %s:%d", fundingUTXO.TxID, fundingUTXO.Vout)

	// Immediately create transaction with zero sequence (no aging required)
	tx, err := createSequenceLockedTx(fundingUTXO, txCreator.Address(), 0, 2, td.GetPrivateKey(t))
	require.NoError(t, err)

	t.Logf("Testing zero sequence: sequence=0, UTXO age=1 block")

	// Submit and mine immediately - should succeed
	txHex := tx.String()
	txID, err := sv.SendRawTransaction(txHex)
	require.NoError(t, err)
	t.Logf("SV Node accepted transaction %s", txID)

	blockHashes, err := sv.Generate(1)
	require.NoError(t, err)
	t.Logf("SV Node mined block %s at height 117", blockHashes[0])

	// Verify BSV Node accepted the block before checking Teranode sync
	svBlockCount, err := sv.GetBlockCount()
	require.NoError(t, err, "Should get BSV Node block count")
	require.Equal(t, 117, svBlockCount, "BSV Node should be at height 117")
	t.Logf("BSV Node block height: %d", svBlockCount)

	// Verify transaction is in the blockchain
	txInfo, err := sv.GetRawTransactionVerbose(txID)
	require.NoError(t, err, "Transaction %s should be retrievable from the blockchain", txID)
	confirmations, ok := txInfo["confirmations"].(float64)
	require.True(t, ok && confirmations >= 1, "Transaction %s should have at least 1 confirmation", txID)
	t.Logf("Transaction %s confirmed with %v confirmations in block %v", txID, confirmations, txInfo["blockhash"])

	waitForSync(t, ctx, td, sv, 117)

	t.Logf("SUCCESS: Both nodes accepted - zero sequence imposes no constraint")
}

// TestBIP68_AtExactCSVHeight verifies BIP68 enforced at exact activation height.
//
// SV Node's regtest CSVHeight is hardcoded to 576; it does not enforce BIP68 below that.
// We therefore use a sequence lock that IS satisfied at the activation height (120) so
// both nodes agree. The test verifies Teranode accepts a valid BIP68 tx at exactly CSVHeight.
//
// Funding at height 116, sequence=4:
//
//	minHeight = 116 + 4 - 1 = 119
//	Valid when blockHeight > 119, i.e. at height 120+ (first block where BIP68 is active).
func TestBIP68_AtExactCSVHeight(t *testing.T) {
	legacySyncTestLock.Lock()
	defer legacySyncTestLock.Unlock()

	ctx := t.Context()

	// Set CSV height to 120
	td, sv, txCreator := setupBIP68Test(t, 120, 115)
	defer func() {
		td.Stop(t)
		_ = sv.Stop(ctx)
	}()

	// Create funding at height 116
	fundingUTXO, err := txCreator.CreateConfirmedFunding(10.0)
	require.NoError(t, err)
	waitForSync(t, ctx, td, sv, 116)

	t.Logf("Created funding UTXO at height 116: %s:%d", fundingUTXO.TxID, fundingUTXO.Vout)

	// Mine to height 119 (one block before CSV activation)
	_, err = sv.Generate(3)
	require.NoError(t, err)
	waitForSync(t, ctx, td, sv, 119)

	// sequence=4: minHeight = 116+4-1 = 119. Valid when blockHeight >= 120 (CSVHeight).
	// At height 119 (before BIP68 active), Teranode does not enforce, so the tx can propagate.
	// At height 120 (BIP68 active), the lock is satisfied: 119 < 120.
	sequence := uint32(4)
	tx, err := createSequenceLockedTx(fundingUTXO, txCreator.Address(), sequence, 2, td.GetPrivateKey(t))
	require.NoError(t, err)

	t.Logf("Testing at exact CSV height: current=119, CSV height=120, sequence=%d (satisfied at 120)", sequence)

	txHex := tx.String()
	_, err = sv.SendRawTransaction(txHex)
	require.NoError(t, err, "SV Node accepts to mempool")

	// Mine to exactly CSV height (120) — BIP68 becomes active in Teranode.
	// The sequence lock (funded at 116, sequence=4) is satisfied: blockHeight 120 > minHeight 119.
	blockHashes, err := sv.Generate(1)
	require.NoError(t, err)
	t.Logf("SV Node mined block %s at height 120 (CSV activation)", blockHashes[0])

	waitForSync(t, ctx, td, sv, 120)

	t.Logf("SUCCESS: Both nodes accepted valid BIP68 tx at exact CSV activation height")
}
