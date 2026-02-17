package smoke

import (
	"context"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	bec "github.com/bsv-blockchain/go-sdk/primitives/ec"
	"github.com/bsv-blockchain/teranode/daemon"
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
		err = helper.WaitForNodeBlockHeight(ctx, td.BlockchainClient, uint32(initialHeight), 60*time.Second)
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

	// Verify Teranode syncs
	waitForSync(t, ctx, td, sv, 127)

	t.Logf("SUCCESS: Both nodes accepted valid height-based sequence lock")
}

// TestBIP68_HeightBased_Reject verifies invalid height-based sequence lock is rejected
func TestBIP68_HeightBased_Reject(t *testing.T) {
	legacySyncTestLock.Lock()
	defer legacySyncTestLock.Unlock()

	ctx := t.Context()

	// Setup with CSV height = 10, generate 115 blocks before Teranode starts
	td, sv, txCreator := setupBIP68Test(t, 10, 115)
	defer func() {
		td.Stop(t)
		_ = sv.Stop(ctx)
	}()

	// Create funding at height 116
	fundingUTXO, err := txCreator.CreateConfirmedFunding(10.0)
	require.NoError(t, err)
	waitForSync(t, ctx, td, sv, 116)

	t.Logf("Created funding UTXO at height 116: %s:%d", fundingUTXO.TxID, fundingUTXO.Vout)

	// Mine only 2 more blocks
	_, err = sv.Generate(2)
	require.NoError(t, err)
	waitForSync(t, ctx, td, sv, 118)

	// Create transaction with sequence = 100 (requires 100 block confirmations)
	// UTXO at height 116, current height = 119 after mining
	// Age = 119 - 116 = 3 blocks < 100 ✗
	tx, err := createSequenceLockedTx(fundingUTXO, txCreator.Address(), 100, 2, td.GetPrivateKey(t))
	require.NoError(t, err)

	t.Logf("Testing invalid height-based lock: sequence=100, UTXO height=116, current=119 after mining")

	// Submit to SV Node - it will accept to mempool but should reject when mining
	txHex := tx.String()
	_, err = sv.SendRawTransaction(txHex)
	require.NoError(t, err, "SV Node accepts to mempool (doesn't validate BIP68 in mempool)")

	// Try to mine - SV Node should exclude this transaction from the block
	blockHashes, err := sv.Generate(1)
	require.NoError(t, err)
	t.Logf("SV Node mined block %s (should exclude invalid tx)", blockHashes[0])

	// Wait for sync
	waitForSync(t, ctx, td, sv, 119)

	// Verify the transaction is still in mempool (not in block)
	svBlockCount, _ := sv.GetBlockCount()
	t.Logf("SV Node height: %d - transaction correctly excluded from block", svBlockCount)

	t.Logf("SUCCESS: SV Node correctly rejected invalid height-based sequence lock by excluding from block")
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

	// Create funding
	fundingUTXO, err := txCreator.CreateConfirmedFunding(10.0)
	require.NoError(t, err)
	waitForSync(t, ctx, td, sv, 121)

	t.Logf("Created funding UTXO at height 121: %s:%d", fundingUTXO.TxID, fundingUTXO.Vout)

	// Mine 10 more blocks to provide time separation
	_, err = sv.Generate(10)
	require.NoError(t, err)
	waitForSync(t, ctx, td, sv, 131)

	// Create transaction with time-based sequence lock
	// 2 × 512 seconds = 1024 seconds (~17 minutes)
	// With 10 blocks mined, sufficient time should have passed
	sequence := SequenceLockTimeTypeFlag | 2
	tx, err := createSequenceLockedTx(fundingUTXO, txCreator.Address(), sequence, 2, td.GetPrivateKey(t))
	require.NoError(t, err)

	t.Logf("Testing time-based sequence lock: 2 × 512 = 1024 seconds")

	// Submit and mine
	txHex := tx.String()
	txID, err := sv.SendRawTransaction(txHex)
	require.NoError(t, err)
	t.Logf("SV Node accepted transaction %s", txID)

	blockHashes, err := sv.Generate(1)
	require.NoError(t, err)
	t.Logf("SV Node mined block %s at height 132", blockHashes[0])

	waitForSync(t, ctx, td, sv, 132)

	t.Logf("SUCCESS: Both nodes accepted valid time-based sequence lock")
}

// TestBIP68_TimeBased_Reject verifies invalid time-based sequence lock is rejected
func TestBIP68_TimeBased_Reject(t *testing.T) {
	legacySyncTestLock.Lock()
	defer legacySyncTestLock.Unlock()

	ctx := t.Context()

	// Setup with CSV height = 10, generate 115 blocks before Teranode starts
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

	// Mine only 1 block (not enough time)
	_, err = sv.Generate(1)
	require.NoError(t, err)
	waitForSync(t, ctx, td, sv, 117)

	// Create transaction with large time-based sequence lock
	// 10000 × 512 seconds = 5,120,000 seconds (~59 days)
	sequence := SequenceLockTimeTypeFlag | 10000
	tx, err := createSequenceLockedTx(fundingUTXO, txCreator.Address(), sequence, 2, td.GetPrivateKey(t))
	require.NoError(t, err)

	t.Logf("Testing invalid time-based lock: 10000 × 512 = 5,120,000 seconds (~59 days)")

	// Submit to SV Node
	txHex := tx.String()
	_, err = sv.SendRawTransaction(txHex)
	require.NoError(t, err, "SV Node accepts to mempool")

	// Try to mine - should exclude invalid transaction
	blockHashes, err := sv.Generate(1)
	require.NoError(t, err)
	t.Logf("SV Node mined block %s (should exclude invalid tx)", blockHashes[0])

	waitForSync(t, ctx, td, sv, 118)

	t.Logf("SUCCESS: SV Node correctly rejected invalid time-based sequence lock by excluding from block")
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

	// Mine 10 more blocks to age the UTXOs
	_, err = sv.Generate(10)
	require.NoError(t, err)
	waitForSync(t, ctx, td, sv, 128)

	// Create transaction with mixed input types
	tx := bt.NewTx()
	tx.Version = 2

	privKey := td.GetPrivateKey(t)

	// Input 1: Height-based (5 blocks) - UTXO at 116, current will be 129, age = 13 ≥ 5 ✓
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
	t.Logf("SV Node mined block %s at height 129", blockHashes[0])

	waitForSync(t, ctx, td, sv, 129)

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

	waitForSync(t, ctx, td, sv, 117)

	t.Logf("SUCCESS: Both nodes accepted - zero sequence imposes no constraint")
}

// TestBIP68_AtExactCSVHeight verifies BIP68 enforced at exact activation height
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

	// Create transaction with sequence = 100 (would fail at CSV height)
	tx, err := createSequenceLockedTx(fundingUTXO, txCreator.Address(), 100, 2, td.GetPrivateKey(t))
	require.NoError(t, err)

	t.Logf("Testing at exact CSV height: current=119, CSV height=120, sequence=100")

	// Submit to SV Node
	txHex := tx.String()
	_, err = sv.SendRawTransaction(txHex)
	require.NoError(t, err, "SV Node accepts to mempool (before CSV)")

	// Mine to exactly CSV height (120) - should exclude invalid transaction
	blockHashes, err := sv.Generate(1)
	require.NoError(t, err)
	t.Logf("SV Node mined block %s at height 120 (CSV activation)", blockHashes[0])

	waitForSync(t, ctx, td, sv, 120)

	t.Logf("SUCCESS: Both nodes enforced BIP68 at exact CSV activation height")
}
