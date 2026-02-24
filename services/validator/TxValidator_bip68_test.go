package validator

import (
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/require"
)

// TestSequenceLocks_BeforeCSVHeight verifies that BIP68 is not enforced before CSVHeight.
func TestSequenceLocks_BeforeCSVHeight(t *testing.T) {
	logger := ulogger.TestLogger{}
	tSettings := test.CreateBaseTestSettings(t)
	txValidator := NewTxValidator(logger, tSettings)

	// Create a transaction with sequence number that would fail BIP68 if it were active
	tx := bt.NewTx()
	require.NoError(t, tx.From("0000000000000000000000000000000000000000000000000000000000000001", 0, "76a914000000000000000000000000000000000000000088ac", 100))
	require.NoError(t, tx.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 50))

	// Set sequence number with relative lock-time (512 seconds * 100 = 51200 seconds)
	tx.Inputs[0].SequenceNumber = 100

	// Block height before CSVHeight (mainnet CSVHeight = 419328)
	blockHeight := uint32(419327)
	utxoHeights := []uint32{419320}
	utxoMTPs := []uint32{1000000}
	blockMTP := uint32(1000100)

	// Should succeed because BIP68 is not active yet
	err := txValidator.ValidateTransaction(tx, blockHeight, utxoHeights, &Options{SkipPolicyChecks: true})
	require.NoError(t, err, "BIP68 should not be enforced before CSVHeight")
	err = txValidator.ValidateBIP68(tx, blockHeight, utxoHeights, utxoMTPs, blockMTP)
	require.NoError(t, err, "BIP68 should not be enforced before CSVHeight")
}

// TestSequenceLocks_Version1Transaction verifies that version 1 transactions bypass BIP68.
func TestSequenceLocks_Version1Transaction(t *testing.T) {
	logger := ulogger.TestLogger{}
	tSettings := test.CreateBaseTestSettings(t)
	txValidator := NewTxValidator(logger, tSettings)

	tx := bt.NewTx()
	require.NoError(t, tx.From("0000000000000000000000000000000000000000000000000000000000000001", 0, "76a914000000000000000000000000000000000000000088ac", 100))
	require.NoError(t, tx.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 50))

	// Set version to 1 (BIP68 requires version >= 2)
	tx.Version = 1
	// Set sequence number with relative lock-time
	tx.Inputs[0].SequenceNumber = 100

	blockHeight := uint32(420000) // After CSVHeight
	utxoHeights := []uint32{419900}
	utxoMTPs := []uint32{1000000}
	blockMTP := uint32(1000100)

	// Should succeed because version 1 transactions bypass BIP68
	err := txValidator.ValidateTransaction(tx, blockHeight, utxoHeights, &Options{SkipPolicyChecks: true})
	require.NoError(t, err, "Version 1 transactions should bypass BIP68")
	err = txValidator.ValidateBIP68(tx, blockHeight, utxoHeights, utxoMTPs, blockMTP)
	require.NoError(t, err, "Version 1 transactions should bypass BIP68")
}

// TestSequenceLocks_DisabledFlag verifies that the disable flag bypasses BIP68.
func TestSequenceLocks_DisabledFlag(t *testing.T) {
	logger := ulogger.TestLogger{}
	tSettings := test.CreateBaseTestSettings(t)
	txValidator := NewTxValidator(logger, tSettings)

	tx := bt.NewTx()
	require.NoError(t, tx.From("0000000000000000000000000000000000000000000000000000000000000001", 0, "76a914000000000000000000000000000000000000000088ac", 100))
	require.NoError(t, tx.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 50))

	tx.Version = 2
	// Set sequence number with disable flag (bit 31)
	tx.Inputs[0].SequenceNumber = SequenceLockTimeDisableFlag | 100

	blockHeight := uint32(420000)
	utxoHeights := []uint32{419900}
	utxoMTPs := []uint32{1000000}
	blockMTP := uint32(1000100)

	// Should succeed because disable flag is set
	err := txValidator.ValidateTransaction(tx, blockHeight, utxoHeights, &Options{SkipPolicyChecks: true})
	require.NoError(t, err, "Sequence lock disable flag should bypass BIP68")
	err = txValidator.ValidateBIP68(tx, blockHeight, utxoHeights, utxoMTPs, blockMTP)
	require.NoError(t, err, "Sequence lock disable flag should bypass BIP68")
}

// TestSequenceLocks_HeightBased_Success verifies successful height-based relative lock-time.
func TestSequenceLocks_HeightBased_Success(t *testing.T) {
	logger := ulogger.TestLogger{}
	tSettings := test.CreateBaseTestSettings(t)
	txValidator := NewTxValidator(logger, tSettings)

	tx := bt.NewTx()
	require.NoError(t, tx.From("0000000000000000000000000000000000000000000000000000000000000001", 0, "76a914000000000000000000000000000000000000000088ac", 100))
	require.NoError(t, tx.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 50))

	tx.Version = 2
	// Set sequence number for 10 block relative lock (no type flag = height-based)
	tx.Inputs[0].SequenceNumber = 10

	// UTXO created at height 419900, requires 10 confirmations
	// Minimum height = 419900 + 10 = 419910
	// Current block height = 419911 (sufficient)
	blockHeight := uint32(419911)
	utxoHeights := []uint32{419900}
	utxoMTPs := []uint32{1000000}
	blockMTP := uint32(1000100)

	// Should succeed because enough blocks have passed
	err := txValidator.ValidateTransaction(tx, blockHeight, utxoHeights, &Options{SkipPolicyChecks: true})
	require.NoError(t, err, "Height-based sequence lock should succeed when enough blocks have passed")
	err = txValidator.ValidateBIP68(tx, blockHeight, utxoHeights, utxoMTPs, blockMTP)
	require.NoError(t, err, "Height-based sequence lock should succeed when enough blocks have passed")
}

// TestSequenceLocks_HeightBased_Failure verifies failed height-based relative lock-time.
func TestSequenceLocks_HeightBased_Failure(t *testing.T) {
	logger := ulogger.TestLogger{}
	tSettings := test.CreateBaseTestSettings(t)
	txValidator := NewTxValidator(logger, tSettings)

	tx := bt.NewTx()
	require.NoError(t, tx.From("0000000000000000000000000000000000000000000000000000000000000001", 0, "76a914000000000000000000000000000000000000000088ac", 100))
	require.NoError(t, tx.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 50))

	tx.Version = 2
	// Set sequence number for 100 block relative lock
	tx.Inputs[0].SequenceNumber = 100

	// UTXO created at height 419900, requires 100 confirmations
	// Minimum height = 419900 + 100 = 420000
	// Current block height = 419950 (insufficient)
	blockHeight := uint32(419950)
	utxoHeights := []uint32{419900}
	utxoMTPs := []uint32{1000000}
	blockMTP := uint32(1000100)

	// Should fail because not enough blocks have passed
	err := txValidator.ValidateTransaction(tx, blockHeight, utxoHeights, &Options{SkipPolicyChecks: true})
	require.NoError(t, err)
	err = txValidator.ValidateBIP68(tx, blockHeight, utxoHeights, utxoMTPs, blockMTP)
	require.Error(t, err, "Height-based sequence lock should fail when insufficient blocks have passed")
	require.Contains(t, err.Error(), "sequence lock height not satisfied")
}

// TestSequenceLocks_TimeBased_Success verifies successful time-based relative lock-time.
func TestSequenceLocks_TimeBased_Success(t *testing.T) {
	logger := ulogger.TestLogger{}
	tSettings := test.CreateBaseTestSettings(t)
	txValidator := NewTxValidator(logger, tSettings)

	tx := bt.NewTx()
	require.NoError(t, tx.From("0000000000000000000000000000000000000000000000000000000000000001", 0, "76a914000000000000000000000000000000000000000088ac", 100))
	require.NoError(t, tx.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 50))

	tx.Version = 2
	// Set sequence number for time-based lock: 10 * 512 seconds = 5120 seconds
	// Type flag (bit 22) must be set for time-based lock
	tx.Inputs[0].SequenceNumber = SequenceLockTimeTypeFlag | 10

	// UTXO created at block with MTP = 1000000
	// Minimum time = 1000000 + (10 << 9) = 1000000 + 5120 = 1005120
	// Current block MTP = 1006000 (sufficient)
	blockHeight := uint32(419911)
	utxoHeights := []uint32{419900}
	utxoMTPs := []uint32{1000000}
	blockMTP := uint32(1006000)

	// Should succeed because enough time has passed
	err := txValidator.ValidateTransaction(tx, blockHeight, utxoHeights, &Options{SkipPolicyChecks: true})
	require.NoError(t, err, "Time-based sequence lock should succeed when enough time has passed")
	err = txValidator.ValidateBIP68(tx, blockHeight, utxoHeights, utxoMTPs, blockMTP)
	require.NoError(t, err, "Time-based sequence lock should succeed when enough time has passed")
}

// TestSequenceLocks_TimeBased_Failure verifies failed time-based relative lock-time.
func TestSequenceLocks_TimeBased_Failure(t *testing.T) {
	logger := ulogger.TestLogger{}
	tSettings := test.CreateBaseTestSettings(t)
	txValidator := NewTxValidator(logger, tSettings)

	tx := bt.NewTx()
	require.NoError(t, tx.From("0000000000000000000000000000000000000000000000000000000000000001", 0, "76a914000000000000000000000000000000000000000088ac", 100))
	require.NoError(t, tx.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 50))

	tx.Version = 2
	// Set sequence number for time-based lock: 100 * 512 seconds = 51200 seconds
	tx.Inputs[0].SequenceNumber = SequenceLockTimeTypeFlag | 100

	// UTXO created at block with MTP = 1000000
	// Minimum time = 1000000 + (100 << 9) = 1000000 + 51200 = 1051200
	// Current block MTP = 1030000 (insufficient)
	blockHeight := uint32(419911)
	utxoHeights := []uint32{419900}
	utxoMTPs := []uint32{1000000}
	blockMTP := uint32(1030000)

	// Should fail because not enough time has passed
	err := txValidator.ValidateTransaction(tx, blockHeight, utxoHeights, &Options{SkipPolicyChecks: true})
	require.NoError(t, err)
	err = txValidator.ValidateBIP68(tx, blockHeight, utxoHeights, utxoMTPs, blockMTP)
	require.Error(t, err, "Time-based sequence lock should fail when insufficient time has passed")
	require.Contains(t, err.Error(), "sequence lock time not satisfied")
}

// TestSequenceLocks_MultipleInputs verifies sequence locks with multiple inputs.
func TestSequenceLocks_MultipleInputs(t *testing.T) {
	logger := ulogger.TestLogger{}
	tSettings := test.CreateBaseTestSettings(t)
	txValidator := NewTxValidator(logger, tSettings)

	tx := bt.NewTx()
	// Add multiple inputs with different sequence numbers
	require.NoError(t, tx.From("0000000000000000000000000000000000000000000000000000000000000001", 0, "76a914000000000000000000000000000000000000000088ac", 50))
	require.NoError(t, tx.From("0000000000000000000000000000000000000000000000000000000000000002", 0, "76a914000000000000000000000000000000000000000088ac", 50))
	require.NoError(t, tx.From("0000000000000000000000000000000000000000000000000000000000000003", 0, "76a914000000000000000000000000000000000000000088ac", 50))
	require.NoError(t, tx.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 100))

	tx.Version = 2
	// Input 0: 10 blocks relative lock
	tx.Inputs[0].SequenceNumber = 10
	// Input 1: 5 blocks relative lock
	tx.Inputs[1].SequenceNumber = 5
	// Input 2: disabled (no relative lock)
	tx.Inputs[2].SequenceNumber = SequenceLockTimeDisableFlag

	// UTXOs created at different heights
	// Input 0: height 419900, needs 10 blocks -> min height 419910
	// Input 1: height 419950, needs 5 blocks -> min height 419955
	// Input 2: disabled, no requirement
	// Maximum of all: 419955
	blockHeight := uint32(419956) // Just enough
	utxoHeights := []uint32{419900, 419950, 419940}
	utxoMTPs := []uint32{1000000, 1002000, 1001000}
	blockMTP := uint32(1003000)

	// Should succeed because the most restrictive input is satisfied
	err := txValidator.ValidateTransaction(tx, blockHeight, utxoHeights, &Options{SkipPolicyChecks: true})
	require.NoError(t, err, "Multiple inputs should succeed when all sequence locks are satisfied")
	err = txValidator.ValidateBIP68(tx, blockHeight, utxoHeights, utxoMTPs, blockMTP)
	require.NoError(t, err, "Multiple inputs should succeed when all sequence locks are satisfied")
}

// TestSequenceLocks_MultipleInputs_Failure verifies failure with multiple inputs.
func TestSequenceLocks_MultipleInputs_Failure(t *testing.T) {
	logger := ulogger.TestLogger{}
	tSettings := test.CreateBaseTestSettings(t)
	txValidator := NewTxValidator(logger, tSettings)

	tx := bt.NewTx()
	require.NoError(t, tx.From("0000000000000000000000000000000000000000000000000000000000000001", 0, "76a914000000000000000000000000000000000000000088ac", 50))
	require.NoError(t, tx.From("0000000000000000000000000000000000000000000000000000000000000002", 0, "76a914000000000000000000000000000000000000000088ac", 50))
	require.NoError(t, tx.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 75))

	tx.Version = 2
	// Input 0: 10 blocks (satisfied)
	tx.Inputs[0].SequenceNumber = 10
	// Input 1: 100 blocks (NOT satisfied)
	tx.Inputs[1].SequenceNumber = 100

	// Input 0: min height 419910 (satisfied by 419950)
	// Input 1: min height 420000 (NOT satisfied by 419950)
	blockHeight := uint32(419950)
	utxoHeights := []uint32{419900, 419900}
	utxoMTPs := []uint32{1000000, 1000000}
	blockMTP := uint32(1002000)

	// Should fail because input 1's sequence lock is not satisfied
	err := txValidator.ValidateTransaction(tx, blockHeight, utxoHeights, &Options{SkipPolicyChecks: true})
	require.NoError(t, err)
	err = txValidator.ValidateBIP68(tx, blockHeight, utxoHeights, utxoMTPs, blockMTP)
	require.Error(t, err, "Multiple inputs should fail when any sequence lock is not satisfied")
	require.Contains(t, err.Error(), "sequence lock height not satisfied")
}

// TestSequenceLocks_MaxValue verifies behavior with maximum sequence number.
func TestSequenceLocks_MaxValue(t *testing.T) {
	logger := ulogger.TestLogger{}
	tSettings := test.CreateBaseTestSettings(t)
	txValidator := NewTxValidator(logger, tSettings)

	tx := bt.NewTx()
	require.NoError(t, tx.From("0000000000000000000000000000000000000000000000000000000000000001", 0, "76a914000000000000000000000000000000000000000088ac", 100))
	require.NoError(t, tx.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 50))

	tx.Version = 2
	// Set sequence number to maximum value (0x0000ffff = 65535)
	tx.Inputs[0].SequenceNumber = SequenceLockTimeMask

	// UTXO created at height 419900, requires 65535 confirmations
	// Minimum height = 419900 + 65535 = 485435
	// Current block height = 500000 (sufficient)
	blockHeight := uint32(500000)
	utxoHeights := []uint32{419900}
	utxoMTPs := []uint32{1000000}
	blockMTP := uint32(2000000)

	// Should succeed with maximum value
	err := txValidator.ValidateTransaction(tx, blockHeight, utxoHeights, &Options{SkipPolicyChecks: true})
	require.NoError(t, err, "Sequence lock with maximum value should succeed when satisfied")
	err = txValidator.ValidateBIP68(tx, blockHeight, utxoHeights, utxoMTPs, blockMTP)
	require.NoError(t, err, "Sequence lock with maximum value should succeed when satisfied")
}

// TestSequenceLocks_NotEnforcedInMempool verifies that BIP68 is only enforced during block validation.
func TestSequenceLocks_NotEnforcedInMempool(t *testing.T) {
	logger := ulogger.TestLogger{}
	tSettings := test.CreateBaseTestSettings(t)
	txValidator := NewTxValidator(logger, tSettings)

	tx := bt.NewTx()
	require.NoError(t, tx.From("0000000000000000000000000000000000000000000000000000000000000001", 0, "76a914000000000000000000000000000000000000000088ac", 100))
	require.NoError(t, tx.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 50))

	tx.Version = 2
	// Set sequence number that would fail if it were checked
	tx.Inputs[0].SequenceNumber = 1000

	blockHeight := uint32(419950)
	utxoHeights := []uint32{419900}

	// Should succeed because SkipPolicyChecks = false means mempool validation
	// BIP68 is only enforced during block validation (SkipPolicyChecks = true)
	err := txValidator.ValidateTransaction(tx, blockHeight, utxoHeights, &Options{SkipPolicyChecks: false})
	require.NoError(t, err, "BIP68 should not be enforced in mempool (SkipPolicyChecks=false)")
}

// TestSequenceLocks_AtExactCSVHeight verifies BIP68 activates exactly at CSVHeight
// (inclusive), matching BSV C++: if (pindex_->GetHeight() >= consensusParams.CSVHeight).
func TestSequenceLocks_AtExactCSVHeight(t *testing.T) {
	logger := ulogger.TestLogger{}
	tSettings := test.CreateBaseTestSettings(t)
	txValidator := NewTxValidator(logger, tSettings)

	blockHeight := tSettings.ChainCfgParams.CSVHeight
	utxoMTPs := []uint32{1000000}
	blockMTP := uint32(1100000)

	// tx: sequence=10, UTXO at blockHeight-50.
	// minHeight = (blockHeight-50) + 10 - 1 = blockHeight-41 < blockHeight → passes.
	tx := bt.NewTx()
	require.NoError(t, tx.From("0000000000000000000000000000000000000000000000000000000000000001", 0, "76a914000000000000000000000000000000000000000088ac", 100))
	require.NoError(t, tx.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 50))
	tx.Version = 2
	tx.Inputs[0].SequenceNumber = 10
	utxoHeights := []uint32{blockHeight - 50}

	err := txValidator.ValidateTransaction(tx, blockHeight, utxoHeights, &Options{SkipPolicyChecks: true})
	require.NoError(t, err, "BIP68 should be enforced at exact CSVHeight and pass when satisfied")
	err = txValidator.ValidateBIP68(tx, blockHeight, utxoHeights, utxoMTPs, blockMTP)
	require.NoError(t, err, "BIP68 should be enforced at exact CSVHeight and pass when satisfied")

	// tx2: sequence=100, UTXO at blockHeight-50.
	// minHeight = (blockHeight-50) + 100 - 1 = blockHeight+49 >= blockHeight → rejected.
	tx2 := bt.NewTx()
	require.NoError(t, tx2.From("0000000000000000000000000000000000000000000000000000000000000001", 0, "76a914000000000000000000000000000000000000000088ac", 100))
	require.NoError(t, tx2.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 50))
	tx2.Version = 2
	tx2.Inputs[0].SequenceNumber = 100
	utxoHeights2 := []uint32{blockHeight - 50}

	err = txValidator.ValidateTransaction(tx2, blockHeight, utxoHeights2, &Options{SkipPolicyChecks: true})
	require.NoError(t, err)
	err = txValidator.ValidateBIP68(tx2, blockHeight, utxoHeights2, utxoMTPs, blockMTP)
	require.Error(t, err, "BIP68 should be enforced at exact CSVHeight and fail when not satisfied")
	require.Contains(t, err.Error(), "sequence lock height not satisfied")
}

// TestSequenceLocks_MixedTypes verifies transaction with mixed sequence lock types.
func TestSequenceLocks_MixedTypes(t *testing.T) {
	logger := ulogger.TestLogger{}
	tSettings := test.CreateBaseTestSettings(t)
	txValidator := NewTxValidator(logger, tSettings)

	tx := bt.NewTx()
	// Add 4 inputs with different sequence lock types
	require.NoError(t, tx.From("0000000000000000000000000000000000000000000000000000000000000001", 0, "76a914000000000000000000000000000000000000000088ac", 25))
	require.NoError(t, tx.From("0000000000000000000000000000000000000000000000000000000000000002", 0, "76a914000000000000000000000000000000000000000088ac", 25))
	require.NoError(t, tx.From("0000000000000000000000000000000000000000000000000000000000000003", 0, "76a914000000000000000000000000000000000000000088ac", 25))
	require.NoError(t, tx.From("0000000000000000000000000000000000000000000000000000000000000004", 0, "76a914000000000000000000000000000000000000000088ac", 25))
	require.NoError(t, tx.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 75))

	tx.Version = 2
	// Input 0: Height-based lock (5 blocks)
	tx.Inputs[0].SequenceNumber = 5
	// Input 1: Time-based lock (10 * 512 seconds = 5120 seconds)
	tx.Inputs[1].SequenceNumber = SequenceLockTimeTypeFlag | 10
	// Input 2: Disabled (no lock)
	tx.Inputs[2].SequenceNumber = SequenceLockTimeDisableFlag
	// Input 3: Height-based lock (3 blocks)
	tx.Inputs[3].SequenceNumber = 3

	blockHeight := uint32(420000)
	utxoHeights := []uint32{419900, 419920, 419950, 419980} // Various heights
	// For time-based lock: need MTP at utxoHeight-1
	// Input 1 at height 419920, requires 5120 seconds after its MTP
	utxoMTPs := []uint32{1000000, 1000000, 1000000, 1000000}
	// minTime = 1000000 + 5120 = 1005120
	blockMTP := uint32(1010000) // Sufficient time has passed

	// Calculate expected minHeight:
	// Input 0: 419900 + 5 = 419905
	// Input 3: 419980 + 3 = 419983 (this is the most restrictive)
	// minHeight = 419983, blockHeight = 420000, so 419983 < 420000 ✓

	err := txValidator.ValidateTransaction(tx, blockHeight, utxoHeights, &Options{SkipPolicyChecks: true})
	require.NoError(t, err, "Mixed sequence lock types should succeed when all constraints are satisfied")
	err = txValidator.ValidateBIP68(tx, blockHeight, utxoHeights, utxoMTPs, blockMTP)
	require.NoError(t, err, "Mixed sequence lock types should succeed when all constraints are satisfied")
}

// TestSequenceLocks_MixedTypes_Failure verifies failure when any mixed lock type fails.
func TestSequenceLocks_MixedTypes_Failure(t *testing.T) {
	logger := ulogger.TestLogger{}
	tSettings := test.CreateBaseTestSettings(t)
	txValidator := NewTxValidator(logger, tSettings)

	tx := bt.NewTx()
	require.NoError(t, tx.From("0000000000000000000000000000000000000000000000000000000000000001", 0, "76a914000000000000000000000000000000000000000088ac", 50))
	require.NoError(t, tx.From("0000000000000000000000000000000000000000000000000000000000000002", 0, "76a914000000000000000000000000000000000000000088ac", 50))
	require.NoError(t, tx.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 75))

	tx.Version = 2
	// Input 0: Height-based lock (10 blocks) - will pass
	tx.Inputs[0].SequenceNumber = 10
	// Input 1: Time-based lock (1000 * 512 = 512000 seconds) - will FAIL
	tx.Inputs[1].SequenceNumber = SequenceLockTimeTypeFlag | 1000

	blockHeight := uint32(420000)
	utxoHeights := []uint32{419900, 419950}
	utxoMTPs := []uint32{1000000, 1000000}
	// minTime for input 1 = 1000000 + 512000 = 1512000
	blockMTP := uint32(1100000) // Not enough time has passed (1100000 < 1512000)

	err := txValidator.ValidateTransaction(tx, blockHeight, utxoHeights, &Options{SkipPolicyChecks: true})
	require.NoError(t, err)
	err = txValidator.ValidateBIP68(tx, blockHeight, utxoHeights, utxoMTPs, blockMTP)
	require.Error(t, err, "Mixed types should fail when time-based constraint is not satisfied")
	require.Contains(t, err.Error(), "sequence lock time not satisfied")
}

// TestSequenceLocks_ZeroValue verifies behavior with zero sequence number.
func TestSequenceLocks_ZeroValue(t *testing.T) {
	logger := ulogger.TestLogger{}
	tSettings := test.CreateBaseTestSettings(t)
	txValidator := NewTxValidator(logger, tSettings)

	tx := bt.NewTx()
	require.NoError(t, tx.From("0000000000000000000000000000000000000000000000000000000000000001", 0, "76a914000000000000000000000000000000000000000088ac", 100))
	require.NoError(t, tx.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 50))

	tx.Version = 2
	// Zero sequence number means no relative lock-time constraint
	tx.Inputs[0].SequenceNumber = 0

	blockHeight := uint32(420000)
	utxoHeights := []uint32{419999} // UTXO just 1 block ago
	utxoMTPs := []uint32{1000000}
	blockMTP := uint32(1000001)

	// Should succeed because sequence = 0 means minHeight = 419999 + 0 = 419999
	// and 419999 < 420000
	err := txValidator.ValidateTransaction(tx, blockHeight, utxoHeights, &Options{SkipPolicyChecks: true})
	require.NoError(t, err, "Zero sequence number should impose no additional constraint")
	err = txValidator.ValidateBIP68(tx, blockHeight, utxoHeights, utxoMTPs, blockMTP)
	require.NoError(t, err, "Zero sequence number should impose no additional constraint")
}

// TestSequenceLocks_JustBelowDisableFlag verifies sequence number just below disable flag.
func TestSequenceLocks_JustBelowDisableFlag(t *testing.T) {
	logger := ulogger.TestLogger{}
	tSettings := test.CreateBaseTestSettings(t)
	txValidator := NewTxValidator(logger, tSettings)

	tx := bt.NewTx()
	require.NoError(t, tx.From("0000000000000000000000000000000000000000000000000000000000000001", 0, "76a914000000000000000000000000000000000000000088ac", 100))
	require.NoError(t, tx.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 50))

	tx.Version = 2
	// Set sequence to just below disable flag: 0x7FFFFFFF (bit 31 not set, all others set)
	// This means: bit 22 is SET (time-based), and masked value is 0x0000ffff
	tx.Inputs[0].SequenceNumber = 0x7FFFFFFF

	blockHeight := uint32(420000)
	utxoHeights := []uint32{419000}
	utxoMTPs := []uint32{1000000}
	// Masked value = 0x7FFFFFFF & 0x0000FFFF = 0x0000FFFF = 65535
	// Type flag (bit 22) is SET in 0x7FFFFFFF, so it's time-based
	// minTime = 1000000 + (65535 << 9) = 1000000 + 33553920 = 34553920
	blockMTP := uint32(40000000) // Sufficient time

	err := txValidator.ValidateTransaction(tx, blockHeight, utxoHeights, &Options{SkipPolicyChecks: true})
	require.NoError(t, err, "Sequence number just below disable flag should work when satisfied")
	err = txValidator.ValidateBIP68(tx, blockHeight, utxoHeights, utxoMTPs, blockMTP)
	require.NoError(t, err, "Sequence number just below disable flag should work when satisfied")
}

// TestSequenceLocks_TypeFlagOnly verifies type flag set with zero lock value.
func TestSequenceLocks_TypeFlagOnly(t *testing.T) {
	logger := ulogger.TestLogger{}
	tSettings := test.CreateBaseTestSettings(t)
	txValidator := NewTxValidator(logger, tSettings)

	tx := bt.NewTx()
	require.NoError(t, tx.From("0000000000000000000000000000000000000000000000000000000000000001", 0, "76a914000000000000000000000000000000000000000088ac", 100))
	require.NoError(t, tx.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 50))

	tx.Version = 2
	// Set type flag without any lock value (time-based with 0 seconds)
	// This is equivalent to SequenceLockTimeTypeFlag | 0
	tx.Inputs[0].SequenceNumber = SequenceLockTimeTypeFlag

	blockHeight := uint32(420000)
	utxoHeights := []uint32{419999}
	utxoMTPs := []uint32{1000000}
	blockMTP := uint32(1000001)

	// minTime = 1000000 + (0 << 9) = 1000000
	// blockMTP = 1000001, so 1000000 < 1000001 ✓
	err := txValidator.ValidateTransaction(tx, blockHeight, utxoHeights, &Options{SkipPolicyChecks: true})
	require.NoError(t, err, "Type flag with zero value should impose no time constraint")
	err = txValidator.ValidateBIP68(tx, blockHeight, utxoHeights, utxoMTPs, blockMTP)
	require.NoError(t, err, "Type flag with zero value should impose no time constraint")
}

// TestSequenceLocks_AllInputsDisabled verifies transaction with all inputs disabled.
func TestSequenceLocks_AllInputsDisabled(t *testing.T) {
	logger := ulogger.TestLogger{}
	tSettings := test.CreateBaseTestSettings(t)
	txValidator := NewTxValidator(logger, tSettings)

	tx := bt.NewTx()
	require.NoError(t, tx.From("0000000000000000000000000000000000000000000000000000000000000001", 0, "76a914000000000000000000000000000000000000000088ac", 30))
	require.NoError(t, tx.From("0000000000000000000000000000000000000000000000000000000000000002", 0, "76a914000000000000000000000000000000000000000088ac", 30))
	require.NoError(t, tx.From("0000000000000000000000000000000000000000000000000000000000000003", 0, "76a914000000000000000000000000000000000000000088ac", 30))
	require.NoError(t, tx.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 70))

	tx.Version = 2
	// All inputs have disable flag set
	tx.Inputs[0].SequenceNumber = SequenceLockTimeDisableFlag
	tx.Inputs[1].SequenceNumber = SequenceLockTimeDisableFlag | 0xFFFF // Disable flag + max value
	tx.Inputs[2].SequenceNumber = SequenceLockTimeDisableFlag | SequenceLockTimeTypeFlag | 1000

	blockHeight := uint32(420000)
	utxoHeights := []uint32{419999, 419999, 419999} // All UTXOs very recent
	utxoMTPs := []uint32{1000000, 1000000, 1000000}
	blockMTP := uint32(1000001) // Very little time passed

	// Should succeed because all inputs are disabled (minHeight and minTime remain 0)
	err := txValidator.ValidateTransaction(tx, blockHeight, utxoHeights, &Options{SkipPolicyChecks: true})
	require.NoError(t, err, "All inputs disabled should bypass all sequence lock checks")
	err = txValidator.ValidateBIP68(tx, blockHeight, utxoHeights, utxoMTPs, blockMTP)
	require.NoError(t, err, "All inputs disabled should bypass all sequence lock checks")
}
