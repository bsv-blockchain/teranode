/*
Package validator implements BSV Blockchain transaction validation functionality.

This package provides comprehensive transaction validation for BSV Blockchain nodes,
including BDK transaction validation, UTXO management, and policy enforcement.

Key features:
  - Transaction validation against Bitcoin consensus rules
  - UTXO spending and creation
  - BDK transaction validation
  - Policy enforcement
  - Block assembly integration
  - Kafka integration for transaction metadata

Usage:

	validator := NewTxValidator(logger, policy, params)
	err := validator.ValidateTransaction(tx, blockHeight, nil)
*/
package validator

import (
	"os"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/go-chaincfg"
	bec "github.com/bsv-blockchain/go-sdk/primitives/ec"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/test/utils/transactions"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type args struct {
	tx          *bt.Tx
	blockHeight uint32
	utxoHeights []uint32
	network     string
}

// 7be4fa421844154ec4105894def768a8bcd80da25792947d585274ce38c07105
var aTx, _ = bt.NewTxFromString("020000000000000000ef023f6c667203b47ce2fed8c8bcc78d764c39da9c0094f1a49074e05f66910e9c44000000006b4c69522102401d5481712745cf7ada12b7251c85ca5f1b8b6c859c7e81b8002a85b0f36d3c21039d8b1e461715ddd4d10806125be8592e6f48fb69e4c31699ce6750da1c9eaeb32103af3b35d4ad547fd1ce102bbd5cce36de2277723796f1b4001ec0ea6a1db6474053aeffffffffa73018250000000017a91413402e079464ec2a85e5a613732c78b0613fcc65873f6c667203b47ce2fed8c8bcc78d764c39da9c0094f1a49074e05f66910e9c44010000006b4c69522102401d5481712745cf7ada12b7251c85ca5f1b8b6c859c7e81b8002a85b0f36d3c21039d8b1e461715ddd4d10806125be8592e6f48fb69e4c31699ce6750da1c9eaeb32103af3b35d4ad547fd1ce102bbd5cce36de2277723796f1b4001ec0ea6a1db6474053aeffffffff34b82f000000000017a91413402e079464ec2a85e5a613732c78b0613fcc65870187e74725000000001976a9141be3d23725148a90807ee6df191bcdfcf083a3b288ac00000000")

var txTests = []struct {
	name    string
	args    args
	wantErr assert.ErrorAssertionFunc
}{
	// {
	// 	name: "TestScriptVerifier - Empty Tx",
	// 	args: args{
	// 		tx:          bt.NewTx(),
	// 		blockHeight: 0,
	// 		utxoHeights: []uint32{},
	// 	},
	// 	wantErr: assert.NoError,
	// },
	{
		name: "TestScriptVerifier - ",
		args: args{
			tx:          aTx,
			blockHeight: 110300,
			utxoHeights: []uint32{631924, 631924},
			network:     "mainnet",
		},
		wantErr: assert.NoError,
	},
}

func TestScriptVerifierGoBDK(t *testing.T) {
	for _, tt := range txTests {
		t.Run(tt.name, func(t *testing.T) {
			params, err := chaincfg.GetChainParams(tt.args.network)
			require.NoError(t, err)

			bdkValidator := newScriptVerifierGoBDK(ulogger.TestLogger{}, settings.NewPolicySettings(), params)
			tt.wantErr(t, bdkValidator.ValidateTransaction(tt.args.tx, tt.args.blockHeight, true, tt.args.utxoHeights))
		})
	}
}

type countingBDKValidator struct {
	calls       int
	blockHeight uint32
	consensus   bool
	utxoHeights []uint32
}

func (c *countingBDKValidator) ValidateTransaction(_ *bt.Tx, blockHeight uint32, consensus bool, utxoHeights []uint32) error {
	c.calls++
	c.blockHeight = blockHeight
	c.consensus = consensus
	c.utxoHeights = append([]uint32(nil), utxoHeights...)
	return nil
}

func TestTxValidatorCallsBDKValidationOnceInValidateTransaction(t *testing.T) {
	tSettings := test.CreateBaseTestSettings(t)
	counter := &countingBDKValidator{}
	txValidator := &TxValidator{
		logger:   ulogger.TestLogger{},
		settings: tSettings,
		bdk:      counter,
	}

	validationOptions := &Options{SkipPolicyChecks: true}
	require.NoError(t, txValidator.ValidateTransaction(aTx, 100, []uint32{99, 99}, validationOptions))
	assert.Equal(t, 1, counter.calls)
	assert.True(t, counter.consensus)
	assert.Equal(t, uint32(100), counter.blockHeight)
	assert.Equal(t, []uint32{99, 99}, counter.utxoHeights)
}

func TestTxValidatorSkipsBDKWhenSkipScriptValidationSet(t *testing.T) {
	tSettings := test.CreateBaseTestSettings(t)
	counter := &countingBDKValidator{}
	txValidator := &TxValidator{
		logger:   ulogger.TestLogger{},
		settings: tSettings,
		bdk:      counter,
	}

	validationOptions := &Options{SkipPolicyChecks: true, SkipScriptValidation: true}
	require.NoError(t, txValidator.ValidateTransaction(aTx, 100, []uint32{99, 99}, validationOptions))
	assert.Equal(t, 0, counter.calls, "BDK ValidateTransaction must not be called when SkipScriptValidation is set")
}

// policy settings tests
func TestMaxTxSizePolicy(t *testing.T) {
	tSettings := test.CreateBaseTestSettings(t)

	tSettings.Policy.MaxTxSizePolicy = 100000 // BDK rejects values below 99999
	tSettings.Policy.RequireStandard = true
	txValidator := NewTxValidator(ulogger.TestLogger{}, tSettings)

	txBytes, err := os.ReadFile("testdata/65cbf31895f6cab997e6c3688b2263808508adc69bcc9054eef5efac6f7895d3.bin.extended")
	require.NoError(t, err)

	tx, err := bt.NewTxFromBytes(txBytes)
	require.NoError(t, err)
	require.Greater(t, tx.Size(), tSettings.Policy.MaxTxSizePolicy)

	utxoHeights := make([]uint32, len(tx.Inputs))
	for i := range utxoHeights {
		utxoHeights[i] = 720898
	}

	err = txValidator.ValidateTransaction(tx, 720899, utxoHeights, &Options{})
	assert.Error(t, err)
	assert.ErrorIs(t, err, errors.ErrTxPolicy)
}
func TestMaxOpsPerScriptPolicy(t *testing.T) {
	// TxID := 9f569c12dfe382504748015791d1994725a7d81d92ab61a6221eadab9f122ece
	testTxHex := "010000000000000000ef011c044c4db32b3da68aa54e3f30c71300db250e0b48ea740bd3897a8ea1a2cc9a020000006b483045022100c6177fa406ecb95817d3cdd3e951696439b23f8e888ef993295aa73046504029022052e75e7bfd060541be406ec64f4fc55e708e55c3871963e95bf9bd34df747ee041210245c6e32afad67f6177b02cfc2878fce2a28e77ad9ecbc6356960c020c592d867ffffffffd4c7a70c000000001976a914296b03a4dd56b3b0fe5706c845f2edff22e84d7388ac0301000000000000001976a914a4429da7462800dedc7b03a4fc77c363b8de40f588ac000000000000000024006a4c2042535620466175636574207c20707573682d7468652d627574746f6e2e617070d2c7a70c000000001976a914296b03a4dd56b3b0fe5706c845f2edff22e84d7388ac00000000"
	testTx, errTx := bt.NewTxFromString(testTxHex)
	assert.NoError(t, errTx)

	testBlockHeight := uint32(886413)
	testUtxoHeights := []uint32{886412}

	tSettings := test.CreateBaseTestSettings(t)
	tSettings.Policy.MaxOpsPerScriptPolicy = 2       // insanely low
	tSettings.Policy.MaxScriptSizePolicy = 100000000 // quite high
	tSettings.ChainCfgParams = &chaincfg.MainNetParams

	txValidator := NewTxValidator(ulogger.TestLogger{}, tSettings)
	err := txValidator.ValidateTransaction(testTx, testBlockHeight, testUtxoHeights, &Options{})
	assert.Error(t, err)
	assert.ErrorIs(t, err, errors.ErrTxPolicy)
}

func TestMaxScriptSizePolicy(t *testing.T) {
	// TxID := 9f569c12dfe382504748015791d1994725a7d81d92ab61a6221eadab9f122ece
	testTxHex := "010000000000000000ef011c044c4db32b3da68aa54e3f30c71300db250e0b48ea740bd3897a8ea1a2cc9a020000006b483045022100c6177fa406ecb95817d3cdd3e951696439b23f8e888ef993295aa73046504029022052e75e7bfd060541be406ec64f4fc55e708e55c3871963e95bf9bd34df747ee041210245c6e32afad67f6177b02cfc2878fce2a28e77ad9ecbc6356960c020c592d867ffffffffd4c7a70c000000001976a914296b03a4dd56b3b0fe5706c845f2edff22e84d7388ac0301000000000000001976a914a4429da7462800dedc7b03a4fc77c363b8de40f588ac000000000000000024006a4c2042535620466175636574207c20707573682d7468652d627574746f6e2e617070d2c7a70c000000001976a914296b03a4dd56b3b0fe5706c845f2edff22e84d7388ac00000000"
	testTx, errTx := bt.NewTxFromString(testTxHex)
	assert.NoError(t, errTx)

	testBlockHeight := uint32(886413)
	testUtxoHeights := []uint32{886412}

	tSettings := test.CreateBaseTestSettings(t)
	tSettings.Policy.MaxScriptSizePolicy = 1 // low
	tSettings.ChainCfgParams = &chaincfg.MainNetParams

	txValidator := NewTxValidator(ulogger.TestLogger{}, tSettings)
	err := txValidator.ValidateTransaction(testTx, testBlockHeight, testUtxoHeights, &Options{})
	assert.Error(t, err)
	assert.ErrorIs(t, err, errors.ErrTxPolicy)
}

func TestMaxPubKeysPerMultiSigPolicy(t *testing.T) {
	// TxID := 52bde64f77c31417721f63a3b9c24d4e8b0be8853c95ec0017bd496471bac432
	testTxHex := "010000000000000000ef03fecf3b19ae909ad1e808164adb055a2f21eacc4cf94fd3d8294fbae0beb2f86f0b0000006a47304402200120f4c1460b9a1063dde41960436e204ab5caa03e6a76533b68fd39157b105d02207b31ba2f093c63453962d9a86c4b99cc30169125ea3aa034db61fcbd084d389e412102293474e46f71eeb59baf53731a17ba28fd60c8ee960a9e529bbba085e82095baffffffff6f000000000000001976a914fee86b11cb1412b5beb2b89d4c47f29a75d4775188ac33c38cd6fdd327f224f25be360255ee4b4315da334ad8cdee380cfd5c9384ee704000000490047304402202666318f36de707a784be991f53b11c9fa92d2fa405e420065fa70837353eaf202203ba5f173a2488321fd7f0e48868931163a3a143d1f1bfe9959c5bd353dd6a5a841ffffffff010000000000000069512102293474e46f71eeb59baf53731a17ba28fd60c8ee960a9e529bbba085e82095ba2102b5697bc3cdf0a72b34614f1932f9759e802f2ae0d7aa54c1efa756f6cf7cb9ce2102cb560e47b1ae629416b4293256443cef4427cd5e5f233a8fd2a92f1912ece4a453ae33c38cd6fdd327f224f25be360255ee4b4315da334ad8cdee380cfd5c9384ee7050000006a47304402201bdbbdefe8caa2e36103fd1be6c22aac1500e52568276d36bdf134137d3ef34a02201d9987c9dd0de7181fc29d7546760b9221bf98a5615b9cfc77c9bd4f156219bb412103c599a0306be976db09cc22d98d1f899fca72500dec7ff2d1c50f593501b74767ffffffff351d0100000000001976a9141f8b7afba54277eab442e9797167971898b191a288ac060000000000000000fd6e03006a034c6f554d65031f8b0800000000000003dd566d6fdb3610fe2fc43e2a185f25cadf3a230d0a2ca993b868872208c8e3d111e2488e24b77603f7b7ef28e7c5c3bacd485f360cb001d3473ef7dc3d0f4fba63151bbd3f10197d78262e32d6d0fa8ec56ade63cb4677ac5f2fb0632399b1cf6cc4a693d79397dbd82663bd6b67d88f9b65ddb391c8588bb7cbaac56ed236370bfaab6f9798b145b5c084f43cccae77330c03a99f68731ea3f5d22372ef749465b041e9184b0d00e8a490d1386e82d74ee4dca20da2742577284ae52146bc548c7210d091bbc1c9dcad53d68b0722892971a89b1a88b2d864ffc94efc1ddd80f1bc9aa5fa4ece7f163c55fba9a929cace7b07d7b484a6eedb663eafead9b6fe5784c9c4e9e1ad3eaba379fb515d5d8bf34fab97c7ebb76d3817d76a7d323d391cb3c7448a5cd256b3aaa6735c83f236f7b6d0bc301af32064698bbc50a0ad953e46c325775af102bdb7bae428f2600d0f068c2bf152b0447b466a0c3c4cd09c30cc8173ce1ee8e8e997c778a0ace742800d32a69a1603f3eed116a2f08581b4c317a5214790f8cefbe8a516de23404453c4921c212418914b27731f413b6b40440d97f20bb6c8beafe548cf7eb0032cdb16eb7eba6ca9a33a7b584fae5c97847bb3b8465c240cda70bac4250e65ef9cba07a536a87747ddaf8bb117ab651d56c7d377bf9d4a84e3aefde5a8adbadb26c154dd9bba69fbab2634ab6dcee8e61da672bf88b98f33fe0a735b26f92e3176be9a577d85ddce6a9d1af0c700345dffa4ac8e0683f32a180df495a5e1da491d8bc2594f6a4a0f421ae5a5b12072654005574a2e9c92655e4a7c54763aa3bbd4f543eb7773a79863db2b7576fcc2b34dda81241c0c34ee6fdb1976cdfc038e5dd7bfb83fbdc9be3557f127ae890b38eaf68359be499e5d478e1fd1a99ee663bdbf9112afe1d23ecd9aedcceabefbb8be9f442699f659b6c242a3b250d2188a96a30a68c11137a74c50c26bd406a5c929627c94d45b298ba06208b905504afd485b7d35d73d6df5d579fed956fbcc927fdf56f9b36df5236db1afacff1b590a2a70fb147cdd8644fcfd7eccf718664925e8ab0f78b8abc4f0e83a49c95fd50157c42063b1696f0615a98b75a09e3cbd1749bef36204aaf0211a5d185b442c5d2c65e4a5c90be1734941a79d0617a5ce214405de78e7a388c07363699687f46274b1f91de9a84363a00b000001000000000000001976a914fee86b11cb1412b5beb2b89d4c47f29a75d4775188ac01000000000000001976a914fee86b11cb1412b5beb2b89d4c47f29a75d4775188ac6f000000000000001976a914fee86b11cb1412b5beb2b89d4c47f29a75d4775188ac010000000000000069512102293474e46f71eeb59baf53731a17ba28fd60c8ee960a9e529bbba085e82095ba2102b5697bc3cdf0a72b34614f1932f9759e802f2ae0d7aa54c1efa756f6cf7cb9ce2102cb560e47b1ae629416b4293256443cef4427cd5e5f233a8fd2a92f1912ece4a453aedd1c0100000000001976a9141f8b7afba54277eab442e9797167971898b191a288ac00000000"
	testTx, errTx := bt.NewTxFromString(testTxHex)
	assert.NoError(t, errTx)

	testBlockHeight := uint32(766657)
	testUtxoHeights := []uint32{766656, 766656, 766656}

	t.Run("low max pubkeys per multisig policy must fail", func(t *testing.T) {
		tSettings := test.CreateBaseTestSettings(t)
		tSettings.Policy.MaxPubKeysPerMultisigPolicy = 2
		tSettings.ChainCfgParams = &chaincfg.MainNetParams

		txValidator := NewTxValidator(ulogger.TestLogger{}, tSettings)
		err := txValidator.ValidateTransaction(testTx, testBlockHeight, testUtxoHeights, &Options{SkipPolicyChecks: false})
		assert.Error(t, err)
		assert.ErrorIs(t, err, errors.ErrTxPolicy)
	})

	t.Run("hight max pubkeys per multisig policy must pass", func(t *testing.T) {
		tSettings := test.CreateBaseTestSettings(t)
		tSettings.Policy.MaxPubKeysPerMultisigPolicy = 3 // low
		tSettings.ChainCfgParams = &chaincfg.MainNetParams

		txValidator := NewTxValidator(ulogger.TestLogger{}, tSettings)
		err := txValidator.ValidateTransaction(testTx, testBlockHeight, testUtxoHeights, &Options{SkipPolicyChecks: false})
		assert.NoError(t, err)
	})
}

func TestMaxStackMemoryUsagePolicy(t *testing.T) {
	// GetMaxStackMemoryUsage:
	// 	if utxo before genesis: return max_uint64
	// 	if utxo after genesis:
	// 	- If SkipPolicyChecks/consensus: return MaxStackMemoryUsageConsensus
	// 	- If not SkipPolicyChecks/consensus: return MaxStackMemoryUsage
	t.Run("set MaxStackMemoryUsageConsensus lower must fail", func(t *testing.T) {
		defer func() {
			if r := recover(); r == nil {
				t.Errorf("Expected panic, but function did not panic")
			}
		}()

		tSettings := test.CreateBaseTestSettings(t)
		tSettings.Policy.MaxStackMemoryUsagePolicy = 2
		tSettings.Policy.MaxStackMemoryUsageConsensus = 1

		txValidator := NewTxValidator(ulogger.TestLogger{}, tSettings)
		assert.Nil(t, txValidator)
	})

	// TxID := cc2f3e03d014f8aeb1b7a68ed69191dd4fca708589a6a89b35200f194c70ea16
	testTxHex := "010000000000000000ef012406151a100b01a5a552058ee09335e3757fe0d889540375ccd71b1f6a12b3df323d00006a47304402201bfe18b51551d6185ebf3062d63465d9f7eafb8256e5b264a638bc13811eddfa0220245a04733acef8bd4819ae9ddd4bcc2bb3693a36501c042dd63ba621b0a40951412102105c84ff3f71aa06f3c232b820addb416f7f965e9c3b05d95f1cecfe4e4e9de7ffffffff01000000000000001976a9141fbb02bf34941726bc83b88fd8ed105698bd3bb088ac0100000000000000000a006a0762697461696c7300000000"
	testTx, errTx := bt.NewTxFromString(testTxHex)
	assert.NoError(t, errTx)

	testBlockHeight := uint32(815351)
	testUtxoHeights := []uint32{815262}

	t.Run("low MaxStackMemoryUsageConsensus must fail", func(t *testing.T) {
		tSettings := test.CreateBaseTestSettings(t)
		tSettings.Policy.MaxStackMemoryUsagePolicy = 1
		tSettings.Policy.MaxStackMemoryUsageConsensus = 271
		tSettings.ChainCfgParams = &chaincfg.MainNetParams

		txValidator := NewTxValidator(ulogger.TestLogger{}, tSettings)
		err := txValidator.ValidateTransaction(testTx, testBlockHeight, testUtxoHeights, &Options{SkipPolicyChecks: true})
		assert.Error(t, err)
		assert.ErrorIs(t, err, errors.ErrTxPolicy)
	})

	t.Run("high MaxStackMemoryUsageConsensus must pass", func(t *testing.T) {
		tSettings := test.CreateBaseTestSettings(t)
		tSettings.Policy.MaxStackMemoryUsagePolicy = 1
		tSettings.Policy.MaxStackMemoryUsageConsensus = 272
		tSettings.ChainCfgParams = &chaincfg.MainNetParams

		txValidator := NewTxValidator(ulogger.TestLogger{}, tSettings)
		err := txValidator.ValidateTransaction(testTx, testBlockHeight, testUtxoHeights, &Options{SkipPolicyChecks: true})
		assert.NoError(t, err)
	})

	t.Run("low MaxStackMemoryUsage must fail", func(t *testing.T) {
		tSettings := test.CreateBaseTestSettings(t)
		tSettings.Policy.MaxStackMemoryUsagePolicy = 271
		tSettings.Policy.MaxStackMemoryUsageConsensus = 1000000
		tSettings.ChainCfgParams = &chaincfg.MainNetParams

		txValidator := NewTxValidator(ulogger.TestLogger{}, tSettings)
		err := txValidator.ValidateTransaction(testTx, testBlockHeight, testUtxoHeights, &Options{SkipPolicyChecks: false})
		assert.Error(t, err)
		assert.ErrorIs(t, err, errors.ErrTxPolicy)
	})

	t.Run("high MaxStackMemoryUsage must pass", func(t *testing.T) {
		tSettings := test.CreateBaseTestSettings(t)
		tSettings.Policy.MaxStackMemoryUsagePolicy = 272
		tSettings.Policy.MaxStackMemoryUsageConsensus = 1000000
		tSettings.ChainCfgParams = &chaincfg.MainNetParams

		txValidator := NewTxValidator(ulogger.TestLogger{}, tSettings)
		err := txValidator.ValidateTransaction(testTx, testBlockHeight, testUtxoHeights, &Options{SkipPolicyChecks: false})
		assert.NoError(t, err)
	})
}

// MaxScriptNumLengthPolicy :
// 	if utxo before genesis : return 4
// 	if utxo  after genesis :
// 	- If     SkipPolicyChecks/consensus
// 	           - If before chronicle : 750000
// 	           - If  after chronicle : max_uint64
// 	- If not SkipPolicyChecks/consensus : return custom MaxScriptNumLength

// Since the policy is triggered if an only if the transaction is non standard and have big number arithmetic operations
// We can not find it on chain, we created a simple transaction with append to the scriptPubkey, it becomes non standard
//
//	scriptPubkey : OP_DUP OP_HASH160 20 <data20> OP_EQUALVERIFY OP_CHECKSIG <BigNum1> <BigNum2> OP_NUMEQUALVERIFY
//
// The two big number a identical array of length 6 with all values 1
// So
//
//	MaxScriptNumLengthPolicy = 5 --> fail
//	MaxScriptNumLengthPolicy = 6 --> pass
//
// TxID := no need
func TestMaxScriptNumLengthPolicy(t *testing.T) {
	testTxHex := "010000000000000000ef01905d0e9cfb36fb99b2e1cb0c2c6cff609c565a0e9a3dd27a07ecaadf2b35105c000000008a47304402200384b288c18d0c4a65139db537d7e1b89abb137ad38da930066e740bfe66f03a02202826f5ef0e2e970db785aecc08d74976ed99b1026ef968aa074873d42d472f5b4141040b4c866585dd868a9d62348a9cd008d6a312937048fff31670e7e920cfc7a7447b5f0bba9e01e6fe4735c8383e6e7a3347a0fd72381b8f797a19f694054e5a69ffffffff40420f00000000002876a914ff197b14e502ab41f3bc8ccb48c4abac9eab35bc88ac06010101010101060101010101019d0140420f00000000000000000000"
	testTx, errTx := bt.NewTxFromString(testTxHex)
	assert.NoError(t, errTx)

	// The transaction above is a real mainnet transaction but its UTXO locking script is
	// non-standard: it appends two 6-byte number pushes and OP_NUMEQUALVERIFY after a
	// standard P2PKH, specifically to exercise the MaxScriptNumLength policy limit.
	//
	// BDK added a standardness check during policy-mode validation
	// (consensus=false). On mainnet, chainParams.RequireStandard()=true, so BDK runs
	// IsStandardTx/IsInputStandard before executing the script. The non-standard UTXO
	// script causes that check to fail with SCRIPT_ERR_UNKNOWN_ERROR, masking the
	// MaxScriptNumLength policy violation we want to test.
	//
	// On testnet, chainParams.RequireStandard()=false, so the standardness check is
	// skipped entirely and BDK proceeds directly to script execution where the policy
	// limit is enforced. Heights 1400000/1399999 place the transaction in testnet's
	// Post-Genesis era (genesis=1344302, chronicle=1713168), matching the same protocol
	// era that the original mainnet heights (820540/820539) represented.
	testBlockHeight := uint32(1400000)
	testUtxoHeights := []uint32{1399999}

	t.Run("low MaxScriptNumLengthPolicy must fail", func(t *testing.T) {
		tSettings := test.CreateBaseTestSettings(t)
		tSettings.Policy.MaxScriptNumLengthPolicy = 5
		tSettings.Policy.MinMiningTxFee = 0
		tSettings.ChainCfgParams = &chaincfg.TestNetParams

		txValidator := NewTxValidator(ulogger.TestLogger{}, tSettings)
		err := txValidator.ValidateTransaction(testTx, testBlockHeight, testUtxoHeights, &Options{SkipPolicyChecks: false})
		assert.Error(t, err)
		assert.ErrorIs(t, err, errors.ErrTxPolicy)
	})

	t.Run("high MaxScriptNumLengthPolicy must pass", func(t *testing.T) {
		tSettings := test.CreateBaseTestSettings(t)
		tSettings.Policy.MaxScriptNumLengthPolicy = 6
		tSettings.Policy.MinMiningTxFee = 0
		tSettings.ChainCfgParams = &chaincfg.TestNetParams

		txValidator := NewTxValidator(ulogger.TestLogger{}, tSettings)
		err := txValidator.ValidateTransaction(testTx, testBlockHeight, testUtxoHeights, &Options{SkipPolicyChecks: false})
		assert.NoError(t, err)
	})
}

func TestMaxTxSigopsCountsPolicy(t *testing.T) {
	t.Skip("Skipping until a focused BDK ValidateTransaction sigops-policy fixture is added")

	// TxID := 9f569c12dfe382504748015791d1994725a7d81d92ab61a6221eadab9f122ece
	testTxHex := "010000000000000000ef011c044c4db32b3da68aa54e3f30c71300db250e0b48ea740bd3897a8ea1a2cc9a020000006b483045022100c6177fa406ecb95817d3cdd3e951696439b23f8e888ef993295aa73046504029022052e75e7bfd060541be406ec64f4fc55e708e55c3871963e95bf9bd34df747ee041210245c6e32afad67f6177b02cfc2878fce2a28e77ad9ecbc6356960c020c592d867ffffffffd4c7a70c000000001976a914296b03a4dd56b3b0fe5706c845f2edff22e84d7388ac0301000000000000001976a914a4429da7462800dedc7b03a4fc77c363b8de40f588ac000000000000000024006a4c2042535620466175636574207c20707573682d7468652d627574746f6e2e617070d2c7a70c000000001976a914296b03a4dd56b3b0fe5706c845f2edff22e84d7388ac00000000"
	testTx, errTx := bt.NewTxFromString(testTxHex)
	assert.NoError(t, errTx)

	testBlockHeight := uint32(886413)
	testUtxoHeights := []uint32{886412}

	tSettings := test.CreateBaseTestSettings(t)
	tSettings.Policy.MaxTxSigopsCountsPolicy = 1 // low
	tSettings.ChainCfgParams = &chaincfg.MainNetParams

	txValidator := NewTxValidator(ulogger.TestLogger{}, tSettings)
	err := txValidator.ValidateTransaction(testTx, testBlockHeight, testUtxoHeights, &Options{})
	assert.Error(t, err)
	assert.ErrorIs(t, err, errors.ErrTxPolicy)
}

func TestCheckP2SHOutput(t *testing.T) {
	tSettings := test.CreateBaseTestSettings(t)
	tSettings.ChainCfgParams.RequireStandard = true
	// Disable BIP68 for this test (set CSVHeight above test heights)
	// This test is about P2SH validation, not BIP68
	tSettings.ChainCfgParams.CSVHeight = 1000000

	txValidator := NewTxValidator(ulogger.TestLogger{}, tSettings)

	// See https://github.com/bitcoin-sv/teranode/issues/4333
	txP2SH, err := bt.NewTxFromString("020000000000000000ef01e0d8bc7aae870d67eaf3021492735637ddae403feb7914fb739a53872a82d301000000006a473044022041215b9ac965ce93684340d86d74df5ccf2d0910f36173a9d691e8405b37fd400220300ab0376d9d75542eaaffb4fe1eead267f0ac537ae13a4349506274978066f7412103afe4a8eb7f3f69757235bb8db804a01156af9d1cace07af534ca9be7f4928a5effffffffacc88203000000001976a9140533653ad7e12be8ee8151bc586f04bf859ae4d788ac0267307e03000000001976a9140533653ad7e12be8ee8151bc586f04bf859ae4d788ace09304000000000017a914496164f9f2e373628c5cc0a5895d995aaf3bec658700000000")
	require.NoError(t, err)

	err = txValidator.ValidateTransaction(txP2SH, 1_000_001, []uint32{1_000_000}, &Options{SkipPolicyChecks: true})
	require.Error(t, err)
	assert.ErrorIs(t, err, errors.ErrTxInvalid)
	assert.NotErrorIs(t, err, errors.ErrTxPolicy)
	assert.Contains(t, err.Error(), "bad-txns-vout-p2sh")
}

func TestSubErrorTxInvalid(t *testing.T) {
	rootErr := errors.NewUnknownError("a returned root error")

	t.Run("simple errors", func(t *testing.T) {
		newPolicyError := errors.NewTxPolicyError("An error by policy check", rootErr)
		isPolicyError := errors.Is(newPolicyError, errors.ErrTxPolicy)
		assert.True(t, isPolicyError)
		assert.ErrorIs(t, newPolicyError, errors.ErrTxPolicy) // std check

		// Policy error is different than the invalid tx error
		assert.False(t, errors.Is(errors.ErrTxPolicy, errors.ErrTxInvalid))

		assert.False(t, errors.Is(newPolicyError, errors.ErrTxInvalid))
	})

	t.Run("combined ErrTxInvalid NewTxPolicyError", func(t *testing.T) {
		policyError := errors.NewTxPolicyError("An error by policy check", rootErr)
		combinedError := errors.NewTxInvalidError("Final error triggered by policy check", policyError)

		assert.ErrorIs(t, combinedError, errors.ErrTxPolicy)  // std check
		assert.ErrorIs(t, combinedError, errors.ErrTxInvalid) // std check

		assert.True(t, errors.Is(combinedError, errors.ErrTxPolicy))
		assert.True(t, errors.Is(combinedError, errors.ErrTxInvalid))
	})
}

func TestZeroSatoshiOutputRequiresOpFalseOpReturn(t *testing.T) {
	t.Skip("wip - will be fixed with later pr")
	tSettings := test.CreateBaseTestSettings(t)

	privKey, err := bec.NewPrivateKey()
	require.NoError(t, err)

	pubKey := privKey.PubKey()

	parentTx := transactions.Create(t,
		transactions.WithCoinbaseData(100, "/Test miner/"),
		transactions.WithP2PKHOutputs(1, 100000, privKey.PubKey()),
	)

	// Create a child transaction that spends the parent and creates a zero-satoshi output with OP_RETURN (not OP_FALSE OP_RETURN)
	// Standard OP_RETURN script: 0x6a <data>
	customOpReturn := []byte{0x6a, 0x04, 0xde, 0xad, 0xbe, 0xef}
	childTx := transactions.Create(t,
		transactions.WithPrivateKey(privKey),
		transactions.WithInput(parentTx, 0, privKey),
		transactions.WithOutput(0, bscript.NewFromBytes(customOpReturn)),
		transactions.WithP2PKHOutputs(1, 900, pubKey),
	)

	t.Run("zero-satoshi output with OP_RETURN (not OP_FALSE OP_RETURN) is not rejected when genesis activation height is not reached", func(t *testing.T) {
		txValidator := NewTxValidator(ulogger.TestLogger{}, tSettings)
		err := txValidator.ValidateTransaction(childTx, tSettings.ChainCfgParams.GenesisActivationHeight-1, nil, &Options{})
		assert.NoError(t, err)
	})

	t.Run("zero-satoshi output with OP_RETURN (not OP_FALSE OP_RETURN) is not rejected at genesis activation height but is rejected after", func(t *testing.T) {
		tSettings.ChainCfgParams.RequireStandard = true

		txValidator := NewTxValidator(ulogger.TestLogger{}, tSettings)

		// At Genesis activation height, should not be rejected
		err := txValidator.ValidateTransaction(childTx, tSettings.ChainCfgParams.GenesisActivationHeight, nil, &Options{})
		assert.NoError(t, err)

		// After Genesis activation height, should be rejected
		err = txValidator.ValidateTransaction(childTx, tSettings.ChainCfgParams.GenesisActivationHeight+1, nil, &Options{})
		if assert.Error(t, err) {
			assert.Contains(t, err.Error(), "zero-satoshi outputs require 'OP_FALSE OP_RETURN' prefix")
		}
	})

	t.Run("zero-satoshi output with OP_RETURN (not OP_FALSE OP_RETURN) is not rejected at genesis activation height when require standard is false", func(t *testing.T) {
		tSettings.ChainCfgParams.RequireStandard = false

		txValidator := NewTxValidator(ulogger.TestLogger{}, tSettings)

		err := txValidator.ValidateTransaction(childTx, tSettings.ChainCfgParams.GenesisActivationHeight, nil, &Options{})
		assert.NoError(t, err)
	})

	t.Run("zero-satoshi P2PKH output validation with different policy settings", func(t *testing.T) {
		tSettings.ChainCfgParams.RequireStandard = true

		// Create a transaction with a zero-satoshi P2PKH output
		zeroSatoshiP2PKHTx := transactions.Create(t,
			transactions.WithPrivateKey(privKey),
			transactions.WithInput(parentTx, 0, privKey),
			transactions.WithP2PKHOutputs(1, 0, pubKey),   // 0 satoshis P2PKH output
			transactions.WithP2PKHOutputs(1, 900, pubKey), // change output
		)

		txValidator := NewTxValidator(ulogger.TestLogger{}, tSettings)

		// Test after Genesis activation (block 620538+)
		afterGenesisHeight := tSettings.ChainCfgParams.GenesisActivationHeight + 1

		// Case 1: Mempool validation (SkipPolicyChecks = false) - should REJECT
		err := txValidator.ValidateTransaction(zeroSatoshiP2PKHTx, afterGenesisHeight, nil, &Options{SkipPolicyChecks: false})
		if assert.Error(t, err, "Expected error for mempool validation with zero-satoshi P2PKH output") {
			assert.Contains(t, err.Error(), "zero-satoshi outputs require 'OP_FALSE OP_RETURN' prefix")
		}

		// Case 2: Block validation (SkipPolicyChecks = true) - should ACCEPT
		err = txValidator.ValidateTransaction(zeroSatoshiP2PKHTx, afterGenesisHeight, nil, &Options{SkipPolicyChecks: true})
		assert.NoError(t, err, "Expected no error for block validation with zero-satoshi P2PKH output")

		// Case 3: Before Genesis activation - should ACCEPT regardless of SkipPolicyChecks
		beforeGenesisHeight := tSettings.ChainCfgParams.GenesisActivationHeight - 1

		err = txValidator.ValidateTransaction(zeroSatoshiP2PKHTx, beforeGenesisHeight, nil, &Options{SkipPolicyChecks: false})
		assert.NoError(t, err, "Expected no error before Genesis activation even with policy checks")

		err = txValidator.ValidateTransaction(zeroSatoshiP2PKHTx, beforeGenesisHeight, nil, &Options{SkipPolicyChecks: true})
		assert.NoError(t, err, "Expected no error before Genesis activation without policy checks")

		// Case 4: At Genesis activation height (block 620538) - should ACCEPT
		// (special case: transactions in the Genesis block itself were created before the rules)
		err = txValidator.ValidateTransaction(zeroSatoshiP2PKHTx, tSettings.ChainCfgParams.GenesisActivationHeight, nil, &Options{SkipPolicyChecks: false})
		assert.NoError(t, err, "Expected no error at Genesis activation height with policy checks")

		err = txValidator.ValidateTransaction(zeroSatoshiP2PKHTx, tSettings.ChainCfgParams.GenesisActivationHeight, nil, &Options{SkipPolicyChecks: true})
		assert.NoError(t, err, "Expected no error at Genesis activation height without policy checks")
	})

	t.Run("zero-satoshi OP_FALSE OP_RETURN output is always accepted", func(t *testing.T) {
		tSettings.ChainCfgParams.RequireStandard = true

		// Create OP_FALSE OP_RETURN script
		opFalseOpReturn := bscript.NewFromBytes([]byte{0x00, 0x6a}) // OP_FALSE OP_RETURN

		// Create a transaction with a zero-satoshi OP_FALSE OP_RETURN output
		zeroSatoshiOpFalseReturnTx := transactions.Create(t,
			transactions.WithPrivateKey(privKey),
			transactions.WithInput(parentTx, 0, privKey),
			transactions.WithOutput(0, opFalseOpReturn),   // 0 satoshis OP_FALSE OP_RETURN output
			transactions.WithP2PKHOutputs(1, 900, pubKey), // change output
		)

		txValidator := NewTxValidator(ulogger.TestLogger{}, tSettings)
		afterGenesisHeight := tSettings.ChainCfgParams.GenesisActivationHeight + 1

		// Should be accepted for both mempool and block validation
		err := txValidator.ValidateTransaction(zeroSatoshiOpFalseReturnTx, afterGenesisHeight, nil, &Options{SkipPolicyChecks: false})
		assert.NoError(t, err, "OP_FALSE OP_RETURN should be accepted for mempool validation")

		err = txValidator.ValidateTransaction(zeroSatoshiOpFalseReturnTx, afterGenesisHeight, nil, &Options{SkipPolicyChecks: true})
		assert.NoError(t, err, "OP_FALSE OP_RETURN should be accepted for block validation")
	})
}

func TestTx5f37c7a38b5e0bc177a4c353481f30c6de1bc46db534019846d7bc829f58254a(t *testing.T) {
	tSettings := test.CreateBaseTestSettings(t)
	tSettings.ChainCfgParams = &chaincfg.MainNetParams

	tx, err := bt.NewTxFromString("0100000001a8596c1f7485c86b276d3b28c5172abee5e690af29030e0823eb3c3204ae0495000000008b4830450221008c541fe8c778400d3e9c22520b40978937352ccc2b9cf811e64969bd818f967602204f5d373ace66845178071583ced5a67b5ac8e063459084c70cb2b4dc6356f073414104af1b5109b422ca6440f205f01d8f6956b71110a1a71db190f8773f432ecbc3fd7f62a7fec01a47f6bf2adc89625e0f2fb31c70f7b9b7b1b882168e2c3ff03412ffffffff0200000000000000001976a91406fb1d4b212d8d4a576bc3c15cefc6232f198e6c88ace70b0000000000001976a91406fb1d4b212d8d4a576bc3c15cefc6232f198e6c88ac00000000")
	require.NoError(t, err)

	txValidator := NewTxValidator(ulogger.TestLogger{}, tSettings)
	require.NoError(t, err)

	err = txValidator.ValidateTransaction(tx, 687064, []uint32{687002}, &Options{
		SkipPolicyChecks: false,
	})
	require.Error(t, err)

	err = txValidator.ValidateTransaction(tx, 687064, []uint32{687002}, &Options{
		SkipPolicyChecks: true,
	})
	require.Error(t, err)
	assert.ErrorIs(t, err, errors.ErrTxInvalid)
}

func TestMaxCoinsViewCacheSize(t *testing.T) {
	// TxID := 9f569c12dfe382504748015791d1994725a7d81d92ab61a6221eadab9f122ece
	testTxHex := "010000000000000000ef011c044c4db32b3da68aa54e3f30c71300db250e0b48ea740bd3897a8ea1a2cc9a020000006b483045022100c6177fa406ecb95817d3cdd3e951696439b23f8e888ef993295aa73046504029022052e75e7bfd060541be406ec64f4fc55e708e55c3871963e95bf9bd34df747ee041210245c6e32afad67f6177b02cfc2878fce2a28e77ad9ecbc6356960c020c592d867ffffffffd4c7a70c000000001976a914296b03a4dd56b3b0fe5706c845f2edff22e84d7388ac0301000000000000001976a914a4429da7462800dedc7b03a4fc77c363b8de40f588ac000000000000000024006a4c2042535620466175636574207c20707573682d7468652d627574746f6e2e617070d2c7a70c000000001976a914296b03a4dd56b3b0fe5706c845f2edff22e84d7388ac00000000"
	testTx, errTx := bt.NewTxFromString(testTxHex)
	require.NoError(t, errTx)

	testBlockHeight := uint32(886413)
	testUtxoHeights := []uint32{886412}

	// Calculate N: the actual accumulated previous tx script size
	var accumulatedScriptSize uint64
	for _, input := range testTx.Inputs {
		accumulatedScriptSize += uint64(len(*input.PreviousTxScript))
	}

	t.Run("MaxCoinsViewCacheSize zero - should pass", func(t *testing.T) {
		tSettings := test.CreateBaseTestSettings(t)
		tSettings.Policy.MaxCoinsViewCacheSize = 0 // disabled
		tSettings.ChainCfgParams = &chaincfg.MainNetParams

		txValidator := NewTxValidator(ulogger.TestLogger{}, tSettings)
		err := txValidator.ValidateTransaction(testTx, testBlockHeight, testUtxoHeights, &Options{})
		require.NoError(t, err)
	})

	t.Run("MaxCoinsViewCacheSize N - should pass", func(t *testing.T) {
		tSettings := test.CreateBaseTestSettings(t)
		tSettings.Policy.MaxCoinsViewCacheSize = accumulatedScriptSize // exactly at limit
		tSettings.ChainCfgParams = &chaincfg.MainNetParams

		txValidator := NewTxValidator(ulogger.TestLogger{}, tSettings)
		err := txValidator.ValidateTransaction(testTx, testBlockHeight, testUtxoHeights, &Options{})
		require.NoError(t, err)
	})

	t.Run("MaxCoinsViewCacheSize N-1 - should fail", func(t *testing.T) {
		tSettings := test.CreateBaseTestSettings(t)
		tSettings.Policy.MaxCoinsViewCacheSize = accumulatedScriptSize - 1 // below limit
		tSettings.ChainCfgParams = &chaincfg.MainNetParams

		txValidator := NewTxValidator(ulogger.TestLogger{}, tSettings)
		err := txValidator.ValidateTransaction(testTx, testBlockHeight, testUtxoHeights, &Options{})
		require.Error(t, err)
		require.Contains(t, err.Error(), "bad-txns-inputs-too-large")
	})

	t.Run("SkipPolicyChecks true - should pass even with low limit", func(t *testing.T) {
		tSettings := test.CreateBaseTestSettings(t)
		tSettings.Policy.MaxCoinsViewCacheSize = 1 // very low limit
		tSettings.ChainCfgParams = &chaincfg.MainNetParams

		txValidator := NewTxValidator(ulogger.TestLogger{}, tSettings)
		err := txValidator.ValidateTransaction(testTx, testBlockHeight, testUtxoHeights, &Options{
			SkipPolicyChecks: true,
		})
		require.NoError(t, err)
	})
}
