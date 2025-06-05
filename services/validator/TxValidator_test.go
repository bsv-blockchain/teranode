/*
Package validator implements Bitcoin SV transaction validation functionality.

This package provides comprehensive transaction validation for Bitcoin SV nodes,
including script verification, UTXO management, and policy enforcement. It supports
multiple script interpreters (GoBT, GoSDK, GoBDK) and implements the full Bitcoin
transaction validation ruleset.

Key features:
  - Transaction validation against Bitcoin consensus rules
  - UTXO spending and creation
  - Script verification using multiple interpreters
  - Policy enforcement
  - Block assembly integration
  - Kafka integration for transaction metadata

Usage:

	validator := NewTxValidator(logger, policy, params)
	err := validator.ValidateTransaction(tx, blockHeight)
*/
package validator

import (
	"context"
	"fmt"
	"os"
	"testing"

	"github.com/bitcoin-sv/teranode/errors"
	"github.com/bitcoin-sv/teranode/model"
	"github.com/bitcoin-sv/teranode/pkg/go-chaincfg"
	"github.com/bitcoin-sv/teranode/settings"
	"github.com/bitcoin-sv/teranode/ulogger"
	"github.com/bitcoin-sv/teranode/util/test"
	"github.com/libsv/go-bk/wif"
	"github.com/libsv/go-bt/v2"
	"github.com/libsv/go-bt/v2/unlocker"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type args struct {
	tx          *bt.Tx
	blockHeight uint32
	utxoHeights []uint32
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
		},
		wantErr: assert.NoError,
	},
}

func TestScriptVerifierGoBt(t *testing.T) {
	for _, tt := range txTests {
		t.Run(tt.name, func(t *testing.T) {
			scriptInterpreter := newScriptVerifierGoBt(ulogger.TestLogger{}, settings.NewPolicySettings(), chaincfg.GetChainParamsFromConfig())
			tt.wantErr(t, scriptInterpreter.VerifyScript(tt.args.tx, tt.args.blockHeight, true, tt.args.utxoHeights), fmt.Sprintf("scriptVerifierGoBt(%v, %v)", tt.args.tx, tt.args.blockHeight))
		})
	}
}

func TestScriptVerifierGoSDK(t *testing.T) {
	for _, tt := range txTests {
		t.Run(tt.name, func(t *testing.T) {
			scriptInterpreter := newScriptVerifierGoSDK(ulogger.TestLogger{}, settings.NewPolicySettings(), chaincfg.GetChainParamsFromConfig())
			tt.wantErr(t, scriptInterpreter.VerifyScript(tt.args.tx, tt.args.blockHeight, true, tt.args.utxoHeights), fmt.Sprintf("scriptVerifierGoSDK(%v, %v)", tt.args.tx, tt.args.blockHeight))
		})
	}
}

func TestScriptVerifierGoBDK(t *testing.T) {
	for _, tt := range txTests {
		t.Run(tt.name, func(t *testing.T) {
			scriptInterpreter := newScriptVerifierGoBDK(ulogger.TestLogger{}, settings.NewPolicySettings(), chaincfg.GetChainParamsFromConfig())
			tt.wantErr(t, scriptInterpreter.VerifyScript(tt.args.tx, tt.args.blockHeight, true, tt.args.utxoHeights), fmt.Sprintf("scriptVerifierGoBDK(%v, %v)", tt.args.tx, tt.args.blockHeight))
		})
	}
}

func Test_Tx(t *testing.T) {
	f1, err := os.Open("testdata/65cbf31895f6cab997e6c3688b2263808508adc69bcc9054eef5efac6f7895d3.bin")
	require.NoError(t, err)
	defer f1.Close()

	f2, err := os.Open("testdata/65cbf31895f6cab997e6c3688b2263808508adc69bcc9054eef5efac6f7895d3.bin.extended")
	require.NoError(t, err)
	defer f2.Close()

	var tx bt.Tx
	_, err = tx.ReadFrom(f1)
	require.NoError(t, err)

	var txE bt.Tx
	_, err = txE.ReadFrom(f2)
	require.NoError(t, err)

	assert.Equal(t, "65cbf31895f6cab997e6c3688b2263808508adc69bcc9054eef5efac6f7895d3", tx.TxID())
	assert.Equal(t, tx.TxID(), txE.TxID())

	scriptInterpreter := newScriptVerifierGoSDK(ulogger.TestLogger{}, settings.NewPolicySettings(), chaincfg.GetChainParamsFromConfig())

	err = scriptInterpreter.VerifyScript(&tx, 720899, true, []uint32{720899})
	require.Error(t, err)

	err = scriptInterpreter.VerifyScript(&txE, 729000, true, []uint32{729000})
	require.NoError(t, err)
}

func TestGoBt2GoSDKTransaction(t *testing.T) {
	t.Run("TestGoBt2GoSDKTransaction", func(t *testing.T) {
		largeTxHex, err := os.ReadFile("./testdata/9a87105441107db10d3e4cf2146022f754241b3b93c39539e2ce882a398e7d69.bin.extended")
		require.NoError(t, err)

		largeTx, err := bt.NewTxFromBytes(largeTxHex)
		require.NoError(t, err)

		txBytes := largeTx.Bytes()

		sdkTx := goBt2GoSDKTransaction(largeTx)

		assert.Equal(t, largeTx.TxID(), sdkTx.TxID().String())
		assert.Equal(t, txBytes, sdkTx.Bytes())
	})
}

func BenchmarkVerifyTransactionGoBt(b *testing.B) {
	scriptInterpreter := newScriptVerifierGoBt(ulogger.TestLogger{}, settings.NewPolicySettings(), chaincfg.GetChainParamsFromConfig())
	txHex, err := os.ReadFile("./testdata/f65ec8dcc934c8118f3c65f86083c2b7c28dad0579becd0cfe87243e576d9ae9")
	require.NoError(b, err)
	tx, err := bt.NewTxFromBytes(txHex)
	require.NoError(b, err)

	b.Run("BenchmarkCheckScripts", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_ = scriptInterpreter.VerifyScript(tx, 740975, true, []uint32{740975})
		}
	})
}

func BenchmarkVerifyTransactionGoSDK(b *testing.B) {
	scriptInterpreter := newScriptVerifierGoSDK(ulogger.TestLogger{}, settings.NewPolicySettings(), chaincfg.GetChainParamsFromConfig())
	txHex, err := os.ReadFile("./testdata/f65ec8dcc934c8118f3c65f86083c2b7c28dad0579becd0cfe87243e576d9ae9.bin")
	require.NoError(b, err)
	tx, err := bt.NewTxFromBytes(txHex)
	require.NoError(b, err)

	b.Run("BenchmarkCheckScripts", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_ = scriptInterpreter.VerifyScript(tx, 740975, true, []uint32{740975})
		}
	})
}

func BenchmarkVerifyTransactionGoSDK2(b *testing.B) {
	scriptInterpreter := newScriptVerifierGoSDK(ulogger.TestLogger{}, settings.NewPolicySettings(), chaincfg.GetChainParamsFromConfig())
	txHex, err := os.ReadFile("./testdata/f568c66631de7b5842ebae84594cee00f7864132828997d09441fc2a937e9fab.hex")
	require.NoError(b, err)
	tx, err := bt.NewTxFromString(string(txHex))
	require.NoError(b, err)

	b.Run("BenchmarkCheckScripts", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_ = scriptInterpreter.VerifyScript(tx, 740975, true, []uint32{740975})
		}
	})
}

// policy settings tests
func TestMaxTxSizePolicy(t *testing.T) {
	tSettings := test.CreateBaseTestSettings()

	tSettings.Policy.MaxTxSizePolicy = 10 // insanely low
	txValidator := NewTxValidator(ulogger.TestLogger{}, tSettings)

	err := txValidator.ValidateTransaction(aTx, 10000000, &Options{})
	assert.Error(t, err)
	assert.ErrorIs(t, err, errors.New(errors.ERR_TX_INVALID, "transaction size in bytes is greater than max tx size policy 10"))
}
func TestMaxOpsPerScriptPolicy(t *testing.T) {

	// TxID := 9f569c12dfe382504748015791d1994725a7d81d92ab61a6221eadab9f122ece
	testTxHex := "010000000000000000ef011c044c4db32b3da68aa54e3f30c71300db250e0b48ea740bd3897a8ea1a2cc9a020000006b483045022100c6177fa406ecb95817d3cdd3e951696439b23f8e888ef993295aa73046504029022052e75e7bfd060541be406ec64f4fc55e708e55c3871963e95bf9bd34df747ee041210245c6e32afad67f6177b02cfc2878fce2a28e77ad9ecbc6356960c020c592d867ffffffffd4c7a70c000000001976a914296b03a4dd56b3b0fe5706c845f2edff22e84d7388ac0301000000000000001976a914a4429da7462800dedc7b03a4fc77c363b8de40f588ac000000000000000024006a4c2042535620466175636574207c20707573682d7468652d627574746f6e2e617070d2c7a70c000000001976a914296b03a4dd56b3b0fe5706c845f2edff22e84d7388ac00000000"
	testTx, errTx := bt.NewTxFromString(testTxHex)
	assert.NoError(t, errTx)

	testBlockHeight := uint32(886413)
	testUtxoHeights := []uint32{886412}

	tSettings := test.CreateBaseTestSettings()
	tSettings.Policy.MaxOpsPerScriptPolicy = 2       // insanely low
	tSettings.Policy.MaxScriptSizePolicy = 100000000 // quite high
	tSettings.ChainCfgParams = &chaincfg.MainNetParams

	txValidator := NewTxValidator(ulogger.TestLogger{}, tSettings)
	err := txValidator.ValidateTransaction(testTx, testBlockHeight, &Options{})
	assert.NoError(t, err)

	err = txValidator.ValidateTransactionScripts(testTx, testBlockHeight, testUtxoHeights, &Options{})

	assert.Error(t, err)
	assert.ErrorIs(t, err, errors.New(errors.ERR_TX_INVALID, "max ops per script policy limit exceeded"))
}

func TestMaxScriptSizePolicy(t *testing.T) {
	// TxID := 9f569c12dfe382504748015791d1994725a7d81d92ab61a6221eadab9f122ece
	testTxHex := "010000000000000000ef011c044c4db32b3da68aa54e3f30c71300db250e0b48ea740bd3897a8ea1a2cc9a020000006b483045022100c6177fa406ecb95817d3cdd3e951696439b23f8e888ef993295aa73046504029022052e75e7bfd060541be406ec64f4fc55e708e55c3871963e95bf9bd34df747ee041210245c6e32afad67f6177b02cfc2878fce2a28e77ad9ecbc6356960c020c592d867ffffffffd4c7a70c000000001976a914296b03a4dd56b3b0fe5706c845f2edff22e84d7388ac0301000000000000001976a914a4429da7462800dedc7b03a4fc77c363b8de40f588ac000000000000000024006a4c2042535620466175636574207c20707573682d7468652d627574746f6e2e617070d2c7a70c000000001976a914296b03a4dd56b3b0fe5706c845f2edff22e84d7388ac00000000"
	testTx, errTx := bt.NewTxFromString(testTxHex)
	assert.NoError(t, errTx)

	testBlockHeight := uint32(886413)
	testUtxoHeights := []uint32{886412}

	tSettings := test.CreateBaseTestSettings()
	tSettings.Policy.MaxScriptSizePolicy = 1 // low
	tSettings.ChainCfgParams = &chaincfg.MainNetParams

	txValidator := NewTxValidator(ulogger.TestLogger{}, tSettings)
	err := txValidator.ValidateTransaction(testTx, testBlockHeight, &Options{})
	assert.NoError(t, err)

	err = txValidator.ValidateTransactionScripts(testTx, testBlockHeight, testUtxoHeights, &Options{})

	assert.Error(t, err)
	assert.ErrorIs(t, err, errors.New(errors.ERR_TX_INVALID, "max ops per script policy limit exceeded"))
}

func TestMaxPubKeysPerMultiSigPolicy(t *testing.T) {
	// TxID := 52bde64f77c31417721f63a3b9c24d4e8b0be8853c95ec0017bd496471bac432
	testTxHex := "010000000000000000ef03fecf3b19ae909ad1e808164adb055a2f21eacc4cf94fd3d8294fbae0beb2f86f0b0000006a47304402200120f4c1460b9a1063dde41960436e204ab5caa03e6a76533b68fd39157b105d02207b31ba2f093c63453962d9a86c4b99cc30169125ea3aa034db61fcbd084d389e412102293474e46f71eeb59baf53731a17ba28fd60c8ee960a9e529bbba085e82095baffffffff6f000000000000001976a914fee86b11cb1412b5beb2b89d4c47f29a75d4775188ac33c38cd6fdd327f224f25be360255ee4b4315da334ad8cdee380cfd5c9384ee704000000490047304402202666318f36de707a784be991f53b11c9fa92d2fa405e420065fa70837353eaf202203ba5f173a2488321fd7f0e48868931163a3a143d1f1bfe9959c5bd353dd6a5a841ffffffff010000000000000069512102293474e46f71eeb59baf53731a17ba28fd60c8ee960a9e529bbba085e82095ba2102b5697bc3cdf0a72b34614f1932f9759e802f2ae0d7aa54c1efa756f6cf7cb9ce2102cb560e47b1ae629416b4293256443cef4427cd5e5f233a8fd2a92f1912ece4a453ae33c38cd6fdd327f224f25be360255ee4b4315da334ad8cdee380cfd5c9384ee7050000006a47304402201bdbbdefe8caa2e36103fd1be6c22aac1500e52568276d36bdf134137d3ef34a02201d9987c9dd0de7181fc29d7546760b9221bf98a5615b9cfc77c9bd4f156219bb412103c599a0306be976db09cc22d98d1f899fca72500dec7ff2d1c50f593501b74767ffffffff351d0100000000001976a9141f8b7afba54277eab442e9797167971898b191a288ac060000000000000000fd6e03006a034c6f554d65031f8b0800000000000003dd566d6fdb3610fe2fc43e2a185f25cadf3a230d0a2ca993b868872208c8e3d111e2488e24b77603f7b7ef28e7c5c3bacd485f360cb001d3473ef7dc3d0f4fba63151bbd3f10197d78262e32d6d0fa8ec56ade63cb4677ac5f2fb0632399b1cf6cc4a693d79397dbd82663bd6b67d88f9b65ddb391c8588bb7cbaac56ed236370bfaab6f9798b145b5c084f43cccae77330c03a99f68731ea3f5d22372ef749465b041e9184b0d00e8a490d1386e82d74ee4dca20da2742577284ae52146bc548c7210d091bbc1c9dcad53d68b0722892971a89b1a88b2d864ffc94efc1ddd80f1bc9aa5fa4ece7f163c55fba9a929cace7b07d7b484a6eedb663eafead9b6fe5784c9c4e9e1ad3eaba379fb515d5d8bf34fab97c7ebb76d3817d76a7d323d391cb3c7448a5cd256b3aaa6735c83f236f7b6d0bc301af32064698bbc50a0ad953e46c325775af102bdb7bae428f2600d0f068c2bf152b0447b466a0c3c4cd09c30cc8173ce1ee8e8e997c778a0ace742800d32a69a1603f3eed116a2f08581b4c317a5214790f8cefbe8a516de23404453c4921c212418914b27731f413b6b40440d97f20bb6c8beafe548cf7eb0032cdb16eb7eba6ca9a33a7b584fae5c97847bb3b8465c240cda70bac4250e65ef9cba07a536a87747ddaf8bb117ab651d56c7d377bf9d4a84e3aefde5a8adbadb26c154dd9bba69fbab2634ab6dcee8e61da672bf88b98f33fe0a735b26f92e3176be9a577d85ddce6a9d1af0c700345dffa4ac8e0683f32a180df495a5e1da491d8bc2594f6a4a0f421ae5a5b12072654005574a2e9c92655e4a7c54763aa3bbd4f543eb7773a79863db2b7576fcc2b34dda81241c0c34ee6fdb1976cdfc038e5dd7bfb83fbdc9be3557f127ae890b38eaf68359be499e5d478e1fd1a99ee663bdbf9112afe1d23ecd9aedcceabefbb8be9f442699f659b6c242a3b250d2188a96a30a68c11137a74c50c26bd406a5c929627c94d45b298ba06208b905504afd485b7d35d73d6df5d579fed956fbcc927fdf56f9b36df5236db1afacff1b590a2a70fb147cdd8644fcfd7eccf718664925e8ab0f78b8abc4f0e83a49c95fd50157c42063b1696f0615a98b75a09e3cbd1749bef36204aaf0211a5d185b442c5d2c65e4a5c90be1734941a79d0617a5ce214405de78e7a388c07363699687f46274b1f91de9a84363a00b000001000000000000001976a914fee86b11cb1412b5beb2b89d4c47f29a75d4775188ac01000000000000001976a914fee86b11cb1412b5beb2b89d4c47f29a75d4775188ac6f000000000000001976a914fee86b11cb1412b5beb2b89d4c47f29a75d4775188ac010000000000000069512102293474e46f71eeb59baf53731a17ba28fd60c8ee960a9e529bbba085e82095ba2102b5697bc3cdf0a72b34614f1932f9759e802f2ae0d7aa54c1efa756f6cf7cb9ce2102cb560e47b1ae629416b4293256443cef4427cd5e5f233a8fd2a92f1912ece4a453aedd1c0100000000001976a9141f8b7afba54277eab442e9797167971898b191a288ac00000000"
	testTx, errTx := bt.NewTxFromString(testTxHex)
	assert.NoError(t, errTx)

	testBlockHeight := uint32(766657)
	testUtxoHeights := []uint32{766656, 766656, 766656}

	t.Run("low max pubkeys per multisig policy must fail", func(t *testing.T) {
		tSettings := test.CreateBaseTestSettings()
		tSettings.Policy.MaxPubKeysPerMultisigPolicy = 2
		tSettings.ChainCfgParams = &chaincfg.MainNetParams

		txValidator := NewTxValidator(ulogger.TestLogger{}, tSettings)
		err := txValidator.ValidateTransactionScripts(testTx, testBlockHeight, testUtxoHeights, &Options{SkipPolicyChecks: false})
		assert.Error(t, err)
		assert.ErrorIs(t, err, errors.New(errors.ERR_TX_INVALID, "max pubkeys per multisig policy limit exceeded"))
	})

	t.Run("hight max pubkeys per multisig policy must pass", func(t *testing.T) {
		tSettings := test.CreateBaseTestSettings()
		tSettings.Policy.MaxPubKeysPerMultisigPolicy = 3 // low
		tSettings.ChainCfgParams = &chaincfg.MainNetParams

		txValidator := NewTxValidator(ulogger.TestLogger{}, tSettings)
		err := txValidator.ValidateTransactionScripts(testTx, testBlockHeight, testUtxoHeights, &Options{SkipPolicyChecks: false})
		assert.NoError(t, err)
	})
}

func TestMaxStackMemoryUsagePolicy(t *testing.T) {
	// GetMaxStackMemoryUsage :
	// 	if utxo before genesis : return max_uint64
	// 	if utxo  after genesis :
	// 	- If     SkipPolicyChecks/consensus : return MaxStackMemoryUsageConsensus
	// 	- If not SkipPolicyChecks/consensus : return MaxStackMemoryUsage
	t.Run("set MaxStackMemoryUsageConsensus lower must fail", func(t *testing.T) {
		defer func() {
			if r := recover(); r == nil {
				t.Errorf("Expected panic, but function did not panic")
			}
		}()

		tSettings := test.CreateBaseTestSettings()
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
		tSettings := test.CreateBaseTestSettings()
		tSettings.Policy.MaxStackMemoryUsagePolicy = 1
		tSettings.Policy.MaxStackMemoryUsageConsensus = 271
		tSettings.ChainCfgParams = &chaincfg.MainNetParams

		txValidator := NewTxValidator(ulogger.TestLogger{}, tSettings)
		err := txValidator.ValidateTransactionScripts(testTx, testBlockHeight, testUtxoHeights, &Options{SkipPolicyChecks: true})
		assert.Error(t, err)
		assert.ErrorIs(t, err, errors.New(errors.ERR_TX_INVALID, "max stack memory usage consensus policy limit exceeded"))
	})

	t.Run("high MaxStackMemoryUsageConsensus must pass", func(t *testing.T) {
		tSettings := test.CreateBaseTestSettings()
		tSettings.Policy.MaxStackMemoryUsagePolicy = 1
		tSettings.Policy.MaxStackMemoryUsageConsensus = 272
		tSettings.ChainCfgParams = &chaincfg.MainNetParams

		txValidator := NewTxValidator(ulogger.TestLogger{}, tSettings)
		err := txValidator.ValidateTransactionScripts(testTx, testBlockHeight, testUtxoHeights, &Options{SkipPolicyChecks: true})
		assert.NoError(t, err)
	})

	t.Run("low MaxStackMemoryUsage must fail", func(t *testing.T) {
		tSettings := test.CreateBaseTestSettings()
		tSettings.Policy.MaxStackMemoryUsagePolicy = 271
		tSettings.Policy.MaxStackMemoryUsageConsensus = 1000000
		tSettings.ChainCfgParams = &chaincfg.MainNetParams

		txValidator := NewTxValidator(ulogger.TestLogger{}, tSettings)
		err := txValidator.ValidateTransactionScripts(testTx, testBlockHeight, testUtxoHeights, &Options{SkipPolicyChecks: false})
		assert.Error(t, err)
		assert.ErrorIs(t, err, errors.New(errors.ERR_TX_INVALID, "max stack memory usage policy limit exceeded"))
	})

	t.Run("high MaxStackMemoryUsage must pass", func(t *testing.T) {
		tSettings := test.CreateBaseTestSettings()
		tSettings.Policy.MaxStackMemoryUsagePolicy = 272
		tSettings.Policy.MaxStackMemoryUsageConsensus = 1000000
		tSettings.ChainCfgParams = &chaincfg.MainNetParams

		txValidator := NewTxValidator(ulogger.TestLogger{}, tSettings)
		err := txValidator.ValidateTransactionScripts(testTx, testBlockHeight, testUtxoHeights, &Options{SkipPolicyChecks: false})
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

	testBlockHeight := uint32(820540)
	testUtxoHeights := []uint32{820539}

	t.Run("low MaxScriptNumLengthPolicy must fail", func(t *testing.T) {
		tSettings := test.CreateBaseTestSettings()
		tSettings.Policy.MaxScriptNumLengthPolicy = 5
		tSettings.ChainCfgParams = &chaincfg.MainNetParams

		txValidator := NewTxValidator(ulogger.TestLogger{}, tSettings)
		err := txValidator.ValidateTransactionScripts(testTx, testBlockHeight, testUtxoHeights, &Options{SkipPolicyChecks: false})
		assert.Error(t, err)
		assert.ErrorIs(t, err, errors.New(errors.ERR_TX_INVALID, "max ScriptNum length policy limit exceeded"))
	})

	t.Run("high MaxScriptNumLengthPolicy must pass", func(t *testing.T) {
		tSettings := test.CreateBaseTestSettings()
		tSettings.Policy.MaxScriptNumLengthPolicy = 6
		tSettings.ChainCfgParams = &chaincfg.MainNetParams

		txValidator := NewTxValidator(ulogger.TestLogger{}, tSettings)
		err := txValidator.ValidateTransactionScripts(testTx, testBlockHeight, testUtxoHeights, &Options{SkipPolicyChecks: false})
		assert.NoError(t, err)
	})
}

func TestMaxTxSigopsCountsPolicy(t *testing.T) {
	t.Skip("Skipping this test as we've disabled the method sigOpsCheck")

	// TxID := 9f569c12dfe382504748015791d1994725a7d81d92ab61a6221eadab9f122ece
	testTxHex := "010000000000000000ef011c044c4db32b3da68aa54e3f30c71300db250e0b48ea740bd3897a8ea1a2cc9a020000006b483045022100c6177fa406ecb95817d3cdd3e951696439b23f8e888ef993295aa73046504029022052e75e7bfd060541be406ec64f4fc55e708e55c3871963e95bf9bd34df747ee041210245c6e32afad67f6177b02cfc2878fce2a28e77ad9ecbc6356960c020c592d867ffffffffd4c7a70c000000001976a914296b03a4dd56b3b0fe5706c845f2edff22e84d7388ac0301000000000000001976a914a4429da7462800dedc7b03a4fc77c363b8de40f588ac000000000000000024006a4c2042535620466175636574207c20707573682d7468652d627574746f6e2e617070d2c7a70c000000001976a914296b03a4dd56b3b0fe5706c845f2edff22e84d7388ac00000000"
	testTx, errTx := bt.NewTxFromString(testTxHex)
	assert.NoError(t, errTx)

	testBlockHeight := uint32(886413)
	testUtxoHeights := []uint32{886412}

	tSettings := test.CreateBaseTestSettings()
	tSettings.Policy.MaxTxSigopsCountsPolicy = 1 // low
	tSettings.ChainCfgParams = &chaincfg.MainNetParams

	txValidator := NewTxValidator(ulogger.TestLogger{}, tSettings)
	err := txValidator.ValidateTransaction(testTx, testBlockHeight, &Options{})
	assert.NoError(t, err)

	err = txValidator.ValidateTransactionScripts(testTx, testBlockHeight, testUtxoHeights, &Options{})

	assert.Error(t, err)
	assert.ErrorIs(t, err, errors.New(errors.ERR_TX_INVALID, "max ops per script policy limit exceeded"))
}

func TestMaxOpsPerScriptPolicyWithConcensus(t *testing.T) {
	tSettings := test.CreateBaseTestSettings()

	tSettings.Policy.MaxOpsPerScriptPolicy = 2       // insanely low
	tSettings.Policy.MaxScriptSizePolicy = 100000000 // quite high
	tSettings.ChainCfgParams.GenesisActivationHeight = 100

	txValidator := NewTxValidator(ulogger.TestLogger{}, tSettings)

	err := txValidator.ValidateTransaction(aTx, 101, &Options{})
	assert.NoError(t, err)
}

func Test_MinFeePolicy(t *testing.T) {
	tests := []struct {
		name         string
		opReturnSize int
		expectError  bool
		fee          uint64
	}{
		{
			name:         "very small op_return 100 bytes, no fees",
			opReturnSize: 100,
			expectError:  true,
			fee:          0,
		},
		{
			name:         "very small op_return 100 bytes, fee 1 sat",
			opReturnSize: 100,
			expectError:  false,
			fee:          1,
		},
		{
			name:         "small op_return 800 bytes, fee 0 sat",
			opReturnSize: 800,
			expectError:  true,
			fee:          0,
		},
		{
			name:         "medium op_return 1300 bytes, fee 1 sat",
			opReturnSize: 1300,
			expectError:  false,
			fee:          1,
		},
		{
			name:         "large op_return 1700 bytes, fee 1 sat",
			opReturnSize: 1700,
			expectError:  false,
			fee:          1,
		},
		{
			name:         "large op_return 1700 bytes, no fees",
			opReturnSize: 1700,
			expectError:  true,
			fee:          0,
		},
		{
			name:         "large op_return 2100 bytes, fee 1 sat",
			opReturnSize: 2100,
			expectError:  true,
			fee:          1,
		},
		{
			name:         "large op_return 2100 bytes, fee 2 sat",
			opReturnSize: 2100,
			expectError:  false,
			fee:          2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tSettings := test.CreateBaseTestSettings()

			coinbaseTx, err := bt.NewTxFromString(model.CoinbaseHex)
			require.NoError(t, err)

			output := coinbaseTx.Outputs[0]

			utxo := &bt.UTXO{
				TxIDHash:      coinbaseTx.TxIDChainHash(),
				Vout:          0,
				LockingScript: output.LockingScript,
				Satoshis:      output.Satoshis,
			}

			tx := bt.NewTx()

			err = tx.FromUTXOs(utxo)
			require.NoError(t, err)

			var inputSatoshis uint64 = 1666666668
			outputSatoshis := inputSatoshis - tt.fee

			err = tx.AddP2PKHOutputFromAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", outputSatoshis)
			require.NoError(t, err)

			// Add OP_RETURN output with test case size
			data := make([]byte, tt.opReturnSize)
			for i := range data {
				data[i] = byte(i % 256)
			}

			err = tx.AddOpReturnOutput(data)
			require.NoError(t, err)

			privateKey, err := wif.DecodeWIF("L56TgyTpDdvL3W24SMoALYotibToSCySQeo4pThLKxw6EFR6f93Q")
			require.NoError(t, err)

			err = tx.FillAllInputs(context.Background(), &unlocker.Getter{PrivateKey: privateKey.PrivKey})
			require.NoError(t, err)

			// Log transaction details for debugging
			t.Logf("Test case: %s", tt.name)
			t.Logf("Total Transaction size: %d bytes", tx.Size())

			txValidator := NewTxValidator(ulogger.TestLogger{}, tSettings)
			err = txValidator.ValidateTransaction(tx, 10000000, &Options{})

			if tt.expectError {
				if assert.Error(t, err) {
					assert.ErrorIs(t, err, errors.New(errors.ERR_TX_INVALID, "transaction fee"))
				}
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestCheckFees(t *testing.T) {
	tSettings := test.CreateBaseTestSettings()
	feeQuote := feesToBtFeeQuote(tSettings.Policy.MinMiningTxFee)

	tv := &TxValidator{
		settings: tSettings,
	}

	txFeeCheck, err := bt.NewTxFromString("010000000000000000ef01dc3f29fd73b911566cdcfd67274d265f1806b127e4c297eccb080cbb0fd342b5e20100006a4730440220173236a95e32cefb753d736bb59778c9ea710fccabe47609e61bef8655cce3e90220339dd63ef7113660d8d788f91b2c9a0fee88c244ab50c09336f1c3e6741409f04121022ff4def2419d43ce5fc6bcecd97156eca13cc5a95679bc8576c8dbd75745b360ffffffff05000000000000001976a914240928667a38cb4556416a166f0c064d918c1f2488ac020000000000000000a3006a22314c3771486e31376d3254503636796a553358596f425971366d763351786b4153534c667b2273746174696f6e5f6964223a3131353537352c227368613531325f68617368223a2232646563636334366131303331333662626234333164343235316436643639663036393939316531666563376530363065396663626664316339313063666463227d106170706c69636174696f6e2f6a736f6e04757466380100000000000000be006322314c3771486e31376d3254503636796a553358596f425971366d763351786b415353514c667b2273746174696f6e5f6964223a3131353537352c227368613531325f68617368223a2232646563636334366131303331333662626234333164343235316436643639663036393939316531666563376530363065396663626664316339313063666463227d00106170706c69636174696f6e2f6a736f6e6876a976a914240928667a38cb4556416a166f0c064d918c1f2488ac88ac00000000")
	require.NoError(t, err)

	err = tv.checkFees(txFeeCheck, feeQuote)
	require.NoError(t, err)
}
