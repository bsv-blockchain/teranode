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
	err := validator.ValidateTransaction(tx, blockHeight, nil)
*/
package validator

import (
	"context"
	"fmt"
	"os"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/go-bt/v2/unlocker"
	"github.com/bsv-blockchain/go-chaincfg"
	bec "github.com/bsv-blockchain/go-sdk/primitives/ec"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
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

func TestScriptVerifierGoBt(t *testing.T) {
	for _, tt := range txTests {
		t.Run(tt.name, func(t *testing.T) {
			params, err := chaincfg.GetChainParams(tt.args.network)
			require.NoError(t, err)

			scriptInterpreter := newScriptVerifierGoBt(ulogger.TestLogger{}, settings.NewPolicySettings(), params)
			tt.wantErr(t, scriptInterpreter.VerifyScript(tt.args.tx, tt.args.blockHeight, true, tt.args.utxoHeights), fmt.Sprintf("scriptVerifierGoBt(%v, %v)", tt.args.tx, tt.args.blockHeight))
		})
	}
}

func TestScriptVerifierGoSDK(t *testing.T) {
	for _, tt := range txTests {
		t.Run(tt.name, func(t *testing.T) {
			params, err := chaincfg.GetChainParams(tt.args.network)
			require.NoError(t, err)

			scriptInterpreter := newScriptVerifierGoSDK(ulogger.TestLogger{}, settings.NewPolicySettings(), params)
			tt.wantErr(t, scriptInterpreter.VerifyScript(tt.args.tx, tt.args.blockHeight, true, tt.args.utxoHeights), fmt.Sprintf("scriptVerifierGoSDK(%v, %v)", tt.args.tx, tt.args.blockHeight))
		})
	}
}

func TestScriptVerifierGoBDK(t *testing.T) {
	for _, tt := range txTests {
		t.Run(tt.name, func(t *testing.T) {
			params, err := chaincfg.GetChainParams(tt.args.network)
			require.NoError(t, err)

			scriptInterpreter := newScriptVerifierGoBDK(ulogger.TestLogger{}, settings.NewPolicySettings(), params)
			tt.wantErr(t, scriptInterpreter.VerifyScript(tt.args.tx, tt.args.blockHeight, true, tt.args.utxoHeights), fmt.Sprintf("scriptVerifierGoBDK(%v, %v)", tt.args.tx, tt.args.blockHeight))
		})
	}
}

func Test_Tx(t *testing.T) {
	f1, err := os.Open("testdata/65cbf31895f6cab997e6c3688b2263808508adc69bcc9054eef5efac6f7895d3.bin")
	require.NoError(t, err)
	defer f1.Close()

	// The extended version of the transaction is used to test the script verification
	// in Go SDK, which contains additional metadata such as the block height.

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

	params, err := chaincfg.GetChainParams("mainnet")
	require.NoError(t, err)

	scriptInterpreter := newScriptVerifierGoSDK(ulogger.TestLogger{}, settings.NewPolicySettings(), params)

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
	params, err := chaincfg.GetChainParams("mainnet")
	require.NoError(b, err)

	scriptInterpreter := newScriptVerifierGoBt(ulogger.TestLogger{}, settings.NewPolicySettings(), params)
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
	params, err := chaincfg.GetChainParams("mainnet")
	require.NoError(b, err)

	scriptInterpreter := newScriptVerifierGoSDK(ulogger.TestLogger{}, settings.NewPolicySettings(), params)
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
	params, err := chaincfg.GetChainParams("mainnet")
	require.NoError(b, err)

	scriptInterpreter := newScriptVerifierGoSDK(ulogger.TestLogger{}, settings.NewPolicySettings(), params)
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
	tSettings := test.CreateBaseTestSettings(t)

	tSettings.Policy.MaxTxSizePolicy = 10 // insanely low
	txValidator := NewTxValidator(ulogger.TestLogger{}, tSettings)

	err := txValidator.ValidateTransaction(aTx, 10000000, nil, &Options{})
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

	tSettings := test.CreateBaseTestSettings(t)
	tSettings.Policy.MaxOpsPerScriptPolicy = 2       // insanely low
	tSettings.Policy.MaxScriptSizePolicy = 100000000 // quite high
	tSettings.ChainCfgParams = &chaincfg.MainNetParams

	txValidator := NewTxValidator(ulogger.TestLogger{}, tSettings)
	err := txValidator.ValidateTransaction(testTx, testBlockHeight, testUtxoHeights, &Options{})
	assert.NoError(t, err)

	err = txValidator.ValidateTransactionScripts(testTx, testBlockHeight, testUtxoHeights, &Options{})

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
	assert.NoError(t, err)

	err = txValidator.ValidateTransactionScripts(testTx, testBlockHeight, testUtxoHeights, &Options{})

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
		err := txValidator.ValidateTransactionScripts(testTx, testBlockHeight, testUtxoHeights, &Options{SkipPolicyChecks: false})
		assert.Error(t, err)
		assert.ErrorIs(t, err, errors.ErrTxPolicy)
	})

	t.Run("hight max pubkeys per multisig policy must pass", func(t *testing.T) {
		tSettings := test.CreateBaseTestSettings(t)
		tSettings.Policy.MaxPubKeysPerMultisigPolicy = 3 // low
		tSettings.ChainCfgParams = &chaincfg.MainNetParams

		txValidator := NewTxValidator(ulogger.TestLogger{}, tSettings)
		err := txValidator.ValidateTransactionScripts(testTx, testBlockHeight, testUtxoHeights, &Options{SkipPolicyChecks: false})
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
		err := txValidator.ValidateTransactionScripts(testTx, testBlockHeight, testUtxoHeights, &Options{SkipPolicyChecks: true})
		assert.Error(t, err)
		assert.ErrorIs(t, err, errors.ErrTxPolicy)
	})

	t.Run("high MaxStackMemoryUsageConsensus must pass", func(t *testing.T) {
		tSettings := test.CreateBaseTestSettings(t)
		tSettings.Policy.MaxStackMemoryUsagePolicy = 1
		tSettings.Policy.MaxStackMemoryUsageConsensus = 272
		tSettings.ChainCfgParams = &chaincfg.MainNetParams

		txValidator := NewTxValidator(ulogger.TestLogger{}, tSettings)
		err := txValidator.ValidateTransactionScripts(testTx, testBlockHeight, testUtxoHeights, &Options{SkipPolicyChecks: true})
		assert.NoError(t, err)
	})

	t.Run("low MaxStackMemoryUsage must fail", func(t *testing.T) {
		tSettings := test.CreateBaseTestSettings(t)
		tSettings.Policy.MaxStackMemoryUsagePolicy = 271
		tSettings.Policy.MaxStackMemoryUsageConsensus = 1000000
		tSettings.ChainCfgParams = &chaincfg.MainNetParams

		txValidator := NewTxValidator(ulogger.TestLogger{}, tSettings)
		err := txValidator.ValidateTransactionScripts(testTx, testBlockHeight, testUtxoHeights, &Options{SkipPolicyChecks: false})
		assert.Error(t, err)
		assert.ErrorIs(t, err, errors.ErrTxPolicy)
	})

	t.Run("high MaxStackMemoryUsage must pass", func(t *testing.T) {
		tSettings := test.CreateBaseTestSettings(t)
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
		tSettings := test.CreateBaseTestSettings(t)
		tSettings.Policy.MaxScriptNumLengthPolicy = 5
		tSettings.ChainCfgParams = &chaincfg.MainNetParams

		txValidator := NewTxValidator(ulogger.TestLogger{}, tSettings)
		err := txValidator.ValidateTransactionScripts(testTx, testBlockHeight, testUtxoHeights, &Options{SkipPolicyChecks: false})
		assert.Error(t, err)
		assert.ErrorIs(t, err, errors.ErrTxPolicy)
	})

	t.Run("high MaxScriptNumLengthPolicy must pass", func(t *testing.T) {
		tSettings := test.CreateBaseTestSettings(t)
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

	tSettings := test.CreateBaseTestSettings(t)
	tSettings.Policy.MaxTxSigopsCountsPolicy = 1 // low
	tSettings.ChainCfgParams = &chaincfg.MainNetParams

	txValidator := NewTxValidator(ulogger.TestLogger{}, tSettings)
	err := txValidator.ValidateTransaction(testTx, testBlockHeight, testUtxoHeights, &Options{})
	assert.NoError(t, err)

	err = txValidator.ValidateTransactionScripts(testTx, testBlockHeight, testUtxoHeights, &Options{})

	assert.Error(t, err)
	assert.ErrorIs(t, err, errors.ErrTxPolicy)
}

func TestMaxOpsPerScriptPolicyWithConcensus(t *testing.T) {
	tSettings := test.CreateBaseTestSettings(t)

	tSettings.Policy.MaxOpsPerScriptPolicy = 2       // insanely low
	tSettings.Policy.MaxScriptSizePolicy = 100000000 // quite high
	tSettings.ChainCfgParams.GenesisActivationHeight = 100

	txValidator := NewTxValidator(ulogger.TestLogger{}, tSettings)

	err := txValidator.ValidateTransaction(aTx, 101, nil, &Options{})
	assert.NoError(t, err)
}

func TestReturnConsensusError(t *testing.T) {
	// TxID := 9f569c12dfe382504748015791d1994725a7d81d92ab61a6221eadab9f122ece
	testTxHex := "010000000000000000ef011c044c4db32b3da68aa54e3f30c71300db250e0b48ea740bd3897a8ea1a2cc9a020000006b483045022100c6177fa406ecb95817d3cdd3e951696439b23f8e888ef993295aa73046504029022052e75e7bfd060541be406ec64f4fc55e708e55c3871963e95bf9bd34df747ee041210245c6e32afad67f6177b02cfc2878fce2a28e77ad9ecbc6356960c020c592d867ffffffffd4c7a70c000000001976a914296b03a4dd56b3b0fe5706c845f2edff22e84d7388ac0301000000000000001976a914a4429da7462800dedc7b03a4fc77c363b8de40f588ac000000000000000024006a4c2042535620466175636574207c20707573682d7468652d627574746f6e2e617070d2c7a70c000000001976a914296b03a4dd56b3b0fe5706c845f2edff22e84d7388ac00000000"
	testTx, errTx := bt.NewTxFromString(testTxHex)
	assert.NoError(t, errTx)

	testBlockHeight := uint32(886413)
	testUtxoHeights := []uint32{886412, 886412} // Add one element to make utxo array wrong

	tSettings := test.CreateBaseTestSettings(t)
	tSettings.Policy.MaxTxSigopsCountsPolicy = 1 // low
	tSettings.ChainCfgParams = &chaincfg.MainNetParams

	txValidator := NewTxValidator(ulogger.TestLogger{}, tSettings)
	err := txValidator.ValidateTransaction(testTx, testBlockHeight, testUtxoHeights, &Options{})
	assert.NoError(t, err)

	err = txValidator.ValidateTransactionScripts(testTx, testBlockHeight, testUtxoHeights, &Options{SkipPolicyChecks: true})

	assert.Error(t, err)
	assert.ErrorIs(t, err, errors.ErrTxConsensus)
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
			tSettings := test.CreateBaseTestSettings(t)
			// Set minimum fee to 0.000000010 BSV/kB
			// This equals 1 satoshi/kB = 0.001 satoshis/byte
			// So transactions > 1000 bytes need more than 1 satoshi
			tSettings.Policy.MinMiningTxFee = 0.000000010

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

			privateKey, err := bec.PrivateKeyFromWif("L56TgyTpDdvL3W24SMoALYotibToSCySQeo4pThLKxw6EFR6f93Q")
			require.NoError(t, err)

			err = tx.FillAllInputs(context.Background(), &unlocker.Getter{PrivateKey: privateKey})
			require.NoError(t, err)

			// Log transaction details for debugging
			t.Logf("Test case: %s", tt.name)
			t.Logf("Total Transaction size: %d bytes", tx.Size())

			txValidator := NewTxValidator(ulogger.TestLogger{}, tSettings)
			err = txValidator.ValidateTransaction(tx, 10000000, nil, &Options{})

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

	// At Genesis activation height, p2sh should not be rejected
	err = txValidator.ValidateTransaction(txP2SH, tSettings.ChainCfgParams.GenesisActivationHeight, nil, &Options{})
	require.NoError(t, err)

	err = txValidator.checkOutputs(txP2SH, tSettings.ChainCfgParams.GenesisActivationHeight, &Options{})
	require.NoError(t, err)

	// After Genesis activation height, p2sh should be rejected
	err = txValidator.ValidateTransaction(txP2SH, tSettings.ChainCfgParams.GenesisActivationHeight+1, nil, &Options{})
	require.Error(t, err)

	err = txValidator.checkOutputs(txP2SH, tSettings.ChainCfgParams.GenesisActivationHeight+1, &Options{})
	require.Error(t, err)

	// After Genesis activation height, with skip policy check, p2sh should be accepted
	err = txValidator.ValidateTransaction(txP2SH, tSettings.ChainCfgParams.GenesisActivationHeight+1, nil, &Options{SkipPolicyChecks: true})
	require.NoError(t, err)

	err = txValidator.checkOutputs(txP2SH, tSettings.ChainCfgParams.GenesisActivationHeight+1, &Options{SkipPolicyChecks: true})
	require.NoError(t, err)
}

func TestCheckFees(t *testing.T) {
	tSettings := test.CreateBaseTestSettings(t)

	tv := &TxValidator{
		settings: tSettings,
	}

	txFeeCheck, err := bt.NewTxFromString("010000000000000000ef01dc3f29fd73b911566cdcfd67274d265f1806b127e4c297eccb080cbb0fd342b5e20100006a4730440220173236a95e32cefb753d736bb59778c9ea710fccabe47609e61bef8655cce3e90220339dd63ef7113660d8d788f91b2c9a0fee88c244ab50c09336f1c3e6741409f04121022ff4def2419d43ce5fc6bcecd97156eca13cc5a95679bc8576c8dbd75745b360ffffffff05000000000000001976a914240928667a38cb4556416a166f0c064d918c1f2488ac020000000000000000a3006a22314c3771486e31376d3254503636796a553358596f425971366d763351786b4153534c667b2273746174696f6e5f6964223a3131353537352c227368613531325f68617368223a2232646563636334366131303331333662626234333164343235316436643639663036393939316531666563376530363065396663626664316339313063666463227d106170706c69636174696f6e2f6a736f6e04757466380100000000000000be006322314c3771486e31376d3254503636796a553358596f425971366d763351786b415353514c667b2273746174696f6e5f6964223a3131353537352c227368613531325f68617368223a2232646563636334366131303331333662626234333164343235316436643639663036393939316531666563376530363065396663626664316339313063666463227d00106170706c69636174696f6e2f6a736f6e6876a976a914240928667a38cb4556416a166f0c064d918c1f2488ac88ac00000000")
	require.NoError(t, err)

	err = tv.checkFees(txFeeCheck, 10000000, nil)
	require.NoError(t, err)
}

func TestSubErrorTxInvalid(t *testing.T) {
	rootErr := errors.NewUnknownError("a returned root error")

	t.Run("simple errors", func(t *testing.T) {
		newConsensusError := errors.NewTxConsensusError("An error by consenss", rootErr)
		isConcensusError := errors.Is(newConsensusError, errors.ErrTxConsensus)
		assert.True(t, isConcensusError)
		assert.ErrorIs(t, newConsensusError, errors.ErrTxConsensus) // std check

		newPolicyError := errors.NewTxPolicyError("An error by policy check", rootErr)
		isPolicyError := errors.Is(newPolicyError, errors.ErrTxPolicy)
		assert.True(t, isPolicyError)
		assert.ErrorIs(t, newPolicyError, errors.ErrTxPolicy) // std check

		// Policy, consensus errors are different than the invalid tx error
		assert.False(t, errors.Is(errors.ErrTxConsensus, errors.ErrTxInvalid))
		assert.False(t, errors.Is(errors.ErrTxPolicy, errors.ErrTxInvalid))
		assert.False(t, errors.Is(errors.ErrTxPolicy, errors.ErrTxConsensus))

		assert.False(t, errors.Is(newConsensusError, newPolicyError))

		assert.False(t, errors.Is(newConsensusError, errors.ErrTxInvalid))
		assert.False(t, errors.Is(newConsensusError, errors.ErrTxPolicy))

		assert.False(t, errors.Is(newPolicyError, errors.ErrTxInvalid))
		assert.False(t, errors.Is(newPolicyError, errors.ErrTxConsensus))
	})

	t.Run("combined ErrTxInvalid ErrTxConsensus", func(t *testing.T) {
		consensusError := errors.NewTxConsensusError("An error by consensus check", rootErr)
		combinedError := errors.NewTxInvalidError("Final error triggered by consensus check", consensusError)

		assert.ErrorIs(t, combinedError, errors.ErrTxConsensus) // std check
		assert.ErrorIs(t, combinedError, errors.ErrTxInvalid)   // std check

		assert.True(t, errors.Is(combinedError, errors.ErrTxConsensus))
		assert.True(t, errors.Is(combinedError, errors.ErrTxInvalid))
		assert.False(t, errors.Is(combinedError, errors.ErrTxPolicy))
	})

	t.Run("combined ErrTxInvalid NewTxPolicyError", func(t *testing.T) {
		policyError := errors.NewTxPolicyError("An error by policy check", rootErr)
		combinedError := errors.NewTxInvalidError("Final error triggered by policy check", policyError)

		assert.ErrorIs(t, combinedError, errors.ErrTxPolicy)  // std check
		assert.ErrorIs(t, combinedError, errors.ErrTxInvalid) // std check

		assert.True(t, errors.Is(combinedError, errors.ErrTxPolicy))
		assert.True(t, errors.Is(combinedError, errors.ErrTxInvalid))
		assert.False(t, errors.Is(combinedError, errors.ErrTxConsensus))
	})
}

func TestZeroSatoshiOutputRequiresOpFalseOpReturn(t *testing.T) {
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

func Test_isConsolidationTx(t *testing.T) {
	tSettings := test.CreateBaseTestSettings(t)
	txValidator := NewTxValidator(ulogger.TestLogger{}, tSettings)

	// Create a consolidation transaction with multiple inputs and a single output
	privKey, err := bec.NewPrivateKey()
	require.NoError(t, err)

	parentTx1 := transactions.Create(t,
		transactions.WithCoinbaseData(100, "/Test miner/"),
		transactions.WithP2PKHOutputs(1, 100000, privKey.PubKey()),
	)

	isConsolidation := txValidator.isConsolidationTx(parentTx1, nil, 0)
	assert.False(t, isConsolidation)

	parentTx2 := transactions.Create(t,
		transactions.WithCoinbaseData(100, "/Test miner/"),
		transactions.WithP2PKHOutputs(1, 100000, privKey.PubKey()),
	)

	isConsolidation = txValidator.isConsolidationTx(parentTx2, nil, 0)
	assert.False(t, isConsolidation)

	consolidationTx, err := bt.NewTxFromString("01000000975c502c7ae210e7ffc46c86288162c52f9c2bc249d82d856eeac168097fba94de060000006a47304402207268c5bc341a739cf1de9eb485d2ad1663218e805e16a3cf1d8f2c5a6b43264202200678b334d5c023dec5f984d1492bf1423518ae89585e290b35530ad09826a4d74121031c4ba1483206ba173a52cf74717a9e67cbca382736680f5c3b801fa8befde630ffffffff5c502c7ae210e7ffc46c86288162c52f9c2bc249d82d856eeac168097fba94de070000006a4730440220023cafe88cd96dc4831d2b92e1c6edc14b3ef941d553bbaa90849988a72089c9022074e177f473fe95da2f68419869af562986c978215474a37cfbe5ed6480056a6e412102b370a284a2ae3a4a8a0aa23751f8740cfb044101bc279803eecc3f39e3f9c7b6ffffffff5c502c7ae210e7ffc46c86288162c52f9c2bc249d82d856eeac168097fba94de080000006a47304402202ea835a205ca389b4bed806471a2f4a5a425ad057da746727aafc874ef3fff730220775a4291c350eb3e034467485c3defb4dd44da037b665882faa2ab93734b5f4e4121027c52bcb15c5ee20de0f18728afdb7f1673b6c91e2ff8b6c79b09d167a7007d6fffffffff5c502c7ae210e7ffc46c86288162c52f9c2bc249d82d856eeac168097fba94de090000006a4730440220719cde2d02be44a623d02e29e09807fcdc38a52441854773dec3c9334a8bd0c702205e5f41a9d42c0713a795318933172148aaa9a3d0b58426749065403a0155cdaa412103750cc588e842568c12425f691c68f5290b35e737de239df3a223891e1cd97820ffffffff5c502c7ae210e7ffc46c86288162c52f9c2bc249d82d856eeac168097fba94de0a0000006a4730440220487f7940ba131208a4b488fc99dd47f9e3b9ad2282aa76e53ee7489d8c92e176022007951cbf2078fbb57966204d6fd5fce1c78892ca605f1c53526fd248e6eb10854121027c2738c2d617b0ee39894e388f0abc798a7e6e2a5e376bf33d3eb3d5608b11b8ffffffff5c502c7ae210e7ffc46c86288162c52f9c2bc249d82d856eeac168097fba94de0b0000006a473044022056ee0440b0cd8b971a675e0099ea2c91e3ded38f4b3b6f07eb0d9d942a9bb8be0220448681a47f2da5f7eade03dca14ac31bac666658955689b900e8b738cc9506724121023d3da7bb566bcaae868f2de60e8f5f943bfb0d788cf048a73be759db885cf8f6ffffffff5c502c7ae210e7ffc46c86288162c52f9c2bc249d82d856eeac168097fba94de0c0000006a473044022015ae940c380032b0761126c7e9cb837423f62a12652de22b048c203014d74ec10220778ad1a8e4dfab6beaa64e17863fd2c2e31db3feff7c13fad479ccbb209afc8b41210226efd429cc4650ae86f653bb2d4aaa989f871d679ddac9c167e8f303fd09a620fffffffff14a3ec9325179c75b6ce462362997545ae63b27db63c035f6f9c7a022ff1743010000006a47304402207fa9dcc5810aa3eac5e32b098c8e1837d18e3a2c6d44b7fc1e0c7cdffe6bb6ef022039fd53a9dc15c9b43373398ddd9e0fb034c8f82ee2beb27af979a05f5d354483412103ac1f2ecf661cd8a7eb6cfe421e342751098eda1c5f8e87f28f22eb45743f883ffffffffff14a3ec9325179c75b6ce462362997545ae63b27db63c035f6f9c7a022ff1743020000006a47304402205a2bb6bf521b61d93a135582c033956a2c9bf427b2ed83c28821517e66a37bec02204021a5713ba1c8de51a29322bb3f3373927e2599d7cb91444542612cee202197412102abe613444825e0da751581ff653ae160ae348f620535e9b73935a6f5b6156d94fffffffff14a3ec9325179c75b6ce462362997545ae63b27db63c035f6f9c7a022ff1743030000006a4730440220657a5fe83cf953bf1c257a34989636841251608f31c5a40d0151b4bda1cf8d12022062fbb3a9a4f5c515593ce8ef3c1f5399d92f2a0ba0839f761341a48a237656e041210304726feaf36df88ca72c99269bbeaa1c6b1d4feae66dba52def152b08cd7ab3bfffffffff14a3ec9325179c75b6ce462362997545ae63b27db63c035f6f9c7a022ff1743040000006a473044022028b47c23da697cd390b2dbfb34b9130b39e097565114deacebbacc053315b38b022076ebc20abd22a95ed0783d2e378052d3377a74b094313be6355f219e0a211460412102fe094266b5fafbfe1f0a73b0c2b5db727e6fd341959c014ba73b56fdf1c7adaafffffffff14a3ec9325179c75b6ce462362997545ae63b27db63c035f6f9c7a022ff1743050000006a47304402204b5e1edc08fe63625c6e610de64f1b52d38ec047aa041380d22dea0a40f349490220071d7d01ce774406c7feb94e9a06b4d5da6d25a401e58eae5e532dc3ba3655b0412102724c7397fe79adca4506b1127f524f8ee3f680ef24d29c811487225128fd4d07fffffffff14a3ec9325179c75b6ce462362997545ae63b27db63c035f6f9c7a022ff1743060000006a473044022032186a5c4dd5165f8d1bb6c34c253e4bf2963d35618bea507289ef7b966663c5022056e48e41443fd18ad47cccaad5eae4c858422d90e41aa56be8cb83eae053cdba4121030b8404f023d364fedae7428ac8a9b9e3f03d72c6dad2b6ef07ffe940984e75b7fffffffff14a3ec9325179c75b6ce462362997545ae63b27db63c035f6f9c7a022ff1743070000006a4730440220532bfbe5f2c696cf1279084946e5452c33a78717b35212d3ef8e562830b36d6302202d69c3f0ee9a4316738631915133c6247c17fc9ecf371d941aa83edb1a9ae2724121021aac36978030489c19e7b54b1f0e140350bb57b3423b323eb1bea9f28e7a34c5fffffffff14a3ec9325179c75b6ce462362997545ae63b27db63c035f6f9c7a022ff1743080000006a4730440220595f782af96797c99a8b75eb997552881b26a543f1a8ce2aaa942b7eccdc317902201cd32d4432e812f600d29d4ba64081dab12140fdd02eccca46c9e9532f389417412103f87f656bcb692ca15cea277fae53a96dc51bb26d3dfedbb4b48c2f7a37f72876fffffffff14a3ec9325179c75b6ce462362997545ae63b27db63c035f6f9c7a022ff1743090000006a47304402206000d5c8d8575bf246c787d0cdf6a4378abd1eb905e1c51dc19193d4c3af30640220627fb011a95c47bab1cd789ca8ddc9dcdf58a39733fc0ba9268bb4a349076237412103aba9e58d0860426acccad6ec9cff2225cb2ad03503e62b96aa0a43bec3a3a282fffffffff14a3ec9325179c75b6ce462362997545ae63b27db63c035f6f9c7a022ff17430a0000006a47304402203db3a88e85bf198a94a19727825eda360ee22e5eee9aba4d7a84d910744c23a902207bdc209c363269c5355a866731ea6174475cf5ccdaa12e070f1cb27865769b08412103f2f139909a4671a5ca443e825e85a1a45d73e209b65673b12c2d2e024dbd40aaffffffff840a1ddfaa87d728f70a060bb40bbc3fcd9eb5f05dce0e7ada7d210adde02697040000006a47304402203264700c10e5c5d71ba1bd776e1ce2f4913bb5c34eb8d76f326d97a1b36f845f02204f081d9686638a65c469e1a70a3da583a2f1198e1ef3fa55c1878c8f4ab993f841210287fb7b0dfa2b3cdca29612be52dba9f70db7c259146aa99fbe8bbb97525edb61ffffffff840a1ddfaa87d728f70a060bb40bbc3fcd9eb5f05dce0e7ada7d210adde02697050000006a47304402206b41ec49a05ff12deb255f88da20c6ce39b7ed54371c68181db4dc3bae50342a0220161cad643c9e80867473dc2ff65b432e9c2f242b25f6782ef13625d5599b182941210370491e8d326b7ccb617b7696b0abfb6ccd887be017d8c4d7a2804e0740fbda5cffffffff840a1ddfaa87d728f70a060bb40bbc3fcd9eb5f05dce0e7ada7d210adde02697060000006a47304402207e1e45f517b54ba19d571a503d707955f9a2d780d3c17aede7ddf332638aeaba02204fef4e0a76023a62678930401d330d7fd6237074938667aef706ee252d3d9ecc412103a0c9ff07864ffce6513cdd3414e04052e43024ba94d4fb6c1290fb833d66949affffffff840a1ddfaa87d728f70a060bb40bbc3fcd9eb5f05dce0e7ada7d210adde02697070000006a47304402205b3ec5c26e8930777cbe3abd1b82de47f2591e158064ba526c5d5ad595f2814d022038301e6368eb3370315be8e4ba5ec4ced121bf6957413b30db3c2fa598a52a9b4121022d62b8e59a553cf423a66386611ec7d5cf2c569e839f9833b8711dc6539ae54cffffffff840a1ddfaa87d728f70a060bb40bbc3fcd9eb5f05dce0e7ada7d210adde02697080000006a473044022019d269fdb990e6ee1f19d9d77b5b261f5026bbbd2c681618a012dabe9a4ae7100220231bb7f9267bc7d254bf6f54b547751569fd0de80f5ac3f0034d48dbed15cc6a4121020da4a140b39b22f49d50a0b6094995f012705182693cb4fcf779eb2fcc1eb206ffffffff840a1ddfaa87d728f70a060bb40bbc3fcd9eb5f05dce0e7ada7d210adde02697090000006a473044022031c7479acb77b7c8afc186222a0a66f582875b32400bbc98d30192029849eb920220275e10b59209f35955a7dc6fbff94dc277685b307497cb0a270e74d775ade23d4121034885b85006cc76c53f641fdb86fad1d8dc357083298645bee931542b76716d87ffffffff840a1ddfaa87d728f70a060bb40bbc3fcd9eb5f05dce0e7ada7d210adde026970a0000006a473044022054388848b3dc004077e85499ed548c02418130cec14f2b69bb865c06a005a2a802206220b663ba3261bd742009b33e53c3675ad2443816c9024ba8f173bfd7593afd412102944939cde46af6126be577ffb802da86ab001119234a5cd88f627ac29edf432cffffffff840a1ddfaa87d728f70a060bb40bbc3fcd9eb5f05dce0e7ada7d210adde026970b0000006a4730440220679ea7be37bec4854fb886ffa48cdb42cc1afa7b1aac40416c2c2551ba62e29b022074f8a46fa04195d8290ac45d1746aacfed0d11675b5da0f24979c77db4a18bb241210361ba5ba3c68337c2910a6479fcf00a51b96f95a0e993f14428da6ca383c49ad6ffffffff840a1ddfaa87d728f70a060bb40bbc3fcd9eb5f05dce0e7ada7d210adde026970c0000006a47304402202fdab6f7d0400eb73cd26b224fcb89d7474a7ca24b302fe5b68a9aacb9b34a03022059d4de2c2f566fac41bfe8bd740d632d9ea77357858b5fd90f9f9c8270c6c460412103bed7a30a81db6c53bd9ca350a49acbe18e70ab337760c685112c1bf188c9de1fffffffff840a1ddfaa87d728f70a060bb40bbc3fcd9eb5f05dce0e7ada7d210adde026970d0000006a47304402207d3da9282c80b0d8764a5d3e334838859decc796b058e8ab409ec7ce121299e702201cbea65c527677658ea6d6aa8e51ab105e097969fc9fd9dc8e614110234069d0412102330c9b71c23f8ebb0ca5371d686cf99ec5ea9d226e1ed9ed35810eed9e24646effffffffb8b519858676d6b55ae6904d6b702f0097b7cfa96a385723d377372a8ae7200c010000006a47304402202d6213ae78ee8fa924d3fbb7d0e7fc1d8db370e916d2b2f3c3a67da9e1647e0f0220199cd2aa30f52f2c19311961dc19f6540eb87bfc0c64f28a3ed704a6aeb43d3641210259598335a9fa3887e867efbcc9b8b27ee7c0d1127c06bb53881aa1424d3d10b4ffffffffb8b519858676d6b55ae6904d6b702f0097b7cfa96a385723d377372a8ae7200c020000006a47304402202c57a81224b0c8f9f25a677c1bb9d6f1fbf6c8479ec5454a65d0ff27f8b0d17e0220038bd382401a49446265b846dbf0c6e769cb87c8c9c41eaaafc1f55e9fd80a60412103a7476ea11e2c69a3927e763c928b298f88310590429c1972b650005a6b654e89ffffffffb8b519858676d6b55ae6904d6b702f0097b7cfa96a385723d377372a8ae7200c030000006a47304402202265fd6f1aaa06dfa34e2860682d50e966061a6f833e9b6797f9ce83419f956a022015873ae9cd624ed5458255f891cc2b4e23b0dcd1c70c08014e2d8a77919556964121026f04c600500b2f96bf5c2fc345c9ac18a60c271bd6979b4e51f3edc8fe78efe4ffffffffb8b519858676d6b55ae6904d6b702f0097b7cfa96a385723d377372a8ae7200c040000006a4730440220638888ce5eeff080d77746a97f69a69f76195fff3d153594a58e7c2bbf9de3b2022018a46bf4a78a49e4687fd05f1a470438eaad8c157e0e12349ee260fe2c8ec8ba412102bc7be5443fbf34aa6bced5a5dd2425b32ad96fcb4eb13315fdb228450879ceccffffffffb8b519858676d6b55ae6904d6b702f0097b7cfa96a385723d377372a8ae7200c050000006a47304402200ebb14e305a98e37fabcb671cb590898941e64212db709751f2453415817aafe0220472777ae3dfdbe1a52bd0db820c525959ed5f29a12a8e0ae1ed0ac302b454b1a4121022fdaf3f7012bbf6e7721c0caa5cb99cfcc4e2c8f978d9a55a1d24a7f2a302477ffffffffb8b519858676d6b55ae6904d6b702f0097b7cfa96a385723d377372a8ae7200c060000006a47304402200af7445b1ead3ff39b5e62308dfba2711dba54197413ad4a2835f5b9fb1c8b5b02202cacaaa9b87ac268d07456193e31c0bf33886f887b4112ac3b5e29d5583ff0b94121027527ce579cf531ae18b8060a3108b59428a1a939d84f325825a96ed2b9178e2cffffffffb8b519858676d6b55ae6904d6b702f0097b7cfa96a385723d377372a8ae7200c070000006a4730440220263833938a0686b9ec8d7afee5a146548db33fb546a5cae57593997eb691c33702207c41e09f219e042f228c73e1c30030230b1eec1f51dfee014caed959e53dbe0c412102c063d2ca2538fa25e130b111a0c11555c3c665127f70d320313cd3d956d0c7f9ffffffffb8b519858676d6b55ae6904d6b702f0097b7cfa96a385723d377372a8ae7200c080000006a47304402207c32fdca982f4c5f95d7db0a9fb8c58b27cf23358a5e623939664a858b96df93022031c9b370b8fe65a7bec29abb88e96c0ca43586caa5a8280635daa11e1b7796f0412102ca72ec4547617f4430c045974fe3fec33982f01000b56d5b06b777a08b8ee338ffffffff8b932d6efcb29868dfecc79e90c6520c1f79e8c3249fd161a50af3c70e0cd126010000006a47304402206f80042bdc461e94ab8a7752d6dc4612eb23726c52bb891892ffb56dd4fdc125022030af61267a5e99de645fb56f9d0411fac36f7c82d88089a8f4c661471f06ed294121028fb96696c507034cff02edb7bcf1658c606071a1c95438792795d2bb8a07a8b6ffffffff1f8a08561f0c333115e1e1a71714c87a6553ae326de483a5e9b1f076d813fa44010000006a47304402204642df9390be8702d4f109cbde5fd6dd1ff8d165c767657266eb4e15eeceb5d5022061d023f4654392c301b73109981665186deba6338d8a3d49a572cd66a3c5c727412102132a5320bc20c2d32225c1b1d0a051a661bb49cc10855fd39e1a1c7ca5c4ac91ffffffff80e6cc670519c88f063d4bec120c357a0fad9729010e793a574846173acf2d53010000006a4730440220558901d5fe2b0ae47507c16cb3937117e5cc36d4556096fd06f3596aea1ac62e02205c6a2ab198ef26f21cc4fa8d3c926ee8c106c3551414caedc184ea4e480100c041210241c3409b15e2e0c5aae8ece3400cbc27d47586caabb66f5578f0209dcd150810ffffffff34c2f2d408960748f4f7e82d141f51b812b1802061467723f8468de9d5603a69010000006a47304402205e269abad2688f59ca2c33f431d5be35a3e46108b9a21755a3ae17a30c77302002200743c78a02081fe4d59b60bc2807fa92b4833561e518232ef591dd600fda4ed941210373f520d1697f570917663d585c58417dc195adc9be66cacae2ff16041e453c87ffffffffe68ec5d37fc5cfe07f23b57c0647e6f4dc827f483f8671e549da01fb43d4ef9d010000006a47304402207414e58b2273cedbc79e35961bcc9df9d43ccca65ddce13675dd9fc0057fe2d1022020922be4964db86d75ed8f37dddf2bbc496faba4021269ae950c096377f76991412102edbf2c22d555c141ac602ef68237556ffc16b93dbd714bb7255b40d45e04fd87ffffffff4424875be228b424b4fb758ff626bda960ec8c81e558fcb9f1415d2ceef525f6010000006a47304402205eaae274810eabff5123a8e7b9118ff3ef9871b90ecff69101bdddaa805a1fdd02204badf71afb3e07042e2c751905ce67c8b2ff09bb893cfb072219f1fc7aa70b744121027c3f5a5eb400bdebdda960d58a7ec4fbe7a409f49a05c58f0616f508daa63b3affffffffda187bb038cf0f16983fb3fe8c841af9a92852a9250c63144012aadd6c4fcc60030000006a473044022023bedc41b7d038a0dab27a91c61b6f70dc1d84ba28fd61fc64eb8c4e328618670220389402ff3af82f44056fcc3c9a18f25117b8dc55e0d17e314273e6c237d9a1e44121029a9cbbff041e59d8aa2c3b15ce4e5f3def4f572497c97b8270fda1e567033496ffffffffda187bb038cf0f16983fb3fe8c841af9a92852a9250c63144012aadd6c4fcc60040000006a47304402202623fbc20ff96061d3e2337f49ab66131155363cb28a2fa3c5b4a6f8018b00dc02200caaa9e64ca911c630feb2fdf991c5155d8b84ec328257933210bfb5cd1fec264121026d03a992540e5571d1c1e3654665f45c91ac182f7e6d517c8d65083d8112b481ffffffffda187bb038cf0f16983fb3fe8c841af9a92852a9250c63144012aadd6c4fcc60050000006a4730440220113d7cce815b86040570895f2c75356aeb11f581ed56efee921df119049890af02201b9d720d2901aa382902d2732fa64945ea57ce8f3cbe665a89ac4b57e595ba9d412103ba95ca9a3b586fdab2a43b710641adffe3ff658a177e5afc7e377f5f9cda9a99ffffffffda187bb038cf0f16983fb3fe8c841af9a92852a9250c63144012aadd6c4fcc60060000006a4730440220365e8f8c2d638f4d841de06ce94976950743f44e5adacd65676da6313c9dbc6a02206d3e32f8a9ac846429b2afa2d4caedc3ced174893d753c247a8b4c96bf962b4c412102b38b3de9a647a1466d0012b5e85d22f993fcbaba9a823554ab5a8199112250e4ffffffffda187bb038cf0f16983fb3fe8c841af9a92852a9250c63144012aadd6c4fcc60070000006a4730440220638f323337d5322648c0598137448b99d5f639a523dced79a285ee195b1bc0dd02201cceb7c5c3af4dd76f1c1f53fcf6d86f9364e29f010b263398aaddfd856d76cd41210333293538b911c634deff94aed549fa1530edb6bb69cb470f31ca2e24cbaea2faffffffffda187bb038cf0f16983fb3fe8c841af9a92852a9250c63144012aadd6c4fcc60080000006a473044022051501c72bd66dba5d8c99f3500924397eb3427916de2f1e202d8bf52ece7797e0220079fb63c66148d73ab463b2534f96fc2e7ca3dcaaeb2246309a8a8ed04e93cae412102871b2a9d3335c6251b35a2ec1cf24aeb3296c4da4cfbd19a503e3feb2c5ed1f2ffffffffda187bb038cf0f16983fb3fe8c841af9a92852a9250c63144012aadd6c4fcc60090000006a473044022071cd98ce216a24a4b887cee7f9dc91c36d2c17989dfed23cf9b843eafded93ce0220431a55f7595b0c1adb61c94200a68055e8f3b249f8683575c609b3a082cccc0041210302bc4659e5bdf60ef7f54fbf1873981f1c9c4b5d9d270a20f18d5d4c42374489ffffffffda187bb038cf0f16983fb3fe8c841af9a92852a9250c63144012aadd6c4fcc600a0000006a473044022013e2903d5165c42bfac2ce7cac34e713d81e8bbd8348e48b4df44ae1a82a6e2e02203fa770cd24d29e403003802d46bd5ea69b83db6374e636fb56e1c5dfcf6f51194121023f652217b161ffcb497b2899eb18043071a798e3c6b55256386d83730685a07dffffffffa26edeb86e8cecf7c790d916b749332cd847f008e5c08cfbe0f588c908fb895a010000006a47304402207eff8fb41ab6b9485c8d9fd4fe69fcf5e8524fe566fa95828e2835b40c48aab4022028a7c74c710969cde0ec9502f5b98e135d2d61fdd5e3bfc6c3b04cf552eb07b341210315ad541eb275ec1b777b3a16c72e13d58062158934ba624a5ab2a8cb2c940431ffffffffa26edeb86e8cecf7c790d916b749332cd847f008e5c08cfbe0f588c908fb895a020000006a47304402203202d72b2b0d4fe98176efcff643b02e0d9df799cb747e149b61d9315f0dbabb0220331fe3ad5bc304f5f4e8a654f0e2692c5a2b258fac0e4d3d7af424e9a1eb3b6b4121033654b11f1177a66d3b04cf558053d5869de4b2a77367d26ef3c3d367d35b7a38ffffffffa26edeb86e8cecf7c790d916b749332cd847f008e5c08cfbe0f588c908fb895a030000006a473044022049fc7a6c69284a7b8c968976b07c1d1f94f3f95adcb7c770ef81ed67d28d9e150220465d8a715d7bd4f22f939c8ed369d19d6d44e1f9e098e4585a8947fc2302d303412103503f62dbfc90e1301341990d82d1e6aa22ccbe7c2658b3c627abacec25476133ffffffffa26edeb86e8cecf7c790d916b749332cd847f008e5c08cfbe0f588c908fb895a040000006a473044022053abc83a16ddf5fc5eb7f451ea19514c9c3ede65172ad3fba5bf5505da1dff2602202c9cf1e4aacaa214ea817ae4de37a0f43ee75ae4f52bf381b0d07d3e09f3270941210336ce9263b4f85ff51fa2f7d903075a26d2ba95a50afa66ea61a96dcf2678e9a7ffffffffa26edeb86e8cecf7c790d916b749332cd847f008e5c08cfbe0f588c908fb895a050000006a47304402204b41c6029e7e9913c062091ed2786033dfe3d3a107514af9d42f4a55fe36408c02205a247aaeffe9c3147a58caff48303af7fa6ea16b265e9e824b4e9859ecfceba84121034b10019d624b049158d6016ad832a0edb976b92014798366ed6fde4ca8fda4a8ffffffffa26edeb86e8cecf7c790d916b749332cd847f008e5c08cfbe0f588c908fb895a060000006a473044022062a0cdfcea1cf4e623ca40ed68680246c53558f3fce91f6b932a12e6be2df90502202c73ae50d54cb91dcf68422e29f1f94aef3d8d2b2ebda746ffbbde0286d0bad9412102279379060d9cfb95732743e1b46d2505493baaf07fc817ac7cf979e5edd31f83ffffffffa26edeb86e8cecf7c790d916b749332cd847f008e5c08cfbe0f588c908fb895a070000006a4730440220659ed9f2dccaf8b01d9d712a0980b3e999460420889098770df6691209796083022030d148c8eda92bf2920a17a660c10960e59efa9f22976fc88e8c55ecae661210412103d061268933c820cb5c576d6b152b100590239c602899a2138f9da973eaf6f8eeffffffffb7c88027daaab7bcdbf040c4bf91b97edbc0cf1a16e23dafce3b433803afc757050000006a47304402206621ba51f242050f6d9a830655cb6e18675ccbe2b2071d9c4ce161f0d518c5e902206613b66fcf006b4449e73bb1910df4506e2c7a1a382649c04f999e49c2c39f76412102204e1afb2d4ecc741be8f9d967b7fd71b30e78a1b726b53e0b75790b472cdc52ffffffffb7c88027daaab7bcdbf040c4bf91b97edbc0cf1a16e23dafce3b433803afc757060000006a473044022004e3b07757fe1d2c7d84f2aa67ae750838329181fbc21e9610df62f5cdf081b302200e4f8309ef472b71f3842b0b7181faf7f870515a6aeeac45c0c9e62fe56100f141210246ed75cddc2608c8eb5691201eb0b7bf61d73fea0157303fe08fcc8f0265fd9effffffffb7c88027daaab7bcdbf040c4bf91b97edbc0cf1a16e23dafce3b433803afc757070000006a47304402204663db3a353ac8677677a466044dd46563d8e1c8695025a0c173316bdfd7e20b02207a5c858e483a0350caf0acee1e00703a992ab3d10c40ab6b48e05f2b4efb94914121034eef8b32ea13411ac44d138cd27e29f057180d7f5b8c38633a047ca2a7d86f2dffffffffb7c88027daaab7bcdbf040c4bf91b97edbc0cf1a16e23dafce3b433803afc757080000006a47304402202c16f828e43cefd0367463b2c1bf94f01439e009339c4fcd4ab295632e8f727202202444f42e74d30a4cc8daa674b25d63ab5e7cea0dcf1e99bab0650ac2403f57764121027ff0fce8fac9d54936ae7662b57a433fa5542fb21c7d7b29ed0e1eef90bee615ffffffffb7c88027daaab7bcdbf040c4bf91b97edbc0cf1a16e23dafce3b433803afc757090000006a473044022051516e7a4469071abbd71f70b0afe48999d2fcd6f048155fd8391c8ad88c9a9a022025c21bbe3fc214be1e1f2b0969bb4ce94a3e12f9da7cc61a5dd8d0a7118dc86841210296533f887e4fc34d5a8c650cb9eb050a55079e6e7b8add67b86866a9fccb06ddffffffffb7c88027daaab7bcdbf040c4bf91b97edbc0cf1a16e23dafce3b433803afc7570a0000006a47304402205b29fe34c5ae276d74e046b3b1993a960b01e914a0510d9c13adba27249c8ef702207314435472ab1e3ac2db6b435ebbff30b526ee3676a7b409a0ca777a62d7a5f141210219dc75ac32732775a7b22a4683985fc5d32244ab1c848cd56a47fdf770477673ffffffff3e4331c9ebf98a43a5a00501ccc016a1047196183a50e37f598c331036d44a28020000006a47304402200984872ea22101e7eeed500ae286248183dac65e8d183ec0dfbcc85e2781257a0220440e917af9ded168141daf5b3f047a76e887b525eacdbecf6ebfccab00cf91fe4121036dbf8d0e631a35fa7e8505c9296b5d42fc553acc813ea669e060e7b3f2846a17ffffffff3e4331c9ebf98a43a5a00501ccc016a1047196183a50e37f598c331036d44a28030000006a473044022031f1cb414fb200ff6751dd1d457002f583a8515ce37f3c9b4c05241620f761ce022069df4f4a69312c41af6e8ec4e097e013cb1fd0609300b318333e24cdb9e83a2441210353e89ac71b02356db4462206ec03a30dc8093062decb4cc602fb799bf6fe9359ffffffff3e4331c9ebf98a43a5a00501ccc016a1047196183a50e37f598c331036d44a28040000006a4730440220236b834bd5be035011ff644eaf42bc34e3eb1caf3e3bba0a954c8cdaf27bed2602200bb85b53db5e5057a15aba090eb87e2ad0d8420b40737e02935a61b147ba97cd4121029601a9051202d689d8342be2b9d6fb14e7812a21b299a9ba62932cadcfa6a451ffffffff3e4331c9ebf98a43a5a00501ccc016a1047196183a50e37f598c331036d44a28050000006a47304402206d0f09257ef5cf501ea1d611d0b2d5bccddf594ad545fcd9b5776f7b9ebe3c650220459b4a44374b6f1d2d6c7c522fdcf9d0c6946c69d4f495f11feeb4e5cb54993141210370c4a7bf00b549c9ad60a8b1074c14cf56aca46f65b55b179a9b1ba5f20ffbe7fffffffff48e908c23a2c48a85ce35952434ed2724e825165a90dba90741723e4eadc1df010000006a473044022027937c53006383b7e13ffa97cc033d16e7536d2df06350250ec09d5ffe284b9102206324a46a4912772280d7eb6ceb84998d856e57cd5bded3cd49325e8db571d6dd41210287bbf315eb26ef63fefb9a6549470093894270852289c565aae4daa387d4b70dfffffffff48e908c23a2c48a85ce35952434ed2724e825165a90dba90741723e4eadc1df020000006a47304402207c30b606a24931bf8b04f32b240d348dbf4c974b124f7b879cbf9f244c3e7582022038081469d3589cf5ee981764fd694ee52628474581cc00cbd5b36c0ed028aec54121022ae824c46aa984edc405502824604f0f5c2eb656fb3de5979be8f0b2af9f8473fffffffff48e908c23a2c48a85ce35952434ed2724e825165a90dba90741723e4eadc1df030000006a4730440220764937530609362502b658ec6572fdd1f93f58b00a10c9d99ba8f1c23d4835ca0220286cb459657dcbbc8a2d93997b1e443a28f8d87513213a1a73350b3d395737904121030476a88324e6f616e72bfc63c959ee5aa2160dbba95c6a470f07fb2ca14979e8fffffffff48e908c23a2c48a85ce35952434ed2724e825165a90dba90741723e4eadc1df040000006a4730440220544e93083fb79a876355ffde5673feaa7bd927e8f403d883b43cc2cb7dd991b002200bff81af41c985e6e3a99f86233de7663a80f5b21524198ac2ee0f60e9b8a10c4121037d201296a4b3772fdb2f135f4572982cabb2cded551499125447d3f3d0402264fffffffff48e908c23a2c48a85ce35952434ed2724e825165a90dba90741723e4eadc1df050000006a47304402203e65832027f6f6ee7b55f6f1ef0d474943119958007d5f36e3b574d7587ad6fa02200b2110243c0eead4b4fa299f1dda97a0edf5ba8de13272b7e76d4fb85de7ef70412103ecb31250acf1fce78cea67c526bc04d856c231d95bf84947fb1efdaa7845c11afffffffff48e908c23a2c48a85ce35952434ed2724e825165a90dba90741723e4eadc1df060000006a47304402202c8f07d2db765f7c122d14875c31bda9439e98e686898dd9bda2f73e8083d37702203e9f23c71facce807d684d6daa9c493c186b1c950827b06b8ff616824daffdd4412103383d9d9fc6ad7664c020b6af8295f1d2e02cf37958a85551c5ea26e7280f8365ffffffff7694d10b9e2e0e33dd580b71c19551d885b154d23c6992ee7340d09880e0b825000000006a47304402202832c306b7fc9cf2e57e4526b153c6c5b7f9eaa6c8d28035ecb515d648bb69f6022057f5321657070e6ece84207349044a4b8dbdb2cfcfcaab8f81e303a29404b34a4121024da9148e4f03ec14668a08447ec95955fc001015b915e5506b937fefdeb3ebfeffffffff67ca6698f40a560207ff8d06cd0e20c0060abea3ffaab4f6c97c165ae9ba3f18010000006a47304402204eda30eda46b754a030d9418bd0e45e8b45ddfa56fc7b9d0de1d8434abf94cf902204e8fa0678ad65b2b9c68521cf650129ffe0d2ebd8049f3e79496014bdc86935b412102e5f56e7688c6745d37f5e5a94c02bcaf75e31b1e5924a68a91a7fcc9302da48cffffffff57b055d5ef415fd989ad0b3ae26501b29eafa97c88ab7175d86134f4e987700d000000006a47304402204baa775d1b8338c2aa01ee4cc1b6f0c07cab99fe2193415c8339d5d5be9d7a0b022014de533a2f3d715ab01206e5b4c0eee74aa2045692912e58ac53fdb98b9cc33c412103ff441350f63c7139b5b3dcc2561dddb1256f68bee2c2cda9e264b46016555303ffffffffad4ccc79c4c480a86606937ba57a510baddae84a44e8215c999e19a47cf934f9010000006a473044022004419184d3743175a736011fc849fe2e60b037431acb67b552ade1d79ac48fa102201969884130d194393947d0d82db541e3d3d3593eb15cd30686e4164c91ec7ee94121022209471884cd560521d31b5c7b211f54909918722301b60b60df2ed94a31764fffffffffce43716fa66cfafabc15bbd789c2fbb50da9caa392ac9f0b99a1f7ff38feb92d010000006a47304402207205ae8377fe23f49fa73b317e4984b47aaf04ef38561f2a74346d5c20c3029b022075fef61be9f48119bfc89cff31f091c500d2b09570dbf268c35b7480f981d0594121022dc1f9d705a4a55078b6c4f982f2ebba058d3ced475138f84b32259d9cd365e6ffffffffcae76cae46a0b26719109cad1997861fdb3ede79a1e9808395ff647193310e01010000006a473044022052b844ca9315fea1ba8ec136e54710db1c252fdf51f9814e2fba53fdd7d084e50220331e36e82d0b1467347a469a6c97328f0e051d4b6606085fdb5efa971422921c4121023a93183127eb12139c83080d61edfc9757d030e9884caec87bf444ee8b3c5932ffffffffa3679dfc60f23c4df87907a64fa5c960303358075b7a2839df4634fb8ff8770e010000006a47304402207daba07ddfc7e3dd8a1e3c7dc48da12df1e5d481a056f753807fef2abf03f97c02201456579ad3805538a946da781e7dacc1e7c5f5d28fda74bf2bbb1591d6b83ffd412102d156e46361e04527c4318fe2a7f79f54c9b349b0ffffcbb2ec0e6b7612a7106affffffff3be60af3ea8c90ebe7760b6f0a4da1d9a77539bf12bcd56aacfe6e1b293ef814010000006a47304402206b451172e8bcd8c878f29aeaf75811d7681564738ad5e2bb19d216b7e717bde102200da6bba24aee2a66f276c8075ecbb3c32a75450a630cf8c1045deb57cbfd3ba14121025ddd04d1f92e5616b05dab992c702bb343b06b2e11d68b4f87db62d7c6929b86ffffffffa6cb7e5ac76d559d51d23ad43be27961e8c39f10723fe9c436526110278a687c010000006a473044022041d1c070b55385cc75d85dc6d5706808922cd6cc28fb6e8b0e64530d0d032c4902206b5e8dfb3da2c653e8970c13eebf660a5a8ca0187dfd419a8e6db169cf7275cc41210357bd4031d82034a4e884b9092e29751610d98241140bf293e6f2680e0fd6591cffffffff488f1982bd2054ebb7360c75a1124609314e902d245a7f287e0908a77cf55f77000000006a473044022046f7246642d6a27d08ab8052645ba16eb836c1f809133173680de21f6e38684802202c0b88f525e78ae0873913fcaf338957d77cb234925141939823b7d77df3bcb4412102f4e00a743cf2e9b65484c978e43467c34c774bdf94fb44a18f61039ca19e9687ffffffff889cdefdd2018c5feda897d06d1b51d17f960247c37e98b0933b978be6c925ae010000006a473044022035003052d94e0ac2ad4a45cceb36af9e3f8a751a59bb5661277eb3c206749c0902206e8faea573902370f577b508ea8c70e84d24ee5fcc1d3c45ee7632d09f380455412103cc124e7e237e44a8db4266465e8ebc6ad10d3e695db2e17b8557c05a15ac7925ffffffffdb69efd4b3a9ba1f3c6b0a1095f9f6062767878b73c0e2e3e650eccdb3d9fbcc000000006a473044022052a76713bb0246541b2974a44f5336ac8cdca0508a2cd8c0790ad14d290146700220679a210e70dd39c157cd59da48f51c48eb33c281353074e9efa9bd95eaaf1a25412103110d6d215a91e5ebcd75c2caded19958f75ee489a212588f52e2ec2bbcc8dabaffffffff9ed99296569212418110b4f4c33a303c269d2815ee01d9a262b3ea4eddac651b010000006a47304402203157cd41f4d1c9f972655c0c5895d267aad1bc8d08e98230557f1c2d9d10e4c4022028f1dbd1d6ea89c10ec6e1ff4ee703d1dac765392063c83bfc149bd5ab55fea9412103be9fb1ebe016c487fc75825456ffed5f30f0a9c4a3a7f4f41434c40ed8746a5cffffffffd86038c85a6a87f293a4312d891a5f8680427e3afc335ca672cb7976ccf05fda010000006a4730440220386ef1ea87fc878439b0f507f6475d3faf609e6d1067ea0f3589a381a0c16ad3022031ccc8fcfd1f2c5e501360d67a9a323357531f32c9812ebe7548db6919a0e90541210263c935016f918ec80f5b6c0c26855a67bb4bd9aafc607d739275d1ca8bb2df91ffffffff41169120df6a57cbdacb8238eb1d1574ea6136aeb399b4e610e740ea4f215eb4010000006a473044022052f1c172f0c14040b47ee72cb1178c0999af80db59f290869dfca02b1eb86ff502202a2abe87bf57f2a49d99c6c7c8443cd6bc1437bdf9289d4fe7f935b3103858554121032cdc39f94452bc01906c98df1137ec1e66f216f01a5731a4e70cbbab1ee179f6ffffffff440824eb6ac675ed03c4dbd4223a8b04f56818ab014b493dda304e898e83bcf60100000069463043021f69583261ef921ca9f632c16d737b9eef5782386d183e80c180fc623934d44a02200300b7d01b9cf53d8ca93739f769488f146513beeb20d5e4d9e629fa56cd6acf4121023d58c4c44dc173be680bbbe0e64074868a593d497a29d8b562823a833fcb8d3cffffffff759d64bb6e08550f202415b0c7b23347c4bc7bd1233729bce9fd2ac2ec37a2c1010000006a47304402203fa77deefca26b8f87fcbc2465a16c769dadbbfe7b6eac258a7d2bc6c0be6cda02202b80304f99a2f12c2d7791ffc5c74c6a13eeafdb71d037bdb9df97d9662e59c5412103fb8413d08b2a8ebb3bb888ab04a7cc98560e259759edc801fe00362e397b14f6ffffffff19001d9233146fa729978c17eb0466b8763be9dcaa6d25df1e6cde50ca7a8dac010000006a4730440220263ec8353cd6a094e6f49fb5e82a0efb486547d6d6755bb87283c3e2ae2a774f022072873096dd42cc93c4aa136e9269731cfe6aee954cfa5363a1a61e26efa0da784121030044f93977f8e8bb6765e323c25dff1c3947ee9546d7aaa2b8e22326de408f1cffffffff55d19807b2b0546c5829fe3d5fc9122d1ca1c6c74b2c9f700c90b2b5f4f04b53010000006a47304402200ab921a7d3d3e646cd5c3c8ee24ea38ad1d8be9160220dc725228083604027a20220177964972779c325d5a99485e6889edfa65352d2914106c5c35665817cafb54d412103da633d6f9ebed6ed3e07134c257eaed76ae6b12a0fe21352f0d99f5871d441a7ffffffff3b0478a3277496657ba49868dac6897f97ad7569f5319e8f5de1897954355a9a010000006a47304402205a4a9efbfd89cbc32bd8b22447223ee8a24d2166bea60849f89894651f0a2e3b02203b71b9f8d25cc6b3112f20be62f7f3269f0caa157b721a3b069544e4fdbfa9fc412102ca2b90cc811557431858d2558b09c8c15a54b260b716f0b1f3da0ef7f440a14fffffffff4f8e6e94a174e7688d135064846f0a54efa460bde3152053ec7c208c42ac8976010000006a473044022050e87b003450d06181d70f38e3ed619c5f6480f6fa0bbe42758fe28dade2c3b002207b72318691178f773a6ec1312cf39146c10b894f9b0931d97f372314f9c219ee4121026c4d1217f3800b434211b953378c3e1483e97522d490c673f85eb080fff1df4effffffff06ee57acdc63d5e9d6ec2a7ccf5c8724390e4dfb27958af44382c2de119683c5010000006a47304402207488bf15e379c93a22c401844989e62ce5671ff1e907119d1583dbabc68fda0a02205e609134e9b0676d9411941a9a5298cf7d2c1a859fbd965e47fda8b3f378d37f412102f96bafce3e3fcc2aa8a60e660a9d30e0d2b178533897b13b0fe9386c987a10e4ffffffffea53155f55a2d9cb731e01c1e7f122ab65829c15bbc7fe5fabf46d57d833b04c010000006a473044022048efa9fe04389e8a461a02d9a57861417100de4626a500c46e5b0b6ee4ba740402204a1e7f79fe5ec9cc81edd3f8ff02767aba97a102602c212236d80d5e2dfb40654121030047daf3fc8c65137d433d59ecb56d7a3ff462abafdbf262a97102b91c9d16a2ffffffff9f27f8a6888f45442ef0012e4c9dc84cda2a786ab55cfe90370960a3e9ae9e2c000000006a4730440220365b79a0c2b3a80f51b399f2ece11cdc570b99d426a37a56e98c208add89946202202900de4b25daa54f713a0140d0392f572f28d34284bff6cfb3db6af40fdf13014121024c5618d0e7c4c170cffa76d5ae923250dbe6196f936adc1cb9c91d92b1d9fd10ffffffffd72868be0d0a1366481ffc11999048f2c823e1883f99bc026faf2d23d0be5397010000006a47304402207095f5395d890cf737aa63c5f84eaf5c414b48a39fca64ab6fc91dc73c295c4a02201e2cf699b3af57f5efbf5be3c83d7331e6b0d698288875b86ebc017c9e064b6f41210334b707664f13d00976f4fc8839df38d84985583a0d388e436d2072630615759bffffffff3c47006a1faced503301e97fd91e15a7bff2c349c80b2cc6fd6928eccaf94f78000000006a47304402200b4cf12807d60194f50347c08eac67ed94adbeec40316be8ead65a2783cfe614022026239d0e33046b2b2d478ee36cf46830e504fb0a72195b503364bde7b9311c2041210397b387a14078bb94936a4dfaa41e749524ca107276cfb4743f3eac0177cfac16ffffffff91bec3ec799ac91b2b519291c2dc8e1017fa54980179f0bcf3f7e0ee4a8e0bac010000006a47304402207517abab39f357f321e4d515e2ecb4ecc41eb4291bbda11f1fa314f01d9567ab02200e2f7686fc2cbfa11a2d335475bdc3081969ab600ee25fc6771cc623b75237bf41210362260bca39bb18e069e4201bcd85fb91e5574b5b2f727b15e3388d0113c34be7ffffffff77d7dfd91862fabaa7b4110926240977a2edbdefc12dae46c24f570a667e2566010000006a47304402200cd1e8f65ed8270fb2c6232fd321c5e094b0932d7de789d94eb8d64adff0a7f90220527dbe54982a366bd47f0f4fcee349a8f42a87d8bb0827adc92fbbc90e78cd07412103c8398b1991696402dc0b9cf0f3cd1dc43ed82ab5e67bf0a551a5bc9fa9aae79affffffff5f36e6ab0bbd13287601bc2cfbc83fc4efb41de78bf0dd2cf76c23a020885e79000000006a47304402203da48e614375b6c6f5b2b909d36c68d8de61119d1ee2f12198acfc4e586df0a002203accc89097f95241634ca4fbc5c59170bf7361710e1be8d84b32b19357b0b1ef412102ef6dfc4e5a39663ac1a8dfcf7afcd858defa370d3b6fceb47f78e23f37512c11ffffffffbbffa5c82a00bc83d512e8ea2344e5773f2d32fb8d08ff3943ef127b31e02d41010000006a4730440220429eab60e6d0101aa60659d068de9a322ed747819f97309a5d1ecdca47de5ccd02207c6074729e1923b61cbcaf470620db9a2b0e182e4346e29d1f04b610bc97a5a7412103b484be3253c820a6e083fed54e3fc618984d276982bad31e9db8d5cda6aa7991fffffffff2045072bc516db61251703150368fbc10162b758c6c9777c9aa5dfe23612608010000006a47304402202bf4a96d4493a2906d3c0957a731deb32ee0a13401a71c359b34f6bcd9d2670b022075cd83f5b6c7826dc8ab654d555962ffa5533393204ec7d0838301a7ce00f63f41210218608f387ab6063dc55300d24bebc933478e0d7a931ae7565dec97b15ca1f4f3ffffffff4538a8217413ada82fd7a9d5f7298e7c17b6d6d3a32de647bbad22fa06505407010000006a473044022038e5763755c108cbdf2d4e3e6e6e870bdac6d4b883260f2e9408630eac8a6b5e022039c4ca18126e28a058c0a8a82a6e0f8db38cf3468de666733434413abad2b55e412102b94769a3c3e5a9da4cf8355d4402aad0b2bed638ff19349beb76a81bc5114841ffffffffbd2fc2e98b6206ad241b299d9579b766cac39a132636e18ed15b4d8dc0eff966010000006a47304402200d495a82b49a4e39bad3f2c725db6bf2eaadebdfd27bc8e695ff88ea1cc0832d02204afa5ca6c3a11edf72afea1f894019e87d777f752c0d7f1e78765f3ece222b44412102aaacd2ec5415a2e5efa8b90f9b17614f2d548b87765b145a6f2f89256119d341ffffffff18c24693a037e2612ecfac6b38cb36ab5b8cff94c2bfbcf1a0ad6dcce075a4b3010000006a47304402203bc2f14161796c7eaadbcd80ddf1b9d437870293b91cffd3f9cf6eb04ad0087702201e8911c4d73a52b0fb4b1f8eeac30c0de2e22bdda856ae4040a5ffd3c0dd89ef412103b0b87e8981abb90fa186d4e866b5e816f3763ce6d1de136ad3feb8246177f895ffffffff6b061d9dc5ca5022cc4f5fb02951e73045f0a8cf53a9a5b44ac6c396c159fa1e010000006a47304402207c8124d01aae45e29ed4f9ee1e5b5545cf2b5022059634be9fb26a445ef6f2e002201975ce603071fc5c4145d652d49b2bc6c1d991d44c5032f1920b7ef4b1969929412103fe16c19f043e99aef36a475554e4706ad99832d5e13ede0f1559b298e2f61ffbffffffff2c7a352055a075acdcf85d16d4c477f060a86d0b43a0205165586c550ded88e8010000006a4730440220544fca491fb9538ea15f103182e3f0f4f3b342a0c6a5a1db99506b111ec99116022069663b9a52f262af7dc82d42307ef49356d247c126625f5f33e4101c89236b5241210395c323c7e0fe534671a16dbc622b671b35e69f9526fb298f70ee58dd90c89d2dffffffffe8208d3cc6fd6b56ba9dbda5cb21259672deb19c5f32502d51806137eb12da49010000006a47304402203a2eb6dd1844342473f4ea06ef3434492d630b66fbd121ac9fc132748292e576022069ef70339360c45f0f0df2660092558e9429a0321439b33c32f2bdf6bd4db6b44121033bd369aec607dc3f80a42d48b45d337cd4a08c58888f8c02ab90920eba42f1caffffffff2a97afd9168d221d8cf36d29de9ea9cb315e0eec023180425c586342db44a243010000006a47304402204ff94d5b04ed70d1c5062f918982c5f5d20b305cc2dca9b67a61cf430d77ce1a02207313940096cfa50c14c06febaf564e9599fffce0f4ebf25e5626e284f03a12914121037f6c1a09ebe88c93adfd9b4404e547b70efc424b0534044d3b28fffc80ae06f7ffffffff3275a231741780c10c3fb70eec352f9e28f5ae48f0c1498c2d9516fb5e7cfb5f010000006a47304402206114be8df511390b9aef15390e331f7e6fe944aeaf125284c4e1d30ed1497d1902203c3f9294314fcb77be56d7f9ae4ef621592e1dff8edeaa1dc665406a6159b9f1412102664ea178ea65c44ae1e93e8fdce505db41a745604006871c8a44fbea9b8d5d3dffffffff4300523b83637a36431949d5bede9b52e3cbd0d75db6bbcfa9a7b871cadaa983010000006a473044022076061686f484643086edfbbf8477cb20f56ecd49f5e387edbc537b8c914df6cc0220707df04d202deb37a4cef11128dfa284202ba543b16b4aad7112fed3267841e14121038a299a4f7dee8632086525aec8a6015569906a14f8509fef436ff1bc4b9bdd57fffffffff240452e9c46ebc545bed17e0311f8ee74f74c7c2d67d500e21628482684da93010000006a47304402200910f4685167b10e9093b5e335e15e519978d504d5caad83c5b59af8a5ceea12022045f2ea00a92dfb5b8c8eb50c46e72bcfb508a4d97472fa814ad1577eb7d72ace41210388fb29f888339dc641b2c351ff7f1f2e806a31e9f67ec316ceda9c390a69aef1ffffffff16c7bb16fc470be4378c684ab0aeb771350d56e320f07d5eeb2a85cef301ca53010000006a4730440220413497d3c01fe8275918914091078e1269f4e12c2d670c11cca58f96d8439a1e02203915c339de35f9278bc494a5222fc05b191fa6591881f5f3ee9a15b61ddc25624121023c9e0a7a8f47d46c91e2cfb59d770ec858fb66eeb1d43367834e3024346b7cb6ffffffff4e960dda5d67de9b48cabfca5b2ddd444ea41c6f320cdbf70a15145302d3eda4010000006a473044022072dfad3f34a9339f35b055b32fd6d28ec053d3c0e1857ccba9f9d5eb2d9cdce602207d7345896282dc4b993f5d190118b815542d2e32e6391d5ac9cc64e176dbf99a412103d339fdf5700f3651cdb649ecd31676d162f2653340dd01b8f4b94d54f6d8034bffffffff65e204227de78193f9a13c0879bbcd9bd21eb04f20779aabc7c9212197295236010000006a47304402200a17e351c7596b4fc86f14896419414f74feebd53cc226629c59564e06ba43e3022061cb7c480492ed6cfc5e9a79ad2738ff4f05cf42ed27279d9cc25a9cad1329b5412102989c7c386d9abdf30a5529b9b20076ead9366c46c123446674c38380de3fd5afffffffff03b32c403b1e509d8da11f6b8006d128998b6fbaa55bb4ce90ee918326bdc148010000006a47304402200e51a134244592ed3443e97f3da05e9e18028de855081f242a08c6d191fe67eb022064376b089b8b240296eee435f50fbd59219bd4cc1eec44acf8bc6b39be791dbc412102616acf6f41e46680c84d66c56745644f75d6e5a3fabda504101248cd50315d20ffffffffdb9fd3f41836e74458c0ec9ed5815405ac0ae3337a5b45168af2c26543adc547010000006a473044022057cbaebc834e3f1bd4b39dddb568b0eb4e864cd1385c3010045a56eaca04fd01022078ee0f6de2e539641c3be3648393e81e29d517457be4169dc31b8b79b4fb23c3412102681559b59c18a23e729d08627544ae6de8712c9da4d3507f736396c3a0972da3ffffffff46ecac9b145c6621ec5625d648c9b718fc5602b787ce4eea50b13255931ba303010000006a4730440220375993714040e18a3dd2e969be6337801b06b464ec5204020499d4a3adf06a5502204ffc9f8aeebac656e2fcf9a7e1ebd13a6e49e3ef8ecacabbdee2cef9c5ba954a41210370734c6626410c7c71557aad742253fbf0abe30904c3ed170e9e18162f810598ffffffff99a61955830a8b706614706aab86461537e63ed9d9be857b313bbed0237be629010000006a47304402205ab30a782985dc3c8df6f1707f1a474e5cb8f43791207781b07af9c22312511f02203cc0a1099bb53873c68fdc76dff6f1df9392617c4fd9d2f3735333e0b73f2bea412103190d3c155f4ccb10896a097b4b5a4c124566e0218dc5741f832a0b57a2b8c6d0ffffffff21fa68a780ec2b4204c94f8acc32f84f21b58c4bbc23d72798da62bc3b1504c7010000006a47304402201313c25b0aab8ea85dcc3c750b4bf20df2cc86355bd8dc4c2aa6e554f5c4bc7202203f7ee7f8606e3994d8854548403017b3a0236705cc50900838d7a715fdf46ae541210311456ff04873338025cf17e2010227aad8cabc771bb0124293ccd5da2a0ada60ffffffffbc066a92fdb86733596426d888cf784652005a32efcddd818dc9103a51e94565010000006a4730440220590029dcf2911acc146428d1d7c03ff26771f07176e895aecf2c75793ff3d77c02201289f6a5751543a1e36b8c6b5c3332469a099b8ae70b08ef21d392e7011f619541210307a01ecaacd645bab9a29521d53dc87cc80071f1f5d41777ff3dd52f163dd9c9ffffffffcc0bc62c074157485c9d71e78e885ca792fb9c4e76ff3340dd8c60ae88109c8c000000006a47304402204e82f1e01004d57fad2304b53a9cccfaf96521dee0a2f74dfdbe742b6107594e022054e2beae003a7b2dedd035c67a4bf56f8e7875e2b6108b179eecdc7f8539288341210239e6c705fccb786a0ae8de8f35ebf37242730d13f31668f16b6da64284dae0bcffffffff4a14658ae83d01039b9df1a1ab93fd256da740cee8b1db2732e30f91417f3d41010000006a473044022043e93c6b4abbe0313ff90a6db6dc4cbbbd03137055e0fb0c60a8fe306003fbb402205d8d3b9aa14e5a0508867cbb945a461408f71e626d0e8c2dc59c95a9aae5517941210233199ae1827e5f339fee6a0f031469579f58f7b36b30da1dc1b9ca5c332d9364ffffffff1e532bf729ebcbc0188e68cf42b521624a0c1f8e29853a8418bfed89c4638e4a010000006a473044022019691750f12f57c75fde0b857fb47147704f025211eba39212001f803248a34502204fec73d7db765033c5b076a8164ab04dbcae9bbf65d3707b7ca7d7de8f9319c341210209df969f9aa38e2ea8be5ae2ad126f73b13317d89607e6e608657e069093ffcafffffffff6aa67b6eda80c539966d51e453735fc2b61d915e12e3278d9ad3b068b93f8cb010000006a47304402200be2d564fd267819ed3181b9cd51d20f54e8969c3f8cfb5f22eb99aeb32cf71c02202fece51d2cd7475a288a11ef69a61b74dbff198980bec8055dbc7f3bc1841d43412103f82de1af50da19cdcf5c32113d1b62b706c618d832b90ba07e4e04e6ae4a94bcfffffffff681d8c580a5229624f77a6ce4fd2579e8af1bc10f9d9834f0618d7976908dec010000006a47304402202bc8c70f3060c49e5a8bbded8065d757386c2c55e1767ff08f51cdbf3504f241022040b39797f78128ce7d9ba922ed9a082eb6c4db830112cfb1e2a46bc05b0d33ec412102456ccdf5f47e0a7f5db46a4358c909139d64c0c42f77c5e3aba3eaad590e99afffffffffbee5b003129f7c8048f59718e7bd38fb40ce6430ed98855a46727488c308d1b6010000006a47304402203055ca2de22670cc6766694d047e91ad4c20573edab93af8388a2d55148ea48902203203753bbd59f1388c8ece82055878a4be3f077890091ff67f571fcbe5e54c714121026d3511cf7b4538fcc4b9f8be27235b6f7c4c94606461a4a261051b68bc5660a8ffffffffb2bb434300c8611ffad5826aa276d7e7fbfe578415b2d8af0a5905a0e1711cee010000006a47304402205a98991467ef00af9964eb1ed994414b2d0d3d45a6277668b677ab3c99de0511022073a62993a1561a5e16d8c418fe04c698d452a6b6207adc26ce58080848febc4041210384b434dcc1b0ba45aba8dac3be4e90c3a32dd6799e00f612bf3171fe7d6fdd46ffffffff22f7058b2d266a59d1c606d7a5600b181e3085b12c1882da134c0933d6650169000000006a4730440220433a04a42c8d894d6da9ddefac2e0a73898b28bbfbd0be2cddf97560164c1dd7022055bbab853d8315cf8cf063756cce2e0840f4ef1cbdbcb7e8165b0fc653b947204121023c0e047e677c1362272b05ead52377b06b89d524f52b036d88f30ace99c04c2dffffffff0b719ac93bc107789c0dff8b3204df9e51167987946866a02089e38c872de611010000006a473044022026b7b80473cc024bdffd8c501fd2ce909c747cbc905c21cfea3cfba684e1c6e602205a7c6e38c69b7fde58a790fb97e89a270087f662eb6d950ef9b97abb744804c4412102d8a040047ed87ab63d9316ac05fae6d1db525d65e577e40317af187e116e2f4cffffffff57152dd7b845c94058dff02ed5bd6c6e9ddf590aa18b12ae23993139dd85c5c9010000006a47304402205471de23d9f4ba888d2f45a1f070be9279c66f7081b98d8e828c59461218e52402201ebcd5ed0aa33980f8d5626d46f9523927da3bc1b5c49162863f42e7c46aa37c412102c3bcb87862db9872975fedd0ee09bf8b25d28fbaeb8fef5988b4081e1ad344a6ffffffff31e7d3d9126347597174ff7c56332e8cd11a55ab98ba6f730d6e8e2d76877dbc010000006a473044022067547644b01f2d89bc4f77c319780493f2fdd383ae08a9ed39f4852eb855f53402206c333b2e7a3ad786413e69beff83238a48908196709727cb57d8d9ee47b80b21412102b36ee7a01c3ec335fdd8e1077fb4e94758cb31b9cdcef28fc9bbe343cda63dfdffffffffe798697e76f59572bb5766aed4da02006a3cda27380790a5c547db5ee5dec439010000006a47304402204750a426cb6d2bb6671478c8e573f5f57810497c460fdf784d810d03b2caa48902205067f32da67adb98f7f90d11942118167be1cd76fdbd1cb505e9192ed71dbc1e412102b895dbb3561b748bdd4eab591e198878d49a9912ce1de22327fe5098d1bac017ffffffffab32986f6f79d88c13472a1741f8498428c7b5fe34acb5e5e7badd061728acf6010000006a473044022011c3b82a8d9b731b26b992ad4a2c2927327d7b8acd8b98d3b99ebb3da523651e02202ba1c3aa50bb912fe13f1263852ff050238ddaf5a884465088bf767f72c6bd67412103715e69802a71f11d4975e5f179b40e5e3cd5cd40b91febbbce25b4ab890e4033ffffffff8b07ae6d6f8b6a9fc733f692d3c9293ba67f76a2b3faeea4407a50e85c042c20010000006a473044022075d6dca3063a9ec4976d9d5bb8570231f925bbadbcae21d56e69c75e73bfbc920220510cb5ace6b872a84a5fae0cf63105f62ae057fd83225283ca98eceeb0ed7c64412102571770c2f66a93af0556e917d6a4be128b8f5d88cd303bb16930863d1945782cffffffff0242ba4f718e7f101907b6f11f53b86923e8444e123e89496443538ec850167f010000006a47304402202c06953a519c13a12298c157156eb392a7c21982a3cb97cf1bb1c83bb2a4420602200d225f7cedb859ac3fdeca0837e3d6b5fb73e9b1093a4655ad96c9e8d739e23941210221aaf8ee33437b3e6acc04ca145e137845de337263862e61df5bb6a10c52a656ffffffff71ebc4bbd503be272827b71cc32b63a98449c440661021ef690638a85056bb3b010000006a47304402207e5134149bfa21dac918ef425c5f6fceb9ffb391a99475d540d91367c9dde0b802203f5c91372547059709c082d814381766b0aa467d1a5930d6404034b94d63f23a41210334b9b50c16cb73c92ecd21f34a67ebc0919e964dd63c4877425e005bab06d2c8ffffffff6c2d37c3b286e27bb99d0b2bb03ad57b9e703b05098c5e509af84859bf07a23c010000006a47304402205d6d95e754f208277ad2a882cbb3fe5536f104a4d613debfd32a7faaa91023e90220065344026456d015f61b834d15bfcb8072d9a31f5f823d35f41ddfbe45fb548841210361781bc5e35dd23a631efbc7f896c40fca9d5c8b80b92453f24cd10f8c7dd4c1ffffffffff71b03f947eb8e00fe13668a08422c7d43bc691b215bd8ce8df52321360f9fa010000006a47304402201736559d20bdd56c7e296b8dd8db63744f2ee50067817b7429f3cf1cb45be72d02202faa21fd0de99767e3300cf87cd31c23d862fe2c9905d4ba8de603e9d98b7523412103b09ccffd95fb4db02035d8951e5b9d03b22af0f8ee0aa63afabc8d05c61e8d76fffffffffb283a61bd6d7919a64d1556661fb8b0cee8b261ec86c25f1e4d5424282929d3010000006a47304402204e8af66b68620e72f4ffd54f5b9de5b94a54a28fa6076cc796fc5e053f436aa402207d27f2ce874f98991bfddd0ac617249a1521109c8e73f84154453db409b012b0412102dd5363c9a9c8a33c08364a7c33227cb8bdb2ee8b8166052e3e8deb8298271a0dffffffff49956a0285a60fe2d14df125a84fda906e29925fba5472312298f1cae00f1bc9010000006a47304402206ee6fe5e126142b126212b586a509c75999a3734e8eab1268ace52bbab7dd64f02201414e8db175b6d6206c92f94bccb16bdf91b1f5893baaad32a2d2cc1723e4663412102f615736da5236aff3df5bc0817d63e92b99438484c107ce655c0f185ace516f5ffffffffb440ea492cd5a9306ff980a61e0cd915651a36c638cb31368bdea52360a9b732010000006a47304402200546ff482b9ba0410b1fd1b3e6f45489b80632c31093755acca69a73d603d0280220732be1af6fcbc8e8a0980e73c2fea3d971c1a09a99a002fcb51e39da57558f654121025fd8c66d9b6b448650ec552bf60fa674dac7701cc364f6b6c732a38fd7009bffffffffffb3532572e8a42ba3d18ab712c1017de60b3009ced68ca058e061a5284b57cdd1010000006946304302203e128b83bb81fcefbd37a49c354c92b07193f4ef04c428613a01679cf8dad6d4021f427fca9835631c447f031d085d03290a00f9bcbee69fb389d8efb298283b8a41210302b66e0d1d755c3f3aa39f4e52c0d7d434f3e6c670601595b9ebfc69fc10adddffffffff72c92845a3b09ebd79bc7fc5739c4620b6f75064d3c5bfffd529337f995cc39f010000006a473044022032defd47e42edc30a8b26d4d177bcabfba5711707eb23f610d6e6199e6ce90400220120bcbb059e55056fc27fcc6a91a05567d81a66b087b37ef0c030d5c31a23b8c412102e0ced693c3ebc3f84f45695a931d847ddf7047668e1db1e0ae62dbadd67364f0ffffffff5e977a62b452e8da3b6b30df01b5756ae69914777ad3a5bb98374477c15309a7010000006a473044022037de85d3ea9ce8e966fe4e39cd5cb61be17a46fb0f83a21ba67101cdf83a1ec2022037805f3e6784d0e512ccaad227e7aeeedef509f45d6abfab1a03b43a212c412541210205dac12934f8971a126d934d75f953f48c6dc6d7d9ba0c65803d3ef17d1f8fadffffffffb7c8d1c9240d00a60a8bef80ce36c11069037eb68a146903ae5c77421212edb6010000006a47304402205b3551b3d9d44aba7eb8414e92ad49602e548d584e3986d6dbd2feb67b6e2a2f0220093fd6f147bb1c24da0d16bd40c0d7c2a5234ebb01268bad7d7c2f6cb19c73494121027b6ebdada9bf6bba27f140d9401b9372ea2b9f292e5fa7559ac2d5270fc5cb1bffffffffdadd1975d2461dc6e35f2c9f363712283b478bf5e62c9c8d40e7c70afd332c8e010000006a4730440220719cbd45ea954c17f372e194b3befab033f037b03c79f45467e91870131f30be02204a77b9516ce0343aa4b47367436faf51af2f4588eeb075bdbdc38b3f77460eec4121036fc399bcbb11e17481474541dc43a6c3a5e065d7fc6588836e72eda24a568a4dffffffff359332f0e45656c1e2ab1085ecf5e69ded19311aa49c8ef9edb86105bdddc140010000006a473044022001cdbbaaeb86bcfa0dc65bf1ed306275423b537362bce67211df65026344d52b022011c178bc6d1df21da80378c303bc5ba9e4b04f2670be12b67a37e82579fb085d41210240ca38e4cc524a3998a3312cebea1b35db30a992a2485ffd1e40d85ed938b39dffffffffcd413baa689980651250b27db4dcfd23ae12c428630da2a57e56c390e5e36caf010000006a47304402203ebaa8eb1c8bd989ca2eb94f20e83dc19737a6e1394f819313b1ffe434718e6d02202b4d12a75717378ad5b3091f4e46321dcb5de2a71e72948549ffad0c2403a90c4121029407e5ca603d387382aa607d627784006a484a591c730b1590d8028d7ef69bbaffffffff158d8582f99df4cd4f7ea0441dc30e591ec5bb6fb7979048b81c2115b627a31e010000006a4730440220298ee86a193232429141dc0b11fc1a8c45c7959bc3f0a4eb06f2587d3ea5a8160220262b5d4acd557b6a1ef24eb31db274305fcb2f70529795d01f46b74c0090ad3d412103d00ffce12d00aec54f606c8f0f88821159891148a16beee08cec976dc515ace5ffffffff01bd43d552000000001976a9148322b8999b880eda6c8eb28ae8423b6c4a692a1b88ac00000000")
	require.NoError(t, err)

	// Add PreviousTxScript to all inputs to make it an extended transaction
	for _, input := range consolidationTx.Inputs {
		// Create a typical P2PKH locking script
		lockingScript := createP2PKHLockingScript()
		input.PreviousTxScript = lockingScript
	}

	isConsolidation = txValidator.isConsolidationTx(consolidationTx, nil, 0)
	assert.True(t, isConsolidation)
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
	require.NoError(t, err)
}
