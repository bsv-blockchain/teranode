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
	"testing"

	"github.com/bitcoin-sv/teranode/chaincfg"
	"github.com/bitcoin-sv/teranode/errors"
	"github.com/bitcoin-sv/teranode/model"
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
	{
		name: "TestScriptVerifier - Empty Tx",
		args: args{
			tx:          bt.NewTx(),
			blockHeight: 0,
			utxoHeights: []uint32{},
		},
		wantErr: assert.NoError,
	},
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
	err := txValidator.ValidateTransaction(testTx, testBlockHeight, &Options{disableConsensus: true})
	assert.NoError(t, err)

	err = txValidator.ValidateTransactionScripts(testTx, testBlockHeight, testUtxoHeights, &Options{disableConsensus: true})

	assert.Error(t, err)
	assert.ErrorIs(t, err, errors.New(errors.ERR_TX_INVALID, "max ops per script policy limit exceeded"))
}

func TestMaxOpsPerScriptPolicyWithConcensus(t *testing.T) {
	tSettings := test.CreateBaseTestSettings()

	tSettings.Policy.MaxOpsPerScriptPolicy = 2       // insanely low
	tSettings.Policy.MaxScriptSizePolicy = 100000000 // quite high
	tSettings.ChainCfgParams.GenesisActivationHeight = 100

	txValidator := NewTxValidator(ulogger.TestLogger{}, tSettings)

	err := txValidator.ValidateTransaction(aTx, 101, &Options{disableConsensus: false})
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
