package validator

import (
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/go-chaincfg"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCheckOutputs_P2SH_PostGenesis(t *testing.T) {
	// Create settings with Genesis activation at height 1000.
	// Copy MainNetParams to avoid mutating the global variable.
	mainNetParamsCopy := chaincfg.MainNetParams
	mainNetParamsCopy.GenesisActivationHeight = 1000
	tSettings := &settings.Settings{
		Policy:         settings.NewPolicySettings(),
		ChainCfgParams: &mainNetParamsCopy,
	}

	tv := &TxValidator{
		logger:   ulogger.TestLogger{},
		settings: tSettings,
	}

	// Create a P2SH output script
	// P2SH format: OP_HASH160 <20 bytes> OP_EQUAL
	p2shScript := &bscript.Script{0xa9, 0x14} // OP_HASH160, push 20 bytes
	for i := 0; i < 20; i++ {
		*p2shScript = append(*p2shScript, byte(i))
	}
	*p2shScript = append(*p2shScript, 0x87) // OP_EQUAL

	// Verify it's detected as P2SH
	require.True(t, p2shScript.IsP2SH(), "Script should be detected as P2SH")

	// Create a transaction with P2SH output
	tx := bt.NewTx()
	tx.Inputs = append(tx.Inputs, &bt.Input{
		PreviousTxSatoshis: 1000,
		PreviousTxScript:   &bscript.Script{},
		UnlockingScript:    &bscript.Script{},
	})
	tx.Outputs = append(tx.Outputs, &bt.Output{
		Satoshis:      500,
		LockingScript: p2shScript,
	})

	t.Run("pre-Genesis allows P2SH outputs", func(t *testing.T) {
		// Before Genesis (height 999)
		err := tv.checkOutputs(tx, 999, &Options{})
		assert.NoError(t, err, "P2SH outputs should be allowed before Genesis")
	})

	t.Run("post-Genesis rejects P2SH outputs", func(t *testing.T) {
		// After Genesis (height 1000)
		err := tv.checkOutputs(tx, 1000, &Options{})
		require.Error(t, err, "P2SH outputs should be rejected after Genesis")
		assert.Contains(t, err.Error(), "bad-txns-vout-p2sh")
	})

	t.Run("post-Genesis allows non-P2SH outputs", func(t *testing.T) {
		// Create transaction with non-P2SH output (P2PKH)
		txNonP2SH := bt.NewTx()
		txNonP2SH.Inputs = append(txNonP2SH.Inputs, &bt.Input{
			PreviousTxSatoshis: 1000,
			PreviousTxScript:   &bscript.Script{},
			UnlockingScript:    &bscript.Script{},
		})
		// P2PKH: OP_DUP OP_HASH160 <20 bytes> OP_EQUALVERIFY OP_CHECKSIG
		p2pkhScript := &bscript.Script{0x76, 0xa9, 0x14} // OP_DUP OP_HASH160 push 20
		for i := 0; i < 20; i++ {
			*p2pkhScript = append(*p2pkhScript, byte(i))
		}
		*p2pkhScript = append(*p2pkhScript, 0x88, 0xac) // OP_EQUALVERIFY OP_CHECKSIG

		txNonP2SH.Outputs = append(txNonP2SH.Outputs, &bt.Output{
			Satoshis:      500,
			LockingScript: p2pkhScript,
		})

		err := tv.checkOutputs(txNonP2SH, 1000, &Options{})
		assert.NoError(t, err, "Non-P2SH outputs should be allowed after Genesis")
	})
}
