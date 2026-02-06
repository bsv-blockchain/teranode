package validator

import (
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCheckPrevOutputs_NullPrevout(t *testing.T) {
	tv := &TxValidator{
		logger:   ulogger.TestLogger{},
		settings: &settings.Settings{},
	}

	t.Run("rejects null prevout (all zeros txid and max uint32 index)", func(t *testing.T) {
		tx := bt.NewTx()
		
		// Create input with null prevout
		// txid = all zeros, output index = 0xFFFFFFFF
		input := &bt.Input{
			PreviousTxOutIndex: 0xFFFFFFFF, // uint32 max value
			PreviousTxSatoshis: 1000,
			PreviousTxScript:   &bscript.Script{},
			UnlockingScript:    &bscript.Script{},
		}
		// Add all-zero hash
		_ = input.PreviousTxIDAdd(&chainhash.Hash{}) // Hash{} is all zeros
		
		tx.Inputs = append(tx.Inputs, input)
		tx.Outputs = append(tx.Outputs, &bt.Output{
			Satoshis:      500,
			LockingScript: &bscript.Script{},
		})

		err := tv.checkPrevOutputs(tx)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "bad-txns-prevout-null")
	})

	t.Run("allows valid prevout (non-zero txid)", func(t *testing.T) {
		tx := bt.NewTx()
		
		// Create input with valid prevout
		input := &bt.Input{
			PreviousTxOutIndex: 0xFFFFFFFF, // max index but non-zero txid
			PreviousTxSatoshis: 1000,
			PreviousTxScript:   &bscript.Script{},
			UnlockingScript:    &bscript.Script{},
		}
		// Add non-zero hash
		hash := chainhash.Hash{1, 2, 3, 4, 5} // Non-zero hash
		_ = input.PreviousTxIDAdd(&hash)
		
		tx.Inputs = append(tx.Inputs, input)
		tx.Outputs = append(tx.Outputs, &bt.Output{
			Satoshis:      500,
			LockingScript: &bscript.Script{},
		})

		err := tv.checkPrevOutputs(tx)
		assert.NoError(t, err)
	})

	t.Run("allows valid prevout (zero txid but non-max index)", func(t *testing.T) {
		tx := bt.NewTx()
		
		// Create input with valid prevout
		input := &bt.Input{
			PreviousTxOutIndex: 0, // valid index, even though txid is zero
			PreviousTxSatoshis: 1000,
			PreviousTxScript:   &bscript.Script{},
			UnlockingScript:    &bscript.Script{},
		}
		// Add zero hash (but index is not max)
		_ = input.PreviousTxIDAdd(&chainhash.Hash{})
		
		tx.Inputs = append(tx.Inputs, input)
		tx.Outputs = append(tx.Outputs, &bt.Output{
			Satoshis:      500,
			LockingScript: &bscript.Script{},
		})

		err := tv.checkPrevOutputs(tx)
		assert.NoError(t, err)
	})

	t.Run("allows normal transaction with valid inputs", func(t *testing.T) {
		tx := bt.NewTx()
		
		// Create input with normal prevout
		input := &bt.Input{
			PreviousTxOutIndex: 1,
			PreviousTxSatoshis: 1000,
			PreviousTxScript:   &bscript.Script{},
			UnlockingScript:    &bscript.Script{},
		}
		hash := chainhash.Hash{0xaa, 0xbb, 0xcc}
		_ = input.PreviousTxIDAdd(&hash)
		
		tx.Inputs = append(tx.Inputs, input)
		tx.Outputs = append(tx.Outputs, &bt.Output{
			Satoshis:      500,
			LockingScript: &bscript.Script{},
		})

		err := tv.checkPrevOutputs(tx)
		assert.NoError(t, err)
	})

	t.Run("rejects transaction with one null and one valid prevout", func(t *testing.T) {
		tx := bt.NewTx()
		
		// Add valid input first
		validInput := &bt.Input{
			PreviousTxOutIndex: 1,
			PreviousTxSatoshis: 1000,
			PreviousTxScript:   &bscript.Script{},
			UnlockingScript:    &bscript.Script{},
		}
		validHash := chainhash.Hash{0xaa, 0xbb, 0xcc}
		_ = validInput.PreviousTxIDAdd(&validHash)
		tx.Inputs = append(tx.Inputs, validInput)
		
		// Add null input
		nullInput := &bt.Input{
			PreviousTxOutIndex: 0xFFFFFFFF,
			PreviousTxSatoshis: 1000,
			PreviousTxScript:   &bscript.Script{},
			UnlockingScript:    &bscript.Script{},
		}
		_ = nullInput.PreviousTxIDAdd(&chainhash.Hash{}) // all zeros
		tx.Inputs = append(tx.Inputs, nullInput)
		
		tx.Outputs = append(tx.Outputs, &bt.Output{
			Satoshis:      1500,
			LockingScript: &bscript.Script{},
		})

		err := tv.checkPrevOutputs(tx)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "bad-txns-prevout-null")
	})
}
