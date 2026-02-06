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

func TestCheckConsensusSigops_PreGenesis(t *testing.T) {
	// Create settings with Genesis activation at height 1000
	tSettings := &settings.Settings{
		Policy:         settings.NewPolicySettings(),
		ChainCfgParams: &chaincfg.MainNetParams,
	}
	tSettings.ChainCfgParams.GenesisActivationHeight = 1000

	tv := &TxValidator{
		logger:   ulogger.TestLogger{},
		settings: tSettings,
	}

	t.Run("pre-Genesis allows transaction under sigops limit", func(t *testing.T) {
		tx := bt.NewTx()
		tx.Inputs = append(tx.Inputs, &bt.Input{
			PreviousTxSatoshis: 1000,
			PreviousTxScript:   &bscript.Script{},
			UnlockingScript:    &bscript.Script{},
		})
		// Create output with 10 OP_CHECKSIG operations (well under 20,000 limit)
		lockingScript := &bscript.Script{}
		for i := 0; i < 10; i++ {
			*lockingScript = append(*lockingScript, bscript.OpCHECKSIG)
		}
		tx.Outputs = append(tx.Outputs, &bt.Output{
			Satoshis:      500,
			LockingScript: lockingScript,
		})

		// Pre-Genesis height (999)
		err := tv.checkConsensusSigops(tx, 999)
		assert.NoError(t, err)
	})

	t.Run("pre-Genesis rejects transaction exceeding sigops limit", func(t *testing.T) {
		tx := bt.NewTx()
		tx.Inputs = append(tx.Inputs, &bt.Input{
			PreviousTxSatoshis: 1000,
			PreviousTxScript:   &bscript.Script{},
			UnlockingScript:    &bscript.Script{},
		})
		// Create output with 20,001 OP_CHECKSIG operations (exceeds 20,000 limit)
		lockingScript := &bscript.Script{}
		for i := 0; i < 20001; i++ {
			*lockingScript = append(*lockingScript, bscript.OpCHECKSIG)
		}
		tx.Outputs = append(tx.Outputs, &bt.Output{
			Satoshis:      500,
			LockingScript: lockingScript,
		})

		// Pre-Genesis height (999)
		err := tv.checkConsensusSigops(tx, 999)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "bad-txn-sigops")
	})

	t.Run("pre-Genesis allows exactly 20,000 sigops", func(t *testing.T) {
		tx := bt.NewTx()
		tx.Inputs = append(tx.Inputs, &bt.Input{
			PreviousTxSatoshis: 1000,
			PreviousTxScript:   &bscript.Script{},
			UnlockingScript:    &bscript.Script{},
		})
		// Create output with exactly 20,000 OP_CHECKSIG operations
		lockingScript := &bscript.Script{}
		for i := 0; i < 20000; i++ {
			*lockingScript = append(*lockingScript, bscript.OpCHECKSIG)
		}
		tx.Outputs = append(tx.Outputs, &bt.Output{
			Satoshis:      500,
			LockingScript: lockingScript,
		})

		// Pre-Genesis height (999)
		err := tv.checkConsensusSigops(tx, 999)
		assert.NoError(t, err, "Exactly 20,000 sigops should be allowed")
	})

	t.Run("post-Genesis allows unlimited sigops", func(t *testing.T) {
		tx := bt.NewTx()
		tx.Inputs = append(tx.Inputs, &bt.Input{
			PreviousTxSatoshis: 1000,
			PreviousTxScript:   &bscript.Script{},
			UnlockingScript:    &bscript.Script{},
		})
		// Create output with 100,000 OP_CHECKSIG operations (way over pre-Genesis limit)
		lockingScript := &bscript.Script{}
		for i := 0; i < 100000; i++ {
			*lockingScript = append(*lockingScript, bscript.OpCHECKSIG)
		}
		tx.Outputs = append(tx.Outputs, &bt.Output{
			Satoshis:      500,
			LockingScript: lockingScript,
		})

		// Post-Genesis height (1000)
		err := tv.checkConsensusSigops(tx, 1000)
		assert.NoError(t, err, "Post-Genesis should allow unlimited sigops")
	})

	t.Run("counts sigops in both inputs and outputs", func(t *testing.T) {
		tx := bt.NewTx()
		
		// Input with 5,000 OP_CHECKSIG
		unlockingScript := &bscript.Script{}
		for i := 0; i < 5000; i++ {
			*unlockingScript = append(*unlockingScript, bscript.OpCHECKSIG)
		}
		tx.Inputs = append(tx.Inputs, &bt.Input{
			PreviousTxSatoshis: 1000,
			PreviousTxScript:   &bscript.Script{},
			UnlockingScript:    unlockingScript,
		})
		
		// Output with 15,001 OP_CHECKSIG (total = 20,001, should fail)
		lockingScript := &bscript.Script{}
		for i := 0; i < 15001; i++ {
			*lockingScript = append(*lockingScript, bscript.OpCHECKSIG)
		}
		tx.Outputs = append(tx.Outputs, &bt.Output{
			Satoshis:      500,
			LockingScript: lockingScript,
		})

		// Pre-Genesis height (999)
		err := tv.checkConsensusSigops(tx, 999)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "bad-txn-sigops")
	})
}
