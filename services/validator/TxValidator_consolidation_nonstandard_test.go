package validator

import (
	"testing"

	"github.com/bitcoin-sv/teranode/settings"
	"github.com/bitcoin-sv/teranode/ulogger"
	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-chaincfg"
	"github.com/stretchr/testify/assert"
)

func TestTxValidator_isConsolidationTx_NonStandardScripts(t *testing.T) {
	// Create policy settings
	policy := settings.NewPolicySettings()
	policy.MinConsolidationFactor = 20
	policy.AcceptNonStdConsolidationInput = false // Reject non-standard inputs

	// Create settings with the policy
	tSettings := &settings.Settings{
		Policy:         policy,
		ChainCfgParams: &chaincfg.MainNetParams,
	}

	// Create TxValidator
	tv := &TxValidator{
		logger:   ulogger.TestLogger{},
		settings: tSettings,
	}

	tests := []struct {
		name           string
		setupTx        func() *bt.Tx
		want           bool
		wantReasonPart string
	}{
		{
			name: "consolidation with standard P2PKH-like unlocking scripts passes",
			setupTx: func() *bt.Tx {
				tx := bt.NewTx()
				// Add 20 inputs with standard push-only scripts (like P2PKH unlock)
				for i := 0; i < 20; i++ {
					// Create a standard P2PKH-like unlocking script (sig + pubkey)
					unlockScript := bscript.Script{}
					// Push signature (71 bytes)
					unlockScript = append(unlockScript, 0x47) // Push 71 bytes
					for j := 0; j < 71; j++ {
						unlockScript = append(unlockScript, byte(j))
					}
					// Push pubkey (33 bytes)
					unlockScript = append(unlockScript, 0x21) // Push 33 bytes
					for j := 0; j < 33; j++ {
						unlockScript = append(unlockScript, byte(j+100))
					}

					input := &bt.Input{
						PreviousTxSatoshis: 1000,
						PreviousTxScript:   createP2PKHLockingScript(),
						UnlockingScript:    &unlockScript,
						SequenceNumber:     0xfffffffe,
						PreviousTxOutIndex: uint32(i),
					}
					_ = input.PreviousTxIDAdd(&chainhash.Hash{})
					tx.Inputs = append(tx.Inputs, input)
				}
				// Add 1 output
				tx.Outputs = append(tx.Outputs, &bt.Output{
					Satoshis:      19000,
					LockingScript: &bscript.Script{},
				})
				return tx
			},
			want:           true,
			wantReasonPart: "",
		},
		{
			name: "consolidation with non-standard script containing OP_DUP fails",
			setupTx: func() *bt.Tx {
				tx := bt.NewTx()
				// Add 20 inputs, first one has non-standard script
				for i := 0; i < 20; i++ {
					var unlockScript *bscript.Script
					if i == 0 {
						// First input has non-standard script with OP_DUP
						unlockScript = &bscript.Script{0x76} // OP_DUP
					} else {
						// Others have empty scripts (standard)
						unlockScript = &bscript.Script{}
					}

					input := &bt.Input{
						PreviousTxSatoshis: 1000,
						PreviousTxScript:   createP2PKHLockingScript(),
						UnlockingScript:    unlockScript,
						SequenceNumber:     0xfffffffe,
						PreviousTxOutIndex: uint32(i),
					}
					_ = input.PreviousTxIDAdd(&chainhash.Hash{})
					tx.Inputs = append(tx.Inputs, input)
				}
				// Add 1 output
				tx.Outputs = append(tx.Outputs, &bt.Output{
					Satoshis:      19000,
					LockingScript: &bscript.Script{},
				})
				return tx
			},
			want:           false,
			wantReasonPart: "input 0 has non-standard script",
		},
		{
			name: "consolidation with script containing arithmetic opcodes fails",
			setupTx: func() *bt.Tx {
				tx := bt.NewTx()
				// Add 20 inputs
				for i := 0; i < 20; i++ {
					var unlockScript *bscript.Script
					if i == 5 {
						// 6th input has script with OP_ADD
						unlockScript = &bscript.Script{0x51, 0x52, 0x93} // OP_1 OP_2 OP_ADD
					} else {
						unlockScript = &bscript.Script{}
					}

					input := &bt.Input{
						PreviousTxSatoshis: 1000,
						PreviousTxScript:   createP2PKHLockingScript(),
						UnlockingScript:    unlockScript,
						SequenceNumber:     0xfffffffe,
						PreviousTxOutIndex: uint32(i),
					}
					_ = input.PreviousTxIDAdd(&chainhash.Hash{})
					tx.Inputs = append(tx.Inputs, input)
				}
				// Add 1 output
				tx.Outputs = append(tx.Outputs, &bt.Output{
					Satoshis:      19000,
					LockingScript: &bscript.Script{},
				})
				return tx
			},
			want:           false,
			wantReasonPart: "input 5 has non-standard script",
		},
		{
			name: "consolidation with AcceptNonStdConsolidationInput=true accepts non-standard scripts",
			setupTx: func() *bt.Tx {
				tx := bt.NewTx()
				// Add 20 inputs with non-standard scripts
				for i := 0; i < 20; i++ {
					// Non-standard script with OP_DUP
					unlockScript := &bscript.Script{0x76} // OP_DUP

					input := &bt.Input{
						PreviousTxSatoshis: 1000,
						PreviousTxScript:   createP2PKHLockingScript(),
						UnlockingScript:    unlockScript,
						SequenceNumber:     0xfffffffe,
						PreviousTxOutIndex: uint32(i),
					}
					_ = input.PreviousTxIDAdd(&chainhash.Hash{})
					tx.Inputs = append(tx.Inputs, input)
				}
				// Add 1 output
				tx.Outputs = append(tx.Outputs, &bt.Output{
					Satoshis:      19000,
					LockingScript: &bscript.Script{},
				})
				return tx
			},
			want:           true,
			wantReasonPart: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create a fresh TxValidator for each test
			localTV := tv
			if tt.name == "consolidation with AcceptNonStdConsolidationInput=true accepts non-standard scripts" {
				// Create policy that accepts non-standard inputs
				localPolicy := settings.NewPolicySettings()
				localPolicy.MinConsolidationFactor = 20
				localPolicy.AcceptNonStdConsolidationInput = true // Accept non-standard

				localSettings := &settings.Settings{
					Policy:         localPolicy,
					ChainCfgParams: &chaincfg.MainNetParams,
				}

				localTV = &TxValidator{
					logger:   ulogger.TestLogger{},
					settings: localSettings,
				}
			}

			tx := tt.setupTx()
			utxoHeights := make([]uint32, len(tx.Inputs))
			currentHeight := uint32(500000) // Post-UAHF height

			got := localTV.isConsolidationTx(tx, utxoHeights, currentHeight)
			assert.Equal(t, tt.want, got, "isConsolidationTx() = %v, want %v", got, tt.want)
		})
	}
}

func TestTxValidator_isConsolidationTx_PreUAHF(t *testing.T) {
	// Create policy settings
	policy := settings.NewPolicySettings()
	policy.MinConsolidationFactor = 20
	policy.AcceptNonStdConsolidationInput = false // Reject non-standard inputs

	// Create settings with the policy
	tSettings := &settings.Settings{
		Policy:         policy,
		ChainCfgParams: &chaincfg.MainNetParams,
	}

	// Create TxValidator
	tv := &TxValidator{
		logger:   ulogger.TestLogger{},
		settings: tSettings,
	}

	// Test that non-push scripts are accepted before UAHF
	tx := bt.NewTx()
	// Add 20 inputs with non-push scripts
	for i := 0; i < 20; i++ {
		// Script with OP_DUP which is not push-only
		unlockScript := &bscript.Script{0x76} // OP_DUP

		input := &bt.Input{
			PreviousTxSatoshis: 1000,
			PreviousTxScript:   &bscript.Script{},
			UnlockingScript:    unlockScript,
			SequenceNumber:     0xfffffffe,
			PreviousTxOutIndex: uint32(i),
		}
		_ = input.PreviousTxIDAdd(&chainhash.Hash{})
		tx.Inputs = append(tx.Inputs, input)
	}
	// Add 1 output
	tx.Outputs = append(tx.Outputs, &bt.Output{
		Satoshis:      19000,
		LockingScript: &bscript.Script{},
	})

	utxoHeights := make([]uint32, len(tx.Inputs))
	preUAHFHeight := uint32(470000) // Before UAHF

	// Should pass even with non-push scripts before UAHF
	got := tv.isConsolidationTx(tx, utxoHeights, preUAHFHeight)
	assert.True(t, got, "consolidation should be valid pre-UAHF even with non-push scripts")
}
