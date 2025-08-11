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

// createP2PKHLockingScript creates a typical P2PKH locking script
func createP2PKHLockingScript() *bscript.Script {
	// P2PKH: OP_DUP OP_HASH160 <pubKeyHash> OP_EQUALVERIFY OP_CHECKSIG
	script := bscript.Script{}

	script = append(script, 0x76) // OP_DUP
	script = append(script, 0xa9) // OP_HASH160
	script = append(script, 0x14) // Push 20 bytes
	// Add 20 bytes of pubkey hash
	for i := 0; i < 20; i++ {
		script = append(script, byte(i))
	}
	script = append(script, 0x88) // OP_EQUALVERIFY
	script = append(script, 0xac) // OP_CHECKSIG

	return &script
}

func TestTxValidator_isDustReturnTx(t *testing.T) {
	tests := []struct {
		name    string
		setupTx func() *bt.Tx
		want    bool
	}{
		{
			name:    "nil transaction returns false",
			setupTx: func() *bt.Tx { return nil },
			want:    false,
		},
		{
			name: "transaction with single 0-satoshi unspendable output is dust return",
			setupTx: func() *bt.Tx {
				tx := bt.NewTx()
				// Add an input
				input := &bt.Input{
					PreviousTxSatoshis: 1,
					PreviousTxScript:   &bscript.Script{},
					UnlockingScript:    &bscript.Script{},
				}
				_ = input.PreviousTxIDAdd(&chainhash.Hash{})
				tx.Inputs = append(tx.Inputs, input)
				// Add unspendable output with 0 satoshis
				tx.Outputs = append(tx.Outputs, &bt.Output{
					Satoshis:      0,
					LockingScript: &bscript.Script{0x00, 0x6a}, // OP_FALSE OP_RETURN
				})
				return tx
			},
			want: true,
		},
		{
			name: "transaction with multiple outputs is not dust return",
			setupTx: func() *bt.Tx {
				tx := bt.NewTx()
				// Add outputs
				tx.Outputs = append(tx.Outputs, &bt.Output{
					Satoshis:      0,
					LockingScript: &bscript.Script{0x00, 0x6a},
				})
				tx.Outputs = append(tx.Outputs, &bt.Output{
					Satoshis:      100,
					LockingScript: &bscript.Script{},
				})
				return tx
			},
			want: false,
		},
		{
			name: "transaction with non-zero satoshis is not dust return",
			setupTx: func() *bt.Tx {
				tx := bt.NewTx()
				tx.Outputs = append(tx.Outputs, &bt.Output{
					Satoshis:      1,
					LockingScript: &bscript.Script{0x00, 0x6a},
				})
				return tx
			},
			want: false,
		},
		{
			name: "transaction with spendable output is not dust return",
			setupTx: func() *bt.Tx {
				tx := bt.NewTx()
				tx.Outputs = append(tx.Outputs, &bt.Output{
					Satoshis:      0,
					LockingScript: &bscript.Script{0x76}, // OP_DUP (spendable)
				})
				return tx
			},
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tv := &TxValidator{
				logger:   ulogger.TestLogger{},
				settings: &settings.Settings{},
			}

			tx := tt.setupTx()
			got := tv.isDustReturnTx(tx)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestTxValidator_isConsolidationTx(t *testing.T) {
	tests := []struct {
		name                   string
		minConsolidationFactor int
		setupTx                func() *bt.Tx
		utxoHeights            []uint32
		currentHeight          uint32
		want                   bool
		wantReason             string
	}{
		{
			name:                   "nil transaction returns false",
			minConsolidationFactor: 20,
			setupTx:                func() *bt.Tx { return nil },
			utxoHeights:            []uint32{},
			currentHeight:          1000,
			want:                   false,
			wantReason:             "transaction is nil",
		},
		{
			name:                   "transaction with exactly minConsolidationFactor inputs and 1 output is consolidation",
			minConsolidationFactor: 20,
			setupTx: func() *bt.Tx {
				tx := bt.NewTx()
				// Add 20 inputs
				for i := 0; i < 20; i++ {
					input := &bt.Input{
						PreviousTxSatoshis: 1000,
						PreviousTxScript:   createP2PKHLockingScript(),
						UnlockingScript:    &bscript.Script{},
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
			utxoHeights:   make([]uint32, 20), // All at height 0
			currentHeight: 1000,
			want:          true,
			wantReason:    "",
		},
		{
			name:                   "transaction with less than minConsolidationFactor inputs is not consolidation",
			minConsolidationFactor: 20,
			setupTx: func() *bt.Tx {
				tx := bt.NewTx()
				// Add 19 inputs (one less than minConsolidationFactor)
				for i := 0; i < 19; i++ {
					input := &bt.Input{
						PreviousTxSatoshis: 1000,
						PreviousTxScript:   createP2PKHLockingScript(),
						UnlockingScript:    &bscript.Script{},
						SequenceNumber:     0xfffffffe,
						PreviousTxOutIndex: uint32(i),
					}
					_ = input.PreviousTxIDAdd(&chainhash.Hash{})
					tx.Inputs = append(tx.Inputs, input)
				}
				// Add 1 output
				tx.Outputs = append(tx.Outputs, &bt.Output{
					Satoshis:      18000,
					LockingScript: &bscript.Script{},
				})
				return tx
			},
			utxoHeights:   make([]uint32, 19),
			currentHeight: 1000,
			want:          false,
			wantReason:    "insufficient input to output ratio: 19 inputs < 20 factor × 1 outputs",
		},
		{
			name:                   "transaction with 100 inputs and 5 outputs is consolidation (ratio=20)",
			minConsolidationFactor: 20,
			setupTx: func() *bt.Tx {
				tx := bt.NewTx()
				// Add 100 inputs
				for i := 0; i < 100; i++ {
					input := &bt.Input{
						PreviousTxSatoshis: 1000,
						PreviousTxScript:   createP2PKHLockingScript(),
						UnlockingScript:    &bscript.Script{},
						SequenceNumber:     0xfffffffe,
						PreviousTxOutIndex: uint32(i),
					}
					_ = input.PreviousTxIDAdd(&chainhash.Hash{})
					tx.Inputs = append(tx.Inputs, input)
				}
				// Add 5 outputs
				for i := 0; i < 5; i++ {
					tx.Outputs = append(tx.Outputs, &bt.Output{
						Satoshis:      19000,
						LockingScript: &bscript.Script{},
					})
				}
				return tx
			},
			utxoHeights:   make([]uint32, 100),
			currentHeight: 1000,
			want:          true,
			wantReason:    "",
		},
		{
			name:                   "transaction with 100 inputs and 6 outputs is not consolidation (ratio=16.67)",
			minConsolidationFactor: 20,
			setupTx: func() *bt.Tx {
				tx := bt.NewTx()
				// Add 100 inputs
				for i := 0; i < 100; i++ {
					input := &bt.Input{
						PreviousTxSatoshis: 1000,
						PreviousTxScript:   createP2PKHLockingScript(),
						UnlockingScript:    &bscript.Script{},
						SequenceNumber:     0xfffffffe,
						PreviousTxOutIndex: uint32(i),
					}
					_ = input.PreviousTxIDAdd(&chainhash.Hash{})
					tx.Inputs = append(tx.Inputs, input)
				}
				// Add 6 outputs
				for i := 0; i < 6; i++ {
					tx.Outputs = append(tx.Outputs, &bt.Output{
						Satoshis:      16000,
						LockingScript: &bscript.Script{},
					})
				}
				return tx
			},
			utxoHeights:   make([]uint32, 100),
			currentHeight: 1000,
			want:          false,
			wantReason:    "insufficient input to output ratio",
		},
		{
			name:                   "transaction with many inputs and no outputs is consolidation",
			minConsolidationFactor: 20,
			setupTx: func() *bt.Tx {
				tx := bt.NewTx()
				// Add 20 inputs
				for i := 0; i < 20; i++ {
					input := &bt.Input{
						PreviousTxSatoshis: 1000,
						PreviousTxScript:   createP2PKHLockingScript(),
						UnlockingScript:    &bscript.Script{},
						SequenceNumber:     0xfffffffe,
						PreviousTxOutIndex: uint32(i),
					}
					_ = input.PreviousTxIDAdd(&chainhash.Hash{})
					tx.Inputs = append(tx.Inputs, input)
				}
				// No outputs
				return tx
			},
			utxoHeights:   make([]uint32, 20),
			currentHeight: 1000,
			want:          true,
			wantReason:    "",
		},
		{
			name:                   "transaction with 1 input and 1 output is not consolidation",
			minConsolidationFactor: 20,
			setupTx: func() *bt.Tx {
				tx := bt.NewTx()
				input := &bt.Input{
					PreviousTxSatoshis: 1000,
					PreviousTxScript:   &bscript.Script{},
					UnlockingScript:    &bscript.Script{},
					SequenceNumber:     0xfffffffe,
					PreviousTxOutIndex: 0,
				}
				_ = input.PreviousTxIDAdd(&chainhash.Hash{})
				tx.Inputs = append(tx.Inputs, input)
				tx.Outputs = append(tx.Outputs, &bt.Output{
					Satoshis:      900,
					LockingScript: &bscript.Script{},
				})
				return tx
			},
			utxoHeights:   []uint32{100},
			currentHeight: 1000,
			want:          false,
			wantReason:    "insufficient input to output ratio: 1 inputs < 20 factor × 1 outputs",
		},
		{
			name:                   "custom minConsolidationFactor of 10 with 10 inputs and 1 output is consolidation",
			minConsolidationFactor: 10,
			setupTx: func() *bt.Tx {
				tx := bt.NewTx()
				// Add 10 inputs
				for i := 0; i < 10; i++ {
					input := &bt.Input{
						PreviousTxSatoshis: 1000,
						PreviousTxScript:   createP2PKHLockingScript(),
						UnlockingScript:    &bscript.Script{},
						SequenceNumber:     0xfffffffe,
						PreviousTxOutIndex: uint32(i),
					}
					_ = input.PreviousTxIDAdd(&chainhash.Hash{})
					tx.Inputs = append(tx.Inputs, input)
				}
				// Add 1 output
				tx.Outputs = append(tx.Outputs, &bt.Output{
					Satoshis:      9000,
					LockingScript: &bscript.Script{},
				})
				return tx
			},
			utxoHeights:   make([]uint32, 10),
			currentHeight: 1000,
			want:          true,
			wantReason:    "",
		},
		{
			name:                   "minConsolidationFactor of 0 uses default value of 20",
			minConsolidationFactor: 0,
			setupTx: func() *bt.Tx {
				tx := bt.NewTx()
				// Add 20 inputs
				for i := 0; i < 20; i++ {
					input := &bt.Input{
						PreviousTxSatoshis: 1000,
						PreviousTxScript:   createP2PKHLockingScript(),
						UnlockingScript:    &bscript.Script{},
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
			utxoHeights:   make([]uint32, 20),
			currentHeight: 1000,
			want:          false,
			wantReason:    "consolidation transactions are disabled",
		},
		{
			name:                   "minConsolidationFactor of 0 with 19 inputs is not consolidation (uses default 20)",
			minConsolidationFactor: 0,
			setupTx: func() *bt.Tx {
				tx := bt.NewTx()
				// Add 19 inputs
				for i := 0; i < 19; i++ {
					input := &bt.Input{
						PreviousTxSatoshis: 1000,
						PreviousTxScript:   createP2PKHLockingScript(),
						UnlockingScript:    &bscript.Script{},
						SequenceNumber:     0xfffffffe,
						PreviousTxOutIndex: uint32(i),
					}
					_ = input.PreviousTxIDAdd(&chainhash.Hash{})
					tx.Inputs = append(tx.Inputs, input)
				}
				// Add 1 output
				tx.Outputs = append(tx.Outputs, &bt.Output{
					Satoshis:      18000,
					LockingScript: &bscript.Script{},
				})
				return tx
			},
			utxoHeights:   make([]uint32, 19),
			currentHeight: 1000,
			want:          false,
			wantReason:    "consolidation transactions are disabled",
		},
		{
			name:                   "transaction with 40 inputs and 2 outputs is consolidation (ratio=20)",
			minConsolidationFactor: 20,
			setupTx: func() *bt.Tx {
				tx := bt.NewTx()
				// Add 40 inputs
				for i := 0; i < 40; i++ {
					input := &bt.Input{
						PreviousTxSatoshis: 1000,
						PreviousTxScript:   createP2PKHLockingScript(),
						UnlockingScript:    &bscript.Script{},
						SequenceNumber:     0xfffffffe,
						PreviousTxOutIndex: uint32(i),
					}
					_ = input.PreviousTxIDAdd(&chainhash.Hash{})
					tx.Inputs = append(tx.Inputs, input)
				}
				// Add 2 outputs
				for i := 0; i < 2; i++ {
					tx.Outputs = append(tx.Outputs, &bt.Output{
						Satoshis:      19500,
						LockingScript: &bscript.Script{},
					})
				}
				return tx
			},
			utxoHeights:   make([]uint32, 40),
			currentHeight: 1000,
			want:          true,
			wantReason:    "",
		},
		{
			name:                   "transaction with 39 inputs and 2 outputs is not consolidation (ratio=19.5)",
			minConsolidationFactor: 20,
			setupTx: func() *bt.Tx {
				tx := bt.NewTx()
				// Add 39 inputs
				for i := 0; i < 39; i++ {
					input := &bt.Input{
						PreviousTxSatoshis: 1000,
						PreviousTxScript:   createP2PKHLockingScript(),
						UnlockingScript:    &bscript.Script{},
						SequenceNumber:     0xfffffffe,
						PreviousTxOutIndex: uint32(i),
					}
					_ = input.PreviousTxIDAdd(&chainhash.Hash{})
					tx.Inputs = append(tx.Inputs, input)
				}
				// Add 2 outputs
				for i := 0; i < 2; i++ {
					tx.Outputs = append(tx.Outputs, &bt.Output{
						Satoshis:      19000,
						LockingScript: &bscript.Script{},
					})
				}
				return tx
			},
			utxoHeights:   make([]uint32, 39),
			currentHeight: 1000,
			want:          false,
			wantReason:    "insufficient input to output ratio",
		},
		{
			name:                   "negative minConsolidationFactor uses default value of 20",
			minConsolidationFactor: -5,
			setupTx: func() *bt.Tx {
				tx := bt.NewTx()
				// Add 20 inputs
				for i := 0; i < 20; i++ {
					input := &bt.Input{
						PreviousTxSatoshis: 1000,
						PreviousTxScript:   createP2PKHLockingScript(),
						UnlockingScript:    &bscript.Script{},
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
			utxoHeights:   make([]uint32, 20),
			currentHeight: 1000,
			want:          false,
			wantReason:    "consolidation transactions are disabled",
		},
		{
			name:                   "dust return transaction is recognized as consolidation",
			minConsolidationFactor: 20,
			setupTx: func() *bt.Tx {
				tx := bt.NewTx()
				// Add 5 inputs
				for i := 0; i < 5; i++ {
					input := &bt.Input{
						PreviousTxSatoshis: 1,
						PreviousTxScript:   createP2PKHLockingScript(),
						UnlockingScript:    &bscript.Script{},
						SequenceNumber:     0xfffffffe,
						PreviousTxOutIndex: uint32(i),
					}
					_ = input.PreviousTxIDAdd(&chainhash.Hash{})
					tx.Inputs = append(tx.Inputs, input)
				}
				// Add 1 output with 0 satoshis and unspendable script
				output := &bt.Output{
					Satoshis:      0,
					LockingScript: &bscript.Script{0x00, 0x6a}, // OP_FALSE OP_RETURN
				}
				tx.Outputs = append(tx.Outputs, output)
				return tx
			},
			utxoHeights:   make([]uint32, 5),
			currentHeight: 1000,
			want:          true,
			wantReason:    "",
		},
		{
			name:                   "transaction with insufficient confirmations fails",
			minConsolidationFactor: 20,
			setupTx: func() *bt.Tx {
				tx := bt.NewTx()
				// Add 20 inputs
				for i := 0; i < 20; i++ {
					input := &bt.Input{
						PreviousTxSatoshis: 1000,
						PreviousTxScript:   createP2PKHLockingScript(),
						UnlockingScript:    &bscript.Script{},
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
			utxoHeights:   []uint32{998, 998, 998, 998, 998, 998, 998, 998, 998, 998, 998, 998, 998, 998, 998, 998, 998, 998, 998, 998}, // Only 2 confirmations
			currentHeight: 1000,
			want:          false,
			wantReason:    "input 0 has insufficient confirmations: 2 < 6 required",
		},
		{
			name:                   "transaction with large input scripts fails",
			minConsolidationFactor: 20,
			setupTx: func() *bt.Tx {
				tx := bt.NewTx()
				// Add 20 inputs with large scripts
				for i := 0; i < 20; i++ {
					largeScript := make(bscript.Script, 200) // Larger than default 150 limit
					input := &bt.Input{
						PreviousTxSatoshis: 1000,
						PreviousTxScript:   createP2PKHLockingScript(),
						UnlockingScript:    &largeScript,
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
			utxoHeights:   make([]uint32, 20),
			currentHeight: 1000,
			want:          false,
			wantReason:    "input 0 scriptSig size 200 exceeds maximum 150 bytes",
		},
		{
			name:                   "transaction with large output scripts relative to inputs fails",
			minConsolidationFactor: 20,
			setupTx: func() *bt.Tx {
				tx := bt.NewTx()
				// Add 20 inputs with small scripts
				for i := 0; i < 20; i++ {
					smallScript := make(bscript.Script, 10)
					input := &bt.Input{
						PreviousTxSatoshis: 1000,
						PreviousTxScript:   createP2PKHLockingScript(),
						UnlockingScript:    &smallScript,
						SequenceNumber:     0xfffffffe,
						PreviousTxOutIndex: uint32(i),
					}
					_ = input.PreviousTxIDAdd(&chainhash.Hash{})
					tx.Inputs = append(tx.Inputs, input)
				}
				// Add 1 output with very large script
				largeOutputScript := make(bscript.Script, 500) // 500 > 200 * 2
				tx.Outputs = append(tx.Outputs, &bt.Output{
					Satoshis:      19000,
					LockingScript: &largeOutputScript,
				})
				return tx
			},
			utxoHeights:   make([]uint32, 20),
			currentHeight: 1000,
			want:          false,
			wantReason:    "insufficient script size ratio: input scriptPubKey sizes 500 < 20 factor × output scriptPubKey sizes 500",
		},
		{
			name:                   "coinbase transaction cannot be consolidation",
			minConsolidationFactor: 20,
			setupTx: func() *bt.Tx {
				tx := bt.NewTx()
				// Create a coinbase input
				input := &bt.Input{
					PreviousTxSatoshis: 0,
					PreviousTxScript:   nil,
					UnlockingScript:    &bscript.Script{0x00, 0x01}, // Coinbase script
					SequenceNumber:     0xffffffff,
					PreviousTxOutIndex: 0xffffffff,
				}
				// Set previous tx ID to all zeros (coinbase)
				_ = input.PreviousTxIDAdd(&chainhash.Hash{})
				tx.Inputs = append(tx.Inputs, input)
				// Add output
				tx.Outputs = append(tx.Outputs, &bt.Output{
					Satoshis:      5000000000, // 50 BTC
					LockingScript: &bscript.Script{},
				})
				return tx
			},
			utxoHeights:   []uint32{0},
			currentHeight: 1000,
			want:          false,
			wantReason:    "coinbase transactions cannot be consolidation transactions",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create policy settings with the specified minConsolidationFactor
			policy := settings.NewPolicySettings()
			policy.MinConsolidationFactor = tt.minConsolidationFactor
			policy.MaxConsolidationInputScriptSize = 150
			policy.MinConfConsolidationInput = 6
			policy.AcceptNonStdConsolidationInput = false

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

			// Get the test transaction
			tx := tt.setupTx()

			// Test the isConsolidationTx method
			got := tv.isConsolidationTx(tx, tt.utxoHeights, tt.currentHeight)
			assert.Equal(t, tt.want, got, "isConsolidationTx() = %v, want %v", got, tt.want)
		})
	}
}

// TestTxValidator_checkFees_consolidation tests that consolidation transactions
// skip fee validation
func TestTxValidator_checkFees_consolidation(t *testing.T) {
	// Create policy settings
	policy := settings.NewPolicySettings()
	policy.MinConsolidationFactor = 20
	policy.MinMiningTxFee = 0.00000500 // Set a minimum fee (0.00000500 BSV/kB = 500 satoshis/kB = 0.5 satoshi/byte)

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

	// Create a consolidation transaction with insufficient fees
	tx := bt.NewTx()

	// Add 20 inputs with 1000 sats each
	for i := 0; i < 20; i++ {
		input := &bt.Input{
			PreviousTxSatoshis: 1000,
			PreviousTxScript:   createP2PKHLockingScript(),
			UnlockingScript:    &bscript.Script{},
			SequenceNumber:     0xfffffffe,
			PreviousTxOutIndex: uint32(i),
		}
		_ = input.PreviousTxIDAdd(&chainhash.Hash{})
		tx.Inputs = append(tx.Inputs, input)
	}

	// Add 1 output with 19999 sats (only 1 sat fee, which is below minimum)
	tx.Outputs = append(tx.Outputs, &bt.Output{
		Satoshis:      19999,
		LockingScript: &bscript.Script{},
	})

	// Test that checkFees returns nil for consolidation transaction
	// even though the fee is insufficient
	err := tv.checkFees(tx, 500000, nil)
	assert.NoError(t, err, "checkFees should return nil for consolidation transactions")

	// Now test with a non-consolidation transaction with the same insufficient fee
	nonConsolidationTx := bt.NewTx()

	// Add only 2 inputs
	for i := 0; i < 2; i++ {
		input := &bt.Input{
			PreviousTxSatoshis: 1000,
			PreviousTxScript:   &bscript.Script{},
			UnlockingScript:    &bscript.Script{},
			SequenceNumber:     0xfffffffe,
			PreviousTxOutIndex: uint32(i),
		}
		_ = input.PreviousTxIDAdd(&chainhash.Hash{})
		nonConsolidationTx.Inputs = append(nonConsolidationTx.Inputs, input)
	}

	// Add 1 output with 1999 sats (only 1 sat fee)
	nonConsolidationTx.Outputs = append(nonConsolidationTx.Outputs, &bt.Output{
		Satoshis:      1999,
		LockingScript: &bscript.Script{},
	})

	// Test that checkFees returns an error for non-consolidation transaction
	// With 0.5 satoshi/byte minimum fee rate, this transaction with only 1 sat fee
	// should definitely fail the fee check
	err = tv.checkFees(nonConsolidationTx, 500000, nil)
	assert.Error(t, err, "checkFees should return an error for non-consolidation transactions with insufficient fees")
}

// Benchmark test for isConsolidationTx
func BenchmarkTxValidator_isConsolidationTx(b *testing.B) {
	// Create policy settings
	policy := settings.NewPolicySettings()
	policy.MinConsolidationFactor = 20

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

	// Create a test transaction with 100 inputs and 5 outputs
	tx := bt.NewTx()
	for i := 0; i < 100; i++ {
		input := &bt.Input{
			PreviousTxSatoshis: 1000,
			PreviousTxScript:   &bscript.Script{},
			UnlockingScript:    &bscript.Script{},
			SequenceNumber:     0xfffffffe,
			PreviousTxOutIndex: uint32(i),
		}
		_ = input.PreviousTxIDAdd(&chainhash.Hash{})
		tx.Inputs = append(tx.Inputs, input)
	}
	for i := 0; i < 5; i++ {
		tx.Outputs = append(tx.Outputs, &bt.Output{
			Satoshis:      19000,
			LockingScript: &bscript.Script{},
		})
	}

	// Create mock UTXO heights for benchmark
	utxoHeights := make([]uint32, 100)
	currentHeight := uint32(1000)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = tv.isConsolidationTx(tx, utxoHeights, currentHeight)
	}
}
