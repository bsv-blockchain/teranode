package util

import (
	"fmt"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGetFeesCoinbaseTransaction(t *testing.T) {
	tests := []struct {
		name          string
		outputs       []uint64
		expectedFees  uint64
		expectedError bool
	}{
		{
			name:          "coinbase with single output",
			outputs:       []uint64{5000000000}, // 50 BSV
			expectedFees:  5000000000,
			expectedError: false,
		},
		{
			name:          "coinbase with multiple outputs",
			outputs:       []uint64{2500000000, 2500000000}, // 25 + 25 BSV
			expectedFees:  5000000000,
			expectedError: false,
		},
		{
			name:          "coinbase with zero-value outputs",
			outputs:       []uint64{5000000000, 0, 0}, // 50 BSV + zeros
			expectedFees:  5000000000,
			expectedError: false,
		},
		{
			name:          "coinbase with all zero outputs",
			outputs:       []uint64{0, 0, 0},
			expectedFees:  0,
			expectedError: false,
		},
		{
			name:          "coinbase with no outputs",
			outputs:       []uint64{},
			expectedFees:  0,
			expectedError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create coinbase transaction from hex template
			// This is a simplified coinbase with the required structure
			baseHex := "01000000010000000000000000000000000000000000000000000000000000000000000000ffffffff00ffffffff"

			if len(tt.outputs) == 0 {
				baseHex += "00" // no outputs
			} else {
				// Add one output (we'll modify afterward if needed)
				baseHex += "01"               // 1 output
				baseHex += "0000000000000000" // 8 bytes for satoshis (will update)
				baseHex += "00"               // empty locking script
			}
			baseHex += "00000000" // locktime

			tx, err := bt.NewTxFromString(baseHex)
			require.NoError(t, err)

			// Clear outputs and add the test outputs
			tx.Outputs = make([]*bt.Output, len(tt.outputs))
			for i, satoshis := range tt.outputs {
				tx.Outputs[i] = &bt.Output{
					Satoshis:      satoshis,
					LockingScript: &bscript.Script{},
				}
			}

			// Verify it's detected as coinbase
			require.True(t, tx.IsCoinbase(), "transaction should be detected as coinbase")

			fees, err := GetFees(tx)

			if tt.expectedError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.expectedFees, fees)
			}
		})
	}
}

func TestGetFeesEmptyInputs(t *testing.T) {
	// Create transaction with no inputs (but not coinbase)
	tx := &bt.Tx{
		Version: 1,
		Inputs:  []*bt.Input{}, // empty inputs
		Outputs: []*bt.Output{
			{
				Satoshis:      1000000, // 0.01 BSV
				LockingScript: &bscript.Script{},
			},
		},
	}

	// Verify it's not a coinbase
	require.False(t, tx.IsCoinbase(), "transaction should not be coinbase")

	fees, err := GetFees(tx)
	require.NoError(t, err)
	assert.Equal(t, uint64(0), fees, "empty inputs should return 0 fees")
}

func TestGetFeesRegularTransaction(t *testing.T) {
	tests := []struct {
		name          string
		inputs        []uint64 // PreviousTxSatoshis values
		outputs       []uint64 // output satoshi values
		expectedFees  uint64
		expectedError bool
	}{
		{
			name:          "positive fees - inputs greater than outputs",
			inputs:        []uint64{1000000, 500000}, // 1.5M total input
			outputs:       []uint64{800000, 600000},  // 1.4M total output
			expectedFees:  100000,                    // 0.1M fee
			expectedError: false,
		},
		{
			name:          "zero fees - inputs equal outputs",
			inputs:        []uint64{1000000},
			outputs:       []uint64{1000000},
			expectedFees:  0,
			expectedError: false,
		},
		{
			name:          "single input single output",
			inputs:        []uint64{5000000},
			outputs:       []uint64{4900000},
			expectedFees:  100000,
			expectedError: false,
		},
		{
			name:          "outputs with zero satoshis",
			inputs:        []uint64{1000000},
			outputs:       []uint64{900000, 0, 0}, // zeros should be ignored
			expectedFees:  100000,
			expectedError: false,
		},
		{
			name:          "multiple inputs and outputs",
			inputs:        []uint64{2000000, 3000000, 1000000}, // 6M total
			outputs:       []uint64{1500000, 2500000, 1800000}, // 5.8M total
			expectedFees:  200000,                              // 0.2M fee
			expectedError: false,
		},
		{
			name:          "all zero output values",
			inputs:        []uint64{1000000},
			outputs:       []uint64{0, 0, 0},
			expectedFees:  1000000, // all input becomes fee
			expectedError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tx := bt.NewTx()

			// Add inputs using From() method
			for i, satoshis := range tt.inputs {
				// Create a unique previous transaction hash as hex string
				prevTxHashHex := "0000000000000000000000000000000000000000000000000000000000000000"
				if i < 16 {
					prevTxHashHex = prevTxHashHex[:62] + fmt.Sprintf("%02x", i+1) // make each hash unique
				}

				err := tx.From(
					prevTxHashHex,
					0,        // output index
					"",       // unlocking script (empty for test)
					satoshis, // satoshis from previous output
				)
				require.NoError(t, err)
			}

			// Add outputs using PayToAddress (need a dummy address)
			for _, satoshis := range tt.outputs {
				err := tx.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", satoshis)
				require.NoError(t, err)
			}

			// Verify it's not a coinbase
			require.False(t, tx.IsCoinbase(), "transaction should not be coinbase")

			fees, err := GetFees(tx)

			if tt.expectedError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.expectedFees, fees)
			}
		})
	}
}

func TestGetFeesEdgeCases(t *testing.T) {
	t.Run("large satoshi values", func(t *testing.T) {
		tx := bt.NewTx()

		// Add input with large value using From() method
		prevTxHashHex := "0100000000000000000000000000000000000000000000000000000000000000"
		err := tx.From(
			prevTxHashHex,
			0,                  // output index
			"",                 // unlocking script
			21000000*100000000, // 21M BSV in satoshis
		)
		require.NoError(t, err)

		// Add output with slightly smaller value
		err = tx.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 21000000*100000000-1000)
		require.NoError(t, err)

		fees, err := GetFees(tx)
		require.NoError(t, err)
		assert.Equal(t, uint64(1000), fees)
	})

	t.Run("coinbase with large outputs", func(t *testing.T) {
		// Create coinbase transaction from hex template (same as the coinbase test function)
		baseHex := "01000000010000000000000000000000000000000000000000000000000000000000000000ffffffff00ffffffff"
		baseHex += "01"               // 1 output
		baseHex += "0000000000000000" // 8 bytes for satoshis (will update)
		baseHex += "00"               // empty locking script
		baseHex += "00000000"         // locktime

		tx, err := bt.NewTxFromString(baseHex)
		require.NoError(t, err)

		// Update the output with large value
		tx.Outputs = []*bt.Output{
			{
				Satoshis:      21000000 * 100000000, // 21M BSV
				LockingScript: &bscript.Script{},
			},
		}

		require.True(t, tx.IsCoinbase())

		fees, err := GetFees(tx)
		require.NoError(t, err)
		assert.Equal(t, uint64(21000000*100000000), fees)
	})
}
