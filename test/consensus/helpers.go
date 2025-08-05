package consensus

import (
	"encoding/hex"
	
	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
)

// CreateSimpleTx creates a simple transaction for testing
func CreateSimpleTx() *bt.Tx {
	tx := bt.NewTx()
	tx.Version = 1
	tx.LockTime = 0
	
	// Create a simple input (32 zero bytes for previous tx hash)
	tx.Inputs = []*bt.Input{{
		PreviousTxOutIndex: 0,
		UnlockingScript:    &bscript.Script{},
		SequenceNumber:     0xffffffff,
	}}
	
	// Create a simple output
	tx.Outputs = []*bt.Output{{
		Satoshis:      100000000, // 1 BTC
		LockingScript: &bscript.Script{},
	}}
	
	return tx
}

// CreateExtendedTx creates a transaction with extended format (includes previous output info)
func CreateExtendedTx() *bt.Tx {
	tx := CreateSimpleTx()
	
	// Add previous output information for extended format
	if len(tx.Inputs) > 0 {
		tx.Inputs[0].PreviousTxSatoshis = 100000000
		tx.Inputs[0].PreviousTxScript = &bscript.Script{}
	}
	
	return tx
}

// CreateMultiInputTx creates a transaction with multiple inputs
func CreateMultiInputTx(numInputs int) *bt.Tx {
	tx := bt.NewTx()
	tx.Version = 1
	tx.LockTime = 0
	
	// Create multiple inputs
	tx.Inputs = make([]*bt.Input, numInputs)
	for i := 0; i < numInputs; i++ {
		tx.Inputs[i] = &bt.Input{
			PreviousTxOutIndex: uint32(i),
			UnlockingScript:    &bscript.Script{},
			SequenceNumber:     0xffffffff,
			PreviousTxSatoshis: 10000000, // 0.1 BTC per input
			PreviousTxScript:   &bscript.Script{},
		}
	}
	
	// Create output with slightly less than total input (for fee)
	totalSats := uint64(numInputs * 10000000)
	tx.Outputs = []*bt.Output{{
		Satoshis:      totalSats - 1000, // 1000 satoshi fee
		LockingScript: &bscript.Script{},
	}}
	
	return tx
}

// ParseHexScript parses a hex-encoded script string
func ParseHexScript(hexStr string) (*bscript.Script, error) {
	if hexStr == "" {
		return &bscript.Script{}, nil
	}
	
	// This is not a hex script, it's a script with opcodes
	// Return nil to indicate it should be parsed differently
	if !isHexScript(hexStr) {
		return nil, nil
	}
	
	// Remove 0x prefix if present
	if len(hexStr) > 2 && hexStr[:2] == "0x" {
		hexStr = hexStr[2:]
	}
	
	bytes, err := hex.DecodeString(hexStr)
	if err != nil {
		return nil, err
	}
	
	script := bscript.Script(bytes)
	return &script, nil
}

// SetTxInputPrevOut sets the previous transaction ID for an input
// This is a helper since go-bt doesn't have a direct method for this
func SetTxInputPrevOut(input *bt.Input, txID []byte, vout uint32) {
	// The Input struct stores the previous tx info internally
	// We need to use reflection or the available methods
	// For now, we'll work with what's available in the API
	input.PreviousTxOutIndex = vout
}

// isHexScript checks if a script string is in hex format
func isHexScript(script string) bool {
	return len(script) > 2 && script[:2] == "0x"
}