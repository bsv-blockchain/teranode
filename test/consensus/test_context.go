package consensus

import (
	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
)

// TestContext provides proper context for transaction validation
type TestContext struct {
	// Previous transaction that creates the output being spent
	creditTx *bt.Tx
	
	// The transaction being tested
	spendTx *bt.Tx
	
	// UTXO heights for each input
	utxoHeights []uint32
	
	// Block height for validation
	blockHeight uint32
}

// NewTestContext creates a new test context with proper setup
func NewTestContext(scriptPubKey *bscript.Script, amount uint64) *TestContext {
	// Create crediting transaction
	creditTx := bt.NewTx()
	creditTx.Version = 1
	creditTx.LockTime = 0
	
	// Add a dummy input to make it valid
	creditTx.Inputs = []*bt.Input{{
		PreviousTxOutIndex: 0xffffffff,
		UnlockingScript:    &bscript.Script{},
		SequenceNumber:     0xffffffff,
	}}
	
	// Add output with the script we want to test
	creditTx.Outputs = []*bt.Output{{
		Satoshis:      amount,
		LockingScript: scriptPubKey,
	}}
	
	// Create spending transaction
	spendTx := bt.NewTx()
	spendTx.Version = 1
	spendTx.LockTime = 0
	
	// Create input that spends the credit tx output
	creditTxID := creditTx.TxID()
	
	input := &bt.Input{
		PreviousTxOutIndex: 0,
		UnlockingScript:    &bscript.Script{},
		SequenceNumber:     0xffffffff,
		PreviousTxSatoshis: amount,
		PreviousTxScript:   scriptPubKey,
	}
	_ = input.PreviousTxIDAddStr(creditTxID)
	
	spendTx.Inputs = []*bt.Input{input}
	
	// Add a dummy output
	spendTx.Outputs = []*bt.Output{{
		Satoshis:      amount - 1000, // Leave some for fee
		LockingScript: &bscript.Script{},
	}}
	
	return &TestContext{
		creditTx:    creditTx,
		spendTx:     spendTx,
		utxoHeights: []uint32{0}, // Default to height 0
		blockHeight: 1,            // Default to block 1
	}
}

// SetUnlockingScript sets the unlocking script for the spending transaction
func (tc *TestContext) SetUnlockingScript(script *bscript.Script) {
	if len(tc.spendTx.Inputs) > 0 {
		tc.spendTx.Inputs[0].UnlockingScript = script
	}
}

// SetBlockHeight sets the block height for validation
func (tc *TestContext) SetBlockHeight(height uint32) {
	tc.blockHeight = height
}

// SetUTXOHeights sets the UTXO heights for validation
func (tc *TestContext) SetUTXOHeights(heights []uint32) {
	tc.utxoHeights = heights
}

// GetSpendTx returns the spending transaction
func (tc *TestContext) GetSpendTx() *bt.Tx {
	return tc.spendTx
}

// GetUTXOHeights returns the UTXO heights
func (tc *TestContext) GetUTXOHeights() []uint32 {
	return tc.utxoHeights
}

// GetBlockHeight returns the block height
func (tc *TestContext) GetBlockHeight() uint32 {
	return tc.blockHeight
}