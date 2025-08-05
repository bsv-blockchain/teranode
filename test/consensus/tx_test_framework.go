package consensus

import (
	"encoding/hex"
	"fmt"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
)

// TxTestContext holds context for transaction validation tests
type TxTestContext struct {
	Tx          *bt.Tx
	Inputs      []TxTestInput
	Flags       []string
	Comment     string
	ShouldPass  bool
	Parser      *ScriptParser
	Validator   *ValidatorIntegration
}

// TxTestResult holds the result of transaction validation
type TxTestResult struct {
	Valid   bool
	Error   error
	Details string
}

// NewTxTestContext creates a new transaction test context
func NewTxTestContext() *TxTestContext {
	return &TxTestContext{
		Parser:    NewScriptParser(),
		Validator: NewValidatorIntegration(),
	}
}

// LoadFromTxTest loads test data from a TxTest structure
func (ctx *TxTestContext) LoadFromTxTest(test TxTest, shouldPass bool) error {
	ctx.Inputs = test.Inputs
	ctx.Flags = test.Flags
	ctx.Comment = test.Comment
	ctx.ShouldPass = shouldPass
	
	// Parse the raw transaction
	txBytes, err := hex.DecodeString(test.RawTx)
	if err != nil {
		return fmt.Errorf("failed to decode transaction hex: %w", err)
	}
	
	tx, err := bt.NewTxFromBytes(txBytes)
	if err != nil {
		return fmt.Errorf("failed to parse transaction: %w", err)
	}
	
	ctx.Tx = tx
	
	// Extend the transaction with previous output information
	return ctx.extendTransaction()
}

// extendTransaction adds previous output information to transaction inputs
func (ctx *TxTestContext) extendTransaction() error {
	if len(ctx.Inputs) != len(ctx.Tx.Inputs) {
		return fmt.Errorf("input count mismatch: test has %d inputs, tx has %d", 
			len(ctx.Inputs), len(ctx.Tx.Inputs))
	}
	
	for i, testInput := range ctx.Inputs {
		if i >= len(ctx.Tx.Inputs) {
			return fmt.Errorf("test input index %d out of range", i)
		}
		
		// Parse the previous output script
		prevScript, err := ctx.Parser.ParseScript(testInput.ScriptPubKey)
		if err != nil {
			return fmt.Errorf("failed to parse previous output script at index %d: %w", i, err)
		}
		
		// Set previous output information
		script := bscript.Script(prevScript)
		ctx.Tx.Inputs[i].PreviousTxScript = &script
		ctx.Tx.Inputs[i].PreviousTxSatoshis = 100000000 // 1 BTC default
		
		// Set previous transaction ID if provided
		if testInput.TxID != "" && testInput.TxID != "0000000000000000000000000000000000000000000000000000000000000000" {
			txIDBytes, err := hex.DecodeString(testInput.TxID)
			if err != nil {
				return fmt.Errorf("invalid txid at input %d: %w", i, err)
			}
			
			if len(txIDBytes) == 32 {
				// Reverse bytes for little-endian
				for j := 0; j < 16; j++ {
					txIDBytes[j], txIDBytes[31-j] = txIDBytes[31-j], txIDBytes[j]
				}
				// Note: PreviousTxID is a method in go-bt, not a field
				// We'll work with the bytes we have for now
			}
		}
		
		// Verify output index matches
		if ctx.Tx.Inputs[i].PreviousTxOutIndex != testInput.Vout {
			return fmt.Errorf("output index mismatch at input %d: got %d, expected %d", 
				i, ctx.Tx.Inputs[i].PreviousTxOutIndex, testInput.Vout)
		}
	}
	
	return nil
}

// Validate runs transaction validation using all available validators
func (ctx *TxTestContext) Validate() TxTestResult {
	// Run validation with all validators
	results := ctx.Validator.ValidateWithAllValidators(ctx.Tx, 0, make([]uint32, len(ctx.Tx.Inputs)))
	
	// Check if all validators agree
	allAgree, differences := CompareResults(results)
	
	// Determine overall validity
	// Consider valid if at least one validator says it's valid
	valid := false
	var firstError error
	for _, result := range results {
		if result.Success {
			valid = true
			break
		} else if firstError == nil {
			firstError = result.Error
		}
	}
	
	details := fmt.Sprintf("Validators: ")
	for validatorType, result := range results {
		if result.Success {
			details += fmt.Sprintf("%s=OK ", validatorType)
		} else {
			details += fmt.Sprintf("%s=FAIL ", validatorType)
		}
	}
	
	if !allAgree {
		details += fmt.Sprintf("(Disagreement: %s)", differences)
	}
	
	return TxTestResult{
		Valid:   valid,
		Error:   firstError,
		Details: details,
	}
}

// ValidateWithValidator runs validation with a specific validator
func (ctx *TxTestContext) ValidateWithValidator(validatorType ValidatorType) TxTestResult {
	result := ctx.Validator.ValidateScript(validatorType, ctx.Tx, 0, make([]uint32, len(ctx.Tx.Inputs)))
	
	return TxTestResult{
		Valid:   result.Success,
		Error:   result.Error,
		Details: fmt.Sprintf("%s: %v", validatorType, result.Error),
	}
}

// GetScriptFlags converts string flags to flag values
func (ctx *TxTestContext) GetScriptFlags() uint32 {
	var flags uint32
	
	for _, flag := range ctx.Flags {
		switch flag {
		case "P2SH":
			// flags |= SCRIPT_VERIFY_P2SH
		case "STRICTENC":
			// flags |= SCRIPT_VERIFY_STRICTENC
		case "DERSIG":
			// flags |= SCRIPT_VERIFY_DERSIG
		case "LOW_S":
			// flags |= SCRIPT_VERIFY_LOW_S
		case "NULLDUMMY":
			// flags |= SCRIPT_VERIFY_NULLDUMMY
		case "SIGPUSHONLY":
			// flags |= SCRIPT_VERIFY_SIGPUSHONLY
		case "MINIMALDATA":
			// flags |= SCRIPT_VERIFY_MINIMALDATA
		case "DISCOURAGE_UPGRADABLE_NOPS":
			// flags |= SCRIPT_VERIFY_DISCOURAGE_UPGRADABLE_NOPS
		case "CLEANSTACK":
			// flags |= SCRIPT_VERIFY_CLEANSTACK
		case "CHECKLOCKTIMEVERIFY":
			// flags |= SCRIPT_VERIFY_CHECKLOCKTIMEVERIFY
		case "CHECKSEQUENCEVERIFY":
			// flags |= SCRIPT_VERIFY_CHECKSEQUENCEVERIFY
		case "UTXO_AFTER_GENESIS":
			// flags |= SCRIPT_VERIFY_UTXO_AFTER_GENESIS
		case "FORKID":
			// flags |= SCRIPT_VERIFY_FORKID
		}
	}
	
	return flags
}

// IsExtended checks if the transaction has been extended with previous output info
func (ctx *TxTestContext) IsExtended() bool {
	for _, input := range ctx.Tx.Inputs {
		if input.PreviousTxScript == nil || input.PreviousTxSatoshis == 0 {
			return false
		}
	}
	return true
}

// GetTransactionInfo returns information about the transaction
func (ctx *TxTestContext) GetTransactionInfo() map[string]interface{} {
	info := make(map[string]interface{})
	
	info["txid"] = ctx.Tx.TxID()
	info["version"] = ctx.Tx.Version
	info["locktime"] = ctx.Tx.LockTime
	info["size"] = ctx.Tx.Size()
	info["num_inputs"] = len(ctx.Tx.Inputs)
	info["num_outputs"] = len(ctx.Tx.Outputs)
	info["is_extended"] = ctx.IsExtended()
	info["flags"] = ctx.Flags
	info["should_pass"] = ctx.ShouldPass
	
	// Calculate total input and output values
	var totalInput, totalOutput uint64
	for _, input := range ctx.Tx.Inputs {
		totalInput += input.PreviousTxSatoshis
	}
	for _, output := range ctx.Tx.Outputs {
		totalOutput += output.Satoshis
	}
	
	info["total_input"] = totalInput
	info["total_output"] = totalOutput
	info["fee"] = int64(totalInput) - int64(totalOutput)
	
	return info
}

// GetInputScripts returns the unlocking and locking scripts for each input
func (ctx *TxTestContext) GetInputScripts() []map[string]string {
	var scripts []map[string]string
	
	for i, input := range ctx.Tx.Inputs {
		scriptInfo := make(map[string]string)
		
		if input.UnlockingScript != nil {
			scriptInfo["unlocking_script"] = hex.EncodeToString(*input.UnlockingScript)
		} else {
			scriptInfo["unlocking_script"] = ""
		}
		
		if input.PreviousTxScript != nil {
			scriptInfo["locking_script"] = hex.EncodeToString(*input.PreviousTxScript)
		} else {
			scriptInfo["locking_script"] = ""
		}
		
		if i < len(ctx.Inputs) {
			scriptInfo["original_locking_script"] = ctx.Inputs[i].ScriptPubKey
		}
		
		scripts = append(scripts, scriptInfo)
	}
	
	return scripts
}

// CreateTestTransaction creates a transaction for testing purposes
func CreateTestTransaction(inputs []TxTestInput, outputs []TestOutput) (*bt.Tx, error) {
	tx := bt.NewTx()
	tx.Version = 1
	tx.LockTime = 0
	
	parser := NewScriptParser()
	
	// Add inputs
	tx.Inputs = make([]*bt.Input, len(inputs))
	for i, input := range inputs {
		// Parse previous output script
		prevScript, err := parser.ParseScript(input.ScriptPubKey)
		if err != nil {
			return nil, fmt.Errorf("failed to parse previous output script for input %d: %w", i, err)
		}
		script := bscript.Script(prevScript)
		
		tx.Inputs[i] = &bt.Input{
			PreviousTxOutIndex: input.Vout,
			UnlockingScript:    &bscript.Script{}, // Will be set later
			SequenceNumber:     0xffffffff,
			PreviousTxSatoshis: 100000000, // 1 BTC default
			PreviousTxScript:   &script,
		}
		
		// TODO: Set previous transaction ID if needed
		// The go-bt library handles this differently
	}
	
	// Add outputs
	tx.Outputs = make([]*bt.Output, len(outputs))
	for i, output := range outputs {
		// Parse locking script
		lockingScript, err := parser.ParseScript(output.Script)
		if err != nil {
			return nil, fmt.Errorf("failed to parse locking script for output %d: %w", i, err)
		}
		script := bscript.Script(lockingScript)
		
		tx.Outputs[i] = &bt.Output{
			Satoshis:      output.Satoshis,
			LockingScript: &script,
		}
	}
	
	return tx, nil
}

// TestOutput represents a transaction output for testing
type TestOutput struct {
	Satoshis uint64
	Script   string
}