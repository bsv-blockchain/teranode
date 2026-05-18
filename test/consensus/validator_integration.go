package consensus

import (
	"fmt"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-chaincfg"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/services/validator"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/ulogger"
)

// ValidatorType represents the type of validator to use
type ValidatorType string

const (
	ValidatorGoBT  ValidatorType = "go-bt"
	ValidatorGoSDK ValidatorType = "go-sdk"
	ValidatorGoBDK ValidatorType = "go-bdk"
)

// ValidatorResult represents the result of a validation
type ValidatorResult struct {
	ValidatorType ValidatorType
	Success       bool
	Error         error
	ErrorCode     string
	ErrorMessage  string
}

// ValidatorIntegration provides access to different script validators
type ValidatorIntegration struct {
	logger ulogger.Logger
	policy *settings.PolicySettings
	params *chaincfg.Params
}

// NewValidatorIntegration creates a new validator integration
func NewValidatorIntegration() *ValidatorIntegration {
	return &ValidatorIntegration{
		logger: ulogger.TestLogger{},
		policy: settings.NewPolicySettings(),
		params: &chaincfg.MainNetParams,
	}
}

// ValidateScript validates a transaction using the specified validator.
//
// The method name is historical. For GoBDK, TxScriptInterpreter.VerifyScript
// now delegates to BDK ValidateTransaction in production. This consensus-test
// helper intentionally keeps script-only coverage by calling BDK VerifyScript
// through verifyGoBDKScriptOnly.
func (vi *ValidatorIntegration) ValidateScript(validatorType ValidatorType, tx *bt.Tx, blockHeight uint32, utxoHeights []uint32) ValidatorResult {
	result := ValidatorResult{
		ValidatorType: validatorType,
		Success:       true,
	}

	// Get the appropriate validator from the factory
	switch validatorType {
	case ValidatorGoBDK:
		if err := vi.verifyGoBDKScriptOnly(tx, blockHeight, utxoHeights); err != nil {
			result.Success = false
			result.Error = err
			result.ErrorMessage = err.Error()
		}

		return result

	case ValidatorGoBT:
		factory, exists := validator.TxScriptInterpreterFactory[validator.TxInterpreterGoBT]
		if !exists {
			result.Success = false
			result.Error = errors.NewProcessingError("go-bt validator not registered")
			return result
		}

		return vi.validateWithScriptInterpreter(result, factory(vi.logger, vi.policy, vi.params), tx, blockHeight, utxoHeights)

	case ValidatorGoSDK:
		factory, exists := validator.TxScriptInterpreterFactory[validator.TxInterpreterGoSDK]
		if !exists {
			result.Success = false
			result.Error = errors.NewProcessingError("go-sdk validator not registered")
			return result
		}

		return vi.validateWithScriptInterpreter(result, factory(vi.logger, vi.policy, vi.params), tx, blockHeight, utxoHeights)

	default:
		result.Success = false
		result.Error = errors.NewProcessingError("unknown validator type: %s", validatorType)
		return result
	}
}

func (vi *ValidatorIntegration) validateWithScriptInterpreter(result ValidatorResult, scriptInterpreter validator.TxScriptInterpreter, tx *bt.Tx, blockHeight uint32, utxoHeights []uint32) ValidatorResult {
	// Run legacy script-interpreter validation.
	err := scriptInterpreter.VerifyScript(tx, blockHeight, true, utxoHeights)
	if err != nil {
		result.Success = false
		result.Error = err
		result.ErrorMessage = err.Error()
		// TODO: Extract error code from error if available
	}

	return result
}

// ValidateWithAllValidators runs validation with all available validators
func (vi *ValidatorIntegration) ValidateWithAllValidators(tx *bt.Tx, blockHeight uint32, utxoHeights []uint32) map[ValidatorType]ValidatorResult {
	results := make(map[ValidatorType]ValidatorResult)

	// Test with all validators
	// Only use GoBDK validator - go-bt and go-sdk disabled
	validators := []ValidatorType{ValidatorGoBDK}
	for _, v := range validators {
		results[v] = vi.ValidateScript(v, tx, blockHeight, utxoHeights)
	}

	return results
}

// CompareResults compares validation results from different validators
func CompareResults(results map[ValidatorType]ValidatorResult) (bool, string) {
	// Check if all validators agree on success/failure
	var firstResult *ValidatorResult
	allAgree := true
	var differences []string

	for validatorType, result := range results {
		if firstResult == nil {
			firstResult = &result
		} else {
			if firstResult.Success != result.Success {
				allAgree = false
				diff := fmt.Sprintf("%s: success=%v, %s: success=%v",
					firstResult.ValidatorType, firstResult.Success,
					validatorType, result.Success)
				differences = append(differences, diff)
			}
			// Also compare error messages if both failed
			if !firstResult.Success && !result.Success && firstResult.ErrorMessage != result.ErrorMessage {
				diff := fmt.Sprintf("%s error: %s, %s error: %s",
					firstResult.ValidatorType, firstResult.ErrorMessage,
					validatorType, result.ErrorMessage)
				differences = append(differences, diff)
			}
		}
	}

	if allAgree {
		return true, ""
	}

	return false, fmt.Sprintf("Validators disagree: %v", differences)
}
