package consensus

import (
	"fmt"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-chaincfg"
	"github.com/bsv-blockchain/teranode/errors"
	validatorservice "github.com/bsv-blockchain/teranode/services/validator"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/ulogger"
)

// ValidatorType represents the type of validator to use
type ValidatorType string

const (
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

// ValidatorIntegration provides access to different transaction validators.
type ValidatorIntegration struct {
	logger   ulogger.Logger
	settings *settings.Settings
}

// NewValidatorIntegration creates a new validator integration
func NewValidatorIntegration() *ValidatorIntegration {
	testSettings := settings.NewSettings()
	testSettings.ChainCfgParams = &chaincfg.MainNetParams

	return &ValidatorIntegration{
		logger:   ulogger.TestLogger{},
		settings: testSettings,
	}
}

// ValidateTransaction validates a transaction using the specified validator.
func (vi *ValidatorIntegration) ValidateTransaction(validatorType ValidatorType, tx *bt.Tx, blockHeight uint32, utxoHeights []uint32) ValidatorResult {
	result := ValidatorResult{
		ValidatorType: validatorType,
		Success:       true,
	}

	switch validatorType {
	case ValidatorGoBDK:
		if err := vi.validateGoBDKTransaction(tx, blockHeight, utxoHeights); err != nil {
			result.Success = false
			result.Error = err
			result.ErrorMessage = err.Error()
		}

		return result

	default:
		result.Success = false
		result.Error = errors.NewProcessingError("unknown validator type: %s", validatorType)
		return result
	}
}

// ValidateWithAllValidators runs validation with all available validators
func (vi *ValidatorIntegration) ValidateWithAllValidators(tx *bt.Tx, blockHeight uint32, utxoHeights []uint32) map[ValidatorType]ValidatorResult {
	results := make(map[ValidatorType]ValidatorResult)

	validators := []ValidatorType{ValidatorGoBDK}
	for _, v := range validators {
		results[v] = vi.ValidateTransaction(v, tx, blockHeight, utxoHeights)
	}

	return results
}

func (vi *ValidatorIntegration) validateGoBDKTransaction(tx *bt.Tx, blockHeight uint32, utxoHeights []uint32) error {
	txValidator := validatorservice.NewTxValidator(vi.logger, vi.settings)

	return txValidator.ValidateTransaction(tx, blockHeight, utxoHeights, &validatorservice.Options{SkipPolicyChecks: true})
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
