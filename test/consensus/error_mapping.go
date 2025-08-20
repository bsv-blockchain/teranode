package consensus

import (
	"strings"

	"github.com/bitcoin-sv/teranode/errors"
)

// MapValidatorError maps validator package errors to our ScriptError types
func MapValidatorError(err error) ScriptError {
	if err == nil {
		return SCRIPT_ERR_OK
	}

	errStr := err.Error()

	// Check for specific error patterns
	switch {
	// Transaction errors
	case strings.Contains(errStr, "transaction has no inputs or outputs"):
		return SCRIPT_ERR_UNKNOWN_ERROR
	case strings.Contains(errStr, "transaction output") && strings.Contains(errStr, "satoshis is invalid"):
		return SCRIPT_ERR_UNKNOWN_ERROR
	case strings.Contains(errStr, "transaction output total satoshis is too high"):
		return SCRIPT_ERR_UNKNOWN_ERROR
	case strings.Contains(errStr, "zero-satoshi outputs require 'OP_FALSE OP_RETURN' prefix"):
		return SCRIPT_ERR_UNKNOWN_ERROR

	// Script validation errors
	case strings.Contains(errStr, "Script evaluated without error but finished with a false/empty top stack element"):
		return SCRIPT_ERR_EVAL_FALSE
	case strings.Contains(errStr, "script evaluated without error but finished with a false"):
		return SCRIPT_ERR_EVAL_FALSE
	case strings.Contains(errStr, "false stack entry at end of script execution"):
		return SCRIPT_ERR_EVAL_FALSE
	case strings.Contains(errStr, "OP_RETURN"):
		return SCRIPT_ERR_OP_RETURN
	case strings.Contains(errStr, "script returned early"):
		return SCRIPT_ERR_OP_RETURN
	case strings.Contains(errStr, "script is too large"):
		return SCRIPT_ERR_SCRIPT_SIZE
	case strings.Contains(errStr, "push value size limit exceeded"):
		return SCRIPT_ERR_PUSH_SIZE
	case strings.Contains(errStr, "operation limit exceeded"):
		return SCRIPT_ERR_OP_COUNT
	case strings.Contains(errStr, "stack size limit exceeded"):
		return SCRIPT_ERR_STACK_SIZE
	case strings.Contains(errStr, "signature count"):
		return SCRIPT_ERR_SIG_COUNT
	case strings.Contains(errStr, "pubkey count"):
		return SCRIPT_ERR_PUBKEY_COUNT

	// Operation errors
	case strings.Contains(errStr, "invalid operand size"):
		return SCRIPT_ERR_INVALID_OPERAND_SIZE
	case strings.Contains(errStr, "invalid number range"):
		return SCRIPT_ERR_INVALID_NUMBER_RANGE
	case strings.Contains(errStr, "invalid split range"):
		return SCRIPT_ERR_INVALID_SPLIT_RANGE
	case strings.Contains(errStr, "script number overflow"):
		return SCRIPT_ERR_SCRIPTNUM_OVERFLOW
	case strings.Contains(errStr, "not minimally encoded"):
		return SCRIPT_ERR_SCRIPTNUM_MINENCODE

	// Verification errors
	case strings.Contains(errStr, "script failed an OP_VERIFY operation"):
		return SCRIPT_ERR_VERIFY
	case strings.Contains(errStr, "script failed an OP_EQUALVERIFY operation"):
		return SCRIPT_ERR_EQUALVERIFY
	case strings.Contains(errStr, "OP_EQUALVERIFY failed"):
		return SCRIPT_ERR_EQUALVERIFY
	case strings.Contains(errStr, "script failed an OP_CHECKMULTISIGVERIFY operation"):
		return SCRIPT_ERR_CHECKMULTISIGVERIFY
	case strings.Contains(errStr, "script failed an OP_CHECKSIGVERIFY operation"):
		return SCRIPT_ERR_CHECKSIGVERIFY
	case strings.Contains(errStr, "script failed an OP_NUMEQUALVERIFY operation"):
		return SCRIPT_ERR_NUMEQUALVERIFY

	// Opcode errors
	case strings.Contains(errStr, "bad opcode") || strings.Contains(errStr, "opcode missing"):
		return SCRIPT_ERR_BAD_OPCODE
	case strings.Contains(errStr, "disabled opcode"):
		return SCRIPT_ERR_DISABLED_OPCODE
	case strings.Contains(errStr, "invalid stack operation"):
		return SCRIPT_ERR_INVALID_STACK_OPERATION
	case strings.Contains(errStr, "Operation not valid with the current stack size"):
		return SCRIPT_ERR_INVALID_STACK_OPERATION
	case strings.Contains(errStr, "index") && strings.Contains(errStr, "is invalid for stack size"):
		return SCRIPT_ERR_INVALID_STACK_OPERATION
	case strings.Contains(errStr, "invalid altstack operation"):
		return SCRIPT_ERR_INVALID_ALTSTACK_OPERATION
	case strings.Contains(errStr, "unbalanced conditional"):
		return SCRIPT_ERR_UNBALANCED_CONDITIONAL
	case strings.Contains(errStr, "Invalid OP_IF construction"):
		return SCRIPT_ERR_UNBALANCED_CONDITIONAL
	case strings.Contains(errStr, "end of script reached in conditional execution"):
		return SCRIPT_ERR_UNBALANCED_CONDITIONAL

	// Locktime errors
	case strings.Contains(errStr, "negative locktime"):
		return SCRIPT_ERR_NEGATIVE_LOCKTIME
	case strings.Contains(errStr, "locktime requirement not satisfied"):
		return SCRIPT_ERR_UNSATISFIED_LOCKTIME

	// Signature errors
	case strings.Contains(errStr, "signature hash type"):
		return SCRIPT_ERR_SIG_HASHTYPE
	case strings.Contains(errStr, "non-canonical DER signature"):
		return SCRIPT_ERR_SIG_DER
	case strings.Contains(errStr, "data push larger than necessary"):
		return SCRIPT_ERR_MINIMALDATA
	case strings.Contains(errStr, "only non-push operators allowed in signatures"):
		return SCRIPT_ERR_SIG_PUSHONLY
	case strings.Contains(errStr, "signature is not canonical high S value"):
		return SCRIPT_ERR_SIG_HIGH_S
	case strings.Contains(errStr, "dummy element must be zero"):
		return SCRIPT_ERR_SIG_NULLDUMMY
	case strings.Contains(errStr, "public key is neither compressed or uncompressed"):
		return SCRIPT_ERR_PUBKEYTYPE
	case strings.Contains(errStr, "extra items left on stack"):
		return SCRIPT_ERR_CLEANSTACK
	case strings.Contains(errStr, "OP_IF/NOTIF argument must be minimal"):
		return SCRIPT_ERR_MINIMALIF
	case strings.Contains(errStr, "signature must be zero for failed CHECK"):
		return SCRIPT_ERR_SIG_NULLFAIL
	case strings.Contains(errStr, "NOPx reserved"):
		return SCRIPT_ERR_DISCOURAGE_UPGRADABLE_NOPS
	case strings.Contains(errStr, "non-compressed public key"):
		return SCRIPT_ERR_NONCOMPRESSED_PUBKEY
	case strings.Contains(errStr, "illegal use of SIGHASH_FORKID"):
		return SCRIPT_ERR_ILLEGAL_FORKID
	case strings.Contains(errStr, "must use SIGHASH_FORKID"):
		return SCRIPT_ERR_MUST_USE_FORKID

	// Math errors
	case strings.Contains(errStr, "division by zero"):
		return SCRIPT_ERR_DIV_BY_ZERO
	case strings.Contains(errStr, "modulo by zero"):
		return SCRIPT_ERR_MOD_BY_ZERO

	default:
		return SCRIPT_ERR_UNKNOWN_ERROR
	}
}

// ExtractScriptError attempts to extract a specific script error from a validator error
func ExtractScriptError(err error) ScriptError {
	if err == nil {
		return SCRIPT_ERR_OK
	}

	// First try direct mapping
	scriptErr := MapValidatorError(err)
	if scriptErr != SCRIPT_ERR_UNKNOWN_ERROR {
		return scriptErr
	}

	// Check if it's a wrapped error
	if terranodeErr, ok := err.(*errors.Error); ok {
		// Try to extract the actual message from the teranode error
		return MapValidatorError(terranodeErr)
	}

	return SCRIPT_ERR_UNKNOWN_ERROR
}

// CompareScriptError checks if a validator error matches the expected script error
func CompareScriptError(validatorErr error, expectedErr ScriptError) bool {
	if expectedErr == SCRIPT_ERR_OK {
		return validatorErr == nil
	}

	if validatorErr == nil {
		return false
	}

	actualErr := ExtractScriptError(validatorErr)
	return actualErr == expectedErr
}
