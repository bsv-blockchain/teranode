package consensus

import "fmt"

// ScriptError represents a script validation error
type ScriptError int

// All script error types from Bitcoin Core/SV
const (
	SCRIPT_ERR_OK ScriptError = iota
	SCRIPT_ERR_UNKNOWN_ERROR
	SCRIPT_ERR_EVAL_FALSE
	SCRIPT_ERR_OP_RETURN
	SCRIPT_ERR_SCRIPT_SIZE
	SCRIPT_ERR_PUSH_SIZE
	SCRIPT_ERR_OP_COUNT
	SCRIPT_ERR_STACK_SIZE
	SCRIPT_ERR_SIG_COUNT
	SCRIPT_ERR_PUBKEY_COUNT
	SCRIPT_ERR_INVALID_OPERAND_SIZE
	SCRIPT_ERR_INVALID_NUMBER_RANGE
	SCRIPT_ERR_INVALID_SPLIT_RANGE
	SCRIPT_ERR_SCRIPTNUM_OVERFLOW
	SCRIPT_ERR_SCRIPTNUM_MINENCODE
	SCRIPT_ERR_VERIFY
	SCRIPT_ERR_EQUALVERIFY
	SCRIPT_ERR_CHECKMULTISIGVERIFY
	SCRIPT_ERR_CHECKSIGVERIFY
	SCRIPT_ERR_NUMEQUALVERIFY
	SCRIPT_ERR_BAD_OPCODE
	SCRIPT_ERR_DISABLED_OPCODE
	SCRIPT_ERR_INVALID_STACK_OPERATION
	SCRIPT_ERR_INVALID_ALTSTACK_OPERATION
	SCRIPT_ERR_UNBALANCED_CONDITIONAL
	SCRIPT_ERR_NEGATIVE_LOCKTIME
	SCRIPT_ERR_UNSATISFIED_LOCKTIME
	SCRIPT_ERR_SIG_HASHTYPE
	SCRIPT_ERR_SIG_DER
	SCRIPT_ERR_MINIMALDATA
	SCRIPT_ERR_SIG_PUSHONLY
	SCRIPT_ERR_SIG_HIGH_S
	SCRIPT_ERR_SIG_NULLDUMMY
	SCRIPT_ERR_PUBKEYTYPE
	SCRIPT_ERR_CLEANSTACK
	SCRIPT_ERR_MINIMALIF
	SCRIPT_ERR_SIG_NULLFAIL
	SCRIPT_ERR_DISCOURAGE_UPGRADABLE_NOPS
	SCRIPT_ERR_NONCOMPRESSED_PUBKEY
	SCRIPT_ERR_ILLEGAL_FORKID
	SCRIPT_ERR_MUST_USE_FORKID
	SCRIPT_ERR_MISSING_FORKID // Alias for SCRIPT_ERR_MUST_USE_FORKID
	SCRIPT_ERR_DIV_BY_ZERO
	SCRIPT_ERR_MOD_BY_ZERO
	SCRIPT_ERR_ERROR_COUNT
)

// ScriptErrorDesc describes a script error
type ScriptErrorDesc struct {
	Error ScriptError
	Name  string
}

// scriptErrors contains all script error mappings
var scriptErrors = []ScriptErrorDesc{
	{SCRIPT_ERR_OK, "OK"},
	{SCRIPT_ERR_UNKNOWN_ERROR, "UNKNOWN_ERROR"},
	{SCRIPT_ERR_EVAL_FALSE, "EVAL_FALSE"},
	{SCRIPT_ERR_OP_RETURN, "OP_RETURN"},
	{SCRIPT_ERR_SCRIPT_SIZE, "SCRIPT_SIZE"},
	{SCRIPT_ERR_PUSH_SIZE, "PUSH_SIZE"},
	{SCRIPT_ERR_OP_COUNT, "OP_COUNT"},
	{SCRIPT_ERR_STACK_SIZE, "STACK_SIZE"},
	{SCRIPT_ERR_SIG_COUNT, "SIG_COUNT"},
	{SCRIPT_ERR_PUBKEY_COUNT, "PUBKEY_COUNT"},
	{SCRIPT_ERR_INVALID_OPERAND_SIZE, "OPERAND_SIZE"},
	{SCRIPT_ERR_INVALID_NUMBER_RANGE, "INVALID_NUMBER_RANGE"},
	{SCRIPT_ERR_INVALID_SPLIT_RANGE, "SPLIT_RANGE"},
	{SCRIPT_ERR_SCRIPTNUM_OVERFLOW, "SCRIPTNUM_OVERFLOW"},
	{SCRIPT_ERR_SCRIPTNUM_MINENCODE, "SCRIPTNUM_MINENCODE"},
	{SCRIPT_ERR_VERIFY, "VERIFY"},
	{SCRIPT_ERR_EQUALVERIFY, "EQUALVERIFY"},
	{SCRIPT_ERR_CHECKMULTISIGVERIFY, "CHECKMULTISIGVERIFY"},
	{SCRIPT_ERR_CHECKSIGVERIFY, "CHECKSIGVERIFY"},
	{SCRIPT_ERR_NUMEQUALVERIFY, "NUMEQUALVERIFY"},
	{SCRIPT_ERR_BAD_OPCODE, "BAD_OPCODE"},
	{SCRIPT_ERR_DISABLED_OPCODE, "DISABLED_OPCODE"},
	{SCRIPT_ERR_INVALID_STACK_OPERATION, "INVALID_STACK_OPERATION"},
	{SCRIPT_ERR_INVALID_ALTSTACK_OPERATION, "INVALID_ALTSTACK_OPERATION"},
	{SCRIPT_ERR_UNBALANCED_CONDITIONAL, "UNBALANCED_CONDITIONAL"},
	{SCRIPT_ERR_NEGATIVE_LOCKTIME, "NEGATIVE_LOCKTIME"},
	{SCRIPT_ERR_UNSATISFIED_LOCKTIME, "UNSATISFIED_LOCKTIME"},
	{SCRIPT_ERR_SIG_HASHTYPE, "SIG_HASHTYPE"},
	{SCRIPT_ERR_SIG_DER, "SIG_DER"},
	{SCRIPT_ERR_MINIMALDATA, "MINIMALDATA"},
	{SCRIPT_ERR_SIG_PUSHONLY, "SIG_PUSHONLY"},
	{SCRIPT_ERR_SIG_HIGH_S, "SIG_HIGH_S"},
	{SCRIPT_ERR_SIG_NULLDUMMY, "SIG_NULLDUMMY"},
	{SCRIPT_ERR_PUBKEYTYPE, "PUBKEYTYPE"},
	{SCRIPT_ERR_CLEANSTACK, "CLEANSTACK"},
	{SCRIPT_ERR_MINIMALIF, "MINIMALIF"},
	{SCRIPT_ERR_SIG_NULLFAIL, "NULLFAIL"},
	{SCRIPT_ERR_DISCOURAGE_UPGRADABLE_NOPS, "DISCOURAGE_UPGRADABLE_NOPS"},
	{SCRIPT_ERR_NONCOMPRESSED_PUBKEY, "NONCOMPRESSED_PUBKEY"},
	{SCRIPT_ERR_ILLEGAL_FORKID, "ILLEGAL_FORKID"},
	{SCRIPT_ERR_MUST_USE_FORKID, "MISSING_FORKID"},
	{SCRIPT_ERR_MISSING_FORKID, "MISSING_FORKID"},
	{SCRIPT_ERR_DIV_BY_ZERO, "DIV_BY_ZERO"},
	{SCRIPT_ERR_MOD_BY_ZERO, "MOD_BY_ZERO"},
}

// String returns the string representation of a script error
func (e ScriptError) String() string {
	if e == SCRIPT_ERR_OK {
		return "No error"
	}
	for _, desc := range scriptErrors {
		if desc.Error == e {
			return desc.Name
		}
	}
	return "UNKNOWN_ERROR"
}

// ParseScriptError converts a string to a ScriptError
func ParseScriptError(name string) (ScriptError, error) {
	for _, desc := range scriptErrors {
		if desc.Name == name {
			return desc.Error, nil
		}
	}
	return SCRIPT_ERR_UNKNOWN_ERROR, fmt.Errorf("unknown script error: %s", name)
}

// GetScriptErrorString returns a human-readable error message
func GetScriptErrorString(err ScriptError) string {
	switch err {
	case SCRIPT_ERR_OK:
		return "No error"
	case SCRIPT_ERR_EVAL_FALSE:
		return "Script evaluated without error but finished with a false/empty top stack element"
	case SCRIPT_ERR_VERIFY:
		return "Script failed an OP_VERIFY operation"
	case SCRIPT_ERR_EQUALVERIFY:
		return "Script failed an OP_EQUALVERIFY operation"
	case SCRIPT_ERR_CHECKMULTISIGVERIFY:
		return "Script failed an OP_CHECKMULTISIGVERIFY operation"
	case SCRIPT_ERR_CHECKSIGVERIFY:
		return "Script failed an OP_CHECKSIGVERIFY operation"
	case SCRIPT_ERR_NUMEQUALVERIFY:
		return "Script failed an OP_NUMEQUALVERIFY operation"
	case SCRIPT_ERR_SCRIPT_SIZE:
		return "Script is too big"
	case SCRIPT_ERR_PUSH_SIZE:
		return "Push value size limit exceeded"
	case SCRIPT_ERR_OP_COUNT:
		return "Operation limit exceeded"
	case SCRIPT_ERR_STACK_SIZE:
		return "Stack size limit exceeded"
	case SCRIPT_ERR_SIG_COUNT:
		return "Signature count negative or greater than pubkey count"
	case SCRIPT_ERR_PUBKEY_COUNT:
		return "Pubkey count negative or limit exceeded"
	case SCRIPT_ERR_BAD_OPCODE:
		return "Opcode missing or not understood"
	case SCRIPT_ERR_DISABLED_OPCODE:
		return "Attempted to use a disabled opcode"
	case SCRIPT_ERR_INVALID_STACK_OPERATION:
		return "Operation not valid with the current stack size"
	case SCRIPT_ERR_INVALID_ALTSTACK_OPERATION:
		return "Operation not valid with the current altstack size"
	case SCRIPT_ERR_OP_RETURN:
		return "OP_RETURN was encountered"
	case SCRIPT_ERR_UNBALANCED_CONDITIONAL:
		return "Invalid OP_IF construction"
	case SCRIPT_ERR_NEGATIVE_LOCKTIME:
		return "Negative locktime"
	case SCRIPT_ERR_UNSATISFIED_LOCKTIME:
		return "Locktime requirement not satisfied"
	case SCRIPT_ERR_SIG_HASHTYPE:
		return "Signature hash type missing or not understood"
	case SCRIPT_ERR_SIG_DER:
		return "Non-canonical DER signature"
	case SCRIPT_ERR_MINIMALDATA:
		return "Data push larger than necessary"
	case SCRIPT_ERR_SIG_PUSHONLY:
		return "Only non-push operators allowed in signatures"
	case SCRIPT_ERR_SIG_HIGH_S:
		return "Non-canonical signature: S value is unnecessarily high"
	case SCRIPT_ERR_SIG_NULLDUMMY:
		return "Dummy CHECKMULTISIG argument must be zero"
	case SCRIPT_ERR_MINIMALIF:
		return "OP_IF/NOTIF argument must be minimal"
	case SCRIPT_ERR_SIG_NULLFAIL:
		return "Signature must be zero for failed CHECK(MULTI)SIG operation"
	case SCRIPT_ERR_DISCOURAGE_UPGRADABLE_NOPS:
		return "NOPx reserved for soft-fork upgrades"
	case SCRIPT_ERR_PUBKEYTYPE:
		return "Public key is neither compressed or uncompressed"
	case SCRIPT_ERR_CLEANSTACK:
		return "Extra items left on stack after execution"
	case SCRIPT_ERR_NONCOMPRESSED_PUBKEY:
		return "Using non-compressed public key"
	case SCRIPT_ERR_ILLEGAL_FORKID:
		return "Illegal use of SIGHASH_FORKID"
	case SCRIPT_ERR_MUST_USE_FORKID:
		return "Signature must use SIGHASH_FORKID"
	case SCRIPT_ERR_INVALID_OPERAND_SIZE:
		return "Invalid operand size"
	case SCRIPT_ERR_INVALID_NUMBER_RANGE:
		return "Invalid number range"
	case SCRIPT_ERR_INVALID_SPLIT_RANGE:
		return "Invalid OP_SPLIT range"
	case SCRIPT_ERR_SCRIPTNUM_OVERFLOW:
		return "Script number overflow"
	case SCRIPT_ERR_SCRIPTNUM_MINENCODE:
		return "Number not minimally encoded"
	case SCRIPT_ERR_DIV_BY_ZERO:
		return "Division by zero"
	case SCRIPT_ERR_MOD_BY_ZERO:
		return "Modulo by zero"
	default:
		return "Unknown error"
	}
}