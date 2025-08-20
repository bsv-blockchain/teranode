package consensus

import (
	"strings"

	"github.com/bitcoin-sv/teranode/errors"
)

// Script verification flags - must match C++ definitions
const (
	SCRIPT_VERIFY_NONE                       uint32 = 0
	SCRIPT_VERIFY_P2SH                       uint32 = (1 << 0)
	SCRIPT_VERIFY_STRICTENC                  uint32 = (1 << 1)
	SCRIPT_VERIFY_DERSIG                     uint32 = (1 << 2)
	SCRIPT_VERIFY_LOW_S                      uint32 = (1 << 3)
	SCRIPT_VERIFY_SIGPUSHONLY                uint32 = (1 << 5)
	SCRIPT_VERIFY_MINIMALDATA                uint32 = (1 << 6)
	SCRIPT_VERIFY_NULLDUMMY                  uint32 = (1 << 7)
	SCRIPT_VERIFY_DISCOURAGE_UPGRADABLE_NOPS uint32 = (1 << 8)
	SCRIPT_VERIFY_CLEANSTACK                 uint32 = (1 << 9)
	SCRIPT_VERIFY_MINIMALIF                  uint32 = (1 << 10)
	SCRIPT_VERIFY_NULLFAIL                   uint32 = (1 << 11)
	SCRIPT_VERIFY_CHECKLOCKTIMEVERIFY        uint32 = (1 << 12)
	SCRIPT_VERIFY_CHECKSEQUENCEVERIFY        uint32 = (1 << 13)
	SCRIPT_VERIFY_COMPRESSED_PUBKEYTYPE      uint32 = (1 << 14)
	SCRIPT_ENABLE_SIGHASH_FORKID             uint32 = (1 << 15)
	SCRIPT_GENESIS                           uint32 = (1 << 16)
	SCRIPT_UTXO_AFTER_GENESIS                uint32 = (1 << 17)
)

// mapFlagNames maps flag names to their values
var mapFlagNames = map[string]uint32{
	"NONE":                       SCRIPT_VERIFY_NONE,
	"P2SH":                       SCRIPT_VERIFY_P2SH,
	"STRICTENC":                  SCRIPT_VERIFY_STRICTENC,
	"DERSIG":                     SCRIPT_VERIFY_DERSIG,
	"LOW_S":                      SCRIPT_VERIFY_LOW_S,
	"SIGPUSHONLY":                SCRIPT_VERIFY_SIGPUSHONLY,
	"MINIMALDATA":                SCRIPT_VERIFY_MINIMALDATA,
	"NULLDUMMY":                  SCRIPT_VERIFY_NULLDUMMY,
	"DISCOURAGE_UPGRADABLE_NOPS": SCRIPT_VERIFY_DISCOURAGE_UPGRADABLE_NOPS,
	"CLEANSTACK":                 SCRIPT_VERIFY_CLEANSTACK,
	"MINIMALIF":                  SCRIPT_VERIFY_MINIMALIF,
	"NULLFAIL":                   SCRIPT_VERIFY_NULLFAIL,
	"CHECKLOCKTIMEVERIFY":        SCRIPT_VERIFY_CHECKLOCKTIMEVERIFY,
	"CHECKSEQUENCEVERIFY":        SCRIPT_VERIFY_CHECKSEQUENCEVERIFY,
	"COMPRESSED_PUBKEYTYPE":      SCRIPT_VERIFY_COMPRESSED_PUBKEYTYPE,
	"SIGHASH_FORKID":             SCRIPT_ENABLE_SIGHASH_FORKID,
	"GENESIS":                    SCRIPT_GENESIS,
	"UTXO_AFTER_GENESIS":         SCRIPT_UTXO_AFTER_GENESIS,
}

// ParseScriptFlags parses a comma-separated list of script flags
func ParseScriptFlags(strFlags string) (uint32, error) {
	if strFlags == "" {
		return 0, nil
	}

	var flags uint32
	words := strings.Split(strFlags, ",")

	for _, word := range words {
		word = strings.TrimSpace(word)
		if flagValue, ok := mapFlagNames[word]; ok {
			flags |= flagValue
		} else {
			return 0, errors.NewProcessingError("unknown verification flag '%s'", word)
		}
	}

	return flags, nil
}

// FormatScriptFlags converts flags back to a comma-separated string
func FormatScriptFlags(flags uint32) string {
	if flags == 0 {
		return ""
	}

	var flagStrs []string
	// Check flags in a specific order for consistent output
	flagOrder := []struct {
		name string
		flag uint32
	}{
		{"P2SH", SCRIPT_VERIFY_P2SH},
		{"STRICTENC", SCRIPT_VERIFY_STRICTENC},
		{"DERSIG", SCRIPT_VERIFY_DERSIG},
		{"LOW_S", SCRIPT_VERIFY_LOW_S},
		{"SIGPUSHONLY", SCRIPT_VERIFY_SIGPUSHONLY},
		{"MINIMALDATA", SCRIPT_VERIFY_MINIMALDATA},
		{"NULLDUMMY", SCRIPT_VERIFY_NULLDUMMY},
		{"DISCOURAGE_UPGRADABLE_NOPS", SCRIPT_VERIFY_DISCOURAGE_UPGRADABLE_NOPS},
		{"CLEANSTACK", SCRIPT_VERIFY_CLEANSTACK},
		{"MINIMALIF", SCRIPT_VERIFY_MINIMALIF},
		{"NULLFAIL", SCRIPT_VERIFY_NULLFAIL},
		{"CHECKLOCKTIMEVERIFY", SCRIPT_VERIFY_CHECKLOCKTIMEVERIFY},
		{"CHECKSEQUENCEVERIFY", SCRIPT_VERIFY_CHECKSEQUENCEVERIFY},
		{"COMPRESSED_PUBKEYTYPE", SCRIPT_VERIFY_COMPRESSED_PUBKEYTYPE},
		{"SIGHASH_FORKID", SCRIPT_ENABLE_SIGHASH_FORKID},
		{"GENESIS", SCRIPT_GENESIS},
		{"UTXO_AFTER_GENESIS", SCRIPT_UTXO_AFTER_GENESIS},
	}

	for _, item := range flagOrder {
		if flags&item.flag != 0 {
			flagStrs = append(flagStrs, item.name)
		}
	}

	return strings.Join(flagStrs, ",")
}

// ConvertToValidatorFlags converts script flags to validator-specific flags
// This is needed because the validator package may use different flag values
func ConvertToValidatorFlags(scriptFlags uint32) uint32 {
	// TODO: Map script test flags to validator package flags
	// For now, return a sensible default
	var validatorFlags uint32

	// Map common flags
	if scriptFlags&SCRIPT_VERIFY_P2SH != 0 {
		// validatorFlags |= validator.SCRIPT_VERIFY_P2SH
	}
	if scriptFlags&SCRIPT_VERIFY_STRICTENC != 0 {
		// validatorFlags |= validator.SCRIPT_VERIFY_STRICTENC
	}
	// Add more mappings as needed

	return validatorFlags
}
