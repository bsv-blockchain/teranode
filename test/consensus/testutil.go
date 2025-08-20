package consensus

import (
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/bitcoin-sv/teranode/errors"
)

// ScriptTest represents a single script validation test case from script_tests.json
type ScriptTest struct {
	ScriptSig    string        // Unlocking script (scriptSig)
	ScriptPubKey string        // Locking script (scriptPubKey)
	Flags        []string      // Validation flags (e.g., "P2SH", "STRICTENC")
	Expected     string        // Expected result (e.g., "OK", specific error)
	Comment      string        // Optional comment describing the test
	Raw          []interface{} // Raw test data for debugging
}

// TxTest represents a transaction validation test case from tx_valid.json or tx_invalid.json
type TxTest struct {
	Inputs  []TxTestInput // Previous outputs being spent
	RawTx   string        // Serialized transaction in hex
	Flags   []string      // Validation flags
	Comment string        // Optional comment
	Raw     []interface{} // Raw test data
}

// TxTestInput represents an input in a transaction test
type TxTestInput struct {
	TxID         string // Previous transaction ID
	Vout         uint32 // Output index
	ScriptPubKey string // Locking script of the output being spent
}

// SigHashTest represents a signature hash test case from sighash.json
type SigHashTest struct {
	RawTx        string        // Raw transaction hex
	Script       string        // Script being executed
	InputIndex   int           // Index of input being signed
	HashType     int           // Signature hash type
	ExpectedHash string        // Expected signature hash
	Comment      string        // Optional comment
	Raw          []interface{} // Raw test data
}

// LoadScriptTests loads script test vectors from a JSON file
func LoadScriptTests(t *testing.T, filename string) []ScriptTest {
	t.Helper()

	data, err := os.ReadFile(filename)
	if err != nil {
		t.Fatalf("Failed to read test file %s: %v", filename, err)
	}

	var rawTests [][]interface{}
	if err := json.Unmarshal(data, &rawTests); err != nil {
		t.Fatalf("Failed to parse test file %s: %v", filename, err)
	}

	tests := make([]ScriptTest, 0, len(rawTests))
	for i, rawTest := range rawTests {
		// Skip comments (arrays with single string)
		if len(rawTest) == 1 {
			continue
		}

		if len(rawTest) < 4 {
			t.Logf("Skipping malformed test at index %d: %v", i, rawTest)
			continue
		}

		test := ScriptTest{
			Raw: rawTest,
		}

		// Parse scriptSig
		if sig, ok := rawTest[0].(string); ok {
			test.ScriptSig = sig
		}

		// Parse scriptPubKey
		if pubkey, ok := rawTest[1].(string); ok {
			test.ScriptPubKey = pubkey
		}

		// Parse flags
		if flags, ok := rawTest[2].(string); ok {
			test.Flags = strings.Split(flags, ",")
			// Trim whitespace from flags
			for j := range test.Flags {
				test.Flags[j] = strings.TrimSpace(test.Flags[j])
			}
		}

		// Parse expected result
		if expected, ok := rawTest[3].(string); ok {
			test.Expected = expected
		}

		// Parse optional comment
		if len(rawTest) > 4 {
			if comment, ok := rawTest[4].(string); ok {
				test.Comment = comment
			}
		}

		tests = append(tests, test)
	}

	return tests
}

// LoadTxTests loads transaction test vectors from a JSON file
func LoadTxTests(t *testing.T, filename string) []TxTest {
	t.Helper()

	data, err := os.ReadFile(filename)
	if err != nil {
		t.Fatalf("Failed to read test file %s: %v", filename, err)
	}

	var rawTests [][]interface{}
	if err := json.Unmarshal(data, &rawTests); err != nil {
		t.Fatalf("Failed to parse test file %s: %v", filename, err)
	}

	tests := make([]TxTest, 0, len(rawTests))
	for i, rawTest := range rawTests {
		// Skip comments
		if len(rawTest) == 1 {
			continue
		}

		if len(rawTest) < 3 {
			t.Logf("Skipping malformed test at index %d", i)
			continue
		}

		test := TxTest{
			Raw: rawTest,
		}

		// Parse inputs
		if inputs, ok := rawTest[0].([]interface{}); ok {
			for _, rawInput := range inputs {
				if inputArray, ok := rawInput.([]interface{}); ok && len(inputArray) >= 3 {
					input := TxTestInput{}

					// Parse txid
					if txid, ok := inputArray[0].(string); ok {
						input.TxID = txid
					}

					// Parse vout
					if vout, ok := inputArray[1].(float64); ok {
						input.Vout = uint32(vout)
					}

					// Parse scriptPubKey
					if script, ok := inputArray[2].(string); ok {
						input.ScriptPubKey = script
					}

					test.Inputs = append(test.Inputs, input)
				}
			}
		}

		// Parse raw transaction
		if rawtx, ok := rawTest[1].(string); ok {
			test.RawTx = rawtx
		}

		// Parse flags
		if flags, ok := rawTest[2].([]interface{}); ok {
			for _, flag := range flags {
				if flagStr, ok := flag.(string); ok {
					test.Flags = append(test.Flags, flagStr)
				}
			}
		} else if flagStr, ok := rawTest[2].(string); ok {
			// Some tests have flags as a single string
			test.Flags = strings.Split(flagStr, ",")
		}

		// Parse optional comment
		if len(rawTest) > 3 {
			if comment, ok := rawTest[3].(string); ok {
				test.Comment = comment
			}
		}

		tests = append(tests, test)
	}

	return tests
}

// LoadSigHashTests loads signature hash test vectors from a JSON file
func LoadSigHashTests(t *testing.T, filename string) []SigHashTest {
	t.Helper()

	data, err := os.ReadFile(filename)
	if err != nil {
		t.Fatalf("Failed to read test file %s: %v", filename, err)
	}

	var rawTests [][]interface{}
	if err := json.Unmarshal(data, &rawTests); err != nil {
		t.Fatalf("Failed to parse test file %s: %v", filename, err)
	}

	tests := make([]SigHashTest, 0, len(rawTests))
	for i, rawTest := range rawTests {
		// Skip comments
		if len(rawTest) == 1 {
			continue
		}

		if len(rawTest) < 5 {
			t.Logf("Skipping malformed test at index %d", i)
			continue
		}

		test := SigHashTest{
			Raw: rawTest,
		}

		// Parse raw transaction
		if rawtx, ok := rawTest[0].(string); ok {
			test.RawTx = rawtx
		}

		// Parse script
		if script, ok := rawTest[1].(string); ok {
			test.Script = script
		}

		// Parse input index
		if idx, ok := rawTest[2].(float64); ok {
			test.InputIndex = int(idx)
		}

		// Parse hash type
		if hashType, ok := rawTest[3].(float64); ok {
			test.HashType = int(hashType)
		}

		// Parse expected hash
		if hash, ok := rawTest[4].(string); ok {
			test.ExpectedHash = hash
		}

		// Parse optional comment
		if len(rawTest) > 5 {
			if comment, ok := rawTest[5].(string); ok {
				test.Comment = comment
			}
		}

		tests = append(tests, test)
	}

	return tests
}

// GetTestDataPath returns the path to the testdata directory
func GetTestDataPath() string {
	return filepath.Join("testdata")
}

// ParseScriptString converts a human-readable script string to bytes
// Handles formats like "1 2 ADD", "0x20 0xabcd...", "'text'", etc.
func ParseScriptString(scriptStr string) ([]byte, error) {
	if scriptStr == "" {
		return []byte{}, nil
	}

	// If it's already hex, decode it
	if strings.HasPrefix(scriptStr, "0x") {
		return hex.DecodeString(scriptStr[2:])
	}

	// Otherwise, this would need a full script parser
	// For now, we'll need to integrate with the script package
	// This is a placeholder that will be implemented when we have the script parser
	return nil, errors.NewProcessingError("script parsing not yet implemented for: %s", scriptStr)
}

// FlagsFromStrings converts string flags to the appropriate validation flags
func FlagsFromStrings(flagStrs []string) uint32 {
	// This will map string flags to actual validation flag constants
	// For now, return a placeholder
	// TODO: Implement flag mapping based on the validation engine being used
	var flags uint32
	for _, flag := range flagStrs {
		switch flag {
		case "P2SH":
			// flags |= SCRIPT_VERIFY_P2SH
		case "STRICTENC":
			// flags |= SCRIPT_VERIFY_STRICTENC
			// Add more flag mappings as needed
		}
	}
	return flags
}
