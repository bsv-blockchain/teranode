package consensus

import (
	"encoding/hex"
	"encoding/json"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/bsv-blockchain/teranode/errors"
)

// ScriptTest represents a single script validation test case from script_tests.json
type ScriptTest struct {
	Index        int           // Position of the entry in the JSON array
	Amount       uint64        // Value of the output being spent, in satoshis; 0 unless the entry starts with [amount in BSV]
	TxVersion    int32         // Version of the spending transaction
	ScriptSig    string        // Unlocking script (scriptSig)
	ScriptPubKey string        // Locking script (scriptPubKey)
	Flags        []string      // Validation flags (e.g., "P2SH", "STRICTENC")
	Expected     string        // Expected result (e.g., "OK", specific error)
	Comment      string        // Optional comment describing the test
	Raw          []interface{} // Raw test data for debugging
}

// TxTest represents a transaction validation test case from tx_valid.json or tx_invalid.json
type TxTest struct {
	Index    int           // Position of the entry in the JSON array
	Inputs   []TxTestInput // Previous outputs being spent
	RawTx    string        // Serialized transaction in hex
	FlagSets []string      // Each entry is one comma-separated flag set to verify with, or "NONE"
	Comment  string        // The comment entries that precede the test in the file
	Raw      []interface{} // Raw test data
}

// TxTestInput represents an input in a transaction test
type TxTestInput struct {
	TxID         string // Previous transaction ID, in display (big-endian) hex
	Vout         uint32 // Output index
	ScriptPubKey string // Locking script of the output being spent
	Amount       int64  // Value of the output being spent; 0 when the vector gives none, as in bitcoin-sv
}

// SigHashTest represents a signature hash test case from sighash.json:
// [raw transaction, script code, input index, hash type, SignatureHash, SignatureHashOriginal].
type SigHashTest struct {
	Index        int           // Position of the entry in the JSON array
	RawTx        string        // Raw transaction hex
	Script       string        // Script code, hex
	InputIndex   int           // Index of the input being signed
	HashType     int64         // Hash type, a full 32-bit value
	RegularHash  string        // bitcoin-sv SignatureHash with amount 0: BIP143 when the FORKID bit is set, else the original algorithm
	OriginalHash string        // bitcoin-sv SignatureHashOriginal: the original algorithm whatever the FORKID bit
	Raw          []interface{} // Raw test data
}

// LoadScriptTests loads script_tests.json in bitcoin-sv's format (v1.2.3): an entry is a
// comment, [ "text" ], or [[amount]?, tx version, scriptSig, scriptPubKey, flags,
// expected error, comment?], where amount is the spent output's value in BSV, read as
// bitcoin-sv's AmountFromValue does. A malformed entry fails the test, as in bitcoin-sv.
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
		if len(rawTest) == 1 {
			continue
		}

		test, err := parseScriptTest(rawTest)
		if err != nil {
			t.Fatalf("Bad test at index %d of %s: %v", i, filename, err)
		}

		test.Index = i
		tests = append(tests, test)
	}

	return tests
}

func parseScriptTest(rawTest []interface{}) (ScriptTest, error) {
	test := ScriptTest{Raw: rawTest}

	if amount, ok := rawTest[0].([]interface{}); ok {
		var btc float64

		if len(amount) > 0 {
			btc, ok = amount[0].(float64)
		}

		if !ok || len(amount) == 0 || btc < 0 {
			return test, errors.NewProcessingError("bad amount %v", amount)
		}

		test.Amount = uint64(math.Round(btc * 1e8))
		rawTest = rawTest[1:]
	}

	if len(rawTest) < 5 || len(rawTest) > 6 {
		return test, errors.NewProcessingError("want [tx version, scriptSig, scriptPubKey, flags, expected, comment?], got %d elements", len(rawTest))
	}

	fields := make([]string, len(rawTest))

	for j, f := range rawTest {
		s, ok := f.(string)
		if !ok {
			return test, errors.NewProcessingError("element %d is not a string", j)
		}

		fields[j] = s
	}

	version, err := strconv.ParseInt(fields[0], 10, 32)
	if err != nil {
		return test, errors.NewProcessingError("bad tx version %q", fields[0], err)
	}

	test.TxVersion = int32(version)
	test.ScriptSig, test.ScriptPubKey, test.Expected = fields[1], fields[2], fields[4]
	test.Flags = strings.Split(fields[3], ",")

	if len(fields) == 6 {
		test.Comment = fields[5]
	}

	return test, nil
}

// LoadTxTests loads tx_valid.json or tx_invalid.json in bitcoin-sv's format: an entry is
// either a comment, [ "text" ], or [[[prevout hash, prevout index, prevout scriptPubKey,
// optional amount], ...], serialized transaction, flags], where flags is one
// comma-separated flag set or an array of them. Comment entries are attached to the test
// that follows them. A malformed test entry fails the test, as it does in bitcoin-sv.
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

	var (
		tests    = make([]TxTest, 0, len(rawTests))
		comments []string
	)

	for i, rawTest := range rawTests {
		if _, isInputs := rawTest[0].([]interface{}); !isInputs {
			for _, c := range rawTest {
				if text, ok := c.(string); ok {
					comments = append(comments, text)
				}
			}

			continue
		}

		test, err := parseTxTest(rawTest)
		if err != nil {
			t.Fatalf("Bad test at index %d of %s: %v", i, filename, err)
		}

		test.Index = i
		test.Comment = strings.Join(comments, " ")
		comments = nil

		tests = append(tests, test)
	}

	return tests
}

func parseTxTest(rawTest []interface{}) (TxTest, error) {
	test := TxTest{Raw: rawTest}

	if len(rawTest) != 3 {
		return test, errors.NewProcessingError("want 3 elements, got %d", len(rawTest))
	}

	for j, rawInput := range rawTest[0].([]interface{}) {
		input, err := parseTxTestInput(rawInput)
		if err != nil {
			return test, errors.NewProcessingError("input %d", j, err)
		}

		test.Inputs = append(test.Inputs, input)
	}

	rawTx, ok := rawTest[1].(string)
	if !ok {
		return test, errors.NewProcessingError("second element must be the transaction hex")
	}

	test.RawTx = rawTx

	switch flags := rawTest[2].(type) {
	case string:
		test.FlagSets = []string{flags}
	case []interface{}:
		for _, f := range flags {
			set, ok := f.(string)
			if !ok {
				return test, errors.NewProcessingError("flag sets must be strings")
			}

			test.FlagSets = append(test.FlagSets, set)
		}
	}

	if len(test.FlagSets) == 0 {
		return test, errors.NewProcessingError("third element must be a flag set or a non-empty array of them")
	}

	return test, nil
}

func parseTxTestInput(rawInput interface{}) (TxTestInput, error) {
	var input TxTestInput

	fields, ok := rawInput.([]interface{})
	if !ok || len(fields) < 3 || len(fields) > 4 {
		return input, errors.NewProcessingError("want [hash, index, scriptPubKey] or [hash, index, scriptPubKey, amount]")
	}

	txid, ok1 := fields[0].(string)
	vout, ok2 := fields[1].(float64)
	script, ok3 := fields[2].(string)

	if !ok1 || !ok2 || !ok3 {
		return input, errors.NewProcessingError("bad hash, index or scriptPubKey type")
	}

	// bitcoin-sv reads the index with get_int(), so -1 is the null prevout's 0xffffffff.
	input.TxID, input.Vout, input.ScriptPubKey = txid, uint32(int64(vout)), script

	if len(fields) == 4 {
		amount, ok := fields[3].(float64)
		if !ok {
			return input, errors.NewProcessingError("bad amount type")
		}

		input.Amount = int64(amount)
	}

	return input, nil
}

// LoadSigHashTests loads sighash.json. A one-element entry is a comment; any other entry
// must have six elements, or the test fails, as in bitcoin-sv.
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
		if len(rawTest) == 1 {
			continue
		}

		test, ok := parseSigHashTest(rawTest)
		if !ok {
			t.Fatalf("Bad test at index %d of %s: want [tx, script, input index, hash type, hash, original hash]", i, filename)
		}

		test.Index = i
		tests = append(tests, test)
	}

	return tests
}

func parseSigHashTest(rawTest []interface{}) (SigHashTest, bool) {
	if len(rawTest) != 6 {
		return SigHashTest{}, false
	}

	rawTx, ok1 := rawTest[0].(string)
	script, ok2 := rawTest[1].(string)
	inputIndex, ok3 := rawTest[2].(float64)
	hashType, ok4 := rawTest[3].(float64)
	regular, ok5 := rawTest[4].(string)
	original, ok6 := rawTest[5].(string)

	return SigHashTest{
		RawTx:        rawTx,
		Script:       script,
		InputIndex:   int(inputIndex),
		HashType:     int64(hashType),
		RegularHash:  regular,
		OriginalHash: original,
		Raw:          rawTest,
	}, ok1 && ok2 && ok3 && ok4 && ok5 && ok6
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
