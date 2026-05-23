package settings

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"

	"github.com/bsv-blockchain/go-sdk/chainhash"
)

// TestSortedJSONMarshaling tests that the marshalSortedJSON function
// properly sorts struct fields alphabetically while leaving arrays unchanged
func TestSortedJSONMarshaling(t *testing.T) {
	// Create a test struct with unsorted fields and arrays
	testStruct := struct {
		ZField       string   `json:"z_field"`
		AField       string   `json:"a_field"`
		MField       string   `json:"m_field"`
		StringArray  []string `json:"string_array"`
		NestedStruct struct {
			YNested string `json:"y_nested"`
			BNested string `json:"b_nested"`
		} `json:"nested_struct"`
	}{
		ZField:      "z_value",
		AField:      "a_value",
		MField:      "m_value",
		StringArray: []string{"zebra", "apple", "banana"},
		NestedStruct: struct {
			YNested string `json:"y_nested"`
			BNested string `json:"b_nested"`
		}{
			YNested: "y_value",
			BNested: "b_value",
		},
	}

	// Marshal with our sorted function
	sortedJSON, err := marshalSortedJSON(testStruct, "  ")
	if err != nil {
		t.Fatalf("Failed to marshal sorted JSON: %v", err)
	}

	// Convert to string for easier testing
	sortedStr := string(sortedJSON)

	// Test that top-level fields are sorted
	aFieldPos := strings.Index(sortedStr, `"a_field"`)
	mFieldPos := strings.Index(sortedStr, `"m_field"`)
	zFieldPos := strings.Index(sortedStr, `"z_field"`)

	if aFieldPos == -1 || mFieldPos == -1 || zFieldPos == -1 {
		t.Fatal("Could not find expected fields in JSON output")
	}

	if !(aFieldPos < mFieldPos && mFieldPos < zFieldPos) {
		t.Error("Top-level fields are not sorted alphabetically")
	}

	// Test that string arrays maintain their original order (not sorted)
	if !strings.Contains(sortedStr, `"zebra"`) || !strings.Contains(sortedStr, `"apple"`) || !strings.Contains(sortedStr, `"banana"`) {
		t.Error("String array elements are missing from output")
	}

	// Verify the array maintains original order: zebra, apple, banana
	zebraPos := strings.Index(sortedStr, `"zebra"`)
	applePos := strings.Index(sortedStr, `"apple"`)
	bananaPos := strings.Index(sortedStr, `"banana"`)

	if !(zebraPos < applePos && applePos < bananaPos) {
		t.Error("String array order was changed - should maintain original order")
	}

	// Test that nested struct fields are sorted
	nestedStart := strings.Index(sortedStr, `"nested_struct"`)
	if nestedStart == -1 {
		t.Fatal("Could not find nested_struct in JSON output")
	}

	nestedSection := sortedStr[nestedStart:]
	bNestedPos := strings.Index(nestedSection, `"b_nested"`)
	yNestedPos := strings.Index(nestedSection, `"y_nested"`)

	if bNestedPos == -1 || yNestedPos == -1 {
		t.Fatal("Could not find nested fields in JSON output")
	}

	if bNestedPos > yNestedPos {
		t.Error("Nested struct fields are not sorted alphabetically")
	}

	t.Logf("Sorted JSON output:\n%s", sortedStr)
}

// TestSortValueWithNilPointer tests that nil pointers are handled correctly
func TestSortValueWithNilPointer(t *testing.T) {
	var nilPtr *string
	result := sortValue(nilPtr)
	if result != nil {
		t.Error("Expected nil pointer to return nil")
	}
}

// TestSortValueWithEmptySlice tests that empty slices are handled correctly
func TestSortValueWithEmptySlice(t *testing.T) {
	emptySlice := []string{}
	result := sortValue(emptySlice)
	if result == nil {
		t.Error("Expected empty slice to return the slice, not nil")
	}
}

// CustomHash represents a hash type that implements json.Marshaler
type CustomHash [32]byte

func (h CustomHash) MarshalJSON() ([]byte, error) {
	// Convert to hex string like real hash types do
	hexStr := ""
	for _, b := range h {
		if b < 16 {
			hexStr += "0"
		}
		hexStr += string("0123456789abcdef"[b>>4]) + string("0123456789abcdef"[b&0xf])
	}
	return json.Marshal(hexStr)
}

// TestCustomJSONMarshaler tests that types implementing json.Marshaler are preserved
func TestCustomJSONMarshaler(t *testing.T) {
	hash := CustomHash{0, 0, 0, 0, 9, 51, 234, 1, 173, 14, 233, 132, 32, 151, 121, 186, 174, 195, 206, 217, 15, 163, 244, 8, 113, 149, 38, 248, 215, 127, 73, 67}

	testStruct := struct {
		GenesisHash CustomHash `json:"GenesisHash"`
		OtherField  string     `json:"OtherField"`
	}{
		GenesisHash: hash,
		OtherField:  "test",
	}

	// Marshal with our sorted function
	sortedJSON, err := marshalSortedJSON(testStruct, "  ")
	if err != nil {
		t.Fatalf("Failed to marshal sorted JSON: %v", err)
	}

	sortedStr := string(sortedJSON)

	// Should contain the hex string representation, not the byte array
	if !strings.Contains(sortedStr, `"GenesisHash": "`) || !strings.Contains(sortedStr, `"00000000`) {
		t.Error("Custom JSON marshaler was not preserved - expected hex string representation")
	}

	// Should not contain byte array representation
	if strings.Contains(sortedStr, `[0,0,0,0,9,51,234,1,`) {
		t.Error("Custom JSON marshaler was not preserved - found byte array representation")
	}

	t.Logf("Custom marshaler test output:\n%s", sortedStr)
}

// TestByteSlicePreservation tests that byte slices are converted to hex strings
func TestByteSlicePreservation(t *testing.T) {
	// Create a byte slice that should be encoded as base64
	pkScript := []byte{65, 4, 103, 138, 253, 176, 254, 85, 72, 39, 25, 103, 241, 166, 113, 48, 183, 16, 92, 214, 168, 40, 224, 57, 9, 166, 121, 98, 224, 234, 31, 97, 222, 182, 73, 246, 188, 63, 76, 239, 56, 196, 243, 85, 4, 229, 30, 193, 18, 222, 92, 56, 77, 247, 186, 11, 141, 87, 138, 76, 112, 43, 107, 241, 29, 95, 172}

	testStruct := struct {
		PkScript   []byte `json:"PkScript"`
		OtherField string `json:"OtherField"`
	}{
		PkScript:   pkScript,
		OtherField: "test",
	}

	// Marshal with our sorted function
	sortedJSON, err := marshalSortedJSON(testStruct, "  ")
	if err != nil {
		t.Fatalf("Failed to marshal sorted JSON: %v", err)
	}

	sortedStr := string(sortedJSON)

	// Should contain the hex string representation, not the byte array
	if !strings.Contains(sortedStr, `"PkScript": "`) {
		t.Error("Byte slice was not converted - missing PkScript field")
	}

	// Should not contain numeric array representation
	if strings.Contains(sortedStr, `[65,4,103,138,`) || strings.Contains(sortedStr, `"PkScript": [`) {
		t.Error("Byte slice was not converted - found numeric array representation")
	}

	// Should contain hex string (check for expected hex output)
	if !strings.Contains(sortedStr, `"4104678afdb0fe5548271967f1a67130`) {
		t.Error("Byte slice was not encoded as hex string")
	}

	t.Logf("Byte slice test output:\n%s", sortedStr)
}

// TestByteArrayPreservation tests that anonymous 32-byte arrays match standard JSON behavior
func TestByteArrayPreservation(t *testing.T) {
	// Create a byte array that should be encoded as hex (like a hash)
	var hash [32]byte // All zeros, like the Hash field showing the issue

	testStruct := struct {
		Hash       [32]byte `json:"Hash"`
		OtherField string   `json:"OtherField"`
	}{
		Hash:       hash,
		OtherField: "test",
	}

	// Marshal with our sorted function
	sortedJSON, err := marshalSortedJSON(testStruct, "  ")
	if err != nil {
		t.Fatalf("Failed to marshal sorted JSON: %v", err)
	}

	sortedStr := string(sortedJSON)

	// Should contain the Hash field
	if !strings.Contains(sortedStr, `"Hash":`) {
		t.Error("Byte array was not preserved - missing Hash field")
	}

	// Compare with standard JSON marshaling
	standardJSON, err := json.MarshalIndent(testStruct, "", "  ")
	if err != nil {
		t.Fatalf("Failed to marshal with standard JSON: %v", err)
	}

	t.Logf("Standard JSON output:\n%s", string(standardJSON))
	t.Logf("Sorted JSON output:\n%s", sortedStr)

	// Anonymous [32]byte arrays should now match standard JSON behavior (numeric arrays)
	// since we removed special handling and rely on json.Marshaler detection
	if sortedStr != string(standardJSON) {
		t.Error("Sorted JSON output doesn't match standard JSON output for anonymous [32]byte")
	}
}

// TestChainHashPreservation tests that chainhash.Hash types are preserved
func TestChainHashPreservation(t *testing.T) {
	// Create a chainhash.Hash
	hash := chainhash.Hash{}

	// Test both value and pointer types
	testStruct := struct {
		ChainHashValue   chainhash.Hash  `json:"ChainHashValue"`
		ChainHashPointer *chainhash.Hash `json:"ChainHashPointer"`
		OtherField       string          `json:"OtherField"`
	}{
		ChainHashValue:   hash,
		ChainHashPointer: &hash,
		OtherField:       "test",
	}

	// Marshal with our sorted function
	sortedJSON, err := marshalSortedJSON(testStruct, "  ")
	if err != nil {
		t.Fatalf("Failed to marshal sorted JSON: %v", err)
	}

	// Marshal with standard JSON
	standardJSON, err := json.MarshalIndent(testStruct, "", "  ")
	if err != nil {
		t.Fatalf("Failed to marshal with standard JSON: %v", err)
	}

	sortedStr := string(sortedJSON)
	standardStr := string(standardJSON)

	t.Logf("Standard JSON output:\n%s", standardStr)
	t.Logf("Sorted JSON output:\n%s", sortedStr)

	// Test if the hash implements json.Marshaler on value or pointer
	if _, ok := interface{}(hash).(json.Marshaler); ok {
		t.Logf("chainhash.Hash implements json.Marshaler on value")
	} else {
		t.Logf("chainhash.Hash does NOT implement json.Marshaler on value")
	}

	if _, ok := interface{}(&hash).(json.Marshaler); ok {
		t.Logf("*chainhash.Hash implements json.Marshaler on pointer")
	} else {
		t.Logf("*chainhash.Hash does NOT implement json.Marshaler on pointer")
	}

	// Test our reflection-based detection
	typ := reflect.TypeOf(hash)
	val := reflect.ValueOf(hash)
	ptrType := reflect.PtrTo(typ)
	if ptrType.Implements(reflect.TypeOf((*json.Marshaler)(nil)).Elem()) {
		t.Logf("Reflection detects that *chainhash.Hash implements json.Marshaler")
	} else {
		t.Logf("Reflection does NOT detect that *chainhash.Hash implements json.Marshaler")
	}

	// Test CanAddr behavior
	if val.CanAddr() {
		t.Logf("chainhash.Hash value CanAddr() = true")
		if _, ok := val.Addr().Interface().(json.Marshaler); ok {
			t.Logf("val.Addr().Interface() implements json.Marshaler")
		} else {
			t.Logf("val.Addr().Interface() does NOT implement json.Marshaler")
		}
	} else {
		t.Logf("chainhash.Hash value CanAddr() = false")
	}

	// Test our pointer creation logic directly
	newVal := reflect.New(typ)
	newVal.Elem().Set(val)
	createdPtr := newVal.Interface()

	createdPtrJSON, err := json.Marshal(createdPtr)
	if err != nil {
		t.Fatalf("Failed to marshal created pointer: %v", err)
	}
	t.Logf("Created pointer marshals to: %s", string(createdPtrJSON))

	// Check that chainhash.Hash values are now converted to hex strings (improvement over standard JSON)
	if !strings.Contains(sortedStr, `"ChainHashValue": "0000000000000000000000000000000000000000000000000000000000000000"`) {
		t.Error("ChainHashValue was not converted to hex string")
	}

	// Check that chainhash.Hash pointers remain as hex strings
	if !strings.Contains(sortedStr, `"ChainHashPointer": "0000000000000000000000000000000000000000000000000000000000000000"`) {
		t.Error("ChainHashPointer was not preserved as hex string")
	}

	// Verify fields are sorted alphabetically
	if !strings.Contains(sortedStr, `"ChainHashPointer": "0000000000000000000000000000000000000000000000000000000000000000",
  "ChainHashValue": "0000000000000000000000000000000000000000000000000000000000000000",
  "OtherField": "test"`) {
		t.Error("Fields are not sorted alphabetically")
	}
}
