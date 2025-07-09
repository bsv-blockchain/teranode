package settings

import (
	"net/url"
	"testing"
	"time"

	"github.com/ordishs/gocore"
)

func TestGetString(t *testing.T) {
	gocore.Config().Set("test_string", "hello")
	defer gocore.Config().Set("test_string", "")

	result := getString("test_string", "default")
	if result != "hello" {
		t.Errorf("Expected 'hello', got '%s'", result)
	}

	result = getString("missing_key", "default")
	if result != "default" {
		t.Errorf("Expected 'default', got '%s'", result)
	}
}

func TestGetMultiString(t *testing.T) {
	gocore.Config().Set("test_multi_string", "a | b | c")
	defer gocore.Config().Unset("test_multi_string")

	result := getMultiString("test_multi_string", "|", []string{"default"})
	if len(result) != 3 || result[0] != "a" || result[1] != "b" || result[2] != "c" {
		t.Errorf("Expected [a b c], got %v", result)
	}
}

func TestGetInt(t *testing.T) {
	gocore.Config().Set("test_int", "42")
	defer gocore.Config().Unset("test_int")

	result := getInt("test_int", 0)
	if result != 42 {
		t.Errorf("Expected 42, got %d", result)
	}

	result = getInt("missing_key", 0)
	if result != 0 {
		t.Errorf("Expected 0, got %d", result)
	}
}

func TestGetURL(t *testing.T) {
	testURL, _ := url.Parse("https://example.com")
	gocore.Config().Set("test_url", testURL.String())

	defer gocore.Config().Unset("test_url")

	result := getURL("test_url", "")
	if result.String() != testURL.String() {
		t.Errorf("Expected %s, got %s", testURL, result)
	}
}
func TestGetURL_empty(t *testing.T) {
	gocore.Config().Set("test_url", "")

	defer gocore.Config().Unset("test_url")

	result := getURL("test_url", "")
	if result != nil {
		t.Errorf("Expected nil got %s", result)
	}
}

func TestGetUTXOStoreURL(t *testing.T) {
	testURL, _ := url.Parse("sqlite:///utxostore")

	gocore.Config().Set("utxostore", "sqlite:///utxostore")

	defer gocore.Config().Unset("utxostore")

	result := getURL("utxostore", "")
	if result.String() != testURL.String() {
		t.Errorf("Expected %s, got %s", testURL, result)
	}
}

func TestGetBool(t *testing.T) {
	gocore.Config().Set("test_bool", "true")
	defer gocore.Config().Unset("test_bool")

	result := getBool("test_bool", false)
	if !result {
		t.Error("Expected true, got false")
	}

	result = getBool("missing_key", false)
	if result {
		t.Error("Expected false, got true")
	}
}

func TestGetFloat64(t *testing.T) {
	gocore.Config().Set("test_float64", "3.14")
	defer gocore.Config().Unset("test_float64")

	result := getFloat64("test_float64", 0.0)
	if result != 3.14 {
		t.Errorf("Expected 3.14, got %f", result)
	}

	result = getFloat64("missing_key", 0.0)
	if result != 0.0 {
		t.Errorf("Expected 0.0, got %f", result)
	}
}

func TestGetDuration(t *testing.T) {
	gocore.Config().Set("test_duration", "1m30s")
	defer gocore.Config().Unset("test_duration")

	result := getDuration("test_duration", 0)

	expected := 90 * time.Second
	if result != expected {
		t.Errorf("Expected %v, got %v", expected, result)
	}

	result = getDuration("missing_key", 0)
	if result != 0 {
		t.Errorf("Expected 0, got %v", result)
	}
}

func TestGetDuration_invalid(t *testing.T) {
	gocore.Config().Set("test_duration", "5000ms")
	defer gocore.Config().Unset("test_duration")

	result := getDuration("test_duration", 0)

	expected := 5 * time.Second
	if result != expected {
		t.Errorf("Expected %v, got %v", expected, result)
	}

	result = getDuration("missing_key", 0)
	if result != 0 {
		t.Errorf("Expected 0, got %v", result)
	}
}

func TestGetIntSlice(t *testing.T) {
	tests := []struct {
		name         string
		configValue  string
		defaultValue []int
		expected     []int
	}{
		{
			name:         "valid comma-separated values",
			configValue:  "517,417,8080",
			defaultValue: []int{},
			expected:     []int{517, 417, 8080},
		},
		{
			name:         "single value",
			configValue:  "8080",
			defaultValue: []int{},
			expected:     []int{8080},
		},
		{
			name:         "empty config returns default",
			configValue:  "",
			defaultValue: []int{5173, 4173},
			expected:     []int{5173, 4173},
		},
		{
			name:         "invalid values are skipped",
			configValue:  "517,invalid,417",
			defaultValue: []int{},
			expected:     []int{517, 417},
		},
		{
			name:         "all invalid values return default",
			configValue:  "invalid,not-a-number",
			defaultValue: []int{8080},
			expected:     []int{8080},
		},
		{
			name:         "values with spaces",
			configValue:  "5173, 4173, 8080",
			defaultValue: []int{},
			expected:     []int{5173, 4173, 8080},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Set config value if not empty
			if tt.configValue != "" {
				gocore.Config().Set("test_int_slice", tt.configValue)
				defer gocore.Config().Unset("test_int_slice")
			}

			result := getIntSlice("test_int_slice", tt.defaultValue)

			// Check length
			if len(result) != len(tt.expected) {
				t.Errorf("Expected slice length %d, got %d", len(tt.expected), len(result))
				return
			}

			// Check values
			for i, v := range result {
				if v != tt.expected[i] {
					t.Errorf("Expected value %d at index %d, got %d", tt.expected[i], i, v)
				}
			}
		})
	}
}

func TestGetIntSlice_MissingKey(t *testing.T) {
	// Test with missing key returns default
	defaultValue := []int{5173, 4173}
	result := getIntSlice("non_existent_key", defaultValue)

	if len(result) != len(defaultValue) {
		t.Errorf("Expected default slice length %d, got %d", len(defaultValue), len(result))
		return
	}

	for i, v := range result {
		if v != defaultValue[i] {
			t.Errorf("Expected default value %d at index %d, got %d", defaultValue[i], i, v)
		}
	}
}
