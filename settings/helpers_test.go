package settings

import (
	"net/url"
	"testing"
	"time"

	"github.com/ordishs/gocore"
	"github.com/stretchr/testify/require"
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

func TestGetUint64AtLeast(t *testing.T) {
	t.Run("value above the floor is used as configured", func(t *testing.T) {
		gocore.Config().Set("test_at_least", "50")
		defer gocore.Config().Unset("test_at_least")

		require.Equal(t, uint64(50), getUint64AtLeast("test_at_least", 100, 11))
	})

	t.Run("value below the floor is raised to it", func(t *testing.T) {
		gocore.Config().Set("test_at_least", "5")
		defer gocore.Config().Unset("test_at_least")

		require.Equal(t, uint64(11), getUint64AtLeast("test_at_least", 100, 11))
	})

	t.Run("zero is raised to the floor", func(t *testing.T) {
		gocore.Config().Set("test_at_least", "0")
		defer gocore.Config().Unset("test_at_least")

		require.Equal(t, uint64(11), getUint64AtLeast("test_at_least", 100, 11))
	})

	t.Run("missing key falls back to the default", func(t *testing.T) {
		require.Equal(t, uint64(100), getUint64AtLeast("missing_at_least_key", 100, 11))
	})
}

// TestPreviousBlockHeaderCountFloor pins the consensus floor on the header run that
// CheckHeaderContextual evaluates median time past over. svnode's window
// (CBlockIndex::nMedianTimeSpan) is a hard constant; configuring fewer than 11 here would
// silently compute the median over fewer blocks and accept blocks the network rejects.
func TestPreviousBlockHeaderCountFloor(t *testing.T) {
	gocore.Config().Set("blockvalidation_previous_block_header_count", "5")
	defer gocore.Config().Unset("blockvalidation_previous_block_header_count")

	require.GreaterOrEqual(t, NewSettings().BlockValidation.PreviousBlockHeaderCount, uint64(11))
}

// TestGetPostgresPoolSettings_CircuitBreaker pins the per-service override
// path for the five circuit breaker keys (<prefix>_postgres_circuitBreaker*).
// Before this was wired, getPostgresPoolSettings never read these keys, so a
// service-specific override silently fell back to whatever the global
// Postgres settings produced.
func TestGetPostgresPoolSettings_CircuitBreaker(t *testing.T) {
	t.Run("per-service circuit breaker keys populate the returned settings", func(t *testing.T) {
		gocore.Config().Set("myservice_postgres_circuitBreakerEnabled", "true")
		gocore.Config().Set("myservice_postgres_circuitBreakerFailureThreshold", "9")
		gocore.Config().Set("myservice_postgres_circuitBreakerHalfOpenMax", "6")
		gocore.Config().Set("myservice_postgres_circuitBreakerCooldown", "15s")
		gocore.Config().Set("myservice_postgres_circuitBreakerFailureWindow", "5s")

		defer func() {
			gocore.Config().Unset("myservice_postgres_circuitBreakerEnabled")
			gocore.Config().Unset("myservice_postgres_circuitBreakerFailureThreshold")
			gocore.Config().Unset("myservice_postgres_circuitBreakerHalfOpenMax")
			gocore.Config().Unset("myservice_postgres_circuitBreakerCooldown")
			gocore.Config().Unset("myservice_postgres_circuitBreakerFailureWindow")
		}()

		poolSettings := getPostgresPoolSettings("myservice")
		require.NotNil(t, poolSettings)
		require.NotNil(t, poolSettings.CircuitBreakerEnabled)
		require.True(t, *poolSettings.CircuitBreakerEnabled)
		require.Equal(t, 9, poolSettings.CircuitBreakerFailureThreshold)
		require.Equal(t, 6, poolSettings.CircuitBreakerHalfOpenMax)
		require.Equal(t, 15*time.Second, poolSettings.CircuitBreakerCooldown)
		require.Equal(t, 5*time.Second, poolSettings.CircuitBreakerFailureWindow)
	})

	t.Run("a single circuit breaker key alone counts as configured", func(t *testing.T) {
		gocore.Config().Set("otherservice_postgres_circuitBreakerFailureThreshold", "9")
		defer gocore.Config().Unset("otherservice_postgres_circuitBreakerFailureThreshold")

		poolSettings := getPostgresPoolSettings("otherservice")
		require.NotNil(t, poolSettings, "setting only a circuit breaker key must not be treated as unconfigured")
		require.Equal(t, 9, poolSettings.CircuitBreakerFailureThreshold)
	})

	t.Run("nothing configured for the prefix returns nil", func(t *testing.T) {
		require.Nil(t, getPostgresPoolSettings("unconfiguredservice"))
	})

	t.Run("explicit false is distinguishable from absent", func(t *testing.T) {
		gocore.Config().Set("falseservice_postgres_circuitBreakerEnabled", "false")
		defer gocore.Config().Unset("falseservice_postgres_circuitBreakerEnabled")

		poolSettings := getPostgresPoolSettings("falseservice")
		require.NotNil(t, poolSettings, "an explicit false key must count as configured, not as absent")
		require.NotNil(t, poolSettings.CircuitBreakerEnabled)
		require.False(t, *poolSettings.CircuitBreakerEnabled)
	})
}
