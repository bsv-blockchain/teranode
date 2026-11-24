package util

import (
	"net/url"
	"testing"
	"time"

	"github.com/bsv-blockchain/aerospike-client-go/v8"
	"github.com/bsv-blockchain/teranode/pkg/urlutil"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/uaerospike"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGetQueryBool(t *testing.T) {
	logger := ulogger.New("test")

	tests := []struct {
		name         string
		urlString    string
		key          string
		defaultValue bool
		expected     bool
		expectError  bool
	}{
		{
			name:         "missing query parameter returns default",
			urlString:    "aerospike://localhost:3000/namespace",
			key:          "missing",
			defaultValue: true,
			expected:     true,
			expectError:  false,
		},
		{
			name:         "valid true value",
			urlString:    "aerospike://localhost:3000/namespace?flag=true",
			key:          "flag",
			defaultValue: false,
			expected:     true,
			expectError:  false,
		},
		{
			name:         "valid false value",
			urlString:    "aerospike://localhost:3000/namespace?flag=false",
			key:          "flag",
			defaultValue: true,
			expected:     false,
			expectError:  false,
		},
		{
			name:         "valid 1 value",
			urlString:    "aerospike://localhost:3000/namespace?flag=1",
			key:          "flag",
			defaultValue: false,
			expected:     true,
			expectError:  false,
		},
		{
			name:         "valid 0 value",
			urlString:    "aerospike://localhost:3000/namespace?flag=0",
			key:          "flag",
			defaultValue: true,
			expected:     false,
			expectError:  false,
		},
		{
			name:         "invalid value returns error",
			urlString:    "aerospike://localhost:3000/namespace?flag=invalid",
			key:          "flag",
			defaultValue: false,
			expected:     false,
			expectError:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			u, err := url.Parse(tt.urlString)
			require.NoError(t, err)

			result, err := getQueryBool(u, tt.key, tt.defaultValue, logger)

			if tt.expectError {
				assert.Error(t, err)
				assert.Equal(t, tt.defaultValue, result)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.expected, result)
			}
		})
	}
}

// TestWritePoliciesPinCommitAll guards teranode's own durability contract: every
// consumer of these two builders (UTXO record creation, the tx-creation lock, the
// conflict WAL, setMined, unspend, preserve/DAH writes) is a write whose loss is
// not self-healing, so the commit level is deliberately NOT configurable here.
// The pruner relaxes its own removals instead — see
// stores/utxo/aerospike/pruner/prune_policies.go.
func TestWritePoliciesPinCommitAll(t *testing.T) {
	tSettings := &settings.Settings{
		Aerospike: settings.AerospikeSettings{UseDefaultPolicies: false},
	}

	require.Equal(t, aerospike.COMMIT_ALL, GetAerospikeWritePolicy(tSettings, 0).CommitLevel)
	require.Equal(t, aerospike.COMMIT_ALL, GetAerospikeBatchWritePolicy(tSettings).CommitLevel)
}

func TestGetQueryInt(t *testing.T) {
	logger := ulogger.New("test")

	tests := []struct {
		name         string
		urlString    string
		key          string
		defaultValue int
		expected     int
		expectError  bool
	}{
		{
			name:         "missing query parameter returns default",
			urlString:    "aerospike://localhost:3000/namespace",
			key:          "missing",
			defaultValue: 42,
			expected:     42,
			expectError:  false,
		},
		{
			name:         "valid positive integer",
			urlString:    "aerospike://localhost:3000/namespace?value=123",
			key:          "value",
			defaultValue: 0,
			expected:     123,
			expectError:  false,
		},
		{
			name:         "valid negative integer",
			urlString:    "aerospike://localhost:3000/namespace?value=-456",
			key:          "value",
			defaultValue: 0,
			expected:     -456,
			expectError:  false,
		},
		{
			name:         "zero value",
			urlString:    "aerospike://localhost:3000/namespace?value=0",
			key:          "value",
			defaultValue: 100,
			expected:     0,
			expectError:  false,
		},
		{
			name:         "invalid value returns error",
			urlString:    "aerospike://localhost:3000/namespace?value=invalid",
			key:          "value",
			defaultValue: 42,
			expected:     42,
			expectError:  true,
		},
		{
			name:         "float value returns error",
			urlString:    "aerospike://localhost:3000/namespace?value=3.14",
			key:          "value",
			defaultValue: 42,
			expected:     42,
			expectError:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			u, err := url.Parse(tt.urlString)
			require.NoError(t, err)

			result, err := getQueryInt(u, tt.key, tt.defaultValue, logger)

			if tt.expectError {
				assert.Error(t, err)
				assert.Equal(t, tt.defaultValue, result)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.expected, result)
			}
		})
	}
}

func TestGetQueryDuration(t *testing.T) {
	logger := ulogger.New("test")

	tests := []struct {
		name         string
		urlString    string
		key          string
		defaultValue time.Duration
		expected     time.Duration
		expectError  bool
	}{
		{
			name:         "missing query parameter returns default",
			urlString:    "aerospike://localhost:3000/namespace",
			key:          "missing",
			defaultValue: 5 * time.Second,
			expected:     5 * time.Second,
			expectError:  false,
		},
		{
			name:         "valid seconds duration",
			urlString:    "aerospike://localhost:3000/namespace?timeout=10s",
			key:          "timeout",
			defaultValue: time.Second,
			expected:     10 * time.Second,
			expectError:  false,
		},
		{
			name:         "valid milliseconds duration",
			urlString:    "aerospike://localhost:3000/namespace?timeout=500ms",
			key:          "timeout",
			defaultValue: time.Second,
			expected:     500 * time.Millisecond,
			expectError:  false,
		},
		{
			name:         "valid minutes duration",
			urlString:    "aerospike://localhost:3000/namespace?timeout=2m",
			key:          "timeout",
			defaultValue: time.Second,
			expected:     2 * time.Minute,
			expectError:  false,
		},
		{
			name:         "valid nanoseconds duration",
			urlString:    "aerospike://localhost:3000/namespace?timeout=1000ns",
			key:          "timeout",
			defaultValue: time.Second,
			expected:     1000 * time.Nanosecond,
			expectError:  false,
		},
		{
			name:         "invalid duration format",
			urlString:    "aerospike://localhost:3000/namespace?timeout=invalid",
			key:          "timeout",
			defaultValue: 5 * time.Second,
			expected:     5 * time.Second,
			expectError:  true,
		},
		{
			name:         "number without unit returns error",
			urlString:    "aerospike://localhost:3000/namespace?timeout=123",
			key:          "timeout",
			defaultValue: 5 * time.Second,
			expected:     5 * time.Second,
			expectError:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			u, err := url.Parse(tt.urlString)
			require.NoError(t, err)

			result, err := getQueryDuration(u, tt.key, tt.defaultValue, logger)

			if tt.expectError {
				assert.Error(t, err)
				assert.Equal(t, tt.defaultValue, result)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.expected, result)
			}
		})
	}
}

func TestGetQueryFloat64(t *testing.T) {
	logger := ulogger.New("test")

	tests := []struct {
		name         string
		urlString    string
		key          string
		defaultValue float64
		expected     float64
		expectError  bool
	}{
		{
			name:         "missing query parameter returns default",
			urlString:    "aerospike://localhost:3000/namespace",
			key:          "missing",
			defaultValue: 3.14,
			expected:     3.14,
			expectError:  false,
		},
		{
			name:         "valid positive float",
			urlString:    "aerospike://localhost:3000/namespace?multiplier=2.5",
			key:          "multiplier",
			defaultValue: 1.0,
			expected:     2.5,
			expectError:  false,
		},
		{
			name:         "valid negative float",
			urlString:    "aerospike://localhost:3000/namespace?multiplier=-1.5",
			key:          "multiplier",
			defaultValue: 1.0,
			expected:     -1.5,
			expectError:  false,
		},
		{
			name:         "valid integer as float",
			urlString:    "aerospike://localhost:3000/namespace?multiplier=5",
			key:          "multiplier",
			defaultValue: 1.0,
			expected:     5.0,
			expectError:  false,
		},
		{
			name:         "zero value",
			urlString:    "aerospike://localhost:3000/namespace?multiplier=0.0",
			key:          "multiplier",
			defaultValue: 1.0,
			expected:     0.0,
			expectError:  false,
		},
		{
			name:         "scientific notation",
			urlString:    "aerospike://localhost:3000/namespace?multiplier=1.23e-4",
			key:          "multiplier",
			defaultValue: 1.0,
			expected:     1.23e-4,
			expectError:  false,
		},
		{
			name:         "invalid value returns error",
			urlString:    "aerospike://localhost:3000/namespace?multiplier=invalid",
			key:          "multiplier",
			defaultValue: 3.14,
			expected:     3.14,
			expectError:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			u, err := url.Parse(tt.urlString)
			require.NoError(t, err)

			result, err := getQueryFloat64(u, tt.key, tt.defaultValue, logger)

			if tt.expectError {
				assert.Error(t, err)
				assert.Equal(t, tt.defaultValue, result)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.expected, result)
			}
		})
	}
}

func TestGetAerospikeClient_CachedConnections(t *testing.T) {
	logger := ulogger.New("test")

	// Clear existing connections for clean test
	aerospikeConnectionMutex.Lock()
	aerospikeConnections = make(map[string]*uaerospike.Client)
	aerospikeConnectionMutex.Unlock()

	// Mock settings for testing
	testSettings := &settings.Settings{
		Aerospike: settings.AerospikeSettings{
			UseDefaultBasePolicies: false,
		},
	}

	u, err := url.Parse("aerospike://localhost:3000/test?Timeout=100ms&LoginTimeout=100ms")
	require.NoError(t, err)

	// First call should create a new connection
	client1, err := GetAerospikeClient(logger, u, testSettings)
	// If Aerospike is running locally, this will succeed
	// If not, we'll get an error but that's OK for this test
	if err == nil {
		assert.NotNil(t, client1)

		// Verify that connection was cached
		aerospikeConnectionMutex.Lock()
		_, found := aerospikeConnections[u.Host]
		aerospikeConnectionMutex.Unlock()
		assert.True(t, found)

		// Second call should reuse the cached connection
		client2, err := GetAerospikeClient(logger, u, testSettings)
		assert.NoError(t, err)
		assert.NotNil(t, client2)
		assert.Equal(t, client1, client2) // Should be the same instance
	} else {
		// If Aerospike is not running, verify that connection was not cached
		aerospikeConnectionMutex.Lock()
		_, found := aerospikeConnections[u.Host]
		aerospikeConnectionMutex.Unlock()
		assert.False(t, found)
	}
}

func TestGetAerospikeClient_InvalidNamespace(t *testing.T) {
	logger := ulogger.New("test")

	// Clear existing connections for clean test
	aerospikeConnectionMutex.Lock()
	aerospikeConnections = make(map[string]*uaerospike.Client)
	aerospikeConnectionMutex.Unlock()

	testSettings := &settings.Settings{
		Aerospike: settings.AerospikeSettings{
			UseDefaultBasePolicies: false,
		},
	}

	// Test with missing namespace (no path)
	u, err := url.Parse("aerospike://localhost:3000?Timeout=100ms&LoginTimeout=100ms")
	require.NoError(t, err)

	_, err = GetAerospikeClient(logger, u, testSettings)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "aerospike namespace not found")
}

func TestGetAerospikeClient_AuthenticationSetup(t *testing.T) {
	logger := ulogger.New("test")

	// Clear existing connections for clean test
	aerospikeConnectionMutex.Lock()
	aerospikeConnections = make(map[string]*uaerospike.Client)
	aerospikeConnectionMutex.Unlock()

	testSettings := &settings.Settings{
		Aerospike: settings.AerospikeSettings{
			UseDefaultBasePolicies: false,
		},
	}

	// Test with authentication credentials
	u, err := url.Parse("aerospike://user:password@localhost:1111/test?Timeout=100ms&LoginTimeout=100ms")
	require.NoError(t, err)

	// This will fail on actual connection but we can test the auth setup
	_, err = GetAerospikeClient(logger, u, testSettings)
	assert.Error(t, err) // Expected connection failure
}

func TestGetAerospikeClient_MultipleHosts(t *testing.T) {
	logger := ulogger.New("test")

	// Clear existing connections for clean test
	aerospikeConnectionMutex.Lock()
	aerospikeConnections = make(map[string]*uaerospike.Client)
	aerospikeConnectionMutex.Unlock()

	testSettings := &settings.Settings{
		Aerospike: settings.AerospikeSettings{
			UseDefaultBasePolicies: false,
		},
	}

	tests := []struct {
		name        string
		urlString   string
		expectError bool
	}{
		{
			name:        "single host with port",
			urlString:   "aerospike://localhost:9999/namespace?Timeout=100ms&LoginTimeout=100ms",
			expectError: true, // Connection will fail but host parsing should work
		},
		// {
		// 	name:        "single host without port",
		// 	urlString:   "aerospike://localhost/namespace",
		// 	expectError: true, // Connection will fail but should default to port 3000
		// },
		{
			name:        "multiple hosts with ports",
			urlString:   "aerospike://host1:3000,host2:3001/namespace?Timeout=100ms&LoginTimeout=100ms",
			expectError: true, // Connection will fail but host parsing should work
		},
		{
			name:        "invalid port - connection will fail",
			urlString:   "aerospike://localhost:99999/namespace?Timeout=100ms&LoginTimeout=100ms",
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Clear connections before each test
			aerospikeConnectionMutex.Lock()
			aerospikeConnections = make(map[string]*uaerospike.Client)
			aerospikeConnectionMutex.Unlock()

			u, err := urlutil.ParseMultiHostURL(tt.urlString)
			require.NoError(t, err)

			_, err = GetAerospikeClient(logger, u, testSettings)
			if tt.expectError {
				assert.Error(t, err)
			}
		})
	}
}

// TestAerospikePolicyReadsDoNotRaceClientCreation pins that creating a client, which parses and
// publishes the policy values, can run while stores read policies: tests sharing one Aerospike
// server create clients while other tests' stores read. Run with -race.
func TestAerospikePolicyReadsDoNotRaceClientCreation(t *testing.T) {
	tSettings := settings.NewSettings()
	logger := ulogger.New("test")

	done := make(chan struct{})

	go func() {
		defer close(done)

		for range 200 {
			values, err := loadAerospikePolicyValues(logger, tSettings)
			if err != nil {
				t.Error(err)
				return
			}

			aerospikePolicies.Store(values)
		}
	}()

	for range 200 {
		_ = GetAerospikeReadPolicy(tSettings)
		_ = GetAerospikeWritePolicy(tSettings, 0)
		_ = GetAerospikeBatchPolicy(tSettings)
		_ = GetAerospikeQueryPolicy(tSettings)
	}

	<-done

	values, err := loadAerospikePolicyValues(logger, tSettings)
	require.NoError(t, err)
	require.Equal(t, values.readTimeout, GetAerospikeReadPolicy(tSettings).TotalTimeout)
}

// usePublishedPolicies publishes values for the rest of the test and restores what was published
// before.
func usePublishedPolicies(t *testing.T, values *aerospikePolicyValues) {
	t.Helper()

	previous := aerospikePolicies.Load()
	t.Cleanup(func() { aerospikePolicies.Store(previous) })
	aerospikePolicies.Store(values)
}

func mustParsePolicyURL(t *testing.T, query string) *url.URL {
	t.Helper()

	u, err := url.Parse("aerospike:///?" + query)
	require.NoError(t, err)

	return u
}

// TestLoadAerospikePolicyValuesReachTheGetters pins that every policy setting is parsed and that
// the Get*Policy functions hand out what was parsed.
func TestLoadAerospikePolicyValuesReachTheGetters(t *testing.T) {
	tSettings := settings.NewSettings()
	tSettings.Aerospike.UseDefaultPolicies = false
	tSettings.Aerospike.ReadPolicyURL = mustParsePolicyURL(t, "MaxRetries=7&TotalTimeout=7s&SocketTimeout=6s&SleepBetweenRetries=5ms&SleepMultiplier=1.5&ExitFastOnExhaustedConnectionPool=true")
	tSettings.Aerospike.WritePolicyURL = mustParsePolicyURL(t, "MaxRetries=8&TotalTimeout=8s&SocketTimeout=4s&SleepBetweenRetries=6ms&SleepMultiplier=2.5&ExitFastOnExhaustedConnectionPool=true")
	tSettings.Aerospike.BatchPolicyURL = mustParsePolicyURL(t, "MaxRetries=9&TotalTimeout=9s&SocketTimeout=3s&SleepBetweenRetries=7ms&SleepMultiplier=3.5&AllowInlineSSD=true&ConcurrentNodes=4")
	tSettings.Aerospike.QueryPolicyURL = mustParsePolicyURL(t, "MaxRetries=10&TotalTimeout=10s&SocketTimeout=2s&SleepBetweenRetries=8ms&SleepMultiplier=4.5")

	values, err := loadAerospikePolicyValues(ulogger.New("test"), tSettings)
	require.NoError(t, err)
	usePublishedPolicies(t, values)

	read := GetAerospikeReadPolicy(tSettings)
	require.Equal(t, 7, read.MaxRetries)
	require.Equal(t, 7*time.Second, read.TotalTimeout)
	require.Equal(t, 6*time.Second, read.SocketTimeout)
	require.Equal(t, 5*time.Millisecond, read.SleepBetweenRetries)
	require.InDelta(t, 1.5, read.SleepMultiplier, 0)
	require.True(t, read.ExitFastOnExhaustedConnectionPool)

	write := GetAerospikeWritePolicy(tSettings, 0)
	require.Equal(t, 8, write.MaxRetries)
	require.Equal(t, 8*time.Second, write.TotalTimeout)
	require.Equal(t, 4*time.Second, write.SocketTimeout)
	require.Equal(t, 6*time.Millisecond, write.SleepBetweenRetries)
	require.InDelta(t, 2.5, write.SleepMultiplier, 0)
	require.True(t, write.ExitFastOnExhaustedConnectionPool)

	batch := GetAerospikeBatchPolicy(tSettings)
	require.Equal(t, 9, batch.MaxRetries)
	require.Equal(t, 9*time.Second, batch.TotalTimeout)
	require.Equal(t, 3*time.Second, batch.SocketTimeout)
	require.Equal(t, 7*time.Millisecond, batch.SleepBetweenRetries)
	require.InDelta(t, 3.5, batch.SleepMultiplier, 0)
	require.True(t, batch.AllowInlineSSD)
	require.Equal(t, 4, batch.ConcurrentNodes)

	query := GetAerospikeQueryPolicy(tSettings)
	require.Equal(t, 10, query.MaxRetries)
	require.Equal(t, 10*time.Second, query.TotalTimeout)
	require.Equal(t, 2*time.Second, query.SocketTimeout)
	require.Equal(t, 8*time.Millisecond, query.SleepBetweenRetries)
	require.InDelta(t, 4.5, query.SleepMultiplier, 0)
}

// TestLoadAerospikePolicyValuesDefaults pins the aerospike client's own defaults for a setting
// the policy URL leaves out, and for a query policy that is not configured at all.
func TestLoadAerospikePolicyValuesDefaults(t *testing.T) {
	tSettings := settings.NewSettings()
	tSettings.Aerospike.ReadPolicyURL = mustParsePolicyURL(t, "")
	tSettings.Aerospike.WritePolicyURL = mustParsePolicyURL(t, "")
	tSettings.Aerospike.BatchPolicyURL = mustParsePolicyURL(t, "")
	tSettings.Aerospike.QueryPolicyURL = nil

	values, err := loadAerospikePolicyValues(ulogger.New("test"), tSettings)
	require.NoError(t, err)

	require.Equal(t, aerospike.NewPolicy().MaxRetries, values.readMaxRetries)
	require.Equal(t, aerospike.NewWritePolicy(0, 0).TotalTimeout, values.writeTimeout)
	require.Equal(t, aerospike.NewBatchPolicy().SocketTimeout, values.batchSocketTimeout)
	require.Equal(t, aerospike.NewQueryPolicy().MaxRetries, values.queryMaxRetries)
	require.Equal(t, aerospike.NewQueryPolicy().TotalTimeout, values.queryTotalTimeout)
}

// TestLoadAerospikePolicyValuesRefusesABadValue pins that a value that does not parse fails the
// load rather than falling back to a default.
func TestLoadAerospikePolicyValuesRefusesABadValue(t *testing.T) {
	for _, tc := range []struct {
		name  string
		apply func(*settings.Settings)
	}{
		{"read MaxRetries", func(s *settings.Settings) { s.Aerospike.ReadPolicyURL = mustParsePolicyURL(t, "MaxRetries=many") }},
		{"batch ConcurrentNodes", func(s *settings.Settings) { s.Aerospike.BatchPolicyURL = mustParsePolicyURL(t, "ConcurrentNodes=many") }},
		{"batch SocketTimeout", func(s *settings.Settings) { s.Aerospike.BatchPolicyURL = mustParsePolicyURL(t, "SocketTimeout=soon") }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tSettings := settings.NewSettings()
			tc.apply(tSettings)

			_, err := loadAerospikePolicyValues(ulogger.New("test"), tSettings)
			require.Error(t, err)
		})
	}
}

// TestGetAerospikeClientPublishesThePolicies pins that creating a client publishes the policy
// values it parsed; here the connection itself then fails, as nothing listens.
func TestGetAerospikeClientPublishesThePolicies(t *testing.T) {
	tSettings := settings.NewSettings()
	tSettings.Aerospike.UseDefaultBasePolicies = false
	tSettings.Aerospike.ReadPolicyURL = mustParsePolicyURL(t, "MaxRetries=11")

	usePublishedPolicies(t, &aerospikePolicyValues{})

	storeURL, err := url.Parse("aerospike://127.0.0.1:1/test?Timeout=200ms")
	require.NoError(t, err)

	_, _ = getAerospikeClient(ulogger.New("test"), storeURL, tSettings)

	require.Equal(t, 11, currentAerospikePolicies().readMaxRetries)
}

func TestGetAerospikeReadPolicy(t *testing.T) {
	tests := []struct {
		name               string
		useDefaultPolicies bool
		options            []AerospikeReadPolicyOptions
		validatePolicy     func(*testing.T, *aerospike.BasePolicy)
	}{
		{
			name:               "default policies enabled",
			useDefaultPolicies: true,
			options:            nil,
			validatePolicy: func(t *testing.T, policy *aerospike.BasePolicy) {
				// Should use aerospike defaults
				defaultPolicy := aerospike.NewPolicy()
				assert.Equal(t, defaultPolicy.MaxRetries, policy.MaxRetries)
				assert.Equal(t, defaultPolicy.TotalTimeout, policy.TotalTimeout)
			},
		},
		{
			name:               "custom policies without options",
			useDefaultPolicies: false,
			options:            nil,
			validatePolicy: func(t *testing.T, policy *aerospike.BasePolicy) {
				// Should use configured global values
				assert.Equal(t, currentAerospikePolicies().readMaxRetries, policy.MaxRetries)
				assert.Equal(t, currentAerospikePolicies().readTimeout, policy.TotalTimeout)
				assert.Equal(t, currentAerospikePolicies().readSocketTimeout, policy.SocketTimeout)
			},
		},
		{
			name:               "custom policies with options",
			useDefaultPolicies: false,
			options: []AerospikeReadPolicyOptions{
				WithTotalTimeout(10 * time.Second),
				WithSocketTimeout(5 * time.Second),
				WithMaxRetries(3),
			},
			validatePolicy: func(t *testing.T, policy *aerospike.BasePolicy) {
				assert.Equal(t, 10*time.Second, policy.TotalTimeout)
				assert.Equal(t, 5*time.Second, policy.SocketTimeout)
				assert.Equal(t, 3, policy.MaxRetries)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			settings := &settings.Settings{
				Aerospike: settings.AerospikeSettings{
					UseDefaultPolicies: tt.useDefaultPolicies,
				},
			}

			policy := GetAerospikeReadPolicy(settings, tt.options...)
			require.NotNil(t, policy)
			tt.validatePolicy(t, policy)
		})
	}
}

func TestGetAerospikeWritePolicy(t *testing.T) {
	tests := []struct {
		name               string
		useDefaultPolicies bool
		generation         uint32
		options            []AerospikeWritePolicyOptions
		validatePolicy     func(*testing.T, *aerospike.WritePolicy)
	}{
		{
			name:               "default policies enabled",
			useDefaultPolicies: true,
			generation:         5,
			options:            nil,
			validatePolicy: func(t *testing.T, policy *aerospike.WritePolicy) {
				assert.Equal(t, uint32(5), policy.Generation)
				assert.Equal(t, uint32(aerospike.TTLDontExpire), policy.Expiration)
			},
		},
		{
			name:               "custom policies without options",
			useDefaultPolicies: false,
			generation:         10,
			options:            nil,
			validatePolicy: func(t *testing.T, policy *aerospike.WritePolicy) {
				assert.Equal(t, uint32(10), policy.Generation)
				assert.Equal(t, currentAerospikePolicies().writeMaxRetries, policy.MaxRetries)
				assert.Equal(t, currentAerospikePolicies().writeTimeout, policy.TotalTimeout)
				assert.Equal(t, aerospike.COMMIT_ALL, policy.CommitLevel)
			},
		},
		{
			name:               "custom policies with options",
			useDefaultPolicies: false,
			generation:         15,
			options: []AerospikeWritePolicyOptions{
				WithTotalTimeoutWrite(20 * time.Second),
				WithSocketTimeoutWrite(10 * time.Second),
				WithMaxRetriesWrite(5),
			},
			validatePolicy: func(t *testing.T, policy *aerospike.WritePolicy) {
				assert.Equal(t, 20*time.Second, policy.TotalTimeout)
				assert.Equal(t, 10*time.Second, policy.SocketTimeout)
				assert.Equal(t, 5, policy.MaxRetries)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			settings := &settings.Settings{
				Aerospike: settings.AerospikeSettings{
					UseDefaultPolicies: tt.useDefaultPolicies,
				},
			}

			policy := GetAerospikeWritePolicy(settings, tt.generation, tt.options...)
			require.NotNil(t, policy)
			tt.validatePolicy(t, policy)
		})
	}
}

func TestGetAerospikeBatchPolicy(t *testing.T) {
	tests := []struct {
		name               string
		useDefaultPolicies bool
		validatePolicy     func(*testing.T, *aerospike.BatchPolicy)
	}{
		{
			name:               "default policies enabled",
			useDefaultPolicies: true,
			validatePolicy: func(t *testing.T, policy *aerospike.BatchPolicy) {
				defaultPolicy := aerospike.NewBatchPolicy()
				assert.Equal(t, defaultPolicy.TotalTimeout, policy.TotalTimeout)
				assert.Equal(t, defaultPolicy.MaxRetries, policy.MaxRetries)
			},
		},
		{
			name:               "custom policies",
			useDefaultPolicies: false,
			validatePolicy: func(t *testing.T, policy *aerospike.BatchPolicy) {
				assert.Equal(t, currentAerospikePolicies().batchTotalTimeout, policy.TotalTimeout)
				assert.Equal(t, currentAerospikePolicies().batchSocketTimeout, policy.SocketTimeout)
				assert.Equal(t, currentAerospikePolicies().batchAllowInlineSSD, policy.AllowInlineSSD)
				assert.Equal(t, currentAerospikePolicies().concurrentNodes, policy.ConcurrentNodes)
				assert.Equal(t, currentAerospikePolicies().batchMaxRetries, policy.MaxRetries)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			settings := &settings.Settings{
				Aerospike: settings.AerospikeSettings{
					UseDefaultPolicies: tt.useDefaultPolicies,
				},
			}

			policy := GetAerospikeBatchPolicy(settings)
			require.NotNil(t, policy)
			tt.validatePolicy(t, policy)
		})
	}
}

func TestGetAerospikeBatchWritePolicy(t *testing.T) {
	tests := []struct {
		name               string
		useDefaultPolicies bool
		validatePolicy     func(*testing.T, *aerospike.BatchWritePolicy)
	}{
		{
			name:               "default policies enabled",
			useDefaultPolicies: true,
			validatePolicy: func(t *testing.T, policy *aerospike.BatchWritePolicy) {
				defaultPolicy := aerospike.NewBatchWritePolicy()
				assert.Equal(t, defaultPolicy.CommitLevel, policy.CommitLevel)
			},
		},
		{
			name:               "custom policies",
			useDefaultPolicies: false,
			validatePolicy: func(t *testing.T, policy *aerospike.BatchWritePolicy) {
				assert.Equal(t, aerospike.COMMIT_ALL, policy.CommitLevel)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			settings := &settings.Settings{
				Aerospike: settings.AerospikeSettings{
					UseDefaultPolicies: tt.useDefaultPolicies,
				},
			}

			policy := GetAerospikeBatchWritePolicy(settings)
			require.NotNil(t, policy)
			tt.validatePolicy(t, policy)
		})
	}
}

func TestGetAerospikeBatchReadPolicy(t *testing.T) {
	tests := []struct {
		name               string
		useDefaultPolicies bool
	}{
		{
			name:               "default policies enabled",
			useDefaultPolicies: true,
		},
		{
			name:               "custom policies",
			useDefaultPolicies: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			settings := &settings.Settings{
				Aerospike: settings.AerospikeSettings{
					UseDefaultPolicies: tt.useDefaultPolicies,
				},
			}

			policy := GetAerospikeBatchReadPolicy(settings)
			require.NotNil(t, policy)

			// BatchReadPolicy doesn't have custom configurations in the current implementation
			defaultPolicy := aerospike.NewBatchReadPolicy()
			assert.Equal(t, defaultPolicy.FilterExpression, policy.FilterExpression)
		})
	}
}

func TestPolicyOptions(t *testing.T) {
	t.Run("WithTotalTimeout", func(t *testing.T) {
		policy := aerospike.NewPolicy()
		option := WithTotalTimeout(30 * time.Second)
		option(policy)
		assert.Equal(t, 30*time.Second, policy.TotalTimeout)
	})

	t.Run("WithSocketTimeout", func(t *testing.T) {
		policy := aerospike.NewPolicy()
		option := WithSocketTimeout(15 * time.Second)
		option(policy)
		assert.Equal(t, 15*time.Second, policy.SocketTimeout)
	})

	t.Run("WithMaxRetries", func(t *testing.T) {
		policy := aerospike.NewPolicy()
		option := WithMaxRetries(5)
		option(policy)
		assert.Equal(t, 5, policy.MaxRetries)
	})

	t.Run("WithTotalTimeoutWrite", func(t *testing.T) {
		policy := aerospike.NewWritePolicy(0, 0)
		option := WithTotalTimeoutWrite(25 * time.Second)
		option(policy)
		assert.Equal(t, 25*time.Second, policy.TotalTimeout)
	})

	t.Run("WithSocketTimeoutWrite", func(t *testing.T) {
		policy := aerospike.NewWritePolicy(0, 0)
		option := WithSocketTimeoutWrite(12 * time.Second)
		option(policy)
		assert.Equal(t, 12*time.Second, policy.SocketTimeout)
	})

	t.Run("WithMaxRetriesWrite", func(t *testing.T) {
		policy := aerospike.NewWritePolicy(0, 0)
		option := WithMaxRetriesWrite(7)
		option(policy)
		assert.Equal(t, 7, policy.MaxRetries)
	})
}

// Test helper to verify package initialization
func TestPackageInitialization(t *testing.T) {
	// Verify that the package-level variables are initialized
	assert.NotNil(t, aerospikeConnections)
	assert.NotNil(t, &aerospikePrometheusMetrics)
	assert.NotNil(t, &aerospikePrometheusHistograms)
}

// Test coverage for edge cases in host parsing
func TestHostParsing_EdgeCases(t *testing.T) {
	logger := ulogger.New("test")

	// For edge case testing, provide minimal policy URLs
	readPolicy, _ := url.Parse("aerospike://dummy?Timeout=100ms")
	writePolicy, _ := url.Parse("aerospike://dummy?Timeout=100ms")
	batchPolicy, _ := url.Parse("aerospike://dummy?Timeout=100ms")

	testSettings := &settings.Settings{
		Aerospike: settings.AerospikeSettings{
			UseDefaultBasePolicies: false,
			ReadPolicyURL:          readPolicy,
			WritePolicyURL:         writePolicy,
			BatchPolicyURL:         batchPolicy,
		},
	}

	t.Run("empty host should fail", func(t *testing.T) {
		u, err := url.Parse("aerospike:///namespace?Timeout=100ms&LoginTimeout=100ms")
		require.NoError(t, err)

		_, err = GetAerospikeClient(logger, u, testSettings)
		assert.Error(t, err)
	})

	t.Run("invalid port parsing", func(t *testing.T) {
		u, err := url.Parse("aerospike://localhost:99999999999999999999/namespace?Timeout=100ms&LoginTimeout=100ms")
		require.NoError(t, err)

		_, err = GetAerospikeClient(logger, u, testSettings)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "invalid port")
	})
}

// Test buffer size setting behavior
func TestAerospikeBufferSize(t *testing.T) {
	logger := ulogger.New("test")

	// For edge case testing, provide minimal policy URLs
	readPolicy, _ := url.Parse("aerospike://dummy?Timeout=100ms")
	writePolicy, _ := url.Parse("aerospike://dummy?Timeout=100ms")
	batchPolicy, _ := url.Parse("aerospike://dummy?Timeout=100ms")

	testSettings := &settings.Settings{
		Aerospike: settings.AerospikeSettings{
			UseDefaultBasePolicies: false,
			ReadPolicyURL:          readPolicy,
			WritePolicyURL:         writePolicy,
			BatchPolicyURL:         batchPolicy,
		},
	}

	u, err := url.Parse("aerospike://localhost:9999/test?Timeout=100ms&LoginTimeout=100ms")
	require.NoError(t, err)

	// MaxBufferSize is set exactly once in this package's init (not per
	// GetAerospikeClient call, which raced with in-flight batch reads on already
	// running clients). So by the time any test runs it must already be 512MB.
	const expectedBufferSize = 1024 * 1024 * 512 // 512MB
	require.Equal(t, 536870912, expectedBufferSize, "Buffer size constant should be 512MB")
	require.Equal(t, expectedBufferSize, aerospike.MaxBufferSize, "init must set MaxBufferSize to 512MB")

	// A GetAerospikeClient that fails to connect must not touch MaxBufferSize.
	_, err = GetAerospikeClient(logger, u, testSettings)
	require.Error(t, err, "Expected connection error since no Aerospike server running")
	require.Equal(t, expectedBufferSize, aerospike.MaxBufferSize, "Buffer size must stay 512MB after a failed connection")
}

// Test initStats function with minimal testing to improve coverage
func TestInitStatsBasic(t *testing.T) {
	logger := ulogger.New("test")

	// Create a real client but expect it to fail to connect
	// This will still execute the initStats function and improve coverage
	testSettings := &settings.Settings{
		Aerospike: settings.AerospikeSettings{
			StatsRefreshDuration: 10 * time.Millisecond,
		},
	}

	// This test uses a nil client to test error handling paths
	// In practice, initStats would not be called with a nil client,
	// but this allows us to test the function exists and can be called
	t.Run("initStats with nil client", func(t *testing.T) {
		// This will cause initStats to panic or handle nil gracefully
		defer func() {
			if r := recover(); r != nil {
				// initStats panicked, which is expected behavior with nil client
				t.Logf("initStats panicked as expected with nil client: %v", r)
			}
		}()

		// This will test that initStats can be called
		initStats(logger, nil, testSettings)
		time.Sleep(10 * time.Millisecond) // Brief wait
	})
}
