package settings

import (
	"reflect"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestAerospikePolicyLongdescsNameOnlyParsedKeys guards the operator text for the
// aerospike_readPolicy, aerospike_writePolicy and aerospike_batchPolicy URLs.
// The longdescs offered ReadModeSC, ReadModeAP, CommitLevel, RecordExistsAction
// and GenerationPolicy as controls and described the value as a JSON file, but
// getAerospikeClient (util/aerospike.go) reads only a fixed set of query keys
// from each URL and CommitLevel is hard-coded to COMMIT_ALL, so an operator who
// followed the text changed nothing.
func TestAerospikePolicyLongdescsNameOnlyParsedKeys(t *testing.T) {
	longdescs := map[string]string{}

	typ := reflect.TypeOf(AerospikeSettings{})
	for i := 0; i < typ.NumField(); i++ {
		field := typ.Field(i)
		longdescs[field.Tag.Get("key")] = field.Tag.Get("longdesc")
	}

	stalePhrases := []string{
		"ReadModeSC",
		"ReadModeAP",
		"JSON configuration",
		"RecordExistsAction",
		"GenerationPolicy",
		"COMMIT_MASTER",
		"MaxConcurrentThreads",
		"SendSetName",
	}

	parsedKeys := map[string][]string{
		"aerospike_readPolicy":  {"MaxRetries", "TotalTimeout", "SocketTimeout", "SleepBetweenRetries", "SleepMultiplier", "ExitFastOnExhaustedConnectionPool"},
		"aerospike_writePolicy": {"MaxRetries", "TotalTimeout", "SocketTimeout", "SleepBetweenRetries", "SleepMultiplier", "ExitFastOnExhaustedConnectionPool"},
		"aerospike_batchPolicy": {"MaxRetries", "TotalTimeout", "SocketTimeout", "SleepBetweenRetries", "SleepMultiplier", "AllowInlineSSD", "ConcurrentNodes"},
	}

	for key, keys := range parsedKeys {
		text, ok := longdescs[key]
		require.True(t, ok, "AerospikeSettings has no field tagged %s", key)

		for _, phrase := range stalePhrases {
			require.False(t, strings.Contains(text, phrase), "the %s longdesc offers a control the parser does not read: %q", key, phrase)
		}

		for _, parsed := range keys {
			require.True(t, strings.Contains(text, parsed), "the %s longdesc must name the parsed query key %s", key, parsed)
		}
	}

	require.Contains(t, longdescs["aerospike_writePolicy"], "COMMIT_ALL", "the write policy longdesc must state that CommitLevel is fixed at COMMIT_ALL")
}
