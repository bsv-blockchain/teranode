package settings

import (
	"reflect"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestAerospikePolicyLongdescsNameOnlyParsedKeys guards the operator text for the
// aerospike_readPolicy, aerospike_writePolicy, aerospike_batchPolicy and
// aerospike_queryPolicy URLs.
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
		"COMMIT_ALL vs COMMIT_MASTER",
		"MaxConcurrentThreads",
		"SendSetName",
		"MaxConcurrentNodes",
		"RecordQueueSize",
		"IncludeBinData",
		"FailOnClusterChange",
		"Ignored when aerospike_useDefaultBasePolicies is true",
	}

	parsedKeys := map[string][]string{
		"aerospike_readPolicy":  {"MaxRetries", "TotalTimeout", "SocketTimeout", "SleepBetweenRetries", "SleepMultiplier", "ExitFastOnExhaustedConnectionPool"},
		"aerospike_writePolicy": {"MaxRetries", "TotalTimeout", "SocketTimeout", "SleepBetweenRetries", "SleepMultiplier", "ExitFastOnExhaustedConnectionPool"},
		"aerospike_batchPolicy": {"MaxRetries", "TotalTimeout", "SocketTimeout", "SleepBetweenRetries", "SleepMultiplier", "AllowInlineSSD", "ConcurrentNodes"},
		"aerospike_queryPolicy": {"MaxRetries", "TotalTimeout", "SocketTimeout", "SleepBetweenRetries", "SleepMultiplier"},
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
	require.Contains(t, longdescs["aerospike_writePolicy"], "pruner_relaxRemovalCommitLevel", "the write policy longdesc must name the pruner's COMMIT_MASTER exception")

	// aerospike_useDefaultBasePolicies skips URL parsing and leaves the policy package
	// variables at zero, so GetAerospike*Policy applies no timeout and no retries. Every
	// policy longdesc, and the flag's own, must say so rather than call the URL "ignored".
	for key := range parsedKeys {
		require.Contains(t, longdescs[key], "aerospike_useDefaultPolicies", "the %s longdesc must name the flag that does ignore the URL", key)
		require.Contains(t, longdescs[key], "TotalTimeout=0", "the %s longdesc must warn what aerospike_useDefaultBasePolicies does", key)
	}

	require.Contains(t, longdescs["aerospike_useDefaultBasePolicies"], "TotalTimeout=0", "the flag's own longdesc must warn that it leaves the policies without a timeout")
}
