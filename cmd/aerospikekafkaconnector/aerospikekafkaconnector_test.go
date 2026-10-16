package aerospikekafkaconnector

import (
	"strings"
	"testing"

	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/stretchr/testify/require"
)

// TestReadAerospikeKafkaRejectsUnparseableURLWithoutEchoingIt covers the same
// mechanism as cmd/seedimport: url.Parse embeds the string it was given
// verbatim in its error, so wrapping that error whole undoes the redaction
// applied to the same value fourteen lines earlier in this function.
//
// --kafka-url is operator-supplied and carries its password in the userinfo,
// and this binary runs in a container, so its error output is container logs.
func TestReadAerospikeKafkaRejectsUnparseableURLWithoutEchoingIt(t *testing.T) {
	const password = "canary-kafka-url-password"

	// A space in the host is what makes url.Parse refuse it.
	kafkaURL := "kafka://teranode:" + password + "@broker host:9092/txs"

	err := ReadAerospikeKafka(ulogger.TestLogger{}, &settings.Settings{}, kafkaURL, "", "", "", 0)
	require.Error(t, err)

	require.NotContains(t, err.Error(), password, "the Kafka URL password reached the error message")
	require.NotContains(t, err.Error(), "teranode:", "the Kafka URL userinfo reached the error message")
	require.NotContains(t, err.Error(), "broker host", "the Kafka URL host reached the error message")

	// The operator still learns both which URL failed and why. Asserting the
	// reason and not only the prefix is what makes this catch a message left
	// carrying an unrendered format verb.
	require.Contains(t, err.Error(), "failed to parse Kafka URL")
	require.Contains(t, strings.ToLower(err.Error()), "invalid character",
		"expected the parse failure reason to survive, got: %v", err)
	require.NotContains(t, err.Error(), "%", "the message renders a format verb literally")
}

// TestReadAerospikeKafkaRejectsURLWithDelimiterInPasswordWithoutQuotingIt
// covers what the space-in-host canary cannot: a raw "?" in the password ends
// the authority there, and url.Parse's own reason quotes everything before it
// as an invalid port.
func TestReadAerospikeKafkaRejectsURLWithDelimiterInPasswordWithoutQuotingIt(t *testing.T) {
	err := ReadAerospikeKafka(ulogger.TestLogger{}, &settings.Settings{},
		"kafka://teranode:canary-kafka?url-password@broker:9092/txs", "", "", "", 0)
	require.Error(t, err)

	require.NotContains(t, err.Error(), "canary-kafka", "the start of the Kafka URL password reached the error message")
	require.Contains(t, err.Error(), "failed to parse Kafka URL")
	require.Contains(t, err.Error(), "percent-encode", "expected the fixed reason, got: %v", err)
}
