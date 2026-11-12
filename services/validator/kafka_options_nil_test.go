package validator

import (
	"testing"

	kafkamessage "github.com/bsv-blockchain/teranode/util/kafka/kafka_message"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

// TestOptionsFromKafkaMessage_NilOptions reproduces the crash reported for
// services/validator/Server.go: a validatortxs Kafka message that omits the
// optional Options field unmarshals with kafkaMsg.Options == nil. Dereferencing
// it must not panic; it must fall back to the exact defaults the in-tree
// producer sends (validator.NewDefaultOptions()).
func TestOptionsFromKafkaMessage_NilOptions(t *testing.T) {
	// Build the wire bytes the way a foreign/crafted producer could: a
	// KafkaTxValidationTopicMessage with Options simply omitted.
	msg := &kafkamessage.KafkaTxValidationTopicMessage{
		Tx:     []byte{0x01, 0x02, 0x03},
		Height: 0,
	}

	raw, err := proto.Marshal(msg)
	require.NoError(t, err)

	var decoded kafkamessage.KafkaTxValidationTopicMessage
	require.NoError(t, proto.Unmarshal(raw, &decoded))
	require.Nil(t, decoded.Options, "test setup: Options must be absent on the wire")

	// This must not panic.
	require.NotPanics(t, func() {
		opts := optionsFromKafkaMessage(decoded.Options)

		// Defaults must match what NewDefaultOptions() (and therefore the
		// in-tree producer) already sends, so a foreign producer that omits
		// Options can never get more permissive treatment than the
		// documented default.
		defaults := NewDefaultOptions()
		require.Equal(t, defaults.SkipUtxoCreation, opts.SkipUtxoCreation)
		require.Equal(t, defaults.AddTXToBlockAssembly, opts.AddTXToBlockAssembly)
		require.Equal(t, defaults.SkipPolicyChecks, opts.SkipPolicyChecks)
		require.Equal(t, defaults.CreateConflicting, opts.CreateConflicting)
	})
}

// TestOptionsFromKafkaMessage_PresentOptions ensures a message that does
// carry Options still has its fields honoured verbatim (no accidental
// default-clobbering of an explicitly-set message).
func TestOptionsFromKafkaMessage_PresentOptions(t *testing.T) {
	kafkaOpts := &kafkamessage.KafkaTxValidationOptions{
		SkipUtxoCreation:     true,
		AddTXToBlockAssembly: false,
		SkipPolicyChecks:     true,
		CreateConflicting:    true,
	}

	opts := optionsFromKafkaMessage(kafkaOpts)

	require.True(t, opts.SkipUtxoCreation)
	require.False(t, opts.AddTXToBlockAssembly)
	require.True(t, opts.SkipPolicyChecks)
	require.True(t, opts.CreateConflicting)
}

// TestKafkaValidationOptions_NilOptions is the end-to-end check for the value
// actually handed to ValidateWithOptions by the validatortxs Kafka handler
// (services/validator/Server.go) when a message omits Options: every field must
// match NewDefaultOptions() - the in-tree producer's defaults - with one
// deliberate exception, WaitForBlockAssembly, which the handler always forces to
// true regardless of what the message carried. That field is not part of the
// wire message at all (KafkaTxValidationOptions has no such field); it is a
// property of the Kafka ingest path itself (see kafkaValidationOptions), so it
// is asserted explicitly here rather than folded into the defaults comparison -
// it is exactly the field a naive nil-options fallback could silently drop.
func TestKafkaValidationOptions_NilOptions(t *testing.T) {
	opts := kafkaValidationOptions(nil)

	require.True(t, opts.WaitForBlockAssembly, "WaitForBlockAssembly must always be true on the Kafka ingest path")

	expected := NewDefaultOptions()
	expected.WaitForBlockAssembly = true
	require.Equal(t, expected, opts, "every other field must be the exact producer default - never more permissive")
}

// TestKafkaValidationOptions_PresentOptions mirrors TestKafkaValidationOptions_NilOptions
// for a message that does carry Options: the wire fields must be honoured verbatim
// and WaitForBlockAssembly must still be forced true.
func TestKafkaValidationOptions_PresentOptions(t *testing.T) {
	kafkaOpts := &kafkamessage.KafkaTxValidationOptions{
		SkipUtxoCreation:     true,
		AddTXToBlockAssembly: false,
		SkipPolicyChecks:     true,
		CreateConflicting:    true,
	}

	opts := kafkaValidationOptions(kafkaOpts)

	require.True(t, opts.WaitForBlockAssembly, "WaitForBlockAssembly must always be true on the Kafka ingest path")

	expected := &Options{
		SkipUtxoCreation:     true,
		AddTXToBlockAssembly: false,
		SkipPolicyChecks:     true,
		CreateConflicting:    true,
		WaitForBlockAssembly: true,
	}
	require.Equal(t, expected, opts)
}
