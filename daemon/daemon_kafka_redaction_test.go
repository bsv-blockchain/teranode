package daemon

import (
	"net/url"
	"testing"

	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/stretchr/testify/require"
)

// TestGetKafkaConsumerGroupErrorRedactsURL covers the error returned when a
// Kafka consumer URL is rejected. The URL is formatted into that error, which
// is logged, so any userinfo password must reach it masked. A nil URL used to
// panic on the same line, because the message called url.String() on it.
func TestGetKafkaConsumerGroupErrorRedactsURL(t *testing.T) {
	t.Run("credentialed URL is masked", func(t *testing.T) {
		const password = "canary-kafka-url-password"

		// sessionTimeout below 3x heartbeatInterval is rejected before any
		// broker is contacted.
		u, err := url.Parse("kafka://teranode:" + password + "@broker:9092/blocks?sessionTimeout=1000&heartbeatInterval=1000")
		require.NoError(t, err)

		consumer, err := getKafkaConsumerGroup(ulogger.TestLogger{}, u, "test-group", false, nil)
		require.Nil(t, consumer)
		require.Error(t, err)

		require.NotContains(t, err.Error(), password, "the Kafka URL password reached the error message")
		require.Contains(t, err.Error(), "broker:9092", "the broker should survive redaction")
	})

	t.Run("nil URL reports an error instead of panicking", func(t *testing.T) {
		require.NotPanics(t, func() {
			consumer, err := getKafkaConsumerGroup(ulogger.TestLogger{}, nil, "test-group", false, nil)
			require.Nil(t, consumer)
			require.Error(t, err)
		})
	})
}
