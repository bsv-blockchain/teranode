package daemon

import (
	"testing"

	"github.com/bsv-blockchain/teranode/util/kafka"
	"github.com/stretchr/testify/require"
)

func TestNonNilConsumerGroup(t *testing.T) {
	// A typed nil must map to a true nil interface so downstream nil checks
	// (e.g. p2p's startInvalidBlocksConsumer) behave correctly.
	require.Nil(t, nonNilConsumerGroup(nil))

	group := &kafka.KafkaConsumerGroup{}
	require.NotNil(t, nonNilConsumerGroup(group))
	require.Same(t, group, nonNilConsumerGroup(group))
}
