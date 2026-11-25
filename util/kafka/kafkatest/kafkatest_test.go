package kafkatest

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// Tests on a shared broker tell their topics and consumer groups apart only by
// UniqueName, so two calls with the same prefix must not return the same name.
func TestUniqueNameNeverRepeatsAPrefix(t *testing.T) {
	first := UniqueName("topic")
	second := UniqueName("topic")

	require.NotEqual(t, first, second)
	require.Contains(t, first, "topic-")
}
