package kafkatest

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// Tests on a shared broker tell their topics and consumer groups apart only by
// UniqueName, so calls with the same prefix must never return the same name.
func TestUniqueNameNeverRepeatsAPrefix(t *testing.T) {
	seen := map[string]bool{}

	for range 3 {
		name := UniqueName("topic")
		require.Contains(t, name, "topic-")
		require.False(t, seen[name], "UniqueName returned %q twice", name)

		seen[name] = true
	}
}
