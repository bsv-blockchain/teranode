package httpimpl

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestReadMode_String(t *testing.T) {
	t.Run("BINARY_STREAM mode", func(t *testing.T) {
		mode := BINARY_STREAM

		assert.Equal(t, "BINARY", mode.String())
	})

	t.Run("HEX mode", func(t *testing.T) {
		mode := HEX

		assert.Equal(t, "HEX", mode.String())
	})

	t.Run("JSON mode", func(t *testing.T) {
		mode := JSON

		assert.Equal(t, "JSON", mode.String())
	})

	t.Run("Unknown mode", func(t *testing.T) {
		var mode ReadMode = 999

		assert.Equal(t, "UNKNOWN", mode.String())
	})
}
