package http_impl

import (
	"bytes"
	"encoding/hex"
	"io"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSubtreeReader(t *testing.T) {
	b, err := os.ReadFile("subtree.bin")
	require.NoError(t, err)

	reader := bytes.NewReader(b)

	r, err := NewSubtreeNodesReader(reader)
	require.NoError(t, err)
	assert.Equal(t, 161, r.itemCount)
	assert.Equal(t, 0, r.itemsRead)

	buf := make([]byte, 32)

	n, err := r.Read(buf)
	require.NoError(t, err)
	assert.Equal(t, 32, n)
	assert.Equal(t, "ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff", hex.EncodeToString(buf))

	for i := 1; i < r.itemCount; i++ {
		n, err := r.Read(buf)
		require.NoError(t, err)
		assert.Equal(t, 32, n)
		t.Logf("Read %s", hex.EncodeToString(buf))
	}

	assert.Equal(t, 161, r.itemsRead)

	n, err = r.Read(buf)
	assert.ErrorIs(t, err, io.EOF)
	assert.Equal(t, 0, n)
}
