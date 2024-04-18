package http_impl

import (
	"bytes"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"testing"
	"time"

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

	n, err := io.ReadFull(r, buf)
	require.NoError(t, err)
	assert.Equal(t, 32, n)
	assert.Equal(t, "ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff", hex.EncodeToString(buf))

	for i := 1; i < r.itemCount; i++ {
		n, err := io.ReadFull(r, buf)
		require.NoError(t, err)
		assert.Equal(t, 32, n)
		// t.Logf("Read %s", hex.EncodeToString(buf))
	}

	// At the end of this loop, check the last hash is correct
	assert.Equal(t, "3b2a83072a9ed1caa2f1d00cee48b7f11ba1c8afc9841d0e04bee7f6cf6d7f4e", hex.EncodeToString(buf))

	assert.Equal(t, 161, r.itemsRead)

	n, err = io.ReadFull(r, buf)
	assert.ErrorIs(t, err, io.EOF)
	assert.Equal(t, 0, n)
}

func TestCalculateSpeed(t *testing.T) {
	b := make([]byte, 1024)
	duration := 1 * time.Second
	sizeInKB := float64(len(b)) / 1024

	res := fmt.Sprintf("%.2f kB): in %s (%.2f kB/sec)", sizeInKB, duration, calculateSpeed(duration, sizeInKB))
	assert.Equal(t, "1.00 kB): in 1s (1.00 kB/sec)", res)
}
