package model

import (
	"bufio"
	"bytes"
	"testing"

	"github.com/libsv/go-bt/v2"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewUTXODiffFromReader(t *testing.T) {
	// Create a valid byte slice
	hash := chainhash.HashH([]byte{0x00, 0x01, 0x02, 0x03, 0x04})

	// Create a new UTXODiff
	ud1 := NewUTXODiff(&hash, 22)

	b := []byte{0x00, 0x01, 0x02, 0x03, 0x04}

	// Add some UTXOs
	for i := uint32(0); i < 5; i++ {
		ud1.Add(hash, i, 1000+1, 10+1, []byte{b[i]})
	}

	assert.Equal(t, ud1.Added.Length(), 5)
	assert.Equal(t, ud1.Removed.Length(), 0)

	// Remove some UTXOs
	for i := uint32(3); i < 8; i++ {
		ud1.Delete(hash, i)
	}

	assert.Equal(t, ud1.Added.Length(), 3)
	assert.Equal(t, ud1.Removed.Length(), 3)

	buf := new(bytes.Buffer)
	w := bufio.NewWriter(buf)

	err := ud1.Write(w)
	require.NoError(t, err)

	// Flush the buffer
	err = w.Flush()
	require.NoError(t, err)

	// Read the UTXOMap from the buffer
	r := bufio.NewReader(buf)
	ud2, err := NewUTXODiffFromReader(r)
	require.NoError(t, err)

	// Check the UTXOMap is the same
	assert.Equal(t, ud1.BlockHash, ud2.BlockHash)
	assert.Equal(t, ud1.BlockHeight, ud2.BlockHeight)
	assert.Equal(t, ud1.Added.Length(), ud2.Added.Length())
	assert.Equal(t, ud1.Removed.Length(), ud2.Removed.Length())
}

func TestNewUTXODiffFromReaderWithProcessTx(t *testing.T) {
	hash := chainhash.HashH([]byte{0x00, 0x01, 0x02, 0x03, 0x04})

	tx := bt.NewTx()

	err := tx.From(hash.String(), 42, "0011", 1024)
	require.NoError(t, err)
	err = tx.From(hash.String(), 43, "0011", 1024)
	require.NoError(t, err)
	err = tx.From(hash.String(), 44, "0011", 1024)
	require.NoError(t, err)

	err = tx.PayToAddress("1MM6xtKRdUAHQ4hZkqwVGf8wnDuYu1dHPA", 100)
	require.NoError(t, err)
	err = tx.PayToAddress("1MM6xtKRdUAHQ4hZkqwVGf8wnDuYu1dHPA", 100)
	require.NoError(t, err)
	err = tx.PayToAddress("1MM6xtKRdUAHQ4hZkqwVGf8wnDuYu1dHPA", 100)
	require.NoError(t, err)
	err = tx.PayToAddress("1MM6xtKRdUAHQ4hZkqwVGf8wnDuYu1dHPA", 100)
	require.NoError(t, err)

	err = tx.AddOpReturnOutput([]byte("hello world"))
	require.NoError(t, err)

	// Create a new UTXODiff
	ud1 := NewUTXODiff(&hash, 22)

	ud1.ProcessTx(tx)

	assert.Equal(t, 4, ud1.Added.Length())
	assert.Equal(t, 3, ud1.Removed.Length())

	buf := new(bytes.Buffer)
	w := bufio.NewWriter(buf)

	err = ud1.Write(w)
	require.NoError(t, err)

	// Flush the buffer
	err = w.Flush()
	require.NoError(t, err)

	// Read the UTXOMap from the buffer
	r := bufio.NewReader(buf)
	ud2, err := NewUTXODiffFromReader(r)
	require.NoError(t, err)

	// Check the UTXOMap is the same
	assert.Equal(t, ud1.BlockHash, ud2.BlockHash)
	assert.Equal(t, ud1.BlockHeight, ud2.BlockHeight)
	assert.Equal(t, ud1.Added.Length(), ud2.Added.Length())
	assert.Equal(t, ud1.Removed.Length(), ud2.Removed.Length())
}
