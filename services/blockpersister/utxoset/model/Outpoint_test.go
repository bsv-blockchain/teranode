package model_test

import (
	"bufio"
	"bytes"
	"testing"

	"github.com/bitcoin-sv/ubsv/services/blockpersister/utxoset/model"

	"encoding/binary"

	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewOutpoint(t *testing.T) {
	hash, err := chainhash.NewHashFromStr("0000000000000000000000000000000000000000000000000000000000000001")
	require.NoError(t, err)

	outpoint := model.NewOutpoint(*hash, 1)
	assert.Equal(t, hash.String(), outpoint.TxID.String())
	assert.Equal(t, uint32(1), outpoint.Index)
}

func TestNewOutpointFromBytes(t *testing.T) {
	// Create a valid byte slice
	hashBytes, err := chainhash.NewHashFromStr("0000000000000000000000000000000000000000000000000000000000000001")
	require.NoError(t, err)
	indexBytes := make([]byte, 4)
	binary.LittleEndian.PutUint32(indexBytes, uint32(1))
	b := append(hashBytes[:], indexBytes...)

	outpoint, err := model.NewOutpointFromBytes(b)
	require.NoError(t, err, "NewOutpointFromBytes() failed: %v", err)

	assert.Equal(t, hashBytes.String(), outpoint.TxID.String(), "NewOutpointFromBytes() failed, expected %v:%d, got %v:%d", hashBytes.String())
	assert.Equal(t, uint32(1), outpoint.Index, "NewOutpointFromBytes() failed, expected %v:%d, got %v:%d", 1)

	// Test invalid length
	invalidB := append(b, 0x00)
	_, err = model.NewOutpointFromBytes(invalidB)
	require.Error(t, err, "Expected error for invalid slice length, got none")
}

func TestOutpointBytes(t *testing.T) {
	// Create a valid byte slice
	hash, err := chainhash.NewHashFromStr("0000000000000000000000000000000000000000000000000000000000000001")
	require.NoError(t, err)

	outpoint := model.NewOutpoint(*hash, 1)

	// Create a valid byte slice
	indexBytes := make([]byte, 4)
	binary.LittleEndian.PutUint32(indexBytes, uint32(1))
	expected := append(hash[:], indexBytes...)

	result := outpoint.Bytes()
	assert.Len(t, result, 36, "Bytes() failed, expected length 36, got %d", len(result))
	assert.Equal(t, expected, result, "Bytes() failed, expected %x, got %x", expected, result)
}

func TestSameOutpointBytes(t *testing.T) {
	// Create a valid byte slice
	hash1, err := chainhash.NewHashFromStr("0000000000000000000000000000000000000000000000000000000000000001")
	require.NoError(t, err)

	hash2, err := chainhash.NewHashFromStr("0000000000000000000000000000000000000000000000000000000000000001")
	require.NoError(t, err)

	outpoint1 := model.NewOutpoint(*hash1, 1)
	outpoint2 := model.NewOutpoint(*hash2, 1)
	outpoint3 := model.NewOutpoint(*hash2, 2)

	assert.Equal(t, outpoint1, outpoint2, "Expected outpoints to be equal")
	assert.NotEqual(t, outpoint1, outpoint3, "Expected outpoints to be different")

	b1 := outpoint1.Bytes()
	assert.Len(t, b1, 36, "Bytes() failed, expected length 36, got %d", len(b1))

	b2 := outpoint2.Bytes()
	assert.Len(t, b2, 36, "Bytes() failed, expected length 36, got %d", len(b2))

	b3 := outpoint3.Bytes()

	assert.Equal(t, b1, b2, "Expected byte slices to be equal")
	assert.NotEqual(t, b1, b3, "Expected byte slices to be different")
}

func TestOutpointString(t *testing.T) {
	hash, err := chainhash.NewHashFromStr("0000000000000000000000000000000000000000000000000000000000000001")
	require.NoError(t, err)

	op := model.NewOutpoint(*hash, 1)
	expected := "0000000000000000000000000000000000000000000000000000000000000001:1"
	if op.String() != expected {
		t.Errorf("String() failed, expected %s, got %s", expected, op.String())
	}
}

func TestOutpointAsKey(t *testing.T) {
	m := make(map[model.Outpoint]int)
	assert.Len(t, m, 0, "Expected length 0")

	hash1, err := chainhash.NewHashFromStr("0000000000000000000000000000000000000000000000000000000000000001")
	require.NoError(t, err)

	hash2, err := chainhash.NewHashFromStr("0000000000000000000000000000000000000000000000000000000000000001")
	require.NoError(t, err)

	outpoint1 := model.NewOutpoint(*hash1, 0)
	m[*outpoint1] = 1
	assert.Len(t, m, 1, "Expected length 1")

	n, ok := m[*outpoint1]
	assert.True(t, ok, "Expected true")
	assert.Equal(t, 1, n, "Expected 1, got %d", n)

	outpoint2 := model.NewOutpoint(*hash2, 0)
	m[*outpoint2] = 2
	assert.Len(t, m, 1, "Expected length 1")

	n, ok = m[*outpoint1]
	assert.True(t, ok, "Expected true")
	assert.Equal(t, 2, n, "Expected 2, got %d", n)

	outpoint3 := model.NewOutpoint(*hash1, 1)
	m[*outpoint3] = 3
	assert.Len(t, m, 2, "Expected length 2")

	n, ok = m[*outpoint3]
	assert.True(t, ok, "Expected true")
	assert.Equal(t, 3, n, "Expected 3, got %d", n)

	delete(m, *outpoint1)
	assert.Len(t, m, 1, "Expected length 1")
}

func TestOutpointEqual(t *testing.T) {
	hash1, err := chainhash.NewHashFromStr("0000000000000000000000000000000000000000000000000000000000000001")
	require.NoError(t, err)

	hash2, err := chainhash.NewHashFromStr("0000000000000000000000000000000000000000000000000000000000000001")
	require.NoError(t, err)

	outpoint1 := model.NewOutpoint(*hash1, 1)
	outpoint2 := model.NewOutpoint(*hash2, 1)

	assert.Equal(t, outpoint1, outpoint2, "Expected outpoints to be equal")
	assert.True(t, outpoint1.Equal(outpoint2))
}

func TestNewOutpointFromReader(t *testing.T) {
	hash1, err := chainhash.NewHashFromStr("0000000000000000000000000000000000000000000000000000000000000001")
	require.NoError(t, err)

	hash2, err := chainhash.NewHashFromStr("0000000000000000000000000000000000000000000000000000000000000002")
	require.NoError(t, err)

	outpoint1 := model.NewOutpoint(*hash1, 1)
	outpoint2 := model.NewOutpoint(*hash2, 2)

	// Test the Write(io.Writer) method
	buf := new(bytes.Buffer)
	w := bufio.NewWriter(buf)

	err = outpoint1.Write(w)
	require.NoError(t, err)

	err = outpoint2.Write(w)
	require.NoError(t, err)

	err = w.Flush()
	require.NoError(t, err)

	// Test the Read(io.Reader) method
	r := bufio.NewReader(buf)

	outpoint3, err := model.NewOutpointFromReader(r)
	require.NoError(t, err)

	outpoint4, err := model.NewOutpointFromReader(r)
	require.NoError(t, err)

	assert.Equal(t, outpoint1, outpoint3, "Expected outpoints to be equal")
	assert.Equal(t, outpoint2, outpoint4, "Expected outpoints to be equal")
	assert.NotEqual(t, outpoint1, outpoint4, "Expected outpoints to be different")
	assert.Equal(t, uint32(1), outpoint3.Index)
	assert.Equal(t, uint32(2), outpoint4.Index)
}
