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

func TestNewUTXOKey(t *testing.T) {
	hash, err := chainhash.NewHashFromStr("0000000000000000000000000000000000000000000000000000000000000001")
	require.NoError(t, err)

	key := model.NewUTXOKey(*hash, 1)
	assert.Equal(t, hash.String(), key.TxID.String())
	assert.Equal(t, uint32(1), key.Index)
}

func TestNewUTXOKeyFromBytes(t *testing.T) {
	// Create a valid byte slice
	hashBytes, err := chainhash.NewHashFromStr("0000000000000000000000000000000000000000000000000000000000000001")
	require.NoError(t, err)
	indexBytes := make([]byte, 4)
	binary.LittleEndian.PutUint32(indexBytes, uint32(1))
	b := append(hashBytes[:], indexBytes...)

	key, err := model.NewUTXOKeyFromBytes(b)
	require.NoError(t, err, "NewUTXOKeyFromBytes() failed: %v", err)

	assert.Equal(t, hashBytes.String(), key.TxID.String(), "NewUTXOKeyFromBytes() failed, expected %v:%d, got %v:%d", hashBytes.String())
	assert.Equal(t, uint32(1), key.Index, "NewUTXOKeyFromBytes() failed, expected %v:%d, got %v:%d", 1)

	// Test invalid length
	invalidB := append(b, 0x00)
	_, err = model.NewUTXOKeyFromBytes(invalidB)
	require.Error(t, err, "Expected error for invalid slice length, got none")
}

func TestUTXOKeyBytes(t *testing.T) {
	// Create a valid byte slice
	hash, err := chainhash.NewHashFromStr("0000000000000000000000000000000000000000000000000000000000000001")
	require.NoError(t, err)

	key := model.NewUTXOKey(*hash, 1)

	// Create a valid byte slice
	indexBytes := make([]byte, 4)
	binary.LittleEndian.PutUint32(indexBytes, uint32(1))
	expected := append(hash[:], indexBytes...)

	result := key.Bytes()
	assert.Len(t, result, 36, "Bytes() failed, expected length 36, got %d", len(result))
	assert.Equal(t, expected, result, "Bytes() failed, expected %x, got %x", expected, result)
}

func TestSameUTXOKeyBytes(t *testing.T) {
	// Create a valid byte slice
	hash1, err := chainhash.NewHashFromStr("0000000000000000000000000000000000000000000000000000000000000001")
	require.NoError(t, err)

	hash2, err := chainhash.NewHashFromStr("0000000000000000000000000000000000000000000000000000000000000001")
	require.NoError(t, err)

	key1 := model.NewUTXOKey(*hash1, 1)
	key2 := model.NewUTXOKey(*hash2, 1)
	key3 := model.NewUTXOKey(*hash2, 2)

	assert.Equal(t, key1, key2, "Expected keys to be equal")
	assert.NotEqual(t, key1, key3, "Expected keys to be different")

	b1 := key1.Bytes()
	assert.Len(t, b1, 36, "Bytes() failed, expected length 36, got %d", len(b1))

	b2 := key2.Bytes()
	assert.Len(t, b2, 36, "Bytes() failed, expected length 36, got %d", len(b2))

	b3 := key3.Bytes()

	assert.Equal(t, b1, b2, "Expected byte slices to be equal")
	assert.NotEqual(t, b1, b3, "Expected byte slices to be different")
}

func TestUTXOKeyString(t *testing.T) {
	hash, err := chainhash.NewHashFromStr("0000000000000000000000000000000000000000000000000000000000000001")
	require.NoError(t, err)

	op := model.NewUTXOKey(*hash, 1)
	expected := "0000000000000000000000000000000000000000000000000000000000000001:1"
	if op.String() != expected {
		t.Errorf("String() failed, expected %s, got %s", expected, op.String())
	}
}

func TestUTXOKeyAsKey(t *testing.T) {
	m := make(map[model.UTXOKey]int)
	assert.Len(t, m, 0, "Expected length 0")

	hash1, err := chainhash.NewHashFromStr("0000000000000000000000000000000000000000000000000000000000000001")
	require.NoError(t, err)

	hash2, err := chainhash.NewHashFromStr("0000000000000000000000000000000000000000000000000000000000000001")
	require.NoError(t, err)

	key1 := model.NewUTXOKey(*hash1, 0)
	m[key1] = 1
	assert.Len(t, m, 1, "Expected length 1")

	n, ok := m[key1]
	assert.True(t, ok, "Expected true")
	assert.Equal(t, 1, n, "Expected 1, got %d", n)

	key2 := model.NewUTXOKey(*hash2, 0)
	m[key2] = 2
	assert.Len(t, m, 1, "Expected length 1")

	n, ok = m[key1]
	assert.True(t, ok, "Expected true")
	assert.Equal(t, 2, n, "Expected 2, got %d", n)

	key3 := model.NewUTXOKey(*hash1, 1)
	m[key3] = 3
	assert.Len(t, m, 2, "Expected length 2")

	n, ok = m[key3]
	assert.True(t, ok, "Expected true")
	assert.Equal(t, 3, n, "Expected 3, got %d", n)

	delete(m, key1)
	assert.Len(t, m, 1, "Expected length 1")
}

func TestUTXOKeyEqual(t *testing.T) {
	hash1, err := chainhash.NewHashFromStr("0000000000000000000000000000000000000000000000000000000000000001")
	require.NoError(t, err)

	hash2, err := chainhash.NewHashFromStr("0000000000000000000000000000000000000000000000000000000000000001")
	require.NoError(t, err)

	key1 := model.NewUTXOKey(*hash1, 1)
	key2 := model.NewUTXOKey(*hash2, 1)

	assert.Equal(t, key1, key2, "Expected keys to be equal")
	assert.True(t, key1.Equal(key2))
}

func TestNewUTXOKeyFromReader(t *testing.T) {
	hash1, err := chainhash.NewHashFromStr("0000000000000000000000000000000000000000000000000000000000000001")
	require.NoError(t, err)

	hash2, err := chainhash.NewHashFromStr("0000000000000000000000000000000000000000000000000000000000000002")
	require.NoError(t, err)

	key1 := model.NewUTXOKey(*hash1, 1)
	key2 := model.NewUTXOKey(*hash2, 2)

	// Test the Write(io.Writer) method
	buf := new(bytes.Buffer)
	w := bufio.NewWriter(buf)

	err = key1.Write(w)
	require.NoError(t, err)

	err = key2.Write(w)
	require.NoError(t, err)

	err = w.Flush()
	require.NoError(t, err)

	// Test the Read(io.Reader) method
	r := bufio.NewReader(buf)

	key3, err := model.NewUTXOKeyFromReader(r)
	require.NoError(t, err)

	key4, err := model.NewUTXOKeyFromReader(r)
	require.NoError(t, err)

	assert.Equal(t, key1, *key3, "Expected keys to be equal")
	assert.Equal(t, key2, *key4, "Expected keys to be equal")
	assert.NotEqual(t, key1, *key4, "Expected keys to be different")
	assert.Equal(t, uint32(1), key3.Index)
	assert.Equal(t, uint32(2), key4.Index)
}
