package model_test

import (
	"bufio"
	"bytes"
	"testing"

	"github.com/bitcoin-sv/ubsv/services/blockpersister/utxoset/model"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewUTXOValue(t *testing.T) {
	script := []byte{0x00, 0x01, 0x02, 0x03, 0x04}

	utxo := model.NewUTXOValue(1000, 42, script)

	assert.Equal(t, script, utxo.Script)
	assert.Equal(t, uint64(1000), utxo.Value)
	assert.Equal(t, uint32(42), utxo.Locktime)
	assert.Equal(t, 5, len(utxo.Script))
}

func TestEqual(t *testing.T) {
	script1 := []byte{0x00, 0x01, 0x02, 0x03, 0x04}
	script2 := []byte{0x00, 0x01, 0x02, 0x03, 0x04}

	utxo1 := model.NewUTXOValue(1000, 42, script1)
	utxo2 := model.NewUTXOValue(1000, 42, script2)
	utxo3 := model.NewUTXOValue(1000, 43, script1)

	assert.Equal(t, utxo1, utxo2)
	assert.True(t, utxo1.Equal(utxo2))
	assert.NotEqual(t, utxo1, utxo3)
	assert.False(t, utxo1.Equal(utxo3))
}

func TestNewUTXOValueFromBytes(t *testing.T) {
	script := []byte{0x00, 0x01, 0x02, 0x03, 0x04}

	utxo := model.NewUTXOValue(1000, 42, script)

	b := utxo.Bytes()

	utxo2 := model.NewUTXOValueFromBytes(b)

	assert.Equal(t, utxo, utxo2)
}

func TestNewUTXOValueFromReader(t *testing.T) {
	script1 := []byte{0x00, 0x01, 0x02, 0x03, 0x04}
	script2 := []byte{0x05, 0x06, 0x07, 0x08, 0x09}

	utxo1 := model.NewUTXOValue(1001, 42, script1)
	utxo2 := model.NewUTXOValue(1002, 0, script2)

	assert.NotEqual(t, utxo1, utxo2)

	// Test the Write(io.Writer) method
	buf := new(bytes.Buffer)
	w := bufio.NewWriter(buf)

	err := utxo1.Write(w)
	require.NoError(t, err)

	err = utxo2.Write(w)
	require.NoError(t, err)

	err = w.Flush()
	require.NoError(t, err)

	// Test the Read(io.Reader) method
	r := bufio.NewReader(buf)

	utxo3, err := model.NewUTXOValueFromReader(r)
	require.NoError(t, err)

	utxo4, err := model.NewUTXOValueFromReader(r)
	require.NoError(t, err)

	assert.Equal(t, utxo1, utxo3)
	assert.Equal(t, utxo2, utxo4)
	assert.NotEqual(t, utxo1, utxo4)
}
