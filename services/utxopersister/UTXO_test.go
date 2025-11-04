// Package utxopersister provides functionality for managing UTXO (Unspent Transaction Output) persistence.
package utxopersister

import (
	"bytes"
	"math"
	"os"
	"sync"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBits(t *testing.T) {
	var unsigned uint32 = 12345

	encodedValue1 := unsigned << 1
	bytes1 := append([]byte{}, byte(encodedValue1), byte(encodedValue1>>8), byte(encodedValue1>>16), byte(encodedValue1>>24))

	encodedValue2 := (unsigned << 1) | 1
	bytes2 := append([]byte{}, byte(encodedValue2), byte(encodedValue2>>8), byte(encodedValue2>>16), byte(encodedValue2>>24))

	decodedValue1 := uint32(bytes1[0]) | uint32(bytes1[1])<<8 | uint32(bytes1[2])<<16 | uint32(bytes1[3])<<24
	assert.Equal(t, uint32(12345), decodedValue1>>1)
	assert.False(t, (decodedValue1&1) == 1)

	decodedValue2 := uint32(bytes2[0]) | uint32(bytes2[1])<<8 | uint32(bytes2[2])<<16 | uint32(bytes2[3])<<24
	assert.Equal(t, uint32(12345), decodedValue2>>1)
	assert.True(t, (decodedValue2&1) == 1)
}

func TestBytesNormalTX(t *testing.T) {
	hash := chainhash.HashH([]byte{0x00, 0x01, 0x02, 0x03, 0x04})

	uw := &UTXOWrapper{
		TxID:     hash,
		Height:   12345,
		Coinbase: false,
		UTXOs: []*UTXO{
			{
				Index:  12345,
				Value:  1234567890,
				Script: []byte{0x00, 0x01, 0x02, 0x03, 0x04},
			},
		},
	}

	b := uw.Bytes()
	// t.Logf("b: %x", b)

	assert.Len(t, b, 32+4+4+4+8+4+5)

	uw2, err := NewUTXOWrapperFromBytes(b, math.MaxUint32)
	assert.NoError(t, err)

	assert.Equal(t, uw.TxID, uw2.TxID)
	assert.Equal(t, uw.Height, uw2.Height)
	assert.Equal(t, uw.Coinbase, uw2.Coinbase)
	assert.Equal(t, uw.UTXOs[0].Index, uw2.UTXOs[0].Index)
	assert.Equal(t, uw.UTXOs[0].Value, uw2.UTXOs[0].Value)
	assert.Equal(t, uw.UTXOs[0].Script, uw2.UTXOs[0].Script)
}

func TestBytesCoinbaseTX(t *testing.T) {
	hash := chainhash.HashH([]byte{0x00, 0x01, 0x02, 0x03, 0x04})

	uw := &UTXOWrapper{
		TxID:     hash,
		Height:   12345,
		Coinbase: true,
		UTXOs: []*UTXO{
			{
				Index:  12345,
				Value:  1234567890,
				Script: []byte{0x00, 0x01, 0x02, 0x03, 0x04},
			},
		},
	}

	b := uw.Bytes()
	// t.Logf("b: %x", b)

	assert.Len(t, b, 32+4+4+4+8+4+5)

	u2, err := NewUTXOWrapperFromBytes(b, math.MaxUint32)
	assert.NoError(t, err)

	assert.Equal(t, uw.TxID, u2.TxID)
	assert.Equal(t, uw.Height, u2.Height)
	assert.Equal(t, uw.Coinbase, u2.Coinbase)
	assert.Equal(t, uw.UTXOs[0].Index, u2.UTXOs[0].Index)
	assert.Equal(t, uw.UTXOs[0].Value, u2.UTXOs[0].Value)
	assert.Equal(t, uw.UTXOs[0].Script, u2.UTXOs[0].Script)
}

func TestTxWithOnlyOutputs(t *testing.T) {
	txBytes, err := os.ReadFile("testdata/4827ad32852fd9ca3979a8e507e38c6e557e58bc453184ef19cc7a5f86e7d59b.outputs")
	require.NoError(t, err)

	uw, err := NewUTXOWrapperFromBytes(txBytes, math.MaxUint32)
	require.NoError(t, err)

	assert.Equal(t, 476, len(uw.UTXOs))

	utxos := PadUTXOsWithNil(uw.UTXOs)
	assert.NotNil(t, utxos)
	assert.Equal(t, 1000, len(utxos))

	// check the length of utxos that are not nil
	count := 0

	for _, u := range utxos {
		if u != nil {
			count++
		}
	}

	assert.Equal(t, 476, count)
}

func TestNewUTXOFromReader_DoSProtection(t *testing.T) {
	t.Run("reject script length exceeding MaxUTXOScriptSize", func(t *testing.T) {
		testMaxScriptSize := uint32(500000)
		maliciousScriptLength := testMaxScriptSize + 1

		maliciousBytes := make([]byte, 16)
		maliciousBytes[0] = 0x01

		maliciousBytes[4] = 0x01

		maliciousBytes[12] = byte(maliciousScriptLength)
		maliciousBytes[13] = byte(maliciousScriptLength >> 8)
		maliciousBytes[14] = byte(maliciousScriptLength >> 16)
		maliciousBytes[15] = byte(maliciousScriptLength >> 24)

		reader := bytes.NewReader(maliciousBytes)
		uw := &UTXOWrapper{maxScriptSize: testMaxScriptSize}
		utxo := &UTXO{}

		err := uw.NewUTXOFromReader(reader, utxo)

		require.Error(t, err)
		assert.Contains(t, err.Error(), "script length")
		assert.Contains(t, err.Error(), "exceeds maximum allowed size")
	})

	t.Run("reject maximum uint32 script length", func(t *testing.T) {
		maliciousScriptLength := uint32(0xFFFFFFFF)
		testMaxScriptSize := uint32(500000)

		maliciousBytes := make([]byte, 16)
		maliciousBytes[0] = 0x01

		maliciousBytes[4] = 0x01

		maliciousBytes[12] = byte(maliciousScriptLength)
		maliciousBytes[13] = byte(maliciousScriptLength >> 8)
		maliciousBytes[14] = byte(maliciousScriptLength >> 16)
		maliciousBytes[15] = byte(maliciousScriptLength >> 24)

		reader := bytes.NewReader(maliciousBytes)
		uw := &UTXOWrapper{maxScriptSize: testMaxScriptSize}
		utxo := &UTXO{}

		err := uw.NewUTXOFromReader(reader, utxo)

		require.Error(t, err)
		assert.Contains(t, err.Error(), "script length")
		assert.Contains(t, err.Error(), "exceeds maximum allowed size")
	})

	t.Run("accept script length at MaxUTXOScriptSize", func(t *testing.T) {
		validScriptLength := uint32(500000)

		validBytes := make([]byte, 16+validScriptLength)
		validBytes[0] = 0x01

		validBytes[4] = 0x01

		validBytes[12] = byte(validScriptLength)
		validBytes[13] = byte(validScriptLength >> 8)
		validBytes[14] = byte(validScriptLength >> 16)
		validBytes[15] = byte(validScriptLength >> 24)

		reader := bytes.NewReader(validBytes)
		uw := &UTXOWrapper{maxScriptSize: validScriptLength}
		utxo := &UTXO{}

		err := uw.NewUTXOFromReader(reader, utxo)

		require.NoError(t, err)
		assert.Equal(t, int(validScriptLength), len(utxo.Script))
	})
}

func TestNewUTXOValueFromReader_DoSProtection(t *testing.T) {
	t.Run("reject script length exceeding MaxUTXOScriptSize", func(t *testing.T) {
		testMaxScriptSize := uint32(500000)
		maliciousScriptLength := testMaxScriptSize + 1

		maliciousBytes := make([]byte, 16)
		maliciousBytes[0] = 0x01

		maliciousBytes[4] = 0x01

		maliciousBytes[12] = byte(maliciousScriptLength)
		maliciousBytes[13] = byte(maliciousScriptLength >> 8)
		maliciousBytes[14] = byte(maliciousScriptLength >> 16)
		maliciousBytes[15] = byte(maliciousScriptLength >> 24)

		reader := bytes.NewReader(maliciousBytes)
		uw := &UTXOWrapper{maxScriptSize: testMaxScriptSize}

		value, err := uw.NewUTXOValueFromReader(reader)

		require.Error(t, err)
		assert.Contains(t, err.Error(), "script length")
		assert.Contains(t, err.Error(), "exceeds maximum allowed size")
		assert.Equal(t, uint64(0), value)
	})

	t.Run("reject maximum uint32 script length", func(t *testing.T) {
		maliciousScriptLength := uint32(0xFFFFFFFF)
		testMaxScriptSize := uint32(500000)

		maliciousBytes := make([]byte, 16)
		maliciousBytes[0] = 0x01

		maliciousBytes[4] = 0x01

		maliciousBytes[12] = byte(maliciousScriptLength)
		maliciousBytes[13] = byte(maliciousScriptLength >> 8)
		maliciousBytes[14] = byte(maliciousScriptLength >> 16)
		maliciousBytes[15] = byte(maliciousScriptLength >> 24)

		reader := bytes.NewReader(maliciousBytes)
		uw := &UTXOWrapper{maxScriptSize: testMaxScriptSize}

		value, err := uw.NewUTXOValueFromReader(reader)

		require.Error(t, err)
		assert.Contains(t, err.Error(), "script length")
		assert.Contains(t, err.Error(), "exceeds maximum allowed size")
		assert.Equal(t, uint64(0), value)
	})
}

func TestNewUTXOFromReader_ConcurrentAttack(t *testing.T) {
	numGoroutines := 20
	maliciousScriptLength := uint32(0xFFFFFFFF)
	testMaxScriptSize := uint32(500000)

	t.Logf("Starting %d goroutines, each attempting %.2f GB allocation = %.2f GB total!",
		numGoroutines, float64(maliciousScriptLength)/(1024*1024*1024),
		float64(numGoroutines)*float64(maliciousScriptLength)/(1024*1024*1024))

	maliciousBytes := make([]byte, 16)
	maliciousBytes[0] = 0x01

	maliciousBytes[4] = 0x01

	maliciousBytes[12] = byte(maliciousScriptLength)
	maliciousBytes[13] = byte(maliciousScriptLength >> 8)
	maliciousBytes[14] = byte(maliciousScriptLength >> 16)
	maliciousBytes[15] = byte(maliciousScriptLength >> 24)

	results := make(chan error, numGoroutines)
	var wg sync.WaitGroup

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)

		go func(goroutineID int) {
			defer wg.Done()

			reader := bytes.NewReader(maliciousBytes)
			uw := &UTXOWrapper{maxScriptSize: testMaxScriptSize}
			utxo := &UTXO{}

			err := uw.NewUTXOFromReader(reader, utxo)

			t.Logf("Goroutine %d: %v", goroutineID, err)
			results <- err
		}(i)
	}

	wg.Wait()
	close(results)

	errorCount := 0

	for err := range results {
		if err != nil {
			errorCount++
		}
	}

	require.Equal(t, numGoroutines, errorCount, "All goroutines should fail with validation error")

	t.Logf("✓ DoS protection successful: all %d concurrent allocation attempts were blocked", numGoroutines)
	t.Logf("✓ System prevented %.2f GB of malicious memory allocation",
		float64(numGoroutines)*float64(maliciousScriptLength)/(1024*1024*1024))
}
