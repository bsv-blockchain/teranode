package util

import (
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHashPointersToValues(t *testing.T) {
	t.Run("empty slice", func(t *testing.T) {
		result := HashPointersToValues([]*chainhash.Hash{})
		assert.Empty(t, result)
	})

	t.Run("nil slice", func(t *testing.T) {
		result := HashPointersToValues(nil)
		assert.Empty(t, result)
	})

	t.Run("single hash", func(t *testing.T) {
		hash, err := chainhash.NewHashFromStr("0000000000000000000000000000000000000000000000000000000000000001")
		require.NoError(t, err)

		result := HashPointersToValues([]*chainhash.Hash{hash})
		require.Len(t, result, 1)
		assert.Equal(t, *hash, result[0])
	})

	t.Run("multiple hashes", func(t *testing.T) {
		hash1, err := chainhash.NewHashFromStr("0000000000000000000000000000000000000000000000000000000000000001")
		require.NoError(t, err)
		hash2, err := chainhash.NewHashFromStr("0000000000000000000000000000000000000000000000000000000000000002")
		require.NoError(t, err)
		hash3, err := chainhash.NewHashFromStr("0000000000000000000000000000000000000000000000000000000000000003")
		require.NoError(t, err)

		result := HashPointersToValues([]*chainhash.Hash{hash1, hash2, hash3})
		require.Len(t, result, 3)
		assert.Equal(t, *hash1, result[0])
		assert.Equal(t, *hash2, result[1])
		assert.Equal(t, *hash3, result[2])
	})

	t.Run("nil pointer in slice", func(t *testing.T) {
		hash1, err := chainhash.NewHashFromStr("0000000000000000000000000000000000000000000000000000000000000001")
		require.NoError(t, err)

		result := HashPointersToValues([]*chainhash.Hash{hash1, nil})
		require.Len(t, result, 2)
		assert.Equal(t, *hash1, result[0])
		assert.Equal(t, chainhash.Hash{}, result[1]) // nil converts to zero hash
	})
}

func TestHashValuesToPointers(t *testing.T) {
	t.Run("empty slice", func(t *testing.T) {
		result := HashValuesToPointers([]chainhash.Hash{})
		assert.Empty(t, result)
	})

	t.Run("nil slice", func(t *testing.T) {
		result := HashValuesToPointers(nil)
		assert.Empty(t, result)
	})

	t.Run("single hash", func(t *testing.T) {
		hash, err := chainhash.NewHashFromStr("0000000000000000000000000000000000000000000000000000000000000001")
		require.NoError(t, err)

		result := HashValuesToPointers([]chainhash.Hash{*hash})
		require.Len(t, result, 1)
		assert.Equal(t, hash, result[0])
	})

	t.Run("multiple hashes", func(t *testing.T) {
		hash1, err := chainhash.NewHashFromStr("0000000000000000000000000000000000000000000000000000000000000001")
		require.NoError(t, err)
		hash2, err := chainhash.NewHashFromStr("0000000000000000000000000000000000000000000000000000000000000002")
		require.NoError(t, err)
		hash3, err := chainhash.NewHashFromStr("0000000000000000000000000000000000000000000000000000000000000003")
		require.NoError(t, err)

		result := HashValuesToPointers([]chainhash.Hash{*hash1, *hash2, *hash3})
		require.Len(t, result, 3)
		assert.Equal(t, hash1, result[0])
		assert.Equal(t, hash2, result[1])
		assert.Equal(t, hash3, result[2])
	})
}

func TestRoundTrip(t *testing.T) {
	t.Run("pointers -> values -> pointers", func(t *testing.T) {
		hash1, err := chainhash.NewHashFromStr("0000000000000000000000000000000000000000000000000000000000000001")
		require.NoError(t, err)
		hash2, err := chainhash.NewHashFromStr("0000000000000000000000000000000000000000000000000000000000000002")
		require.NoError(t, err)

		original := []*chainhash.Hash{hash1, hash2}
		values := HashPointersToValues(original)
		roundTrip := HashValuesToPointers(values)

		require.Len(t, roundTrip, 2)
		assert.Equal(t, *original[0], *roundTrip[0])
		assert.Equal(t, *original[1], *roundTrip[1])
	})

	t.Run("values -> pointers -> values", func(t *testing.T) {
		hash1, err := chainhash.NewHashFromStr("0000000000000000000000000000000000000000000000000000000000000001")
		require.NoError(t, err)
		hash2, err := chainhash.NewHashFromStr("0000000000000000000000000000000000000000000000000000000000000002")
		require.NoError(t, err)

		original := []chainhash.Hash{*hash1, *hash2}
		pointers := HashValuesToPointers(original)
		roundTrip := HashPointersToValues(pointers)

		require.Len(t, roundTrip, 2)
		assert.Equal(t, original[0], roundTrip[0])
		assert.Equal(t, original[1], roundTrip[1])
	})
}
