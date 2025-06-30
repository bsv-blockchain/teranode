package txmap

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestConvertSyncMapToUint32Slice(t *testing.T) {
	t.Run("Empty map", func(t *testing.T) {
		var oldBlockIDs sync.Map
		result, hasTransactions := ConvertSyncMapToUint32Slice(&oldBlockIDs)
		assert.Empty(t, result)
		assert.False(t, hasTransactions)
	})

	t.Run("Non-empty map", func(t *testing.T) {
		var oldBlockIDs sync.Map

		oldBlockIDs.Store(uint32(1), struct{}{})
		oldBlockIDs.Store(uint32(2), struct{}{})
		oldBlockIDs.Store(uint32(3), struct{}{})

		result, hasTransactions := ConvertSyncMapToUint32Slice(&oldBlockIDs)
		assert.ElementsMatch(t, []uint32{1, 2, 3}, result)
		assert.True(t, hasTransactions)
	})
}

func TestGenericConvertSyncMapToUint32Slice(t *testing.T) {
	t.Run("Empty map", func(t *testing.T) {
		oldBlockIDs := NewSyncedMap[int, []uint32]()
		result, hasTransactions := ConvertSyncedMapToUint32Slice[int](oldBlockIDs)
		assert.Empty(t, result)
		assert.False(t, hasTransactions)
	})

	t.Run("Non-empty map", func(t *testing.T) {
		oldBlockIDs := NewSyncedMap[int, []uint32]()

		oldBlockIDs.Set(1, []uint32{1})
		oldBlockIDs.Set(2, []uint32{2})
		oldBlockIDs.Set(3, []uint32{3})

		result, hasTransactions := ConvertSyncedMapToUint32Slice[int](oldBlockIDs)
		assert.ElementsMatch(t, []uint32{1, 2, 3}, result)
		assert.True(t, hasTransactions)
	})
}
