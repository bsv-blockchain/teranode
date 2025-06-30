package txmap

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSyncedMapLength(t *testing.T) {
	m := NewSyncedMap[string, int]()
	m.Set("key1", 1)
	m.Set("key2", 2)
	assert.Equal(t, 2, m.Length())
}

func TestSyncedMapExists(t *testing.T) {
	m := NewSyncedMap[string, int]()
	m.Set("key1", 1)
	assert.True(t, m.Exists("key1"))
	assert.False(t, m.Exists("key2"))
}

func TestSyncedMapGet(t *testing.T) {
	m := NewSyncedMap[string, int]()
	m.Set("key1", 1)
	val, ok := m.Get("key1")
	assert.True(t, ok)
	assert.Equal(t, 1, val)
}

func TestSyncedMapGetWithLimit(t *testing.T) {
	t.Run("limit 1", func(t *testing.T) {
		m := NewSyncedMap[string, int](1)
		m.Set("key1", 1)
		m.Set("key2", 2)

		_, ok1 := m.Get("key1")
		_, ok2 := m.Get("key2")
		assert.False(t, ok1 && ok2)
	})

	t.Run("limit multiple", func(t *testing.T) {
		m := NewSyncedMap[string, int](2)
		m.SetMulti([]string{"key1", "key2", "key3"}, 1)

		_, ok1 := m.Get("key1")
		_, ok2 := m.Get("key2")
		_, ok3 := m.Get("key3")
		assert.False(t, ok1 && ok2 && ok3)
	})
}

func TestSyncedMapRange(t *testing.T) {
	m := NewSyncedMap[string, int]()
	m.Set("key1", 1)
	m.Set("key2", 2)
	items := m.Range()
	assert.Equal(t, 2, len(items))
	assert.Equal(t, 1, items["key1"])
	assert.Equal(t, 2, items["key2"])
}

func TestSyncedMapKeys(t *testing.T) {
	m := NewSyncedMap[string, int]()
	m.Set("key1", 1)
	m.Set("key2", 2)
	keys := m.Keys()
	assert.Equal(t, 2, len(keys))
	assert.Contains(t, keys, "key1")
	assert.Contains(t, keys, "key2")
}

func TestSyncedMapSetIfNotExists(t *testing.T) {
	m := NewSyncedMap[string, int]()

	val, isSet := m.SetIfNotExists("key1", 1)
	assert.Equal(t, 1, val)
	assert.True(t, isSet) // should be set since it didn't exist before

	val, isSet = m.SetIfNotExists("key1", 2)
	assert.Equal(t, 1, val)
	assert.False(t, isSet) // should not be set since it already exists
}

func TestSyncedMapIterate(t *testing.T) {
	t.Run("continue iteration", func(t *testing.T) {
		m := NewSyncedMap[string, int]()
		m.Set("key1", 1)
		m.Set("key2", 2)

		count := 0

		m.Iterate(func(k string, v int) bool {
			count++
			return true
		})
		assert.Equal(t, 2, count)
	})

	t.Run("stop iteration", func(t *testing.T) {
		m := NewSyncedMap[string, int]()
		m.Set("key1", 1)
		m.Set("key2", 2)

		count := 0

		m.Iterate(func(k string, v int) bool {
			count++
			return false
		})
		assert.Equal(t, 1, count)
	})
}

func TestSyncedMapSetMulti(t *testing.T) {
	m := NewSyncedMap[string, int]()
	m.SetMulti([]string{"key1", "key2"}, 1)
	assert.Equal(t, 1, m.m["key1"])
	assert.Equal(t, 1, m.m["key2"])
}

func TestSyncedMapDelete(t *testing.T) {
	m := NewSyncedMap[string, int]()
	m.Set("key1", 1)
	assert.True(t, m.Delete("key1"))
	assert.False(t, m.Exists("key1"))
}

func TestSyncedMapClear(t *testing.T) {
	m := NewSyncedMap[string, int]()
	m.Set("key1", 1)
	m.Set("key2", 2)
	assert.True(t, m.Clear())
	assert.Equal(t, 0, m.Length())
}

func TestSyncedSliceLength(t *testing.T) {
	t.Run("length not set", func(t *testing.T) {
		s := NewSyncedSlice[int]()
		assert.Equal(t, 0, s.Length())
		s.Append(new(int))
		assert.Equal(t, 1, s.Length())
		assert.Equal(t, 1, s.Size())
	})

	t.Run("length set", func(t *testing.T) {
		s := NewSyncedSlice[int](5)
		assert.Equal(t, 0, s.Length())
		s.Append(new(int))
		assert.Equal(t, 1, s.Length())
		assert.Equal(t, 5, s.Size())
	})
}

func TestSyncedSliceGet(t *testing.T) {
	s := NewSyncedSlice[int]()
	val := 42
	s.Append(&val)
	item, ok := s.Get(0)
	assert.True(t, ok)
	assert.Equal(t, 42, *item)

	_, ok = s.Get(1)
	assert.False(t, ok)
}

func TestSyncedSliceAppend(t *testing.T) {
	s := NewSyncedSlice[int]()
	val := 42
	s.Append(&val)
	assert.Equal(t, 1, s.Length())
	item, ok := s.Get(0)
	assert.True(t, ok)
	assert.Equal(t, 42, *item)
}

func TestSyncedSlicePop(t *testing.T) {
	s := NewSyncedSlice[int]()
	val := 42
	s.Append(&val)
	item, ok := s.Pop()
	assert.True(t, ok)
	assert.Equal(t, 42, *item)
	assert.Equal(t, 0, s.Length())
	_, ok = s.Pop()
	assert.False(t, ok)
}

func TestSyncedSliceShift(t *testing.T) {
	s := NewSyncedSlice[int]()
	val := 42
	s.Append(&val)

	val2 := 43
	s.Append(&val2)

	item, ok := s.Shift()
	assert.True(t, ok)
	assert.Equal(t, 42, *item)
	assert.Equal(t, 1, s.Length())

	item, ok = s.Shift()
	assert.True(t, ok)
	assert.Equal(t, 43, *item)
	assert.Equal(t, 0, s.Length())

	_, ok = s.Shift()
	assert.False(t, ok)
}

func TestSyncedSwissMapLength(t *testing.T) {
	m := NewSyncedSwissMap[string, int](10)
	m.Set("key1", 1)
	m.Set("key2", 2)
	assert.Equal(t, 2, m.Length())
}

func TestSyncedSwissMapGet(t *testing.T) {
	m := NewSyncedSwissMap[string, int](10)
	m.Set("key1", 1)
	val, ok := m.Get("key1")
	assert.True(t, ok)
	assert.Equal(t, 1, val)
}

func TestSyncedSwissMapRange(t *testing.T) {
	m := NewSyncedSwissMap[string, int](10)
	m.Set("key1", 1)
	m.Set("key2", 2)
	items := m.Range()
	assert.Equal(t, 2, len(items))
	assert.Equal(t, 1, items["key1"])
	assert.Equal(t, 2, items["key2"])
}

func TestSyncedSwissMapDelete(t *testing.T) {
	m := NewSyncedSwissMap[string, int](10)
	m.Set("key1", 1)
	assert.True(t, m.Delete("key1"))
	_, ok := m.Get("key1")
	assert.False(t, ok)
}

func TestSyncedSwissMapDeleteBatch(t *testing.T) {
	m := NewSyncedSwissMap[string, int](10)
	m.Set("key1", 1)
	m.Set("key2", 2)
	assert.True(t, m.DeleteBatch([]string{"key1", "key2"}))
	assert.Equal(t, 0, m.Length())
}
