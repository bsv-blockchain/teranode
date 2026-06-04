package expiringmap

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestExpiringMap_WithMaxSize_CapEnforced(t *testing.T) {
	m := New[string, int](time.Hour).WithMaxSize(3)
	defer m.Stop()

	m.Set("a", 1)
	time.Sleep(2 * time.Millisecond)
	m.Set("b", 2)
	time.Sleep(2 * time.Millisecond)
	m.Set("c", 3)
	time.Sleep(2 * time.Millisecond)
	m.Set("d", 4)
	time.Sleep(2 * time.Millisecond)
	m.Set("e", 5)

	require.LessOrEqual(t, m.Len(), 3)

	_, aExists := m.Get("a")
	_, bExists := m.Get("b")
	_, cExists := m.Get("c")
	_, dExists := m.Get("d")
	_, eExists := m.Get("e")
	require.False(t, aExists, "oldest entry 'a' should have been evicted")
	require.False(t, bExists, "second-oldest entry 'b' should have been evicted")
	require.True(t, cExists, "middle entry 'c' should still be present")
	require.True(t, dExists, "recent entry 'd' should still be present")
	require.True(t, eExists, "newest entry 'e' should still be present")
}

func TestExpiringMap_WithMaxSize_UpdateExistingKey(t *testing.T) {
	m := New[string, int](time.Hour).WithMaxSize(3)
	defer m.Stop()

	m.Set("a", 1)
	m.Set("b", 2)
	m.Set("c", 3)
	require.Equal(t, 3, m.Len())

	m.Set("b", 22)
	require.Equal(t, 3, m.Len())

	v, ok := m.Get("a")
	require.True(t, ok)
	require.Equal(t, 1, v)

	v, ok = m.Get("b")
	require.True(t, ok)
	require.Equal(t, 22, v)

	v, ok = m.Get("c")
	require.True(t, ok)
	require.Equal(t, 3, v)
}

func TestExpiringMap_WithMaxSize_EvictionChannelFires(t *testing.T) {
	ch := make(chan []int, 4)
	m := New[string, int](time.Hour).WithEvictionChannel(ch).WithMaxSize(3)
	defer m.Stop()

	m.Set("a", 1)
	time.Sleep(2 * time.Millisecond)
	m.Set("b", 2)
	time.Sleep(2 * time.Millisecond)
	m.Set("c", 3)
	time.Sleep(2 * time.Millisecond)
	m.Set("d", 4)

	select {
	case items := <-ch:
		require.Len(t, items, 1)
		require.Equal(t, 1, items[0])
	case <-time.After(time.Second):
		t.Fatal("expected eviction-channel notification on cap eviction")
	}
}

func TestExpiringMap_WithMaxSize_EvictionFunctionFires(t *testing.T) {
	var evictedKey string
	var evictedValue int

	m := New[string, int](time.Hour).
		WithEvictionFunction(func(k string, v int) bool {
			evictedKey = k
			evictedValue = v
			return true
		}).
		WithMaxSize(2)
	defer m.Stop()

	m.Set("a", 1)
	time.Sleep(2 * time.Millisecond)
	m.Set("b", 2)
	time.Sleep(2 * time.Millisecond)
	m.Set("c", 3)

	require.Equal(t, "a", evictedKey)
	require.Equal(t, 1, evictedValue)
	require.Equal(t, 2, m.Len())
}

func TestExpiringMap_WithMaxSize_EvictionFunctionVeto(t *testing.T) {
	vetoKey := "a"
	m := New[string, int](time.Hour).
		WithEvictionFunction(func(k string, v int) bool {
			return k != vetoKey
		}).
		WithMaxSize(2)
	defer m.Stop()

	m.Set("a", 1)
	time.Sleep(2 * time.Millisecond)
	m.Set("b", 2)
	time.Sleep(2 * time.Millisecond)
	m.Set("c", 3)

	_, aExists := m.Get("a")
	require.True(t, aExists, "vetoed entry should not be evicted")
	require.LessOrEqual(t, m.Len(), 2, "cap must hold even when eviction is vetoed")

	_, cExists := m.Get("c")
	require.False(t, cExists, "new entry must be dropped when eviction is vetoed")

	vetoKey = ""
	time.Sleep(2 * time.Millisecond)
	m.Set("d", 4)

	require.LessOrEqual(t, m.Len(), 2, "cap must hold after veto-then-non-veto sequence")
	_, dExists := m.Get("d")
	require.True(t, dExists, "non-vetoed insert after a veto should succeed")
}

func TestExpiringMap_WithMaxSize_ZeroIsUnbounded(t *testing.T) {
	m := New[int, int](time.Hour).WithMaxSize(0)
	defer m.Stop()

	for i := 0; i < 100; i++ {
		m.Set(i, i)
	}

	require.Equal(t, 100, m.Len())
}

func TestExpiringMap_NoMaxSize_DefaultUnbounded(t *testing.T) {
	m := New[int, int](time.Hour)
	defer m.Stop()

	for i := 0; i < 100; i++ {
		m.Set(i, i)
	}

	require.Equal(t, 100, m.Len())
}
