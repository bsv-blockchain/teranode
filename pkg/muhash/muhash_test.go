package muhash

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestEmptyDigestDeterministic(t *testing.T) {
	require.Equal(t, New().Digest(), New().Digest())
}

func BenchmarkAdd(b *testing.B) {
	m := New()

	var data [40]byte

	b.ReportAllocs()

	i := 0
	for b.Loop() {
		data[0] = byte(i)
		data[1] = byte(i >> 8)
		data[2] = byte(i >> 16)
		m.Add(data[:])
		i++
	}
}

func TestAddThenRemoveReturnsToEmpty(t *testing.T) {
	empty := New().Digest()

	m := New()
	m.Add([]byte("utxo-a"))
	require.NotEqual(t, empty, m.Digest())

	m.Remove([]byte("utxo-a"))
	require.Equal(t, empty, m.Digest())
}

func TestOrderIndependence(t *testing.T) {
	m1 := New()
	m1.Add([]byte("a"))
	m1.Add([]byte("b"))
	m1.Add([]byte("c"))

	m2 := New()
	m2.Add([]byte("c"))
	m2.Add([]byte("a"))
	m2.Add([]byte("b"))

	require.Equal(t, m1.Digest(), m2.Digest())
}

func TestMultisetCountsMatter(t *testing.T) {
	once := New()
	once.Add([]byte("x"))

	twice := New()
	twice.Add([]byte("x"))
	twice.Add([]byte("x"))

	require.NotEqual(t, once.Digest(), twice.Digest())
}

func TestRemoveBeforeAddIsInverse(t *testing.T) {
	m := New()
	m.Remove([]byte("y"))
	m.Add([]byte("y"))
	require.Equal(t, New().Digest(), m.Digest())
}

func TestBytesRoundTrip(t *testing.T) {
	m := New()
	m.Add([]byte("one"))
	m.Add([]byte("two"))
	m.Remove([]byte("one"))

	restored, err := FromBytes(m.Bytes())
	require.NoError(t, err)
	require.Equal(t, m.Digest(), restored.Digest())
}

func TestFromBytesRejectsWrongLength(t *testing.T) {
	_, err := FromBytes([]byte{1, 2, 3})
	require.Error(t, err)
}
