package model

import (
	"encoding/binary"
	"fmt"
	"io"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/stretchr/testify/require"
)

const iterTestEntries = 3000

var errWriteFailed = errors.NewProcessingError("write failed")

type testMapFactory struct {
	name string
	new  func() genericMap[UTXOKey, uint64]
}

func testMapFactories() []testMapFactory {
	return []testMapFactory{
		{"splitSwissMap", func() genericMap[UTXOKey, uint64] { return NewSplitSwissMap[UTXOKey, uint64](iterTestEntries) }},
		{"splitGoMap", func() genericMap[UTXOKey, uint64] { return NewSplitGoMap[UTXOKey, uint64](iterTestEntries) }},
		{"swissMap", func() genericMap[UTXOKey, uint64] { return newSwissMap[UTXOKey, uint64](iterTestEntries) }},
		{"goMap", func() genericMap[UTXOKey, uint64] { return NewGoMap[UTXOKey, uint64]() }},
	}
}

func iterTestKey(i uint64) UTXOKey {
	var b [8]byte

	binary.LittleEndian.PutUint64(b[:], i)

	return NewUTXOKey(chainhash.HashH(b[:]), 0)
}

func fillIterTestMap(t *testing.T, m genericMap[UTXOKey, uint64], valueOffset uint64) {
	t.Helper()

	for i := uint64(0); i < iterTestEntries; i++ {
		m.Put(iterTestKey(i), i+valueOffset)
	}
}

func TestMapIter_HonoursStop(t *testing.T) {
	for _, f := range testMapFactories() {
		t.Run(f.name, func(t *testing.T) {
			m := f.new()
			fillIterTestMap(t, m, 0)

			seen := map[UTXOKey]int{}

			m.Iter(func(k UTXOKey, _ uint64) bool {
				seen[k]++
				return false
			})

			require.Len(t, seen, iterTestEntries, "returning false must visit every entry")

			for _, c := range seen {
				require.Equal(t, 1, c)
			}

			for _, k := range []int{1, 2, 7, 500, iterTestEntries} {
				t.Run(fmt.Sprintf("stop after %d", k), func(t *testing.T) {
					visits := 0

					m.Iter(func(UTXOKey, uint64) bool {
						visits++
						return visits >= k
					})

					require.Equal(t, k, visits)
				})
			}
		})
	}
}

func TestMapIsEqual(t *testing.T) {
	eq := func(a, b uint64) bool { return a == b }

	for _, f := range testMapFactories() {
		t.Run(f.name, func(t *testing.T) {
			t.Run("identical maps are equal", func(t *testing.T) {
				a, b := f.new(), f.new()
				fillIterTestMap(t, a, 0)
				fillIterTestMap(t, b, 0)

				ok, msg := a.IsEqual(b, eq)
				require.True(t, ok, msg)
			})

			t.Run("maps differing in every value are not equal", func(t *testing.T) {
				a, b := f.new(), f.new()
				fillIterTestMap(t, a, 0)
				fillIterTestMap(t, b, 1)

				ok, msg := a.IsEqual(b, eq)
				require.False(t, ok)
				require.Contains(t, msg, "Values are different")
			})

			t.Run("maps differing in a single value are not equal", func(t *testing.T) {
				a, b := f.new(), f.new()
				fillIterTestMap(t, a, 0)
				fillIterTestMap(t, b, 0)
				b.Put(iterTestKey(iterTestEntries-1), 999999)

				ok, _ := a.IsEqual(b, eq)
				require.False(t, ok)
			})

			t.Run("maps differing in many entries are not equal", func(t *testing.T) {
				a, b := f.new(), f.new()
				fillIterTestMap(t, a, 0)
				fillIterTestMap(t, b, 0)

				for i := uint64(0); i < iterTestEntries; i += 3 {
					b.Put(iterTestKey(i), i+1)
				}

				ok, _ := a.IsEqual(b, eq)
				require.False(t, ok)
			})

			t.Run("same length with different keys are not equal", func(t *testing.T) {
				a, b := f.new(), f.new()
				fillIterTestMap(t, a, 0)

				for i := uint64(iterTestEntries); i < 2*iterTestEntries; i++ {
					b.Put(iterTestKey(i), i)
				}

				ok, _ := a.IsEqual(b, eq)
				require.False(t, ok)
			})
		})
	}
}

// failOnceWriter fails exactly one Write call and succeeds on every other.
type failOnceWriter struct {
	calls  int
	failAt int
}

func (w *failOnceWriter) Write(p []byte) (int, error) {
	w.calls++
	if w.calls == w.failAt {
		return 0, errWriteFailed
	}

	return len(p), nil
}

// TestWrite_ReportsTheWriteError checks that UTXOSet.Write and UTXOMap.Write
// return the write error itself, wherever it happens: a key or a value write, in
// the middle or on the last UTXO. The write callback records the error and
// returns stop; if the split map kept iterating after stop, the next successful
// write would reset it (UTXOSet.Write then returned nil, UTXOMap.Write a count
// mismatch instead of the cause).
func TestWrite_ReportsTheWriteError(t *testing.T) {
	newMap := func() UTXOMap {
		m := newUTXOMap()

		for i := uint64(0); i < 200; i++ {
			m.Put(iterTestKey(i), &UTXOValue{Value: i, Script: []byte{0x51}})
		}

		return m
	}

	writers := map[string]func(w io.Writer) error{
		"UTXOSet": func(w io.Writer) error {
			us := &UTXOSet{Current: newMap()}
			return us.Write(w)
		},
		"UTXOMap": func(w io.Writer) error {
			m := newMap()
			return m.Write(w)
		},
	}

	for name, write := range writers {
		t.Run(name, func(t *testing.T) {
			all := &failOnceWriter{}
			require.NoError(t, write(all))
			require.Greater(t, all.calls, 10)

			// early, middle, a neighbouring call (key vs value), and the last write
			for _, failAt := range []int{3, all.calls / 2, all.calls/2 + 1, all.calls} {
				err := write(&failOnceWriter{failAt: failAt})
				// the cause itself: teranode's errors.Is matches any ProcessingError
				require.ErrorContains(t, err, errWriteFailed.Error(), "write call %d of %d", failAt, all.calls)
			}
		})
	}
}
