package meta

import (
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	subtree "github.com/bsv-blockchain/go-subtree"
	"github.com/stretchr/testify/require"
)

func metaBytesIntoFixtures(t *testing.T) []*Data {
	t.Helper()

	tx := bt.NewTx()
	require.NoError(t, tx.From("0000000000000000000000000000000000000000000000000000000000000001", 0,
		"76a914000000000000000000000000000000000000000088ac", 1000))

	inpoints, err := subtree.NewTxInpointsFromTx(tx)
	require.NoError(t, err)

	return []*Data{
		{Fee: 1000, SizeInBytes: 250, TxInpoints: inpoints},
		{Fee: 0, SizeInBytes: 0, IsCoinbase: true}, // empty inpoints, coinbase
		{Fee: 5, SizeInBytes: 9, Frozen: true, Conflicting: true, Locked: true, InBlock: true, TxInpoints: inpoints},
	}
}

// Test_MetaBytesInto_ParityWithMetaBytes verifies MetaBytesInto produces exactly
// the same bytes as MetaBytes for nil, an empty, and a dirty oversized buffer —
// i.e. recycling a buffer never changes or leaks into the output.
func Test_MetaBytesInto_ParityWithMetaBytes(t *testing.T) {
	for i, d := range metaBytesIntoFixtures(t) {
		want, err := d.MetaBytes()
		require.NoError(t, err)

		gotNil, err := d.MetaBytesInto(nil)
		require.NoError(t, err)
		require.Equal(t, want, gotNil, "fixture %d: nil buffer", i)

		// A dirty, oversized buffer pre-filled with 0xFF must not leak stale
		// bytes (especially into the flags byte) and must reuse the backing.
		dirty := make([]byte, 0, len(want)+64)
		for j := 0; j < cap(dirty); j++ {
			dirty = append(dirty, 0xFF)
		}

		gotDirty, err := d.MetaBytesInto(dirty[:0])
		require.NoError(t, err)
		require.Equal(t, want, gotDirty, "fixture %d: dirty reused buffer", i)
	}
}

// Test_MetaBytesInto_ReuseSavesOuterAlloc verifies that reusing a sufficiently
// sized buffer removes the outer-buffer allocation that MetaBytes makes fresh
// each call. (The separate TxInpoints serialization allocation is in go-subtree
// and out of scope, so this asserts the delta rather than zero.)
func Test_MetaBytesInto_ReuseSavesOuterAlloc(t *testing.T) {
	d := &Data{Fee: 1000, SizeInBytes: 250}

	fresh := testing.AllocsPerRun(100, func() {
		if _, err := d.MetaBytes(); err != nil {
			t.Fatal(err)
		}
	})

	buf := make([]byte, 0, 256)

	reused := testing.AllocsPerRun(100, func() {
		out, err := d.MetaBytesInto(buf[:0])
		if err != nil {
			t.Fatal(err)
		}
		buf = out
	})

	require.Less(t, reused, fresh,
		"reusing a sized buffer must allocate less than MetaBytes (fresh=%v reused=%v)", fresh, reused)
}
