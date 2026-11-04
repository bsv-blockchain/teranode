// Package chainancestrytest builds an Ancestry by hand for tests. Nothing outside a test may
// import it: a hand-built ancestry is a chain answer nobody proved.
package chainancestrytest

import (
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/util/chainancestry"
	"github.com/stretchr/testify/require"
)

// FillID is the id every height gets unless the test names another: a value no test uses for a
// real block, offset by the height so two heights never share it.
func FillID(h uint32) uint32 { return 1_000_000 + h }

// Chain builds an ancestry over [lo, hi] with the ids the test names at the heights it names
// and FillID everywhere else. Its anchor hash is a fixed dummy: no store test reads it.
func Chain(t testing.TB, lo, hi uint32, ids map[uint32]uint32) *chainancestry.Ancestry {
	t.Helper()

	return ChainNotMined(t, lo, hi, ids, nil)
}

// ChainNotMined is Chain with heights whose block reads mined_set false.
func ChainNotMined(t testing.TB, lo, hi uint32, ids map[uint32]uint32, notMined []uint32) *chainancestry.Ancestry {
	t.Helper()

	require.LessOrEqual(t, lo, hi)

	run := make([]uint32, 0, hi-lo+1)

	for h := lo; ; h++ {
		if id, ok := ids[h]; ok {
			run = append(run, id)
		} else {
			run = append(run, FillID(h))
		}

		if h == hi {
			break
		}
	}

	anc, err := chainancestry.New(lo, chainhash.Hash{0xaa}, run, notMined)
	require.NoError(t, err)

	return anc
}
