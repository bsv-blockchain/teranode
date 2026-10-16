package netsync

import (
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/stretchr/testify/require"
)

// TestAdoptWritten covers registering a block whose body is already on disk.
//
// It is the seam the streaming path needs. When a block is streamed straight
// from the wire to the park's store, the bytes land before the park has any
// record of them, which is the reverse of Admit's order: Admit registers an
// entry and then writes. Recovery after a restart already faces the same
// situation and builds its entries the same way, but it runs once at start with
// the park to itself, so its inline version cannot be called from a peer's read
// loop.
func TestAdoptWritten(t *testing.T) {
	newPark := func(t *testing.T) *blockPark {
		t.Helper()

		h := newParkWiringHarness(t, true)

		return h.sm.blockPark
	}

	entryFor := func(n byte, size int64) parkedBlock {
		return parkedBlock{
			hash:      chainhash.Hash{n},
			prevBlock: chainhash.Hash{0xaa},
			size:      size,
			parkedAt:  time.Now(),
		}
	}

	t.Run("an adopted block is held, charged and findable by its parent", func(t *testing.T) {
		p := newPark(t)
		e := entryFor(0x01, 1<<20)

		require.True(t, p.AdoptWritten(e), "a body on disk with no entry must be adoptable")

		require.Equal(t, 1, p.Len())
		require.True(t, p.Has(e.hash), "the inventory path asks this before re-requesting a block")
		require.Equal(t, int64(1<<20), p.Bytes(), "an adopted block must be charged, or the park's own accounting drifts")

		child, ok := p.FirstChildFor(e.prevBlock)
		require.True(t, ok, "the drain finds work through the parent index, so adoption must populate it")
		require.Equal(t, e.hash, child.hash)
	})

	t.Run("a second adoption of the same block is refused", func(t *testing.T) {
		p := newPark(t)
		e := entryFor(0x03, 4096)

		require.True(t, p.AdoptWritten(e))
		require.False(t, p.AdoptWritten(e), "a re-delivered body must not be charged or indexed twice")
		require.Equal(t, 1, p.Len())
		require.Equal(t, int64(4096), p.Bytes())
	})

	t.Run("a nil park refuses rather than panicking", func(t *testing.T) {
		var p *blockPark

		require.False(t, p.AdoptWritten(entryFor(0x05, 1)),
			"every other park method reads nil as the park being switched off, and this must agree")
	})
}
