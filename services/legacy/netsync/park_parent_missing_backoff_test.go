package netsync

import (
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/stretchr/testify/require"
)

// TestParentMissingBackoff is the regression test for a hot retry that had the
// node running flat out and committing nothing.
//
// Measured on mainnet on 2026-09-10 at 14:15. A parked block whose parent was
// genuinely absent failed to commit, kept its blob, and left its parent queued
// for a drain, so the very next turn picked the same block and failed the same
// way: 1,494 of the last 3,000 log lines were that one failure, about seven a
// second, on the goroutine that commits blocks. A second parked block whose
// parent WAS the current tip sat behind it and never got a turn, with 127 more
// stacked behind that.
//
// A parent that is genuinely missing will not appear within a turn, so waiting
// before asking again costs nothing and hands the turn to a block that can be
// committed.
func TestParentMissingBackoff(t *testing.T) {
	newPark := func(t *testing.T) *blockPark {
		t.Helper()

		return newParkWiringHarness(t, true).sm.blockPark
	}

	parent := chainhash.Hash{0xaa}

	entryFor := func(n byte) parkedBlock {
		return parkedBlock{hash: chainhash.Hash{n}, prevBlock: parent, size: 1024, parkedAt: time.Now()}
	}

	t.Run("a block that just failed for a missing parent is passed over", func(t *testing.T) {
		p := newPark(t)

		e := entryFor(0x01)
		e.parentMissingAt = time.Now()
		p.Restore(e)

		_, ok := p.FirstChildFor(parent)
		require.False(t, ok,
			"offering it again in the same second is what spent every turn failing")
	})

	t.Run("and is offered again once the backoff has passed", func(t *testing.T) {
		p := newPark(t)

		e := entryFor(0x02)
		e.parentMissingAt = time.Now().Add(-parentMissingRetryAfter - time.Second)
		p.Restore(e)

		got, ok := p.FirstChildFor(parent)
		require.True(t, ok, "the backoff is a floor on the retry, not an abandonment")
		require.Equal(t, e.hash, got.hash)
	})

	t.Run("a sibling that never failed still gets the turn", func(t *testing.T) {
		// The whole point. One uncommittable block must not starve a committable
		// one that shares its parent.
		p := newPark(t)

		stuck := entryFor(0x03)
		stuck.parentMissingAt = time.Now()
		p.Restore(stuck)

		ready := entryFor(0x04)
		p.Restore(ready)

		got, ok := p.FirstChildFor(parent)
		require.True(t, ok)
		require.Equal(t, ready.hash, got.hash,
			"the drain must reach the block it can commit rather than retrying the one it cannot")
	})

	t.Run("a block that never failed is unaffected", func(t *testing.T) {
		p := newPark(t)
		p.Restore(entryFor(0x05))

		_, ok := p.FirstChildFor(parent)
		require.True(t, ok, "the backoff must only apply to a block that actually failed this way")
	})

	t.Run("a block still being written is still skipped for its own reason", func(t *testing.T) {
		p := newPark(t)

		e := entryFor(0x06)
		e.writing = true
		p.Restore(e)

		_, ok := p.FirstChildFor(parent)
		require.False(t, ok, "the write gate is independent of the backoff and must survive it")
	})
}

// TestParkedBlockFailed_StampsAMissingParent pins the WIRING. The cases above set
// the stamp by hand, so a mutation that never wrote it passed all of them — the
// same trap as a settings field that is declared and never loaded, and the third
// time it has caught me today.
func TestParkedBlockFailed_StampsAMissingParent(t *testing.T) {
	parent := chainhash.Hash{0xba}

	t.Run("a missing parent is stamped, and the block goes back to the park", func(t *testing.T) {
		h := newParkWiringHarness(t, true)

		entry := parkedBlock{hash: chainhash.Hash{0x11}, prevBlock: parent, size: 1024, parkedAt: time.Now()}

		// The error shape the store returns for a parent it does not have, which
		// is what parkCommitFailure grades as the parent being gone.
		err := errors.NewProcessingError("failed to get block header for previous block",
			errors.NewBlockNotFoundError("not found"))

		require.False(t, h.sm.parkedBlockFailed(entry, err),
			"the drain must stop walking this branch")

		got, ok := h.sm.blockPark.Take(entry.hash)
		require.True(t, ok, "a missing parent keeps the blob; the block is already downloaded")
		require.False(t, got.parentMissingAt.IsZero(),
			"without the stamp the next turn picks this same block again, which is the seven-a-second spin")
	})

	t.Run("a local fault is not stamped, because it says nothing about the parent", func(t *testing.T) {
		h := newParkWiringHarness(t, true)

		entry := parkedBlock{hash: chainhash.Hash{0x12}, prevBlock: parent, size: 1024, parkedAt: time.Now()}

		h.sm.parkedBlockFailed(entry, errors.NewStorageError("the store is not answering"))

		got, ok := h.sm.blockPark.Take(entry.hash)
		require.True(t, ok)
		require.True(t, got.parentMissingAt.IsZero(),
			"a busy store is over in seconds and must not delay a block whose parent may be perfectly fine")
	})
}
