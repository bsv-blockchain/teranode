package netsync

import (
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-chaincfg"
	"github.com/bsv-blockchain/go-wire"
	"github.com/stretchr/testify/require"
)

// hashFrom builds a distinct, stable hash from a single byte.
func hashFrom(b byte) chainhash.Hash {
	var h chainhash.Hash
	h[0] = b

	return h
}

// linkedRun builds count headers that each name the one before it, the first
// naming parent, and returns the headers alongside their hashes. It is the shape
// headerCache.Fill accepts: one internally linked run rooted at a given parent.
func linkedRun(parent chainhash.Hash, count int) ([]*wire.BlockHeader, []chainhash.Hash) {
	headers := make([]*wire.BlockHeader, 0, count)
	hashes := make([]chainhash.Hash, 0, count)

	prev := parent

	for i := 0; i < count; i++ {
		header := wire.NewBlockHeader(1, &prev, &chainhash.Hash{}, 0x207fffff, uint32(i)) //nolint:gosec // a small test nonce
		hash := header.BlockHash()

		headers = append(headers, header)
		hashes = append(hashes, hash)

		prev = hash
	}

	return headers, hashes
}

// Only heights at or below an actually matched checkpoint carry provenance.
//
// This is the property upstream's headerNodeProven asserted against the header
// list (PR 1390's security merge). The list is gone; the cache answers the same
// question, and the reason is unchanged — linkage commits BACKWARDS, so matching
// the pinned hash at height H says everything about the run at or below H and
// nothing at all about the run above it.
func TestHeaderCacheProven_DeniesAboveMatchedCheckpoint(t *testing.T) {
	parent := hashFrom(0x41)
	headers, hashes := linkedRun(parent, 10)

	// Checkpoint at height 5, which is hashes[4] when the run starts at height 1.
	cache := newHeaderCache().WithCheckpoints([]chaincfg.Checkpoint{{Height: 5, Hash: &hashes[4]}})

	require.True(t, cache.Fill(parent, 1, headers))
	require.Equal(t, int32(5), cache.ProvenTo())

	require.True(t, cache.Proven(hashes[4]), "the checkpoint height itself is committed by the hash match")
	require.True(t, cache.Proven(hashes[0]), "heights below the matched checkpoint are committed by linkage to it")
	require.False(t, cache.Proven(hashes[5]), "a header above the matched checkpoint is committed by nothing")
	require.False(t, cache.Proven(hashes[9]), "a far-above header must not be proven either")
	require.False(t, cache.Proven(hashFrom(0xee)), "a hash the cache does not name has no provenance")
}

// No checkpoints means no pinned hash to appeal to, so nothing can be proven —
// upstream's TestHeaderNodeProven_FailsClosedWithoutCheckpoint.
func TestHeaderCacheProven_FailsClosedWithoutCheckpoints(t *testing.T) {
	parent := hashFrom(0x42)
	headers, hashes := linkedRun(parent, 4)

	cache := newHeaderCache()
	require.True(t, cache.Fill(parent, 1, headers))

	require.Zero(t, cache.ProvenTo())

	for _, hash := range hashes {
		require.False(t, cache.Proven(hash))
	}

	var nilCache *headerCache
	require.False(t, nilCache.Proven(hashes[0]), "a nil cache must answer, not panic")
	require.Zero(t, nilCache.ProvenTo())
}

// A checkpoint that the run has not reached yet proves nothing — upstream's
// TestHeaderNodeProven_FailsClosedBeforeMatch. This is the COMMON case on this
// branch and the reason the merge resolution is reported as a speed regression:
// one getheaders reply covers two thousand heights and mainnet's checkpoints are
// tens of thousands apart, so most fills reach no checkpoint at all.
func TestHeaderCacheProven_FailsClosedBeforeMatch(t *testing.T) {
	parent := hashFrom(0x43)
	headers, hashes := linkedRun(parent, 4)

	cache := newHeaderCache().WithCheckpoints([]chaincfg.Checkpoint{{Height: 11_111, Hash: &hashes[0]}})

	require.True(t, cache.Fill(parent, 1, headers))
	require.Zero(t, cache.ProvenTo())

	for _, hash := range hashes {
		require.False(t, cache.Proven(hash))
	}
}

// A run that reaches a checkpoint height carrying the wrong hash is a lie about
// the certified chain: the whole batch goes, and the contents already held are
// left untouched rather than replaced by it.
//
// Upstream disconnected the peer for this in handleHeadersMsg. This branch had
// lost the comparison entirely along with the header list; fillHeaderCache
// restores the disconnect on top of this refusal.
func TestHeaderCache_RefusesRunContradictingCheckpoint(t *testing.T) {
	parent := hashFrom(0x44)
	good, goodHashes := linkedRun(parent, 6)

	cache := newHeaderCache().WithCheckpoints([]chaincfg.Checkpoint{{Height: 3, Hash: &goodHashes[2]}})
	require.True(t, cache.Fill(parent, 1, good))
	require.Equal(t, int32(3), cache.ProvenTo())

	// A different run from the same parent: linked, plausible, and wrong at the
	// pinned height.
	forged, forgedHashes := forgedRun(parent, 6)

	require.False(t, cache.Fill(parent, 1, forged), "a run contradicting a pinned checkpoint hash must be refused")
	require.Equal(t, int32(3), cache.ProvenTo(), "the refusal must not disturb the proof already held")
	require.True(t, cache.Proven(goodHashes[0]), "the previous contents must survive the refusal")
	require.False(t, cache.Proven(forgedHashes[0]), "nothing from the refused run may be named")
}

// forgedRun is linkedRun with a different nonce, so it is a valid linked run from
// the same parent that hashes differently at every height.
func forgedRun(parent chainhash.Hash, count int) ([]*wire.BlockHeader, []chainhash.Hash) {
	headers := make([]*wire.BlockHeader, 0, count)
	hashes := make([]chainhash.Hash, 0, count)

	prev := parent

	for i := 0; i < count; i++ {
		header := wire.NewBlockHeader(1, &prev, &chainhash.Hash{0x01}, 0x207fffff, uint32(1000+i)) //nolint:gosec // a small test nonce
		hash := header.BlockHash()

		headers = append(headers, header)
		hashes = append(hashes, hash)

		prev = hash
	}

	return headers, hashes
}

// Discarding the cache discards the proof with it — the analogue of upstream's
// TestHeaderProvenance_ResetDiscardsProof, which asserted that resetting header
// state cleared verifiedCheckpointHeight along with the list it certified.
func TestHeaderCache_DiscardDropsProof(t *testing.T) {
	parent := hashFrom(0x45)
	headers, hashes := linkedRun(parent, 4)

	cache := newHeaderCache().WithCheckpoints([]chaincfg.Checkpoint{{Height: 2, Hash: &hashes[1]}})
	require.True(t, cache.Fill(parent, 1, headers))
	require.Equal(t, int32(2), cache.ProvenTo())

	cache.Discard()

	require.Zero(t, cache.ProvenTo())
	require.False(t, cache.Proven(hashes[0]))
	require.False(t, cache.Proven(hashes[1]))
}

// A refill that reaches no checkpoint drops the proof the previous fill held.
//
// This is deliberate and is the one direction in which the new structure is
// weaker than upstream's: the proof lives with the contents, so a fresh run that
// cannot reach a pinned hash cannot inherit the old one. Two runs can both link
// to the same tip and diverge above it, so carrying the number across would be
// exactly the forgeable-proof bug upstream's second security commit had to go
// back and fix. The cost is full validation; the alternative is a forged spend.
func TestHeaderCache_ProofDoesNotSurviveARefillThatMissesTheCheckpoint(t *testing.T) {
	parent := hashFrom(0x46)
	first, firstHashes := linkedRun(parent, 4)

	cache := newHeaderCache().WithCheckpoints([]chaincfg.Checkpoint{{Height: 2, Hash: &firstHashes[1]}})
	require.True(t, cache.Fill(parent, 1, first))
	require.Equal(t, int32(2), cache.ProvenTo())

	// The tip has moved to height 2; the next reply covers 3..6 and contains no
	// checkpoint height.
	next, nextHashes := linkedRun(firstHashes[1], 4)
	require.True(t, cache.Fill(firstHashes[1], 3, next))

	require.Zero(t, cache.ProvenTo())
	require.False(t, cache.Proven(nextHashes[0]))
}

// blockOrigin fails closed for anything the cache cannot vouch for, and does not
// panic on a manager built as a struct literal with no cache at all — upstream's
// TestBlockOrigin_FailsClosed.
func TestBlockOrigin_FailsClosed(t *testing.T) {
	sm := &SyncManager{}
	require.False(t, sm.blockOrigin(hashFrom(0x78)).headerProven, "no cache means no provenance")

	sm.headerCache = newHeaderCache()
	require.False(t, sm.blockOrigin(hashFrom(0x79)).headerProven, "an empty cache means no provenance")

	parent := hashFrom(0x47)
	headers, hashes := linkedRun(parent, 3)
	sm.headerCache = newHeaderCache().WithCheckpoints([]chaincfg.Checkpoint{{Height: 2, Hash: &hashes[1]}})
	require.True(t, sm.headerCache.Fill(parent, 1, headers))

	require.True(t, sm.blockOrigin(hashes[0]).headerProven)
	require.False(t, sm.blockOrigin(hashes[2]).headerProven, "above the match is not proven")
	require.False(t, sm.blockOrigin(hashFrom(0x7a)).headerProven, "a hash never named has no provenance")
}
