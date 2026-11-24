package protocol

import (
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/stretchr/testify/require"
)

func TestUpdateBlockAvailability_KnownHash(t *testing.T) {
	genesis := testGenesis()
	nc := &nonceCounter{}

	idx, err := NewHeaderIndex(genesis)
	require.NoError(t, err)

	chain := buildChain(t, idx, nc, genesis, 6) // heights 1..6

	atHeight := func(h int) chainhash.Hash { return chain[h-1].BlockHash() }

	// A side chain forking off height 2 gives a second, distinct hash at
	// height 4 (2 -> 3' -> 4'), for a tie-break test that isn't degenerately
	// comparing a hash against itself. Both branches run at the same
	// difficulty, so the two height-4 nodes carry identical chain work.
	side := buildChain(t, idx, nc, chain[1], 2)
	sideHeight4 := side[len(side)-1].BlockHash()
	require.NotEqual(t, atHeight(4), sideHeight4)

	// A heavy branch forking off height 1 and running to height 4. Against
	// the main chain's height-6 tip:
	//   main:  7 * 4_295_032_833                      = 30_065_229_831
	//   heavy: 2 * 4_295_032_833 + 3 * 8_589_934_591  = 34_359_869_439
	// so it is two blocks shorter and still the heavier of the two.
	heavy := buildChainBits(t, idx, nc, chain[0], 3, heavyBits)
	heavyHash := heavy[len(heavy)-1].BlockHash()

	// The state a peer's pindexBestKnownBlock is really in: a snapshot the
	// index handed out, carrying that node's accumulated chain work.
	indexed := func(t *testing.T, hash chainhash.Hash) *HeaderNode {
		t.Helper()

		n, ok := idx.Lookup(hash)
		require.True(t, ok)

		return &n
	}

	tests := []struct {
		name           string
		initialBest    chainhash.Hash // zero means pindexBestKnownBlock starts as nullptr
		announce       chainhash.Hash
		wantBestHeight int32
		wantBestHash   chainhash.Hash
	}{
		{
			name:           "no prior best known block: known hash sets it",
			announce:       atHeight(3),
			wantBestHeight: 3,
			wantBestHash:   atHeight(3),
		},
		{
			name:           "heavier known hash raises pindexBestKnownBlock",
			initialBest:    atHeight(2),
			announce:       atHeight(5),
			wantBestHeight: 5,
			wantBestHash:   atHeight(5),
		},
		{
			name:           "lighter known hash does not lower pindexBestKnownBlock",
			initialBest:    atHeight(5),
			announce:       atHeight(2),
			wantBestHeight: 5,
			wantBestHash:   atHeight(5),
		},
		{
			name:           "equal-work known hash still replaces pindexBestKnownBlock, mirroring the C++ >= compare",
			initialBest:    atHeight(4),
			announce:       sideHeight4,
			wantBestHeight: 4,
			wantBestHash:   sideHeight4,
		},
		{
			name:           "a shorter but heavier announcement raises pindexBestKnownBlock",
			initialBest:    atHeight(6),
			announce:       heavyHash,
			wantBestHeight: 4,
			wantBestHash:   heavyHash,
		},
		{
			name:           "a taller but lighter announcement does not raise pindexBestKnownBlock",
			initialBest:    heavyHash,
			announce:       atHeight(6),
			wantBestHeight: 4,
			wantBestHash:   heavyHash,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			state := newPeerSyncState()

			if tc.initialBest != (chainhash.Hash{}) {
				state.pindexBestKnownBlock = indexed(t, tc.initialBest)
			}

			state.updateBlockAvailability(idx, tc.announce)

			require.NotNil(t, state.pindexBestKnownBlock)
			require.Equal(t, tc.wantBestHeight, state.pindexBestKnownBlock.Height)
			require.Equal(t, tc.wantBestHash, state.pindexBestKnownBlock.Hash)
			require.Equal(t, chainhash.Hash{}, state.hashLastUnknownBlock)
		})
	}
}

func TestUpdateBlockAvailability_UnknownHashParks(t *testing.T) {
	genesis := testGenesis()

	idx, err := NewHeaderIndex(genesis)
	require.NoError(t, err)

	unknown := chainhash.Hash{0xAB}

	state := newPeerSyncState()
	state.updateBlockAvailability(idx, unknown)

	require.Nil(t, state.pindexBestKnownBlock)
	require.Equal(t, unknown, state.hashLastUnknownBlock)
}

func TestUpdateBlockAvailability_ResolvesPendingBeforeHandlingNewAnnouncement(t *testing.T) {
	genesis := testGenesis()
	nc := &nonceCounter{}

	idx, err := NewHeaderIndex(genesis)
	require.NoError(t, err)

	chain := buildChain(t, idx, nc, genesis, 3) // heights 1..3
	nowKnown := chain[2].BlockHash()            // height 3, was unknown when announced

	state := newPeerSyncState()
	state.hashLastUnknownBlock = nowKnown // parked earlier, before AddHeader connected it
	state.pindexBestKnownBlock = nil

	stillUnknown := chainhash.Hash{0xCD}

	// net_processing.cpp UpdateBlockAvailability calls ProcessBlockAvailability
	// FIRST, resolving the pending hashLastUnknownBlock, before evaluating the
	// newly announced hash. If the order were reversed, the newly announced
	// (still unknown) hash would overwrite hashLastUnknownBlock before the
	// pending one got a chance to resolve, and pindexBestKnownBlock would
	// wrongly stay nil.
	state.updateBlockAvailability(idx, stillUnknown)

	require.NotNil(t, state.pindexBestKnownBlock)
	require.Equal(t, int32(3), state.pindexBestKnownBlock.Height)
	require.Equal(t, nowKnown, state.pindexBestKnownBlock.Hash)
	require.Equal(t, stillUnknown, state.hashLastUnknownBlock)
}

func TestProcessBlockAvailability_PromotesWhenLaterKnown(t *testing.T) {
	genesis := testGenesis()
	nc := &nonceCounter{}

	idx, err := NewHeaderIndex(genesis)
	require.NoError(t, err)

	// Build a header but don't feed it to the index yet: announce its hash
	// while the peer is still ahead of what we've indexed.
	futureHeader := childOf(genesis, nc.take())
	futureHash := futureHeader.BlockHash()

	state := newPeerSyncState()
	state.updateBlockAvailability(idx, futureHash)

	require.Nil(t, state.pindexBestKnownBlock)
	require.Equal(t, futureHash, state.hashLastUnknownBlock)

	// A later getheaders round-trip connects that same header into the index.
	connected, err := idx.AddHeader(futureHeader)
	require.NoError(t, err)
	require.True(t, connected)

	state.processBlockAvailability(idx)

	require.NotNil(t, state.pindexBestKnownBlock)
	require.Equal(t, int32(1), state.pindexBestKnownBlock.Height)
	require.Equal(t, futureHash, state.pindexBestKnownBlock.Hash)
	require.Equal(t, chainhash.Hash{}, state.hashLastUnknownBlock)
}

// TestProcessBlockAvailability_ClearsPendingWithoutPromoting pins the C++
// structure where the hashLastUnknownBlock clear sits outside the
// promotion branch: the pending hash resolving to a lower-height node must
// still clear hashLastUnknownBlock, even though it does not raise
// pindexBestKnownBlock. A wrong port that moves the clear inside the
// promotion branch would leave hashLastUnknownBlock set here, and every
// other test in this file has pindexBestKnownBlock == nil at the moment of
// promotion, so none of them would catch that regression.
func TestProcessBlockAvailability_ClearsPendingWithoutPromoting(t *testing.T) {
	genesis := testGenesis()
	nc := &nonceCounter{}

	idx, err := NewHeaderIndex(genesis)
	require.NoError(t, err)

	chain := buildChain(t, idx, nc, genesis, 5) // heights 1..5
	lowerHash := chain[1].BlockHash()           // height 2, resolves but is not better

	best, ok := idx.Lookup(chain[4].BlockHash())
	require.True(t, ok)

	state := newPeerSyncState()
	state.hashLastUnknownBlock = lowerHash
	state.pindexBestKnownBlock = &best

	state.processBlockAvailability(idx)

	require.Equal(t, chainhash.Hash{}, state.hashLastUnknownBlock, "pending hash must clear once resolved, regardless of promotion")
	require.Equal(t, int32(5), state.pindexBestKnownBlock.Height, "pindexBestKnownBlock must not move to a lighter resolved hash")
	require.Equal(t, chain[4].BlockHash(), state.pindexBestKnownBlock.Hash)
}

// TestProcessBlockAvailability_PromotesOnWorkNotHeight pins the
// net_processing.cpp ProcessBlockAvailability compare against nChainWork: the
// pending hash resolving to a shorter branch still raises
// pindexBestKnownBlock when that branch carries more work, and a taller,
// lighter one does not.
func TestProcessBlockAvailability_PromotesOnWorkNotHeight(t *testing.T) {
	genesis := testGenesis()
	nc := &nonceCounter{}

	idx, err := NewHeaderIndex(genesis)
	require.NoError(t, err)

	// light: 5 headers at difficulty 1 → 6 * 4_295_032_833 = 25_770_196_998
	// heavy: 3 headers at heavyBits   → 4_295_032_833 + 3 * 8_589_934_591
	//                                 = 30_064_836_606
	light := buildChainBits(t, idx, nc, genesis, 5, difficulty1Bits)
	heavy := buildChainBits(t, idx, nc, genesis, 3, heavyBits)

	lightTip, ok := idx.Lookup(light[len(light)-1].BlockHash())
	require.True(t, ok)

	heavyTip, ok := idx.Lookup(heavy[len(heavy)-1].BlockHash())
	require.True(t, ok)

	require.Positive(t, heavyTip.ChainWork.Cmp(lightTip.ChainWork))
	require.Less(t, heavyTip.Height, lightTip.Height)

	t.Run("a shorter but heavier resolved hash promotes", func(t *testing.T) {
		best := lightTip

		state := newPeerSyncState()
		state.pindexBestKnownBlock = &best
		state.hashLastUnknownBlock = heavyTip.Hash

		state.processBlockAvailability(idx)

		require.Equal(t, heavyTip.Hash, state.pindexBestKnownBlock.Hash)
		require.Equal(t, chainhash.Hash{}, state.hashLastUnknownBlock)
	})

	t.Run("a taller but lighter resolved hash does not promote", func(t *testing.T) {
		best := heavyTip

		state := newPeerSyncState()
		state.pindexBestKnownBlock = &best
		state.hashLastUnknownBlock = lightTip.Hash

		state.processBlockAvailability(idx)

		require.Equal(t, heavyTip.Hash, state.pindexBestKnownBlock.Hash)
		require.Equal(t, chainhash.Hash{}, state.hashLastUnknownBlock)
	})
}

func TestProcessBlockAvailability_NoOpWhenStillUnknown(t *testing.T) {
	genesis := testGenesis()

	idx, err := NewHeaderIndex(genesis)
	require.NoError(t, err)

	stillUnknown := chainhash.Hash{0x42}

	state := newPeerSyncState()
	state.hashLastUnknownBlock = stillUnknown

	state.processBlockAvailability(idx)

	require.Nil(t, state.pindexBestKnownBlock)
	require.Equal(t, stillUnknown, state.hashLastUnknownBlock)
}

func TestProcessBlockAvailability_NoOpWhenNoPending(t *testing.T) {
	genesis := testGenesis()

	idx, err := NewHeaderIndex(genesis)
	require.NoError(t, err)

	best, ok := idx.Lookup(genesis.BlockHash())
	require.True(t, ok)

	state := newPeerSyncState()
	state.pindexBestKnownBlock = &best

	state.processBlockAvailability(idx)

	require.Equal(t, chainhash.Hash{}, state.hashLastUnknownBlock)
	require.Equal(t, int32(0), state.pindexBestKnownBlock.Height)
}

// ---------------------------------------------------------------------------
// knownBlockSet: pure, table-driven (review round 1, Minor 8 — the type
// backs both peerSyncState.knownBlocks and peerSyncState.knownTxs, each with
// its own capacity since the capacity-as-parameter refactor, and had no
// direct test at either cap before this).
// ---------------------------------------------------------------------------

func TestKnownBlockSet_HasReflectsMarkedMembership(t *testing.T) {
	s := newKnownBlockSet(3)

	hashA := chainhash.Hash{0x01}
	hashB := chainhash.Hash{0x02}

	require.False(t, s.has(hashA), "an unmarked hash must not be known")

	s.mark(hashA)

	require.True(t, s.has(hashA), "a marked hash must be known")
	require.False(t, s.has(hashB), "marking one hash must not affect another")
}

func TestKnownBlockSet_MarkIsIdempotent(t *testing.T) {
	s := newKnownBlockSet(3)
	hash := chainhash.Hash{0x01}

	s.mark(hash)
	s.mark(hash)
	s.mark(hash)

	require.True(t, s.has(hash))
	require.Len(t, s.order, 1, "re-marking an already-known hash must not add a second entry")
}

// TestKnownBlockSet_EvictsOldestAtCapacity pins the FIFO eviction rule
// (mark's own doc comment): once the set holds `capacity` entries, marking
// one more evicts the OLDEST entry, not an arbitrary one, and the evicted
// entry is genuinely forgotten (has reports false for it again).
func TestKnownBlockSet_EvictsOldestAtCapacity(t *testing.T) {
	const capacity = 3

	s := newKnownBlockSet(capacity)

	hashes := make([]chainhash.Hash, capacity+1)
	for i := range hashes {
		hashes[i] = chainhash.Hash{byte(i + 1)}
	}

	for _, h := range hashes[:capacity] {
		s.mark(h)
	}

	for _, h := range hashes[:capacity] {
		require.True(t, s.has(h), "every hash marked up to capacity must still be known")
	}

	// One more mark, past capacity: the OLDEST (hashes[0]) must be evicted,
	// and every other entry marked so far must survive.
	s.mark(hashes[capacity])

	require.False(t, s.has(hashes[0]), "the oldest entry must be evicted once capacity is exceeded")

	for _, h := range hashes[1:] {
		require.True(t, s.has(h), "every entry marked after the evicted one must still be known")
	}
}

// TestKnownBlockSet_IndependentCapacities proves the capacity-as-parameter
// refactor actually isolates two instances of the same type from each
// other's cap — the exact property peerSyncState.knownBlocks and
// peerSyncState.knownTxs depend on (syncstate.go's own knownBlockCap=288 vs
// knownTxCap=wire.MaxInvPerMsg).
func TestKnownBlockSet_IndependentCapacities(t *testing.T) {
	small := newKnownBlockSet(1)
	large := newKnownBlockSet(1000)

	hashA := chainhash.Hash{0x01}
	hashB := chainhash.Hash{0x02}

	small.mark(hashA)
	small.mark(hashB) // evicts hashA at capacity 1

	require.False(t, small.has(hashA))
	require.True(t, small.has(hashB))

	large.mark(hashA)
	large.mark(hashB)

	require.True(t, large.has(hashA), "a set with a larger capacity must not evict on the same schedule as a smaller one")
	require.True(t, large.has(hashB))
}

func TestKnownBlockSet_NilReceiverIsSafe(t *testing.T) {
	var s *knownBlockSet

	require.False(t, s.has(chainhash.Hash{0x01}))
	require.NotPanics(t, func() { s.mark(chainhash.Hash{0x01}) })
}
