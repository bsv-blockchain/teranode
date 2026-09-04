package protocol

import (
	"bytes"
	"encoding/hex"
	"math/big"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-chaincfg"
	"github.com/bsv-blockchain/go-wire"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/stretchr/testify/require"
)

// testEasyBits encodes a target of 0x7fffff * 2^232 (just under 2^255), the
// regtest-style "anything hashes" difficulty. Grinding a header nonce against
// it takes a couple of tries.
const testEasyBits uint32 = 0x207fffff

// testHardBits is the mainnet genesis difficulty (target 0xffff * 2^208). No
// header this test file grinds will ever meet it, which is how the PoW-failure
// rows are built.
const testHardBits uint32 = 0x1d00ffff

// testTarget decodes nBits the same way SVNode's arith_uint256::SetCompact
// does, written out here rather than reusing the production helper so the
// tests pin the rule and not the implementation.
func testTarget(bits uint32) *big.Int {
	exponent := bits >> 24
	mantissa := int64(bits & 0x007fffff)

	if exponent <= 3 {
		return big.NewInt(mantissa >> (8 * (3 - exponent)))
	}

	return new(big.Int).Lsh(big.NewInt(mantissa), uint(8*(exponent-3)))
}

// testMeetsTarget reports whether the header hash, read as a big-endian
// number, is at or below the target its own nBits claims.
func testMeetsTarget(h *wire.BlockHeader) bool {
	hash := h.BlockHash()

	be := make([]byte, len(hash))
	for i := range hash {
		be[len(hash)-1-i] = hash[i]
	}

	return new(big.Int).SetBytes(be).Cmp(testTarget(h.Bits)) <= 0
}

// testPowLimit is the highest target the test chain params allow: 2^255 - 1,
// the regtest powLimit. It is above testEasyBits' target, so a ground header
// passes the range check.
func testPowLimit() *big.Int {
	return new(big.Int).Sub(new(big.Int).Lsh(big.NewInt(1), 255), big.NewInt(1))
}

// syncTestParams returns mainnet params with the regtest powLimit and the
// given checkpoints, so tests can mine headers cheaply while still exercising
// the non-regtest sync-candidate branch.
func syncTestParams(checkpoints []chaincfg.Checkpoint) *chaincfg.Params {
	params := chaincfg.MainNetParams
	params.PowLimit = testPowLimit()
	params.Checkpoints = checkpoints

	return &params
}

func syncGenesis() *wire.BlockHeader {
	zero := chainhash.Hash{}
	h := wire.NewBlockHeader(1, &zero, &zero, testEasyBits, 0)

	for !testMeetsTarget(h) {
		h.Nonce++
	}

	return h
}

// minedChild grinds a child of parent until it meets its own claimed target.
// salt keeps siblings of one parent distinct.
func minedChild(parent *wire.BlockHeader, bits, salt uint32) *wire.BlockHeader {
	prevHash := parent.BlockHash()
	merkle := chainhash.Hash{}
	merkle[0] = byte(salt)
	merkle[1] = byte(salt >> 8)

	h := wire.NewBlockHeader(1, &prevHash, &merkle, bits, 0)

	for !testMeetsTarget(h) {
		h.Nonce++
	}

	return h
}

// failingChild grinds a child of parent until its hash does NOT meet the
// target its nBits claims, giving a deterministic "high-hash" header.
func failingChild(parent *wire.BlockHeader, salt uint32) *wire.BlockHeader {
	prevHash := parent.BlockHash()
	merkle := chainhash.Hash{}
	merkle[0] = byte(salt)

	h := wire.NewBlockHeader(1, &prevHash, &merkle, testHardBits, 0)

	for testMeetsTarget(h) {
		h.Nonce++
	}

	return h
}

// minedRun returns count headers extending from, each with valid PoW, without
// adding them to any index.
func minedRun(from *wire.BlockHeader, count int, salt uint32) []*wire.BlockHeader {
	run := make([]*wire.BlockHeader, 0, count)
	prev := from

	for i := 0; i < count; i++ {
		h := minedChild(prev, testEasyBits, salt+uint32(i)) //nolint:gosec // test-only salt
		run = append(run, h)
		prev = h
	}

	return run
}

// syncFixture is one genesis-rooted index plus the headers already in it.
type syncFixture struct {
	genesis *wire.BlockHeader
	idx     *HeaderIndex
	chain   []*wire.BlockHeader // chain[i] has height i+1
}

// newSyncFixture builds an index whose tip is at localHeight.
func newSyncFixture(t *testing.T, localHeight int) *syncFixture {
	t.Helper()

	genesis := syncGenesis()

	idx, err := NewHeaderIndex(genesis)
	require.NoError(t, err)

	chain := minedRun(genesis, localHeight, 1)
	for _, h := range chain {
		connected, addErr := idx.AddHeader(h)
		require.NoError(t, addErr)
		require.True(t, connected)
	}

	return &syncFixture{genesis: genesis, idx: idx, chain: chain}
}

func (f *syncFixture) tip() *wire.BlockHeader {
	if len(f.chain) == 0 {
		return f.genesis
	}

	return f.chain[len(f.chain)-1]
}

func fullNodePeer(addr string) *SyncPeer {
	return NewSyncPeer(addr, wire.SFNodeNetwork, newPeerSyncState())
}

// requireGetHeaders unwraps the single expected getheaders message.
func requireGetHeaders(t *testing.T, msgs []wire.Message) *wire.MsgGetHeaders {
	t.Helper()

	require.Len(t, msgs, 1)

	gh, ok := msgs[0].(*wire.MsgGetHeaders)
	require.True(t, ok, "expected a getheaders message, got %T", msgs[0])

	return gh
}

func locatorValues(gh *wire.MsgGetHeaders) []chainhash.Hash {
	out := make([]chainhash.Hash, 0, len(gh.BlockLocatorHashes))
	for _, h := range gh.BlockLocatorHashes {
		out = append(out, *h)
	}

	return out
}

// TestHeaderSync_InitialGetHeaders pins the initial getheaders that
// net_processing.cpp SendMessages sends when a sync candidate completes the
// handshake, with the legacy checkpoint hashStop layered on top.
func TestHeaderSync_InitialGetHeaders(t *testing.T) {
	t.Run("fresh peer below a checkpoint gets the pprev locator and the checkpoint hashStop", func(t *testing.T) {
		f := newSyncFixture(t, 5)

		// The checkpoint sits above our tip, so headers-first mode applies.
		cpHash := chainhash.Hash{0xC0}
		hs, err := NewHeaderSync(HeaderSyncConfig{
			Index:  f.idx,
			Params: syncTestParams([]chaincfg.Checkpoint{{Height: 1000, Hash: &cpHash}}),
		})
		require.NoError(t, err)

		peer := fullNodePeer("1.2.3.4:8333")

		gh := requireGetHeaders(t, hs.PeerEstablished(peer))

		// net_processing.cpp SendMessages: "If possible, start at the block
		// preceding the currently best known header" — the locator is built
		// from our tip's parent, not the tip.
		parent := f.chain[len(f.chain)-2].BlockHash()
		require.Equal(t, f.idx.LocatorFrom(parent), locatorValues(gh))

		// Legacy manager.go startSync: while headers-first, getheaders carries
		// the next checkpoint hash as hashStop.
		require.Equal(t, cpHash, gh.HashStop)
		require.True(t, hs.IsHeadersFirstMode())
		require.True(t, peer.State.fSyncStarted)
	})

	t.Run("tip past the final checkpoint gets a zero hashStop and no headers-first mode", func(t *testing.T) {
		f := newSyncFixture(t, 5)

		cpHash := f.chain[1].BlockHash() // height 2, below our tip
		hs, err := NewHeaderSync(HeaderSyncConfig{
			Index:  f.idx,
			Params: syncTestParams([]chaincfg.Checkpoint{{Height: 2, Hash: &cpHash}}),
		})
		require.NoError(t, err)

		gh := requireGetHeaders(t, hs.PeerEstablished(fullNodePeer("1.2.3.4:8333")))

		require.Equal(t, chainhash.Hash{}, gh.HashStop)
		require.False(t, hs.IsHeadersFirstMode())
	})

	t.Run("genesis tip has no parent so the locator is the tip locator", func(t *testing.T) {
		f := newSyncFixture(t, 0)

		hs, err := NewHeaderSync(HeaderSyncConfig{Index: f.idx, Params: syncTestParams(nil)})
		require.NoError(t, err)

		gh := requireGetHeaders(t, hs.PeerEstablished(fullNodePeer("1.2.3.4:8333")))

		require.Equal(t, []chainhash.Hash{f.genesis.BlockHash()}, locatorValues(gh))
	})

	t.Run("only one peer starts header sync at a time", func(t *testing.T) {
		f := newSyncFixture(t, 3)

		// An old tip, so the 24 hour relaxation is off and the single-slot rule
		// is the only thing under test here. See
		// TestHeaderSync_TwentyFourHourSyncRelaxation for the other arm.
		hs, err := NewHeaderSync(HeaderSyncConfig{
			Index:        f.idx,
			Params:       syncTestParams(nil),
			AdjustedTime: fixtureClock(t, f, 25*time.Hour),
		})
		require.NoError(t, err)

		first := fullNodePeer("1.2.3.4:8333")
		second := fullNodePeer("5.6.7.8:8333")

		require.Len(t, hs.PeerEstablished(first), 1)

		// net_processing.cpp SendMessages: nSyncStarted == 0 gates the initial
		// getheaders, so the second peer is not asked.
		require.Nil(t, hs.PeerEstablished(second))
		require.False(t, second.State.fSyncStarted)

		// A repeat event for the same peer must not re-ask either
		// (state.fSyncStarted).
		require.Nil(t, hs.PeerEstablished(first))

		// FinalizeNode: losing the sync peer frees the slot.
		hs.PeerDisconnected(first)
		require.False(t, first.State.fSyncStarted)

		require.Len(t, hs.PeerEstablished(second), 1)
		require.True(t, second.State.fSyncStarted)
	})

	t.Run("sync candidate rules", func(t *testing.T) {
		regtestParams := func() *chaincfg.Params {
			params := chaincfg.RegressionNetParams
			params.PowLimit = testPowLimit()
			params.Checkpoints = nil

			return &params
		}

		tests := []struct {
			name          string
			params        *chaincfg.Params
			allowNonLocal bool
			addr          string
			services      wire.ServiceFlag
			wantCandidate bool
		}{
			// legacy manager.go isSyncCandidate: outside regtest the peer must
			// serve SFNodeNetwork.
			{"full node is a candidate", syncTestParams(nil), false, "1.2.3.4:8333", wire.SFNodeNetwork, true},
			{"non-full node is not a candidate", syncTestParams(nil), false, "1.2.3.4:8333", 0, false},
			// legacy manager.go isSyncCandidate: in regtest the service flag is
			// not required, but the peer must be local unless
			// legacy_allowSyncCandidateFromLocalPeers is set.
			{"regtest localhost candidate without the service flag", regtestParams(), false, "127.0.0.1:18444", 0, true},
			{"regtest remote peer rejected", regtestParams(), false, "10.0.0.9:18444", wire.SFNodeNetwork, false},
			{"regtest remote peer allowed by setting", regtestParams(), true, "10.0.0.9:18444", 0, true},
			{"regtest unparsable address rejected", regtestParams(), false, "not-an-address", wire.SFNodeNetwork, false},
		}

		for _, tc := range tests {
			t.Run(tc.name, func(t *testing.T) {
				f := newSyncFixture(t, 2)

				hs, err := NewHeaderSync(HeaderSyncConfig{
					Index:                            f.idx,
					Params:                           tc.params,
					AllowSyncCandidateFromLocalPeers: tc.allowNonLocal,
				})
				require.NoError(t, err)

				peer := NewSyncPeer(tc.addr, tc.services, newPeerSyncState())
				msgs := hs.PeerEstablished(peer)

				if tc.wantCandidate {
					require.Len(t, msgs, 1)
					require.True(t, peer.State.fSyncStarted)
				} else {
					require.Nil(t, msgs)
					require.False(t, peer.State.fSyncStarted)
				}
			})
		}
	})

	t.Run("regtest never enters headers-first mode", func(t *testing.T) {
		f := newSyncFixture(t, 2)

		params := chaincfg.RegressionNetParams
		params.PowLimit = testPowLimit()

		cpHash := chainhash.Hash{0xC0}
		params.Checkpoints = []chaincfg.Checkpoint{{Height: 1000, Hash: &cpHash}}

		hs, err := NewHeaderSync(HeaderSyncConfig{Index: f.idx, Params: &params})
		require.NoError(t, err)

		gh := requireGetHeaders(t, hs.PeerEstablished(NewSyncPeer("127.0.0.1:18444", 0, newPeerSyncState())))

		// legacy manager.go startSync: "regression test mode does not support
		// the headers-first approach".
		require.False(t, hs.IsHeadersFirstMode())
		require.Equal(t, chainhash.Hash{}, gh.HashStop)
	})

	t.Run("disabled checkpoints leave headers-first mode off", func(t *testing.T) {
		f := newSyncFixture(t, 2)

		cpHash := chainhash.Hash{0xC0}
		hs, err := NewHeaderSync(HeaderSyncConfig{
			Index:              f.idx,
			Params:             syncTestParams([]chaincfg.Checkpoint{{Height: 1000, Hash: &cpHash}}),
			DisableCheckpoints: true,
		})
		require.NoError(t, err)

		gh := requireGetHeaders(t, hs.PeerEstablished(fullNodePeer("1.2.3.4:8333")))

		require.False(t, hs.IsHeadersFirstMode())
		require.Equal(t, chainhash.Hash{}, gh.HashStop)
	})
}

// fixtureClock returns an AdjustedTime function reading age after the
// fixture tip's own block time. wire.NewBlockHeader stamps every fixture
// header with time.Now(), so the fixture cannot place its tip on one side of
// the 24 hour line by itself; the injected clock is the knob instead, and it
// is the SAME clock the time-too-new check reads.
func fixtureClock(t *testing.T, f *syncFixture, age time.Duration) func() int64 {
	t.Helper()

	tip, ok := f.idx.Lookup(f.tip().BlockHash())
	require.True(t, ok)

	now := tip.Time + int64(age/time.Second)

	return func() int64 { return now }
}

// TestHeaderSync_TwentyFourHourSyncRelaxation pins net_processing.cpp
// SendBlockSync (net_processing.cpp:5191-5196): header sync may start with
// ADDITIONAL peers once the best header is within 24 hours of the adjusted
// time. Phase 2 skipped it because HeaderIndex exposed no header timestamp.
func TestHeaderSync_TwentyFourHourSyncRelaxation(t *testing.T) {
	t.Run("a tip within 24 hours lets a second candidate start header sync", func(t *testing.T) {
		f := newSyncFixture(t, 3)

		hs, err := NewHeaderSync(HeaderSyncConfig{
			Index:        f.idx,
			Params:       syncTestParams(nil),
			AdjustedTime: fixtureClock(t, f, 23*time.Hour),
		})
		require.NoError(t, err)

		first := fullNodePeer("1.2.3.4:8333")
		second := fullNodePeer("5.6.7.8:8333")

		require.Len(t, hs.PeerEstablished(first), 1)

		gh := requireGetHeaders(t, hs.PeerEstablished(second))

		require.True(t, second.State.fSyncStarted)
		require.Equal(t, 2, hs.nSyncStarted)

		// No round is running, so the additional peer gets the plain
		// net_processing.cpp getheaders rather than a checkpoint hashStop.
		require.False(t, hs.IsHeadersFirstMode())
		require.Equal(t, chainhash.Hash{}, gh.HashStop)
	})

	t.Run("a tip older than 24 hours keeps the single sync slot", func(t *testing.T) {
		f := newSyncFixture(t, 3)

		hs, err := NewHeaderSync(HeaderSyncConfig{
			Index:        f.idx,
			Params:       syncTestParams(nil),
			AdjustedTime: fixtureClock(t, f, 25*time.Hour),
		})
		require.NoError(t, err)

		first := fullNodePeer("1.2.3.4:8333")
		second := fullNodePeer("5.6.7.8:8333")

		require.Len(t, hs.PeerEstablished(first), 1)

		require.Nil(t, hs.PeerEstablished(second))
		require.False(t, second.State.fSyncStarted)
		require.Equal(t, 1, hs.nSyncStarted)
	})

	t.Run("a headers-first round keeps the single slot however fresh the tip is", func(t *testing.T) {
		f := newSyncFixture(t, 3)

		// A checkpoint above our tip puts the machine in headers-first mode,
		// which is the state Phase 2 Task 5's round invariants are written
		// against: exactly one peer drives the round.
		cpHash := chainhash.Hash{0xC0}
		hs, err := NewHeaderSync(HeaderSyncConfig{
			Index:        f.idx,
			Params:       syncTestParams([]chaincfg.Checkpoint{{Height: 100000, Hash: &cpHash}}),
			AdjustedTime: fixtureClock(t, f, time.Hour),
		})
		require.NoError(t, err)

		first := fullNodePeer("1.2.3.4:8333")
		second := fullNodePeer("5.6.7.8:8333")

		require.Len(t, hs.PeerEstablished(first), 1)
		require.True(t, hs.IsHeadersFirstMode())

		// The tip is one hour old, so the 24 hour test alone would admit this
		// peer. The round is what refuses it.
		require.Nil(t, hs.PeerEstablished(second))
		require.False(t, second.State.fSyncStarted)
		require.Equal(t, 1, hs.nSyncStarted)

		// The round the first peer holds is untouched: a second peer must not
		// re-seed nextCheckpoint, headersFirstMode or the round anchor.
		require.True(t, hs.IsHeadersFirstMode())
		require.True(t, first.State.fSyncStarted)
	})

	t.Run("a checkpointless network never runs a round, so the 24 hour test is the only gate", func(t *testing.T) {
		// regtest and tstn ship no checkpoints. headersFirstMode is only ever
		// set true when nextCheckpoint is non-nil (PeerEstablished), and
		// findNextHeaderCheckpoint reads hs.checkpoints, which is empty both
		// for params carrying no checkpoints and for DisableCheckpoints. So
		// there is no round on such a network for the relaxation to disturb,
		// and two header syncs are exactly what net_processing.cpp does.
		f := newSyncFixture(t, 3)

		hs, err := NewHeaderSync(HeaderSyncConfig{
			Index:        f.idx,
			Params:       syncTestParams(nil),
			AdjustedTime: fixtureClock(t, f, time.Hour),
		})
		require.NoError(t, err)

		require.Empty(t, hs.checkpoints)
		require.Nil(t, hs.nextCheckpoint, "an empty checkpoint list cannot seed a next checkpoint")

		require.Len(t, hs.PeerEstablished(fullNodePeer("1.2.3.4:8333")), 1)

		require.Nil(t, hs.nextCheckpoint,
			"nothing a peer does can produce a checkpoint that is not in the params")
		require.False(t, hs.IsHeadersFirstMode(),
			"no checkpoint means no headers-first round on any tip")

		require.Len(t, hs.PeerEstablished(fullNodePeer("5.6.7.8:8333")), 1)
		require.Equal(t, 2, hs.nSyncStarted)

		// The mode is still off with two peers syncing, which is what makes
		// them plain net_processing.cpp header peers rather than two peers
		// inside one round.
		require.False(t, hs.IsHeadersFirstMode())
	})

	// Two peers may hold fSyncStarted only while headersFirstMode is off, so
	// neither is a roundOwner in acceptHeaders. That is what keeps the shared
	// roundAnchorHeight and the ErrHeadersNoProgress terminator out of reach of
	// a second header source: both live on the machine, not on the peer, and
	// both are gated on the mode. Without that, the second peer's batch would
	// walk the anchor up and the first peer's honest continuation would be read
	// as a replay and disconnected.
	t.Run("parallel header syncs cannot make each other look non-progressing", func(t *testing.T) {
		f := newSyncFixture(t, 3)

		hs, err := NewHeaderSync(HeaderSyncConfig{
			Index:        f.idx,
			Params:       syncTestParams(nil),
			AdjustedTime: fixtureClock(t, f, time.Hour),
		})
		require.NoError(t, err)

		first := fullNodePeer("1.2.3.4:8333")
		second := fullNodePeer("5.6.7.8:8333")

		require.Len(t, hs.PeerEstablished(first), 1)

		// The first peer seeds the anchor from its own start locator, as it
		// always has. Nothing reads it outside a round, but pinning it is what
		// makes the assertions below mean "unchanged" rather than "unset".
		seeded := hs.roundAnchorHeight
		require.Equal(t, int32(2), seeded, "the initial getheaders went out from the tip's parent")

		require.Len(t, hs.PeerEstablished(second), 1)
		require.Equal(t, 2, hs.nSyncStarted)
		require.False(t, hs.IsHeadersFirstMode(), "the round guard is what admitted the second peer")
		require.Equal(t, seeded, hs.roundAnchorHeight,
			"an additional near-tip peer must not re-seed the round state")

		batch := minedRun(f.tip(), 3, 7300)

		// The first peer carries the chain forward. acceptHeaders raises the
		// anchor only for a roundOwner, and neither peer is one here, so there
		// is nothing for a second header source to walk up.
		_, score, err := hs.OnHeaders(first, headersMsg(batch))
		require.NoError(t, err)
		require.Zero(t, score)
		require.Equal(t, seeded, hs.roundAnchorHeight)

		// The second peer then serves exactly the same headers. Every one is
		// already indexed and none reaches past where the first peer left off:
		// nothing new, and not above the anchor. That is precisely the shape
		// the terminator refuses inside a round — see
		// "a repeated batch of known headers ends the round instead of
		// looping", where this same sequence produces ErrHeadersNoProgress.
		// Outside a round it
		// must be tolerated, because the peer is answering our own getheaders
		// honestly.
		_, score, err = hs.OnHeaders(second, headersMsg(batch))
		require.NoError(t, err, "a parallel header source must not be disconnected as a replay")
		require.Zero(t, score)

		require.True(t, first.State.fSyncStarted, "neither peer may lose its slot to the other")
		require.True(t, second.State.fSyncStarted)
		require.Equal(t, seeded, hs.roundAnchorHeight,
			"no round, so no anchor either header source could move")
	})
}

// startedSync returns a machine that has already sent its initial getheaders
// to peer, so OnHeaders rows start from the state a real batch arrives in.
func startedSync(t *testing.T, f *syncFixture, checkpoints []chaincfg.Checkpoint) (*HeaderSync, *SyncPeer) {
	t.Helper()

	hs, err := NewHeaderSync(HeaderSyncConfig{Index: f.idx, Params: syncTestParams(checkpoints)})
	require.NoError(t, err)

	peer := fullNodePeer("1.2.3.4:8333")
	require.Len(t, hs.PeerEstablished(peer), 1)

	return hs, peer
}

func headersMsg(headers []*wire.BlockHeader) *wire.MsgHeaders {
	return &wire.MsgHeaders{Headers: headers}
}

// TestHeaderSync_OnHeadersBatching pins the net_processing.cpp HEADERS rules:
// the MAX_HEADERS_RESULTS cap, the continuation getheaders after a full batch,
// and the silence after a short one.
func TestHeaderSync_OnHeadersBatching(t *testing.T) {
	t.Run("a full batch triggers a continuation getheaders from the new tip locator", func(t *testing.T) {
		f := newSyncFixture(t, 1)

		cpHash := chainhash.Hash{0xC0}
		hs, peer := startedSync(t, f, []chaincfg.Checkpoint{{Height: 100000, Hash: &cpHash}})

		batch := minedRun(f.tip(), MaxHeadersResults, 100)

		msgs, misbehavior, err := hs.OnHeaders(peer, headersMsg(batch))
		require.NoError(t, err)
		require.Zero(t, misbehavior)

		gh := requireGetHeaders(t, msgs)

		last := batch[len(batch)-1].BlockHash()
		require.Equal(t, f.idx.LocatorFrom(last), locatorValues(gh))
		require.Equal(t, cpHash, gh.HashStop)

		// Every header in the batch must have landed in the index.
		tipHash, tipHeight := f.idx.Tip()
		require.Equal(t, last, tipHash)
		require.Equal(t, int32(1+MaxHeadersResults), tipHeight)

		// net_processing.cpp HEADERS: UpdateBlockAvailability with the last
		// header of the batch.
		require.NotNil(t, peer.State.pindexBestKnownBlock)
		require.Equal(t, last, peer.State.pindexBestKnownBlock.Hash)
	})

	t.Run("a short batch inside the headers-first round still asks for the next one", func(t *testing.T) {
		f := newSyncFixture(t, 1)

		cpHash := chainhash.Hash{0xC0}
		hs, peer := startedSync(t, f, []chaincfg.Checkpoint{{Height: 100000, Hash: &cpHash}})

		batch := minedRun(f.tip(), 3, 200)

		msgs, misbehavior, err := hs.OnHeaders(peer, headersMsg(batch))
		require.NoError(t, err)
		require.Zero(t, misbehavior)

		// legacy manager.go handleHeadersMsg re-requests unconditionally while
		// the checkpoint is not reached — no batch-length gate. Without the
		// request the round would stall with nothing outstanding.
		gh := requireGetHeaders(t, msgs)
		require.Equal(t, f.idx.LocatorFrom(batch[len(batch)-1].BlockHash()), locatorValues(gh))
		require.Equal(t, cpHash, gh.HashStop)
		require.True(t, hs.IsHeadersFirstMode())
	})

	t.Run("a repeated batch of known headers ends the round instead of looping", func(t *testing.T) {
		f := newSyncFixture(t, 1)

		cpHash := chainhash.Hash{0xC0}
		hs, peer := startedSync(t, f, []chaincfg.Checkpoint{{Height: 100000, Hash: &cpHash}})

		batch := minedRun(f.tip(), 3, 220)

		// The first delivery is real progress and earns the next request.
		msgs, misbehavior, err := hs.OnHeaders(peer, headersMsg(batch))
		require.NoError(t, err)
		require.Zero(t, misbehavior)
		require.Len(t, msgs, 1)

		// Replaying part of it changes nothing: AddHeader reports headers we
		// already hold as connected, so without the anchor the machine would
		// answer with the same getheaders for ever. legacy manager.go catches
		// the replay on its header-list anchor and disconnects the peer.
		// Replaying a prefix keeps the stall height (3) and the anchor (4)
		// distinct, so the message has to name both.
		msgs, misbehavior, err = hs.OnHeaders(peer, headersMsg(batch[:len(batch)-1]))
		require.Error(t, err)
		require.True(t, errors.Is(err, ErrHeadersNoProgress))
		require.False(t, errors.Is(err, ErrCheckpointMismatch))
		require.Nil(t, msgs)
		require.Zero(t, misbehavior)

		// The message names the height it stalled at and the height the
		// request went out from.
		require.Contains(t, err.Error(), "height 3")
		require.Contains(t, err.Error(), "height 4")

		// The machine does not release the round on the error: the caller
		// disconnects the peer and drives PeerDisconnected.
		require.True(t, hs.IsHeadersFirstMode())
		require.True(t, peer.State.fSyncStarted)

		hs.PeerDisconnected(peer)
		require.False(t, hs.IsHeadersFirstMode())
		require.False(t, peer.State.fSyncStarted)
	})

	t.Run("a fork reply below our tip height counts as progress", func(t *testing.T) {
		// Our tip sits on a branch the sync peer does not share: heights alone
		// would call its honest answer a replay. This is the recovery case the
		// header index's height-based tip makes reachable.
		f := newSyncFixture(t, 6)

		cpHash := chainhash.Hash{0xC0}
		hs, peer := startedSync(t, f, []chaincfg.Checkpoint{{Height: 100000, Hash: &cpHash}})

		// A different chain forking at height 2 and reaching only height 5,
		// below our tip at 6. Every header in it is new to the index.
		fork := minedRun(f.chain[1], 3, 1300)

		msgs, misbehavior, err := hs.OnHeaders(peer, headersMsg(fork))
		require.NoError(t, err)
		require.Zero(t, misbehavior)

		// The round continues from the fork tip the next request is issued
		// from, not from our taller branch.
		gh := requireGetHeaders(t, msgs)
		require.Equal(t, f.idx.LocatorFrom(fork[len(fork)-1].BlockHash()), locatorValues(gh))
		require.True(t, hs.IsHeadersFirstMode())

		for _, h := range fork {
			_, ok := f.idx.Lookup(h.BlockHash())
			require.True(t, ok)
		}

		// Extending that fork keeps working: the anchor moved to the fork, so
		// the next honest batch is progress by height too.
		more := minedRun(fork[len(fork)-1], 2, 1400)

		msgs, misbehavior, err = hs.OnHeaders(peer, headersMsg(more))
		require.NoError(t, err)
		require.Zero(t, misbehavior)
		require.Len(t, msgs, 1)

		// Replaying the fork after that is still caught: nothing new, no
		// higher.
		_, _, err = hs.OnHeaders(peer, headersMsg(fork))
		require.Error(t, err)
		require.True(t, errors.Is(err, ErrHeadersNoProgress))
	})

	t.Run("a peer at our own height makes the only progress it can", func(t *testing.T) {
		f := newSyncFixture(t, 3)

		cpHash := chainhash.Hash{0xC0}
		hs, peer := startedSync(t, f, []chaincfg.Checkpoint{{Height: 100000, Hash: &cpHash}})

		// The initial getheaders is built from our tip's parent, so a peer at
		// our height answers with exactly one header: our own tip. That is not
		// a replay and must not end the round.
		msgs, misbehavior, err := hs.OnHeaders(peer, headersMsg([]*wire.BlockHeader{f.tip()}))
		require.NoError(t, err)
		require.Zero(t, misbehavior)
		require.Len(t, msgs, 1)

		// The follow-up request runs from our tip, so the peer now has nothing
		// to send and the exchange ends on the empty batch.
		msgs, misbehavior, err = hs.OnHeaders(peer, headersMsg(nil))
		require.NoError(t, err)
		require.Zero(t, misbehavior)
		require.Nil(t, msgs)
	})

	t.Run("a short batch outside the headers-first round asks for nothing more", func(t *testing.T) {
		f := newSyncFixture(t, 1)

		// No checkpoints, so the machine never enters headers-first mode and
		// only the net_processing.cpp full-batch rule applies.
		hs, peer := startedSync(t, f, nil)
		require.False(t, hs.IsHeadersFirstMode())

		batch := minedRun(f.tip(), 3, 210)

		msgs, misbehavior, err := hs.OnHeaders(peer, headersMsg(batch))
		require.NoError(t, err)
		require.Zero(t, misbehavior)
		require.Nil(t, msgs)
	})

	t.Run("an empty batch is ignored", func(t *testing.T) {
		f := newSyncFixture(t, 1)
		hs, peer := startedSync(t, f, nil)

		msgs, misbehavior, err := hs.OnHeaders(peer, headersMsg(nil))
		require.NoError(t, err)
		require.Zero(t, misbehavior)
		require.Nil(t, msgs)
	})

	t.Run("more than MAX_HEADERS_RESULTS scores too-many-headers", func(t *testing.T) {
		f := newSyncFixture(t, 1)
		hs, peer := startedSync(t, f, nil)

		batch := minedRun(f.tip(), MaxHeadersResults+1, 300)

		msgs, misbehavior, err := hs.OnHeaders(peer, headersMsg(batch))
		require.NoError(t, err)
		require.Nil(t, msgs)

		// net_processing.cpp HEADERS: Misbehaving(pfrom, 20, "too-many-headers").
		// The wire decoder refuses more than MaxBlockHeadersPerMsg headers, so
		// a real peer cannot reach this rule over the network; the batch here
		// is built in-process. The rule is kept because it belongs with the
		// rest of the HEADERS handling, not because the decoder might miss it.
		require.Equal(t, 20, misbehavior)

		// Nothing from an over-long batch may reach the index.
		_, tipHeight := f.idx.Tip()
		require.Equal(t, int32(1), tipHeight)
	})
}

// TestHeaderSync_UnsolicitedBulkHeaders pins Task 11's policy at the
// round-ignore site (headersync.go OnHeaders, "the ignored batch is
// deliberately unscored" comment): a BULK batch from a peer that does not
// hold the sync slot scores misbehavior UNLESS it answers a getheaders we
// sent that peer. An announcement-size batch (< MaxBlocksToAnnounce) never
// scores, solicited or not — SVNode's HEADERS handler has no solicitation
// check, and breaking that would break block announcement.
//
// PeerManager, not this machine, tracks solicitation (see manager.go
// Established/Headers/Inv): these tests drive OnHeaders directly and set
// peer.getHeadersOutstanding by hand to stand in for that tracking, exactly
// as the manager-level test in manager_test.go does through the real dispatch
// path.
func TestHeaderSync_UnsolicitedBulkHeaders(t *testing.T) {
	// bystanderRound starts a headers-first round with holder and returns a
	// second peer that never holds the slot, matching the existing
	// "during a round a peer without the sync slot cannot index headers" setup.
	bystanderRound := func(t *testing.T) (*HeaderSync, *SyncPeer) {
		t.Helper()

		f := newSyncFixture(t, 1)
		wrong := chainhash.Hash{0xDE, 0xAD}

		hs, holder := startedSync(t, f, []chaincfg.Checkpoint{{Height: 100000, Hash: &wrong}})
		require.True(t, hs.IsHeadersFirstMode())

		bystander := fullNodePeer("5.6.7.8:8333")
		require.Nil(t, hs.PeerEstablished(bystander))
		require.False(t, bystander.State.fSyncStarted)

		// This is what makes bystander a bystander: holder, not it, owns the
		// round's single sync slot.
		require.True(t, holder.State.fSyncStarted)

		return hs, bystander
	}

	t.Run("an unsolicited bulk batch scores misbehavior", func(t *testing.T) {
		hs, bystander := bystanderRound(t)

		batch := minedRun(&wire.BlockHeader{}, MaxBlocksToAnnounce, 5000)

		_, misbehavior, err := hs.OnHeaders(bystander, headersMsg(batch))
		require.NoError(t, err)

		// PARITY-HARNESS-DEPENDENT: scoreUnsolicitedBulkHeaders is a Teranode
		// hardening policy choice with no SVNode counterpart (see the
		// OnHeaders comment at the round-ignore site). The parity harness
		// (Phase 3 Task 26, OPEN QUESTION 1) may move this number; it must not
		// move whether an unsolicited bulk batch scores at all. Grep
		// PARITY-HARNESS-DEPENDENT to find every place this dependency is
		// recorded.
		require.Equal(t, scoreUnsolicitedBulkHeaders, misbehavior)
	})

	t.Run("a solicited bulk batch scores nothing, and consumes the solicitation", func(t *testing.T) {
		hs, bystander := bystanderRound(t)

		// Stands in for PeerManager having just sent this peer one getheaders
		// (Established, Headers, Inv, or an election/sweep grant — see
		// manager.go).
		bystander.getHeadersOutstanding = 1

		batch := minedRun(&wire.BlockHeader{}, MaxBlocksToAnnounce, 5100)

		_, misbehavior, err := hs.OnHeaders(bystander, headersMsg(batch))
		require.NoError(t, err)
		require.Zero(t, misbehavior)

		// The solicitation is consumed by the batch it was waiting for: a
		// second, unrelated bulk batch from the same peer with nothing
		// further requested must score.
		require.Zero(t, bystander.getHeadersOutstanding)

		_, misbehavior, err = hs.OnHeaders(bystander, headersMsg(batch))
		require.NoError(t, err)
		require.Equal(t, scoreUnsolicitedBulkHeaders, misbehavior)
	})

	t.Run("an unsolicited announcement-size batch stays free", func(t *testing.T) {
		hs, bystander := bystanderRound(t)

		// One short of the bulk boundary (headersync_test.go's "one too many
		// for the announcement path" comment pins MaxBlocksToAnnounce itself
		// as bulk, so the free side stops at MaxBlocksToAnnounce-1).
		batch := minedRun(&wire.BlockHeader{}, MaxBlocksToAnnounce-1, 5200)

		_, misbehavior, err := hs.OnHeaders(bystander, headersMsg(batch))
		require.NoError(t, err)
		require.Zero(t, misbehavior)
	})

	t.Run("repeated unsolicited bulk batches reach the ban threshold", func(t *testing.T) {
		hs, bystander := bystanderRound(t)

		batch := minedRun(&wire.BlockHeader{}, MaxBlocksToAnnounce, 5300)

		total := 0
		calls := 0

		for total < banScoreThreshold {
			_, misbehavior, err := hs.OnHeaders(bystander, headersMsg(batch))
			require.NoError(t, err)

			// C11: assert the delta itself, not merely that the cumulative
			// total moved — a mutation that stopped scoring after the first
			// call must fail this loop rather than pass it by coincidence.
			require.Equal(t, scoreUnsolicitedBulkHeaders, misbehavior,
				"every occurrence must score the same policy delta")

			total += misbehavior
			calls++

			require.Less(t, calls, 1000, "the loop must reach the ban threshold, not spin forever")
		}

		require.GreaterOrEqual(t, total, banScoreThreshold)
		require.Less(t, total-scoreUnsolicitedBulkHeaders, banScoreThreshold,
			"the call before this one must not already have reached the threshold")
	})
}

// TestHeaderSync_SyncSlotRelease pins FinalizeNode plus resetHeaderState: the
// sync slot and the header state must both come back when the sync peer goes
// away or stops answering.
func TestHeaderSync_SyncSlotRelease(t *testing.T) {
	release := []struct {
		name string
		call func(hs *HeaderSync, peer *SyncPeer)
	}{
		{"disconnect", func(hs *HeaderSync, peer *SyncPeer) { hs.PeerDisconnected(peer) }},
		{"timeout", func(hs *HeaderSync, peer *SyncPeer) { hs.SyncPeerTimedOut(peer) }},
	}

	for _, tc := range release {
		t.Run(tc.name+" resets header state and frees the slot", func(t *testing.T) {
			f := newSyncFixture(t, 1)

			cpHash := chainhash.Hash{0xC0}
			hs, peer := startedSync(t, f, []chaincfg.Checkpoint{{Height: 100000, Hash: &cpHash}})
			require.True(t, hs.IsHeadersFirstMode())

			tc.call(hs, peer)

			// legacy manager.go resetHeaderState: the mode goes off with the
			// peer that was driving it, so IsHeadersFirstMode cannot stay true
			// with nobody syncing.
			require.False(t, hs.IsHeadersFirstMode())
			require.False(t, peer.State.fSyncStarted)

			// The freed slot must be usable by the next candidate, and the
			// checkpoint re-seeds for the fresh round.
			next := fullNodePeer("5.6.7.8:8333")
			gh := requireGetHeaders(t, hs.PeerEstablished(next))
			require.Equal(t, cpHash, gh.HashStop)
			require.True(t, hs.IsHeadersFirstMode())
		})
	}

	t.Run("releasing a peer that never held the slot changes nothing", func(t *testing.T) {
		f := newSyncFixture(t, 1)

		cpHash := chainhash.Hash{0xC0}
		hs, holder := startedSync(t, f, []chaincfg.Checkpoint{{Height: 100000, Hash: &cpHash}})

		bystander := fullNodePeer("9.9.9.9:8333")
		hs.PeerDisconnected(bystander)
		hs.SyncPeerTimedOut(bystander)

		require.True(t, hs.IsHeadersFirstMode())
		require.True(t, holder.State.fSyncStarted)

		// The ROUND is what refuses the new candidate. A taken slot alone is no
		// longer enough: Task 4's 24 hour relaxation admits further peers on a
		// fresh tip, and every fixture header is stamped time.Now(). The
		// !headersFirstMode conjunct in PeerEstablished is what holds the slot
		// single here.
		require.Nil(t, hs.PeerEstablished(fullNodePeer("5.6.7.8:8333")))
	})
}

// TestHeaderSync_OnHeadersMisbehavior pins the misbehavior scores
// net_processing.cpp assigns to malformed header batches.
func TestHeaderSync_OnHeadersMisbehavior(t *testing.T) {
	t.Run("a non-continuous batch scores 20 and is dropped whole", func(t *testing.T) {
		f := newSyncFixture(t, 2)
		hs, peer := startedSync(t, f, nil)

		good := minedRun(f.tip(), 1, 400)
		// A header that connects to our tip instead of to good[0]: the batch
		// is not a continuous sequence.
		stray := minedChild(f.tip(), testEasyBits, 999)

		msgs, misbehavior, err := hs.OnHeaders(peer, headersMsg([]*wire.BlockHeader{good[0], stray}))
		require.NoError(t, err)
		require.Nil(t, msgs)

		// net_processing.cpp HEADERS: Misbehaving(pfrom, 20, "disconnected
		// headers") → error("non-continuous headers sequence").
		require.Equal(t, 20, misbehavior)

		// The scan runs before any header is accepted, so even the valid
		// leading header must not be in the index.
		_, ok := f.idx.Lookup(good[0].BlockHash())
		require.False(t, ok)
	})

	t.Run("an unconnecting batch re-asks for headers and scores every MAX_UNCONNECTING_HEADERS", func(t *testing.T) {
		f := newSyncFixture(t, 2)
		hs, peer := startedSync(t, f, nil)

		// A chain rooted at a header we have never seen.
		orphanRoot := syncGenesis()
		orphanRoot.Nonce += 7

		for !testMeetsTarget(orphanRoot) {
			orphanRoot.Nonce++
		}

		orphans := minedRun(orphanRoot, 2, 500)

		for i := 1; i <= MaxUnconnectingHeaders; i++ {
			msgs, misbehavior, err := hs.OnHeaders(peer, headersMsg(orphans))
			require.NoError(t, err)

			// net_processing.cpp HEADERS: on a missing prev block, send
			// getheaders from our best header and count the event.
			gh := requireGetHeaders(t, msgs)
			require.Equal(t, f.idx.Locator(), locatorValues(gh))

			if i == MaxUnconnectingHeaders {
				// Misbehaving(pfrom, 20, "too-many-unconnected-headers").
				require.Equal(t, 20, misbehavior)
			} else {
				require.Zero(t, misbehavior)
			}
		}

		_, tipHeight := f.idx.Tip()
		require.Equal(t, int32(2), tipHeight)

		// net_processing.cpp HEADERS: UpdateBlockAvailability runs on this path
		// too, with the last header of the batch. It is unknown to us, so it
		// lands in hashLastUnknownBlock.
		require.Equal(t, orphans[len(orphans)-1].BlockHash(), peer.State.hashLastUnknownBlock)
	})

	t.Run("a bulk unconnecting batch scores prev-blk-not-found instead of gap-filling", func(t *testing.T) {
		f := newSyncFixture(t, 2)
		hs, peer := startedSync(t, f, nil)

		orphanRoot := syncGenesis()
		orphanRoot.Nonce += 11

		for !testMeetsTarget(orphanRoot) {
			orphanRoot.Nonce++
		}

		// MAX_BLOCKS_TO_ANNOUNCE headers is one too many for the announcement
		// path, so net_processing.cpp lets AcceptBlockHeader fail it with
		// DoS(10, "prev-blk-not-found") instead of asking for the gap.
		orphans := minedRun(orphanRoot, MaxBlocksToAnnounce, 550)

		msgs, misbehavior, err := hs.OnHeaders(peer, headersMsg(orphans))
		require.NoError(t, err)
		require.Nil(t, msgs)
		require.Equal(t, 10, misbehavior)

		// The unconnecting counter belongs to the announcement path only.
		require.Zero(t, peer.nUnconnectingHeaders)

		_, tipHeight := f.idx.Tip()
		require.Equal(t, int32(2), tipHeight)
	})

	t.Run("headers that connect reset the unconnecting counter", func(t *testing.T) {
		f := newSyncFixture(t, 2)
		hs, peer := startedSync(t, f, nil)

		orphanRoot := syncGenesis()
		orphanRoot.Nonce += 13

		for !testMeetsTarget(orphanRoot) {
			orphanRoot.Nonce++
		}

		orphans := minedRun(orphanRoot, 2, 600)

		// One short of the score threshold.
		for i := 1; i < MaxUnconnectingHeaders; i++ {
			_, misbehavior, err := hs.OnHeaders(peer, headersMsg(orphans))
			require.NoError(t, err)
			require.Zero(t, misbehavior)
		}

		// net_processing.cpp HEADERS: "resetting nUnconnectingHeaders (%d ->
		// 0)" once a batch connects.
		good := minedRun(f.tip(), 1, 650)

		_, misbehavior, err := hs.OnHeaders(peer, headersMsg(good))
		require.NoError(t, err)
		require.Zero(t, misbehavior)
		require.Zero(t, peer.nUnconnectingHeaders)

		// The next unconnecting batch is the first of a new run, not the tenth
		// of the old one, so it must not score.
		_, misbehavior, err = hs.OnHeaders(peer, headersMsg(orphans))
		require.NoError(t, err)
		require.Zero(t, misbehavior)
	})

	t.Run("a header failing proof of work scores 50 and is not accepted", func(t *testing.T) {
		f := newSyncFixture(t, 2)
		hs, peer := startedSync(t, f, nil)

		bad := failingChild(f.tip(), 600)

		msgs, misbehavior, err := hs.OnHeaders(peer, headersMsg([]*wire.BlockHeader{bad}))
		require.NoError(t, err)
		require.Nil(t, msgs)

		// validation.cpp CheckBlockHeader: CheckProofOfWork failure is
		// DoS(50, ..., "high-hash"), surfaced by net_processing.cpp as
		// Misbehaving(pfrom, nDoS, "invalid header received").
		require.Equal(t, 50, misbehavior)

		_, ok := f.idx.Lookup(bad.BlockHash())
		require.False(t, ok)
	})

	t.Run("a header whose target exceeds powLimit scores 50", func(t *testing.T) {
		f := newSyncFixture(t, 2)

		// Mainnet powLimit (2^224 - 1) with a header claiming the regtest-easy
		// target: the hash meets its claimed target, but the target itself is
		// out of range. pow.cpp CheckProofOfWork rejects on
		// bnTarget > powLimit before it ever compares the hash.
		mainnet := chaincfg.MainNetParams

		hs, err := NewHeaderSync(HeaderSyncConfig{Index: f.idx, Params: &mainnet})
		require.NoError(t, err)

		peer := fullNodePeer("1.2.3.4:8333")
		require.Len(t, hs.PeerEstablished(peer), 1)

		tooEasy := minedChild(f.tip(), testEasyBits, 700)
		require.True(t, testMeetsTarget(tooEasy))

		msgs, misbehavior, err := hs.OnHeaders(peer, headersMsg([]*wire.BlockHeader{tooEasy}))
		require.NoError(t, err)
		require.Nil(t, msgs)
		require.Equal(t, 50, misbehavior)
	})

	t.Run("a header with a zero target scores 50", func(t *testing.T) {
		f := newSyncFixture(t, 2)
		hs, peer := startedSync(t, f, nil)

		prevHash := f.tip().BlockHash()
		zero := chainhash.Hash{}
		// nBits == 0 decodes to target 0, which SetCompact rejects as out of
		// range regardless of the hash.
		malformed := wire.NewBlockHeader(1, &prevHash, &zero, 0, 1)

		msgs, misbehavior, err := hs.OnHeaders(peer, headersMsg([]*wire.BlockHeader{malformed}))
		require.NoError(t, err)
		require.Nil(t, msgs)
		require.Equal(t, 50, misbehavior)
	})
}

// TestCheckBlockHeaderPoW_MainnetVector runs the PoW check against a real
// mainnet header rather than a ground test header, so the byte order of both
// the hash and the compact target is pinned against the live chain and not
// against this file's own helpers.
func TestCheckBlockHeaderPoW_MainnetVector(t *testing.T) {
	// The mainnet genesis header, 80 bytes on the wire: version, null prev
	// hash, merkle root, timestamp, nBits 0x1d00ffff, nonce.
	const genesisHex = "01000000" +
		"0000000000000000000000000000000000000000000000000000000000000000" +
		"3ba3edfd7a7b12b27ac72c3e67768f617fc81bc3888a51323a9fb8aa4b1e5e4a" +
		"29ab5f49" + "ffff001d" + "1dac2b7c"

	raw, err := hex.DecodeString(genesisHex)
	require.NoError(t, err)

	var header wire.BlockHeader
	require.NoError(t, header.Deserialize(bytes.NewReader(raw)))

	// If the vector were wrong in any byte, this would not be the hash the
	// chain params carry.
	require.Equal(t, *chaincfg.MainNetParams.GenesisHash, header.BlockHash())
	require.Equal(t, testHardBits, header.Bits)

	idx, err := NewHeaderIndex(&header)
	require.NoError(t, err)

	mainnet := chaincfg.MainNetParams

	hs, err := NewHeaderSync(HeaderSyncConfig{Index: idx, Params: &mainnet})
	require.NoError(t, err)

	require.True(t, hs.checkBlockHeaderPoW(&header, header.BlockHash()))

	// Same header, different nonce: the work is gone and the check must say so.
	// Grinding to a definite failure keeps the row deterministic.
	broken := header
	broken.Nonce++

	for testMeetsTarget(&broken) {
		broken.Nonce++
	}

	require.False(t, hs.checkBlockHeaderPoW(&broken, broken.BlockHash()))
}

// TestHeaderSync_Checkpoints pins the headers-first-to-next-checkpoint scheme
// carried from legacy netsync manager.go.
func TestHeaderSync_Checkpoints(t *testing.T) {
	t.Run("a wrong hash at the checkpoint height disconnects the peer", func(t *testing.T) {
		f := newSyncFixture(t, 1)

		// The checkpoint claims some other hash at height 3.
		wrong := chainhash.Hash{0xDE, 0xAD}
		hs, peer := startedSync(t, f, []chaincfg.Checkpoint{{Height: 3, Hash: &wrong}})
		require.True(t, hs.IsHeadersFirstMode())

		batch := minedRun(f.tip(), 3, 800)

		msgs, misbehavior, err := hs.OnHeaders(peer, headersMsg(batch))

		// legacy manager.go handleHeadersMsg: "Block header at height %d/hash
		// %s does NOT match expected checkpoint hash" → DisconnectWithWarning.
		require.Error(t, err)
		require.True(t, errors.Is(err, ErrCheckpointMismatch))
		require.Nil(t, msgs)
		require.Zero(t, misbehavior)

		// errors.Is matches by code alone, so pin the rendered message too: it
		// must name the offending height, the hash we got, and the checkpoint
		// hash we wanted.
		require.Contains(t, err.Error(), "height 3")
		require.Contains(t, err.Error(), batch[1].BlockHash().String())
		require.Contains(t, err.Error(), wrong.String())

		// The machine leaves the round held: tearing the peer down is the
		// caller's job, and PeerDisconnected is what releases it.
		require.True(t, hs.IsHeadersFirstMode())
		require.True(t, peer.State.fSyncStarted)

		hs.PeerDisconnected(peer)
		require.False(t, hs.IsHeadersFirstMode())
		require.False(t, peer.State.fSyncStarted)
	})

	t.Run("matching a checkpoint asks for headers up to the next one", func(t *testing.T) {
		f := newSyncFixture(t, 1)

		// Pre-mine the headers the peer will send so the first checkpoint can
		// name the real hash at height 3.
		batch := minedRun(f.tip(), 3, 900)
		first := batch[len(batch)-1].BlockHash()
		second := chainhash.Hash{0xBE, 0xEF}

		hs, peer := startedSync(t, f, []chaincfg.Checkpoint{
			{Height: 4, Hash: &first},
			{Height: 5000, Hash: &second},
		})

		msgs, misbehavior, err := hs.OnHeaders(peer, headersMsg(batch))
		require.NoError(t, err)
		require.Zero(t, misbehavior)

		gh := requireGetHeaders(t, msgs)

		// legacy manager.go: the next round of headers runs from the reached
		// checkpoint up to the checkpoint after it.
		require.Equal(t, f.idx.LocatorFrom(first), locatorValues(gh))
		require.Equal(t, second, gh.HashStop)
		require.True(t, hs.IsHeadersFirstMode())
	})

	t.Run("a short batch reaching the final checkpoint ends headers-first mode", func(t *testing.T) {
		f := newSyncFixture(t, 1)

		batch := minedRun(f.tip(), 3, 1000)
		final := batch[len(batch)-1].BlockHash()

		hs, peer := startedSync(t, f, []chaincfg.Checkpoint{{Height: 4, Hash: &final}})
		require.True(t, hs.IsHeadersFirstMode())

		msgs, misbehavior, err := hs.OnHeaders(peer, headersMsg(batch))
		require.NoError(t, err)
		require.Zero(t, misbehavior)

		// legacy manager.go: "Reached the final checkpoint -- switching to
		// normal mode". The batch is short, so net_processing.cpp asks for
		// nothing more either.
		require.False(t, hs.IsHeadersFirstMode())
		require.Nil(t, msgs)
	})

	t.Run("headers stop at the checkpoint header inside a full batch", func(t *testing.T) {
		f := newSyncFixture(t, 0)

		batch := minedRun(f.genesis, MaxHeadersResults, 1100)
		cpHash := batch[1].BlockHash() // height 2, well inside the batch

		hs, peer := startedSync(t, f, []chaincfg.Checkpoint{{Height: 2, Hash: &cpHash}})

		msgs, misbehavior, err := hs.OnHeaders(peer, headersMsg(batch))
		require.NoError(t, err)
		require.Zero(t, misbehavior)

		// legacy manager.go handleHeadersMsg breaks out of the header loop at
		// the checkpoint, so the rest of the batch is not accepted.
		_, tipHeight := f.idx.Tip()
		require.Equal(t, int32(2), tipHeight)

		_, ok := f.idx.Lookup(batch[2].BlockHash())
		require.False(t, ok)

		// No checkpoint is left, so the continuation runs with a zero hashStop.
		gh := requireGetHeaders(t, msgs)
		require.Equal(t, chainhash.Hash{}, gh.HashStop)
		require.False(t, hs.IsHeadersFirstMode())
	})

	t.Run("during a round a peer without the sync slot cannot index headers", func(t *testing.T) {
		f := newSyncFixture(t, 1)

		batch := minedRun(f.tip(), 3, 1200)
		wrong := chainhash.Hash{0xDE, 0xAD}

		// The sync slot is held by another peer, so this one is outside the
		// headers-first round: legacy scopes that round to its sync peer and
		// returns early for everyone else.
		hs, holder := startedSync(t, f, []chaincfg.Checkpoint{{Height: 3, Hash: &wrong}})
		require.True(t, hs.IsHeadersFirstMode())

		bystander := fullNodePeer("5.6.7.8:8333")
		require.Nil(t, hs.PeerEstablished(bystander))
		require.False(t, bystander.State.fSyncStarted)

		msgs, misbehavior, err := hs.OnHeaders(bystander, headersMsg(batch))

		// The same batch would have disconnected the sync peer at height 3.
		require.NoError(t, err)
		require.Zero(t, misbehavior)
		require.Nil(t, msgs)

		// Nothing was indexed, so the bystander can neither race the round nor
		// push our tip past the checkpoint the round is working toward.
		_, tipHeight := f.idx.Tip()
		require.Equal(t, int32(1), tipHeight)

		for _, h := range batch {
			_, ok := f.idx.Lookup(h.BlockHash())
			require.False(t, ok)
		}

		// The round and its checkpoint are untouched.
		require.True(t, hs.IsHeadersFirstMode())
		require.True(t, holder.State.fSyncStarted)

		// The announcement still counts: the peer is known to have that block
		// once we learn of it, so block download can use it later.
		require.Equal(t, batch[len(batch)-1].BlockHash(), bystander.State.hashLastUnknownBlock)

		// Once the round ends, the same peer's headers index normally — the
		// restriction is round-scoped, not permanent.
		hs.PeerDisconnected(holder)
		require.False(t, hs.IsHeadersFirstMode())

		msgs, misbehavior, err = hs.OnHeaders(bystander, headersMsg(batch))
		require.NoError(t, err)
		require.Zero(t, misbehavior)
		require.Nil(t, msgs)

		_, tipHeight = f.idx.Tip()
		require.Equal(t, int32(4), tipHeight)
	})

	t.Run("findNextHeaderCheckpoint picks the first checkpoint above the height", func(t *testing.T) {
		f := newSyncFixture(t, 0)

		h1 := chainhash.Hash{0x01}
		h2 := chainhash.Hash{0x02}
		h3 := chainhash.Hash{0x03}

		hs, err := NewHeaderSync(HeaderSyncConfig{
			Index: f.idx,
			Params: syncTestParams([]chaincfg.Checkpoint{
				{Height: 100, Hash: &h1},
				{Height: 200, Hash: &h2},
				{Height: 300, Hash: &h3},
			}),
		})
		require.NoError(t, err)

		tests := []struct {
			height int32
			want   *chainhash.Hash
		}{
			{0, &h1},
			{99, &h1},
			{100, &h2},
			{199, &h2},
			{200, &h3},
			{299, &h3},
			{300, nil},
			{4000, nil},
		}

		for _, tc := range tests {
			got := hs.findNextHeaderCheckpoint(tc.height)

			if tc.want == nil {
				require.Nil(t, got, "height %d", tc.height)
				continue
			}

			require.NotNil(t, got, "height %d", tc.height)
			require.Equal(t, *tc.want, *got.Hash, "height %d", tc.height)
		}
	})
}

func TestNewHeaderSync_Validation(t *testing.T) {
	f := newSyncFixture(t, 0)

	_, err := NewHeaderSync(HeaderSyncConfig{Index: nil, Params: syncTestParams(nil)})
	require.Error(t, err)

	_, err = NewHeaderSync(HeaderSyncConfig{Index: f.idx, Params: nil})
	require.Error(t, err)

	// Params without a powLimit would let the header PoW check accept any
	// target a peer claims.
	noLimit := chaincfg.MainNetParams
	noLimit.PowLimit = nil

	_, err = NewHeaderSync(HeaderSyncConfig{Index: f.idx, Params: &noLimit})
	require.Error(t, err)

	// findNextHeaderCheckpoint and lastCheckpointInIndex both read the list as
	// ascending by height, and the latter now gates a disconnect, so a list
	// that is not ascending must not build a machine.
	h1 := chainhash.Hash{0x01}
	h2 := chainhash.Hash{0x02}

	_, err = NewHeaderSync(HeaderSyncConfig{
		Index:  f.idx,
		Params: syncTestParams([]chaincfg.Checkpoint{{Height: 200, Hash: &h1}, {Height: 100, Hash: &h2}}),
	})
	require.Error(t, err)

	// Equal heights are not ascending either.
	_, err = NewHeaderSync(HeaderSyncConfig{
		Index:  f.idx,
		Params: syncTestParams([]chaincfg.Checkpoint{{Height: 100, Hash: &h1}, {Height: 100, Hash: &h2}}),
	})
	require.Error(t, err)

	// Every network go-chaincfg ships is ascending, so the check is inert in
	// production. Pin that, so a params change cannot silently disable sync.
	for _, params := range []*chaincfg.Params{
		&chaincfg.MainNetParams,
		&chaincfg.TestNetParams,
		&chaincfg.StnParams,
		&chaincfg.RegressionNetParams,
		&chaincfg.TeraTestNetParams,
		&chaincfg.TeraScalingTestNetParams,
	} {
		_, err = NewHeaderSync(HeaderSyncConfig{Index: f.idx, Params: params})
		require.NoError(t, err, "network %s", params.Name)
	}
}

// ---------------------------------------------------------------------------
// Contextual difficulty and the checkpoint fence
// ---------------------------------------------------------------------------

// daaFixtureBaseTime anchors the DAA-era fixture chain. Its headers sit
// exactly ten minutes apart, which is the spacing the DAA is tuned for, so the
// difficulty it computes for the next header is the one the chain already
// runs at.
const daaFixtureBaseTime int64 = 1600000000

// daaSyncParams returns mainnet params with the regtest powLimit and the DAA
// active from height 0, so a cheaply ground fixture chain still exercises the
// contextual difficulty check. Mainnet's own DaaForkHeight is far above any
// height a test chain reaches.
func daaSyncParams(checkpoints []chaincfg.Checkpoint) *chaincfg.Params {
	params := chaincfg.MainNetParams
	params.PowLimit = testPowLimit()
	params.DaaForkHeight = 0
	params.Checkpoints = checkpoints

	return &params
}

// timedChild grinds a child of parent with an explicit timestamp, so the
// fixture's block spacing is deterministic instead of whatever wall clock
// wire.NewBlockHeader stamps.
func timedChild(parent *wire.BlockHeader, bits, salt uint32, when int64) *wire.BlockHeader {
	prevHash := parent.BlockHash()

	merkle := chainhash.Hash{}
	merkle[0] = byte(salt)
	merkle[1] = byte(salt >> 8)
	merkle[2] = byte(salt >> 16)

	h := &wire.BlockHeader{
		Version:    1,
		PrevBlock:  prevHash,
		MerkleRoot: merkle,
		Timestamp:  time.Unix(when, 0),
		Bits:       bits,
	}

	for !testMeetsTarget(h) {
		h.Nonce++
	}

	return h
}

// daaGenesis is syncGenesis with a fixed timestamp.
func daaGenesis() *wire.BlockHeader {
	zero := chainhash.Hash{}

	h := &wire.BlockHeader{
		Version:    1,
		PrevBlock:  zero,
		MerkleRoot: zero,
		Timestamp:  time.Unix(daaFixtureBaseTime, 0),
		Bits:       testEasyBits,
	}

	for !testMeetsTarget(h) {
		h.Nonce++
	}

	return h
}

// newDaaSyncFixture builds an index of localHeight headers at testEasyBits,
// ten minutes apart. Under the regtest powLimit that spacing is a fixed point
// of the DAA: each header contributes a chain work of exactly 2, so the
// 144-block window yields the powLimit target again, whose compact form is
// testEasyBits. The fixture therefore satisfies its own contextual difficulty,
// which the first assertion of every test below pins.
func newDaaSyncFixture(t *testing.T, localHeight int) *syncFixture {
	t.Helper()

	genesis := daaGenesis()

	idx, err := NewHeaderIndex(genesis)
	require.NoError(t, err)

	chain := make([]*wire.BlockHeader, 0, localHeight)
	prev := genesis

	for i := 1; i <= localHeight; i++ {
		h := timedChild(prev, testEasyBits, uint32(i), daaFixtureBaseTime+int64(i)*600) //nolint:gosec // test heights are small

		connected, addErr := idx.AddHeader(h)
		require.NoError(t, addErr)
		require.True(t, connected)

		chain = append(chain, h)
		prev = h
	}

	return &syncFixture{genesis: genesis, idx: idx, chain: chain}
}

// startedSyncWith is startedSync for a caller that needs params other than
// syncTestParams.
func startedSyncWith(t *testing.T, f *syncFixture, params *chaincfg.Params) (*HeaderSync, *SyncPeer) {
	t.Helper()

	hs, err := NewHeaderSync(HeaderSyncConfig{Index: f.idx, Params: params})
	require.NoError(t, err)

	peer := fullNodePeer("1.2.3.4:8333")
	require.Len(t, hs.PeerEstablished(peer), 1)

	return hs, peer
}

// TestHeaderSync_ContextualDifficulty pins ContextualCheckBlockHeader's
// "bad-diffbits" rule on received headers.
func TestHeaderSync_ContextualDifficulty(t *testing.T) {
	const fixtureHeight = 200

	// The fixture spacing must be a fixed point of the DAA, or every row below
	// would be asserting against an accidental value.
	requireFixtureIsDaaStable := func(t *testing.T, f *syncFixture, params *chaincfg.Params) HeaderNode {
		t.Helper()

		tip, ok := f.idx.Lookup(f.tip().BlockHash())
		require.True(t, ok)
		require.Equal(t, int32(fixtureHeight), tip.Height)

		want, validated := GetNextWorkRequired(f.idx, params, tip, tip.Time+600)
		require.True(t, validated)
		require.Equal(t, testEasyBits, want, "the fixture chain must satisfy its own contextual difficulty")

		return tip
	}

	t.Run("a header claiming the wrong nBits is refused with the bad-diffbits score", func(t *testing.T) {
		f := newDaaSyncFixture(t, fixtureHeight)
		params := daaSyncParams(nil)
		tip := requireFixtureIsDaaStable(t, f, params)

		hs, peer := startedSyncWith(t, f, params)

		// One tick below the target the DAA requires: still inside the
		// powLimit, so the cheap CheckBlockHeader proof-of-work check passes
		// and only the contextual check can catch it.
		bad := timedChild(f.tip(), testEasyBits-1, 9001, tip.Time+600)
		require.True(t, hs.checkBlockHeaderPoW(bad, bad.BlockHash()))

		msgs, misbehavior, err := hs.OnHeaders(peer, headersMsg([]*wire.BlockHeader{bad}))

		// validation.cpp ContextualCheckBlockHeader:
		// state.DoS(100, ..., "bad-diffbits"), applied by net_processing.cpp as
		// Misbehaving(pfrom, nDoS, "invalid header received").
		require.NoError(t, err)
		require.Equal(t, scoreBadDiffBits, misbehavior)
		require.Nil(t, msgs)

		// The header never reaches the index, which is what bounds its growth.
		_, ok := f.idx.Lookup(bad.BlockHash())
		require.False(t, ok)

		_, tipHeight := f.idx.Tip()
		require.Equal(t, int32(fixtureHeight), tipHeight)
	})

	t.Run("a header claiming the nBits the DAA requires is accepted", func(t *testing.T) {
		f := newDaaSyncFixture(t, fixtureHeight)
		params := daaSyncParams(nil)
		tip := requireFixtureIsDaaStable(t, f, params)

		hs, peer := startedSyncWith(t, f, params)

		good := timedChild(f.tip(), testEasyBits, 9002, tip.Time+600)

		msgs, misbehavior, err := hs.OnHeaders(peer, headersMsg([]*wire.BlockHeader{good}))
		require.NoError(t, err)
		require.Zero(t, misbehavior)
		require.Nil(t, msgs)

		node, ok := f.idx.Lookup(good.BlockHash())
		require.True(t, ok)
		require.Equal(t, int32(fixtureHeight+1), node.Height)
	})

	t.Run("the check applies to a peer that never started a sync", func(t *testing.T) {
		f := newDaaSyncFixture(t, fixtureHeight)
		params := daaSyncParams(nil)
		tip := requireFixtureIsDaaStable(t, f, params)

		hs, err := NewHeaderSync(HeaderSyncConfig{Index: f.idx, Params: params})
		require.NoError(t, err)

		// No PeerEstablished, so this peer holds no sync slot and no round is
		// running: the net_processing.cpp path, where every peer's headers are
		// indexed. The contextual check covers it too.
		bystander := fullNodePeer("5.6.7.8:8333")
		bad := timedChild(f.tip(), testEasyBits-1, 9003, tip.Time+600)

		msgs, misbehavior, err := hs.OnHeaders(bystander, headersMsg([]*wire.BlockHeader{bad}))
		require.NoError(t, err)
		require.Equal(t, scoreBadDiffBits, misbehavior)
		require.Nil(t, msgs)

		_, ok := f.idx.Lookup(bad.BlockHash())
		require.False(t, ok)
	})

	t.Run("a batch is refused at the first bad header, keeping the ones before it", func(t *testing.T) {
		f := newDaaSyncFixture(t, fixtureHeight)
		params := daaSyncParams(nil)
		tip := requireFixtureIsDaaStable(t, f, params)

		hs, peer := startedSyncWith(t, f, params)

		good := timedChild(f.tip(), testEasyBits, 9004, tip.Time+600)
		bad := timedChild(good, testEasyBits-1, 9005, tip.Time+1200)

		msgs, misbehavior, err := hs.OnHeaders(peer, headersMsg([]*wire.BlockHeader{good, bad}))
		require.NoError(t, err)
		require.Equal(t, scoreBadDiffBits, misbehavior)
		require.Nil(t, msgs)

		// AcceptBlockHeader inserts each header as it passes, so the good one
		// stays; the loop stops at the bad one.
		_, ok := f.idx.Lookup(good.BlockHash())
		require.True(t, ok)

		_, ok = f.idx.Lookup(bad.BlockHash())
		require.False(t, ok)
	})

	t.Run("a header re-served from the index is not re-checked", func(t *testing.T) {
		f := newDaaSyncFixture(t, fixtureHeight)
		params := daaSyncParams(nil)
		requireFixtureIsDaaStable(t, f, params)

		hs, peer := startedSyncWith(t, f, params)

		// AcceptBlockHeader returns early for a header already in
		// mapBlockIndex, before every contextual check. Replaying the tail of
		// our own chain must therefore cost the peer nothing.
		replay := f.chain[len(f.chain)-3:]

		msgs, misbehavior, err := hs.OnHeaders(peer, headersMsg(replay))
		require.NoError(t, err)
		require.Zero(t, misbehavior)
		require.Nil(t, msgs)
	})

	t.Run("a pre-DAA header is not contextually validated", func(t *testing.T) {
		f := newDaaSyncFixture(t, fixtureHeight)

		// Real mainnet DaaForkHeight, far above the fixture, so every header
		// here sits in a historic difficulty era this port never evaluates.
		params := daaSyncParams(nil)
		params.DaaForkHeight = chaincfg.MainNetParams.DaaForkHeight
		require.Greater(t, int32(params.DaaForkHeight), int32(fixtureHeight)) //nolint:gosec // fork heights are small positive constants

		hs, peer := startedSyncWith(t, f, params)

		wrongBits := timedChild(f.tip(), testEasyBits-1, 9006, daaFixtureBaseTime+(fixtureHeight+1)*600)

		msgs, misbehavior, err := hs.OnHeaders(peer, headersMsg([]*wire.BlockHeader{wrongBits}))
		require.NoError(t, err)
		require.Zero(t, misbehavior)
		require.Nil(t, msgs)

		_, ok := f.idx.Lookup(wrongBits.BlockHash())
		require.True(t, ok, "a pre-DAA header is accepted on its own claimed target alone")
	})
}

// TestHeaderSync_CheckpointFence pins validation.cpp
// CheckIndexAgainstCheckpoint's "Don't accept any forks from the main chain
// prior to last checkpoint" rule. It runs against syncTestParams, whose
// DaaForkHeight is real mainnet's, so no contextual difficulty answer exists
// at these heights and the fence is the only gate under test.
func TestHeaderSync_CheckpointFence(t *testing.T) {
	const (
		fixtureHeight = 120
		cpHeight      = 60
		forkHeight    = 30
	)

	// newFenced returns a fixture whose checkpoint at cpHeight names a header
	// the index already holds, which is what makes GetLastCheckpoint answer.
	newFenced := func(t *testing.T, disable bool) (*syncFixture, *HeaderSync, *SyncPeer) {
		t.Helper()

		f := newSyncFixture(t, fixtureHeight)
		cpHash := f.chain[cpHeight-1].BlockHash()

		hs, err := NewHeaderSync(HeaderSyncConfig{
			Index:              f.idx,
			Params:             syncTestParams([]chaincfg.Checkpoint{{Height: cpHeight, Hash: &cpHash}}),
			DisableCheckpoints: disable,
		})
		require.NoError(t, err)

		peer := fullNodePeer("1.2.3.4:8333")
		require.Len(t, hs.PeerEstablished(peer), 1)
		require.False(t, hs.IsHeadersFirstMode(), "the fixture tip is past the final checkpoint")

		return f, hs, peer
	}

	t.Run("a fork below the last known checkpoint disconnects the peer", func(t *testing.T) {
		f, hs, peer := newFenced(t, false)

		// A brand-new header building on our chain at forkHeight: its own
		// height is forkHeight+1, well below the checkpoint.
		fork := minedChild(f.chain[forkHeight-1], testEasyBits, 7001)

		msgs, misbehavior, err := hs.OnHeaders(peer, headersMsg([]*wire.BlockHeader{fork}))

		require.Error(t, err)
		require.True(t, errors.Is(err, ErrCheckpointFork))
		require.Nil(t, msgs)
		require.Zero(t, misbehavior)

		require.Contains(t, err.Error(), "height 31")
		require.Contains(t, err.Error(), "60")

		_, ok := f.idx.Lookup(fork.BlockHash())
		require.False(t, ok)
	})

	t.Run("a header at or above the checkpoint height is accepted", func(t *testing.T) {
		f, hs, peer := newFenced(t, false)

		extend := minedChild(f.tip(), testEasyBits, 7002)

		msgs, misbehavior, err := hs.OnHeaders(peer, headersMsg([]*wire.BlockHeader{extend}))
		require.NoError(t, err)
		require.Zero(t, misbehavior)
		require.Nil(t, msgs)

		_, ok := f.idx.Lookup(extend.BlockHash())
		require.True(t, ok)
	})

	t.Run("headers we already hold below the checkpoint are not fenced", func(t *testing.T) {
		f, hs, peer := newFenced(t, false)

		// The duplicate early return in AcceptBlockHeader runs before the
		// fence, so our own history is never refused back to us.
		replay := f.chain[forkHeight-1 : forkHeight+2]

		msgs, misbehavior, err := hs.OnHeaders(peer, headersMsg(replay))
		require.NoError(t, err)
		require.Zero(t, misbehavior)
		require.Nil(t, msgs)
	})

	t.Run("disabled checkpoints lift the fence", func(t *testing.T) {
		f, hs, peer := newFenced(t, true)

		// fCheckpointsEnabled gates CheckIndexAgainstCheckpoint entirely.
		fork := minedChild(f.chain[forkHeight-1], testEasyBits, 7003)

		msgs, misbehavior, err := hs.OnHeaders(peer, headersMsg([]*wire.BlockHeader{fork}))
		require.NoError(t, err)
		require.Zero(t, misbehavior)
		require.Nil(t, msgs)

		_, ok := f.idx.Lookup(fork.BlockHash())
		require.True(t, ok)
	})

	t.Run("a checkpoint we have not reached yet does not fence anything", func(t *testing.T) {
		f := newSyncFixture(t, fixtureHeight)

		// GetLastCheckpoint looks the checkpoint hash up in the index; a
		// checkpoint we never reached is not there, so no fence height exists.
		unknown := chainhash.Hash{0xC0}

		hs, err := NewHeaderSync(HeaderSyncConfig{
			Index:  f.idx,
			Params: syncTestParams([]chaincfg.Checkpoint{{Height: cpHeight, Hash: &unknown}}),
		})
		require.NoError(t, err)

		peer := fullNodePeer("1.2.3.4:8333")
		require.Len(t, hs.PeerEstablished(peer), 1)

		fork := minedChild(f.chain[forkHeight-1], testEasyBits, 7004)

		msgs, misbehavior, err := hs.OnHeaders(peer, headersMsg([]*wire.BlockHeader{fork}))
		require.NoError(t, err)
		require.Zero(t, misbehavior)
		require.Nil(t, msgs)

		_, ok := f.idx.Lookup(fork.BlockHash())
		require.True(t, ok)
	})

	t.Run("the highest checkpoint present in the index sets the fence", func(t *testing.T) {
		f := newSyncFixture(t, fixtureHeight)

		low := f.chain[cpHeight-1].BlockHash()
		high := f.chain[fixtureHeight-1].BlockHash()

		hs, err := NewHeaderSync(HeaderSyncConfig{
			Index: f.idx,
			Params: syncTestParams([]chaincfg.Checkpoint{
				{Height: cpHeight, Hash: &low},
				{Height: fixtureHeight, Hash: &high},
			}),
		})
		require.NoError(t, err)

		peer := fullNodePeer("1.2.3.4:8333")
		require.Len(t, hs.PeerEstablished(peer), 1)

		// Between the two checkpoints: refused, because the higher one is the
		// last checkpoint the index holds.
		fork := minedChild(f.chain[cpHeight+4], testEasyBits, 7005)

		_, _, err = hs.OnHeaders(peer, headersMsg([]*wire.BlockHeader{fork}))
		require.Error(t, err)
		require.True(t, errors.Is(err, ErrCheckpointFork))
		require.Contains(t, err.Error(), "120")
	})
}

// TestHeaderSync_HydrationBypassesContextualChecks pins the deliberate split
// between the two ways a header reaches the index: PeerManager.AddHeaders, the
// path Server hydration and the blockchain subscription use, carries headers
// our own validated store already accepted and runs none of the checks
// acceptHeaders applies to a peer's headers.
func TestHeaderSync_HydrationBypassesContextualChecks(t *testing.T) {
	const fixtureHeight = 200

	f := newDaaSyncFixture(t, fixtureHeight)

	tip, ok := f.idx.Lookup(f.tip().BlockHash())
	require.True(t, ok)

	cpHash := f.chain[fixtureHeight/2-1].BlockHash()
	params := daaSyncParams([]chaincfg.Checkpoint{{Height: fixtureHeight / 2, Hash: &cpHash}})

	// Two headers a peer could never get past acceptHeaders: one claiming the
	// wrong contextual difficulty, one forking below the last checkpoint.
	wrongBits := timedChild(f.tip(), testEasyBits-1, 8001, tip.Time+600)
	fork := timedChild(f.chain[9], testEasyBits, 8002, daaFixtureBaseTime+11*600)

	hs, peer := startedSyncWith(t, f, params)

	_, misbehavior, err := hs.OnHeaders(peer, headersMsg([]*wire.BlockHeader{wrongBits}))
	require.NoError(t, err)
	require.Equal(t, scoreBadDiffBits, misbehavior)

	_, _, err = hs.OnHeaders(peer, headersMsg([]*wire.BlockHeader{fork}))
	require.Error(t, err)
	require.True(t, errors.Is(err, ErrCheckpointFork))

	for _, h := range []*wire.BlockHeader{wrongBits, fork} {
		_, indexed := f.idx.Lookup(h.BlockHash())
		require.False(t, indexed)
	}

	// The same two headers arriving through the hydration path are indexed.
	manager := newTestManager(t, nil)
	require.NoError(t, manager.ConfigureSync(SyncConfig{Index: f.idx}))

	orphans, err := manager.AddHeaders([]*wire.BlockHeader{wrongBits, fork})
	require.NoError(t, err)
	require.Empty(t, orphans)

	for _, h := range []*wire.BlockHeader{wrongBits, fork} {
		_, indexed := f.idx.Lookup(h.BlockHash())
		require.True(t, indexed, "the hydration path must index what our own store already accepted")
	}
}

// fixedClock returns an AdjustedTime function pinned to ts, so a
// time-too-new row is decided by arithmetic rather than by the wall clock.
func fixedClock(ts int64) func() int64 {
	return func() int64 { return ts }
}

// TestHeaderSync_TimeTooNew pins ContextualCheckBlockHeader's timestamp cap
// (validation.cpp:5909-5913): a header more than MAX_FUTURE_BLOCK_TIME past
// the adjusted time is refused. state.Invalid carries no DoS score, so the
// peer is neither scored nor disconnected.
func TestHeaderSync_TimeTooNew(t *testing.T) {
	const fixtureHeight = 200

	// The clock is pinned to the fixture tip's own timestamp, so every row
	// below reads as an offset from the chain's own notion of now.
	newAtTip := func(t *testing.T) (*syncFixture, *HeaderSync, *SyncPeer, HeaderNode) {
		t.Helper()

		f := newDaaSyncFixture(t, fixtureHeight)

		tip, ok := f.idx.Lookup(f.tip().BlockHash())
		require.True(t, ok)

		hs, err := NewHeaderSync(HeaderSyncConfig{
			Index:        f.idx,
			Params:       daaSyncParams(nil),
			AdjustedTime: fixedClock(tip.Time),
		})
		require.NoError(t, err)

		peer := fullNodePeer("1.2.3.4:8333")
		require.Len(t, hs.PeerEstablished(peer), 1)

		return f, hs, peer, tip
	}

	t.Run("a header exactly at the cap is accepted", func(t *testing.T) {
		f := newDaaSyncFixture(t, fixtureHeight)

		tip, ok := f.idx.Lookup(f.tip().BlockHash())
		require.True(t, ok)

		// The comparison is strict, so adjustedTime + MAX_FUTURE_BLOCK_TIME is
		// the last accepted second.
		at := timedChild(f.tip(), testEasyBits, 6001, tip.Time+MaxFutureBlockTime)

		hs, err := NewHeaderSync(HeaderSyncConfig{
			Index:        f.idx,
			Params:       daaSyncParams(nil),
			AdjustedTime: fixedClock(tip.Time),
		})
		require.NoError(t, err)

		peer := fullNodePeer("1.2.3.4:8333")
		require.Len(t, hs.PeerEstablished(peer), 1)

		msgs, misbehavior, err := hs.OnHeaders(peer, headersMsg([]*wire.BlockHeader{at}))
		require.NoError(t, err)
		require.Zero(t, misbehavior)
		require.Nil(t, msgs)

		_, ok = f.idx.Lookup(at.BlockHash())
		require.True(t, ok)
	})

	t.Run("a header one second past the cap is refused without a score", func(t *testing.T) {
		f, hs, peer, tip := newAtTip(t)

		past := timedChild(f.tip(), testEasyBits, 6002, tip.Time+MaxFutureBlockTime+1)
		require.True(t, hs.checkBlockHeaderPoW(past, past.BlockHash()))

		msgs, misbehavior, err := hs.OnHeaders(peer, headersMsg([]*wire.BlockHeader{past}))

		// state.Invalid is DoS(0): net_processing.cpp calls Misbehaving only
		// when nDoS > 0, so the peer keeps its score and its connection.
		require.NoError(t, err)
		require.Zero(t, misbehavior)

		// ProcessNewBlockHeaders failed, so the handler returns before the
		// availability update and before any continuation getheaders.
		require.Nil(t, msgs)

		_, ok := f.idx.Lookup(past.BlockHash())
		require.False(t, ok)

		_, tipHeight := f.idx.Tip()
		require.Equal(t, int32(fixtureHeight), tipHeight)
	})

	t.Run("a batch keeps the headers before the too-new one", func(t *testing.T) {
		f, hs, peer, tip := newAtTip(t)

		good := timedChild(f.tip(), testEasyBits, 6003, tip.Time+600)
		tooNew := timedChild(good, testEasyBits, 6004, tip.Time+MaxFutureBlockTime+1)

		msgs, misbehavior, err := hs.OnHeaders(peer, headersMsg([]*wire.BlockHeader{good, tooNew}))
		require.NoError(t, err)
		require.Zero(t, misbehavior)
		require.Nil(t, msgs)

		_, ok := f.idx.Lookup(good.BlockHash())
		require.True(t, ok)

		_, ok = f.idx.Lookup(tooNew.BlockHash())
		require.False(t, ok)
	})

	t.Run("the refusal is not permanent", func(t *testing.T) {
		f := newDaaSyncFixture(t, fixtureHeight)

		tip, ok := f.idx.Lookup(f.tip().BlockHash())
		require.True(t, ok)

		ahead := timedChild(f.tip(), testEasyBits, 6005, tip.Time+MaxFutureBlockTime+600)

		now := tip.Time
		clock := &now

		hs, err := NewHeaderSync(HeaderSyncConfig{
			Index:        f.idx,
			Params:       daaSyncParams(nil),
			AdjustedTime: func() int64 { return *clock },
		})
		require.NoError(t, err)

		peer := fullNodePeer("1.2.3.4:8333")
		require.Len(t, hs.PeerEstablished(peer), 1)

		_, _, err = hs.OnHeaders(peer, headersMsg([]*wire.BlockHeader{ahead}))
		require.NoError(t, err)

		_, ok = f.idx.Lookup(ahead.BlockHash())
		require.False(t, ok)

		// Our clock advances past the header's timestamp; the same header now
		// passes. Nothing about the header changed, which is why the refusal
		// must not be a score or a disconnect.
		*clock = ahead.Timestamp.Unix()

		_, misbehavior, err := hs.OnHeaders(peer, headersMsg([]*wire.BlockHeader{ahead}))
		require.NoError(t, err)
		require.Zero(t, misbehavior)

		_, ok = f.idx.Lookup(ahead.BlockHash())
		require.True(t, ok)
	})

	t.Run("a nil clock defaults to the wall clock", func(t *testing.T) {
		f := newDaaSyncFixture(t, fixtureHeight)

		hs, err := NewHeaderSync(HeaderSyncConfig{Index: f.idx, Params: daaSyncParams(nil)})
		require.NoError(t, err)

		peer := fullNodePeer("1.2.3.4:8333")
		require.Len(t, hs.PeerEstablished(peer), 1)

		// A day ahead of any real clock this test can run under.
		ahead := timedChild(f.tip(), testEasyBits, 6006, time.Now().Add(24*time.Hour).Unix())

		msgs, misbehavior, err := hs.OnHeaders(peer, headersMsg([]*wire.BlockHeader{ahead}))
		require.NoError(t, err)
		require.Zero(t, misbehavior)
		require.Nil(t, msgs)

		_, ok := f.idx.Lookup(ahead.BlockHash())
		require.False(t, ok)
	})
}

// TestHeaderSync_MinDifficultyWalkIsDepthBounded pins the bound time-too-new
// actually buys on a ReduceMinDifficulty network, and pins its limit.
//
// The min-difficulty branch answers the powLimit whenever a header's timestamp
// is more than 2 * nPowTargetSpacing past its parent's, so a peer can chain
// cheap headers only by walking timestamps forward 1201 seconds at a time.
// time-too-new caps that walk at MAX_FUTURE_BLOCK_TIME, which is
// 7200 / 1201 = 5 headers deep from a tip at the current time.
//
// It bounds DEPTH, not WIDTH: the last row shows a peer can still put
// arbitrarily many cheap siblings on one parent, because each needs only a
// distinct hash and a timestamp inside the same window. SVNode carries the
// same exposure on its min-difficulty networks — this is parity, not a fix.
func TestHeaderSync_MinDifficultyWalkIsDepthBounded(t *testing.T) {
	const fixtureHeight = 200

	f := newDaaSyncFixture(t, fixtureHeight)

	params := daaSyncParams(nil)
	params.ReduceMinDifficulty = true

	tip, ok := f.idx.Lookup(f.tip().BlockHash())
	require.True(t, ok)

	// Under the test powLimit the min-difficulty answer is testEasyBits, the
	// same bits the fixture already runs at, so a walk header is cheap.
	require.Equal(t, testEasyBits, compactFromTarget(params.PowLimit))

	hs, err := NewHeaderSync(HeaderSyncConfig{
		Index:        f.idx,
		Params:       params,
		AdjustedTime: fixedClock(tip.Time),
	})
	require.NoError(t, err)

	peer := fullNodePeer("1.2.3.4:8333")
	require.Len(t, hs.PeerEstablished(peer), 1)

	// Walk forward one step past the min-difficulty threshold each time.
	step := int64(2*int64(params.TargetTimePerBlock/time.Second)) + 1

	parent := f.tip()
	accepted := 0

	for i := 0; i < 50; i++ {
		next := timedChild(parent, testEasyBits, uint32(5000+i), tip.Time+step*int64(i+1)) //nolint:gosec // test salt

		_, misbehavior, onErr := hs.OnHeaders(peer, headersMsg([]*wire.BlockHeader{next}))
		require.NoError(t, onErr)
		require.Zero(t, misbehavior)

		if _, indexed := f.idx.Lookup(next.BlockHash()); !indexed {
			break
		}

		accepted++
		parent = next
	}

	// floor(7200 / 1201) = 5 steps land at or under the cap; the sixth is
	// 7206 seconds ahead and is refused.
	require.Equal(t, 5, accepted)

	// WIDTH IS NOT BOUNDED. Siblings of one parent all sit inside the same
	// window, so the cap never fires for them.
	for i := 0; i < 20; i++ {
		sibling := timedChild(f.tip(), testEasyBits, uint32(6100+i), tip.Time+step) //nolint:gosec // test salt

		_, misbehavior, onErr := hs.OnHeaders(peer, headersMsg([]*wire.BlockHeader{sibling}))
		require.NoError(t, onErr)
		require.Zero(t, misbehavior)

		_, indexed := f.idx.Lookup(sibling.BlockHash())
		require.True(t, indexed, "sibling %d", i)
	}
}
