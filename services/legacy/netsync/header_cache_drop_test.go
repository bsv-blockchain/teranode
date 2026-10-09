package netsync

import (
	"bytes"
	"context"
	"os"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-chaincfg"
	"github.com/bsv-blockchain/go-wire"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/expiringmap"
	"github.com/stretchr/testify/require"
)

// testnetHeadersAboveGenesis is testnet's real headers 1 to 547, for a trunk
// that holds only the store's own genesis.
func testnetHeadersAboveGenesis(t *testing.T) []*wire.BlockHeader {
	t.Helper()

	data, err := os.ReadFile("../../blockchain/testdata/testnet_headers_0_547.bin")
	require.NoError(t, err)

	headers := make([]*wire.BlockHeader, 0, 547)

	for i := 80; i < len(data); i += 80 {
		var header wire.BlockHeader
		require.NoError(t, header.Deserialize(bytes.NewReader(data[i:i+80])))
		headers = append(headers, &header)
	}

	return headers
}

// stallOnce makes the first GetBlockHeader for hash after arm() block until
// release is closed or the call's context ends, and reports on entered when it
// starts blocking.
type stallOnce struct {
	trunk   *faultyTrunk
	entered chan struct{}
	release chan struct{}
	armed   bool
}

func newStallOnce(trunk *faultyTrunk, hash chainhash.Hash) *stallOnce {
	s := &stallOnce{trunk: trunk, entered: make(chan struct{}), release: make(chan struct{})}

	trunk.before = func(ctx context.Context, h chainhash.Hash) error {
		trunk.mu.Lock()
		stall := s.armed && h == hash
		if stall {
			s.armed = false
		}
		trunk.mu.Unlock()

		if !stall {
			return nil
		}

		close(s.entered)

		select {
		case <-s.release:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	return s
}

func (s *stallOnce) arm() {
	s.trunk.mu.Lock()
	s.armed = true
	s.trunk.mu.Unlock()
}

// rulesCacheOverStalledTrunk is a cache judging testnet headers through the
// real rules over a sqlitememory trunk holding genesis, with a stall on the
// trunk's genesis lookup ready to arm. It has no proof-of-work ceiling, so a
// forged header reaches the rules (and the stalled lookup) instead of being
// refused before any store call.
func rulesCacheOverStalledTrunk(t *testing.T) (*headerCache, *stallOnce, []*wire.BlockHeader, chainhash.Hash) {
	t.Helper()

	params := chaincfg.TestNetParams
	trunk := newRealTrunk(t, &params, 0, nil)
	faulty := &faultyTrunk{HeaderSource: trunk.client}
	genesis := *params.GenesisHash
	stall := newStallOnce(faulty, genesis)

	t.Cleanup(func() {
		select {
		case <-stall.release:
		default:
			close(stall.release)
		}
	})

	rules, err := newHeaderRules(ulogger.TestLogger{}, trunk.settings, &params, faulty)
	require.NoError(t, err)

	rules.now = time.Now

	cache := newHeaderCache().
		WithCheckpoints(params.Checkpoints).
		WithHeaderRules(rules)

	return cache, stall, testnetHeadersAboveGenesis(t), genesis
}

// DropPeer runs on the block handler's goroutine at every peer departure, so it
// must not wait for a fill stuck in a blockchain call. It returns at once, the
// departed peer's branch is gone from every reader at once, and its headers are
// freed when the stuck fill finishes.
func TestHeaderCacheDropPeer_ReturnsWhileAFillIsInAStoreCall(t *testing.T) {
	cache, stall, headers, genesis := rulesCacheOverStalledTrunk(t)

	require.True(t, cache.FillFrom("A", genesis, 1, headers[:100]).accepted)
	require.Equal(t, 100, cache.heldHeaders())

	// B's batch forks from genesis, so its fill reads genesis from the trunk,
	// and that read stalls while the fill holds fillMu.
	stall.arm()

	fork, _ := forgedRun(genesis, 3)
	fillDone := make(chan fillResult, 1)

	go func() { fillDone <- cache.FillFrom("B", genesis, 1, fork) }()

	<-stall.entered

	dropped := make(chan struct{})

	go func() {
		cache.DropPeer("A")
		close(dropped)
	}()

	select {
	case <-dropped:
	case <-time.After(5 * time.Second):
		t.Fatal("DropPeer waited on a fill stuck in a store call")
	}

	_, _, ok := cache.PeerTop("A")
	require.False(t, ok, "the departed peer's branch is gone at once")

	_, ok = cache.At(1)
	require.False(t, ok, "no reader sees the departed peer's headers")

	close(stall.release)

	<-fillDone

	require.Zero(t, cache.heldHeaders(), "the stuck fill frees the dropped branch's headers when it finishes")
}

// A fill whose store call never answers gives up at the deadline, refuses the
// batch as unjudgeable, and blames nobody.
func TestHeaderCacheFill_AStalledStoreCallEndsAtTheDeadline(t *testing.T) {
	cache, stall, headers, genesis := rulesCacheOverStalledTrunk(t)
	cache.storeTimeout = 100 * time.Millisecond

	stall.arm()

	fillDone := make(chan fillResult, 1)

	go func() { fillDone <- cache.FillFrom("A", genesis, 1, headers[:10]) }()

	var result fillResult

	select {
	case result = <-fillDone:
	case <-time.After(5 * time.Second):
		t.Fatal("a fill stuck in a store call held fillMu past its deadline")
	}

	require.Equal(t, rejectUnjudgeable, result.rejection)
	require.False(t, result.rejection.disconnects())
	require.False(t, result.accepted)
	require.Zero(t, cache.heldHeaders())
}

// A fill that was running when its peer left installs nothing for it: the peer
// is gone from peerStates before its branch is dropped, and the fill checks
// that before it installs, so no branch is left that no DropPeer will come for.
func TestFillHeaderCache_AFillForADepartedPeerInstallsNothing(t *testing.T) {
	cache, stall, headers, genesis := rulesCacheOverStalledTrunk(t)

	sm := newHeaderCacheManager(t)
	sm.headerCache = cache.WithOwnerLive(sm.headerOwnerLive)

	peer, _, _ := connectRacePeer(t, 248, 1000)
	sm.peerStates.Set(peer, &peerSyncState{requestedTxns: expiringmap.New[chainhash.Hash, struct{}](time.Hour)})

	stall.arm()

	fillDone := make(chan fillResult, 1)

	go func() { fillDone <- sm.headerCache.FillFrom(sm.headerOwner(peer), genesis, 1, headers[:10]) }()

	<-stall.entered

	sm.handleDonePeerMsg(peer)

	close(stall.release)

	result := <-fillDone
	require.Equal(t, headerAccepted, result.rejection, "%s: %s", result.rejection, result.detail)
	require.False(t, result.accepted)

	_, _, ok := sm.headerCache.PeerTop(peer)
	require.False(t, ok)
	require.Zero(t, sm.headerCache.heldHeaders(), "nothing is held for a departed peer")

	// A fill that starts after the peer left installs nothing either.
	result = sm.headerCache.FillFrom(peer, genesis, 1, headers[:10])
	require.False(t, result.accepted)
	require.Zero(t, sm.headerCache.heldHeaders())
}
