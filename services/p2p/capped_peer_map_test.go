package p2p

import (
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/stretchr/testify/require"
)

// TestCappedPeerMapFloodBounded pins issue 1409: a distinct-hash flood must
// not grow the attribution map past its cap between sweeps. Before the inline
// cap, every announcement inserted unconditionally and only the timer-driven
// sweep clawed the map back, so peak memory tracked the flood size and the
// sweep's full-map sort scaled with it.
func TestCappedPeerMapFloodBounded(t *testing.T) {
	var m cappedPeerMap

	m.setMaxSize(100)

	now := time.Now()
	for i := 0; i < 250; i++ {
		m.Store(fmt.Sprintf("hash-%d", i), peerMapEntry{peerID: "attacker", timestamp: now})
	}

	require.Equal(t, 100, m.Len(), "map must not grow past its cap under a distinct-hash flood")

	// Eviction count is exactly the overflow — cost tracks the flood only in
	// the sense that each surplus insert drops exactly one entry, never more.
	evictions := m.EvictionsSinceLastRead()
	require.Equal(t, int64(150), evictions.total)
	require.Equal(t, "attacker", evictions.topPeer, "a single flooder must be named")
	require.Equal(t, int64(150), evictions.topCount)
	require.False(t, evictions.spread)
	require.Equal(t, "all from peer attacker", evictions.String())

	require.Equal(t, int64(0), m.EvictionsSinceLastRead().total, "counter must reset on read")
}

// TestCappedPeerMapEvictionAttribution pins the signal the at-capacity warning
// relies on: an operator has to be able to tell sustained legitimate
// throughput (pressure spread across peers, where raising the cap helps) from
// one peer spraying hashes (where raising the cap only buys the attacker more
// memory — issue 1503). The tracker is itself bounded, so past
// evictorTrackLimit contributors it reports the spread instead of a name.
func TestCappedPeerMapEvictionAttribution(t *testing.T) {
	now := time.Now()

	t.Run("one dominant contributor is named", func(t *testing.T) {
		var m cappedPeerMap

		m.setMaxSize(10)

		for i := 0; i < 100; i++ {
			peerID := "busy-peer"
			if i%10 == 0 {
				peerID = "quiet-peer"
			}

			m.Store(fmt.Sprintf("hash-%d", i), peerMapEntry{peerID: peerID, timestamp: now})
		}

		evictions := m.EvictionsSinceLastRead()
		require.Equal(t, int64(90), evictions.total)
		require.Equal(t, "busy-peer", evictions.topPeer)
		require.False(t, evictions.spread)
		require.Contains(t, evictions.String(), "top contributor peer busy-peer")
	})

	t.Run("a balanced few contributors report as spread, not as a flooder", func(t *testing.T) {
		var m cappedPeerMap

		m.setMaxSize(10)

		// Few enough contributors that the tracker never overflows, so the
		// counts are exact and spread stays unset. The busiest of them still
		// holds well under half the evictions, which is the whole question:
		// the verdict must follow the share, not the number of contributors.
		// Naming a plurality here would point the operator at the busiest
		// honest peer with the wording the setting's guidance reads as "ban
		// this peer".
		const contributors = 4

		for i := 0; i < 100; i++ {
			m.Store(fmt.Sprintf("hash-%d", i), peerMapEntry{
				peerID:    fmt.Sprintf("peer-%d", i%contributors),
				timestamp: now,
			})
		}

		evictions := m.EvictionsSinceLastRead()
		require.Equal(t, int64(90), evictions.total)
		require.False(t, evictions.spread, "four contributors must fit the tracker")
		require.NotEmpty(t, evictions.topPeer, "exact counts must still name the largest contributor")
		require.LessOrEqual(t, evictions.topCount*2, evictions.total, "no contributor holds a majority")

		require.Equal(t,
			fmt.Sprintf("spread across peers; largest contributor peer %s with %d of %d",
				evictions.topPeer, evictions.topCount, evictions.total),
			evictions.String(),
			"a plurality below a majority is throughput, whatever the contributor count")
	})

	t.Run("pressure spread past the tracking limit reports the spread", func(t *testing.T) {
		var m cappedPeerMap

		m.setMaxSize(10)

		for i := 0; i < 200; i++ {
			m.Store(fmt.Sprintf("hash-%d", i), peerMapEntry{
				peerID:    fmt.Sprintf("peer-%d", i%(evictorTrackLimit*4)),
				timestamp: now,
			})
		}

		evictions := m.EvictionsSinceLastRead()
		require.Equal(t, int64(190), evictions.total)
		require.True(t, evictions.spread, "more contributors than the tracker can name must report as spread")

		// Asserted exactly rather than by substring: several arms of String
		// open with "spread across", so a loose match would keep passing if a
		// future edit routed this case through a different one.
		require.Equal(t,
			fmt.Sprintf("spread across more than %d peers; largest tracked contributor peer %s with at least %d of %d",
				evictorTrackLimit, evictions.topPeer, evictions.topCount, evictions.total),
			evictions.String())

		// The tracker itself must stay bounded: that is the failure mode of
		// issue 1409, and a diagnostic is no exception.
		m.mu.Lock()
		require.LessOrEqual(t, len(m.evictors), evictorTrackLimit)
		m.mu.Unlock()
	})

	t.Run("a long tail with no heavy hitter names nobody", func(t *testing.T) {
		var m cappedPeerMap

		const capacity = 10

		m.setMaxSize(capacity)

		// Every contributor distinct and none repeating is the case Misra-Gries
		// cancels out entirely: sixteen peers fill the tracker, the seventeenth
		// decrements all of them to zero, and the cycle repeats. Landing on an
		// exact multiple of that cycle leaves the tracker empty, which is not a
		// failure to attribute but the answer itself — pressure with no heavy
		// hitter is throughput. It is the one case where the verdict has no
		// name to offer, so it has its own line.
		const rounds = 11

		evictions := (evictorTrackLimit + 1) * rounds

		for i := 0; i < capacity+evictions; i++ {
			m.Store(fmt.Sprintf("hash-%d", i), peerMapEntry{
				peerID:    fmt.Sprintf("one-shot-peer-%d", i),
				timestamp: now,
			})
		}

		stats := m.EvictionsSinceLastRead()
		require.Equal(t, int64(evictions), stats.total)
		require.True(t, stats.spread)
		require.Empty(t, stats.topPeer, "a uniform long tail must leave no survivor to name")
		require.Equal(t,
			fmt.Sprintf("spread across more than %d peers", evictorTrackLimit),
			stats.String())
	})

	t.Run("cheap identities cannot crowd the flooder out of the tracker", func(t *testing.T) {
		var m cappedPeerMap

		m.setMaxSize(10)

		for i := 0; i < 10; i++ {
			m.Store(fmt.Sprintf("filler-%d", i), peerMapEntry{peerID: "filler", timestamp: now})
		}

		require.Equal(t, int64(0), m.EvictionsSinceLastRead().total, "filling to the cap must not evict")

		// Peer IDs are free and connection limits are unbounded (issue 1163), so
		// an attacker can seed the tracker with throwaway identities before it
		// starts flooding. First-come-first-served tracking would then never
		// admit the flooder at all, and the warning would report spread no
		// matter how dominant the flood became.
		for i := 0; i < evictorTrackLimit; i++ {
			m.Store(fmt.Sprintf("sybil-%d", i), peerMapEntry{
				peerID:    fmt.Sprintf("sybil-peer-%d", i),
				timestamp: now,
			})
		}

		for i := 0; i < 100; i++ {
			m.Store(fmt.Sprintf("junk-%d", i), peerMapEntry{peerID: "attacker", timestamp: now})
		}

		evictions := m.EvictionsSinceLastRead()
		require.Equal(t, int64(evictorTrackLimit+100), evictions.total)
		require.Equal(t, "attacker", evictions.topPeer, "the flooder must displace the throwaway identities")
		require.Contains(t, evictions.String(), "top contributor peer attacker")
		require.NotContains(t, evictions.String(), "spread across")

		// A counted share is never inflated, so the dominance verdict cannot be
		// an accusation the evidence does not support.
		require.LessOrEqual(t, evictions.topCount, int64(100))

		m.mu.Lock()
		require.LessOrEqual(t, len(m.evictors), evictorTrackLimit)
		m.mu.Unlock()
	})

	t.Run("a dominant flooder is still named when the tracker overflowed", func(t *testing.T) {
		var m cappedPeerMap

		m.setMaxSize(10)

		// The attacker forces the bulk of the evictions.
		for i := 0; i < 100; i++ {
			m.Store(fmt.Sprintf("junk-%d", i), peerMapEntry{peerID: "attacker", timestamp: now})
		}

		// Then enough distinct honest peers arrive to overflow the tracker. At
		// capacity every new hash evicts, so on a real mesh this happens within
		// one sweep window whenever the node has more peers than the tracker can
		// name — which must not be allowed to hide the flooder behind "spread".
		for i := 0; i < evictorTrackLimit+4; i++ {
			m.Store(fmt.Sprintf("honest-%d", i), peerMapEntry{
				peerID:    fmt.Sprintf("honest-peer-%d", i),
				timestamp: now,
			})
		}

		evictions := m.EvictionsSinceLastRead()
		require.True(t, evictions.spread, "more contributors than the tracker can name must set spread")
		require.Equal(t, "attacker", evictions.topPeer)
		require.Greater(t, evictions.topCount*2, evictions.total, "the attacker must hold a majority share")

		// total is exact even under overflow, so a majority share cannot be an
		// artefact of which peers the tracker happened to name first. Reporting
		// "spread" here would tell the operator to raise the cap, which is the
		// one response issue 1503 says is wrong for a flood.
		require.Contains(t, evictions.String(), "top contributor peer attacker")
		require.NotContains(t, evictions.String(), "spread across")
	})

	t.Run("the flooder is named, not the honest peers it displaced", func(t *testing.T) {
		var m cappedPeerMap

		m.setMaxSize(10)

		// Honest peers fill the map first, so every entry the flood evicts
		// belongs to one of them.
		for i := 0; i < 10; i++ {
			m.Store(fmt.Sprintf("honest-%d", i), peerMapEntry{
				peerID:    fmt.Sprintf("honest-peer-%d", i),
				timestamp: now,
			})
		}

		require.Equal(t, int64(0), m.EvictionsSinceLastRead().total, "filling to the cap must not evict")

		// Flood less than the cap, so every entry dropped belongs to an honest
		// peer and none of the attacker's own entries are reached.
		for i := 0; i < 8; i++ {
			m.Store(fmt.Sprintf("junk-%d", i), peerMapEntry{peerID: "attacker", timestamp: now})
		}

		// Attribution must follow the insert that forced the eviction, not the
		// entry that was dropped: blaming the dropped entries would name eight
		// honest peers, one eviction each, and point the operator at the
		// victims of the flood rather than its source.
		evictions := m.EvictionsSinceLastRead()
		require.Equal(t, int64(8), evictions.total)
		require.Equal(t, "attacker", evictions.topPeer, "the peer applying the pressure must be named")
		require.Equal(t, int64(8), evictions.topCount)
		require.False(t, evictions.spread)
	})

	t.Run("attribution resets with the counter", func(t *testing.T) {
		var m cappedPeerMap

		m.setMaxSize(2)

		for i := 0; i < 10; i++ {
			m.Store(fmt.Sprintf("hash-%d", i), peerMapEntry{peerID: "attacker", timestamp: now})
		}

		require.Equal(t, int64(8), m.EvictionsSinceLastRead().total)

		next := m.EvictionsSinceLastRead()
		require.Equal(t, int64(0), next.total)
		require.Empty(t, next.topPeer)
		require.Equal(t, "none", next.String())
	})
}

// TestCappedPeerMapKeepsNewestUnderFlood pins the security-critical direction
// of the eviction policy: a flooder that fills every slot with junk must NOT
// be able to suppress attribution for the announcement that arrives next.
// Refusing new keys at capacity would let an attacker switch off the node's
// only automatic ban path for invalid blocks; evicting the oldest keeps the
// newest announcement attributable.
func TestCappedPeerMapKeepsNewestUnderFlood(t *testing.T) {
	var m cappedPeerMap

	m.setMaxSize(50)

	now := time.Now()
	for i := 0; i < 500; i++ {
		m.Store(fmt.Sprintf("junk-%d", i), peerMapEntry{peerID: "attacker", timestamp: now})
	}

	// The honest announcement arriving after the flood must be recorded.
	m.Store("real-block", peerMapEntry{peerID: "honest-peer", timestamp: now})

	entry, ok := m.Load("real-block")
	require.True(t, ok, "announcement after a full-map flood must still be attributable")
	require.Equal(t, "honest-peer", entry.peerID)
	require.Equal(t, 50, m.Len())

	// The oldest junk is what got dropped.
	_, ok = m.Load("junk-0")
	require.False(t, ok, "oldest entries are the ones evicted")
}

// TestStorePeerMapEntryKeepsAttributionUnderFlood drives the production insert
// path: after a flood through storePeerMapEntry fills the map, the next
// announcement must still be attributable via the same lookup the
// invalid-block ban path uses.
func TestStorePeerMapEntryKeepsAttributionUnderFlood(t *testing.T) {
	s := &Server{logger: ulogger.TestLogger{}}
	s.blockPeerMap.setMaxSize(20)

	now := time.Now()
	for i := 0; i < 200; i++ {
		s.storePeerMapEntry(&s.blockPeerMap, fmt.Sprintf("%064d", i), "attacker", now)
	}

	require.Equal(t, 20, s.blockPeerMap.Len(), "gossip inserts must not grow the map past the cap")

	realHash := fmt.Sprintf("%064d", 999999)
	s.storePeerMapEntry(&s.blockPeerMap, realHash, "peer-to-ban", now)

	peerID, err := s.getPeerFromMap(&s.blockPeerMap, realHash, "block")
	require.NoError(t, err, "the ban path must still find the announcing peer after a flood")
	require.Equal(t, "peer-to-ban", peerID)
}

// TestStorePeerMapEntryKeepsSubtreeAttributionUnderFlood mirrors the block-side
// flood test on the subtree path, through the same two helpers the handlers
// use. The subtree map is the one that realistically sits at capacity — by the
// sizing analysis roughly 16 subtrees a second fills the default cap for a
// whole TTL window — and storePeerMapEntry(&s.subtreePeerMap, …) has exactly
// one call site, all of it production code, so nothing else would catch the
// subtree handler being pointed at the wrong map.
func TestStorePeerMapEntryKeepsSubtreeAttributionUnderFlood(t *testing.T) {
	s := &Server{logger: ulogger.TestLogger{}}
	s.subtreePeerMap.setMaxSize(20)

	now := time.Now()
	for i := 0; i < 200; i++ {
		s.storePeerMapEntry(&s.subtreePeerMap, fmt.Sprintf("%064d", i), "attacker", now)
	}

	require.Equal(t, 20, s.subtreePeerMap.Len(), "gossip inserts must not grow the subtree map past the cap")
	require.Equal(t, 0, s.blockPeerMap.Len(), "the subtree path must not touch the block map")

	realHash := fmt.Sprintf("%064d", 999999)
	s.storePeerMapEntry(&s.subtreePeerMap, realHash, "peer-to-ban", now)

	peerID, err := s.getPeerFromMap(&s.subtreePeerMap, realHash, "subtree")
	require.NoError(t, err, "the ban path must still find the announcing peer after a flood")
	require.Equal(t, "peer-to-ban", peerID)
}

// TestCappedPeerMapCostBoundedByCapNotFlood pins the other half of what issue
// 1409 asks for: not just that the map stays small, but that the work done to
// keep it small does not scale with the flood.
//
// Two properties, both deterministic. Inserting at capacity does a fixed
// number of allocations regardless of how many hashes have already been thrown
// at the map — the old scheme let the surplus accumulate and then paid for a
// full-map sort. And the sweep, the only whole-map walk left, allocates
// nothing and visits at most cap entries however large the flood was; the
// removed sort.Slice snapshotted and sorted every one of them.
func TestCappedPeerMapCostBoundedByCapNotFlood(t *testing.T) {
	const (
		maxSize    = 64
		smallFlood = 1_000
		largeFlood = 100_000
	)

	now := time.Now()

	// Pre-generate keys so the measured closure allocates nothing of its own.
	keys := make([]string, largeFlood+2_000)
	for i := range keys {
		keys[i] = fmt.Sprintf("hash-%d", i)
	}

	newFloodedMap := func(flood int) *cappedPeerMap {
		m := &cappedPeerMap{}
		m.setMaxSize(maxSize)

		for i := 0; i < flood; i++ {
			m.Store(keys[i], peerMapEntry{peerID: "attacker", timestamp: now})
		}

		require.Equal(t, maxSize, m.Len(), "a flood of %d must leave exactly the cap behind", flood)

		return m
	}

	allocsPerStoreAfter := func(flood int) float64 {
		m := newFloodedMap(flood)
		next := flood

		return testing.AllocsPerRun(1_000, func() {
			m.Store(keys[next], peerMapEntry{peerID: "attacker", timestamp: now})
			next++
		})
	}

	small := allocsPerStoreAfter(smallFlood)
	large := allocsPerStoreAfter(largeFlood)
	require.InDelta(t, small, large, 0.5,
		"insert cost at capacity must not depend on how many hashes were flooded before it")

	// The sweep is the only whole-map walk left. However large the flood was,
	// it visits at most cap entries and allocates nothing — where the removed
	// sort.Slice snapshotted and sorted every entry the flood had produced.
	m := newFloodedMap(largeFlood)

	walkAllocs := testing.AllocsPerRun(100, func() {
		// Cutoff in the past: walks all maxSize entries, removes none, so
		// every run measures exactly the same work.
		m.DeleteExpired(now.Add(-time.Hour))
	})
	require.Zero(t, walkAllocs, "the sweep's whole-map walk must allocate nothing")

	require.Equal(t, maxSize, m.DeleteExpired(now.Add(time.Hour)),
		"the sweep touches at most cap entries after a %d-hash flood", largeFlood)
	require.Equal(t, 0, m.Len())
}

// BenchmarkCappedPeerMapStoreUnderFlood is the wall-clock companion to
// TestCappedPeerMapCostBoundedByCapNotFlood: ns/op must stay flat as the flood
// grows by two orders of magnitude. Against the removed sweep-time sort it
// would not have.
func BenchmarkCappedPeerMapStoreUnderFlood(b *testing.B) {
	now := time.Now()

	for _, flood := range []int{1000, 100000} {
		b.Run(fmt.Sprintf("flood=%d", flood), func(b *testing.B) {
			var m cappedPeerMap

			m.setMaxSize(defaultPeerMapMaxSize)

			keys := make([]string, flood)
			for i := range keys {
				keys[i] = fmt.Sprintf("hash-%d", i)
			}

			for i := 0; i < flood; i++ {
				m.Store(keys[i], peerMapEntry{peerID: "attacker", timestamp: now})
			}

			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				m.Store(keys[i%flood], peerMapEntry{peerID: "attacker", timestamp: now})
			}
		})
	}
}

// TestCappedPeerMapUpdateAndDelete pins the non-growth operations: updating an
// existing key refreshes it (and its recency) without evicting, and a delete
// frees a slot.
func TestCappedPeerMapUpdateAndDelete(t *testing.T) {
	var m cappedPeerMap

	m.setMaxSize(2)

	now := time.Now()
	m.Store("a", peerMapEntry{peerID: "p1", timestamp: now})
	m.Store("b", peerMapEntry{peerID: "p1", timestamp: now})

	// Update 'a': no eviction, and 'a' becomes the most recent. The peer also
	// changes, which must move the node between per-peer lists — the fair-share
	// accounting reads their lengths.
	m.Store("a", peerMapEntry{peerID: "p2", timestamp: now})
	require.Equal(t, 2, m.Len())
	require.Equal(t, int64(0), m.EvictionsSinceLastRead().total, "updating an existing key must not evict")
	requireMapConsistent(t, &m)

	entry, ok := m.Load("a")
	require.True(t, ok)
	require.Equal(t, "p2", entry.peerID)

	// A new key now evicts 'b', which is the oldest after 'a' was refreshed.
	m.Store("c", peerMapEntry{peerID: "p3", timestamp: now})
	_, ok = m.Load("b")
	require.False(t, ok, "refreshed key must outlive the un-refreshed one")
	_, ok = m.Load("a")
	require.True(t, ok)

	m.Delete("a")
	require.Equal(t, 1, m.Len())
	requireMapConsistent(t, &m)

	// 'a' was p2's only entry, so its per-peer list must be gone: a lingering
	// empty list would inflate the fair-share denominator forever.
	m.mu.Lock()
	_, p2Listed := m.perPeer["p2"]
	m.mu.Unlock()
	require.False(t, p2Listed, "a peer's list must be dropped with its last entry")
}

// TestCappedPeerMapZeroValueIsBounded pins that the control fails CLOSED. The
// zero value is reachable from any bare Server literal, and from any future
// construction path that forgets applyPeerMapLimits; if that meant "unbounded"
// then the one guarantee this type exists to provide would be off by default
// and every test would still be green. An unconfigured map takes the default
// cap instead — there is no unbounded mode.
func TestCappedPeerMapZeroValueIsBounded(t *testing.T) {
	var m cappedPeerMap

	now := time.Now()
	for i := 0; i < defaultPeerMapMaxSize+250; i++ {
		m.Store(fmt.Sprintf("hash-%d", i), peerMapEntry{peerID: "p", timestamp: now})
	}

	require.Equal(t, defaultPeerMapMaxSize, m.Len(), "an unconfigured map must still be bounded")
	require.Equal(t, int64(250), m.EvictionsSinceLastRead().total)

	// A non-positive cap is not an opt-out either.
	m.setMaxSize(0)
	m.Store("still-bounded", peerMapEntry{peerID: "p", timestamp: now})
	require.Equal(t, defaultPeerMapMaxSize, m.Len())

	m.Clear()
	require.Equal(t, 0, m.Len())

	// Usable again after Clear.
	m.Store("after-clear", peerMapEntry{peerID: "p", timestamp: now})
	require.Equal(t, 1, m.Len())
	requireMapConsistent(t, &m)
}

// TestCappedPeerMapConvergesWhenOverCap pins that a map holding more than its
// cap drains back down instead of sitting over-cap forever. Evicting exactly
// one entry per insert would hold it at the same size indefinitely, and the
// sweep no longer has a size pass to correct that.
func TestCappedPeerMapConvergesWhenOverCap(t *testing.T) {
	var m cappedPeerMap

	m.setMaxSize(50)

	now := time.Now()
	for i := 0; i < 50; i++ {
		m.Store(fmt.Sprintf("hash-%d", i), peerMapEntry{peerID: "p", timestamp: now})
	}

	require.Equal(t, 50, m.Len())

	// Lower the cap on a populated map: the next insert must bring it all the
	// way down, not shave off a single entry.
	m.setMaxSize(5)
	m.Store("after-shrink", peerMapEntry{peerID: "p", timestamp: now})

	require.Equal(t, 5, m.Len(), "an over-cap map must converge to the new cap")
	requireMapConsistent(t, &m)
}

// requireMapConsistent asserts the map, its insertion-order list, and the
// per-peer lists all describe the same set of entries. They are three
// structures kept in step by hand, so a path that updates one and not the
// others would leak: entries orphaned in a list are invisible to Len() yet
// still hold memory, which is the very failure issue 1409 is about — and a
// per-peer list out of step miscounts a peer's fair share (issue 1503).
func requireMapConsistent(t *testing.T, m *cappedPeerMap) {
	t.Helper()

	m.mu.Lock()
	defer m.mu.Unlock()

	if m.order == nil {
		require.Empty(t, m.entries, "map must be empty when the order list is unset")
		require.Empty(t, m.perPeer, "per-peer lists must be empty when the order list is unset")
		return
	}

	require.Equal(t, len(m.entries), m.order.Len(), "order list and map must hold the same number of entries")

	perPeerTotal := 0
	for peerID, pl := range m.perPeer {
		require.Positive(t, pl.Len(), "peer %q has an empty per-peer list; it should have been dropped", peerID)
		perPeerTotal += pl.Len()

		for element := pl.Front(); element != nil; element = element.Next() {
			node := element.Value.(*peerMapNode)
			require.Equal(t, peerID, node.entry.peerID, "entry %q threaded onto peer %q's list", node.hash, peerID)
			require.Same(t, element, node.peerElem, "entry %q disagrees with its per-peer element", node.hash)
		}
	}
	require.Equal(t, len(m.entries), perPeerTotal, "per-peer lists and map must hold the same number of entries")

	for element := m.order.Front(); element != nil; element = element.Next() {
		node := element.Value.(*peerMapNode)

		stored, ok := m.entries[node.hash]
		require.True(t, ok, "order list holds %q but the map does not", node.hash)
		require.Same(t, node, stored, "map and order list disagree about %q", node.hash)
		require.Same(t, element, node.globalElem, "entry %q disagrees with its order-list element", node.hash)
	}
}

// TestCappedPeerMapConcurrent exercises the mutex this type introduced in place
// of a sync.Map. Run under -race it catches unsynchronised access; the
// consistency check afterwards catches the order-list leak that a race detector
// cannot see. Gossip drives every one of these operations concurrently in
// production: handlers store, the ban path loads, and the sweep ranges and
// deletes.
func TestCappedPeerMapConcurrent(t *testing.T) {
	const (
		maxSize    = 64
		iterations = 400
		storers    = 8
		deleters   = 4
		sweepers   = 2
	)

	var m cappedPeerMap

	m.setMaxSize(maxSize)

	now := time.Now()

	var wg sync.WaitGroup

	for g := 0; g < storers; g++ {
		wg.Add(1)

		go func(g int) {
			defer wg.Done()

			for i := 0; i < iterations; i++ {
				m.Store(fmt.Sprintf("hash-%d-%d", g, i), peerMapEntry{peerID: fmt.Sprintf("peer-%d", g), timestamp: now})
			}
		}(g)
	}

	for g := 0; g < deleters; g++ {
		wg.Add(1)

		go func(g int) {
			defer wg.Done()

			for i := 0; i < iterations; i++ {
				m.Delete(fmt.Sprintf("hash-%d-%d", g, i))
				m.Load(fmt.Sprintf("hash-%d-%d", g, i))
				m.Len()
			}
		}(g)
	}

	// The sweep's shape: a whole-map expiry pass, plus the eviction-counter
	// read that follows it, running against live inserts.
	for g := 0; g < sweepers; g++ {
		wg.Add(1)

		go func() {
			defer wg.Done()

			for i := 0; i < iterations/10; i++ {
				m.DeleteExpired(now.Add(-time.Hour))
				m.DeleteExpired(now.Add(time.Hour))
				m.EvictionsSinceLastRead()
			}
		}()
	}

	wg.Wait()

	require.LessOrEqual(t, m.Len(), maxSize, "the cap must hold under concurrent inserts")
	requireMapConsistent(t, &m)
}

// TestCappedPeerMapDeleteExpired pins the TTL sweep's single-pass expiry:
// entries older than the cutoff go, newer ones stay, and the structures stay
// in step.
func TestCappedPeerMapDeleteExpired(t *testing.T) {
	var m cappedPeerMap

	base := time.Now()
	for i := 0; i < 10; i++ {
		m.Store(fmt.Sprintf("hash-%d", i), peerMapEntry{peerID: "p", timestamp: base.Add(time.Duration(i) * time.Minute)})
	}

	// Cutoff at +5m expires hash-0..hash-4, whose timestamps precede it.
	require.Equal(t, 5, m.DeleteExpired(base.Add(5*time.Minute)))
	require.Equal(t, 5, m.Len())

	_, ok := m.Load("hash-4")
	require.False(t, ok, "an entry older than the cutoff must be expired")
	_, ok = m.Load("hash-5")
	require.True(t, ok, "an entry at the cutoff must survive")

	requireMapConsistent(t, &m)

	// Nothing left to expire is not an error, and a zero value is safe.
	require.Equal(t, 0, m.DeleteExpired(base))

	var zero cappedPeerMap

	require.Equal(t, 0, zero.DeleteExpired(base))
}

// TestCappedPeerMapClearRetainsMaxSize pins that Clear frees the entries but
// keeps the configured cap. Stop calls Clear, so a cap dropped here would leave
// the maps unbounded for the rest of the process's life.
func TestCappedPeerMapClearRetainsMaxSize(t *testing.T) {
	var m cappedPeerMap

	m.setMaxSize(3)

	now := time.Now()
	for i := 0; i < 3; i++ {
		m.Store(fmt.Sprintf("hash-%d", i), peerMapEntry{peerID: "p", timestamp: now})
	}

	// Evict a few so there is pressure recorded to carry across the Clear.
	for i := 0; i < 5; i++ {
		m.Store(fmt.Sprintf("pressure-%d", i), peerMapEntry{peerID: "attacker", timestamp: now})
	}

	m.Clear()
	require.Equal(t, 0, m.Len())

	// The counters described entries that no longer exist, so they go with
	// them: otherwise the sweep after a Clear reports pressure from before it.
	cleared := m.EvictionsSinceLastRead()
	require.Equal(t, int64(0), cleared.total, "Clear must drop the eviction counter")
	require.Empty(t, cleared.topPeer, "Clear must drop the eviction attribution")

	for i := 0; i < 50; i++ {
		m.Store(fmt.Sprintf("after-%d", i), peerMapEntry{peerID: "p", timestamp: now})
	}

	require.Equal(t, 3, m.Len(), "the cap must survive Clear")
	requireMapConsistent(t, &m)
}

// requireConfiguredCap asserts the cap a map was actually configured with,
// read under its own lock. Behavioural fills cannot substitute here: since the
// zero value now resolves to defaultPeerMapMaxSize, a map that lost its wiring
// still enforces the default, so only the configured value distinguishes
// "wired" from "fell back".
func requireConfiguredCap(t *testing.T, m *cappedPeerMap, want int) {
	t.Helper()

	m.mu.Lock()
	defer m.mu.Unlock()

	require.Equal(t, want, m.maxSize, "configured cap did not reach the map")
}

// TestApplyPeerMapLimits pins that the configured size actually reaches both
// attribution maps. Nothing else asserts it, so a refactor could drop the
// wiring and leave both maps silently on the fallback with every other test
// still green.
func TestApplyPeerMapLimits(t *testing.T) {
	now := time.Now()

	fill := func(t *testing.T, s *Server, n int) {
		t.Helper()

		for i := 0; i < n; i++ {
			s.blockPeerMap.Store(fmt.Sprintf("b-%d", i), peerMapEntry{peerID: "p", timestamp: now})
			s.subtreePeerMap.Store(fmt.Sprintf("s-%d", i), peerMapEntry{peerID: "p", timestamp: now})
		}
	}

	t.Run("configured size binds both maps", func(t *testing.T) {
		tSettings := &settings.Settings{}
		tSettings.P2P.PeerMapMaxSize = 7
		tSettings.P2P.PeerMapTTL = 3 * time.Minute

		s := &Server{logger: ulogger.TestLogger{}}
		s.applyPeerMapLimits(tSettings)

		require.Equal(t, 3*time.Minute, s.peerMapTTL)
		requireConfiguredCap(t, &s.blockPeerMap, 7)
		requireConfiguredCap(t, &s.subtreePeerMap, 7)

		fill(t, s, 20)
		require.Equal(t, 7, s.blockPeerMap.Len())
		require.Equal(t, 7, s.subtreePeerMap.Len())
	})

	t.Run("unset settings fall back to the service defaults", func(t *testing.T) {
		s := &Server{logger: ulogger.TestLogger{}}
		s.applyPeerMapLimits(&settings.Settings{})

		require.Equal(t, defaultPeerMapTTL, s.peerMapTTL)

		// The default must be applied to the maps, not merely relied on as
		// their fallback: this is what would fail if the wiring were dropped.
		requireConfiguredCap(t, &s.blockPeerMap, defaultPeerMapMaxSize)
		requireConfiguredCap(t, &s.subtreePeerMap, defaultPeerMapMaxSize)

		fill(t, s, 5)
		require.Equal(t, 5, s.blockPeerMap.Len())
		require.Equal(t, 5, s.subtreePeerMap.Len())
	})

	t.Run("an unconfigured TTL does not expire everything on the next sweep", func(t *testing.T) {
		// The zero TTL is the mirror hazard of the zero cap. It reads like
		// "expire nothing" and behaves like "expire everything": the sweep
		// cutoff lands on now, so every announcement is gone before the block
		// it names finishes validating and the ban path finds nobody to blame.
		s := &Server{logger: ulogger.TestLogger{}}
		require.Zero(t, s.peerMapTTL, "the hazard only exists while the field is unset")
		require.Equal(t, defaultPeerMapTTL, s.peerMapTTLOrDefault())

		s.storePeerMapEntry(&s.blockPeerMap, "block-hash", "peer-to-ban", now)
		s.storePeerMapEntry(&s.subtreePeerMap, "subtree-hash", "peer-to-ban", now)

		s.cleanupPeerMaps()

		peerID, err := s.getPeerFromMap(&s.blockPeerMap, "block-hash", "block")
		require.NoError(t, err, "a sweep on an unconfigured Server must not wipe attribution")
		require.Equal(t, "peer-to-ban", peerID)

		peerID, err = s.getPeerFromMap(&s.subtreePeerMap, "subtree-hash", "subtree")
		require.NoError(t, err)
		require.Equal(t, "peer-to-ban", peerID)
	})

	t.Run("a non-positive configured size cannot unbound the maps", func(t *testing.T) {
		tSettings := &settings.Settings{}
		tSettings.P2P.PeerMapMaxSize = -1

		s := &Server{logger: ulogger.TestLogger{}}
		s.applyPeerMapLimits(tSettings)

		requireConfiguredCap(t, &s.blockPeerMap, defaultPeerMapMaxSize)
		requireConfiguredCap(t, &s.subtreePeerMap, defaultPeerMapMaxSize)
	})
}

// recordingLogger captures the lines a test triggers so the startup
// announcements can be asserted on. Everything else is the no-op TestLogger.
type recordingLogger struct {
	ulogger.TestLogger

	mu    sync.Mutex
	warns []string
	infos []string
}

func (l *recordingLogger) Warnf(format string, args ...interface{}) {
	l.mu.Lock()
	defer l.mu.Unlock()

	l.warns = append(l.warns, fmt.Sprintf(format, args...))
}

func (l *recordingLogger) Infof(format string, args ...interface{}) {
	l.mu.Lock()
	defer l.mu.Unlock()

	l.infos = append(l.infos, fmt.Sprintf(format, args...))
}

// linesContaining returns the recorded lines holding substr, so an assertion
// names the line it means rather than depending on how many others were logged.
func (l *recordingLogger) linesContaining(lines []string, substr string) []string {
	l.mu.Lock()
	defer l.mu.Unlock()

	var matched []string

	for _, line := range lines {
		if strings.Contains(line, substr) {
			matched = append(matched, line)
		}
	}

	return matched
}

func (l *recordingLogger) warnsContaining(substr string) []string {
	return l.linesContaining(l.warns, substr)
}

func (l *recordingLogger) infosContaining(substr string) []string {
	return l.linesContaining(l.infos, substr)
}

// TestAnnouncePeerMapLimits pins the startup announcements, which are the only
// warning an operator gets that these three keys changed status. They carried
// struct tags but were never read, so a config that pinned the pre-tuning
// 100000/30m/5m from the old reference docs was inert and is now live — a 10x
// growth in the attribution maps arriving on a change whose purpose is bounding
// them. Nothing else exercises these branches, so a wrong format verb or a
// dropped condition would ship silently and only surface as a leak weeks later.
func TestAnnouncePeerMapLimits(t *testing.T) {
	t.Run("each configured value above its default announces itself", func(t *testing.T) {
		tSettings := &settings.Settings{}
		tSettings.P2P.PeerMapMaxSize = defaultPeerMapMaxSize * 10
		tSettings.P2P.PeerMapTTL = defaultPeerMapTTL * 3
		tSettings.P2P.PeerMapCleanupInterval = defaultPeerMapCleanupInterval * 5

		logger := &recordingLogger{}
		s := &Server{logger: logger}
		s.applyPeerMapLimits(tSettings)

		// Asserted per key, and with the configured value in the line: an
		// operator reading it has to be able to tell which setting to look at
		// and what it resolved to, and a swapped format argument would
		// otherwise pass.
		require.Equal(t, []string{fmt.Sprintf(
			"[applyPeerMapLimits] p2p_peer_map_max_size=%d exceeds the %d default; this key was inert before and is now read, so check the value is intended — it also lengthens the cleanup sweep's locked walk",
			defaultPeerMapMaxSize*10, defaultPeerMapMaxSize)},
			logger.warnsContaining("p2p_peer_map_max_size"))

		require.Equal(t, []string{fmt.Sprintf(
			"[applyPeerMapLimits] p2p_peer_map_ttl=%s exceeds the %s default; this key was inert before and is now read, so check the value is intended",
			defaultPeerMapTTL*3, defaultPeerMapTTL)},
			logger.warnsContaining("p2p_peer_map_ttl"))

		require.Equal(t, []string{fmt.Sprintf(
			"[applyPeerMapLimits] p2p_peer_map_cleanup_interval=%s exceeds the %s default; this key was inert before and is now read, so check the value is intended",
			defaultPeerMapCleanupInterval*5, defaultPeerMapCleanupInterval)},
			logger.warnsContaining("p2p_peer_map_cleanup_interval"))
	})

	t.Run("values at or below their defaults stay quiet", func(t *testing.T) {
		tSettings := &settings.Settings{}
		tSettings.P2P.PeerMapMaxSize = defaultPeerMapMaxSize
		tSettings.P2P.PeerMapTTL = defaultPeerMapTTL
		tSettings.P2P.PeerMapCleanupInterval = defaultPeerMapCleanupInterval

		logger := &recordingLogger{}
		s := &Server{logger: logger}
		s.applyPeerMapLimits(tSettings)

		require.Empty(t, logger.warns, "a config matching the defaults is not worth a startup warning")
	})

	t.Run("a coerced non-positive cap says so", func(t *testing.T) {
		// The adjacent p2p_peer_registry_max_size documents 0 as "disable
		// enforcement", so an operator can reasonably set 0 here expecting the
		// same and get a bound they did not ask for. Silence would look exactly
		// like the inert key this change just finished fixing.
		tSettings := &settings.Settings{}
		tSettings.P2P.PeerMapMaxSize = 0

		logger := &recordingLogger{}
		s := &Server{logger: logger}
		s.applyPeerMapLimits(tSettings)

		require.Equal(t, []string{fmt.Sprintf(
			"[applyPeerMapLimits] p2p_peer_map_max_size=%d is not a usable cap; using the %d default — there is no unbounded mode",
			0, defaultPeerMapMaxSize)},
			logger.infosContaining("p2p_peer_map_max_size"))

		require.Empty(t, logger.warnsContaining("p2p_peer_map_max_size"),
			"a coerced cap is not also an exceeds-the-default warning")
	})
}

// TestCappedPeerMapFloodEvictsFlooderNotVictim pins the fair-share rule (issue
// 1503) against the exploit it closes: an attacker who announces an invalid
// block and then floods distinct hashes to age its victims' — or every other
// peer's — attribution out of the shared map before validation reports. With
// the fair-share rule the flooder is over its share of the map almost
// immediately, so its pressure cannibalizes its own junk and the honest
// entries survive the entire flood.
func TestCappedPeerMapFloodEvictsFlooderNotVictim(t *testing.T) {
	var m cappedPeerMap

	m.setMaxSize(100)

	now := time.Now()

	// Honest announcements land first — the entries the flood used to wash out.
	for i := 0; i < 10; i++ {
		m.Store(fmt.Sprintf("honest-%d", i), peerMapEntry{peerID: fmt.Sprintf("honest-peer-%d", i), timestamp: now})
	}

	// A flood orders of magnitude past the cap.
	for i := 0; i < 5000; i++ {
		m.Store(fmt.Sprintf("junk-%d", i), peerMapEntry{peerID: "attacker", timestamp: now})
	}

	require.Equal(t, 100, m.Len())

	for i := 0; i < 10; i++ {
		entry, ok := m.Load(fmt.Sprintf("honest-%d", i))
		require.True(t, ok, "honest attribution %d must survive a flood by another peer", i)
		require.Equal(t, fmt.Sprintf("honest-peer-%d", i), entry.peerID)
	}

	// The junk the flooder holds is its newest: everything older self-evicted.
	_, ok := m.Load("junk-0")
	require.False(t, ok, "the flooder's own oldest entries are the ones evicted")

	evictions := m.EvictionsSinceLastRead()
	require.Equal(t, "attacker", evictions.topPeer, "the flood must be attributed to the flooder")
	requireMapConsistent(t, &m)
}

// TestCappedPeerMapFairShareAdapts pins the denominator of the fair share: it
// is the peers actually holding entries, not a fixed quota. Two peers filling
// the map split it evenly, and each one's further inserts displace its own
// oldest entry, never the other's.
func TestCappedPeerMapFairShareAdapts(t *testing.T) {
	var m cappedPeerMap

	m.setMaxSize(10)

	now := time.Now()
	for i := 0; i < 5; i++ {
		m.Store(fmt.Sprintf("a-%d", i), peerMapEntry{peerID: "peer-a", timestamp: now})
		m.Store(fmt.Sprintf("b-%d", i), peerMapEntry{peerID: "peer-b", timestamp: now})
	}

	require.Equal(t, 10, m.Len())

	// peer-a holds exactly its share (10/2 = 5): its next insert must evict its
	// own oldest, leaving all of peer-b's entries intact.
	m.Store("a-next", peerMapEntry{peerID: "peer-a", timestamp: now})

	_, ok := m.Load("a-0")
	require.False(t, ok, "a peer at its fair share evicts its own oldest entry")

	for i := 0; i < 5; i++ {
		_, ok := m.Load(fmt.Sprintf("b-%d", i))
		require.True(t, ok, "the other peer's entries must be untouched")
	}

	// A peer with no entries yet is below any share: its insert takes the
	// global oldest instead. The stores interleaved a-0, b-0, a-1, …, and a-0
	// is already gone, so the global oldest is b-0.
	m.Store("c-0", peerMapEntry{peerID: "peer-c", timestamp: now})

	_, ok = m.Load("b-0")
	require.False(t, ok, "a peer below its share displaces the global oldest")
	requireMapConsistent(t, &m)
}

// TestStorePeerMapEntryFloodCannotWashOutAttribution drives the exploit from
// the original finding end-to-end through the production helpers: attacker
// announces invalid block B, floods far more distinct hashes than the map
// holds, and the ban path must still resolve B to the attacker.
func TestStorePeerMapEntryFloodCannotWashOutAttribution(t *testing.T) {
	s := &Server{logger: ulogger.TestLogger{}}
	s.blockPeerMap.setMaxSize(100)

	now := time.Now()

	invalidBlockHash := fmt.Sprintf("%064d", 424242)
	s.storePeerMapEntry(&s.blockPeerMap, invalidBlockHash, "attacker", now)

	for i := 0; i < 10000; i++ {
		s.storePeerMapEntry(&s.blockPeerMap, fmt.Sprintf("%064d", i), "attacker", now)
	}

	// Self-eviction is the residual (issue 1433): the flooder CAN drop its own
	// oldest entry, so the map alone cannot pin its own invalid block...
	_, err := s.getPeerFromMap(&s.blockPeerMap, invalidBlockHash, "block")
	require.Error(t, err, "a flooder can still age out its own attribution from the map")

	// ...but it cannot touch anyone else's: an honest announcement made before
	// the flood must still resolve.
	s.blockPeerMap.Clear()
	honestHash := fmt.Sprintf("%064d", 777777)
	s.storePeerMapEntry(&s.blockPeerMap, honestHash, "honest-peer", now)

	for i := 0; i < 10000; i++ {
		s.storePeerMapEntry(&s.blockPeerMap, fmt.Sprintf("%064d", i), "attacker", now)
	}

	peerID, err := s.getPeerFromMap(&s.blockPeerMap, honestHash, "block")
	require.NoError(t, err, "a flood must not evict another peer's attribution")
	require.Equal(t, "honest-peer", peerID)
}
