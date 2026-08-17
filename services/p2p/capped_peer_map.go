package p2p

import (
	"container/list"
	"fmt"
	"sync"
	"time"
)

// cappedPeerMap is a size-bounded map from announced hash to the peer that
// announced it (ban attribution). Gossip inserts are driven entirely by
// untrusted input — an announcement stores the peer BEFORE any check that the
// hash names a real block or subtree — so the bound is enforced inline at
// insert (issue 1409): before this, the only limit was a timer-driven sweep,
// letting a distinct-hash flood balloon memory between sweeps and making the
// sweep's own full-map sort scale with the flood.
//
// At capacity a new key evicts the OLDEST entry rather than being refused.
// That direction is deliberate: refusing the new key would let a flooder fill
// every slot with junk hashes ahead of time and so suppress attribution for
// the invalid block it announces next. Evicting oldest keeps memory bounded
// while always retaining the most recent announcements, which are the ones an
// in-flight validation is about to report.
//
// What this does NOT do is make attribution flood-proof, in two distinct ways.
//
// Cross-peer: the map is a single space shared by all peers, so a peer that
// announces enough distinct hashes AFTER an honest announcement can age that
// honest entry out before the block finishes validating. Evicting oldest
// changes when a flooder can strike, not whether it can; closing that needs a
// per-peer share of the map, tracked in issue 1503.
//
// Self-eviction: a peer can drop ITS OWN attribution the same way, by
// announcing maxSize further distinct hashes on the same topic between serving
// an invalid block and that block being reported. This is the sharper of the
// two, because the invalid-block Kafka message carries only the hash and a
// reason, so this map is the sole attribution channel the ban path has — losing
// the entry means the peer is never scored. It is not created here (the sweep
// on main trimmed oldest-first too), but it is made cheaper: the retention
// window between sweeps is now set by the announcement rate, which the attacker
// controls, rather than by the cleanup timer, which it does not. A per-peer
// share does not help, since a peer evicting from its own share is exactly the
// case; it needs a per-peer announcement budget, tracked in issue 1433.
//
// Attribution is best-effort here, as the TTL already implies.
//
// Eviction is O(1): a list holds keys in insertion order (most recent at the
// back), so no sort is needed at insert or at sweep time.
//
// There is no unbounded mode. An unconfigured map falls back to
// defaultPeerMapMaxSize rather than growing without limit, so the control
// cannot be defeated by a construction path that forgets to configure it.
type cappedPeerMap struct {
	mu      sync.Mutex
	entries map[string]*list.Element
	order   *list.List // front = oldest, back = newest; values are *peerMapNode
	maxSize int

	// evicted counts entries dropped to make room since the last sweep read —
	// flood observability without a per-insert log line. Guarded by mu, not
	// atomic: it is only ever touched alongside evictors below, and the two are
	// read together as one verdict. An atomic here would advertise a lock-free
	// read this type does not offer, and taking one would return a count that
	// disagrees with the attribution it is reported with.
	evicted int64

	// evictors attributes those evictions to the peers whose inserts forced
	// them — the pressure, not the entries dropped by it — so the at-capacity
	// warning can tell a busy deployment (pressure spread across peers) from a
	// flood (one dominant peer); the two call for opposite responses. It holds
	// at most evictorTrackLimit distinct peers, so the diagnostic cannot itself
	// become the unbounded map issue 1409 is about, and it spends that budget
	// on whoever dominates rather than whoever arrived first. Guarded by mu.
	evictors         map[string]int64
	evictorsOverflow bool
}

// evictorTrackLimit bounds how many distinct peers the eviction attribution
// holds at once, so the diagnostic cannot itself become the unbounded map issue
// 1409 is about. It also sets the resolution: a peer causing more than
// total/(evictorTrackLimit+1) of the evictions is guaranteed to still be named.
// Overflowing it does not mean the pressure is spread — at capacity every new
// hash evicts, so a busy mesh overflows the tracker routinely — which is why
// String weighs the top contributor's share against the exact total rather than
// trusting the flag.
const evictorTrackLimit = 16

// evictionStats summarises eviction pressure for one sweep window.
type evictionStats struct {
	total int64 // entries dropped to make room since the previous read

	// topPeer is the peer whose inserts forced the most evictions, and topCount
	// its share. Empty when nothing was evicted, and also when the pressure was
	// a long tail that cancelled itself out of the tracker. Under spread the
	// count is a lower bound rather than exact — see recordEvictorLocked — so
	// it can understate a flooder but never overstates one.
	topPeer  string
	topCount int64

	// spread is set when more peers contributed than the tracker can hold. It
	// says the counts are lower bounds; it is not by itself evidence that no
	// peer dominates, which is why String weighs the share instead.
	spread bool
}

// peerMapNode is the list payload: the key plus its entry, so evicting from
// the list front also identifies the map key to delete.
type peerMapNode struct {
	hash  string
	entry peerMapEntry
}

// setMaxSize configures the insert cap, normalising a non-positive value to
// defaultPeerMapMaxSize so that maxSize always holds the cap actually in
// force. This type has no unbounded mode: a map that never reaches
// applyPeerMapLimits — a bare Server literal in a test fixture, or a future
// construction path that forgets the call — is still bounded, by capLocked.
func (m *cappedPeerMap) setMaxSize(maxSize int) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if maxSize <= 0 {
		maxSize = defaultPeerMapMaxSize
	}

	m.maxSize = maxSize
}

// capLocked returns the cap in force, covering the one case setMaxSize cannot:
// a struct whose zero value was never configured at all. Callers must hold the
// mutex.
func (m *cappedPeerMap) capLocked() int {
	if m.maxSize <= 0 {
		return defaultPeerMapMaxSize
	}

	return m.maxSize
}

// init prepares the internal structures; callers must hold the mutex.
func (m *cappedPeerMap) initLocked() {
	if m.entries == nil {
		m.entries = make(map[string]*list.Element)
		m.order = list.New()
	}
}

// Store inserts or updates an entry, evicting the oldest entry first when a
// new key would exceed the cap. Updating an existing key refreshes its value
// and its recency, so attribution for a hash announced by two peers is
// last-writer-wins — unchanged from the sync.Map this replaced, and reachable
// only when a second node genuinely announces the same hash as its own tip,
// since the fromID check rejects re-attribution by relays.
func (m *cappedPeerMap) Store(hash string, entry peerMapEntry) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.initLocked()

	if existing, ok := m.entries[hash]; ok {
		existing.Value.(*peerMapNode).entry = entry
		m.order.MoveToBack(existing)

		return
	}

	// Loop rather than evict once: a cap lowered on a populated map leaves it
	// over-cap, and evicting a single entry per insert would hold it there
	// forever. The sweep no longer has a size pass to correct that. Note this
	// sits below the update path, so it is new keys that drain an over-cap map;
	// updates refresh in place and leave the length alone.
	limit := m.capLocked()

	for m.order.Len() >= limit {
		// Front cannot be nil here: capLocked never returns less than 1, so the
		// loop guard means the list is non-empty. Taken defensively rather than
		// asserted, because the alternative to a wrong guess here is a nil
		// dereference on the gossip hot path.
		oldest := m.order.Front()
		if oldest == nil {
			break
		}

		node := oldest.Value.(*peerMapNode)

		m.order.Remove(oldest)
		delete(m.entries, node.hash)
		m.evicted++

		// Attribute the eviction to the peer whose insert forced it, not to
		// the peer whose entry was dropped. The dropped entry is the victim:
		// blaming it would name honest peers for an attacker's flood, and the
		// warning's whole purpose is to tell the operator who to ban.
		m.recordEvictorLocked(entry.peerID)
	}

	m.entries[hash] = m.order.PushBack(&peerMapNode{hash: hash, entry: entry})
}

// recordEvictorLocked attributes one eviction to the peer whose insert forced
// it, within the evictorTrackLimit budget. Callers must hold the mutex.
//
// The budget is spent by Misra-Gries rather than first-come-first-served,
// because peer IDs are free and connection limits are unbounded (issue 1163):
// keeping the first evictorTrackLimit names would let an attacker seed the
// tracker with throwaway identities and then flood unattributed for the rest of
// the window. Decrementing every counter instead means a long tail of one-off
// peers cancels itself out while a heavy hitter survives, so any peer causing
// more than total/(evictorTrackLimit+1) of the evictions is still named.
//
// The counts this leaves are lower bounds, understated by at most that same
// fraction. That direction is the one worth having: the reported share can miss
// a flooder but can never overstate one, so it cannot accuse an honest peer.
func (m *cappedPeerMap) recordEvictorLocked(peerID string) {
	if m.evictors == nil {
		m.evictors = make(map[string]int64, evictorTrackLimit)
	}

	if _, ok := m.evictors[peerID]; !ok && len(m.evictors) >= evictorTrackLimit {
		m.evictorsOverflow = true

		for id := range m.evictors {
			if m.evictors[id]--; m.evictors[id] <= 0 {
				delete(m.evictors, id)
			}
		}

		return
	}

	m.evictors[peerID]++
}

// Load returns the entry for hash, if present. It does not change recency:
// eviction order tracks when a hash was announced, not when it was looked up.
func (m *cappedPeerMap) Load(hash string) (peerMapEntry, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()

	element, ok := m.entries[hash]
	if !ok {
		return peerMapEntry{}, false
	}

	return element.Value.(*peerMapNode).entry, true
}

// Delete removes the entry for hash, if present.
func (m *cappedPeerMap) Delete(hash string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if element, ok := m.entries[hash]; ok {
		m.order.Remove(element)
		delete(m.entries, hash)
	}
}

// Clear removes every entry, along with the eviction pressure recorded for
// them. The counters describe entries that no longer exist, so carrying them
// past a Clear would report pressure from before the reset on the next sweep.
// The configured cap survives: Stop calls Clear, and a cap dropped here would
// leave the maps on the fallback for the rest of the process's life.
func (m *cappedPeerMap) Clear() {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.entries = nil
	m.order = nil
	m.evictors = nil
	m.evictorsOverflow = false

	m.evicted = 0
}

// Len returns the number of entries.
func (m *cappedPeerMap) Len() int {
	m.mu.Lock()
	defer m.mu.Unlock()

	return len(m.entries)
}

// DeleteExpired removes every entry whose timestamp predates cutoff and
// returns how many it removed. This is the TTL sweep's one pass over the map:
// it holds the lock once and allocates nothing, where Range would copy every
// entry into a snapshot first and the caller would then re-enter the lock per
// deletion.
//
// It is also the only whole-map walk left, and its cost is bounded by the cap
// rather than by how many hashes were announced — the property the removed
// sweep-time sort did not have. Raising the cap lengthens this walk, which
// holds the mutex against every gossip insert for its duration.
//
// It walks the whole list rather than stopping at the first live entry.
// Insertion order tracks timestamp order closely but not strictly — two
// concurrent announcements can read the clock in one order and take the lock
// in the other — so an early exit could skip an expired entry and leave it for
// the next sweep. At the configured cap the full walk costs little enough that
// the exactness is worth more than the saved iterations.
func (m *cappedPeerMap) DeleteExpired(cutoff time.Time) int {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.order == nil {
		return 0
	}

	removed := 0

	for element := m.order.Front(); element != nil; {
		next := element.Next()

		if node := element.Value.(*peerMapNode); node.entry.timestamp.Before(cutoff) {
			m.order.Remove(element)
			delete(m.entries, node.hash)

			removed++
		}

		element = next
	}

	return removed
}

// EvictionsSinceLastRead returns how many entries were dropped to make room
// since the previous call, and who caused them, resetting both. The
// attribution is what lets the caller distinguish sustained legitimate
// throughput — pressure spread across peers, where a larger cap is the right
// answer — from one peer spraying distinct hashes, where a larger cap only
// buys the attacker more of the node's memory (issue 1503).
func (m *cappedPeerMap) EvictionsSinceLastRead() evictionStats {
	m.mu.Lock()
	defer m.mu.Unlock()

	stats := evictionStats{
		total:  m.evicted,
		spread: m.evictorsOverflow,
	}

	for peerID, count := range m.evictors {
		if count > stats.topCount {
			stats.topPeer, stats.topCount = peerID, count
		}
	}

	m.evicted = 0
	m.evictors = nil
	m.evictorsOverflow = false

	return stats
}

// String renders the eviction attribution for the at-capacity log line.
//
// The verdict turns on the top contributor's share, not on how many peers
// contributed. Those two questions come apart in both directions, and the log
// line is read as a recommendation — "spread" means raise the cap, a named
// contributor means ban that peer — so getting either one backwards points the
// operator at the wrong response.
//
// Share is therefore tested before overflow. Once a map sits at capacity every
// new hash evicts, so on a mesh with more peers than the tracker can name the
// overflow flag is set in almost every sweep window — and testing it first would
// discard the name and report "spread" even when one peer caused nearly all of
// the pressure. That is the one answer issue 1503 says is wrong for a flood: it
// tells the operator to raise the cap, which only buys the attacker more memory.
// Comparing against total is safe under overflow because total is exact while
// evictors is not, so a majority share cannot be an artefact of which peers the
// tracker happened to name first.
//
// The same test has to apply when the tracker did not overflow, which is the
// easier case to get right because the counts are exact there. A handful of
// busy honest peers each forcing a quarter of the evictions is throughput, and
// naming the largest of them as a top contributor would recommend banning the
// node's busiest honest peer. Overflow then only decides the wording — whether
// the count is exact or a lower bound, and whether unnamed contributors exist —
// never whether the pressure counts as a flood.
func (s evictionStats) String() string {
	switch {
	case s.total == 0:
		return "none"
	case s.topPeer == "":
		// Nobody survived the Misra-Gries decrements, which is itself the
		// answer: the pressure was a long tail of peers with no heavy hitter.
		return fmt.Sprintf("spread across more than %d peers", evictorTrackLimit)
	case s.topCount == s.total:
		return fmt.Sprintf("all from peer %s", s.topPeer)
	case s.topCount*2 <= s.total:
		if s.spread {
			return fmt.Sprintf("spread across more than %d peers; largest tracked contributor peer %s with at least %d of %d",
				evictorTrackLimit, s.topPeer, s.topCount, s.total)
		}

		return fmt.Sprintf("spread across peers; largest contributor peer %s with %d of %d",
			s.topPeer, s.topCount, s.total)
	case s.spread:
		return fmt.Sprintf("top contributor peer %s with at least %d of %d", s.topPeer, s.topCount, s.total)
	default:
		return fmt.Sprintf("top contributor peer %s with %d of %d", s.topPeer, s.topCount, s.total)
	}
}
