package netsync

import (
	"sync"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	peerpkg "github.com/bsv-blockchain/teranode/services/legacy/peer"
)

const (
	// blockRequestAssignmentTTL is how long a peer stays on the hook for a block
	// we asked it for. It is the ceiling the disconnect decision uses: a peer
	// that delivers within it is answering our question, however late, and must
	// keep its connection. Inherited from the per-peer map this replaced, whose
	// comment explained the hour is what legacy sync and checkpoint batches need.
	blockRequestAssignmentTTL = 60 * time.Minute

	// blockRequestRetryInterval is how long we wait before an announced block is
	// worth asking for again. It is deliberately far shorter than the ownership
	// ceiling: after a minute we are willing to ask somebody else, but the
	// original peer is still not punished if its copy turns up. Inherited from
	// the global map this replaced.
	blockRequestRetryInterval = 60 * time.Second

	// maxTrackedBlockDownloads bounds how many distinct blocks the ledger will
	// track, so a flood of announcements cannot grow it without limit. The cap
	// is applied by refusing the newcomer, never by dropping work already in
	// progress: a block that arrives after its record was dropped looks
	// unrequested and costs an honest peer its connection, and the block we have
	// waited longest for — the frontier everything else is queued behind — is by
	// definition the oldest record of all. Refusing is only safe because Add
	// says so to its caller, which then does not send the getdata; see Add.
	maxTrackedBlockDownloads = 50_000
)

// ownerRecord is what one peer owes us for one block.
type ownerRecord struct {
	// at is when we last asked, and what the ownership ceiling is measured from.
	at time.Time
	// forgiven marks an assignment the peer has been let off. The record stays,
	// because a copy that does turn up must still be admitted rather than costing
	// an honest peer its whole association — but the peer is no longer spending
	// budget on it, and no longer counts as a peer we are downloading from.
	//
	// Without this the two questions were the same question. A demoted peer's
	// slice was reopened by back-dating it, another peer delivered those blocks,
	// and because arrival only discharges the delivering peer the back-dated
	// records sat there for the rest of the hour — spending the whole budget of
	// the peer the demotion had deliberately kept connected in order to use.
	forgiven bool
}

// blockDownloadTracker records which peers owe us which blocks.
//
// It replaces two separate expiring maps — one global, one per peer — that
// between them could only express a single owner per hash. The frontier race
// already breaks that assumption: when the sync peer goes quiet we deliberately
// ask a second peer for the same block, and both of them are then entitled to
// deliver it without being disconnected. Holding the ownership the other way
// round, as a set of (block, peer) pairs, says that directly.
//
// Entries age out on their own. That matters because the call that is supposed
// to release a departing peer's blocks does not always run: handleDonePeerMsg
// returns early for any peer that is not registered in peerStates, which
// includes the stream sub-peers a BlockPriority association resolves through. An
// assignment nothing ever clears must not pin a hash forever.
//
// There is no background goroutine. The two maps this replaced each ran a
// cleanup ticker that was stopped, never cleared, by the code meant to release a
// peer's requests — the cleanup looked like it was happening and was not. Expiry
// here is done by the callers' own reads and writes, so there is nothing to
// start, nothing to stop, and nothing that can silently stop working.
//
// Every method is safe on a nil receiver: reads answer "nothing is owed" and
// writes do nothing. Reading a nil tracker as "we never asked for this" is the
// safe direction — it costs a misbehaving-looking peer its connection rather
// than admitting a block nobody requested.
type blockDownloadTracker struct {
	mu  sync.Mutex
	ttl time.Duration
	// now is the clock, injectable so tests can age assignments without sleeping.
	now func() time.Time
	// byHash answers "who owes us this block, since when, and whether they have
	// been let off it".
	byHash map[chainhash.Hash]map[*peerpkg.Peer]ownerRecord
	// byPeer answers "what does this peer owe us", so a peer's own outstanding
	// count and its removal are both O(what that peer owes) rather than O(all).
	byPeer    map[*peerpkg.Peer]map[chainhash.Hash]struct{}
	lastSweep time.Time
}

// newBlockDownloadTracker builds a ledger whose assignments expire after ttl.
func newBlockDownloadTracker(ttl time.Duration) *blockDownloadTracker {
	return &blockDownloadTracker{
		ttl:    ttl,
		now:    time.Now,
		byHash: make(map[chainhash.Hash]map[*peerpkg.Peer]ownerRecord),
		byPeer: make(map[*peerpkg.Peer]map[chainhash.Hash]struct{}),
	}
}

// Add records that we have asked peer p for block h and reports whether the
// ledger took it. Asking the same peer again refreshes the assignment, which is
// what we want: the clock should run from the most recent time we actually
// asked.
//
// A false answer means the ledger is at its size cap and this is a block it does
// not already know about. The caller must then not send the getdata, because a
// request the ledger cannot vouch for comes back looking unrequested and costs
// an honest peer its connection. Refusing the newcomer is the only way to apply
// the cap that leaves every block we are already waiting on exactly where it
// was; evicting to make room would aim that same disconnect at whichever peer
// lost the eviction, which for oldest-first is the frontier peer — the one block
// sync cannot proceed without.
//
// Recording an additional owner for a block already in the ledger never fails.
// That is what the frontier race needs: asking a second peer for the block that
// is holding up sync must work however full the ledger is, because it adds no
// block to it.
func (t *blockDownloadTracker) Add(p *peerpkg.Peer, h chainhash.Hash) bool {
	if t == nil {
		return false
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	now := t.clock()

	if t.byHash == nil {
		t.byHash = make(map[chainhash.Hash]map[*peerpkg.Peer]ownerRecord)
	}

	if t.byPeer == nil {
		t.byPeer = make(map[*peerpkg.Peer]map[chainhash.Hash]struct{})
	}

	owners := t.byHash[h]
	if owners == nil {
		if len(t.byHash) >= maxTrackedBlockDownloads {
			// Aged-out assignments are the only room this ledger makes for
			// itself, and the walk is worth it before turning a request away.
			t.sweepExpiredLocked(now)

			if len(t.byHash) >= maxTrackedBlockDownloads {
				return false
			}
		}

		owners = make(map[*peerpkg.Peer]ownerRecord, 1)
		t.byHash[h] = owners
	}

	owners[p] = ownerRecord{at: now}

	hashes := t.byPeer[p]
	if hashes == nil {
		hashes = make(map[chainhash.Hash]struct{}, 1)
		t.byPeer[p] = hashes
	}

	hashes[h] = struct{}{}

	t.maybeSweepLocked(now)

	return true
}

// HasOwner reports whether peer p is currently on the hook for block h. This is
// the question the disconnect decision asks, so a false answer costs a peer its
// connection — expiry is judged against the full ownership ceiling, not the far
// shorter re-request window.
func (t *blockDownloadTracker) HasOwner(p *peerpkg.Peer, h chainhash.Hash) bool {
	if t == nil {
		return false
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	now := t.clock()
	t.maybeSweepLocked(now)

	rec, ok := t.byHash[h][p]

	return ok && !t.expiredAt(rec.at, now, t.ttl)
}

// ReassertOwner puts a peer back on the hook for a block it already holds our
// request for, and reports whether it did. A true answer means this peer has
// already been asked and must NOT be asked again.
//
// This is the answer to a pass whose assigner picks, for some header, the very
// peer that already owes it. A demoted peer's reopened slice is exactly that
// case: reopening back-dates the record rather than dropping it, so the walk is
// free to place the block again and nothing stopped it landing back on the same
// peer. Sending a second getdata would have that peer answer twice, and the
// second copy arrives after the first discharged its obligation — unowned, and
// fatal to an honest peer's whole association.
//
// Re-arming the record we already hold leaves the block where it is: with the
// one peer that has the request. Its recovery is unchanged — that peer's own
// stall handler, the frontier race, and this ledger's expiry.
//
// An assignment already past the ownership ceiling is not re-armed. At that age
// the peer has long since dropped the request, so the caller must send a real
// one; false sends it down the ordinary Add path, which overwrites the stale
// record with a fresh one.
func (t *blockDownloadTracker) ReassertOwner(p *peerpkg.Peer, h chainhash.Hash) bool {
	if t == nil {
		return false
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	owners, ok := t.byHash[h]
	if !ok {
		return false
	}

	rec, owned := owners[p]
	if !owned {
		return false
	}

	now := t.clock()
	if t.expiredAt(rec.at, now, t.ttl) {
		return false
	}

	rec.at = now
	rec.forgiven = false
	owners[p] = rec

	return true
}

// OwnersOf returns every peer that still owes us block h, forgiven ones included
// — a peer that has been let off may still be mid-send, and for the question this
// answers ("is anybody actually delivering this?") that is the same thing as
// owing it.
//
// The frontier race needs it in order to judge the peers that owe the block
// rather than the sync peer, which with the fan-out on is routinely not the same
// peer. svnode asks the equivalent question with GetBlockDetails.
func (t *blockDownloadTracker) OwnersOf(h chainhash.Hash) []*peerpkg.Peer {
	if t == nil {
		return nil
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	owners := t.byHash[h]
	if len(owners) == 0 {
		return nil
	}

	now := t.clock()
	out := make([]*peerpkg.Peer, 0, len(owners))

	for p, rec := range owners {
		if t.expiredAt(rec.at, now, t.ttl) {
			continue
		}

		out = append(out, p)
	}

	return out
}

// ForgiveOwners lets every peer that owes block h off the hook, and returns the
// peers it let off. Ownership is kept, so a copy still on the wire from any of
// them is still admitted; only the obligation goes.
//
// This is what every delivery needs, raced or not: handleBlockMsg cancels the
// obligation of the peer that answered and forgives whoever else was asked.
// Cancelling the other owners outright was
// the obvious thing and it was wrong in both directions: it freed their budget,
// which is what the cancel was for, but it also revoked their permission to
// deliver — so a copy arriving afterwards looked unrequested and cost an honest
// peer its whole association, and a separate grace map with a separate expiry
// had to exist to paper over exactly that. svnode never cancels: MarkBlockAsReceived
// removes only {hash, node} and the stall race in FindNextBlocksToDownload only
// ever adds a source. Forgiving gets svnode's admission behaviour and keeps the
// budget release that made cancelling attractive, with one expiry instead of two.
//
// The timestamp is back-dated exactly as ForgetForRetryPeer does it, so the block
// is re-requestable straight away. That matters on the failure path: the arrival
// takes the header off the list before validation runs, and if validation then
// rejects the block the cursor is rewound onto it — which would be silently
// skipped for a retry interval by a record that still looked fresh.
func (t *blockDownloadTracker) ForgiveOwners(h chainhash.Hash, retryWindow time.Duration) []*peerpkg.Peer {
	if t == nil {
		return nil
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	owners := t.byHash[h]
	if len(owners) == 0 {
		return nil
	}

	now := t.clock()
	cut := now.Add(-retryWindow)
	forgiven := make([]*peerpkg.Peer, 0, len(owners))

	for p, rec := range owners {
		if t.expiredAt(rec.at, now, t.ttl) {
			continue
		}

		if rec.at.After(cut) {
			rec.at = cut
		}

		rec.forgiven = true
		owners[p] = rec

		forgiven = append(forgiven, p)
	}

	return forgiven
}

// RequestedWithin reports whether anybody was asked for block h within maxAge.
// This is the question the inv path asks before requesting a block, so a false
// answer means "ask somebody", not "punish somebody".
func (t *blockDownloadTracker) RequestedWithin(h chainhash.Hash, maxAge time.Duration) bool {
	if t == nil {
		return false
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	now := t.clock()
	t.maybeSweepLocked(now)

	for _, rec := range t.byHash[h] {
		if !t.expiredAt(rec.at, now, maxAge) {
			return true
		}
	}

	return false
}

// RemoveOwner cancels just this peer's obligation for block h and leaves any
// other peer still owing it. The frontier race needs the difference: when a
// raced block arrives it cancels the request with the peers it asked and nobody
// else.
func (t *blockDownloadTracker) RemoveOwner(p *peerpkg.Peer, h chainhash.Hash) {
	if t == nil {
		return
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	t.removeOwnerLocked(p, h)
}

// CountForPeer returns how many blocks this peer still owes us, ignoring
// assignments that have aged out. This feeds the per-peer in-flight budget, so
// counting a dead assignment would spend budget on a block that is never coming.
func (t *blockDownloadTracker) CountForPeer(p *peerpkg.Peer) int {
	if t == nil {
		return 0
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	now := t.clock()
	t.maybeSweepLocked(now)

	n := 0

	for h := range t.byPeer[p] {
		if rec, ok := t.byHash[h][p]; ok && !rec.forgiven && !t.expiredAt(rec.at, now, t.ttl) {
			n++
		}
	}

	return n
}

// PeersWithDownloads returns how many distinct peers have at least one live
// assignment. This number widens every peer's block download deadline, so a peer
// whose only assignment has aged out must not count.
func (t *blockDownloadTracker) PeersWithDownloads() int {
	if t == nil {
		return 0
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	now := t.clock()
	t.maybeSweepLocked(now)

	n := 0

	for p, hashes := range t.byPeer {
		for h := range hashes {
			if rec, ok := t.byHash[h][p]; ok && !rec.forgiven && !t.expiredAt(rec.at, now, t.ttl) {
				n++
				break
			}
		}
	}

	return n
}

// ClearPeer releases everything this peer owed us, so the next announcement of
// any of those blocks fetches them from somewhere else. It returns the hashes it
// released, which is what the caller needs to put the download walk back in
// front of them: the walk is forward-only, so a block released here is behind
// the cursor and nothing would ask for it again.
func (t *blockDownloadTracker) ClearPeer(p *peerpkg.Peer) []chainhash.Hash {
	if t == nil {
		return nil
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	released := make([]chainhash.Hash, 0, len(t.byPeer[p]))

	for h := range t.byPeer[p] {
		released = append(released, h)
		t.removeOwnerFromHashLocked(p, h)
	}

	delete(t.byPeer, p)

	return released
}

// ForgetForRetryPeer reopens one peer's outstanding blocks for a fresh request
// without cancelling its permission to deliver them, and returns the hashes it
// reopened so the caller can rewind the download walk onto the lowest of them.
//
// The two are genuinely different questions with different windows, so it moves
// only the shorter one. Every assignment of p's newer than retryWindow is
// back-dated to exactly retryWindow old, which is the point at which
// RequestedWithin stops claiming somebody is already on the job. Ownership is
// judged against the far longer assignment ceiling and survives, one retryWindow
// shorter than it was — so a late copy from p is still admitted rather than
// costing an honest peer its connection.
//
// It is deliberately per-peer. The whole-ledger form this replaced back-dated
// EVERY assignment at once, which made RequestedWithin answer false for every
// outstanding block. That was survivable only while a sync-peer change also
// threw the header list away, because there was then nothing left to re-walk.
// Beside a header list that survives a demotion, a whole-ledger back-date hands
// every in-flight block to a second peer on the very next pass, and both copies
// are admitted and committed — the duplicate-commit storm and the 40P01 deadlock
// on the transaction unique index that came with it.
//
// There is deliberately no method that forgets assignments outright. The ledger
// this replaced was two maps — a global one the sync peer change cleared, and a
// separate per-peer one the disconnect decision read — so clearing could not
// revoke anyone's permission to deliver. With one map it can, and did: an honest
// peer racing the frontier block lost its whole association for answering us.
func (t *blockDownloadTracker) ForgetForRetryPeer(p *peerpkg.Peer, retryWindow time.Duration) []chainhash.Hash {
	if t == nil || retryWindow <= 0 {
		return nil
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	cut := t.clock().Add(-retryWindow)

	reopened := make([]chainhash.Hash, 0, len(t.byPeer[p]))

	for h := range t.byPeer[p] {
		owners, ok := t.byHash[h]
		if !ok {
			continue
		}

		rec, owned := owners[p]
		if !owned {
			continue
		}

		if rec.at.After(cut) {
			rec.at = cut
		}

		rec.forgiven = true
		owners[p] = rec

		reopened = append(reopened, h)
	}

	return reopened
}

// Len returns how many distinct blocks are currently owed by somebody, ignoring
// assignments that have aged out.
func (t *blockDownloadTracker) Len() int {
	if t == nil {
		return 0
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	now := t.clock()
	t.maybeSweepLocked(now)

	n := 0

	for _, owners := range t.byHash {
		for _, rec := range owners {
			if !rec.forgiven && !t.expiredAt(rec.at, now, t.ttl) {
				n++
				break
			}
		}
	}

	return n
}

// clock reads the injected time source, tolerating a tracker built as a struct
// literal without one.
func (t *blockDownloadTracker) clock() time.Time {
	if t.now == nil {
		return time.Now()
	}

	return t.now()
}

// expiredAt reports whether an assignment made at `at` has aged past maxAge. A
// non-positive maxAge means "never expires", which is what a tracker built
// without a ttl gets.
func (t *blockDownloadTracker) expiredAt(at, now time.Time, maxAge time.Duration) bool {
	if maxAge <= 0 {
		return false
	}

	return now.Sub(at) >= maxAge
}

// removeOwnerLocked drops one (block, peer) pair from both directions.
func (t *blockDownloadTracker) removeOwnerLocked(p *peerpkg.Peer, h chainhash.Hash) {
	t.removeOwnerFromHashLocked(p, h)

	if hashes, ok := t.byPeer[p]; ok {
		delete(hashes, h)

		if len(hashes) == 0 {
			delete(t.byPeer, p)
		}
	}
}

// removeOwnerFromHashLocked drops the pair from byHash only, dropping the hash
// entirely once nobody owes it. The caller is responsible for byPeer, so
// ClearPeer can delete a peer's whole set in one go.
func (t *blockDownloadTracker) removeOwnerFromHashLocked(p *peerpkg.Peer, h chainhash.Hash) {
	owners, ok := t.byHash[h]
	if !ok {
		return
	}

	delete(owners, p)

	if len(owners) == 0 {
		delete(t.byHash, h)
	}
}

// maybeSweepLocked drops aged-out assignments if a sweep is due. Sweeping runs
// at most once every quarter of the ttl so it costs nothing in the steady state;
// readers do not depend on it having run, because they check each assignment's
// own age.
func (t *blockDownloadTracker) maybeSweepLocked(now time.Time) {
	if t.ttl <= 0 || now.Sub(t.lastSweep) < t.ttl/4 {
		return
	}

	t.sweepExpiredLocked(now)
}

// sweepExpiredLocked drops every assignment that has aged past the ownership
// ceiling. It is the only thing that removes a record the caller did not ask to
// remove: expiry means the peer's hour is up and its copy is no longer welcome,
// so nothing honest is thrown away.
func (t *blockDownloadTracker) sweepExpiredLocked(now time.Time) {
	t.lastSweep = now

	for h, owners := range t.byHash {
		for p, rec := range owners {
			if t.expiredAt(rec.at, now, t.ttl) {
				t.removeOwnerLocked(p, h)
			}
		}
	}
}
