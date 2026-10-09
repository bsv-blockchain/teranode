package netsync

import (
	"bytes"
	"sort"
	"sync"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-chaincfg"
	peerpkg "github.com/bsv-blockchain/teranode/services/legacy/peer"
	"github.com/bsv-blockchain/teranode/settings"
)

const (
	// blockRequestAssignmentTTL is the FLOOR under how long a peer stays on the
	// hook for a block we asked it for. The ceiling actually used is derived from
	// settings by blockRequestAssignmentCeiling and is never shorter than this.
	// Inherited from the per-peer map this replaced, whose comment explained the
	// hour is what legacy sync and checkpoint batches need.
	blockRequestAssignmentTTL = 60 * time.Minute

	// blockRequestRetryInterval is how long we wait before an announced block is
	// worth asking for again. It is deliberately far shorter than the ownership
	// ceiling: after a minute we are willing to ask somebody else, but the
	// original peer is still not punished if its copy turns up. Inherited from
	// the global map this replaced.
	//
	// A QUIET PEER IS USUALLY A BUSY ONE. Read this before making any rule that
	// times out, disconnects or marks unhealthy a peer that has sent no block
	// bytes for a while.
	//
	// On mainnet on 2026-09-25 peer 135.125.170.182 sent no block bytes from
	// about 10:51 to 11:13 UTC while owing us 11 blocks, then sent the 4 GB block
	// 760,331 in 1m44s at 38 MB/s. Nothing was wrong with it. What an SV Node
	// peer can be doing while it sends us nothing, from its source
	// (github.com/bitcoin-sv/bitcoin-sv):
	//
	//   - Serving our requests in order. It answers getdata one block at a time,
	//     first asked first sent (ProcessGetData, src/net/net_processing.cpp:1163
	//     and 1430-1433). Every block ahead of ours in its queue is sent first, and
	//     at recent heights those can be several GB each.
	//   - Preparing a block before its first byte. For a block it has not served
	//     since loading it by reindex or -loadblock, it reads the whole block from
	//     disk to compute the checksum the message needs before sending anything
	//     (PopulateBlockIndexBlockDiskMetaDataNL, src/block_index.cpp:322-373). For
	//     a 4 GB block that is a 4 GB read. It does this holding cs_main on the one
	//     thread that handles every peer's messages (src/net/net.cpp:2152-2196), so
	//     its other peers stall too.
	//   - Waiting on its own chain work. Connecting a block or flushing its UTXO
	//     cache also holds cs_main (src/validation.cpp:3740), which getdata needs.
	//   - Waiting for us. If this node stops reading the socket, the peer's sends
	//     block and it looks exactly like the cases above from here.
	//
	// So silence alone is not failure. The right response is the one this
	// interval drives: let another peer be asked for the block and keep the
	// connection, since a peer that is dropped loses everything it was working on
	// for us, including its other queued blocks. SV Node itself drops a block
	// peer only when its whole download window cannot move and the peer is below
	// 100 KB/s (DEFAULT_BLOCK_STALLING_TIMEOUT and DEFAULT_MIN_BLOCK_STALLING_RATE,
	// src/validation.h:124 and 129; the check at src/net/net_processing.cpp:5453).
	blockRequestRetryInterval = 60 * time.Second

	// maxTrackedBlockDownloads bounds how many distinct blocks the ledger will
	// track, so a flood of announcements cannot grow it without limit. The cap
	// is applied by refusing the newcomer, never by dropping work already in
	// progress: a block that arrives after its record was dropped looks
	// unrequested and is thrown away, a wasted download, and the block we have
	// waited longest for — the frontier everything else is queued behind — is by
	// definition the oldest record of all. Refusing is only safe because Add
	// says so to its caller, which then does not send the getdata; see Add.
	maxTrackedBlockDownloads = 50_000
)

// ownerRecord is what one peer owes us for one block.
type ownerRecord struct {
	// at is when we last asked, and what the ownership ceiling is measured from.
	// ReassertOwner moves it forward and ForgiveOwners back-dates it, for the retry window.
	at time.Time
	// asked is when the getdata for this record went out. Only Add sets it, the only path that
	// sends one, so it is the time a peer could start the block (RequestedOf, couldStart).
	asked time.Time
	// seq orders this peer's requests as they went out: a peer answers getdata
	// in the order it was asked (SV Node's ProcessGetData, one block at a time,
	// first asked first sent), so the record with the lowest seq among a peer's
	// unforgiven records is the block at the head of its queue. Stamped by Add,
	// which is the only path that sends a getdata; ReassertOwner sends nothing
	// and leaves it alone. Two Adds in the same instant get distinct values,
	// which the request clock cannot promise.
	seq uint64
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
	// seq is the last request sequence number handed out; see ownerRecord.seq.
	seq uint64
}

// blockRequestAssignmentCeiling is how long a peer stays on the hook for a block
// we asked it for: the longest a block download can legitimately take, or an
// hour, whichever is longer.
//
// svnode keeps ONE clock. Its BlockDownloadTracker has no expiry at all: an
// entry leaves when the block arrives, when the download is explicitly failed,
// or when the peer disconnects (src/net/block_download_tracker.h), and the only
// timer is the per-block in-flight timeout in DetectStalling
// (src/net/net_processing.cpp:5446), which is computed from the same
// nPowTargetSpacing * (timeoutBase + timeoutPerPeer * nOtherPeers) figure that
// bounds the transfer. This node has a second record, so it needs a second
// clock, and a flat hour was one that could expire while the peer layer was
// still legitimately extending the same transfer. A multi-gigabyte block
// completing at minute 61 of a 95-minute budget then arrived with no owner: the
// peer lost its whole association for "Got unrequested block" and a finished
// download was thrown away. Deriving both from the same settings is what stops
// them disagreeing again.
//
// The record is deliberately NOT refreshed mid-transfer. svnode refreshes
// nothing, and a refresh would be a second invention layered on the first: the
// ledger would then say a peer owes us a block for as long as it keeps sending
// bytes, which is a different question from the one it is asked.
//
// A long ceiling is the safe direction. It is a backstop, not the stall
// detector: the peer layer disconnects a genuinely stalled peer inside its own
// budget (services/legacy/peer/peer.go, DetectStalling's equivalent in the stall
// handler), and a disconnect clears that peer's assignments outright. What is
// left for expiry is the case svnode does not have to handle, a stream sub-peer
// whose release call does not run. At shipped mainnet settings the ceiling works
// out at 375 minutes: window 1024 over a per-peer depth of 16 is 64 peers, so
// 600% + 63 * 50% of a ten-minute interval.
func blockRequestAssignmentCeiling(tSettings *settings.Settings, params *chaincfg.Params) time.Duration {
	var interval time.Duration
	if params != nil {
		interval = params.TargetTimePerBlock
	}

	return max(blockRequestAssignmentTTL, peerpkg.MaxBlockDownloadBudget(tSettings, interval))
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
// request the ledger cannot vouch for comes back looking unrequested and is
// thrown away, a wasted download. Refusing the newcomer is the only way to apply
// the cap that leaves every block we are already waiting on exactly where it
// was; evicting to make room would aim that same waste at whichever peer lost
// the eviction, which for oldest-first is the frontier peer — the one block
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

	t.seq++
	owners[p] = ownerRecord{at: now, asked: now, seq: t.seq}

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
// This is what every delivery needs, raced or not: handleBlockOnDiskMsg cancels the
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

	return t.requestedWithinLocked(h, now, maxAge)
}

// Requested reports whether anybody is still on the hook for block h, judged
// against the ledger's own ownership ceiling, the same clock HasOwner uses. It is
// the question the streaming gate asks before a body is written to disk: a body
// the ledger still says somebody owes us is one we asked for, however long the
// transfer has legitimately taken.
func (t *blockDownloadTracker) Requested(h chainhash.Hash) bool {
	if t == nil {
		return false
	}

	return t.RequestedWithin(h, t.ttl)
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

// ActiveOwners lists the peers that owe h and have not been let off it, with the earliest time
// any of them was asked.
func (t *blockDownloadTracker) ActiveOwners(h chainhash.Hash) ([]*peerpkg.Peer, time.Time) {
	if t == nil {
		return nil, time.Time{}
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	owners := make([]*peerpkg.Peer, 0, len(t.byHash[h]))
	now := t.clock()

	var first time.Time

	for p, rec := range t.byHash[h] {
		if rec.forgiven || t.expiredAt(rec.at, now, t.ttl) {
			continue
		}

		owners = append(owners, p)

		if first.IsZero() || rec.at.Before(first) {
			first = rec.at
		}
	}

	return owners, first
}

// AnyOwner reports whether some peer that owes h, unforgiven, satisfies pred. pred runs outside
// the ledger's lock.
func (t *blockDownloadTracker) AnyOwner(h chainhash.Hash, pred func(*peerpkg.Peer) bool) bool {
	owners, _ := t.ActiveOwners(h)

	for _, p := range owners {
		if pred(p) {
			return true
		}
	}

	return false
}

// RequestedOf is when the getdata that p owes h for went out, and whether p owes it. Neither
// ReassertOwner, which sends nothing, nor ForgiveOwners' back-dating moves it: couldStart times
// the block from it, and a later time made the block look started later and delivered faster.
func (t *blockDownloadTracker) RequestedOf(p *peerpkg.Peer, h chainhash.Hash) (time.Time, bool) {
	if t == nil {
		return time.Time{}, false
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	rec, ok := t.byHash[h][p]

	return rec.asked, ok
}

// RequestedAt is when a request for h was first recorded, across every peer that owes it, so a
// race's later request does not hide how long ago the block was first asked for.
func (t *blockDownloadTracker) RequestedAt(h chainhash.Hash) (time.Time, bool) {
	if t == nil {
		return time.Time{}, false
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	var first time.Time

	for _, rec := range t.byHash[h] {
		if first.IsZero() || rec.at.Before(first) {
			first = rec.at
		}
	}

	return first, !first.IsZero()
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

// OutstandingAtTip names the blocks the download pass should consider when no
// header cache names any: above the last checkpoint, where a block is asked for
// off an inv and this ledger is the only record that it was asked for at all.
// The result is in request order (seq, oldest first), so a pass over it is
// deterministic and the earliest request is looked at first.
//
// Two kinds of block are named, and the first is bounded per owner:
//
//   - For each peer with records, the block at the head of its queue: its
//     lowest-seq unforgiven record. A peer sends its queue in order, so a later
//     block from the same peer cannot be the one it is stalled on, and naming
//     only the head is what keeps the pass's disk and chain checks off the rest
//     of a long queue: one getblocks reply above the checkpoint has one peer owing
//     up to 500 blocks, and checking all 500 against disk and chain costs a blob
//     read and a round trip each, every sweep. SV Node has the same shape twice
//     over: FindNextBlocksToDownload judges only the first already-in-flight block
//     (net_processing.cpp:462-507) and DetectStalling's disconnect timeout reads
//     vBlocksInFlight.front() (:5476-5500). The peer is named nothing at all while
//     a forgiven record of its, older than the head, is still inside retryWindow
//     for some other peer: that is a re-ask of an earlier head still fresh, and
//     SV Node's front stays the front until it arrives. Without that pause a peer
//     quiet for twenty minutes, which blockRequestRetryInterval's comment says is
//     normal while an SV Node peer reads a multi-gigabyte block from disk, would
//     have one more of its blocks downloaded twice on every sweep.
//   - Every block whose every live record is forgiven. demoteSyncPeer's
//     ForgetForRetryPeer leaves a stalled sync peer's whole queue in that state,
//     and handleCheckSyncPeer runs outside headers-first mode too; a re-ask that
//     found nobody leaves the same shape. Below the checkpoint the header cache
//     names such a block again on the next pass; above it nothing else does.
//
// The second kind costs something, stated plainly. handleBlockOnDiskMsg removes
// the delivering peer's record and forgives the rest, and removeOwnerFromHashLocked
// drops a hash only once nobody owes it, so every block that was re-asked and then
// delivered by either owner keeps one forgiven record for the rest of the
// ownership ceiling (375 minutes at shipped mainnet settings) and is named here on
// every sweep. While it is parked the pass skips it from memory; once committed
// the pass spends one blob read (a miss) and one GetBlockHeader round trip on it
// per sweep before the chain says it has it. The set is bounded by the number of
// re-asks, which the head-of-queue rule above keeps to one per quiet peer per
// retry window. Dropping the records at delivery instead would remove the cost
// and change a settled ledger rule (the never-forget rule at ForgetForRetryPeer),
// so it is left for a separate decision.
//
// Expired records are ignored, as everywhere else in this ledger. A nil receiver
// names nothing.
func (t *blockDownloadTracker) OutstandingAtTip(retryWindow time.Duration) []chainhash.Hash {
	if t == nil {
		return nil
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	now := t.clock()
	t.maybeSweepLocked(now)

	// hash -> the lowest seq of the records that named it, for the final order.
	named := make(map[chainhash.Hash]uint64)

	name := func(h chainhash.Hash, seq uint64) {
		if prev, ok := named[h]; !ok || seq < prev {
			named[h] = seq
		}
	}

	type peerRecord struct {
		hash chainhash.Hash
		rec  ownerRecord
	}

	for p, hashes := range t.byPeer {
		queue := make([]peerRecord, 0, len(hashes))

		for h := range hashes {
			rec, ok := t.byHash[h][p]
			if !ok || t.expiredAt(rec.at, now, t.ttl) {
				continue
			}

			queue = append(queue, peerRecord{hash: h, rec: rec})
		}

		sort.Slice(queue, func(i, j int) bool { return queue[i].rec.seq < queue[j].rec.seq })

		for _, r := range queue {
			if !r.rec.forgiven {
				name(r.hash, r.rec.seq)

				break
			}

			// An earlier head of this peer's, let off and re-asked of somebody
			// whose request is still fresh: leave the peer alone this pass.
			if t.requestedWithinLocked(r.hash, now, retryWindow) {
				break
			}
		}
	}

	for h, owners := range t.byHash {
		live := false
		allForgiven := true

		var lowest uint64

		for _, rec := range owners {
			if t.expiredAt(rec.at, now, t.ttl) {
				continue
			}

			if !rec.forgiven {
				allForgiven = false

				break
			}

			if !live || rec.seq < lowest {
				lowest = rec.seq
			}

			live = true
		}

		if live && allForgiven {
			name(h, lowest)
		}
	}

	out := make([]chainhash.Hash, 0, len(named))
	for h := range named {
		out = append(out, h)
	}

	sort.Slice(out, func(i, j int) bool {
		if named[out[i]] != named[out[j]] {
			return named[out[i]] < named[out[j]]
		}

		return bytes.Compare(out[i][:], out[j][:]) < 0
	})

	return out
}

// requestedWithinLocked is RequestedWithin with the lock already held.
func (t *blockDownloadTracker) requestedWithinLocked(h chainhash.Hash, now time.Time, maxAge time.Duration) bool {
	for _, rec := range t.byHash[h] {
		if !t.expiredAt(rec.at, now, maxAge) {
			return true
		}
	}

	return false
}

// queuedBlock is one block a peer owes, with the request's place in that peer's queue.
type queuedBlock struct {
	hash chainhash.Hash
	seq  uint64
	at   time.Time
}

// Queues lists, for each peer, the blocks it owes and has not been let off, in the order they
// were asked, which is the order the peer sends them (see ownerRecord.seq). Expired records are
// left out, as everywhere else in this ledger. A nil receiver lists nothing.
func (t *blockDownloadTracker) Queues() map[*peerpkg.Peer][]queuedBlock {
	if t == nil {
		return nil
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	now := t.clock()
	t.maybeSweepLocked(now)

	out := make(map[*peerpkg.Peer][]queuedBlock, len(t.byPeer))

	for p, hashes := range t.byPeer {
		for h := range hashes {
			rec, ok := t.byHash[h][p]
			if !ok || rec.forgiven || t.expiredAt(rec.at, now, t.ttl) {
				continue
			}

			out[p] = append(out[p], queuedBlock{hash: h, seq: rec.seq, at: rec.at})
		}

		sort.Slice(out[p], func(i, j int) bool { return out[p][i].seq < out[p][j].seq })
	}

	return out
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
