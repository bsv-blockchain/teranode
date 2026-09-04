package bridge

import (
	"context"
	"io"
	"sync"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/services/svp2p/protocol"
)

// TxFetchFunc reads one transaction's raw bytes, as a stream and its length.
// It is the shape RecentTxIndex.Open answers through; bridge fills it with
// FetchTx (fetch.go), the same UTXO-store read the getdata tx answerer uses.
type TxFetchFunc func(ctx context.Context, hash chainhash.Hash) (io.ReadCloser, uint64, error)

// RecentTxIndex is what compact-block reconstruction matches short IDs
// against. It stands in for the mempool SVNode walks in
// PartiallyDownloadedBlock::InitData (bitcoin-sv
// src/blockencodings.cpp:171-199): Teranode has no mempool, so bridge keeps a
// bounded ring of the transaction hashes it has seen recently, oldest
// evicted first, and reads the bytes from the store only when a block
// actually needs them.
//
// One site feeds it: the txmeta topic's ADD entries (kafka.go
// handleTxMetaMessage), which are the closest thing this node has to
// "entered the mempool". The orphan pool does not feed it: an orphan's bytes
// are not in the store, so an indexed orphan would match as held and fail at
// assembly. SVNode's vExtraTxnForCompact (blockencodings.cpp:194-215) keeps
// the bytes and is not ported.
//
// The index carries its own RWMutex and is never touched under the peer
// manager's syncMu: Match hashes the whole ring and Open reads the store, so
// both are long calls by the manager's standards.
//
// Memory: about 105 bytes per held hash — 32 bytes in the ring, the rest in
// the dedup map, whose 32-byte key costs roughly 70 bytes all-in. At the
// 5,000,000 default that is ~504 MiB of resident heap once the ring fills
// (a 160 MiB ring plus a ~344 MiB map), measured by
// TestRecentTxIndex_FootprintAtDefaultCapacity. Match adds a transient
// copy of the ring alone, 160 MiB at that capacity, for the length of one
// call.
type RecentTxIndex struct {
	mu sync.RWMutex

	// ring holds the hashes in insertion order, up to capacity. next is the
	// slot the following insert takes once the ring is full — which is also
	// the oldest entry, the one that insert evicts.
	ring []chainhash.Hash
	next int

	// held is the dedup set. One hash occupies at most one ring slot, so a
	// transaction announced twice cannot push out a distinct hash that a
	// block might still want.
	held map[chainhash.Hash]struct{}

	capacity int

	fetch TxFetchFunc

	// shortID is protocol.ShortID in production. It is a field so the
	// collision tests can state a collision directly instead of brute
	// forcing 48 bits of SipHash for one.
	shortID func(k0, k1 uint64, hash chainhash.Hash) uint64
}

// compile-time proof that the index is the seam the peer manager takes
// (protocol/txindex.go, Task 4).
var _ protocol.TxIndex = (*RecentTxIndex)(nil)

// NewRecentTxIndex builds an index of the given capacity, in hashes
// (legacy_compactBlocksRecentTxs). A capacity of zero builds an inert index:
// Add keeps nothing and Match matches nothing, which is the state
// legacy_compactBlocks=false leaves the bridge in. The ring grows into its
// capacity as hashes arrive rather than reserving it up front, so a node
// that never fills the ring never pays for the whole of it.
//
// fetch may be nil, which makes every Open answer protocol.ErrTxUnknown: a
// bridge with no injected store holds hashes it cannot read bytes for.
func NewRecentTxIndex(capacity int, fetch TxFetchFunc) *RecentTxIndex {
	if capacity < 0 {
		capacity = 0
	}

	return &RecentTxIndex{
		held:     make(map[chainhash.Hash]struct{}),
		capacity: capacity,
		fetch:    fetch,
		shortID:  protocol.ShortID,
	}
}

// Add records one transaction hash, evicting the oldest when the ring is
// full. A hash already held is left where it is: the index is "seen
// recently", not "seen most recently", and re-ordering on a repeat would
// cost a ring scan for no reconstruction benefit.
//
// Safe on a nil index, which is what a bridge built without New hands its
// feed sites.
func (i *RecentTxIndex) Add(hash chainhash.Hash) {
	if i == nil || i.capacity == 0 {
		return
	}

	i.mu.Lock()
	defer i.mu.Unlock()

	if _, exists := i.held[hash]; exists {
		return
	}

	if len(i.ring) < i.capacity {
		i.ring = append(i.ring, hash)
	} else {
		delete(i.held, i.ring[i.next])

		i.ring[i.next] = hash

		i.next++
		if i.next == i.capacity {
			i.next = 0
		}
	}

	i.held[hash] = struct{}{}
}

// Len reports how many hashes the index holds.
func (i *RecentTxIndex) Len() int {
	if i == nil {
		return 0
	}

	i.mu.RLock()
	defer i.mu.RUnlock()

	return len(i.ring)
}

// Match answers one compact block's short IDs, the port of InitData's
// mempool walk (bitcoin-sv src/blockencodings.cpp:171-199): build a map of
// the short IDs the block asks for, then run every held hash through the
// block's (k0,k1) and fill the positions that hit.
//
// Walking the index against the block's short IDs — rather than mapping
// every held hash to its short ID — is SVNode's own direction and the
// reason the memory cost stays proportional to the block instead of to the
// ring (5,000,000 entries would otherwise be a per-block map of hundreds of
// megabytes).
//
// Two held hashes on one short ID clear that position and set collision
// (:174-183). SVNode clears the slot too, and merely requests the
// transaction; reporting it lets the caller take BIP152's stricter answer.
// The early exit once every requested short ID is matched is SVNode's
// (:192-196), and carries SVNode's own stated risk with it: a collision
// after that point goes unseen.
//
// The ring is copied under RLock and hashed with no lock held, so a compact
// block never stalls the txmeta consumer for the length of the walk.
func (i *RecentTxIndex) Match(k0, k1 uint64, shortIDs []uint64) ([]*chainhash.Hash, bool) {
	matched := make([]*chainhash.Hash, len(shortIDs))

	if i == nil || len(shortIDs) == 0 {
		return matched, false
	}

	// wanted maps a requested short ID to the ring position that produced
	// it: unmatchedPos until one does, collidedPos once two do.
	const (
		unmatchedPos = -1
		collidedPos  = -2
	)

	wanted := make(map[uint64]int, len(shortIDs))
	for _, id := range shortIDs {
		wanted[id] = unmatchedPos
	}

	snapshot := i.snapshot()

	var (
		found     int
		collision bool
	)

	for pos := range snapshot {
		id := i.shortID(k0, k1, snapshot[pos])

		at, asked := wanted[id]
		if !asked {
			continue
		}

		switch at {
		case unmatchedPos:
			wanted[id] = pos
			found++
		case collidedPos:
		default:
			wanted[id] = collidedPos
			collision = true
			found--
		}

		if found == len(wanted) {
			break
		}
	}

	for n, id := range shortIDs {
		if at := wanted[id]; at >= 0 {
			// Copied out of the snapshot rather than pointed into it: a
			// pointer into that slice would hold the whole copy of the ring
			// alive for as long as the caller keeps one hash.
			hash := snapshot[at]
			matched[n] = &hash
		}
	}

	return matched, collision
}

// Open reads a transaction's bytes through the fetch seam. A store that does
// not hold the transaction becomes protocol.ErrTxUnknown, which is the
// caller's signal to ask the peer for it; every other store error is passed
// through, so a sick store never reads as a missing transaction.
//
// Open deliberately does not check the ring first. The store outlives
// eviction, so a hash that fell off the ring between Match and Open is
// still readable, and the store is the only authority on whether the bytes
// are actually there.
func (i *RecentTxIndex) Open(ctx context.Context, hash chainhash.Hash) (io.ReadCloser, uint64, error) {
	if i == nil || i.fetch == nil {
		return nil, 0, protocol.ErrTxUnknown
	}

	rc, size, err := i.fetch(ctx, hash)
	if err != nil {
		if errors.Is(err, errors.ErrTxNotFound) || errors.Is(err, errors.ErrNotFound) {
			return nil, 0, protocol.ErrTxUnknown
		}

		return nil, 0, err
	}

	return rc, size, nil
}

// snapshot copies the ring under the read lock. The copy is what makes the
// short-ID walk lock-free; it costs 32 bytes per held hash for the length of
// one Match.
func (i *RecentTxIndex) snapshot() []chainhash.Hash {
	i.mu.RLock()
	defer i.mu.RUnlock()

	out := make([]chainhash.Hash, len(i.ring))
	copy(out, i.ring)

	return out
}
