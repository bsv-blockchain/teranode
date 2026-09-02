package netsync

import (
	"bufio"
	"bytes"
	"context"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-wire"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/services/legacy/blockchain"
	"github.com/bsv-blockchain/teranode/services/legacy/bsvutil"
	peerpkg "github.com/bsv-blockchain/teranode/services/legacy/peer"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/stores/blob"
	"github.com/bsv-blockchain/teranode/stores/blob/options"
	"github.com/bsv-blockchain/teranode/ulogger"
)

const (
	// parkSubDirectory is the one directory in the temp store the park owns.
	// Nothing else in the repo writes fileformat.FileTypeMsgBlock blobs, so the
	// park cannot collide with anything already there.
	parkSubDirectory = "legacy-parked-blocks"

	// maxParkedEntries bounds the in-memory index, which the byte budget does
	// not: at 11 KB per block, 4 GiB of disk is nearly 400,000 entries. It is a
	// constant rather than a setting because its only job is to stop the index
	// growing without limit in the small-block regime, and 4096 is already an
	// order of magnitude above any out-of-order window multi-peer download can
	// produce (peers x per-peer in-flight, and the per-peer figure is capped at
	// 20 and falls towards 1 as blocks grow).
	maxParkedEntries = 4096

	// parkEntryTTL is how long a block may sit parked before it is given up on
	// and re-requested. A parent that has not arrived in half an hour is not
	// coming from the walk that queued it.
	parkEntryTTL = 30 * time.Minute

	// parkStuckThreshold is how old a parked block must be before the sweep
	// spends an RPC asking whether its parent is in the chain after all. A
	// missing parent is not the only thing that surfaces as ErrBlockNotFound,
	// and restart-recovered blocks never see a commit event for their parent.
	parkStuckThreshold = 2 * time.Minute

	// parkSweepRPCBudget caps how many of those lookups one sweep tick may make,
	// so the safety net can never turn into a scan of the whole park in one go.
	//
	// It has to be big enough that a full pass over a full park finishes inside
	// parkEntryTTL, or the sweep is not a safety net: a restart with a full park
	// would leave most of those blocks unexamined until they expired and were
	// downloaded a second time. parkEntryTTL / parkSweepInterval is 60 ticks, and
	// the park holds up to maxParkedEntries, so the floor is 4096/60 = 69. At 128
	// a full pass takes 32 ticks, sixteen minutes, comfortably inside the half
	// hour — and it is still only 128 sequential chain lookups per thirty
	// seconds on the commit goroutine, which is well under a percent of it.
	// TestBlockPark_AFullParkIsAskedAboutBeforeAnyOfItExpires holds the
	// arithmetic to this.
	parkSweepRPCBudget = 128

	// parkSweepExpiryBudget caps how many expired blocks one sweep tick gives up
	// on, for the same reason parkSweepRPCBudget caps the lookups beside it, and
	// against a bill that is larger per item.
	//
	// Each one costs a store Del carrying legacy_parkStoreTimeout — a write permit
	// from a pool of 256 shared process-wide with subtree writes, transaction
	// writes and both persisters — plus a cursor rewind that takes headerMu. All
	// of it on the one goroutine that commits blocks in order. Expiry arrives in
	// bursts by its nature: the blocks in a stalled park were queued together, so
	// they age out together, and an uncapped pass could hand the whole index to
	// that goroutine in a single tick. With blockQueue full, the outer message
	// loop blocks on it and disconnects, rotation, inv, headers and transaction
	// dispatch stall for every peer.
	//
	// 128 matches its neighbour and drains a full park in 32 ticks, sixteen
	// minutes — well inside parkEntryTTL, so nothing waits appreciably longer to
	// be re-requested than it did when the loop was unbounded. Which 128 a tick
	// takes is unspecified, because map order is, and it does not matter: every
	// candidate is already past the TTL, so there is no fairness question, only a
	// rate one.
	parkSweepExpiryBudget = 128

	// parkRecoverBudgetOps is how many store operations' worth of waiting the
	// whole restart scan gets, as a multiple of the per-operation deadline. It is
	// expressed that way rather than as a number of seconds because it is the
	// same question one layer up: an operator who raises the per-operation
	// deadline because their store is slow has said how patient this node should
	// be, and the scan should be that patient and no more.
	parkRecoverBudgetOps = 3

	// parkMinStoreTimeout is the floor on legacy_parkStoreTimeout. A zero or
	// negative deadline would fail every store operation instantly.
	parkMinStoreTimeout = time.Second

	// parkReadBufferSize buffers the read side of a drain. MsgBlock.Bsvdecode
	// makes many small reads and an unbuffered *os.File would make that
	// syscall-bound.
	parkReadBufferSize = 1 << 20
)

// parkOpts is the ONE option set every park read, write and delete uses, and
// the recovery scan assumes. Two divergent copies would be the whole bug: with
// WithNoHashPrefix the layout is always
// <storePath>/legacy-parked-blocks/<display-hash>.msgBlock, flat, whatever
// hashPrefix or hashSuffix the temp_store URL sets — MergeOptions copies the
// store's prefix in first and then applies these, so the zero here wins. Drop
// WithNoHashPrefix and the blobs land in shard subdirectories that the flat
// recovery scan never finds, and every parked block leaks on every restart.
//
// WithNoDAH is not belt and braces either. The park owns the lifetime of every
// blob it writes: it deletes one when the block commits, when the block is given
// up on, or when the restart scan finds it unusable, and nothing else may. The
// file store, though, derives a delete-at-height from the STORE's block-height
// retention for any blob that carries no DAH of its own, and MergeOptions copies
// that retention in before these options are applied. The temp store has no
// retention today, so without this the park's safety rested on a setting nobody
// is guarding: the day one is added, every parked blob is booked with the
// deletion scheduler and blocks start disappearing from under a live park.
// WithNoDAH clears it here, per operation, which is the only level the park can
// speak at — it does not own the store it is handed.
var parkOpts = []options.FileOption{
	options.WithSubDirectory(parkSubDirectory),
	options.WithNoHashPrefix(),
	options.WithAllowOverwrite(true),
	options.WithNoDAH(),
}

// parkResult says what happened to a block offered to the park.
type parkResult int

const (
	// parkAccepted: the block is on disk (or already was) and the park will
	// commit it when its parent lands. Nothing to re-request.
	parkAccepted parkResult = iota

	// parkRejected: the block failed the stateless checks. That is a peer fault
	// and nothing was written.
	parkRejected

	// parkUnavailable: we could not keep it — budget full, write failed, write
	// timed out. A local fault, and the block must be re-requested.
	parkUnavailable

	// parkDisabled: there is no park. The caller falls back to discarding the
	// block, exactly as it did before the park existed.
	parkDisabled
)

// parkedBlock is what the park remembers about a block on disk. The bytes
// themselves are never resident.
type parkedBlock struct {
	hash      chainhash.Hash
	prevBlock chainhash.Hash
	// height as the delivering peer reported it, which is often 0 — and 0 is a
	// defined state, because HandleBlockDirect derives the height from the
	// parent whenever it is not positive. Blocks recovered from disk after a
	// restart always have 0.
	height int32
	size   int64
	// peer that delivered the block, or nil for a block recovered from disk.
	// Both nil and disconnected are defined states; see livePeer.
	peer *peerpkg.Peer
	// removedFront is the header node this block's arrival took off the front of
	// the header list, or nil when it was never the front. It has to travel with
	// the block because advanceHeaderListFor runs before the park does: by the
	// time a block is parked its header is already removed AND unindexed, so
	// every path that later gives the block up would have nothing to look the
	// header up by, and the block would leave the download walk for good. Always
	// nil for a block recovered from disk after a restart, which is correct —
	// that node's header list was rebuilt from scratch.
	removedFront *headerNode
	parkedAt     time.Time
	// lastSweptAt is when the stuck sweep last handed this entry back for a
	// parent lookup, zero if it never has. The sweep takes the least recently
	// looked at first, which is what makes it a round robin over the whole park
	// rather than a repeated random sample of it.
	lastSweptAt time.Time
}

// blockPark keeps blocks whose parent is not stored yet on disk, and commits
// them when the parent lands.
//
// Every method is safe on a nil receiver and reads nil as "the park is off", so
// the many tests that build SyncManager as a struct literal, and any deployment
// that turns the park off, take the old discard path unchanged.
type blockPark struct {
	logger   ulogger.Logger
	store    blob.Store
	dir      string
	maxBytes int64
	// storeTimeout is the ceiling on ONE blob store operation. Every one of the
	// park's — write, read back, delete, and the header peek the restart scan
	// makes — carries it, because all of them wait on the same process-wide
	// permit pool and all but the restart scan run on the single goroutine that
	// commits blocks in order.
	storeTimeout time.Duration

	mu       sync.Mutex
	entries  map[chainhash.Hash]*parkedBlock
	children map[chainhash.Hash][]chainhash.Hash
	bytes    int64
}

// newBlockPark builds the park, or returns nil when there is not going to be
// one. Every reason to refuse is logged, at WARN when it is a configuration the
// operator may not have meant.
func newBlockPark(logger ulogger.Logger, tSettings *settings.Settings, store blob.Store) *blockPark {
	if tSettings == nil || !tSettings.Legacy.ParkOutOfOrderBlocks {
		return nil
	}

	if store == nil {
		logger.Warnf("[blockPark] out-of-order block parking is on but there is no temp store; blocks whose parent is missing will be discarded")
		return nil
	}

	if tSettings.Legacy.ParkMaxBytes <= 0 {
		logger.Infof("[blockPark] legacy_parkMaxBytes is 0, so out-of-order blocks will be discarded")
		return nil
	}

	dir := parkDirectory(tSettings.Legacy.TempStore)
	if dir == "" {
		scheme := "none"
		if tSettings.Legacy.TempStore != nil {
			scheme = tSettings.Legacy.TempStore.Scheme
		}

		// Not a directory we can enumerate, so a restart could never adopt or
		// clean up what a previous run parked, and every blob would leak. Off is
		// the only honest answer.
		logger.Warnf("[blockPark] temp_store scheme %q cannot be scanned on restart, so out-of-order blocks will be discarded instead of parked", scheme)

		return nil
	}

	storeTimeout := tSettings.Legacy.ParkStoreTimeout
	if storeTimeout < parkMinStoreTimeout {
		storeTimeout = parkMinStoreTimeout
	}

	logger.Infof("[blockPark] parking out-of-order blocks in %s, up to %d bytes, store deadline %s", dir, tSettings.Legacy.ParkMaxBytes, storeTimeout)

	return &blockPark{
		logger:       logger,
		store:        store,
		dir:          dir,
		maxBytes:     tSettings.Legacy.ParkMaxBytes,
		storeTimeout: storeTimeout,
		entries:      make(map[chainhash.Hash]*parkedBlock),
		children:     make(map[chainhash.Hash][]chainhash.Hash),
	}
}

// parkDirectory works out where the park's blobs land, using the file store's
// own rule for turning a store URL into a path. It returns "" for any store
// whose contents cannot be listed from the filesystem.
func parkDirectory(storeURL *url.URL) string {
	if storeURL == nil || storeURL.Scheme != "file" {
		return ""
	}

	path := storeURL.Path

	if storeURL.Host == "." {
		// A relative URL, file://./data/tempstore: the store strips the leading
		// separator to get back to a relative path.
		if len(path) == 0 {
			return ""
		}

		path = path[1:]
	}

	if path == "" {
		return ""
	}

	return filepath.Join(path, parkSubDirectory)
}

// Enabled reports whether there is a park to put blocks in.
func (p *blockPark) Enabled() bool {
	return p != nil
}

// Len returns how many blocks are parked.
func (p *blockPark) Len() int {
	if p == nil {
		return 0
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	return len(p.entries)
}

// Has reports whether this block is parked: downloaded, checked and on disk,
// waiting for its parent. It is the question the inventory path has to ask
// before it asks a peer for a block, because a parked block is in no other place
// that question looks.
func (p *blockPark) Has(hash chainhash.Hash) bool {
	if p == nil {
		return false
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	_, ok := p.entries[hash]

	return ok
}

// Bytes returns the serialized bytes currently charged against the budget.
func (p *blockPark) Bytes() int64 {
	if p == nil {
		return 0
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	return p.bytes
}

// Park checks a block and, if it passes, streams it to disk and remembers it.
//
// The checks come first and nothing is written until they all pass. Checking
// only that the 80-byte header hashes to the key would not be enough: a peer
// can send a genuine header with a garbage transaction list, which would then
// fail only when the block is drained — and since a failed drain gives up on
// that block, one crafted message on a public port would be enough to stop
// sync. This mirrors what SV Node does, which runs the whole stateless
// CheckBlock before it writes a block to disk.
func (p *blockPark) Park(ctx context.Context, entry parkedBlock, msgBlock *wire.MsgBlock) parkResult {
	if p == nil {
		return parkDisabled
	}

	// A re-delivered copy of something we already hold costs nothing. Refresh
	// the recorded peer, because the newer one is more likely to still be
	// connected when the block drains.
	p.mu.Lock()

	if existing, ok := p.entries[entry.hash]; ok {
		if entry.peer != nil {
			existing.peer = entry.peer
		}

		// A re-delivered copy can be the front when the first copy was not, and
		// then this is the only header node anybody still holds. Never overwrite
		// a node already recorded with nil: the first copy's is the one the list
		// is missing.
		if entry.removedFront != nil {
			existing.removedFront = entry.removedFront
		}

		p.mu.Unlock()

		return parkAccepted
	}

	p.mu.Unlock()

	// Timed, not deadlined, and the difference is the point. The stateless check
	// rebuilds the merkle tree over every transaction in the block, which is CPU
	// work on this same in-order commit goroutine that no context can interrupt
	// part way through. legacy_parkStoreTimeout cannot bound it. What it can do
	// is make it visible, so an operator who sees the commit goroutine stalling
	// can tell validation from store contention.
	validationStart := time.Now()

	err := validateParkCandidate(msgBlock, entry.hash)

	if elapsed := time.Since(validationStart); elapsed > p.storeTimeout {
		p.logger.Warnf("[blockPark][%s] the stateless check on a %d transaction block took %s, longer than the %s store deadline; this is CPU on the block commit goroutine and no deadline bounds it", entry.hash, len(msgBlock.Transactions), elapsed, p.storeTimeout)
	}

	if err != nil {
		p.logger.Warnf("[blockPark][%s] refusing to park an invalid block: %v", entry.hash, err)

		return parkRejected
	}

	// SerializeSize is arithmetic over the decoded block, not a serialization.
	entry.size = int64(msgBlock.SerializeSize())
	entry.parkedAt = time.Now()

	// Reserve the space before the write so two writers can never both pass the
	// check. Rolled back below on any failure.
	p.mu.Lock()

	if len(p.entries) >= maxParkedEntries || p.bytes+entry.size > p.maxBytes {
		held, count := p.bytes, len(p.entries)
		p.mu.Unlock()

		p.logger.Warnf("[blockPark][%s] no room for a %d byte block: %d blocks holding %d of %d bytes", entry.hash, entry.size, count, held, p.maxBytes)

		return parkUnavailable
	}

	p.bytes += entry.size
	p.mu.Unlock()

	if err := p.write(ctx, entry.hash, msgBlock); err != nil {
		p.mu.Lock()
		p.bytes -= entry.size
		p.mu.Unlock()

		p.logger.Warnf("[blockPark][%s] failed to park block, it will have to be downloaded again: %v", entry.hash, err)

		return parkUnavailable
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	stored := entry
	p.entries[entry.hash] = &stored
	p.children[entry.prevBlock] = append(p.children[entry.prevBlock], entry.hash)
	p.setGauges()

	return parkAccepted
}

// storeCtx puts the configured deadline on one blob store operation.
//
// EVERY store call the park makes goes through this, and that is the whole of
// what legacy_parkStoreTimeout promises. The file store waits up to 25 seconds
// for one of its 256 write or 768 read permits — permits it shares with subtree
// writes, transaction writes and both persisters — and a caller deadline is the
// only thing that can shorten that wait. Park, read-back and delete all run on
// the single goroutine that commits blocks in order, so an undeadlined one is
// head-of-line blocking for every block queued behind it.
func (p *blockPark) storeCtx(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(ctx, p.storeTimeout)
}

// write streams the block into the blob store. The block is serialized straight
// down a pipe, so a 150 MB block never exists twice in memory.
func (p *blockPark) write(ctx context.Context, hash chainhash.Hash, msgBlock *wire.MsgBlock) error {
	writeCtx, cancel := p.storeCtx(ctx)
	defer cancel()

	pr, pw := io.Pipe()
	serialized := make(chan error, 1)

	go func() {
		err := msgBlock.Serialize(pw)
		_ = pw.CloseWithError(err)
		serialized <- err
	}()

	err := p.store.SetFromReader(writeCtx, hash[:], fileformat.FileTypeMsgBlock, pr, parkOpts...)

	// MANDATORY, not tidiness. SetFromReader never closes the reader it is
	// given, so on any error return the goroutine above would block forever on
	// its next write — one leaked goroutine per failed park, each pinning a
	// whole decoded block. Closing the read end makes that write fail instead.
	_ = pr.Close()

	if serErr := <-serialized; serErr != nil && err == nil {
		err = serErr
	}

	return err
}

// Read fetches a parked block back off disk and checks it is the block the key
// says it is.
func (p *blockPark) Read(ctx context.Context, hash chainhash.Hash) (*wire.MsgBlock, error) {
	if p == nil {
		return nil, errors.NewNotFoundError("[blockPark] no park")
	}

	// The deadline covers getting hold of the reader, which is where the permit
	// wait is. It deliberately does NOT cover the decode below: cancelling a
	// half-read block would leave a good blob looking corrupt and give it up for
	// good, and the decode is local disk reads through a 1 MB buffer, not a
	// contended resource.
	readCtx, cancel := p.storeCtx(ctx)
	defer cancel()

	// The ReadCloser holds one of the store's 768 process-wide read permits and
	// only gives it back on Close, so every path out of here must close it.
	rc, err := p.store.GetIoReader(readCtx, hash[:], fileformat.FileTypeMsgBlock, parkOpts...)
	if err != nil {
		return nil, err
	}

	defer func() {
		if closeErr := rc.Close(); closeErr != nil {
			p.logger.Warnf("[blockPark][%s] failed to close parked block reader: %v", hash, closeErr)
		}
	}()

	msgBlock := &wire.MsgBlock{}
	if err = msgBlock.Deserialize(bufio.NewReaderSize(rc, parkReadBufferSize)); err != nil {
		return nil, errors.NewBlockInvalidError("[blockPark][%s] parked block would not decode", hash, err)
	}

	// Eighty bytes of hashing that catches a mis-keyed or bit-rotted file. The
	// merkle root was checked before the block was written and is checked again
	// by HandleBlockDirect on the way in, so it is not repeated here.
	if got := msgBlock.BlockHash(); !got.IsEqual(&hash) {
		return nil, errors.NewBlockInvalidError("[blockPark][%s] parked block is really %s", hash, got)
	}

	return msgBlock, nil
}

// TakeChildren removes and returns every block parked directly behind parent,
// in the order they were parked. The blobs stay on disk and stay charged
// against the budget until the caller either commits them (Delete) or gives
// them back (Restore).
func (p *blockPark) TakeChildren(parent chainhash.Hash) []parkedBlock {
	if p == nil {
		return nil
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	hashes := p.children[parent]
	if len(hashes) == 0 {
		return nil
	}

	delete(p.children, parent)

	taken := make([]parkedBlock, 0, len(hashes))

	for _, h := range hashes {
		if entry, ok := p.entries[h]; ok {
			taken = append(taken, *entry)
			delete(p.entries, h)
		}
	}

	p.setGauges()

	return taken
}

// Restore puts a block the caller could not commit back in the index. Used when
// the parent has gone missing again under a reorg: the blob is still on disk and
// still charged, so this is a re-index and nothing more.
func (p *blockPark) Restore(entry parkedBlock) {
	if p == nil {
		return
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	if _, ok := p.entries[entry.hash]; ok {
		return
	}

	stored := entry
	p.entries[entry.hash] = &stored
	p.children[entry.prevBlock] = append(p.children[entry.prevBlock], entry.hash)
	p.setGauges()
}

// Delete drops a block's blob and releases its budget. The entry must already
// have been taken out of the index (TakeChildren, Expire) or this is called
// with one that was never in it.
//
// A delete failure is not fatal: Del takes a write permit from the same
// contended pool as the park write, so it can time out — and it carries the
// configured deadline for exactly that reason. The entry is forgotten either
// way and the restart sweep collects the file.
func (p *blockPark) Delete(ctx context.Context, entry parkedBlock) {
	if p == nil {
		return
	}

	p.mu.Lock()
	p.bytes -= entry.size
	if p.bytes < 0 {
		p.bytes = 0
	}
	p.setGauges()
	p.mu.Unlock()

	delCtx, cancel := p.storeCtx(ctx)
	defer cancel()

	if err := p.store.Del(delCtx, entry.hash[:], fileformat.FileTypeMsgBlock, parkOpts...); err != nil {
		p.logger.Warnf("[blockPark][%s] failed to delete parked block, leaving it for the next restart sweep: %v", entry.hash, err)
	}
}

// Expire removes and returns up to limit blocks that have been parked longer
// than parkEntryTTL. Their parents are not coming; the caller re-requests them.
//
// The limit is what stops a burst of expiry becoming one long turn on the
// block-queue consumer — see parkSweepExpiryBudget. Anything over it waits for
// the next tick, which costs it nothing it was not already waiting for.
func (p *blockPark) Expire(now time.Time, limit int) []parkedBlock {
	if p == nil || limit <= 0 {
		return nil
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	var expired []parkedBlock

	for h, entry := range p.entries {
		if len(expired) == limit {
			break
		}

		if now.Sub(entry.parkedAt) < parkEntryTTL {
			continue
		}

		expired = append(expired, *entry)

		delete(p.entries, h)
		p.removeChildLocked(entry.prevBlock, h)
	}

	if len(expired) > 0 {
		p.setGauges()
	}

	return expired
}

// StuckCandidates returns up to limit blocks that have been parked longer than
// parkStuckThreshold, without removing them. The caller asks the chain whether
// their parent is present after all — ErrBlockNotFound has more than one cause,
// and a block recovered from disk after a restart never sees a commit event for
// a parent that is already in the chain.
func (p *blockPark) StuckCandidates(now time.Time, limit int) []parkedBlock {
	if p == nil || limit <= 0 {
		return nil
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	eligible := make([]*parkedBlock, 0, len(p.entries))

	for _, entry := range p.entries {
		if now.Sub(entry.parkedAt) < parkStuckThreshold {
			continue
		}

		eligible = append(eligible, entry)
	}

	// Least recently looked at first, and never looked at — the zero time —
	// before all of those. Together with the stamp below that turns the sweep
	// into a round robin: each tick takes the next budget's worth and a full pass
	// takes exactly len(entries)/limit ticks. Ranging over the map instead took a
	// fresh random sample every tick, so an entry could come up over and over
	// while others were never examined at all before they expired. Ties break on
	// the oldest parked, then on the hash, so the order is total and the sweep is
	// reproducible.
	sort.Slice(eligible, func(i, j int) bool {
		if !eligible[i].lastSweptAt.Equal(eligible[j].lastSweptAt) {
			return eligible[i].lastSweptAt.Before(eligible[j].lastSweptAt)
		}

		if !eligible[i].parkedAt.Equal(eligible[j].parkedAt) {
			return eligible[i].parkedAt.Before(eligible[j].parkedAt)
		}

		return bytes.Compare(eligible[i].hash[:], eligible[j].hash[:]) < 0
	})

	if len(eligible) > limit {
		eligible = eligible[:limit]
	}

	candidates := make([]parkedBlock, 0, len(eligible))

	for _, entry := range eligible {
		entry.lastSweptAt = now

		candidates = append(candidates, *entry)
	}

	return candidates
}

// Take removes one specific block from the index, leaving its blob on disk and
// still charged, exactly as TakeChildren does.
func (p *blockPark) Take(hash chainhash.Hash) (parkedBlock, bool) {
	if p == nil {
		return parkedBlock{}, false
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	entry, ok := p.entries[hash]
	if !ok {
		return parkedBlock{}, false
	}

	delete(p.entries, hash)
	p.removeChildLocked(entry.prevBlock, hash)
	p.setGauges()

	return *entry, true
}

// removeChildLocked drops one parent->child edge. The caller holds mu.
func (p *blockPark) removeChildLocked(parent, child chainhash.Hash) {
	siblings := p.children[parent]

	for i := range siblings {
		if siblings[i].IsEqual(&child) {
			p.children[parent] = append(siblings[:i], siblings[i+1:]...)
			break
		}
	}

	if len(p.children[parent]) == 0 {
		delete(p.children, parent)
	}
}

// setGauges publishes the park's size. The caller holds mu. Nil-guarded because
// tests build the park directly, without going through New() and its metric
// registration.
func (p *blockPark) setGauges() {
	if prometheusLegacyNetsyncParkedBlocks == nil || prometheusLegacyNetsyncParkedBytes == nil {
		return
	}

	prometheusLegacyNetsyncParkedBlocks.Set(float64(len(p.entries)))
	prometheusLegacyNetsyncParkedBytes.Set(float64(p.bytes))
}

// Recover adopts whatever a previous run left on disk, and cleans up whatever
// it cannot adopt. It reports the counts so a restart can never be silent about
// what it found — including finding nothing in a directory that had files in it.
//
// It scans the filesystem because blob.Store has no way to list what it holds.
// That is only sound because every park operation passes the same fixed option
// set, so the layout is flat and known whatever the temp_store URL says.
func (p *blockPark) Recover(ctx context.Context) {
	if p == nil {
		return
	}

	// One overall budget for the whole scan, on top of the per-operation
	// deadline every store call already carries.
	//
	// Without it the cost of starting the node is files x storeTimeout, and the
	// number of files is set by whatever a PREVIOUS run left behind — a run that
	// may have had a much larger legacy_parkMaxBytes, so this run refuses most of
	// them and pays a store delete for each. That happens before blockHandler is
	// started, so it is time the node spends not syncing, not answering, and not
	// visibly doing anything.
	//
	// Whatever the budget does not reach is simply left on disk. It is not lost:
	// nothing is charged for it, nothing points at it, and the next start scans
	// the directory again. Giving up on adopting a block is always cheaper than
	// making a node slow to start.
	recoverCtx, cancel := context.WithTimeout(ctx, parkRecoverBudgetOps*p.storeTimeout)
	defer cancel()

	ctx = recoverCtx

	dirEntries, err := os.ReadDir(p.dir)
	if err != nil {
		if !os.IsNotExist(err) {
			p.logger.Warnf("[blockPark] could not read the park directory %s, starting with an empty park: %v", p.dir, err)
		}

		return
	}

	var adopted, skipped, discarded int

	var adoptedBytes int64

	var abandoned int

	for i, dirEntry := range dirEntries {
		if ctx.Err() != nil {
			abandoned = len(dirEntries) - i

			break
		}

		name := dirEntry.Name()

		switch {
		case dirEntry.IsDir():
			skipped++

			continue

		case strings.HasPrefix(name, "."):
			// A write that a crash interrupted. The store names its in-progress
			// file ".<name>.<random>.tmp", and nothing else writes to this
			// directory, so at Start() none of these can be live.
			p.removeParkFile(name)

			discarded++

			continue

		case !strings.HasSuffix(name, "."+string(fileformat.FileTypeMsgBlock)):
			// Checksum sidecars and anything else. A sidecar whose block is gone
			// is dead weight; anything we do not recognise is left alone.
			if strings.HasSuffix(name, ".sha256") {
				block := strings.TrimSuffix(name, ".sha256")
				if _, statErr := os.Stat(filepath.Join(p.dir, block)); os.IsNotExist(statErr) {
					p.removeParkFile(name)

					discarded++

					continue
				}
			}

			skipped++

			continue
		}

		hash, err := chainhash.NewHashFromStr(strings.TrimSuffix(name, "."+string(fileformat.FileTypeMsgBlock)))
		if err != nil {
			p.logger.Warnf("[blockPark] %s in the park directory is not named after a block hash, leaving it alone: %v", name, err)

			skipped++

			continue
		}

		info, err := dirEntry.Info()
		if err != nil {
			skipped++

			continue
		}

		size := info.Size() - int64(fileformat.Header{}.Size())
		if size < 0 {
			size = 0
		}

		if adopted >= maxParkedEntries || adoptedBytes+size > p.maxBytes {
			// A previous run's park must never exceed this run's budget.
			p.Delete(ctx, parkedBlock{hash: *hash})

			discarded++

			continue
		}

		prevBlock, d, err := p.readParkedPrevBlock(ctx, *hash)
		if err != nil {
			// The same policy the drain uses, for the same reason. A read that
			// could not get one of the store's shared permits, or that was cut
			// short because this scan's own budget ran out, says nothing about
			// the block — and deleting on that would destroy fully downloaded
			// blocks on every restart that happens while the node is busy, which
			// is when restarts happen.
			if d.blob != parkBlobDrop {
				p.logger.Warnf("[blockPark][%s] parked block could not be read (%s), leaving it on disk for the next start: %v", hash, d.reason, err)

				skipped++

				continue
			}

			p.logger.Warnf("[blockPark][%s] parked block is unusable, deleting it: %v", hash, err)
			p.Delete(ctx, parkedBlock{hash: *hash})

			discarded++

			continue
		}

		// The blob's modification time is when the block was parked, and it is on
		// disk, so it is the one thing about a recovered block that survives the
		// restart. Stamping time.Now() here instead would restart every block's
		// half hour on every boot: a node restarting more often than parkEntryTTL
		// would never expire anything, and a block whose parent is genuinely
		// never coming would hold its budget for as long as that went on. Anything
		// unusable — a zero time, or a clock that has gone backwards since the
		// write — falls back to now, which is only ever the old behaviour.
		parkedAt := info.ModTime()
		if parkedAt.IsZero() || parkedAt.After(time.Now()) {
			parkedAt = time.Now()
		}

		// peer nil and height 0 are both defined: post-commit peer actions fall
		// back to the current sync peer, and HandleBlockDirect derives a
		// non-positive height from the parent.
		entry := parkedBlock{hash: *hash, prevBlock: prevBlock, size: size, parkedAt: parkedAt}

		p.mu.Lock()
		stored := entry
		p.entries[entry.hash] = &stored
		p.children[entry.prevBlock] = append(p.children[entry.prevBlock], entry.hash)
		p.bytes += size
		p.setGauges()
		p.mu.Unlock()

		adopted++
		adoptedBytes += size
	}

	if abandoned > 0 {
		p.logger.Warnf("[blockPark] recovery ran out of its %s budget with %d file(s) in %s not looked at; they are left where they are and the next start will find them", parkRecoverBudgetOps*p.storeTimeout, abandoned, p.dir)
	}

	if adopted == 0 && skipped == 0 && discarded == 0 && abandoned == 0 {
		return
	}

	// Logged even when nothing was adopted, so "recovery found nothing" is never
	// silent in a directory that had files in it.
	p.logger.Infof("[blockPark] recovered %d parked block(s) holding %d bytes from %s, discarded %d, left %d file(s) alone", adopted, adoptedBytes, p.dir, discarded, skipped)
}

// readParkedPrevBlock reads just the 80-byte header off a parked blob and
// returns its parent, checking on the way that the file really is the block its
// name claims. GetIoReader has already consumed the store's own 8-byte header,
// so the first bytes it hands back are the block header.
//
// It returns the failure's classification alongside the error, so a caller
// cannot read a parked blob without being handed the answer to "does this say
// anything about the block?". Recovery once decided that for itself and deleted
// good blocks whenever the store was busy; there is no signature here that lets
// that happen again.
func (p *blockPark) readParkedPrevBlock(ctx context.Context, hash chainhash.Hash) (chainhash.Hash, parkDisposition, error) {
	readCtx, cancel := p.storeCtx(ctx)
	defer cancel()

	rc, err := p.store.GetIoReader(readCtx, hash[:], fileformat.FileTypeMsgBlock, parkOpts...)
	if err != nil {
		return chainhash.Hash{}, parkReadFailure(err), err
	}

	defer func() {
		if closeErr := rc.Close(); closeErr != nil {
			p.logger.Warnf("[blockPark][%s] failed to close parked block reader: %v", hash, closeErr)
		}
	}()

	var header wire.BlockHeader
	if err = header.Deserialize(rc); err != nil {
		err = errors.NewBlockInvalidError("[blockPark][%s] could not read the block header", hash, err)

		return chainhash.Hash{}, parkReadFailure(err), err
	}

	if got := header.BlockHash(); !got.IsEqual(&hash) {
		err = errors.NewBlockInvalidError("[blockPark][%s] header belongs to %s", hash, got)

		return chainhash.Hash{}, parkReadFailure(err), err
	}

	return header.PrevBlock, parkDispositionParked, nil
}

// removeParkFile unlinks one file from the park directory by name.
func (p *blockPark) removeParkFile(name string) {
	if err := os.Remove(filepath.Join(p.dir, name)); err != nil && !os.IsNotExist(err) {
		p.logger.Warnf("[blockPark] failed to remove %s from the park directory: %v", name, err)
	}
}

// validateParkCandidate runs the checks that need nothing but the block itself.
// Nothing reaches the disk until every one of them passes.
func validateParkCandidate(msgBlock *wire.MsgBlock, expected chainhash.Hash) error {
	if msgBlock == nil {
		return errors.NewBlockInvalidError("[blockPark][%s] no block", expected)
	}

	// Not cosmetic. BuildMerkleTreeStore below sizes its array as
	// nextPowerOfTwo(n)*2-1, and nextPowerOfTwo(0) is 0, so an empty transaction
	// list asks for a slice of length -1 and panics. A peer can simply send a
	// block with a transaction count of zero — the wire decoder accepts it — so
	// without this guard that is a remote panic on the block-queue goroutine.
	if len(msgBlock.Transactions) == 0 {
		return errors.NewBlockInvalidError("[blockPark][%s] block has no transactions", expected)
	}

	if got := msgBlock.BlockHash(); !got.IsEqual(&expected) {
		return errors.NewBlockInvalidError("[blockPark][%s] block really hashes to %s", expected, got)
	}

	// Proof of work, stateless. This is what stops an attacker minting unlimited
	// distinct "blocks" to fill the park with. It cannot check that nBits itself
	// is right — that needs chain context and stays where it is.
	var headerBytes bytes.Buffer
	if err := msgBlock.Header.Serialize(&headerBytes); err != nil {
		return errors.NewBlockInvalidError("[blockPark][%s] could not serialize the block header", expected, err)
	}

	header, err := model.NewBlockHeaderFromBytes(headerBytes.Bytes())
	if err != nil {
		return errors.NewBlockInvalidError("[blockPark][%s] could not read the block header", expected, err)
	}

	if met, _, err := header.HasMetTargetDifficulty(); !met {
		return errors.NewBlockInvalidError("[blockPark][%s] block does not meet its own target difficulty", expected, err)
	}

	// The merkle root, which is the check that matters most here. Without it a
	// peer can pair a genuine, real-work header with any transaction list it
	// likes: the block passes the hash check above, gets parked, and only fails
	// when it is drained — by which point the block has been given up on. One
	// message on a public port would be enough to stop sync.
	merkles := blockchain.BuildMerkleTreeStore(bsvutil.NewBlock(msgBlock).Transactions())

	root := merkles[len(merkles)-1]
	if root == nil || !root.IsEqual(&msgBlock.Header.MerkleRoot) {
		return errors.NewBlockInvalidError("[blockPark][%s] transactions do not build the block's merkle root", expected)
	}

	return nil
}
