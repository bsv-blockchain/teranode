package netsync

import (
	"bytes"
	"context"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	safeconversion "github.com/bsv-blockchain/go-safe-conversion"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	peerpkg "github.com/bsv-blockchain/teranode/services/legacy/peer"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/stores/blob"
	"github.com/bsv-blockchain/teranode/stores/blob/options"
	"github.com/bsv-blockchain/teranode/ulogger"
)

const (
	// parkSubDirectory is the one directory in the temp store the park owns.
	// Nothing else writes here, so the park cannot collide with anything
	// already there.
	parkSubDirectory = "legacy-parked-blocks"

	// parentMissingRetryAfter is how long a parked block waits before the drain
	// offers it again after a commit that failed for a missing parent.
	//
	// Short enough that a parent arriving normally is acted on promptly, and long
	// enough that one uncommittable block cannot spend every turn. The park sweep
	// runs every thirty seconds and re-offers such a block the moment its parent
	// really is stored, so this is the drain's own floor rather than the only
	// route back.
	parentMissingRetryAfter = 5 * time.Second

	// awaitingProofRetryAfter is parentMissingRetryAfter's twin for a block the
	// header cache has not proven yet (see parkedBlock.awaitingProofAt). The
	// floor exists to stop the drain spinning, not to pace the proof: a fill
	// that proves the block clears the stamp and re-offers it at once.
	awaitingProofRetryAfter = 5 * time.Second

	// parkStuckThreshold is how old a parked block must be before the sweep
	// spends an RPC asking whether its parent is in the chain after all. A
	// missing parent is not the only thing that surfaces as ErrBlockNotFound,
	// and restart-recovered blocks never see a commit event for their parent.
	parkStuckThreshold = 2 * time.Minute

	// parkAbandonAfter is how long a parked block may wait with its parent
	// genuinely absent from the chain before the sweep gives up on it.
	//
	// Without this, a block whose parent never arrives has no reclaim path at
	// all. The park's own Delete needs the parent positively adjudicated first,
	// so it never fires for a parent that is simply never coming. The restart
	// scan only discards a blob that will not decode or hashes wrong, so it
	// re-adopts an orphaned entry on every boot rather than reclaiming it. And
	// the blob carries no delete-at-height of its own (parkOpts' WithNoDAH), so
	// the store cannot prune it underneath the index either. Two ordinary ways
	// to reach this: a losing fork at the frontier, where two peers race for the
	// same height and one side's parent is never going to commit, and no race
	// at all — an unrequested block from a stale or eclipsed peer, parked behind
	// a parent belonging to a chain this node will never follow.
	//
	// The number has to be well clear of any delay a genuinely slow but healthy
	// commit path can produce, or this becomes a new way to lose good blocks
	// rather than a way to stop leaking bad ones. This codebase has already
	// measured what "genuinely slow" can mean: TestNewModel_ACheckpointAnchorDoesNotGateAnything
	// documents mainnet wedged for 10h40m and, the night before, 8h33m, both
	// real stalls with real parked blocks that went on to commit once the
	// underlying bug was fixed. A THIRTY-MINUTE timer was tried in this exact
	// park before EvictBelow replaced it, and it discarded 39 perfectly good
	// blocks in one measured 19-minute window on mainnet — direct evidence that
	// anything under an hour is too tight even in ordinary operation, let alone
	// during a stall like the ones above. 24 hours is comfortably past both
	// measured incidents, so a block still unparented after it is far more
	// likely orphaned than merely waiting, while still bounding the leak to at
	// most a day's worth of entries rather than the life of the process.
	//
	// Deliberately not the store's own block-height retention (8 blocks on
	// mainnet, under a second at this branch's rate): that number bounds how
	// long a COMMITTED block's data is kept, a completely different question
	// answered on a completely different clock, and reusing it here would
	// abandon every legitimately slow park within moments of it forming.
	parkAbandonAfter = 24 * time.Hour

	// parkSweepRPCBudget caps how many of those lookups one sweep tick may make,
	// so the safety net can never turn into a scan of the whole park in one go.
	//
	// It has to be big enough that a full pass over a full park finishes in
	// minutes rather than hours, or the safety net is not one: a restart with a
	// full park would leave most of those blocks unexamined, and a parent that
	// arrived quietly would go unnoticed. The park's disk is bounded by the
	// download walk's read-ahead depth rather than by an entry count, but even a
	// park several thousand blocks deep — far more than the depth will ever
	// produce — clears at 128 a tick in tens of ticks, minutes rather than hours,
	// and it is still only 128 sequential chain lookups per thirty seconds on the
	// commit goroutine, which is well under a percent of it.
	// TestBlockPark_AFullParkIsAskedAboutBeforeAnyOfItExpires holds the
	// arithmetic to this.
	parkSweepRPCBudget = 128

	// parkFullPassBudget is how long the sweep may take to ask about every block
	// in a full park, and it is what parkSweepRPCBudget is sized against.
	//
	// It used to be the thirty-minute expiry that bounded this: a full pass had
	// to finish before blocks started being thrown away. Nothing is thrown away
	// on a clock any more, so the bound is what an operator will tolerate for a
	// parent that turned up quietly to be noticed.
	parkFullPassBudget = 20 * time.Minute

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

// parkedBlock is what the park remembers about a block on disk. The bytes
// themselves are never resident.
type parkedBlock struct {
	hash      chainhash.Hash
	prevBlock chainhash.Hash
	// height from the converted record, or 0 when it will not fit. 0 is a
	// defined state, because HandleConvertedBlock derives the height from the
	// parent whenever the record carries none.
	height int32
	size   int64
	// wireSize is the block's size on the wire, declared when it streamed. size is what the park
	// charges, which is the converted record's small size; the read-ahead budget needs the
	// block's real size. A block recovered from disk takes it from its record.
	wireSize int64
	// peer that delivered the block, or nil for a block recovered from disk.
	// Both nil and disconnected are defined states; see livePeer.
	peer     *peerpkg.Peer
	parkedAt time.Time
	// lastSweptAt is when the stuck sweep last handed this entry back for a
	// parent lookup, zero if it never has. The sweep takes the least recently
	// looked at first, which is what makes it a round robin over the whole park
	// rather than a repeated random sample of it.
	lastSweptAt time.Time
	// parentMissingAt is when a commit of this block last failed because its
	// parent was not in the chain, zero if it never has.
	//
	// It exists to stop a hot retry. A commit that fails this way keeps the
	// blob and leaves the parent queued for a drain, so the very next turn
	// picks the same block and fails the same way. Measured on mainnet on
	// 2026-09-10 at 14:15: 1,494 of the last 3,000 log lines were that one
	// failure, about seven a second, on the goroutine that commits blocks — and
	// a second parked block whose parent WAS the tip never got a turn, so the
	// node ran flat out committing nothing.
	//
	// A parent that is genuinely missing is not going to appear within a turn,
	// so waiting before asking again costs nothing and hands the turn to a block
	// that can actually be committed.
	//
	// Also stamped for a missing parent OUTPUT (parkDispositionLocalUtxoFault),
	// for the same reason: a UTXO set that is missing an output this node
	// should hold is not going to grow it within a turn either, and without
	// the stamp the restored block is re-dispatched every consumer turn off
	// the still-queued drain request for its parent.
	parentMissingAt time.Time
	// awaitingProofAt is when a commit of this block last failed because it is
	// on the unified below-checkpoint route and the header cache could not
	// prove it is on the checkpointed chain (HandleConvertedBlock's refusal),
	// zero if it never has.
	//
	// Same hot-retry problem as parentMissingAt, different cause and different
	// way out. A proof arrives from the header walk, not from a commit, so
	// nothing on the drain's own path changes the answer between one turn and
	// the next; without this floor the consumer would re-dispatch the same
	// unproven block as fast as block validation's existence check answers,
	// for every parked record after a restart, until the walk reaches a
	// checkpoint. fillHeaderCache re-offers the block with the stamp cleared
	// the moment a fill proves it. One window stays open: parkedBlockFailed
	// re-derives the condition at stamp time, but a fill that lands between
	// that read and applyParkDisposition's Restore finds nothing in the park
	// to re-offer, and the entry goes back stamped with the proof present. The
	// cost there is one floor, awaitingProofRetryAfter, before the drain or the
	// sweep offers it again and it commits; never a verdict on the block.
	awaitingProofAt time.Time

	// claim is the hand-out this copy was taken under, set by Take,
	// TakeChildren and TakeChildrenForProof, and zero on a copy nobody took
	// (an entry built for AdoptWritten, or one the on-disk handler posts). Only
	// a copy carrying the claim the park recorded for the hash may Restore it
	// or settle its record with Delete; see blockPark.out.
	claim uint64
}

// blockPark keeps blocks whose parent is not stored yet on disk, and commits
// them when the parent lands.
//
// Every method is safe on a nil receiver and reads nil as "the park is off", so
// the many tests that build SyncManager as a struct literal, and any deployment
// that turns the park off, take the old discard path unchanged.
type blockPark struct {
	logger ulogger.Logger
	store  blob.Store
	dir    string

	// storeTimeout is the ceiling on ONE blob store operation. Every one of the
	// park's — write, read back, delete, and the header peek the restart scan
	// makes — carries it, because all of them wait on the same process-wide
	// permit pool and all but the restart scan run on the single goroutine that
	// commits blocks in order.
	storeTimeout time.Duration

	mu      sync.Mutex
	entries map[chainhash.Hash]*parkedBlock
	// charged is what each block has been billed to the byte budget, by hash.
	//
	// bytes used to be moved by hand at four sites, two of which subtracted a
	// size the CALLER supplied, so a block settled twice was subtracted twice
	// and a block restored without being re-billed was never subtracted at all.
	// Neither shows up as an error: the running total simply drifts, and the
	// floor at zero swallows the evidence. Mainnet's counter read 14.7 GB
	// against 9.2 GB actually on disk.
	//
	// Billing per hash makes both impossible rather than unlikely. A charge is
	// recorded once, a release subtracts exactly what was recorded, and both are
	// idempotent, so bytes is the sum of this map by construction.
	charged  map[chainhash.Hash]int64
	children map[chainhash.Hash][]chainhash.Hash
	bytes    int64

	// out records every block taken out of the index and not yet given back
	// (Restore) or settled (Delete), by hash, with the claim it was taken
	// under. A taken block's record is still on disk and still in use, so
	// while it is out nothing may adopt it again (AdoptWritten, adoptRecord),
	// nothing but its taker may put it back or delete its record, and
	// blockCommitting reads it as in use.
	//
	// It replaces three in-use marks kept outside the park, each of which
	// closed one window between a take and its own mark: the sweep's
	// hand-over to the consumer, the serial drain's commit, and the gap
	// between a take and the dispatcher's frontier. Restore, which every
	// put-back goes through, checked none of them, so a put-back posted by the
	// on-disk handler re-indexed a block the dispatcher was validating, or one
	// already committed and deleted. Every take and every put-back passes
	// through this lock, so the rule is atomic where the marks were not.
	out       map[chainhash.Hash]uint64
	nextClaim uint64
}

// newBlockPark builds the park. It is not optional: every block legacy sync
// downloads streams into it and is committed from it, so a node that cannot build
// one refuses to start rather than running a route nothing uses any more.
func newBlockPark(logger ulogger.Logger, tSettings *settings.Settings, store blob.Store) (*blockPark, error) {
	if tSettings == nil {
		return nil, errors.NewConfigurationError("[blockPark] no settings")
	}

	if store == nil {
		return nil, errors.NewConfigurationError("[blockPark] there is no temp store (temp_store) for the block park")
	}

	dir := parkDirectory(tSettings.Legacy.TempStore)
	if dir == "" {
		scheme := "none"
		if tSettings.Legacy.TempStore != nil {
			scheme = tSettings.Legacy.TempStore.Scheme
		}

		// Not a directory we can enumerate, so a restart could never adopt or
		// clean up what a previous run parked, and every blob would leak.
		return nil, errors.NewConfigurationError("[blockPark] temp_store scheme %q cannot be scanned on restart; the block park needs a file:// temp store", scheme)
	}

	storeTimeout := tSettings.Legacy.ParkStoreTimeout
	if storeTimeout < parkMinStoreTimeout {
		storeTimeout = parkMinStoreTimeout
	}

	logger.Infof("[blockPark] parking out-of-order blocks in %s, store deadline %s", dir, storeTimeout)

	return &blockPark{
		logger:       logger,
		store:        store,
		dir:          dir,
		storeTimeout: storeTimeout,
		entries:      make(map[chainhash.Hash]*parkedBlock),
		children:     make(map[chainhash.Hash][]chainhash.Hash),
		charged:      make(map[chainhash.Hash]int64),
		out:          make(map[chainhash.Hash]uint64),
	}, nil
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

// aheadBytes sums the parked blocks' wire sizes, counting a block with none recorded at unknown.
func (p *blockPark) aheadBytes(unknown int64) int64 {
	if p == nil {
		return 0
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	var total int64

	for _, e := range p.entries {
		switch {
		case e.wireSize > 0:
			total += e.wireSize
		default:
			total += unknown
		}
	}

	return total
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

// HandedOut reports whether this block has been taken out of the park and not
// yet given back or settled: its record is on disk and its taker is using it.
func (p *blockPark) HandedOut(hash chainhash.Hash) bool {
	if p == nil {
		return false
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	_, ok := p.out[hash]

	return ok
}

// handOutLocked removes entry from the index, records it as handed out under a
// fresh claim and returns the copy the taker holds. The caller holds mu and
// removes the parent edge itself, since the three takes keep their edges
// differently.
func (p *blockPark) handOutLocked(entry *parkedBlock) parkedBlock {
	if p.out == nil {
		p.out = make(map[chainhash.Hash]uint64)
	}

	p.nextClaim++

	taken := *entry
	taken.claim = p.nextClaim

	p.out[taken.hash] = taken.claim
	delete(p.entries, taken.hash)

	return taken
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

// WriteConvertedBlock stores a block pipelineBlockSink has already converted and
// merkle-verified, as a serialized model.Block under fileformat.FileTypeBlock —
// a few hundred bytes: header, counts, subtree hashes, coinbase.
//
// Uses the same options as every other park write, parkOpts, so a converted
// record's DAH is cleared here rather than inherited from the store's own
// retention (see parkOpts's doc comment), and the same per-operation deadline
// as WriteStreamedBody, because this runs on the goroutine that is converting
// the block as it streams off the socket and an unbounded store wait would
// block that goroutine the same as any other park write would.
func (p *blockPark) WriteConvertedBlock(ctx context.Context, hash chainhash.Hash, blk *model.Block) error {
	if p == nil || p.store == nil {
		return errors.NewProcessingError("[blockPark][%s] no store to write a converted record into", hash)
	}

	raw, err := blk.Bytes()
	if err != nil {
		return errors.NewProcessingError("[blockPark][%s] failed to serialize the converted block", hash, err)
	}

	writeCtx, cancel := p.storeCtx(ctx)
	defer cancel()

	return p.store.Set(writeCtx, hash[:], fileformat.FileTypeBlock, raw, parkOpts...)
}

// AdoptWritten registers a block whose body is already on disk, and reports
// whether it was taken.
//
// It is the only way in. A streamed body lands before the park hears about it
// at all: the wire handler puts it in the store on its way past, so by the time
// anything can register an entry the bytes are down.
//
// Recovery after a restart builds its entries exactly this way. Its version is
// inline in Recover because it runs once at start with the park to itself; this
// one is called from a peer's read loop while the consumer is working, so it
// takes the lock and reports refusal rather than assuming it can always insert.
//
// Refuses a hash already held, so a re-delivered body is neither charged nor
// indexed twice, and a hash handed out (see out), whose record its taker is
// still using. There is no entry ceiling to refuse at any more: the park's
// disk is bounded upstream, by the download walk's read-ahead depth, not by how
// much room this index has left.
func (p *blockPark) AdoptWritten(entry parkedBlock) bool {
	if p == nil {
		return false
	}

	if entry.parkedAt.IsZero() {
		entry.parkedAt = time.Now()
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	if !p.adoptableLocked(entry.hash) {
		return false
	}

	stored := entry
	stored.claim = 0
	p.entries[entry.hash] = &stored
	p.children[entry.prevBlock] = append(p.children[entry.prevBlock], entry.hash)
	p.chargeLocked(entry.hash, entry.size)
	p.setGauges()

	return true
}

// adoptableLocked reports whether hash may be indexed: not in the index already
// and not handed out. The caller holds mu.
func (p *blockPark) adoptableLocked(hash chainhash.Hash) bool {
	if _, held := p.entries[hash]; held {
		return false
	}

	_, out := p.out[hash]

	return !out
}

// ReadConverted reads back a converted record and checks it is the block the
// key names — the same check Read makes for a whole block, and for the same
// reason: a blob stored under a well-formed hash looks legitimate to everything
// downstream, and nothing there knows to distrust it.
func (p *blockPark) ReadConverted(ctx context.Context, hash chainhash.Hash) (*model.Block, error) {
	if p == nil || p.store == nil {
		return nil, errors.NewNotFoundError("[blockPark][%s] no store to read a converted record from", hash)
	}

	readCtx, cancel := p.storeCtx(ctx)
	defer cancel()

	raw, err := p.store.Get(readCtx, hash[:], fileformat.FileTypeBlock, parkOpts...)
	if err != nil {
		return nil, err
	}

	blk, err := model.NewBlockFromBytes(raw)
	if err != nil {
		return nil, errors.NewBlockInvalidError("[blockPark][%s] converted record would not decode", hash, err)
	}

	if got := blk.Header.Hash(); !got.IsEqual(&hash) {
		return nil, errors.NewBlockInvalidError("[blockPark][%s] converted record's header hashes to %s, not the key it was read under", hash, got)
	}

	return blk, nil
}

// convertedRecordSize returns the byte length of the converted record under
// hash, and whether one exists there at all. handleBlockOnDiskMsg needs this so
// it can charge the park's byte budget with what a pipelined block actually put
// on disk — a few hundred bytes — instead of the whole block's wire size the
// streaming path charges; see handleBlockOnDiskMsg's own comment for why the
// two numbers are deliberately different.
func (p *blockPark) convertedRecordSize(ctx context.Context, hash chainhash.Hash) (int64, bool, error) {
	if p == nil || p.store == nil {
		return 0, false, nil
	}

	readCtx, cancel := p.storeCtx(ctx)
	defer cancel()

	exists, err := p.store.Exists(readCtx, hash[:], fileformat.FileTypeBlock, parkOpts...)
	if err != nil || !exists {
		return 0, false, err
	}

	raw, err := p.store.Get(readCtx, hash[:], fileformat.FileTypeBlock, parkOpts...)
	if err != nil {
		return 0, false, err
	}

	return int64(len(raw)), true, nil
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

	taken := make([]parkedBlock, 0, len(hashes))

	// Kept, not dropped. A block still being written stays in the index AND
	// keeps this edge, because losing the edge is the hole the early
	// registration exists to close.
	var kept []chainhash.Hash

	for _, h := range hashes {
		entry, ok := p.committableChildLocked(h)
		if !ok {
			if entry != nil {
				kept = append(kept, h)
			}

			continue
		}

		taken = append(taken, p.handOutLocked(entry))
	}

	if len(kept) == 0 {
		delete(p.children, parent)
	} else {
		p.children[parent] = kept
	}

	p.setGauges()

	return taken
}

// committableChildLocked answers whether one child hash may be committed now. The
// caller holds mu.
//
// A hash with no entry is gone and its edge goes with it: (nil, false). A hash
// inside its missing-parent backoff keeps its edge but waits: (entry, false).
// Anything else is committable: (entry, true). Two callers ask this question, the
// drain that takes every child at once and the drain step that peeks one at a
// time, and keeping the answer in one place stops them diverging.
func (p *blockPark) committableChildLocked(child chainhash.Hash) (*parkedBlock, bool) {
	entry, ok := p.entries[child]
	if !ok {
		return nil, false
	}

	// Failed for a missing parent within the backoff, so not worth the turn. The
	// sweep re-offers it once the parent is genuinely stored, and the drain will
	// pick it up on its own after the backoff if nothing else does.
	if !entry.parentMissingAt.IsZero() && time.Since(entry.parentMissingAt) < parentMissingRetryAfter {
		return entry, false
	}

	// Refused for a missing header proof within the floor, so not worth the
	// turn either: the proof comes from the header walk, and fillHeaderCache
	// re-offers the block with the stamp cleared the moment a fill proves it.
	if !entry.awaitingProofAt.IsZero() && time.Since(entry.awaitingProofAt) < awaitingProofRetryAfter {
		return entry, false
	}

	return entry, true
}

// FirstChildFor returns a copy of the first child of parent that may be committed
// now, without removing it from the index. It reports false when the parent has no
// child that is ready.
//
// Peek, then claim, and the order is the whole point. Claiming first and asking
// the dispatcher for admission second leaves a refused block stranded: out of the
// index, blob on disk, still charged against the park's budget, no cursor rewind,
// and nothing to recover it until the process restarts. That is not a corner case
// under a one-deep window, which is what the block-size ladder forces in a
// giant-block era, because the depth arm refuses every candidate while anything
// else is in flight.
//
// The copy carries the size Admit already computed, which is what the byte arm of
// the admission test needs and would otherwise read as zero.
func (p *blockPark) FirstChildFor(parent chainhash.Hash) (parkedBlock, bool) {
	if p == nil {
		return parkedBlock{}, false
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	for _, h := range p.children[parent] {
		if entry, ok := p.committableChildLocked(h); ok {
			return *entry, true
		}
	}

	return parkedBlock{}, false
}

// Restore puts a block the caller took and could not commit back in the index:
// the blob is still on disk and still charged, so this is a re-index and nothing
// more. It reports whether it did.
//
// Only the taker may: entry must carry the claim the park recorded when it was
// taken (see out). Anything else is refused. A copy nobody took, such as the
// entry the on-disk handler posts for a block it has just adopted, has no claim,
// and by the time it is received that block may be out with the dispatcher, so
// re-indexing it would give the block a second entry. A copy whose hand-out has
// already ended has a claim the park no longer holds: either it was put back
// already, or its record was deleted (Delete), and re-indexing that would list a
// block whose record is gone.
func (p *blockPark) Restore(entry parkedBlock) bool {
	if p == nil {
		return false
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	if claim, out := p.out[entry.hash]; !out || entry.claim == 0 || claim != entry.claim {
		return false
	}

	delete(p.out, entry.hash)

	if _, ok := p.entries[entry.hash]; ok {
		// Unreachable while every insert checks out, and kept so that an index
		// entry is never overwritten.
		return false
	}

	stored := entry
	stored.claim = 0
	p.entries[entry.hash] = &stored
	p.children[entry.prevBlock] = append(p.children[entry.prevBlock], entry.hash)

	// Re-bill if the path that took this block gave its charge back. Whether it
	// did depends on which path that was, and asking here rather than at each
	// call site is the point: the charge is a property of the block, not of the
	// route it travelled.
	p.chargeLocked(entry.hash, entry.size)
	p.setGauges()

	return true
}

// Delete drops a block's blob and releases its budget. The entry must already
// have been taken out of the index (TakeChildren, Take) or this is called
// with one that was never in it.
//
// A delete failure is not fatal: Del takes a write permit from the same
// contended pool as the park write, so it can time out — and it carries the
// configured deadline for exactly that reason. The entry is forgotten either
// way and the restart sweep collects the file.
//
// This is the ONLY place that deletes a parked blob (applyParkDisposition's
// own comment says as much of its callers), which is exactly why the
// converted record's delete belongs here too: every path that retires an
// entry — a successful commit, an ordinary discard — already
// funnels through this one function, so putting the record's cleanup here
// once covers all of them, rather than only the discard path that happened to
// call it explicitly.
//
// This must never be extended to also
// delete the SUBTREE files a converted record names: those are content-
// addressed and shared, this function runs on the commit path as much as the
// discard path, and a committed block's subtree files are its own data now —
// deleting them here would destroy a block this node just accepted. Only
// pipelineBlockDelete's own discard-only path may remove subtree files.
//
// A handed-out block's record belongs to its taker (see out), so a Delete for a
// handed-out hash by a copy without that claim is refused and the record left:
// it is a copy of the block that was never adopted, and the record on disk is
// the one the taker is reading. The taker's own Delete ends the hand-out only
// after the record is gone, so there is no moment at which the record is on
// disk, out of the index and marked by nothing, which is what the download
// pass would adopt as stranded.
func (p *blockPark) Delete(ctx context.Context, entry parkedBlock) {
	if p == nil {
		return
	}

	p.mu.Lock()

	claim, out := p.out[entry.hash]
	if out && claim != entry.claim {
		p.mu.Unlock()
		p.logger.Warnf("[blockPark][%s] not deleting the converted record: the block is handed out and this copy is not the taker's", entry.hash)

		return
	}

	p.releaseLocked(entry.hash)
	p.setGauges()
	p.mu.Unlock()

	delCtx, cancel := p.storeCtx(ctx)
	defer cancel()

	if err := p.store.Del(delCtx, entry.hash[:], fileformat.FileTypeBlock, parkOpts...); err != nil {
		p.logger.Warnf("[blockPark][%s] failed to delete converted record, leaving it for the next restart sweep: %v", entry.hash, err)
	}

	if out {
		p.mu.Lock()
		if p.out[entry.hash] == entry.claim {
			delete(p.out, entry.hash)
		}
		p.mu.Unlock()
	}
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

// AllParked returns a copy of every currently parked entry, with no age
// threshold and no per-call limit.
//
// It exists for reconcileRecoveredParents, which runs once, right after
// Recover, and needs to ask about every block the restart scan adopted rather
// than a budgeted, round-robin sample of them the way StuckCandidates does for
// the sweep's every-thirty-seconds pass. Bounded by whatever Recover put in the
// park, which is bounded in turn by the download walk's read-ahead depth (see
// Admit's own comment) rather than by anything in this function.
func (p *blockPark) AllParked() []parkedBlock {
	if p == nil {
		return nil
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	out := make([]parkedBlock, 0, len(p.entries))

	for _, entry := range p.entries {
		out = append(out, *entry)
	}

	return out
}

// TakeChildrenForProof removes and returns every block parked directly behind
// parent that is in the index, whether or not a retry floor is holding it back.
// It exists for fillHeaderCache's re-offer: the caller has just obtained the
// proof awaitingProofAt was waiting for, so the floor TakeChildren honours is
// the one thing that must not stop it. The copies come back with their stamps
// intact, because the proof may not be for every child taken: a fork sibling at
// the same height is lifted out too, and the caller gives it back through
// Restore exactly as it was, floor and all. Clearing the stamp is the caller's
// to do, on the one entry the proof is for. A hash with no entry is gone and its
// edge goes with it, as committableChildLocked answers for TakeChildren. Blobs
// stay on disk and charged until the caller commits (Delete) or gives back
// (Restore).
func (p *blockPark) TakeChildrenForProof(parent chainhash.Hash) []parkedBlock {
	if p == nil {
		return nil
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	hashes := p.children[parent]
	if len(hashes) == 0 {
		return nil
	}

	taken := make([]parkedBlock, 0, len(hashes))

	for _, h := range hashes {
		entry, ok := p.entries[h]
		if !ok {
			continue
		}

		taken = append(taken, p.handOutLocked(entry))
	}

	delete(p.children, parent)

	p.setGauges()

	return taken
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

	taken := p.handOutLocked(entry)
	p.removeChildLocked(entry.prevBlock, hash)
	p.setGauges()

	return taken, true
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

// chargeLocked bills a block to the byte budget once. The caller holds mu.
//
// Idempotent on purpose: Restore puts back a block that may or may not still be
// billed, depending on which path took it, and asking that question at every
// call site is how the old accounting drifted.
func (p *blockPark) chargeLocked(hash chainhash.Hash, size int64) {
	if _, ok := p.charged[hash]; ok {
		return
	}

	// Lazily, because a blockPark built as a struct literal — which several
	// tests do — has no map, and a park that cannot bill is worse than one that
	// allocates late.
	if p.charged == nil {
		p.charged = make(map[chainhash.Hash]int64)
	}

	p.charged[hash] = size
	p.bytes += size
}

// releaseLocked gives back exactly what a block was billed, and nothing if it
// was not billed at all. The caller holds mu.
func (p *blockPark) releaseLocked(hash chainhash.Hash) {
	size, ok := p.charged[hash]
	if !ok {
		return
	}

	delete(p.charged, hash)

	p.bytes -= size

	// Unreachable now that every charge and release goes through this pair, and
	// kept as an alarm rather than a cushion: a floor that silently absorbs a
	// negative total is what let the old drift run unnoticed.
	if p.bytes < 0 {
		p.logger.Warnf("[blockPark] byte accounting went negative after releasing %s; this is a bug", hash)

		p.bytes = 0
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

// hasCompleteRecord answers whether a converted record that has already been
// read and hash-checked (record) is safe to treat as held: every subtree file
// it names is still on disk. record being nil means the record itself is not
// there, or would not read, so the answer is (false, nil) before anything is
// walked.
//
// This is the ONE place that decides "complete" for a converted record.
// Recover calls it while deciding whether a record left over from a previous
// run is worth adopting; adoptStranded calls it for a record that reached disk
// with nothing announcing it; holdsBlock calls it while deciding whether a
// block needs downloading again; commitParkedBlock and the dispatcher's
// parkedRun call it before handing a record to block validation. Five callers,
// one question, one answer.
//
// The answer has three states, not two, and the third is the one that
// matters on the commit path. (true, nil): every file is there. (false, nil):
// a stat ran and said a file is absent, positive evidence the record cannot be
// committed from. (false, err): a stat could not run at all. The file store
// takes a read permit before every Exists and gives up on it after its
// deadline with a StorageError, and the same call is cancelled at shutdown;
// neither says anything about the files. holdsBlock and adoptStranded read
// the third state as "not held", which costs a re-download of a record still
// on disk. The commit path must not read it as "absent": there, absent means
// the only copy of a downloaded block is deleted, and deleting it over a store
// that was briefly out of permits is the regression block_park_policy.go
// exists to stop.
//
// Either file type counts, and that is a completeness question rather than a
// validation one. subtreeWriter writes .subtreeToCheck for every record now,
// but a record on disk can still have .subtree beside or instead of it: block
// validation promotes the structure to .subtree after validating and leaves the
// .subtreeToCheck in place on the unified route, and a record parked by an
// earlier build was stamped .subtree from the header proof alone. This used to
// recompute the type from the fast-path predicate, which was wrong as soon as
// that predicate stopped being a pure function of height (it read a proof from
// the header cache that a refill can drop), and a record written as .subtree
// would have been judged incomplete and downloaded again.
//
// Nothing is laundered by accepting both. The file's type is what it is on disk;
// this decides only whether the record still points at files that exist.
// .subtreeToCheck still means "needs validating" to everything downstream, and
// .subtree still means block validation decided it did not; that decision is
// not revisited here and was never revisited here.
func (p *blockPark) hasCompleteRecord(ctx context.Context, hash chainhash.Hash, record *model.Block, subtreeStore blob.Store) (bool, error) {
	if record == nil {
		return false, nil
	}

	if subtreeStore == nil || len(record.Subtrees) == 0 {
		return true, nil
	}

	// Every subtree the record names, not just the first. Checking only
	// Subtrees[0] was the cheap version of this fix and it left the gap it
	// was written to close: a record whose later files have gone is adopted,
	// commits cleanly here, and then fails inside validation, landing on the
	// same destructive path a genuinely bad block does, which deletes the
	// only copy and blames an honest peer.
	for _, subtree := range record.Subtrees {
		var (
			found   bool
			statErr error
		)

		// A stat error on one type and a clean "absent" on the other is not
		// evidence of absence: the file may well be there under the type whose
		// stat failed. So an error is remembered across both types and wins
		// over a clean miss; only two clean misses say the structure is gone.
		for _, structureType := range []fileformat.FileType{fileformat.FileTypeSubtree, fileformat.FileTypeSubtreeToCheck} {
			exists, err := subtreeStore.Exists(ctx, subtree[:], structureType)
			if err != nil {
				statErr = err

				continue
			}

			if exists {
				found = true

				break
			}
		}

		if !found {
			if statErr != nil {
				return false, statErr
			}

			p.logger.Warnf("[blockPark][%s] converted record's subtree %s is gone; treating the record as not held", hash, subtree)

			return false, nil
		}

		// The structure file alone is not proof. subtreeWriter writes it last, so at
		// write time its presence means the data file is there too, but nothing keeps
		// that true afterwards: subtreeWriter.DeleteAll removes the data file first and
		// carries on past a failed delete, and expiry removes each file on its own. Quick
		// validation reads the data file with no other source (readSubtree in
		// blockvalidation's quick_validate.go), and the .subtreeToCheck route
		// (CheckBlockSubtrees) can only replace a missing one by fetching it over HTTP
		// from the delivering peer's base URL, so a record without it is not one this
		// node can be sure of committing.
		dataExists, dataErr := subtreeStore.Exists(ctx, subtree[:], fileformat.FileTypeSubtreeData)
		if dataErr != nil {
			return false, dataErr
		}

		if !dataExists {
			p.logger.Warnf("[blockPark][%s] converted record's subtree %s has no data file; treating the record as not held", hash, subtree)

			return false, nil
		}
	}

	return true, nil
}

// Recover adopts whatever a previous run left on disk, and cleans up whatever
// it cannot adopt. It reports the counts so a restart can never be silent about
// what it found — including finding nothing in a directory that had files in it.
//
// It scans the filesystem because blob.Store has no way to list what it holds.
// That is only sound because every park operation passes the same fixed option
// set, so the layout is flat and known whatever the temp_store URL says.
//
// subtreeStore exists only for the converted-record
// case: the record itself carries no delete-at-height and so never expires,
// but the subtree files it names do (subtree_writer.go), so a record can
// outlive the files it points at. Adopting one anyway would commit and then
// fail inside validation, which lands on the same destructive path a bad
// block does — see HandleConvertedBlock and applyParkDisposition. Before
// adopting, this checks that the record's first subtree file still exists and
// discards the record instead if it does not; the fix is cheap because one
// check stands in for the whole list, and reachability is rated poor because
// it needs both a long-parked conversion and the retention window to have
// actually elapsed underneath it. A nil store (every whole-block test in this
// file passes one) skips the check entirely and a record is adopted exactly as it
// was before this task.
//
// The fast-path predicate this used to take as a second argument is gone: see
// hasCompleteRecord for why the file type is no longer derived from it.
// adoptRecord indexes a converted record that is already complete on disk, under its parent,
// charged at its on-disk size. Startup recovery and the download pass's stranded-record
// adoption share it, so the two cannot disagree about what an adopted entry looks like.
// It reports false when the block is already indexed or handed out.
func (p *blockPark) adoptRecord(hash chainhash.Hash, record *model.Block, size int64, parkedAt time.Time) bool {
	recoveredHeight, heightErr := safeconversion.Uint32ToInt32(record.Height)
	if heightErr != nil {
		p.logger.Warnf("[blockPark][%s] converted record's height %d will not fit, adopting it without one", hash, record.Height)

		recoveredHeight = 0
	}

	// The record carries the size the block had on the wire, which is what the download backstop
	// counts. Without it a recovered block counted at the largest recent block, and after a
	// restart on 2026-09-25 about 28 of them read as 61.7 GB and idled three of four peers.
	wireSize, sizeErr := safeconversion.Uint64ToInt64(record.SizeInBytes)
	if sizeErr != nil {
		wireSize = 0
	}

	entry := parkedBlock{hash: hash, prevBlock: *record.Header.HashPrevBlock, height: recoveredHeight, size: size, wireSize: wireSize, parkedAt: parkedAt}

	p.mu.Lock()
	defer p.mu.Unlock()

	if !p.adoptableLocked(hash) {
		return false
	}

	stored := entry
	p.entries[entry.hash] = &stored
	p.children[entry.prevBlock] = append(p.children[entry.prevBlock], entry.hash)
	p.chargeLocked(entry.hash, size)
	p.setGauges()

	return true
}

// strandedRecordAge is how long a record must sit on disk unannounced before the download pass
// treats it as stranded. A normal record is announced within milliseconds of being written, so
// a minute is far past any honest gap and still recovers a stranded one within a minute.
const strandedRecordAge = time.Minute

// adoptStranded indexes a complete converted record that is on disk but not in the park. A
// record can reach disk with nothing announcing it: the peer that wrote it can be disconnected
// between the write and the on-disk message that admits it, and then only a restart's recovery
// scan would ever find it. Until then the download pass saw the block as held and never asked
// for it, and the drain never offered it, so the chain stopped at its parent for good. It
// reports whether it adopted the record; a record that is unreadable or missing a subtree file
// is left to holdsBlock, which then answers false so the block is downloaded again.
func (p *blockPark) adoptStranded(ctx context.Context, hash chainhash.Hash, subtreeStore blob.Store) bool {
	if p == nil || p.Has(hash) || p.HandedOut(hash) {
		return false
	}

	readCtx, cancel := p.storeCtx(ctx)
	defer cancel()

	// Only a record that has sat unannounced is stranded. One written a moment ago has its
	// announcement on the way, and adopting it first makes the announcement skip scheduling the
	// drain. A record whose file cannot be stat'ed is left alone for the same reason.
	info, statErr := os.Stat(filepath.Join(p.dir, hash.String()+"."+string(fileformat.FileTypeBlock)))
	if statErr != nil || time.Since(info.ModTime()) < strandedRecordAge {
		return false
	}

	record, err := p.ReadConverted(readCtx, hash)
	if err != nil {
		return false
	}

	// Both a clean "absent" and a stat that could not run answer not adopted:
	// the record stays on disk either way, and holdsBlock then answers false
	// for it, so the cost of the unknown case is a re-download, never a loss.
	if complete, checkErr := p.hasCompleteRecord(readCtx, hash, record, subtreeStore); checkErr != nil || !complete {
		return false
	}

	size := info.Size() - int64(fileformat.Header{}.Size())
	if size < 0 {
		size = 0
	}

	parkedAt := info.ModTime()

	return p.adoptRecord(hash, record, size, parkedAt)
}

func (p *blockPark) Recover(ctx context.Context, subtreeStore blob.Store) {
	if p == nil {
		return
	}

	// One overall budget for the whole scan, on top of the per-operation
	// deadline every store call already carries.
	//
	// Without it the cost of starting the node is files x storeTimeout, and the
	// number of files is set by whatever a PREVIOUS run left behind — a run that
	// may have kept far more blocks on disk than this one will, so this run
	// refuses most of them and pays a store delete for each. That happens before
	// blockHandler is started, so it is time the node spends not syncing, not
	// answering, and not visibly doing anything.
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

		case strings.HasSuffix(name, "."+string(fileformat.FileTypeBlock)):
			// A converted record left by a previous run: a header, counts and
			// subtree hashes, not a whole block. Its previous-block hash and size
			// come from the record itself via ReadConverted, never from a wire
			// block — there is no wire block on disk to read one from, and even
			// where a whole-block sibling still existed this record is the thing
			// that will actually be committed, so it is the thing recovery must
			// describe.
			//
			// Losing one of these to a restart used to be silent: before this
			// case existed, a name ending in ".block" fell through to "anything
			// we do not recognise is left alone" and stayed on disk across every
			// future restart, since nothing else in this loop, or anywhere else,
			// would ever revisit it — a downloaded block, quietly never asked
			// for again and never adopted either.
			hash, err := chainhash.NewHashFromStr(strings.TrimSuffix(name, "."+string(fileformat.FileTypeBlock)))
			if err != nil {
				p.logger.Warnf("[blockPark] %s in the park directory is not named after a block hash, leaving it alone: %v", name, err)

				skipped++

				continue
			}

			record, err := p.ReadConverted(ctx, *hash)
			if err != nil {
				// The same policy readParkedPrevBlock's caller applies below: a
				// failure that says nothing about the record (busy store, budget
				// ran out) keeps it for the next start; only a positively bad
				// record — will not decode, or hashes to something else — is
				// deleted. Discarding one of these costs only a re-conversion the
				// next time this hash is needed, not correctness: the subtree
				// files it names expire on their own delete-at-height
				// (subtree_writer.go) regardless of whether this record survives
				// to point at them again.
				d := parkReadFailure(err)
				if d.blob != parkBlobDrop {
					p.logger.Warnf("[blockPark][%s] converted record could not be read (%s), leaving it on disk for the next start: %v", hash, d.reason, err)

					skipped++

					continue
				}

				p.logger.Warnf("[blockPark][%s] converted record is unusable, deleting it: %v", hash, err)
				p.Delete(ctx, parkedBlock{hash: *hash})

				discarded++

				continue
			}

			// The record decoded and hashed correctly, but that says nothing
			// about whether the subtree files it names are still there: the
			// record has no delete-at-height of its own, while every subtree
			// file does (subtree_writer.go), so a record can survive long
			// enough to outlive them. hasCompleteRecord is the one place that
			// answers this; holdsBlock asks it the identical question when
			// deciding whether a block needs downloading again, so the two
			// cannot disagree about what "complete" means.
			//
			// Only a stat that RAN and said absent deletes the record. A stat
			// that could not run (the store out of read permits inside the
			// deadline, the recovery budget spent) leaves the record where it
			// is, under the same policy the unreadable-record arm above
			// applies: the next start looks again, and until then holdsBlock
			// answers false for it so the block is downloaded if it is needed.
			complete, checkErr := p.hasCompleteRecord(ctx, *hash, record, subtreeStore)
			if checkErr != nil {
				p.logger.Warnf("[blockPark][%s] could not check the converted record's subtree files, leaving it on disk for the next start: %v", hash, checkErr)

				skipped++

				continue
			}

			if !complete {
				p.Delete(ctx, parkedBlock{hash: *hash})

				discarded++

				continue
			}

			info, err := dirEntry.Info()
			if err != nil {
				skipped++

				continue
			}

			// The store's own 8-byte header is on disk alongside the record, the
			// same as it is for a whole block below, so it is stripped the same
			// way to leave the record's own byte count.
			size := info.Size() - int64(fileformat.Header{}.Size())
			if size < 0 {
				size = 0
			}

			parkedAt := info.ModTime()
			if parkedAt.IsZero() || parkedAt.After(time.Now()) {
				parkedAt = time.Now()
			}

			// The height is in the record and was already read above for the
			// quick-validation test. Dropping it here is not cosmetic: it is the
			// same height the drain logs and reasons about for every other entry,
			// and a recovered entry that never got one would be the one exception.
			p.adoptRecord(*hash, record, size, parkedAt)

			adopted++
			adoptedBytes += size

			continue

		case isDuplicateCopyFile(name):
			// A second copy of a block a crash left half-written (conversion_race.go). Nothing
			// reads it back.
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

		// A whole raw block, written by an older build. Nothing commits a raw block any more,
		// so it is deleted with its checksum sidecar, and the block is downloaded again.
		p.removeParkFile(name)
		p.removeParkFile(name + ".sha256")

		discarded++
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

// removeParkFile unlinks one file from the park directory by name.
func (p *blockPark) removeParkFile(name string) {
	if err := os.Remove(filepath.Join(p.dir, name)); err != nil && !os.IsNotExist(err) {
		p.logger.Warnf("[blockPark] failed to remove %s from the park directory: %v", name, err)
	}
}
