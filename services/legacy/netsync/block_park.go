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
	safeconversion "github.com/bsv-blockchain/go-safe-conversion"
	"github.com/bsv-blockchain/go-wire"
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
	// Nothing else in the repo writes fileformat.FileTypeMsgBlock blobs, so the
	// park cannot collide with anything already there.
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

	// parkReadSlowAfter is how long reading a parked block back may take before
	// it is worth a line. Below this the read is not the reason a block was slow
	// and a line per commit would only crowd out what is.
	parkReadSlowAfter = 500 * time.Millisecond

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
	peer     *peerpkg.Peer
	parkedAt time.Time
	// lastSweptAt is when the stuck sweep last handed this entry back for a
	// parent lookup, zero if it never has. The sweep takes the least recently
	// looked at first, which is what makes it a round robin over the whole park
	// rather than a repeated random sample of it.
	lastSweptAt time.Time
	// writing is true between the entry being registered and its bytes reaching
	// the disk. The entry is registered first so that a parent committing in
	// that window finds the block in children rather than missing it, and the
	// flag is what stops every reader acting on a blob that is not there yet.
	// Restore and Recover both insert entries whose write has already
	// landed, so the zero value is correct for them.
	writing bool
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
	parentMissingAt time.Time

	// parentDrained records that a drain for this block's parent ran while the
	// block was still being written, and was refused. The drain cannot come
	// back on its own — it is driven by a commit that has already happened — so
	// whoever finishes the write has to ask for it again or the block sits in
	// the park behind a parent that is already in the chain.
	parentDrained bool

	// converted says whether this entry's blob is a converted record
	// (FileTypeBlock) rather than a whole block (FileTypeMsgBlock). Set once,
	// at AdoptWritten (from the sink's own BlockBody.Converted, the only place
	// that genuinely knows) and at Recover (from which suffix the file on disk
	// carried), and read from ever after — never re-derived by asking the
	// store.
	//
	// Before this field existed, commitParkedBlock and the dispatcher's
	// parkedRun each asked blockPark.IsConverted before every commit, which is
	// a store Exists call: one of the file store's 768 process-wide read
	// permits, held for the store's configured timeout, on commitParkedBlock's
	// single goroutine that commits every parked block in order. That ran
	// unconditionally, for every parked commit, whether or not the streaming
	// pipeline was ever active — the same store round trip
	// this park already gates on BlockBody.Converted at streaming_install.go's
	// entry construction (see the comment there), just not here. This field
	// closes that gap the same way: the answer is a fact this entry already
	// carries, not a question the store needs to answer again.
	converted bool
}

// admitResult says what Admit did with an offered block, and in particular
// whether the caller now owes it a write.
type admitResult int

const (
	// admitAlreadyHeld: this block is already parked. Nothing is owed.
	admitAlreadyHeld admitResult = iota
	// admitRegistered: the entry is in the index with its write still owed. The
	// caller MUST follow with WriteAdmitted, which settles it either way.
	admitRegistered
	// admitNoRoom: the budget or the entry cap refused it. Nothing is owed.
	admitNoRoom
)

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

	logger.Infof("[blockPark] parking out-of-order blocks in %s, store deadline %s", dir, storeTimeout)

	return &blockPark{
		logger:       logger,
		store:        store,
		dir:          dir,
		storeTimeout: storeTimeout,
		entries:      make(map[chainhash.Hash]*parkedBlock),
		children:     make(map[chainhash.Hash][]chainhash.Hash),
		charged:      make(map[chainhash.Hash]int64),
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

	stored, admitted := p.Admit(entry, msgBlock)

	switch admitted {
	case admitAlreadyHeld:
		return parkAccepted

	case admitNoRoom:
		return parkUnavailable

	case admitRegistered:
	}

	if result := p.WriteAdmitted(ctx, stored, msgBlock); result != parkAccepted {
		return result
	}

	p.FinishWrite(stored.hash)

	return parkAccepted
}

// Admit takes the cheap half of parking: the duplicate check, the byte budget,
// and registering the entry with its write still owed. It never touches the
// disk, so it is safe on the goroutine that commits blocks in order.
//
// A caller that gets admitRegistered owes the block a WriteAdmitted, and until
// that lands the entry carries the writing flag and every reader refuses it.
//
// The stateless check deliberately runs AFTER this rather than before, which is
// the one thing that changed when the write moved off the commit goroutine. A
// block that turns out to be rubbish therefore holds an entry and its bytes for
// as long as that check takes, which is now a header read rather than the merkle
// rebuild it used to be. That is bounded by the number of parking
// workers rather than by anything an attacker chooses, the entry is invisible to
// every reader while it is held, and WriteAdmitted gives all of it back. The
// order that matters for safety is unchanged: nothing reaches the disk until the
// check has passed.
func (p *blockPark) Admit(entry parkedBlock, msgBlock *wire.MsgBlock) (parkedBlock, admitResult) {
	if p == nil {
		return parkedBlock{}, admitNoRoom
	}

	// SerializeSize is arithmetic over the decoded block, not a serialization.
	entry.size = int64(msgBlock.SerializeSize())
	entry.parkedAt = time.Now()
	entry.writing = true

	p.mu.Lock()
	defer p.mu.Unlock()

	// A re-delivered copy of something we already hold costs nothing. Refresh
	// the recorded peer, because the newer one is more likely to still be
	// connected when the block drains.
	if existing, ok := p.entries[entry.hash]; ok {
		if entry.peer != nil {
			existing.peer = entry.peer
		}

		return *existing, admitAlreadyHeld
	}

	// No cap here, and none is needed. There used to be two: a byte budget, and
	// then an entry count after the byte budget was found to be evaluated too
	// late to save anything — a block's size is only known once it has been
	// downloaded and decoded, so neither could stop the bandwidth being spent,
	// only throw away a block already in hand. On mainnet on 2026-09-09 the byte
	// budget cost 153 discarded gigabyte-class downloads in two hours, one every
	// 47 seconds, each one re-requested and paid for again.
	//
	// The entry count was worse: it could refuse the one block that would empty
	// the park. Its defence was that the oldest parked block is closest to being
	// committable, so the newest arrival is the right one to turn away. That is
	// false whenever there is a hole: the block closest to being committable is
	// the one whose parent is in the chain, and with a hole that is the newest
	// arrival, not the oldest resident. A park filled above a hole therefore
	// refused the block that would have drained it, on every retry, with nothing
	// evicting to make room.
	//
	// What bounds the disk instead is the download walk's read-ahead depth, which
	// is legacy_blockDownloadLowerWindow and is expressed in blocks. That is the
	// bound SV Node uses (fTooFarAhead against MinBlocksToKeep, checked before a
	// block is written rather than after), and being in blocks it can be checked
	// before the bandwidth is spent. A record already on disk is never in
	// anyone's way, so nothing here has to choose what to destroy.

	// Registered BEFORE the write, not after it. A parent that commits while
	// this block is still being written has to find the block in children, or
	// the drain looks at a park that does not yet mention it and only a later
	// sweep recovers it. What keeps that safe is the flag rather than the
	// entry's absence: every reader refuses an entry whose bytes are not on
	// disk yet.
	p.chargeLocked(entry.hash, entry.size)

	stored := entry
	p.entries[entry.hash] = &stored
	p.children[entry.prevBlock] = append(p.children[entry.prevBlock], entry.hash)
	p.setGauges()

	return stored, admitRegistered
}

// WriteAdmitted runs the stateless check and then the blob write for a block
// Admit has registered, and rolls the admission back on any failure. It is the
// half that costs a streamed write of the whole block, so it is what runs on a
// parking worker rather than on the goroutine that commits blocks in order.
//
// The check itself is cheap now. It reads the header and the transaction count
// and nothing else, so the write is the whole cost; see validateParkCandidate.
//
// On parkAccepted the entry is still registered and still flagged; the caller
// clears the flag with FinishWrite once it has done whatever else it owes.
func (p *blockPark) WriteAdmitted(ctx context.Context, entry parkedBlock, msgBlock *wire.MsgBlock) parkResult {
	if p == nil {
		return parkDisabled
	}

	// Timed, not deadlined. The check is CPU work that no context can interrupt
	// part way through, so legacy_parkStoreTimeout cannot bound it; what the
	// timing can do is make it visible, so an operator who sees a parking worker
	// stalling can tell it from store contention.
	//
	// It should now never fire. The check reads the header and the transaction
	// count, so it is microseconds whatever the block's size. It used to rebuild
	// the merkle tree over every transaction and ran to minutes, which is what
	// this timing was added for. Left in place because a warning here would mean
	// the check had grown a walk over the block again.
	validationStart := time.Now()

	err := validateParkCandidate(msgBlock, entry.hash)

	if elapsed := time.Since(validationStart); elapsed > p.storeTimeout {
		p.logger.Warnf("[blockPark][%s] the stateless check on a %d transaction block took %s, longer than the %s store deadline; it reads only the header, so this means it has grown a walk over the block", entry.hash, len(msgBlock.Transactions), elapsed, p.storeTimeout)
	}

	if err != nil {
		p.Abandon(entry)

		p.logger.Warnf("[blockPark][%s] refusing to park an invalid block: %v", entry.hash, err)

		return parkRejected
	}

	if err := p.write(ctx, entry.hash, msgBlock); err != nil {
		p.Abandon(entry)

		p.logger.Warnf("[blockPark][%s] failed to park block, it will have to be downloaded again: %v", entry.hash, err)

		return parkUnavailable
	}

	return parkAccepted
}

// Abandon gives back everything Admit reserved: the entry, its parent edge and
// its byte charge. Anything left behind here is an entry with no blob under it,
// refused by every reader because the flag never clears, holding its bytes
// against the budget for the life of the process.
func (p *blockPark) Abandon(entry parkedBlock) {
	if p == nil {
		return
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	current, ok := p.entries[entry.hash]
	if !ok || !current.writing {
		return
	}

	delete(p.entries, entry.hash)
	p.removeChildLocked(entry.prevBlock, entry.hash)
	p.releaseLocked(entry.hash)
	p.setGauges()
}

// FinishWrite clears the flag on a block whose bytes are now on disk, and
// reports whether a drain for its parent was refused while it was being
// written. A true return means the caller must ask for that drain again: the
// drain is driven by a commit that has already happened and will not come back
// on its own.
//
// The gauges do not move. Both the count and the bytes were published when the
// entry went in.
func (p *blockPark) FinishWrite(hash chainhash.Hash) bool {
	if p == nil {
		return false
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	entry, ok := p.entries[hash]
	if !ok || !entry.writing {
		return false
	}

	entry.writing = false

	return entry.parentDrained
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

// WriteStreamedBody stores a body that arrived straight off the wire, putting
// the header in front of it so what lands is byte-for-byte what write produces
// for a decoded block: header, transaction count, transactions.
//
// The header is passed separately because the wire handler has already read it
// to compute the hash and to put it to the gate, and putting it back on the
// stream there would mean re-serializing it into a buffer the streaming path
// exists to avoid. n is the total size of the finished file, header included.
func (p *blockPark) WriteStreamedBody(ctx context.Context, hash chainhash.Hash, r io.Reader, n int64) error {
	if p == nil || p.store == nil {
		return errors.NewProcessingError("[blockPark][%s] no store to stream into", hash)
	}

	writeCtx, cancel := p.storeCtx(ctx)
	defer cancel()

	return p.store.SetFromReader(writeCtx, hash[:], parkFileType, io.NopCloser(r), parkOpts...)
}

// WriteConvertedBlock stores a block pipelineBlockSink has already converted and
// merkle-verified, as a serialized model.Block under fileformat.FileTypeBlock —
// a few hundred bytes: header, counts, subtree hashes, coinbase — rather than
// under FileTypeMsgBlock, where write and WriteStreamedBody put the gigabytes a
// whole decoded block serializes to.
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
// It is the streaming path's way in, and it is the reverse of Admit's order.
// Admit registers an entry and then writes the bytes, so the flag writing
// exists to tell readers the blob is not there yet. A streamed body lands
// before the park hears about it at all: the wire handler puts it in the store
// on its way past, so by the time anything can register an entry the bytes are
// down and writing must be false or the block would sit behind a write that
// already happened.
//
// Recovery after a restart builds its entries exactly this way. Its version is
// inline in Recover because it runs once at start with the park to itself; this
// one is called from a peer's read loop while the consumer is working, so it
// takes the lock and reports refusal rather than assuming it can always insert.
//
// Refuses a hash already held, so a re-delivered body is neither charged nor
// indexed twice. There is no entry ceiling to refuse at any more: the park's
// disk is bounded upstream, by the download walk's read-ahead depth, not by how
// much room this index has left.
func (p *blockPark) AdoptWritten(entry parkedBlock) bool {
	if p == nil {
		return false
	}

	entry.writing = false

	if entry.parkedAt.IsZero() {
		entry.parkedAt = time.Now()
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	if _, held := p.entries[entry.hash]; held {
		return false
	}

	stored := entry
	p.entries[entry.hash] = &stored
	p.children[entry.prevBlock] = append(p.children[entry.prevBlock], entry.hash)
	p.chargeLocked(entry.hash, entry.size)
	p.setGauges()

	return true
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

	// Timed, because this was the one unlogged step on the drain's critical path
	// and it hid a measurable cost. Tracing a single block on mainnet on
	// 2026-09-10 accounted for every stage of a thirty-one second block except a
	// five-second stretch between the sweep offering it and delivery starting,
	// and reading it back is the only substantial work in that stretch. Parked
	// files run to 143 MB and the whole block is rebuilt as a Go object here.
	//
	// Reported only above a threshold, so the common small block stays quiet and
	// a slow read is visible without the log carrying a line per commit. Bytes
	// and duration together, because the useful figure is the rate: a read that
	// is slow because the file is enormous is a different problem from one that
	// is slow at 20 MB.
	readStart := time.Now()

	msgBlock := &wire.MsgBlock{}
	if err = msgBlock.Deserialize(bufio.NewReaderSize(rc, parkReadBufferSize)); err != nil {
		return nil, errors.NewBlockInvalidError("[blockPark][%s] parked block would not decode", hash, err)
	}

	if took := time.Since(readStart); took >= parkReadSlowAfter {
		size := int64(msgBlock.SerializeSize())
		p.logger.Infof("[blockPark][%s] reading the parked block back took %s for %d bytes (%.1f MB/s)",
			hash, took.Round(time.Millisecond), size, float64(size)/took.Seconds()/(1024*1024))
	}

	// Eighty bytes of hashing that catches a mis-keyed or bit-rotted file. The
	// merkle root is checked by HandleBlockDirect on the way in, which is the
	// only place it is checked at all, so it is not repeated here.
	if got := msgBlock.BlockHash(); !got.IsEqual(&hash) {
		return nil, errors.NewBlockInvalidError("[blockPark][%s] parked block is really %s", hash, got)
	}

	return msgBlock, nil
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

// IsConverted answers whether a converted record exists under hash. Task 3 and
// Task 4 both need to tell a converted entry apart from a whole-block one, and
// the blob's own file type already carries that distinction — FileTypeBlock
// against FileTypeMsgBlock — so nothing new has to be persisted to answer it.
func (p *blockPark) IsConverted(ctx context.Context, hash chainhash.Hash) (bool, error) {
	if p == nil || p.store == nil {
		return false, nil
	}

	readCtx, cancel := p.storeCtx(ctx)
	defer cancel()

	return p.store.Exists(readCtx, hash[:], fileformat.FileTypeBlock, parkOpts...)
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

		taken = append(taken, *entry)
		delete(p.entries, h)
	}

	if len(kept) == 0 {
		delete(p.children, parent)
	} else {
		p.children[parent] = kept
	}

	p.setGauges()

	return taken
}

// committableChildLocked answers whether one child hash may be committed now, and
// applies the writing rule when it may not. The caller holds mu.
//
// Three answers, and the middle one is why this is a function rather than an
// inline test. A hash with no entry is gone and its edge goes with it: (nil,
// false). A hash whose bytes are not on disk yet is not committable but its edge
// must stay, and the refused drain has to be remembered, because the drain is
// driven by a commit that has already happened and will not come round again on
// its own: (entry, false). Anything else is committable: (entry, true).
//
// It exists because two callers now ask this question, the drain that takes every
// child at once and the drain step that peeks one at a time, and a divergence
// between them is exactly the hole that registering an entry before its write
// exists to close.
func (p *blockPark) committableChildLocked(child chainhash.Hash) (*parkedBlock, bool) {
	entry, ok := p.entries[child]
	if !ok {
		return nil, false
	}

	if entry.writing {
		entry.parentDrained = true

		return entry, false
	}

	// Failed for a missing parent within the backoff, so not worth the turn. The
	// sweep re-offers it once the parent is genuinely stored, and the drain will
	// pick it up on its own after the backoff if nothing else does.
	if !entry.parentMissingAt.IsZero() && time.Since(entry.parentMissingAt) < parentMissingRetryAfter {
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

	// Re-bill if the path that took this block gave its charge back. Whether it
	// did depends on which path that was, and asking here rather than at each
	// call site is the point: the charge is a property of the block, not of the
	// route it travelled.
	p.chargeLocked(entry.hash, entry.size)
	p.setGauges()
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
// It always attempts both file types under the entry's hash, never only one.
// A single DELIVERY never writes both — pipelineBlockSink either converts
// (writing FileTypeBlock) or falls back to streamingBlockSink (writing
// FileTypeMsgBlock), never both in the same call — but a single HASH can
// still end up with both on disk at once: admitPipelineSink's
// ErrDuplicateBlockInFlight branch exists precisely to let one peer's read
// loop convert a hash while a second peer's concurrent delivery of the same
// hash is declined admission and falls back to streamingBlockSink, which
// writes the whole body under FileTypeMsgBlock right alongside the first
// peer's FileTypeBlock record. Attempting both deletes here is what makes
// that harmless: whichever of the two exists for this hash is removed, and
// commitParkedBlock already prefers the record over the whole block when an
// entry could in principle have adopted either (see its own converted field).
// So the second delete is a no-op only for the ordinary, single-delivery
// entry, not for every entry; it is not free lunch on every OTHER hash,
// because the key is this entry's own block hash, which nothing else's data
// lives under. This must never be extended to also
// delete the SUBTREE files a converted record names: those are content-
// addressed and shared, this function runs on the commit path as much as the
// discard path, and a committed block's subtree files are its own data now —
// deleting them here would destroy a block this node just accepted. Only
// pipelineBlockDelete's own discard-only path may remove subtree files.
func (p *blockPark) Delete(ctx context.Context, entry parkedBlock) {
	if p == nil {
		return
	}

	p.mu.Lock()
	p.releaseLocked(entry.hash)
	p.setGauges()
	p.mu.Unlock()

	delCtx, cancel := p.storeCtx(ctx)
	defer cancel()

	if err := p.store.Del(delCtx, entry.hash[:], fileformat.FileTypeMsgBlock, parkOpts...); err != nil {
		p.logger.Warnf("[blockPark][%s] failed to delete parked block, leaving it for the next restart sweep: %v", entry.hash, err)
	}

	if err := p.store.Del(delCtx, entry.hash[:], fileformat.FileTypeBlock, parkOpts...); err != nil {
		p.logger.Warnf("[blockPark][%s] failed to delete converted record, leaving it for the next restart sweep: %v", entry.hash, err)
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
		// No point spending a parent lookup on a block Take would then refuse.
		if entry.writing {
			continue
		}

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

	// Refused while the write is in flight, and the reason is that Take removes
	// the entry. Park would then find nothing under this hash, never clear the
	// flag, and the copy handed back here would carry writing true for good:
	// restored, it would be invisible to every reader while still holding its
	// bytes. StuckCandidates already skips these, so this guard refuses
	// something that cannot reach it today. It is here because the failure it
	// prevents is unrecoverable rather than merely wrong.
	if entry.writing {
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
// there, or would not read, so the answer is false before anything is walked.
//
// This is the ONE place that decides "complete" for a converted record.
// Recover calls it while deciding whether a record left over from a previous
// run is worth adopting; holdsBlock calls it while deciding whether a block
// needs downloading again. Both are the same question, so both get the same
// answer — a record whose subtree files are gone is neither adopted here nor
// reported as held there.
//
// A missing or errored subtree stat answers false. That is the safe
// direction: the alternative is adopting a commit that fails inside
// validation and lands on the path that deletes the only copy and blames an
// honest peer.
//
// Either file type counts, and that is a completeness question rather than a
// validation one. This used to be handed the fast-path predicate and stat exactly
// the type that predicate implied, which worked only while the predicate was a
// pure function of height. It is not any more: the below-checkpoint fast path now
// also requires a checkpoint-ancestry proof read from the header cache (see
// blockRequestOrigin), so the answer for the same height legitimately differs
// between the run that wrote the file and the run that is looking for it, and a
// record written as .subtree would be judged incomplete and downloaded again.
//
// Nothing is laundered by accepting both. The file's type is what it is on disk;
// this decides only whether the record still points at files that exist.
// .subtreeToCheck still means "needs validating" to everything downstream, and
// .subtree still means a previous run decided it did not — that decision is not
// revisited here and was never revisited here.
func (p *blockPark) hasCompleteRecord(ctx context.Context, hash chainhash.Hash, record *model.Block, subtreeStore blob.Store) bool {
	if record == nil {
		return false
	}

	if subtreeStore == nil || len(record.Subtrees) == 0 {
		return true
	}

	// Every subtree the record names, not just the first. Checking only
	// Subtrees[0] was the cheap version of this fix and it left the gap it
	// was written to close: a record whose later files have gone is adopted,
	// commits cleanly here, and then fails inside validation, landing on the
	// same destructive path a genuinely bad block does, which deletes the
	// only copy and blames an honest peer.
	for _, subtree := range record.Subtrees {
		var (
			exists    bool
			existsErr error
		)

		for _, structureType := range []fileformat.FileType{fileformat.FileTypeSubtree, fileformat.FileTypeSubtreeToCheck} {
			exists, existsErr = subtreeStore.Exists(ctx, subtree[:], structureType)
			if existsErr == nil && exists {
				break
			}
		}

		if existsErr != nil || !exists {
			p.logger.Warnf("[blockPark][%s] converted record's subtree %s is gone (exists=%v, err=%v); treating the record as not held",
				hash, subtree, exists, existsErr)

			return false
		}
	}

	return true
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
			// answers this — holdsBlock asks it the identical question when
			// deciding whether a block needs downloading again, so the two
			// cannot disagree about what "complete" means.
			if !p.hasCompleteRecord(ctx, *hash, record, subtreeStore) {
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

			// converted: true — this branch only ever runs for a name ending in
			// the converted-record suffix, so commitParkedBlock and parkedRun
			// must route it to ReadConverted/HandleConvertedBlock, not Read, the
			// same as an entry adopted straight off the wire would be.
			// The height is in the record and was already read above for the
			// quick-validation test. Dropping it here is not cosmetic: it is the
			// same height the drain logs and reasons about for every other entry,
			// and a recovered entry that never got one would be the one exception.
			recoveredHeight, heightErr := safeconversion.Uint32ToInt32(record.Height)
			if heightErr != nil {
				p.logger.Warnf("[blockPark][%s] converted record's height %d will not fit, adopting it without one", hash, record.Height)

				recoveredHeight = 0
			}

			entry := parkedBlock{hash: *hash, prevBlock: *record.Header.HashPrevBlock, height: recoveredHeight, size: size, parkedAt: parkedAt, converted: true}

			p.mu.Lock()
			stored := entry
			p.entries[entry.hash] = &stored
			p.children[entry.prevBlock] = append(p.children[entry.prevBlock], entry.hash)
			p.chargeLocked(entry.hash, size)
			p.setGauges()
			p.mu.Unlock()

			adopted++
			adoptedBytes += size

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
		// window on every boot: a node restarting often would never notice
		// anything had gone stale, and a block whose parent is genuinely
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
		p.chargeLocked(entry.hash, size)
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

	// A block with no transactions has no coinbase and cannot be valid, and the
	// wire decoder accepts a transaction count of zero, so a peer can simply send
	// one. Refusing here keeps it off the disk for free.
	//
	// This guard used to exist for a sharper reason: the merkle builder below it
	// sized its array as nextPowerOfTwo(n)*2-1, and nextPowerOfTwo(0) is 0, so an
	// empty transaction list asked for a slice of length -1 and panicked the
	// block-queue goroutine. That builder has gone, but every reader downstream
	// still assumes at least a coinbase, so the guard stays.
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

	// The merkle root is deliberately NOT checked here, and this is where it used
	// to be.
	//
	// It cost a merkle rebuild over every transaction in the block, which means
	// hashing every one of them, which means serialising every one of them. On
	// mainnet that measured 13.7 seconds for a 100,001-transaction block and a
	// mean of 19.9 seconds across the blocks large enough to log a warning, paid
	// on 91% of blocks because that is how many arrive out of order. By contrast
	// createSubtrees builds the same tree in 0.8 seconds on the drain path, for
	// the simple reason that the transaction hashes are already computed by then.
	//
	// The same verification runs again during normal block processing, on
	// subtrees built from the transactions the peer actually sent, so a
	// fabricated transaction list still fails. On the unified route legacy checks
	// it locally in HandleBlockDirect; off that route block validation checks it
	// server-side in validateSubtrees. Exactly one of the two runs for any block,
	// so removing this leaves one check rather than none. The duplicate-
	// transaction floor, which a merkle root cannot catch because the
	// duplicate-last-when-odd rule preserves it, runs unconditionally on every
	// route in prepareSubtrees.
	//
	// What is given up is timing, not detection: bad bytes now reach the disk and
	// are deleted when the later check refuses them. That path already exists and
	// is exercised by TestParkRejectionLeavesTheBlockRequestable, which pins the
	// property that makes this safe: a rejected block has its bytes dropped, goes
	// back on the download walk, and can be obtained again. The cascade mark the
	// rejection sets is keyed on the rejected block's own hash and the arrival
	// path tests a block's parent hash, so a block's own mark cannot suppress its
	// own retry.
	//
	// The two checks above stay because they are the cheap ones and one of them
	// is the real defence: proof of work on the 80-byte header is what stops a
	// peer minting unlimited distinct blocks to fill the park, and it cannot be
	// forged. Without it this would be an invitation.

	return nil
}
