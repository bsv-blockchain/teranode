package netsync

import (
	"github.com/bsv-blockchain/go-wire"
	"github.com/bsv-blockchain/teranode/errors"
)

// This file is the whole of the park's error policy, and it is one file on
// purpose.
//
// A parked block is a block that is already downloaded, already checked against
// its own header and merkle root, and already written to disk. Many things can
// go wrong with it afterwards — the blob will not read back, the commit fails,
// the parent disappears under a reorg, its time runs out, the node is shutting
// down, the store is out of permits, the budget is full — and each of those has
// to settle the same three questions:
//
//	does the blob survive, or is the download thrown away?
//	does the download walk go back onto the block, so it is asked for again?
//	is the peer that sent it told the block was bad?
//
// Answering those questions separately at each site is what produced three
// rounds of regressions in a row, every one of them an error path doing the
// wrong thing with a block: a good block deleted because the store was busy, a
// front block that could never be asked for again, a getblocks swallowed. So
// the answers live here, in one table, and every path calls into it. Adding a
// new failure means adding a row, not inventing a new combination.

// parkBlobAction is what happens to the bytes on disk and the budget they hold.
type parkBlobAction int

const (
	// parkBlobLeaveAlone: there is nothing to do. Either nothing was ever
	// written, or the entry is already in the index and already charged. It must
	// NOT be confused with parkBlobDrop: Delete gives the budget back, so
	// calling it for a block that never took any would under-count the park.
	parkBlobLeaveAlone parkBlobAction = iota

	// parkBlobKeep: put the entry back in the index. The blob stays on disk and
	// stays charged, so this is a re-index and nothing more. Used whenever the
	// failure says nothing about the block, so the download is still good.
	parkBlobKeep

	// parkBlobDrop: delete the blob and give its budget back. Used when the
	// block is in the chain, or when it has been judged and will have to be
	// downloaded again.
	parkBlobDrop
)

// parkDisposition is one row of the table: everything that happens to a parked
// block once something has gone wrong with it (or, for parkDispositionCommitted,
// once it has gone right).
type parkDisposition struct {
	// reason is the operator-facing half of the row: what the log line says.
	reason string

	blob parkBlobAction

	// rewindCursor puts the download walk back onto the block so it is asked
	// for again. It is set exactly when the blob is NOT kept and the block is
	// not in the chain — otherwise the block is in neither the header list, nor
	// the park, nor any download ledger, and headers-first sync never asks for
	// it again.
	rewindCursor bool

	// blamePeer tells the peer that delivered the block that it was rejected.
	// Set only when the block itself is at fault. A blob we wrote that will not
	// read back is OUR fault, and a store that is out of permits or a node that
	// is shutting down is nobody's.
	blamePeer bool

	// markFailed records the block in recentlyFailedBlocks so its already-queued
	// descendants are short-circuited instead of each failing its own parent
	// lookup (#1333). Only meaningful for a block that has actually been judged
	// and given up on.
	markFailed bool
}

// The table. Read down the columns: keep-or-drop, rewind-or-not, blame-or-not.
var (
	// parkDispositionCommitted — the block is in the chain. The blob has done
	// its job; nothing to re-request and nobody to blame.
	parkDispositionCommitted = parkDisposition{
		reason: "committed from the park",
		blob:   parkBlobDrop,
	}

	// parkDispositionRetryLater — the failure says nothing about the block: the
	// store had no permit free inside the park's deadline, the read or the
	// commit was cancelled by shutdown, or this node's own storage is briefly
	// unwell. The block is already downloaded and already on disk, so keep it
	// and let the sweep try again. Rewinding here would ask a peer to send a
	// block we are holding; deleting it would throw that block away over a
	// condition that is over in seconds. Blocks kept this way are not kept
	// forever: parkEntryTTL expires them into parkDispositionExpired.
	parkDispositionRetryLater = parkDisposition{
		reason: "a local fault that says nothing about the block",
		blob:   parkBlobKeep,
	}

	// parkDispositionParentGone — the parent went missing again, which is a
	// reorg under the drain. Same three answers as retryLater and a different
	// log line, because an operator needs to tell a reorg from a busy store.
	parkDispositionParentGone = parkDisposition{
		reason: "the parent is missing again",
		blob:   parkBlobKeep,
	}

	// parkDispositionBlobUnusable — the blob is gone, or will not decode, or
	// decodes into some other block. That is evidence about the file and not
	// about the peer: we wrote it, so a bad blob is our fault. Delete it and put
	// the walk back on the block so it is downloaded again.
	parkDispositionBlobUnusable = parkDisposition{
		reason: "the parked blob is not the block it claims to be",
		blob:   parkBlobDrop,

		rewindCursor: true,
	}

	// parkDispositionExpired — the parent never arrived. The block may be
	// perfectly good, but it cannot be held any longer, so it is given up on and
	// re-requested. Not the peer's fault: it sent what we asked for.
	parkDispositionExpired = parkDisposition{
		reason: "the parent never arrived",
		blob:   parkBlobDrop,

		rewindCursor: true,
	}

	// parkDispositionBlockRejected — the block itself would not go into the
	// chain. This is the one case where the peer hears about it, and the only
	// one that writes the block off in recentlyFailedBlocks.
	parkDispositionBlockRejected = parkDisposition{
		reason: "the block failed to store or validate",
		blob:   parkBlobDrop,

		rewindCursor: true,
		blamePeer:    true,
		markFailed:   true,
	}

	// parkDispositionBlockRefused — the block failed the park's own stateless
	// checks, so nothing was written. A peer fault, and the block still has to
	// be asked for again because its header has already left the walk.
	parkDispositionBlockRefused = parkDisposition{
		reason: "the block failed its stateless checks",
		blob:   parkBlobLeaveAlone,

		rewindCursor: true,
		blamePeer:    true,
	}

	// parkDispositionNotKept — we could not keep the block: the budget is full,
	// the write failed or timed out, or there is no park at all. Nothing reached
	// the disk, so there is no blob and no budget to release, and a local fault
	// is not the peer's fault.
	parkDispositionNotKept = parkDisposition{
		reason: "there was no room to keep the block",
		blob:   parkBlobLeaveAlone,

		rewindCursor: true,
	}

	// parkDispositionParked — the block is on disk and in the index, waiting for
	// its parent. Nothing to undo.
	parkDispositionParked = parkDisposition{
		reason: "waiting for its parent",
		blob:   parkBlobLeaveAlone,
	}
)

// parkReadFailure classifies an error from reading a parked block back off
// disk.
//
// The default is deliberately the SAFE one, not the tidy one. Destroying a
// downloaded block needs positive evidence that the blob is bad; anything else,
// including an error nobody anticipated, keeps it. A blob that is permanently
// unreadable for an unrecognised reason is not held forever — it is retried by
// the sweep and given up at parkEntryTTL — so the cost of guessing "keep" is a
// delay, while the cost of guessing "drop" is a re-download of a block we have.
//
// One consequence worth naming: the file store raises a StorageError both for a
// genuine IO fault and for a blob whose store header is torn, and those are not
// distinguishable from here. Both are read as "retry", so a torn blob costs the
// TTL rather than an immediate re-request. That is the right way round.
func parkReadFailure(err error) parkDisposition {
	switch {
	case errors.IsContextError(err), errors.IsTransientLocalError(err):
		return parkDispositionRetryLater

	case errors.Is(err, errors.ErrNotFound),
		errors.Is(err, errors.ErrBlobNotFound),
		errors.Is(err, errors.ErrBlockInvalid):
		// ErrNotFound / ErrBlobNotFound: the file store has no such blob.
		// ErrBlockInvalid: blockPark.Read raises it itself, for a blob that will
		// not decode or that hashes to a different block.
		return parkDispositionBlobUnusable

	default:
		return parkDispositionRetryLater
	}
}

// parkCommitFailure classifies an error from committing a parked block.
//
// Its default is the opposite of parkReadFailure's, and that is on purpose: the
// live-delivery path in handleBlockMsg treats any non-transient failure of
// HandleBlockDirect as a judgement on the block and rejects it to the peer, so a
// block committed from disk is judged exactly as a block off the wire is. Two
// different answers to the same error would be its own bug.
//
// What that default costs is worth stating, because it is more than the wasted
// re-download the read path costs. parkDispositionBlockRejected sets markFailed,
// which writes recentlyFailedBlocks, and handleBlockMsg keys its descendant
// suppression on the PARENT hash: a block wrongly judged here takes every child
// with it for recentlyFailedBlocksTTL. handleBlockMsg also reads that map as
// judgedBefore, so the next live delivery of the block spares the delivering peer
// its association eviction. Both follow from IsTransientLocalError matching only
// teranode's own error codes, so a store that hands back a raw driver error
// instead of a StorageError lands here rather than on retryLater
// (stores/utxo/sql/sql.go returns the bare error from db.Begin and txn.Commit).
// That gap is the live path's too, so closing it belongs one layer down in the
// store, not in a guess made here that would break the symmetry above.
func parkCommitFailure(err error) parkDisposition {
	switch {
	case errors.Is(err, errors.ErrBlockNotFound):
		return parkDispositionParentGone

	case errors.IsContextError(err), errors.IsTransientLocalError(err):
		return parkDispositionRetryLater

	default:
		return parkDispositionBlockRejected
	}
}

// parkWriteOutcome maps what the park did with an offered block onto what the
// caller now has to do about it, so the offer path answers the same three
// questions from the same table as every other path.
func parkWriteOutcome(result parkResult) parkDisposition {
	switch result {
	case parkAccepted:
		return parkDispositionParked

	case parkRejected:
		return parkDispositionBlockRefused

	case parkUnavailable, parkDisabled:
		return parkDispositionNotKept

	default:
		return parkDispositionNotKept
	}
}

// withoutBlame returns the same row with the peer left alone. Used while the
// node is catching blocks, where handleBlockMsg suppresses every other reject
// too: we are replaying history and a peer that hands us a block we cannot take
// has not necessarily done anything wrong.
func (d parkDisposition) withoutBlame() parkDisposition {
	d.blamePeer = false

	return d
}

// applyParkDisposition carries out one row of the table. It is the ONLY place
// that deletes a parked blob, restores a parked entry, rewinds the download
// cursor for a parked block, or rejects one to a peer.
func (sm *SyncManager) applyParkDisposition(entry parkedBlock, d parkDisposition) {
	switch d.blob {
	case parkBlobKeep:
		sm.blockPark.Restore(entry)

	case parkBlobDrop:
		sm.blockPark.Delete(sm.ctx, entry)

	case parkBlobLeaveAlone:
	}

	if d.markFailed && sm.recentlyFailedBlocks != nil {
		sm.recentlyFailedBlocks.Set(entry.hash, struct{}{})
	}

	if d.rewindCursor {
		sm.rewindHeaderCursor(entry.hash, entry.removedFront)
	}

	if !d.blamePeer {
		return
	}

	// A misbehaviour signal goes to the peer that actually sent the block or
	// nowhere at all. Aiming it at a fallback peer would punish an innocent one
	// for a block it never sent; losing the signal when the guilty peer has
	// already left is the cheaper mistake.
	if entry.peer != nil && entry.peer.Connected() {
		entry.peer.PushRejectMsg(wire.CmdBlock, wire.RejectInvalid, "block rejected", &entry.hash, false)
	} else {
		sm.logger.Warnf("[applyParkDisposition][%s] no connected peer to reject the block to; the signal is lost", entry.hash)
	}
}
