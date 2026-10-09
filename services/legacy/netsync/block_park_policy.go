package netsync

import (
	"fmt"
	"time"

	"github.com/bsv-blockchain/go-wire"
	"github.com/bsv-blockchain/teranode/errors"
)

// This file is the whole of the park's error policy, and it is one file on
// purpose.
//
// A parked block is a block that is already downloaded, already checked against
// its own header hash and proof of work, and already written to disk. Its merkle
// root was checked against the header by the pipeline sink before the record
// was written (pipelineBlockSink, pipeline_sink.go); what can still go wrong
// afterwards is local: the record or its files on disk, the UTXO set, the
// store, the chain under it. Many things can go wrong with it — the blob will
// not read back, the commit fails, the parent disappears under a reorg, its
// time runs out, the node is shutting down, the store is out of permits, the
// budget is full — and each of those has to settle the same two questions:
//
//	does the blob survive, or is the download thrown away?
//	is the peer that sent it told the block was bad?
//
// A third question used to live here too — does the download walk go back onto
// the block, so it is asked for again — but the wanted-range pass answers it by
// itself now: a block that is dropped and still above the committed tip is
// simply unowed on the next pass, with nothing left to re-anchor.
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
	// name is the row's label on park_dispositions_total: short, fixed, and
	// distinct per row, so a dashboard can group by it.
	name string

	// reason is the operator-facing half of the row: what the log line says.
	reason string

	blob parkBlobAction

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

	// dropPeer disconnects the delivering peer's whole association, resolved
	// through its primary (peer.DisconnectAssociation), so the sync peer rotates
	// and the blocks it owed are released by netsync's handleDonePeerMsg. Set
	// only when the block itself was judged invalid by a verdict that says so
	// (the explicit ErrBlockInvalid row), never on the default arm, where an
	// error nobody has classified lands. Independent of blamePeer, which is the
	// reject message: withoutBlame clears the reject while the node is catching
	// blocks and leaves this set, because a block judged invalid is invalid
	// during replay too. No ban at the drain: a commit-time verdict passes
	// through block validation and the gRPC code rebuild, and a wrong 24 h ban
	// strands the node where a wrong disconnect costs a reconnect;
	// recentlyFailedBlocks already stops the hash being re-asked.
	dropPeer bool
}

// The table. Read down the columns: keep-or-drop, rewind-or-not, blame-or-not.
var (
	// parkDispositionCommitted — the block is in the chain. The blob has done
	// its job; nothing to re-request and nobody to blame.
	parkDispositionCommitted = parkDisposition{
		name:   "committed",
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
	// forever: the sweep evicts them once the chain has gone past them.
	parkDispositionRetryLater = parkDisposition{
		name:   "retry_later",
		reason: "a local fault that says nothing about the block",
		blob:   parkBlobKeep,
	}

	// parkDispositionParentGone — the parent went missing again, which is a
	// reorg under the drain. Same three answers as retryLater and a different
	// log line, because an operator needs to tell a reorg from a busy store.
	parkDispositionParentGone = parkDisposition{
		name:   "parent_missing",
		reason: "the parent is missing again",
		blob:   parkBlobKeep,
	}

	// parkDispositionParentNotMinedYet — the parent is in the chain and valid,
	// but block validation has not yet set its mined flag, and
	// waitForPreviousBlockMined ran out of retries waiting for it (about 80 s
	// with the default blockvalidation_isParentMined_retry_* settings). That is
	// our own pipeline being slow, not anything wrong with this block or its
	// peer: the same block commits on the next attempt once the flag is set.
	// Keep the blob, write nothing off, blame nobody. The sweep resubmits it.
	// Its own log line, because "the mined flag is late" points an operator at
	// setTxMined, where a busy store or a reorg would not.
	parkDispositionParentNotMinedYet = parkDisposition{
		name:   "parent_not_mined",
		reason: "its parent is not marked mined yet",
		blob:   parkBlobKeep,
	}

	// parkDispositionBlobUnusable — the blob is gone, or will not decode, or
	// decodes into some other block. That is evidence about the file and not
	// about the peer: we wrote it, so a bad blob is our fault. Delete it; the
	// block is still in the wanted range and now unowed, so the next
	// wanted-range pass downloads it again.
	parkDispositionBlobUnusable = parkDisposition{
		name:   "blob_unusable",
		reason: "the parked blob is not the block it claims to be",
		blob:   parkBlobDrop,
	}

	// parkDispositionRecordCorrupt — block validation returned a corrupt
	// verdict for a record this node wrote. Every record that commits through
	// the park was converted at the pipeline sink, which checked the merkle
	// root against the header and derived each subtree hash from the bytes it
	// wrote before the record existed, so a corrupt verdict afterwards is about
	// this node's own files, never about the peer's bytes. Drop the record,
	// mark nothing, blame nobody; the block is unowed on the next wanted-range
	// pass and is downloaded again.
	//
	// Dropping repairs the one case it is written for: on the unified route a
	// structure file that does not hash to its key is deleted by
	// quarantineSubtreeKeyMismatch before the verdict is returned
	// (blockvalidation's quick_validate.go), so the re-download writes it
	// afresh; when the quarantine could not confirm the delete, block
	// validation returns a ServiceError instead, which lands on retryLater
	// above. subtreeWriter.put never rewrites a key that is already there, so
	// no other corrupt verdict is repaired by a re-download, and for every
	// other one this row is a loop (drop, re-ask, same verdict). What those
	// others are, and why none is reachable for a sink-written record except
	// through a bug or a damaged file: quick validation's own corrupt verdicts
	// after the anchor (coinbase placeholder outside [0][0], subtree size
	// mismatch, merkle root mismatch) restate what the sink already checked;
	// and on the full route, which a legacy block takes when the unified route
	// is off (the default) or it carries no header proof, block.Valid and
	// subtree validation raise corrupt for a duplicate
	// transaction, a merkle root mismatch, a subtree length mismatch or a
	// missing coinbase, each of which the sink refused or verified on the way
	// in, and removePeerSuppliedSubtreeToCheck leaves a .subtreeToCheck that
	// was on disk before the attempt, which netsync's always is. The one
	// full-route verdict a legacy block CAN reach on an honest run, an invalid
	// transaction in a subtree, is returned as ErrBlockInvalid for baseURL
	// "legacy" (BlockValidation.go, the ErrTxInvalid branch) because the sink
	// bound the list, so it lands on BlockRejected below and not here. A new
	// corrupt producer on either route has to answer the same question before
	// this row is right for it: does the re-download rewrite the file?
	parkDispositionRecordCorrupt = parkDisposition{
		name:   "record_corrupt",
		reason: "a corrupt verdict on a record this node wrote; the body was verified against the header at the sink, so the fault is local",
		blob:   parkBlobDrop,
	}

	// parkDispositionFilesGone — the record names subtree files that are no
	// longer on disk. The record carries no delete-at-height of its own while
	// every subtree file does (subtree_writer.go), so expiry removes the files
	// from under it; a concurrent conversion's cleanup can do the same. Drop
	// the record, mark nothing, blame nobody: the next wanted-range pass sees
	// the block as not held and downloads it again, and the sink rewrites
	// whichever files are missing (put skips the ones still there).
	//
	// Reached two ways. hasCompleteRecord on the commit path finds the files
	// gone before anything is validated, which is the normal route and the
	// one that keeps a missing file out of quick validation's UTXO work.
	// Failing that, the not-found from inside block validation lands here
	// through parkCommitFailure, which relies on one invariant: on the commit
	// chain a code-3 NotFound is raised only by the blob store for a missing
	// file (stores/blob/file/file.go, the bare ErrNotFound it returns and the
	// NotFound wraps readSubtree and readSubtreeStructure put around it). A
	// UTXO-store miss carries code 30 (ErrTxNotFound) or a Storage or Service
	// wrap, and a missing parent block carries code 10 (ErrBlockNotFound),
	// which parkCommitFailure routes BEFORE this row. A new raiser of code 3
	// on a store must be checked against this row.
	parkDispositionFilesGone = parkDisposition{
		name:   "files_gone",
		reason: "the record names subtree files that are no longer on disk",
		blob:   parkBlobDrop,
	}

	// parkDispositionLocalUtxoFault — a parent output this block spends is
	// missing from the UTXO set. Below the checkpoint the block is on the
	// certified chain (the unified route requires the header proof), so the
	// output it spends exists in history and the thing that is wrong is this
	// node's UTXO set, not the block. Keep the blob: a re-download cannot
	// repair a UTXO set, and the block is retried by the sweep for as long as
	// it stays parked (parkStuckThreshold, parkSweepInterval), at error level
	// each time, because only an operator can clear the cause. Blame nobody.
	//
	// SV Node rejects such a block as invalid (bad-txns-inputs-missingorspent,
	// DoS 100), and can, because it has run every script and proven the chain
	// to the checkpoint itself. On this route the UTXO set is the only input
	// that was not verified against the header, so it is the one that can be
	// wrong.
	parkDispositionLocalUtxoFault = parkDisposition{
		name:   "local_utxo_fault",
		reason: "a parent output this node should already hold is missing from its UTXO set",
		blob:   parkBlobKeep,
	}

	// parkDispositionAbandoned — the parent has been genuinely absent from the
	// chain for longer than parkAbandonAfter, so this is judged orphaned rather
	// than merely slow: a losing fork at the frontier, or a block from a stale
	// or eclipsed peer parked behind a parent that belongs to a chain this node
	// will never follow. Drop the blob; nothing will ask for it again, because
	// there is no route back to a parent that was never going to arrive. Not the
	// peer's fault: it sent what we asked for, and being wrong about a fork, or
	// stale, is not misbehaviour.
	parkDispositionAbandoned = parkDisposition{
		name:   "abandoned",
		reason: "its parent never arrived",
		blob:   parkBlobDrop,
	}

	// parkDispositionParentInvalid — the parent is in the chain and marked
	// invalid, so this block can never be committed however long it is held.
	// Drop the blob, do not re-request it, and write it off so its own
	// descendants are short-circuited rather than each discovering this
	// separately. The peer is not blamed: it sent a block whose parent WE
	// rejected, which says nothing about the peer.
	parkDispositionParentInvalid = parkDisposition{
		name:   "parent_invalid",
		reason: "its parent is invalid",
		blob:   parkBlobDrop,

		markFailed: true,
	}

	// parkDispositionBlockInvalid — block validation said the block is
	// invalid, in so many words (ErrBlockInvalid in the chain). The sink bound
	// the body to the header before the record existed, so the block judged is
	// the block the peer sent: the peer hears about it, the hash is written
	// off, and the delivering association is dropped so the sync peer rotates.
	// The base disconnected the association for an invalid block too
	// (shouldDisconnectOnBlockErr, with no FSM gate); only the reject was
	// suppressed outside RUNNING, which withoutBlame still does.
	parkDispositionBlockInvalid = parkDisposition{
		name:   "block_invalid",
		reason: "block validation judged the block invalid",
		blob:   parkBlobDrop,

		blamePeer:  true,
		markFailed: true,
		dropPeer:   true,
	}

	// parkDispositionPolicyDeclined — block validation declined the block
	// under this node's own policy (ErrBlockPolicyDeclined: excessiveblocksize).
	// A statement about this node's configuration, not about the block or the
	// peer: the rest of the network may well accept it. Drop the record and
	// write the hash off so it is not asked for again, blame nobody and drop
	// nobody. The streaming gate refuses an oversize declared payload before
	// it is read, so for a streamed body this row is reached only when the
	// policy changed between download and commit; the table is still the one
	// place the answer lives.
	parkDispositionPolicyDeclined = parkDisposition{
		name:   "policy_declined",
		reason: "declined by this node's own block policy",
		blob:   parkBlobDrop,

		markFailed: true,
	}

	// parkDispositionBlockRejected — the block itself would not go into the
	// chain, for a reason nothing above classified. The peer hears about it and
	// the hash is written off, as for an invalid verdict; the peer is NOT
	// dropped, because this is the default arm, where a raw driver error or an
	// unclassified block-validation failure lands too, and rotating the sync
	// peer on one of those during IBD costs a reconnect for nothing.
	parkDispositionBlockRejected = parkDisposition{
		name:   "block_rejected",
		reason: "the block failed to store or validate",
		blob:   parkBlobDrop,

		blamePeer:  true,
		markFailed: true,
	}
)

// parkDispositionRows is every row of the table above, for the metric that
// pre-initialises one park_dispositions_total series per row. A new row goes
// here and in TestParkDispositionNames_AreUniqueAndSet, which lists the rows by
// hand and fails when one it names is missing here.
var parkDispositionRows = []parkDisposition{
	parkDispositionCommitted,
	parkDispositionRetryLater,
	parkDispositionParentGone,
	parkDispositionParentNotMinedYet,
	parkDispositionBlobUnusable,
	parkDispositionRecordCorrupt,
	parkDispositionFilesGone,
	parkDispositionLocalUtxoFault,
	parkDispositionAbandoned,
	parkDispositionParentInvalid,
	parkDispositionBlockInvalid,
	parkDispositionPolicyDeclined,
	parkDispositionBlockRejected,
}

// parkReadFailure classifies an error from reading a parked block back off
// disk.
//
// The default is deliberately the SAFE one, not the tidy one. Destroying a
// downloaded block needs positive evidence that the blob is bad; anything else,
// including an error nobody anticipated, keeps it. A blob that is permanently
// unreadable for an unrecognised reason is not held forever — it is retried by
// the sweep and dropped once the chain passes it — so guessing "keep" costs a
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
// Its default is the opposite of parkReadFailure's, and that is on purpose: a
// failure of HandleConvertedBlock that nothing above has classified is a
// judgement on the block, and the park is the only path a block commits
// through, so this is where that judgement is made.
//
// What that default costs is worth stating, because it is more than the wasted
// re-download the read path costs. parkDispositionBlockRejected sets markFailed,
// which writes recentlyFailedBlocks, and the wanted range skips that hash for
// recentlyFailedBlocksTTL, so a block wrongly judged here is not asked for again
// for that long. IsTransientLocalError matches only teranode's own error codes,
// so a store that hands back a raw driver error instead of a StorageError lands
// here rather than on retryLater (stores/utxo/sql/sql.go returns the bare error
// from db.Begin and txn.Commit). Closing that gap belongs one layer down in the
// store, not in a guess made here.
//
// The order of the arms is load-bearing. The transient arm stays above the
// local-fault rows so a Service or Storage wrap around a not-found keeps the
// blob: block validation's full route wraps every failure that is not a
// BlockInvalid or BlockCorrupt verdict in a ServiceError (Server.go
// processBlockFound), and readSubtreeStructure wraps a non-ENOENT open failure
// as NotFound around a StorageError. Corrupt is tested before
// ErrBlockInvalid because the errors package guarantees a corrupt error never
// wraps an invalid one (sanitizeCorruptParams) but not the reverse. The
// explicit ErrBlockInvalid arm is the one row that drops the peer: a verdict
// that says invalid in so many words is positive evidence about the block the
// peer sent, where the default arm is only the absence of anything better. It
// also keeps a judgement that happens to wrap a not-found from being read by the
// FilesGone row. The policy-declined arm sits after it: the two codes never
// share a chain (errors/block_policy_declined_test.go pins that), so the order
// between them does not matter, and both sit before the default so neither is
// read as a rejection.
func parkCommitFailure(err error) parkDisposition {
	switch {
	case errors.Is(err, errors.ErrBlockNotFound):
		// Before FilesGone on purpose: the blockchain store raises this code
		// with errors.ErrNotFound wrapped inside it (stores/blockchain/sql
		// GetBlockHeader), so a missing parent carries code 10 AND code 3.
		return parkDispositionParentGone

	case errors.Is(err, errors.ErrBlockParentNotMined):
		// waitForPreviousBlockMined giving up. Before the default arm, which
		// read it as a rejection and threw away a block that commits on the
		// next try (20 times on mainnet between 2026-09-28 and 2026-10-01).
		// Four places raise this code, and only this one reaches the park with
		// the code intact. UpdateTxMinedStatus's "already being processed" is
		// swallowed by block validation's setTxMined. Block validation's own
		// waitForPreviousBlocksToBeProcessed is replaced by an unwrapped block
		// error before it returns. Block assembly's waitForBlockMinedSet stays
		// inside its Reset. None of them says anything about the child, so a
		// change that let one through would still be right to keep the block;
		// but a change that wrapped a genuine rejection in this code would move
		// it here, so keep this list current when touching those call sites.
		return parkDispositionParentNotMinedYet

	case errors.IsContextError(err), errors.IsTransientLocalError(err):
		return parkDispositionRetryLater

	case errors.IsBlockCorrupt(err):
		return parkDispositionRecordCorrupt

	case errors.Is(err, errors.ErrBlockInvalid):
		return parkDispositionBlockInvalid

	case errors.Is(err, errors.ErrBlockPolicyDeclined):
		return parkDispositionPolicyDeclined

	case errors.Is(err, errors.ErrNotFound), errors.Is(err, errors.ErrBlobNotFound):
		return parkDispositionFilesGone

	case errors.Is(err, errors.ErrTxNotFound):
		return parkDispositionLocalUtxoFault

	default:
		return parkDispositionBlockRejected
	}
}

// withoutBlame returns the same row with the reject left unsent. Used while the
// node is catching blocks: we are replaying history and a peer that hands us a
// block we cannot take has not necessarily done anything wrong. dropPeer is left
// as it is on purpose: a block judged invalid by block validation is invalid
// during replay too, and the base disconnected for one in every FSM state.
func (d parkDisposition) withoutBlame() parkDisposition {
	d.blamePeer = false

	return d
}

// parkRejectWriteBound is how long rejectThenDropPeer gives the reject to reach
// the wire before the association is dropped regardless. A reject is a few
// dozen bytes, so on a peer that is reading this is microseconds; the bound is
// for one that is not. Within it the peer is still connected, still the sync
// peer if it was, and may deliver another block, which parks or commits as any
// other; none of that is on the commit goroutine's time.
const parkRejectWriteBound = 2 * time.Second

// applyParkDisposition carries out one row of the table. It is the ONLY place
// that deletes a parked blob, restores a parked entry, rejects one to a peer,
// or drops the peer that delivered one.
//
// It runs on the in-order commit goroutine (parkedBlockFailed, from the drain),
// so nothing here may wait on a peer: a reject is queued, never awaited, and
// the one case that wants the reject written before the socket is closed under
// it, blame and drop together, is handed to rejectThenDropPeer.
func (sm *SyncManager) applyParkDisposition(entry parkedBlock, d parkDisposition) {
	if prometheusLegacyNetsyncParkDispositions != nil {
		prometheusLegacyNetsyncParkDispositions.WithLabelValues(d.name).Inc()
	}

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

	if !d.blamePeer && !d.dropPeer {
		return
	}

	// A misbehaviour signal goes to the peer that actually sent the block or
	// nowhere at all. Aiming it at a fallback peer would punish an innocent one
	// for a block it never sent; losing the signal when the guilty peer has
	// already left is the cheaper mistake.
	if entry.peer == nil || !entry.peer.Connected() {
		// A record recovered from the park after a restart has no peer at all,
		// and a peer can leave between delivery and verdict.
		sm.logger.Warnf("[applyParkDisposition][%s] no connected peer to reject the block to or to drop (reject=%t drop=%t); the signal is lost", entry.hash, d.blamePeer, d.dropPeer)

		return
	}

	dropReason := fmt.Sprintf("block %s rejected (%s)", entry.hash, d.reason)

	switch {
	case d.blamePeer && d.dropPeer:
		// A disconnect straight after a queue closes the socket under the
		// reject, so the one message the peer was owed never left; waiting for
		// the write here held the commit goroutine for as long as the peer
		// chose not to read. Both wants are met off this goroutine.
		sm.rejectThenDropPeer(entry, dropReason)

	case d.blamePeer:
		entry.peer.PushRejectMsg(wire.CmdBlock, wire.RejectInvalid, "block rejected", &entry.hash, false)

	case d.dropPeer:
		// The whole association, through its primary: entry.peer is the DATA1
		// sub-peer whenever the body arrived on its own stream, and only the
		// primary is known to netsync's peer bookkeeping. DisconnectAssociation
		// resolves that itself. Closing sockets and a quit channel, nothing
		// waited on.
		entry.peer.DisconnectAssociation(dropReason)
	}
}

// rejectThenDropPeer queues the reject for entry to the peer that delivered it,
// gives the write parkRejectWriteBound to happen, and then drops the peer's
// whole association, on a goroutine of its own so the commit goroutine that
// called it carries on at once.
//
// The peer connection has no write deadline, so a remote that has stopped
// reading our socket holds a write until the socket is closed, and the idle
// timer does not close it while the remote keeps sending. The bound is what
// ends that: when it passes the reject is given up and the disconnect closes
// the socket, which fails the hung write and signals the done channel, so the
// goroutine ends within the bound either way. A peer that disconnects before
// the write is signalled too (QueueMessage on a disconnected peer, and the
// output handler's drain on quit, both send on it).
func (sm *SyncManager) rejectThenDropPeer(entry parkedBlock, dropReason string) {
	go func() {
		sent := make(chan struct{}, 1)
		entry.peer.QueueRejectMsg(wire.CmdBlock, wire.RejectInvalid, "block rejected", &entry.hash, sent)

		timer := time.NewTimer(parkRejectWriteBound)
		defer timer.Stop()

		select {
		case <-sent:
		case <-timer.C:
			sm.logger.Warnf("[applyParkDisposition][%s] the reject was not written to peer %s within %s; dropping the association without it", entry.hash, entry.peer, parkRejectWriteBound)
		}

		entry.peer.DisconnectAssociation(dropReason)
	}()
}
