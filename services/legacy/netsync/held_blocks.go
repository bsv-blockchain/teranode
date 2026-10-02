package netsync

import (
	"context"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
)

// holdsBlock reports whether this node already has everything needed to
// commit the block, under either of the two shapes a receive path writes.
//
// This is the authority for "what do I hold", and it is the filesystem rather
// than any in-memory index. The park's own Has() reads its entry map, which is
// empty on a restarting node until recovery has run, and which says nothing
// about a block the park has already committed and deleted. A map that is the
// only record of a downloaded block makes every bound on that map a decision to
// throw a block away, and every such decision needs a mechanism somewhere else
// to put it back. There is nothing to put back here.
//
// For the pipeline shape, the record's existence is not enough. The record is
// a few hundred bytes naming the block's subtree files, and it carries no
// delete-at-height of its own, while the subtree files it names do
// (subtree_writer.go) — so a record can outlive the files it points at, or
// the startup recovery scan can have deliberately left it un-adopted because
// it could not read it cleanly or ran out of its budget. Either way, a record
// with files missing is not a block this node can ever commit, and answering
// true for it means the download pass skips this height on every pass while
// nothing ever adopts it: the chain stops at that height for the life of the
// process. hasCompleteRecord is the one piece of code that decides "complete"
// for a converted record; Recover uses the exact same call when deciding
// whether to adopt a record left over from a previous run, so the two places
// cannot disagree about what "complete" means.
//
// The streaming shape has no such gap: it writes one file whose name is the
// block's hash, and only after the block's merkle root has been checked
// against its header, deleting anything already written if that check fails.
// So that file's existence alone proves a complete body arrived and verified.
//
// Every write goes to a temporary sibling, is flushed, and is renamed into
// place, so a partial file is never published and a torn read is not possible.
//
// A store error answers false. That is the safe direction: we re-ask for a block
// we may already have, which costs bandwidth, rather than skip one we do not,
// which stops the chain.
func (sm *SyncManager) holdsBlock(ctx context.Context, hash chainhash.Hash) bool {
	if sm.blockPark == nil || sm.blockPark.store == nil {
		return false
	}

	readCtx, cancel := sm.blockPark.storeCtx(ctx)
	defer cancel()

	// A converted record is the only shape a held block takes; the whole raw blocks an older
	// build wrote are deleted on restart.
	record, err := sm.blockPark.ReadConverted(readCtx, hash)
	if err != nil {
		return false
	}

	return sm.blockPark.hasCompleteRecord(readCtx, hash, record, sm.subtreeStore)
}
