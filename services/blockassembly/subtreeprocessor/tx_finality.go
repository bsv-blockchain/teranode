package subtreeprocessor

import (
	"context"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/errors"
	utxostore "github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
)

// lockTimeThreshold separates a height nLockTime from a time one (bitcoin-sv's LOCKTIME_THRESHOLD).
const lockTimeThreshold = 500_000_000

// txCandidate is the block the template is mined in: the tip it extends, its height and the
// time used for nLockTime (its own timestamp before CSV, parent median time past after).
type txCandidate struct {
	tip            chainhash.Hash
	height         uint32
	lockTimeCutoff uint32
}

// SetTxCandidate sets the block the template is mined in. Block assembly mines a transaction
// without re-checking it, so one whose nLockTime holds it back there must not be in the
// template. A transaction is final at the candidate it was validated for, but a reorg can
// lower its height or time cutoff (a side block's transaction checked against one tip, a
// mempool transaction against the tip a reorg replaced). Block assembly sets the candidate
// after every block move, and rebuilds the
// template when NotFinalTemplateTxs then finds one.
func (stp *SubtreeProcessor) SetTxCandidate(tip *chainhash.Hash, height, lockTimeCutoff uint32) {
	stp.txCandidate.Store(&txCandidate{tip: *tip, height: height, lockTimeCutoff: lockTimeCutoff})
}

// lockHeldTx is a transaction kept back until the candidate makes it final, with what it was
// queued with.
type lockHeldTx struct {
	node     subtreepkg.Node
	inpoints *subtreepkg.TxInpoints
	height   uint32
	lockTime uint32
}

// holdForLockTimeLocked keeps a transaction back to offer again when the candidate moves.
//
// lockHeldSet keeps every held transaction until admitTxLocked has decided it again: true while
// it waits in lockHeldTxs, false once OfferLockHeldTxs has queued it. A queued child that is
// dequeued before its offered parent therefore still sees the parent held, and is held too.
// The caller holds droppedMu.
func (stp *SubtreeProcessor) holdForLockTimeLocked(node subtreepkg.Node, inpoints *subtreepkg.TxInpoints, height, lockTime uint32) {
	if inList, ok := stp.lockHeldSet[node.Hash]; ok && inList {
		return
	}

	if stp.lockHeldSet == nil {
		stp.lockHeldSet = make(map[chainhash.Hash]bool)
	}

	stp.lockHeldSet[node.Hash] = true
	stp.lockHeldTxs = append(stp.lockHeldTxs, lockHeldTx{node: node, inpoints: inpoints, height: height, lockTime: lockTime})
	stp.lockHeldCount.Store(int64(len(stp.lockHeldSet)))
}

// releaseLockHeldLocked forgets that hash is held: admitTxLocked decided it, or a block mined
// it. The caller holds droppedMu.
func (stp *SubtreeProcessor) releaseLockHeldLocked(hash chainhash.Hash) {
	if _, ok := stp.lockHeldSet[hash]; !ok {
		return
	}

	delete(stp.lockHeldSet, hash)
	stp.lockHeldCount.Store(int64(len(stp.lockHeldSet)))
}

// clearLockHeldLocked forgets every held transaction. The caller holds droppedMu.
func (stp *SubtreeProcessor) clearLockHeldLocked() {
	stp.lockHeldTxs = nil
	stp.lockHeldSet = nil
	stp.lockHeldCount.Store(0)
}

// lockHeldTxCount returns how many transactions are held for their lock time, offered ones
// included.
func (stp *SubtreeProcessor) lockHeldTxCount() int {
	return int(stp.lockHeldCount.Load())
}

// OfferLockHeldTxs queues the held transactions again, in the order they were held, so parents
// come before their children, for admitTxLocked to decide against the candidate. Block
// assembly calls it after it sets the candidate for a new block.
//
// The UTXO store decides first which of them are still unmined mempool transactions: a held
// transaction can lose a conflict to a block, which marks it and every descendant conflicting,
// and offering it again would put a double spend in the template. A conflicting or missing one
// is dropped, so its held descendants are dropped too; a mined one is forgotten. A store error
// keeps every held transaction held and is returned.
func (stp *SubtreeProcessor) OfferLockHeldTxs(ctx context.Context) error {
	if stp.lockHeldCount.Load() == 0 {
		return nil
	}

	stp.droppedMu.Lock()
	held := append([]lockHeldTx(nil), stp.lockHeldTxs...)
	stp.droppedMu.Unlock()

	if len(held) == 0 {
		return nil
	}

	const (
		offer = iota
		drop
		forget
	)

	verdicts := make([]int, len(held))

	// One batched read for the whole held set.
	unresolved := make([]*utxostore.UnresolvedMetaData, len(held))
	for i, h := range held {
		unresolved[i] = &utxostore.UnresolvedMetaData{Hash: h.node.Hash, Idx: i}
	}

	if err := stp.utxoStore.BatchDecorate(ctx, unresolved, fields.Conflicting, fields.UnminedSince); err != nil {
		return errors.NewProcessingError("[OfferLockHeldTxs] error reading the held transactions", err)
	}

	for i, item := range unresolved {
		switch {
		case errors.Is(item.Err, errors.ErrTxNotFound):
			verdicts[i] = drop
		case item.Err != nil:
			return errors.NewProcessingError("[OfferLockHeldTxs] error reading held transaction %s", item.Hash.String(), item.Err)
		case item.Data == nil:
			return errors.NewProcessingError("[OfferLockHeldTxs] no data read for held transaction %s", item.Hash.String())
		case item.Data.Conflicting:
			verdicts[i] = drop
		case item.Data.UnminedSince == 0:
			verdicts[i] = forget
		}
	}

	nodes := make([]subtreepkg.Node, 0, len(held))
	inpoints := make([]*subtreepkg.TxInpoints, 0, len(held))
	heights := make([]uint32, 0, len(held))
	lockTimes := make([]uint32, 0, len(held))

	stp.droppedMu.Lock()

	offered := make(map[chainhash.Hash]struct{}, len(held))

	for i, h := range held {
		hash := h.node.Hash
		offered[hash] = struct{}{}

		// Forgotten since the read: a block mined it.
		if _, ok := stp.lockHeldSet[hash]; !ok {
			continue
		}

		switch verdicts[i] {
		case drop:
			stp.releaseLockHeldLocked(hash)
			stp.recordDroppedLocked(hash)
		case forget:
			stp.releaseLockHeldLocked(hash)
		default:
			stp.lockHeldSet[hash] = false
			nodes = append(nodes, h.node)
			inpoints = append(inpoints, h.inpoints)
			heights = append(heights, h.height)
			lockTimes = append(lockTimes, h.lockTime)
		}
	}

	// Keep what the processor held since the read.
	kept := stp.lockHeldTxs[:0]

	for _, h := range stp.lockHeldTxs {
		if _, ok := offered[h.node.Hash]; !ok {
			kept = append(kept, h)
		}
	}

	clear(stp.lockHeldTxs[len(kept):])
	stp.lockHeldTxs = kept

	stp.droppedMu.Unlock()

	if len(nodes) > 0 {
		stp.queue.enqueueTaggedBatch(nodes, inpoints, heights, lockTimes)
	}

	return nil
}

// freshTxCandidate returns the candidate when it describes the block after the processor's
// current block, nil otherwise. Block assembly sets it after the processor moves, and until
// then the previous block's median time past would wrongly hold back transactions validated
// against the new one.
func (stp *SubtreeProcessor) freshTxCandidate() *txCandidate {
	c := stp.txCandidate.Load()
	if c == nil {
		return nil
	}

	current := stp.currentBlockHeader.Load()
	if current == nil || !current.Hash().IsEqual(&c.tip) {
		return nil
	}

	return c
}

// finalAtCandidate reports whether a transaction with lockTime, the nLockTime of a transaction
// with a non-final input sequence, is final in block c (util.ValidLockTime). The cutoff is
// the block's own timestamp before CSV and its parent's median time past afterward.
func (stp *SubtreeProcessor) finalAtCandidate(lockTime uint32, c *txCandidate) bool {
	if lockTime == 0 {
		return true
	}

	if lockTime < lockTimeThreshold {
		return c.height > lockTime
	}

	return c.lockTimeCutoff > lockTime
}

// trackLockTimeLocked records an admitted transaction whose nLockTime can hold it back, so a
// later reorg can find it. The caller holds droppedMu.
func (stp *SubtreeProcessor) trackLockTimeLocked(hash chainhash.Hash, lockTime uint32) {
	if lockTime == 0 {
		return
	}

	if stp.lockedTxs == nil {
		stp.lockedTxs = make(map[chainhash.Hash]uint32)
	}

	if _, ok := stp.lockedTxs[hash]; !ok {
		stp.lockedCount.Add(1)
	}

	stp.lockedTxs[hash] = lockTime
}

// NotFinalTemplateTxs returns how many tracked transactions in the template are not final at
// the candidate. Block assembly reads it after every block move; it is 0 while the candidate is
// not set for the current block. A tracked transaction not in the template is skipped, not
// forgotten: the dequeue loop tracks one before it places it, and may be between the two.
// pruneLockedTxs forgets them on the processor's goroutine.
func (stp *SubtreeProcessor) NotFinalTemplateTxs() int {
	if stp.lockedCount.Load() == 0 {
		return 0
	}

	c := stp.freshTxCandidate()
	if c == nil {
		return 0
	}

	stp.droppedMu.Lock()
	defer stp.droppedMu.Unlock()

	notFinal := 0

	for hash, lockTime := range stp.lockedTxs {
		// Block assembly calls this between block moves, which it hands to the processor and
		// waits for, so the map is not swapped while it is read.
		if _, ok := stp.currentTxMap.Get(hash); !ok {
			continue
		}

		if !stp.finalAtCandidate(lockTime, c) {
			notFinal++
		}
	}

	return notFinal
}

// pruneLockedTxs forgets the tracked lock times of transactions no longer in the template: mined
// in this node's own block, which has no mined set for forgetMinedDroppedTxs, or removed. It runs
// on the processor's goroutine during a block move, when the dequeue loop is not placing
// transactions it has already tracked.
func (stp *SubtreeProcessor) pruneLockedTxs() {
	if stp.lockedCount.Load() == 0 {
		return
	}

	stp.droppedMu.Lock()
	defer stp.droppedMu.Unlock()

	for hash := range stp.lockedTxs {
		if _, ok := stp.currentTxMap.Get(hash); !ok {
			delete(stp.lockedTxs, hash)
			stp.lockedCount.Add(-1)
		}
	}
}
