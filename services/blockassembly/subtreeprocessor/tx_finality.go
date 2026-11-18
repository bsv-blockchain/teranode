package subtreeprocessor

import (
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
)

// lockTimeThreshold separates a height nLockTime from a time one (bitcoin-sv's LOCKTIME_THRESHOLD).
const lockTimeThreshold = 500_000_000

// txCandidate is the block the template is mined in: the tip it extends, its height and the
// tip's median time past.
type txCandidate struct {
	tip            chainhash.Hash
	height         uint32
	medianTimePast uint32
}

// SetTxCandidate sets the block the template is mined in. Block assembly mines a transaction
// without re-checking it, so one whose nLockTime holds it back there must not be in the
// template. A transaction is final at the candidate it was validated for, and a forward block
// only raises the height and the median time past, but a reorg can lower either (issue 4940:
// a side block's transaction checked against one tip, a mempool transaction against the tip a
// reorg replaced). Block assembly sets the candidate after every block move and, after a reorg,
// rebuilds the template when NotFinalTemplateTxs finds one.
func (stp *SubtreeProcessor) SetTxCandidate(tip *chainhash.Hash, height, medianTimePast uint32) {
	stp.txCandidate.Store(&txCandidate{tip: *tip, height: height, medianTimePast: medianTimePast})
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
// with a non-final input sequence, is final in block c (util.ValidLockTime). Below CSV a time
// lock is measured against the block's own timestamp, which is about now and which a reorg does
// not lower, so only a height lock is checked there.
func (stp *SubtreeProcessor) finalAtCandidate(lockTime uint32, c *txCandidate) bool {
	if lockTime == 0 {
		return true
	}

	if lockTime < lockTimeThreshold {
		return c.height > lockTime
	}

	if c.height < stp.settings.ChainCfgParams.CSVHeight {
		return true
	}

	return c.medianTimePast > lockTime
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

// NotFinalTemplateTxs returns how many tracked transactions still in the template are not
// final at the candidate. Block assembly reads it after a reorg; it is 0 while the candidate
// is not set for the current block.
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
		if stp.finalAtCandidate(lockTime, c) {
			continue
		}

		// Block assembly calls this between block moves, which it hands to the processor and
		// waits for, so the map is not swapped while it is read.
		if _, ok := stp.currentTxMap.Get(hash); ok {
			notFinal++
		}
	}

	return notFinal
}
