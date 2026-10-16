package netsync

import (
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-wire"
)

const (
	// reAskFreeAttempts is how many re-asks of one dropped block go out at once. A drop that a
	// re-download repairs (a blob this node wrote badly, files expired under a record) needs
	// one; the second covers a peer that went away before the first was answered.
	reAskFreeAttempts = 2

	// reAskBackoffBase is the wait before the first paced re-ask of a block. Each later one
	// waits twice the one before, up to reAskBackoffMax.
	reAskBackoffBase = 30 * time.Second

	// reAskBackoffMax is the longest wait between two re-asks of one block, so a block whose
	// drop cause has cleared is not left for long: the chain above the last checkpoint waits
	// on it, and nothing else asks for it there (reAskDroppedBlock).
	reAskBackoffMax = 10 * time.Minute

	// maxTrackedReAsks bounds the pacing records, as blockFailureBackoffMaxTracked bounded
	// base's per-block failure backoff. At the bound a new block's re-ask is not sent; the
	// block waits for its next announcement, the case the re-ask was added for.
	maxTrackedReAsks = 1024

	// reAskForgetAfter is how long a pacing record with no re-ask waiting outlives its last
	// re-ask. A block that commits by a route other than the park's committed row never has its
	// record forgotten there, and without this such records would fill maxTrackedReAsks.
	reAskForgetAfter = 6 * reAskBackoffMax
)

// droppedReAsk is the pacing record of one dropped block's re-asks.
type droppedReAsk struct {
	// entry is the block as it was dropped, for the peer to ask when a paced re-ask is due.
	entry parkedBlock
	// asked is how many re-asks were admitted, sent or deferred.
	asked int
	// due is when the deferred re-ask goes out, zero when none is waiting.
	due time.Time
	// last is when the last re-ask was admitted.
	last time.Time
}

// reAskBackoff is the wait before a block's asked-th re-ask, counting from 1. Zero for the free
// attempts.
func reAskBackoff(asked int) time.Duration {
	if asked <= reAskFreeAttempts {
		return 0
	}

	wait := reAskBackoffBase

	for i := reAskFreeAttempts + 1; i < asked; i++ {
		wait *= 2
		if wait >= reAskBackoffMax {
			return reAskBackoffMax
		}
	}

	return wait
}

// admitDroppedReAsk counts one re-ask of entry's block and reports whether it may be sent now.
// Past the free attempts it is deferred to the backoff and sent by sendDueReAsks; a re-ask
// already waiting is left as it is, so a run of drops cannot pull it forward.
func (sm *SyncManager) admitDroppedReAsk(entry parkedBlock, now time.Time) bool {
	sm.reAsksMu.Lock()
	defer sm.reAsksMu.Unlock()

	rec, ok := sm.reAsks[entry.hash]
	if !ok {
		if len(sm.reAsks) >= maxTrackedReAsks {
			sm.logger.Warnf("[applyParkDisposition][%s] %d dropped blocks already waiting to be asked for again; not asking for this one, it waits for its next announcement", entry.hash, maxTrackedReAsks)

			return false
		}

		if sm.reAsks == nil {
			sm.reAsks = make(map[chainhash.Hash]*droppedReAsk)
		}

		rec = &droppedReAsk{}
		sm.reAsks[entry.hash] = rec
	}

	rec.entry = entry

	if !rec.due.IsZero() {
		return false
	}

	rec.asked++
	rec.last = now

	wait := reAskBackoff(rec.asked)
	if wait == 0 {
		return true
	}

	rec.due = now.Add(wait)

	sm.logger.Warnf("[applyParkDisposition][%s] the block was dropped and asked for again %d times; asking again in %s", entry.hash, rec.asked-1, wait)

	return false
}

// forgetDroppedReAsk ends the pacing of hash's re-asks: the block is in the chain or written
// off.
func (sm *SyncManager) forgetDroppedReAsk(hash chainhash.Hash) {
	sm.reAsksMu.Lock()
	defer sm.reAsksMu.Unlock()

	delete(sm.reAsks, hash)
}

// sendDueReAsks sends each paced re-ask whose wait has passed, and forgets a record idle for
// reAskForgetAfter. It runs on the park sweep's ticker. A block the chain now holds, or one the headers-first scheduler owns again (its
// wanted-range pass names a dropped block), is forgotten instead.
func (sm *SyncManager) sendDueReAsks(now time.Time) {
	sm.reAsksMu.Lock()

	var due []parkedBlock

	for hash, rec := range sm.reAsks {
		switch {
		case !rec.due.IsZero() && !now.Before(rec.due):
			rec.due = time.Time{}
			due = append(due, rec.entry)

		case rec.due.IsZero() && now.Sub(rec.last) >= reAskForgetAfter:
			delete(sm.reAsks, hash)
		}
	}

	sm.reAsksMu.Unlock()

	for _, entry := range due {
		if sm.headersFirstMode.Load() {
			sm.forgetDroppedReAsk(entry.hash)

			continue
		}

		if have, err := sm.haveInventory(wire.NewInvVect(wire.InvTypeBlock, &entry.hash)); err == nil && have {
			// In the chain, or already parked, arriving or converting: nothing to ask. A block
			// on its way keeps its record, so a later drop still waits its backoff.
			if !sm.blockHeldLocally(entry.hash) {
				sm.forgetDroppedReAsk(entry.hash)
			}

			continue
		}

		sm.logger.Infof("[applyParkDisposition][%s] the wait before asking again for the dropped block has passed", entry.hash)
		sm.askDroppedBlockAgain(entry)
	}
}
