package blockpersister

import (
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
)

// failurePacer paces retries for failures that will be retried against the same
// block. The loop's flat persistSleep was attached uniformly to every failure,
// which is wrong in both directions: a failed tip query recovers faster when
// retried promptly, while a block whose source data has been pruned (§36 of the
// reconciliation notes) can never succeed without an operator, so a flat
// ten-second retry just spreads identical error logs out forever.
//
// Repeat failures on the same block escalate: first failure waits the shipped
// interval - still right for a store that hiccups - then doubles per
// consecutive failure up to pacerMaxSleep. The first fully persisted block
// resets it. Escalation is deliberately bounded and short-lived rather than
// exponential-forever: an operator who acts gets recovery within one interval,
// and one who does not gets a steady, low-log-rate signal.
type failurePacer struct {
	base  time.Duration
	max   time.Duration
	fails int
	// last is the block the current failure run belongs to. A failure on a
	// different block starts a new run: the escalated wait earned by an earlier
	// block (for example one that was then invalidated) must not carry over.
	last *chainhash.Hash
}

// pacerMaxSleep bounds escalated waiting. Past this point the loop is polling
// slowly enough that a transient cause has long since cleared on its own, and
// waiting longer only delays recovery once the operator has intervened.
const pacerMaxSleep = 5 * time.Minute

// announceAtFailures is when the pacer starts telling the operator the block is
// not going to file itself: by the third consecutive failure at the shipped
// ten-second base the loop has spent ~40 seconds and two identical errors on
// the same block, which distinguishes "store hiccup" from "pruned source data"
// with no new setting.
const announceAtFailures = 3

// next records one more consecutive failure of block and returns how long to
// wait. A block other than the one that failed last starts a fresh run.
func (p *failurePacer) next(block *chainhash.Hash) time.Duration {
	if p.last == nil || !p.last.IsEqual(block) {
		p.fails = 0
		hash := *block
		p.last = &hash
	}

	p.fails++

	d := p.base
	// fails==1 -> base; each further failure doubles, bounded by max.
	for i := 1; i < p.fails && d < p.max; i++ {
		d *= 2
	}
	if d > p.max {
		d = p.max
	}

	return d
}

// announce reports whether the current failure count has just crossed the
// point where the failure stops looking transient. Exactly-once semantics fall
// out of comparing against the constant: further escalations do not re-announce.
func (p *failurePacer) announce() bool {
	return p.fails == announceAtFailures
}

// consecutive returns the current consecutive-failure count.
func (p *failurePacer) consecutive() int {
	return p.fails
}

// reset clears the counter after a block files successfully.
func (p *failurePacer) reset() {
	p.fails = 0
	p.last = nil
}
