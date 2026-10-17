package blockassembly

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
)

// defaultUndecidedExcludeAfter is how long an unmined transaction's input check can go unanswered
// before a load leaves the transaction out instead of failing for it.
const defaultUndecidedExcludeAfter = 10 * time.Minute

// defaultUnminedReloadRetryInterval is how long after a failed reset reload the heartbeat queues
// another reset.
const defaultUnminedReloadRetryInterval = 30 * time.Second

// unminedInputCheckAttempts and unminedInputCheckBackoff bound the retry of one transaction's
// input check inside a load. One read that times out is then not the whole load's failure.
const (
	unminedInputCheckAttempts = 3
	unminedInputCheckBackoff  = 250 * time.Millisecond
)

// recordUnread marks an input check that failed on the transaction's own record, before any
// parent was asked about. It is the shape of a store that is not answering at all, as opposed to
// one that answers for the transaction and fails on a parent.
type recordUnread struct{ err error }

func (r recordUnread) Error() string { return r.err.Error() }

func (r recordUnread) Unwrap() error { return r.err }

func isRecordUnread(err error) bool {
	_, ok := err.(recordUnread) //nolint:errorlint // returned unwrapped by checkUnminedTxInputs

	return ok
}

// persistentUndecided is a transaction a load left out because its input check has gone
// unanswered for longer than the bound.
type persistentUndecided struct {
	hash  chainhash.Hash
	since time.Time
	err   error
}

// undecidedLoad is one load's record of the input checks the store did not answer.
//
// An input check the store did not answer is undecided, not invalid, so the transaction is never
// counted invalid for it. The load fails instead, and a later load decides it. A check that keeps
// failing must not fail every load for ever, so a transaction undecided for longer than bound,
// counted from the first load that could not decide it, is left out and the load goes on.
//
// That exclusion needs evidence that the store is answering. A load in which the store answered
// for no transaction at all is an outage, not a bad record, and leaving out every transaction
// undecided past the bound would empty the candidate. Such a load fails, whatever the clock says.
type undecidedLoad struct {
	before map[chainhash.Hash]time.Time // read only
	bound  time.Duration
	now    time.Time

	// failing is set once the load is going to fail. Workers stop checking then.
	failing atomic.Bool
	// answered counts the transactions the store answered for in this load: decided, permanent,
	// or undecided on a parent after reading the transaction's own record.
	answered atomic.Int64

	mu         sync.Mutex
	still      map[chainhash.Hash]time.Time
	decidedOld []chainhash.Hash
	persistent []persistentUndecided
	err        error
}

func newUndecidedLoad(before map[chainhash.Hash]time.Time, bound time.Duration, now time.Time) *undecidedLoad {
	return &undecidedLoad{before: before, bound: bound, now: now, still: make(map[chainhash.Hash]time.Time)}
}

// decided records a transaction whose input check the store answered.
func (u *undecidedLoad) decided(hash chainhash.Hash) {
	u.answered.Add(1)

	if _, ok := u.before[hash]; ok {
		u.mu.Lock()
		u.decidedOld = append(u.decidedOld, hash)
		u.mu.Unlock()
	}
}

// undecided records a transaction whose input check the store did not answer, even on a retry.
// It reports true when the transaction is left out and the load goes on, false when the load
// fails for it.
func (u *undecidedLoad) undecided(hash chainhash.Hash, err error) bool {
	since, seen := u.before[hash]
	if !seen {
		since = u.now
	}

	if !isRecordUnread(err) {
		u.answered.Add(1)
	}

	u.mu.Lock()
	defer u.mu.Unlock()

	u.still[hash] = since

	if seen && u.now.Sub(since) >= u.bound {
		u.persistent = append(u.persistent, persistentUndecided{hash: hash, since: since, err: err})
		return true
	}

	if u.err == nil {
		cause := err
		if r, ok := err.(recordUnread); ok { //nolint:errorlint // returned unwrapped by checkUnminedTxInputs
			cause = r.err
		}

		u.err = errors.NewProcessingError("[loadUnminedTransactions][%s] could not validate inputs", hash.String(), cause)
	}

	u.failing.Store(true)

	return false
}

// finish is called once the workers have stopped. It returns the error that fails the load, or
// nil.
func (u *undecidedLoad) finish() error {
	u.mu.Lock()
	defer u.mu.Unlock()

	if u.err == nil && len(u.persistent) > 0 && u.answered.Load() == 0 {
		u.err = errors.NewProcessingError("[loadUnminedTransactions] the store answered no input check in this load, so the %d transactions undecided for longer than %s are not left out; the load fails",
			len(u.persistent), u.bound)
	}

	return u.err
}

// next is the undecided list the next load starts from. A load that ran to the end checked every
// transaction, so its own list replaces the old one. A failed load stopped checking part way, so
// the old list is kept, less what this load decided, and this load's additions are put in.
func (u *undecidedLoad) next() map[chainhash.Hash]time.Time {
	u.mu.Lock()
	defer u.mu.Unlock()

	if u.err == nil {
		return u.still
	}

	merged := make(map[chainhash.Hash]time.Time, len(u.before)+len(u.still))

	for h, t := range u.before {
		merged[h] = t
	}

	for _, h := range u.decidedOld {
		delete(merged, h)
	}

	for h, t := range u.still {
		merged[h] = t
	}

	return merged
}

func (b *BlockAssembler) undecidedExcludeAfterOrDefault() time.Duration {
	if b.undecidedExcludeAfter > 0 {
		return b.undecidedExcludeAfter
	}

	return defaultUndecidedExcludeAfter
}

// checkUnminedTxInputsRetrying is checkUnminedTxInputs, asked again with a short backoff when the
// store did not answer. It stops early when the check is permanent, the load is already failing,
// or the context is done.
func (b *BlockAssembler) checkUnminedTxInputsRetrying(ctx context.Context, txHash chainhash.Hash,
	bestBlockIDsMap map[uint32]bool, failing *atomic.Bool) (bool, bool, error) {
	backoff := unminedInputCheckBackoff

	for attempt := 1; ; attempt++ {
		valid, conflicting, err := b.checkUnminedTxInputs(ctx, txHash, bestBlockIDsMap)
		if err == nil || isPermanentInputCheck(err) || attempt >= unminedInputCheckAttempts || failing.Load() {
			return valid, conflicting, err
		}

		select {
		case <-ctx.Done():
			return valid, conflicting, err
		case <-time.After(backoff):
		}

		backoff *= 2
	}
}

// noteFailedUnminedReload records that the reload inside a reset failed, for
// retryFailedUnminedReload. A retry still pending from a reset that validated inputs keeps
// asking for validation, so a later failed reset that did not validate cannot drop it.
func (b *BlockAssembler) noteFailedUnminedReload(validateInputs bool) {
	if b.unminedReloadPending.Load() && b.unminedReloadValidate.Load() {
		validateInputs = true
	}

	b.unminedReloadValidate.Store(validateInputs)
	b.unminedReloadFailedAt.Store(time.Now().UnixNano())
	b.unminedReloadPending.Store(true)
}

// clearPendingUnminedReload drops the retry a failed reload left, once a later reset has reloaded
// the mempool. A reload that did not validate inputs leaves a retry that asked for validation in
// place, so the validation the failed reset meant to do still happens.
func (b *BlockAssembler) clearPendingUnminedReload(validatedInputs bool) {
	if validatedInputs || !b.unminedReloadValidate.Load() {
		b.unminedReloadPending.Store(false)
	}
}

// retryFailedUnminedReload queues a reset when the reload of an earlier one failed and the retry
// interval has passed since. Called from the heartbeat tick. It reports whether it queued one.
//
// The reset that failed had already cleared the subtrees, so until a reload succeeds the mempool
// is in no template, and nothing else would start one: every caller of reset only logs the error.
// The reset it queues validates inputs when the failed one did. A reset that fails again sets the
// flag again, so the retry goes on at the interval for as long as the store keeps failing.
func (b *BlockAssembler) retryFailedUnminedReload() bool {
	if !b.unminedReloadPending.Load() {
		return false
	}

	interval := b.unminedReloadRetryInterval
	if interval <= 0 {
		interval = defaultUnminedReloadRetryInterval
	}

	if time.Since(time.Unix(0, b.unminedReloadFailedAt.Load())) < interval {
		return false
	}

	b.unminedReloadPending.Store(false)

	validate := b.unminedReloadValidate.Load()

	b.logger.Warnf("[BlockAssembler] the unmined reload of an earlier reset failed, so the mempool is in no template; resetting again (validate inputs %t)", validate)
	b.resetWithOptions(false, validate)

	return true
}
