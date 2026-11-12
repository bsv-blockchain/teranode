package validator

import (
	"context"
	"sync"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo/meta"
)

const (
	// inBlockShedRecoveryDelay is how old an in-block shed must be before the recovery check reads
	// it. It leaves a block's setMined time to clear the lock, which it does for every block that
	// validates, so the check only acts on records that no block unlocked.
	inBlockShedRecoveryDelay = 5 * time.Minute

	// inBlockShedTrackLimit bounds the ids tracked for one recovery check. Past it the check
	// resets block assembly without reading the store, because it can no longer name every record.
	inBlockShedTrackLimit = 10000

	inBlockShedRecoveryTimeout = 2 * time.Minute
)

// blockAssemblyResetter is the part of the block assembly client that restores the records an
// in-block shed left Locked outside the template. A reset runs the unmined reload, which reads
// Locked unmined records, adds them to the template and unlocks them.
type blockAssemblyResetter interface {
	ResetBlockAssembly(ctx context.Context) error
}

// shedRecovery tracks the transactions an in-block shed kept Locked outside the template, each
// with the time of its shed, and the one timer that checks them. A transaction is checked only
// once it is a full delay old, because a block's setMined needs that long to unlock it.
type shedRecovery struct {
	mu     sync.Mutex
	hashes map[chainhash.Hash]time.Time

	// overflowAt is the time of the first shed that did not fit under inBlockShedTrackLimit, and
	// zero when none overflowed. The check cannot name those records, so it resets block assembly
	// without reading the store once the first of them is a full delay old.
	overflowAt time.Time
	timer      *time.Timer
}

// trackInBlockShed records a transaction an in-block shed left Locked outside the template and
// arms the recovery check if none is pending. Sheds that arrive while a check is pending join it,
// so a sustained shed causes one check per delay, not one per transaction.
func (v *Validator) trackInBlockShed(hash *chainhash.Hash) {
	r := &v.shedRecovery

	r.mu.Lock()
	defer r.mu.Unlock()

	if r.hashes == nil {
		r.hashes = make(map[chainhash.Hash]time.Time)
	}

	now := time.Now()

	if _, tracked := r.hashes[*hash]; tracked {
		return
	}

	if len(r.hashes) < inBlockShedTrackLimit {
		r.hashes[*hash] = now
	} else if r.overflowAt.IsZero() {
		r.overflowAt = now
	}

	v.armInBlockShedRecoveryLocked(0)
}

func (v *Validator) shedRecoveryDelay() time.Duration {
	if v.inBlockShedRecoveryDelay > 0 {
		return v.inBlockShedRecoveryDelay
	}

	return inBlockShedRecoveryDelay
}

// armInBlockShedRecoveryLocked arms the check for the moment the oldest tracked shed is a full
// delay old, and never sooner than minWait from now, so a failed reset is not retried in a loop.
// It does nothing when a check is already pending or nothing is tracked.
func (v *Validator) armInBlockShedRecoveryLocked(minWait time.Duration) {
	r := &v.shedRecovery

	if r.timer != nil {
		return
	}

	oldest := r.overflowAt

	for _, shedAt := range r.hashes {
		if oldest.IsZero() || shedAt.Before(oldest) {
			oldest = shedAt
		}
	}

	if oldest.IsZero() {
		return
	}

	wait := max(time.Until(oldest.Add(v.shedRecoveryDelay())), minWait)

	r.timer = time.AfterFunc(wait, v.recoverInBlockSheds)
}

// recoverInBlockSheds resets block assembly when a transaction an in-block shed left Locked, and
// that is a full delay old, is still Locked and unmined, which means no block unlocked it and the
// template is missing it. Younger transactions stay tracked and re-arm the check for the oldest of
// them. The ids it checked leave the tracker only once the check is settled, so a failed reset
// keeps them and arms the check again after a delay.
func (v *Validator) recoverInBlockSheds() {
	r := &v.shedRecovery
	cutoff := time.Now().Add(-v.shedRecoveryDelay())

	r.mu.Lock()
	due := make([]chainhash.Hash, 0, len(r.hashes))

	for hash, shedAt := range r.hashes {
		if !shedAt.After(cutoff) {
			due = append(due, hash)
		}
	}

	overflow := !r.overflowAt.IsZero() && !r.overflowAt.After(cutoff)
	r.timer = nil
	r.mu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), inBlockShedRecoveryTimeout)
	defer cancel()

	settled := true
	if overflow || v.anyInBlockShedStillLocked(ctx, due) {
		settled = v.resetForInBlockSheds(ctx, len(due), overflow)
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	if !settled {
		v.armInBlockShedRecoveryLocked(v.shedRecoveryDelay())

		return
	}

	for _, hash := range due {
		delete(r.hashes, hash)
	}

	if overflow {
		r.overflowAt = time.Time{}
	}

	v.armInBlockShedRecoveryLocked(0)
}

// anyInBlockShedStillLocked reports whether any of the ids is still Locked and unmined. A read
// that fails counts as still locked, so an unreadable store leads to a reset and not to silence.
func (v *Validator) anyInBlockShedStillLocked(ctx context.Context, hashes []chainhash.Hash) bool {
	for i := range hashes {
		data := &meta.Data{}

		err := v.utxoStore.GetMeta(ctx, &hashes[i], data)
		if err != nil && !errors.Is(err, errors.ErrTxNotFound) {
			return true
		}

		if err == nil && data.Locked && len(data.BlockIDs) == 0 && !data.Conflicting {
			return true
		}
	}

	return false
}

// resetForInBlockSheds asks block assembly to reset, which runs the unmined reload. It reports
// false only when the reset was requested and failed. A client that cannot reset is logged and
// reported as settled, because retrying would not change that.
func (v *Validator) resetForInBlockSheds(ctx context.Context, tracked int, overflow bool) bool {
	resetter, ok := v.blockAssembler.(blockAssemblyResetter)
	if !ok {
		v.logger.Warnf("[Validator] %d transactions an in-block shed kept Locked are still outside the block assembly template, and the block assembly client cannot reset; they recover at the next block assembly reset or restart", tracked)

		return true
	}

	if err := resetter.ResetBlockAssembly(ctx); err != nil {
		v.logger.Errorf("[Validator] block assembly reset to recover transactions an in-block shed kept Locked failed, retrying later: %v", err)

		return false
	}

	prometheusValidatorShedInBlockRecoveryResets.Inc()

	v.logger.Infof("[Validator] reset block assembly to recover transactions an in-block shed kept Locked outside the template (%d tracked, overflow %t)", tracked, overflow)

	return true
}
