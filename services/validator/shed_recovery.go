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
	// inBlockShedRecoveryDelay is how long after the first in-block shed the recovery check runs.
	// It leaves a block's setMined time to clear the locks, which it does for every block that
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

// shedRecovery tracks the transactions an in-block shed kept Locked outside the template, and
// the one timer that checks them.
type shedRecovery struct {
	mu       sync.Mutex
	hashes   map[chainhash.Hash]struct{}
	overflow bool
	timer    *time.Timer
}

// trackInBlockShed records a transaction an in-block shed left Locked outside the template and
// arms the recovery check if none is pending. Sheds that arrive while a check is pending join
// it, so a sustained shed causes one check per delay, not one per transaction.
func (v *Validator) trackInBlockShed(hash *chainhash.Hash) {
	r := &v.shedRecovery

	r.mu.Lock()
	defer r.mu.Unlock()

	if r.hashes == nil {
		r.hashes = make(map[chainhash.Hash]struct{})
	}

	if len(r.hashes) < inBlockShedTrackLimit {
		r.hashes[*hash] = struct{}{}
	} else {
		r.overflow = true
	}

	v.armInBlockShedRecoveryLocked()
}

func (v *Validator) armInBlockShedRecoveryLocked() {
	r := &v.shedRecovery

	if r.timer != nil {
		return
	}

	delay := v.inBlockShedRecoveryDelay
	if delay <= 0 {
		delay = inBlockShedRecoveryDelay
	}

	r.timer = time.AfterFunc(delay, v.recoverInBlockSheds)
}

// recoverInBlockSheds resets block assembly when a transaction an in-block shed left Locked is
// still Locked and unmined, which means no block unlocked it and the template is missing it. The
// ids it checked leave the tracker only once the check is settled, so a failed reset keeps them
// and arms the check again.
func (v *Validator) recoverInBlockSheds() {
	r := &v.shedRecovery

	r.mu.Lock()
	hashes := make([]chainhash.Hash, 0, len(r.hashes))

	for hash := range r.hashes {
		hashes = append(hashes, hash)
	}

	overflow := r.overflow
	r.timer = nil
	r.mu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), inBlockShedRecoveryTimeout)
	defer cancel()

	if (overflow || v.anyInBlockShedStillLocked(ctx, hashes)) && !v.resetForInBlockSheds(ctx, len(hashes), overflow) {
		r.mu.Lock()
		v.armInBlockShedRecoveryLocked()
		r.mu.Unlock()

		return
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	for _, hash := range hashes {
		delete(r.hashes, hash)
	}

	r.overflow = r.overflow && !overflow
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
