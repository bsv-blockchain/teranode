package pruner

import (
	"context"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/bsv-blockchain/teranode/stores/utxo/meta"
)

// defaultCreatingSweepMaxPerPass is the per-pass cap used when
// Pruner.CreatingTxSweepMaxPerPass is zero. The cap bounds both the store-side scan and
// the serial roll-forward loop so neither grows with the unmined backlog after a crash
// burst. A backlog above the cap is not swept in order: each pass takes one page of it, and the
// store hands back a different page each time (see the note on convergence at sweepCreatingTxs),
// so a record beyond the cap is reached on a later pass, not necessarily the next one.
const defaultCreatingSweepMaxPerPass = 1000

type handOffStage uint8

const (
	stageAnnounce handOffStage = iota
	stageUnlock
)

// handOff is a rolled-forward record whose hand-off to block assembly is not complete. stage is
// the step that is still owed. validatedHeight is the value of the first attempt. lanePosition is
// the lane position the record carries, or 0 when it has none.
type handOff struct {
	hash            chainhash.Hash
	stage           handOffStage
	fee, size       uint64
	inpoints        subtree.TxInpoints
	validatedHeight uint32
	lanePosition    uint64
}

// sweepCreatingTxs heals transactions abandoned in the create-first "creating" state. A
// record still creating is rolled forward: the sweep re-runs the input spends from the
// stored tx bytes and finalizes it. Spends are idempotent for the same spender, so
// re-spending inputs an earlier attempt already spent is safe. The roll-forward does not
// ignore locks: a locked parent is the hand-off barrier, so ErrTxLocked leaves the record
// creating and it is retried on every pass. Only when pruner_creatingTxSweepBlockedAgeBlocks is set
// is such a record rolled back, and then only when it is older than that age and the sweep has seen
// every parent that blocked it locked for that long too (blockedByParentsForLong). A tx that lost a double-spend (ErrSpent,
// ErrTxConflicting, ErrFrozen, ErrUtxoHashMismatch), or that spends a missing parent or a vout
// that does not exist (ErrTxNotFound), is rolled back by
// utxo.RollBackCreating: the record is marked rolling back, every input of the tx is
// unspent and the record becomes a hold that keeps its txid taken for a while. A record already
// marked rolling back was abandoned by a crashed rollback owner; the sweep finishes it (utxo.FinishRollBackCreating) by taking
// the hold of its attempt, unspending every input once no spender holds a live lease, and ending
// the rollback. A hold expires on its own; one that a namespace without record expiry keeps past
// its time is deleted here. A record marked rolling back that also carries the reversed marker has all its
// spends undone: the sweep ends its rollback without taking the hold, unspending or reading the
// transaction, which may be gone from the blob store. The sweep reads the record in two steps, the
// markers first and the transaction only when the record needs a roll-forward or a reversal. Records it cannot roll forward for another reason (e.g. a
// locked parent, or a finalize that fails) are left creating, gated (unspendable), and retried on a later pass. After finalizing, the
// sweep hands the tx to block assembly and only then unlocks it, the order
// loadUnminedTransactions uses. Each of the two steps is tried a few times in the pass. A record
// whose announce or unlock still failed goes on a bounded in-memory list, which the next pass
// drains before its query; the list is lost on restart, and block assembly's reload then announces
// and unlocks the record as before. With no block assembly client configured the record stays
// finalized and locked for that reload and is not listed. Runs whatever the value of utxostore_useCreateFirstOrder, so records stranded before the
// flag was turned off still heal.
//
// A record that belongs to a block is only ever completed. That is a record that carries block IDs,
// or the markOwed stamp that the pre-flight of a longest-chain mark puts on a creating record it
// finds (meta.Data.MarkOwed). Its transaction is in a block, so the sweep never rolls it back,
// whatever its re-spend answers: a rollback unspends by txid, and would free outputs that a mined
// transaction spent. It is not announced to block assembly either, which would offer a mined
// transaction for the next template. Its roll-forward ignores locks, as the pre-flight's does.
//
// Convergence above the cap. The query has no cursor: every pass asks for one page of at most
// pruner_creatingTxSweepMaxPerPass records from the whole range. A record whose parent is still
// creating is left for a later pass, and it heals in a pass that takes it after its parent has
// healed, so a backlog of dependent records drains only as fast as the pages happen to cover
// parents before their children. What keeps it from stalling is that the store does not return the
// same page twice: on a single-node Aerospike, 12 passes of 1000 over 4000 creating records shared
// about a quarter of their hashes with the first page, which is what a random page would share,
// and reached 3873 of the 4000. That is a property of how the server streams a partition query,
// not a contract, and a chain of dependent records in a backlog many times the cap can take many
// blocks to drain. The only signal is the warning below when a non-empty page makes no progress.
func (s *Server) sweepCreatingTxs(ctx context.Context, blockHeight uint32) {
	minAge := s.settings.Pruner.CreatingTxSweepMinAgeBlocks
	if minAge == 0 || blockHeight <= minAge {
		return
	}

	maxPerPass := int(s.settings.Pruner.CreatingTxSweepMaxPerPass)
	if maxPerPass == 0 {
		maxPerPass = defaultCreatingSweepMaxPerPass
	}

	pass := &handOffPass{sleepLeft: handOffSleepBudgetDelays * s.handOffBackoff()}

	defer s.forgetBlockedParents(blockHeight)

	s.drainHandOffs(ctx, blockHeight, pass)

	hashes, err := s.utxoStore.QueryStaleCreatingTxs(ctx, blockHeight-minAge, maxPerPass)
	if err != nil {
		s.logger.Errorf("[pruner][creating-sweep][%d] query failed: %v", blockHeight, err)
		return
	}

	prunerCreatingBacklog.Set(float64(len(hashes)))

	if len(hashes) == 0 {
		return
	}

	if len(hashes) == maxPerPass {
		s.logger.Infof("[pruner][creating-sweep][%d] hit per-pass cap of %d; the stale creating txs beyond it are taken by later passes, in no fixed order", blockHeight, maxPerPass)
	}

	s.logger.Infof("[pruner][creating-sweep][%d] healing %d stale creating txs", blockHeight, len(hashes))

	// Track progress so a page that is stuck for a persistent reason (e.g. ≥cap records
	// with a permanently locked parent) is distinguishable from "more remain,
	// caught up next block" — the reviewers' non-convergence concern.
	progressed := 0
	warnedNoBlockAssembly := false

	// parentBlocked caches, for this pass, whether the sweep has seen a parent locked for at least
	// pruner_creatingTxSweepBlockedAgeBlocks. Many records can wait on one parent.
	parentBlocked := make(map[chainhash.Hash]bool)

	for _, hash := range hashes {
		hash := hash

		if ctx.Err() != nil {
			s.logger.Infof("[pruner][creating-sweep][%d] context cancelled, aborting sweep: %v", blockHeight, ctx.Err())
			return
		}

		md, err := s.utxoStore.Get(ctx, &hash, fields.Creating, fields.RollingBack, fields.Attempt, fields.Reversed)
		if err != nil {
			s.logger.Warnf("[pruner][creating-sweep][%d] get %s failed: %v", blockHeight, hash, err)
			continue
		}

		if md.Hold {
			cleared, clearErr := s.utxoStore.ClearCreateFirstHold(ctx, &hash, false)
			if clearErr != nil {
				s.logger.Errorf("[pruner][creating-sweep][%d] clearing the hold of %s failed, will retry next block: %v", blockHeight, hash, clearErr)
				prunerCreatingSweepFailures.Inc()

				continue
			}

			if cleared {
				progressed++

				s.logger.Infof("[pruner][creating-sweep][%d] deleted the expired hold of %s", blockHeight, hash)
			}

			continue
		}

		if !md.Creating {
			// Finalized between the query and this get.
			continue
		}

		if md.RollingBack && md.Reversed {
			if err = s.utxoStore.EndCreateFirstRollback(ctx, &hash, md.Attempt); err != nil {
				s.logger.Errorf("[pruner][creating-sweep][%d] ending the reversed rollback of %s failed, will retry next block: %v", blockHeight, hash, err)
				prunerCreatingSweepFailures.Inc()

				continue
			}

			progressed++

			s.logger.Infof("[pruner][creating-sweep][%d] ended the reversed rollback of %s", blockHeight, hash)

			continue
		}

		md, err = s.utxoStore.Get(ctx, &hash, fields.Tx, fields.Creating, fields.RollingBack, fields.Attempt, fields.Reversed, fields.MarkOwed, fields.BlockIDs, fields.Fee, fields.SizeInBytes, fields.TxInpoints, fields.UnminedSince, fields.LanePosition)
		if err != nil {
			s.logger.Warnf("[pruner][creating-sweep][%d] get of the transaction of %s failed: %v", blockHeight, hash, err)
			continue
		}

		if !md.Creating || md.Tx == nil || md.Hold {
			// Finalized, replaced by a hold, or unreadable between the two gets.
			continue
		}

		if md.RollingBack && md.Reversed {
			continue
		}

		if md.RollingBack {
			finished, finishErr := utxo.FinishRollBackCreating(utxo.WithReversalCallHeight(ctx, blockHeight), s.logger, s.utxoStore, md.Tx, md.Attempt, false)
			if finishErr != nil {
				s.logger.Errorf("[pruner][creating-sweep][%d] finishing rollback of %s failed, will retry next block: %v", blockHeight, hash, finishErr)
				prunerCreatingSweepFailures.Inc()

				continue
			}

			if finished {
				progressed++

				s.logger.Infof("[pruner][creating-sweep][%d] finished abandoned rollback of %s", blockHeight, hash)
			}

			continue
		}

		if len(md.Attempt) == 0 {
			// A creating record with no attempt token was not written by a create-first attempt.
			// The create that wrote it did not defer the spend of its inputs, and its clear of the
			// creating bin failed. There is no spend to repeat and nothing to roll back: the
			// sweep clears the bin, which is all that the record still needs. Its creator
			// reported it as stored, so it is not announced again.
			finErr := s.utxoStore.FinalizeTransaction(ctx, md.Tx, nil, nil)
			if finErr != nil && !utxo.IsAlreadyFinal(finErr) {
				s.logger.Warnf("[pruner][creating-sweep][%d] clearing the creating state of %s, which carries no attempt token, failed, will retry next block: %v", blockHeight, hash, finErr)
				prunerCreatingSweepFailures.Inc()

				continue
			}

			progressed++

			s.logger.Infof("[pruner][creating-sweep][%d] cleared the creating state of %s, which carries no attempt token", blockHeight, hash)

			continue
		}

		if md.MarkOwed || len(md.BlockIDs) > 0 {
			if s.completeBlockOwned(ctx, blockHeight, hash, md) {
				progressed++
			}

			continue
		}

		// Re-spend the inputs only (the record already exists in the creating state) and
		// finalize, through the shared roll-forward rule. Spends are idempotent for the same
		// spender, which makes roll-forward safe. Locks are not ignored: a locked parent is
		// the hand-off barrier, so ErrTxLocked is retried on a later pass rather than
		// bypassed. The rule takes a spend lease first, which the store refuses for a record
		// that is rolling back, and a rollback that begins later waits for the lease and then
		// reverses the spends this pass made.
		rfSpends, completed, rfErr := utxo.RollForwardCreating(ctx, s.logger, s.utxoStore, md.Tx, md.Attempt, blockHeight)
		if rfErr != nil {
			// A locked or creating parent is transient, and the record waits for it. Only when
			// pruner_creatingTxSweepBlockedAgeBlocks is set is such a record rolled back, and then only
			// when the record is that old and every parent that blocked it has been blocked for that
			// long too: one that is not committing keeps the inputs that did spend, and every output of
			// the record, unspendable for as long as it lasts.
			if utxo.IsUnwinnableSpendError(rfErr) || (isTransientSpendError(rfErr) && s.blockedByParentsForLong(ctx, blockHeight, md.UnminedSince, rfSpends, parentBlocked)) {
				// A lease that is still live belongs to a spender that returned with an unknown
				// outcome or crashed. The rollback would wait for it, so the record is left for a
				// later pass, when the lease is stale.
				if live, liveErr := s.utxoStore.SpendLeasesLive(ctx, &hash, md.Attempt); liveErr != nil || live {
					s.logger.Warnf("[pruner][creating-sweep][%d] %s cannot be completed but a spender may still hold a lease on it, will retry next block: %v", blockHeight, hash, liveErr)
					prunerCreatingSweepFailures.Inc()

					continue
				}

				ended, rbErr := utxo.RollBackCreatingEnded(utxo.WithReversalCallHeight(ctx, blockHeight), s.logger, s.utxoStore, md.Tx, md.Attempt)
				if rbErr != nil {
					s.logger.Errorf("[pruner][creating-sweep][%d] rollback of %s failed, will retry next block: %v", blockHeight, hash, rbErr)
					prunerCreatingSweepFailures.Inc()

					continue
				}

				if ended {
					progressed++ // loser unspent and held
				}

				continue
			}

			s.logger.Warnf("[pruner][creating-sweep][%d] roll-forward of %s failed, will retry next block: %v", blockHeight, hash, rfErr)
			prunerCreatingSweepFailures.Inc()

			continue
		}

		if !completed {
			// Another actor's finalize completed the record. That actor owns the hand-off to block
			// assembly and the unlock after it.
			progressed++

			continue
		}

		if s.blockAssemblyClient == nil {
			if !warnedNoBlockAssembly {
				s.logger.Warnf("[pruner][creating-sweep][%d] no block assembly client; rolled-forward txs are not announced and stay locked", blockHeight)

				warnedNoBlockAssembly = true
			}

			prunerCreatingSweepFailures.Inc()

			continue
		}

		entry := handOff{
			hash:            hash,
			stage:           stageAnnounce,
			fee:             md.Fee,
			size:            md.SizeInBytes,
			inpoints:        md.TxInpoints,
			validatedHeight: utxo.UnminedValidatedHeight(md.UnminedSince, blockHeight, s.settings.UtxoStore.UnminedTxRetention),
			lanePosition:    md.LanePosition,
		}

		if !s.completeHandOff(ctx, blockHeight, &entry, pass) {
			s.queueHandOff(blockHeight, entry)

			continue
		}

		progressed++

		s.logger.Infof("[pruner][creating-sweep][%d] rolled forward %s", blockHeight, hash)
	}

	// Zero progress on a non-empty page (especially a full one) means the page is stuck on
	// records that cannot converge — surface it as a warning rather than the misleading
	// "will catch up next block" info line above.
	if progressed == 0 {
		s.logger.Warnf("[pruner][creating-sweep][%d] rolled forward 0 of %d stale creating txs; the page may be stuck (e.g. a permanently locked parent)", blockHeight, len(hashes))
	}
}

// completeBlockOwned rolls forward a creating record whose transaction is in a block: one that
// carries block IDs, or the markOwed stamp of a longest-chain mark. It reports whether the record
// is complete. The record is never rolled back and never announced to block assembly, whatever the
// roll-forward answers, and it is not unlocked here: the write that marks it mined unlocks it.
func (s *Server) completeBlockOwned(ctx context.Context, blockHeight uint32, hash chainhash.Hash, md *meta.Data) bool {
	if _, _, rfErr := utxo.RollForwardCreating(ctx, s.logger, s.utxoStore, md.Tx, md.Attempt, blockHeight, utxo.WithIgnoreLocked(true)); rfErr != nil {
		s.logger.Warnf("[pruner][creating-sweep][%d] %s belongs to a block (owed the longest-chain mark %t, block IDs %d) and its roll-forward failed, it is never rolled back, will retry next block: %v", blockHeight, hash, md.MarkOwed, len(md.BlockIDs), rfErr)
		prunerCreatingSweepFailures.Inc()

		return false
	}

	s.logger.Infof("[pruner][creating-sweep][%d] completed %s, which belongs to a block (owed the longest-chain mark %t, block IDs %d), and did not announce it", blockHeight, hash, md.MarkOwed, len(md.BlockIDs))

	return true
}

// blockedByParentsForLong reports whether a creating record that failed its re-spend only because
// parents are in flight may be rolled back for its age. It is false when
// pruner_creatingTxSweepBlockedAgeBlocks is 0, which is the default. Otherwise the record must be
// older than that age, and the sweep must have seen every parent that gave a transient answer in
// this pass locked, and not creating, for at least that age (parentBlockedForAtLeast). A failure of
// any other kind on an input, a spend that does not name its parent, a parent that cannot be read
// and a parent that the sweep has not watched for that long all leave the record for a later pass.
func (s *Server) blockedByParentsForLong(ctx context.Context, blockHeight, unminedSince uint32, spends []*utxo.Spend, verified map[chainhash.Hash]bool) bool {
	age := s.blockedAge()
	if age == 0 {
		return false
	}

	var parents []chainhash.Hash

	for _, sp := range spends {
		if sp == nil || sp.Err == nil {
			continue
		}

		if !isTransientSpendError(sp.Err) || sp.TxID == nil {
			return false
		}

		parents = append(parents, *sp.TxID)
	}

	if len(parents) == 0 {
		return false
	}

	// Every parent is read, also after one has answered no and also for a record that is itself too
	// young: a parent that a pass does not look at is not seen, and the pass that first sees it
	// starts its age. So the sweep watches a parent from the first pass in which it blocks a record.
	long := true

	for _, parent := range parents {
		blocked, known := verified[parent]
		if !known {
			blocked = s.parentBlockedForAtLeast(ctx, blockHeight, parent, age)
			verified[parent] = blocked
		}

		long = long && blocked
	}

	if unminedSince == 0 || unminedSince >= blockHeight || blockHeight-unminedSince <= age {
		return false
	}

	return long
}

// blockedAge is pruner_creatingTxSweepBlockedAgeBlocks as the sweep applies it: 0 when the rule is
// off, and otherwise at least one block above pruner_creatingTxSweepMinAgeBlocks. The query of the
// sweep selects records that are at least the minimum age old, so an age at or below it would be met
// by a record on the first pass that sees it, with no earlier pass that retried it.
func (s *Server) blockedAge() uint32 {
	age := s.settings.Pruner.CreatingTxSweepBlockedAgeBlocks
	if age == 0 {
		return 0
	}

	if minAge := s.settings.Pruner.CreatingTxSweepMinAgeBlocks; age <= minAge {
		age = minAge + 1
	}

	return age
}

// blockedSince is what the sweep remembers of a parent that blocks a record: the height of the pass
// that first saw it locked, and of the last pass that saw it so.
type blockedSince struct {
	first, last uint32
}

// defaultBlockedParentLimit bounds the parents the sweep remembers. A parent that does not fit is
// not remembered, so it never counts as blocked for long and the rule stays off for it.
const defaultBlockedParentLimit = 10000

// parentBlockedForAtLeast reads the record of a parent and reports whether the sweep has seen it
// locked, and not creating, for at least age blocks: it is so in this pass, a pass at least age
// blocks ago saw it so, and no pass in between saw it otherwise.
//
// The age is what the sweep saw, kept in memory, and not a field of the record. No field records
// when a lock was taken, and the height at which the parent was stored says nothing about it: a
// parent that was locked a moment ago, for example by the conflict walk of a double-spend, is as
// old as its record, and a reorg backdates that height. So the first pass that sees a parent locked
// only notes it, and answers no. A parent with no note is not yet old enough, whatever its record
// says. That holds after a restart too, which loses every note: no record is rolled back for the
// age of a parent until this process has watched the parent for that long.
//
// A pass that reads the parent as not locked, or as creating, drops its note, so a parent that is
// locked again later starts again. A read that fails says nothing and keeps the note. What the
// rule does not prove is that the parent stayed locked between two passes that both saw it locked.
func (s *Server) parentBlockedForAtLeast(ctx context.Context, blockHeight uint32, parent chainhash.Hash, age uint32) bool {
	md, err := s.utxoStore.Get(ctx, &parent, fields.Locked, fields.Creating)
	if err != nil {
		s.logger.Warnf("[pruner][creating-sweep][%d] reading the parent %s of a blocked record failed, the record is left for the next pass: %v", blockHeight, parent, err)

		return false
	}

	// A creating parent is a record that this sweep heals, from the same crash as the child in
	// many cases, and the order of the query does not put it before the child, so it says nothing
	// about a parent that is stuck. The child waits for the parent to settle: once the parent is
	// rolled forward its lock ends with the hand-off, and once it is rolled back the child's spend
	// fails for good.
	if !md.Locked || md.Creating {
		delete(s.blockedParents, parent)

		return false
	}

	// A note that no pass has renewed for longer than the horizon is as good as none: the sweep did
	// not watch the parent in between.
	seen, noted := s.blockedParents[parent]
	if !noted || blockHeight < seen.last || blockHeight-seen.last > s.blockedParentHorizon(age) {
		limit := s.blockedParentLimit
		if limit <= 0 {
			limit = defaultBlockedParentLimit
		}

		if !noted && len(s.blockedParents) >= limit {
			if s.blockedParentsFullSeen != blockHeight {
				s.blockedParentsFullSeen = blockHeight
				s.logger.Warnf("[pruner][creating-sweep][%d] the sweep remembers %d blocked parents, its limit, so a parent it sees for the first time now is not noted and no record is rolled back for it", blockHeight, limit)
			}

			return false
		}

		if s.blockedParents == nil {
			s.blockedParents = make(map[chainhash.Hash]blockedSince)
		}

		s.blockedParents[parent] = blockedSince{first: blockHeight, last: blockHeight}

		return false
	}

	seen.last = blockHeight
	s.blockedParents[parent] = seen

	return blockHeight-seen.first >= age
}

// blockedParentHorizon is how long a note of a blocked parent holds without a pass renewing it:
// twice the blocked age plus the minimum age of the sweep, and at least 100 blocks. A pass renews
// the note of every parent that blocks a record it takes, and a page does not take every record of
// a large backlog, so the horizon is wider than the age.
func (s *Server) blockedParentHorizon(age uint32) uint32 {
	return max(2*age+s.settings.Pruner.CreatingTxSweepMinAgeBlocks, 100)
}

// forgetBlockedParents drops the notes that are past the horizon, so that the sweep does not keep
// what it no longer uses. A record whose parent is forgotten is not rolled back for the age of that
// parent until the sweep has watched it again for that long. With the rule off there is nothing to
// remember.
func (s *Server) forgetBlockedParents(blockHeight uint32) {
	age := s.blockedAge()
	if age == 0 {
		s.blockedParents = nil

		return
	}

	horizon := s.blockedParentHorizon(age)

	for parent, seen := range s.blockedParents {
		if blockHeight < seen.last || blockHeight-seen.last > horizon {
			delete(s.blockedParents, parent)
		}
	}
}

const (
	// handOffAttempts is how often a pass tries the announce, and then the unlock, of one record.
	handOffAttempts = 3

	// defaultHandOffBackoff is the wait before the second attempt; the wait before the third is twice
	// that. A pass sleeps at most handOffSleepBudgetDelays times one backoff in all, and an announce or
	// unlock that fails for good is not tried again for the rest of the pass, so a block assembly or
	// store outage costs a pass about 0.6 s of waiting and not a wait per record.
	defaultHandOffBackoff    = 100 * time.Millisecond
	handOffSleepBudgetDelays = 20
	defaultHandOffRetryLimit = 10000
)

// handOffPass is the state of one sweep pass for the hand-off to block assembly.
type handOffPass struct {
	sleepLeft    time.Duration
	announceDown bool
	unlockDown   bool
}

func (s *Server) handOffBackoff() time.Duration {
	if s.handOffDelay > 0 {
		return s.handOffDelay
	}

	return defaultHandOffBackoff
}

// retryHandOff runs op up to handOffAttempts times, waiting between attempts and giving up when ctx
// ends or the pass has used its sleep budget.
func (s *Server) retryHandOff(ctx context.Context, pass *handOffPass, op func() error) error {
	delay := s.handOffBackoff()

	var err error

	for attempt := 0; attempt < handOffAttempts; attempt++ {
		if attempt > 0 {
			if delay > pass.sleepLeft {
				return err
			}

			pass.sleepLeft -= delay

			timer := time.NewTimer(delay)

			select {
			case <-ctx.Done():
				timer.Stop()

				return err
			case <-timer.C:
			}

			delay *= 2
		}

		if err = op(); err == nil {
			return nil
		}
	}

	return err
}

// completeHandOff announces a rolled-forward record to block assembly and then unlocks it, from
// the stage the entry still owes. FinalizeTransaction clears creating but not the lock: the
// tentative create carried WithLocked(true) on the block-assembly path, so without the unlock the
// record stays locked and its outputs unspendable. Both steps are idempotent, and the entry keeps
// the stage reached, so an announce that succeeded is not made again. It reports whether the
// hand-off is complete.
func (s *Server) completeHandOff(ctx context.Context, blockHeight uint32, e *handOff, pass *handOffPass) bool {
	if e.stage == stageAnnounce {
		if pass.announceDown {
			return false
		}

		err := s.retryHandOff(ctx, pass, func() error {
			_, storeErr := s.blockAssemblyClient.Store(ctx, &e.hash, e.fee, e.size, e.inpoints, e.validatedHeight, e.lanePosition)

			return storeErr
		})
		if err != nil {
			s.logger.Errorf("[pruner][creating-sweep][%d] announce %s to block assembly failed, leaving it locked for a later pass: %v", blockHeight, e.hash, err)
			prunerCreatingSweepFailures.Inc()

			pass.announceDown = true

			return false
		}

		e.stage = stageUnlock
	}

	if pass.unlockDown {
		return false
	}

	err := s.retryHandOff(ctx, pass, func() error {
		return s.utxoStore.SetLocked(ctx, []chainhash.Hash{e.hash}, false)
	})
	if err != nil {
		s.logger.Errorf("[pruner][creating-sweep][%d] unlock %s failed, leaving it locked for a later pass: %v", blockHeight, e.hash, err)
		prunerCreatingSweepFailures.Inc()

		pass.unlockDown = true

		return false
	}

	return true
}

// queueHandOff puts an entry on the retry list. A full list loses its oldest entry, which is logged
// and counted: block assembly's reload still completes that record.
func (s *Server) queueHandOff(blockHeight uint32, e handOff) {
	limit := s.handOffLimit
	if limit <= 0 {
		limit = defaultHandOffRetryLimit
	}

	var (
		dropped    handOff
		didDrop    bool
		listLength int
	)

	s.handOffMu.Lock()

	if len(s.handOffs) >= limit {
		dropped, didDrop = s.handOffs[0], true

		copy(s.handOffs, s.handOffs[1:])
		s.handOffs[len(s.handOffs)-1] = handOff{}
		s.handOffs = s.handOffs[:len(s.handOffs)-1]
	}

	s.handOffs = append(s.handOffs, e)
	listLength = len(s.handOffs)

	s.handOffMu.Unlock()

	prunerCreatingSweepHandOffPending.Set(float64(listLength))

	if didDrop {
		prunerCreatingSweepHandOffDropped.Inc()
		s.logger.Warnf("[pruner][creating-sweep][%d] the retry list of %d entries is full, dropping its oldest, %s, which stays locked until block assembly reloads", blockHeight, limit, dropped.hash)
	}
}

// drainHandOffs finishes the entries of earlier passes. It reads each record first: one that is
// gone, mined or no longer locked was finished by another component and its entry is dropped, and
// one that shows anything else is kept and logged. The passes run one after the other in the pruner
// processor, so the list has one writer at a time; the lock covers any other caller.
func (s *Server) drainHandOffs(ctx context.Context, blockHeight uint32, pass *handOffPass) {
	s.handOffMu.Lock()
	entries := s.handOffs
	s.handOffs = nil
	s.handOffMu.Unlock()

	if len(entries) == 0 {
		return
	}

	for i := range entries {
		if ctx.Err() != nil {
			for _, rest := range entries[i:] {
				s.queueHandOff(blockHeight, rest)
			}

			break
		}

		if s.resumeHandOff(ctx, blockHeight, &entries[i], pass) {
			s.queueHandOff(blockHeight, entries[i])
		}
	}

	s.handOffMu.Lock()
	listLength := len(s.handOffs)
	s.handOffMu.Unlock()

	prunerCreatingSweepHandOffPending.Set(float64(listLength))
}

// resumeHandOff reports whether the entry stays on the list. The read shows the master record
// only. SetLocked writes the master first and the pagination records after it, so an unlock that
// failed on the pages leaves the master unlocked. An entry at the unlock stage is therefore not
// finished by an unlocked master: its unlock runs again, which is idempotent and covers the pages.
func (s *Server) resumeHandOff(ctx context.Context, blockHeight uint32, e *handOff, pass *handOffPass) bool {
	md, err := s.utxoStore.Get(ctx, &e.hash, fields.Locked, fields.Creating, fields.Conflicting, fields.BlockIDs)

	switch {
	case err != nil && errors.Is(err, errors.ErrTxNotFound):
		s.logger.Infof("[pruner][creating-sweep][%d] dropped the hand-off of %s: the record is gone", blockHeight, e.hash)

		return false
	case err != nil:
		s.logger.Warnf("[pruner][creating-sweep][%d] reading %s for its hand-off failed, will retry next block: %v", blockHeight, e.hash, err)

		return true
	case len(md.BlockIDs) > 0:
		s.logger.Infof("[pruner][creating-sweep][%d] dropped the hand-off of %s: it is mined, which clears its locks", blockHeight, e.hash)

		return false
	case !md.Locked && e.stage != stageUnlock:
		s.logger.Infof("[pruner][creating-sweep][%d] dropped the hand-off of %s: it is no longer locked, so another component finished it", blockHeight, e.hash)

		return false
	case md.Creating || md.Conflicting:
		s.logger.Warnf("[pruner][creating-sweep][%d] the record of %s, whose hand-off is owed, is creating %t and conflicting %t; keeping the entry", blockHeight, e.hash, md.Creating, md.Conflicting)

		return true
	}

	if s.blockAssemblyClient == nil {
		return true
	}

	if !s.completeHandOff(ctx, blockHeight, e, pass) {
		return true
	}

	s.logger.Infof("[pruner][creating-sweep][%d] completed the hand-off of %s from an earlier pass", blockHeight, e.hash)

	return false
}

// isTransientSpendError reports whether a spend error says that a parent is committing: its record
// is locked, or still creating.
func isTransientSpendError(err error) bool {
	return errors.Is(err, errors.ErrTxLocked) || errors.Is(err, errors.ErrTxCreating)
}
