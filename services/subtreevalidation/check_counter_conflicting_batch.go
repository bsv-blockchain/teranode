package subtreevalidation

import (
	"context"
	"slices"
	"sync"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/bsv-blockchain/teranode/stores/utxo/meta"
	"github.com/bsv-blockchain/teranode/util/tracing"
	"golang.org/x/sync/errgroup"
)

// missingRecordPolicy is how checkCounterConflictingOnCurrentChainBatch treats a
// record the store does not hold.
type missingRecordPolicy int

const (
	// skipMissingRecords is the cached path's policy. A conflicting tx whose own
	// or parent record is gone is returned in notFound, and checked only through
	// the records that are left; a cone member that is gone is a leaf of the walk
	// and counts as not mined. A trailer can outlive the records it names, so a
	// gap there is housekeeping, not evidence.
	skipMissingRecords missingRecordPolicy = iota

	// failOnMissingRecords is the policy of the fresh path and of
	// blessMissingTransaction, as it was the per-transaction check's: any record the check needs and cannot read — a conflicting tx, a
	// parent, a counter-spender, a cone member, a member's GetMeta — fails the
	// check with a retryable ErrTxNotFound, unless a conflict is proven from the
	// records that were read (see VERDICT on
	// checkCounterConflictingOnCurrentChainBatch).
	failOnMissingRecords
)

// counterConflictingBatchFanOut caps the concurrent store reads of each phase of
// checkCounterConflictingOnCurrentChainBatch, matching the GetMeta errgroup of
// the per-transaction check it replaced and the walk's own per-level ceiling.
const counterConflictingBatchFanOut = 128

// checkCounterConflictingOnCurrentChainBatch asks, for a whole set of
// conflicting transactions at once, the question the per-transaction check
// (checkCounterConflictingOnCurrentChain, now kept only as a test reference)
// asked of one: has any of them, or any transaction that spent the same
// outputs (and that spender's descendants), been mined on the chain described by
// blockIds? It returns ErrTxInvalid when one has.
//
// # WHY A BATCH
//
// Asked one transaction at a time, the check walks each transaction's own cone
// and its counter-spenders' cones separately. Conflicting transactions in one
// block share those cones: in a linear conflicting chain T1→…→TN each Ti's own
// cone is Ti+1…TN, and only T1 has a counter-spender other than a chain member, so
// per-transaction checks cost ~N²/2 sequential store reads (issue 2037: a
// 1,788-transaction chain took about five hours against a 30 minute timeout, and
// every retry started from zero). Here every record is read a constant number of
// times, and the walk starts from every transaction at once, so a chain costs a
// few wide store rounds rather than N deep ones.
//
// # PHASES
//
//  1. Counter-spenders. Each transaction's inpoints and its parents' spenders are
//     read, and its counter-spenders derived as
//     utxo.GetCounterConflictingTxHashes derives them, including the out-of-range
//     input error and the frozen sentinel.
//  2. One walk. utxo.GetConflictingChildrenFromRoots walks two passes on one
//     visited set: first from every counter-spender, which yields the union of
//     the counter-spender cones, then from the transactions themselves, which
//     yields the union of their own cones less what the first pass already saw. A
//     frozen sentinel in either union is a processing error.
//  3. Mined check. The counter-conflicting union — the transactions, their
//     counter-spenders and the counter-spender cones — is read with GetMeta and
//     checked against blockIds. Own-cone members are frozen-checked only, as in
//     the per-transaction check.
//
// # BUDGET
//
// utxostore_conflictingChildrenMaxNodes is a per-root budget, as it was for each
// per-transaction walk. The phase 2 walk may visit it times the number of
// distinct roots (transactions and counter-spenders): the aggregate bound of the
// per-transaction walks, so any block whose every per-transaction walk fit still
// passes, however many conflicting transactions it carries. Exceeding it stops
// the walk, in either pass. If the counter-conflicting members it reached
// include one mined on the candidate chain, the verdict is ErrTxInvalid on the
// first attempt, and raising the budget does not change that (see VERDICT).
// Otherwise it fails closed with
// ERR_UTXO_WALK_LIMIT_EXCEEDED, retryably, and raising the budget resumes
// progress. The price is that a single runaway cone is stopped only after that
// many reads, not after maxNodes.
//
// # MISSING RECORDS
//
// The policy is the caller's, per path:
//
//   - failOnMissingRecords (the fresh path) is the per-transaction check's
//     behaviour: any record the check needs and the store does not hold returns
//     a retryable processing error wrapping ErrTxNotFound, unless a conflict is
//     proven from the records that were read (see VERDICT).
//   - skipMissingRecords (the cached path) returns a transaction whose own or
//     parent record is gone in notFound. A transaction whose own record is gone is
//     dropped; one whose parent is gone is still checked through its other
//     parents. A cone member that is gone is a leaf of the walk, and a member
//     whose GetMeta is not found counts as not mined; the rest of the union is
//     still checked. That is stricter than the per-transaction loop the cached
//     path used to run, which skipped a transaction's whole check on any missing
//     member.
//
// # VERDICT
//
// A tx-invalid verdict proven from records that were read is final, and is
// returned whatever else in the batch could not be checked. Proven means a member
// of some transaction's counter-conflicting set, read from the store, is mined in
// a block of blockIds; one transaction's failure says nothing against another's
// evidence. Only when nothing is proven does a processing error (a frozen
// sentinel, the walk budget, a store fault, or a missing record under
// failOnMissingRecords) fail the check, retryably. Otherwise one transaction whose
// parent was pruned for good would make a block that also carries a provably
// invalid transaction retry forever instead of being rejected.
//
// So the check carries on past a failure as far as is cheap:
//
//   - Phase 1 fails per input. An input whose parent record cannot be read, which
//     is out of range, or whose spender is the frozen sentinel is recorded, and
//     the spenders of the transaction's other inputs are still collected. Only a
//     transaction whose own record cannot be read is dropped from phases 2 and 3.
//   - Phase 2 is one walk shared by every transaction, so a walk that stops (on
//     the budget, or on a store fault in any cone) stops for all of them. What it
//     reached before stopping is still mined-checked; what it did not reach is
//     not. Carrying a walk on past a fault in one cone would need a walk per
//     cone, which is what this batch replaced.
//   - Phase 3 reads every member, whatever the others return.
//
// Every member is read before a verdict is given, and the reported member and
// error do not depend on read timing.
//
// Every caller asks this one check: the cached path, the fresh path, and
// blessMissingTransaction with a batch of one. The verdict rule is the same
// whichever path reaches a transaction; only the missing-record policy differs
// (see MISSING RECORDS).
func (u *Server) checkCounterConflictingOnCurrentChainBatch(ctx context.Context, txHashes []chainhash.Hash, blockIds map[uint32]bool, missingPolicy missingRecordPolicy) (notFound []chainhash.Hash, err error) {
	ctx, _, deferFn := tracing.Tracer("subtreevalidation").Start(ctx, "checkCounterConflictingOnCurrentChainBatch")
	defer deferFn()

	// processingErr is the first failure in phase order. It is returned only if
	// nothing is proven mined.
	var processingErr error

	fail := func(err error) {
		if processingErr == nil {
			processingErr = err
		}
	}

	txs, notFound, counterSpenders, spentAgainst, txErr := u.counterSpendersBatch(ctx, txHashes)
	if txErr != nil {
		fail(txErr)
	}

	if len(notFound) > 0 && missingPolicy == failOnMissingRecords {
		fail(missingRecordsError("tx or parent record", notFound))
	}

	if len(txs) == 0 {
		if processingErr != nil {
			return nil, processingErr
		}

		return notFound, nil
	}

	// A walk that stops returns the cones it reached so far (see the VERDICT
	// note above); their members are still mined-checked.
	cones, missing, walkErr := utxo.GetConflictingChildrenFromRoots(ctx, u.utxoStore, [][]chainhash.Hash{counterSpenders, txs}, u.settings.UtxoStore.ConflictingChildrenMaxNodes)
	if walkErr != nil {
		fail(errors.NewProcessingError("[checkCounterConflictingOnCurrentChainBatch] failed to walk the cones of %d transaction(s) and %d counter-spender(s)", len(txs), len(counterSpenders), walkErr))
	}

	counterCone, ownCone := cones[0], cones[1]

	// The per-transaction check compares some positions against
	// CoinbasePlaceholderHashValue and others against FrozenBytesTxHash. go-subtree
	// defines both as the same all-0xFF hash, so one comparison per cone covers
	// every position; Test_frozenSentinelsAreOneValue pins that.
	if slices.Contains(counterCone, subtreepkg.FrozenBytesTxHash) {
		fail(errors.NewProcessingError("[checkCounterConflictingOnCurrentChainBatch] a counter-conflicting tx has frozen child"))
	}

	if slices.Contains(ownCone, subtreepkg.FrozenBytesTxHash) {
		fail(errors.NewProcessingError("[checkCounterConflictingOnCurrentChainBatch] child transaction is frozen"))
	}

	if len(missing) > 0 && missingPolicy == failOnMissingRecords {
		fail(missingRecordsError("walked record", missing))
	}

	// A walk that stops, on its budget or on a store fault, still has the counter
	// cone it reached mined-checked: every member of it descends from a
	// counter-spender, so any mined one is proof.
	counterConflicting := make(map[chainhash.Hash]struct{}, len(txs)+len(counterSpenders)+len(counterCone))
	for _, hashes := range [][]chainhash.Hash{txs, counterSpenders, counterCone} {
		for _, hash := range hashes {
			// the frozen sentinel is not a record; it was dealt with above
			if !hash.Equal(subtreepkg.FrozenBytesTxHash) {
				counterConflicting[hash] = struct{}{}
			}
		}
	}

	if len(counterConflicting) > conflictingConeWarnThreshold {
		u.logger.Warnf("[checkCounterConflictingOnCurrentChainBatch] counter-conflicting set of %d transaction(s) is %d transactions", len(txs), len(counterConflicting))
	}

	minedHash, readErr := u.firstMinedOnChain(ctx, counterConflicting, blockIds, missingPolicy)
	if readErr != nil {
		fail(readErr)
	}

	if minedHash == nil {
		if processingErr != nil {
			return nil, processingErr
		}

		return notFound, nil
	}

	if processingErr != nil {
		u.logger.Warnf("[checkCounterConflictingOnCurrentChainBatch][%s] is mined on the candidate chain, so the batch is tx-invalid despite a processing error: %v", minedHash.String(), processingErr)
	}

	// Name the conflicting tx that condemns the block where that is cheap: the
	// mined tx is one of the conflicting txs, or the direct counter-spender of
	// one. A tx found deeper in a counter-spender's cone is named alone; the walk
	// does not record which root reached each node.
	if slices.Contains(txs, *minedHash) {
		return nil, errors.NewTxInvalidError("[checkCounterConflictingOnCurrentChainBatch][%s] conflicting transaction is already mined on our chain", minedHash.String())
	}

	if conflictingTx, ok := spentAgainst[*minedHash]; ok {
		return nil, errors.NewTxInvalidError("[checkCounterConflictingOnCurrentChainBatch][%s] counter-conflicting transaction %s is already mined on our chain", conflictingTx.String(), minedHash.String())
	}

	return nil, errors.NewTxInvalidError("[checkCounterConflictingOnCurrentChainBatch][%s] counter-conflicting transaction is already mined on our chain", minedHash.String())
}

// counterSpendersBatch is phase 1 of checkCounterConflictingOnCurrentChainBatch.
// It returns the deduplicated transactions whose own records were read, those
// with a record not found (its own or a parent's), and the unique counter-spenders
// of the read ones, each derived as utxo.GetCounterConflictingTxHashes derives
// them. A transaction with a parent not found is in both lists: its spenders
// through its other parents are still collected. spentAgainst maps each
// counter-spender to the first transaction, in txHashes order, that it is a
// counter-spender of.
//
// txErr is the first other failure in txHashes order: a store fault on a
// transaction's own record, which drops the transaction, or one of
// counterSpendersOf's per-input failures, which does not. The other results are
// complete either way: one transaction's or input's failure does not stop the
// rest.
func (u *Server) counterSpendersBatch(ctx context.Context, txHashes []chainhash.Hash) (txs, notFound, counterSpenders []chainhash.Hash, spentAgainst map[chainhash.Hash]chainhash.Hash, txErr error) {
	unique := make([]chainhash.Hash, 0, len(txHashes))
	seen := make(map[chainhash.Hash]struct{}, len(txHashes))

	for _, txHash := range txHashes {
		if _, ok := seen[txHash]; !ok {
			seen[txHash] = struct{}{}
			unique = append(unique, txHash)
		}
	}

	// Only the parent hashes are read, so ask for the inpoints rather than the
	// whole transaction (see GetCounterConflictingTxHashes).
	txMetas, txReadErrs := u.getBatch(ctx, unique, fields.TxInpoints)

	parentIdx := make(map[chainhash.Hash]int)
	parents := make([]chainhash.Hash, 0, len(unique))

	for _, txMeta := range txMetas {
		if txMeta == nil {
			continue
		}

		for _, parentHash := range txMeta.TxInpoints.GetParentTxHashes() {
			if _, ok := parentIdx[parentHash]; !ok {
				parentIdx[parentHash] = len(parents)
				parents = append(parents, parentHash)
			}
		}
	}

	parentMetas, parentReadErrs := u.getBatch(ctx, parents, fields.Utxos)

	spentAgainst = make(map[chainhash.Hash]chainhash.Hash)

	for i, txHash := range unique {
		if txReadErrs[i] != nil {
			if txErr == nil {
				txErr = errors.NewProcessingError("[checkCounterConflictingOnCurrentChainBatch][%s] failed to get the inpoints of the transaction", txHash.String(), txReadErrs[i])
			}

			continue
		}

		txMeta := txMetas[i]
		if txMeta == nil {
			notFound = append(notFound, txHash)
			continue
		}

		// A failed input is recorded, not fatal: a spender on another input is
		// still evidence, and the verdict rule decides between them.
		txCounterSpenders, missingParent, err := counterSpendersOf(txHash, txMeta, parentIdx, parentMetas, parentReadErrs)
		if err != nil && txErr == nil {
			txErr = err
		}

		// A parent the store does not hold says nothing about who spent its
		// outputs, so the counter-spenders through it cannot be derived. Those
		// through the transaction's other parents still can.
		if missingParent {
			notFound = append(notFound, txHash)
		}

		for _, counterSpender := range txCounterSpenders {
			if _, ok := spentAgainst[counterSpender]; !ok {
				spentAgainst[counterSpender] = txHash
				counterSpenders = append(counterSpenders, counterSpender)
			}
		}

		txs = append(txs, txHash)
	}

	return txs, notFound, counterSpenders, spentAgainst, txErr
}

// counterSpendersOf derives one transaction's counter-spenders from its parents'
// records, read by counterSpendersBatch. It collects the spender of every input
// it can, whatever happens to the others. missingParent reports a parent the store
// does not hold; err, the first other input that could not be checked: a parent
// that could not be read, an input out of range, or a frozen output.
func counterSpendersOf(txHash chainhash.Hash, txMeta *meta.Data, parentIdx map[chainhash.Hash]int, parentMetas []*meta.Data, parentReadErrs []error) (counterSpenders []chainhash.Hash, missingParent bool, err error) {
	fail := func(inputErr error) {
		if err == nil {
			err = inputErr
		}
	}

	for _, inpoint := range txMeta.TxInpoints.GetTxInpoints() {
		idx := parentIdx[inpoint.Hash]

		if readErr := parentReadErrs[idx]; readErr != nil {
			fail(errors.NewProcessingError("[checkCounterConflictingOnCurrentChainBatch][%s] failed to get the spenders of parent %s", txHash.String(), inpoint.Hash.String(), readErr))
			continue
		}

		if parentMetas[idx] == nil {
			missingParent = true
			continue
		}

		spendingDatas := parentMetas[idx].SpendingDatas

		// check the length of the spending txs, if it's less than the index, then the input is not spent
		if len(spendingDatas) <= int(inpoint.Index) {
			fail(errors.NewProcessingError("[checkCounterConflictingOnCurrentChainBatch][%s] cannot process counter conflicting, input %d of %s is out of range (len: %d)", txHash.String(), inpoint.Index, inpoint.Hash.String(), len(spendingDatas)))
			continue
		}

		spendingData := spendingDatas[inpoint.Index]
		if spendingData == nil || spendingData.TxID == nil {
			continue
		}

		// if a transaction is frozen, the counter-transaction will be the same as the coinbase placeholder
		if spendingData.TxID.Equal(subtreepkg.CoinbasePlaceholderHashValue) {
			fail(errors.NewProcessingError("[checkCounterConflictingOnCurrentChainBatch][%s] counter conflicting tx is frozen", txHash.String()))
			continue
		}

		counterSpenders = append(counterSpenders, *spendingData.TxID)
	}

	return counterSpenders, missingParent, err
}

// missingRecordsError is the failOnMissingRecords verdict: a processing error, so
// it is retried, wrapping ErrTxNotFound, as the per-transaction check returns it.
func missingRecordsError(what string, hashes []chainhash.Hash) error {
	return errors.NewProcessingError("[checkCounterConflictingOnCurrentChainBatch] cannot check counter-conflicting transactions",
		errors.NewTxNotFoundError("[checkCounterConflictingOnCurrentChainBatch][%s] %s not found, %d record(s) missing", hashes[0].String(), what, len(hashes)))
}

// getBatch reads hashes concurrently, index-aligned. A record the store does not
// hold — ErrTxNotFound, or a nil record as aerospike returns — is a nil entry
// with a nil error; any other read error is that entry's error. One failed read
// does not cancel the others.
func (u *Server) getBatch(ctx context.Context, hashes []chainhash.Hash, f ...fields.FieldName) ([]*meta.Data, []error) {
	results := make([]*meta.Data, len(hashes))
	errs := make([]error, len(hashes))

	var g errgroup.Group

	g.SetLimit(counterConflictingBatchFanOut)

	for i := range hashes {
		g.Go(func() error {
			txMeta, err := u.utxoStore.Get(ctx, &hashes[i], f...)
			if err != nil {
				if !errors.Is(err, errors.ErrTxNotFound) {
					errs[i] = err
				}

				return nil
			}

			results[i] = txMeta

			return nil
		})
	}

	_ = g.Wait()

	return results, errs
}

// firstMinedOnChain is phase 3 of checkCounterConflictingOnCurrentChainBatch. It
// reads every member's block IDs and returns a member mined in a block of
// blockIds, or nil. A member the store does not hold counts as not mined under
// skipMissingRecords, and is an error under failOnMissingRecords.
//
// A failed read does not stop the others, and both results can be set: a mined
// member, and the error of a member that could not be read. The caller decides
// which wins. Each is the one of the lowest member hash, so neither depends on
// read timing. Cancelling the other reads after a failure would lose a mined
// member not read yet, and with it a proven conflict.
func (u *Server) firstMinedOnChain(ctx context.Context, members map[chainhash.Hash]struct{}, blockIds map[uint32]bool, missingPolicy missingRecordPolicy) (*chainhash.Hash, error) {
	var (
		mu        sync.Mutex
		mined     *chainhash.Hash
		failed    *chainhash.Hash
		failedErr error
		g         errgroup.Group
	)

	g.SetLimit(counterConflictingBatchFanOut)

	for member := range members {
		g.Go(func() error {
			memberMeta := &meta.Data{}
			if err := u.utxoStore.GetMeta(ctx, &member, memberMeta); err != nil {
				if errors.Is(err, errors.ErrTxNotFound) {
					if missingPolicy == skipMissingRecords {
						return nil
					}

					err = missingRecordsError("counter-conflicting record", []chainhash.Hash{member})
				} else {
					err = errors.NewProcessingError("[checkCounterConflictingOnCurrentChainBatch][%s] failed to get counter conflicting tx meta", member.String(), err)
				}

				mu.Lock()
				if failed == nil || member.String() < failed.String() {
					failed, failedErr = &member, err
				}
				mu.Unlock()

				return nil
			}

			for _, blockID := range memberMeta.BlockIDs {
				if blockIds[blockID] {
					mu.Lock()
					// keep the lowest hash, so the reported member does not depend on read timing
					if mined == nil || member.String() < mined.String() {
						mined = &member
					}
					mu.Unlock()

					break
				}
			}

			return nil
		})
	}

	_ = g.Wait()

	return mined, failedErr
}

// conflictingTxCollector gathers the conflicting transactions that concurrent
// processTxMetaUsingStore batches decorate, with their txMetaSlice index, so they
// can be checked in one batch once every batch is done.
type conflictingTxCollector struct {
	mu      sync.Mutex
	hashes  []chainhash.Hash
	indexes []int
}

// add records the conflicting tx at txMetaSlice[idx]. Safe for concurrent use.
func (c *conflictingTxCollector) add(hash chainhash.Hash, idx int) {
	c.mu.Lock()
	c.hashes = append(c.hashes, hash)
	c.indexes = append(c.indexes, idx)
	c.mu.Unlock()
}

// settleCollectedConflictingTxs ends processTxMetaUsingStore: given the error its
// decorate batches returned, it returns the error the call must return.
//
// It upholds one invariant: an entry processTxMetaUsingStore leaves set has
// passed the counter-conflicting check. A set entry is never decorated again, so
// a conflicting tx left set without a verdict would be accepted on the retry.
//
//   - What was collected is checked even when a batch failed, on ctx rather than
//     the batches' cancelled group context. An ErrTxInvalid verdict is final and
//     stronger than any batch failure (a fail-fast threshold, say), so it is
//     returned in preference, with TX_INVALID on top (provenConflict); any other
//     check failure only replaces a nil.
//   - The collected entries are cleared only when the check failed, so the retry
//     decorates and checks them again. When it passed they have passed and stay
//     set, even if a batch failed: a fail-fast retry then decorates and checks
//     only what is still unset, not the whole batch again.
//
// Missing records fail the check (failOnMissingRecords), as the per-transaction
// check did on this path and as blessMissingTransaction's does.
func (u *Server) settleCollectedConflictingTxs(ctx context.Context, batchErr error, conflicting *conflictingTxCollector, txMetaSlice []metaSliceItem, blockIds map[uint32]bool) error {
	if len(conflicting.hashes) == 0 {
		return batchErr
	}

	_, checkErr := u.checkCounterConflictingOnCurrentChainBatch(ctx, conflicting.hashes, blockIds, failOnMissingRecords)
	if checkErr == nil {
		return batchErr
	}

	for _, idx := range conflicting.indexes {
		txMetaSlice[idx] = metaSliceItem{}
	}

	// A proven conflict keeps TX_INVALID on top, which is how
	// processTxMetaUsingStore tells it from every other failure.
	if provenConflict(checkErr) {
		return errors.NewTxInvalidError("[processTxMetaUsingStore] %d counter conflicting tx(s) conflict with the current chain", len(conflicting.hashes), checkErr)
	}

	checkErr = errors.NewProcessingError("[processTxMetaUsingStore] failed to check %d counter conflicting tx(s) on current chain", len(conflicting.hashes), checkErr)

	if batchErr == nil {
		return checkErr
	}

	return batchErr
}

// provenConflict reports whether err is a verdict that a conflict is proven:
// TX_INVALID at the top of the error, as checkCounterConflictingOnCurrentChainBatch
// and settleCollectedConflictingTxs return one. A TX_INVALID deeper in a chain,
// under another code, is not.
func provenConflict(err error) bool {
	var verdict *errors.Error

	return errors.As(err, &verdict) && verdict.Code() == errors.ERR_TX_INVALID
}
