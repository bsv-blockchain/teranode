// //go:build aerospike

// Package utxo provides UTXO (Unspent Transaction Output) management for the BSV Blockchain Teranode implementation.
//
// This file implements conflicting transaction processing functionality for handling double-spend scenarios
// and transaction conflicts in the UTXO store. It requires the aerospike build tag.
package utxo

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-subtree"
	txmap "github.com/bsv-blockchain/go-tx-map"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/bsv-blockchain/teranode/stores/utxo/meta"
	"github.com/bsv-blockchain/teranode/util/tracing"
	"golang.org/x/sync/errgroup"
)

// step5RetryDelays controls the bounded back-off when SetLocked(false) fails at the very
// last step of ProcessConflicting. The slice length is the number of attempts; the value
// at index i is the delay BEFORE attempt i (so index 0 is always zero — the first attempt
// is immediate). Rolling back at step 5 would create more inconsistency than retrying a
// simple state update — see ProcessConflicting for rationale.
//
// Declared as a package-level var so tests can shrink the delays.
var step5RetryDelays = []time.Duration{0, 50 * time.Millisecond, 200 * time.Millisecond}

// ProcessConflicting is a method to process conflicting transactions
// We got a txp (parent), txa and txb. txa and txb are both spending txp[5].
//
// tx_original is in block 102a
// - tx_original.input[0] = tx_parent1[5]
// - tx_original.input[1] = tx_parent2[6]
//
// tx_original_child1 is in block 102a
// - tx_original_child1.input[0] = tx_parent4[0]
//
// tx_double_spend is in block 102b --> block 103b --> block 104b
// - tx_double_spend.input[0] = tx_parent1[5] - spent by tx_original
// - tx_double_spend.input[1] = tx_parent3[1] - unspent
//
// ReplaceSpend(txb, []chainhash.Hash{txa})
//
// What happens with tx_parent2[6]?
//
/*
5 phase commit
 - 1: mark tx_original and all it's children as conflicting
 - 2: un-spend tx_original (update tx_parent1 & tx_parent2 utxos) and all it's children (tx_parent4 from tx_original_child1),
      marking all unspent txs as not spendable (tx_parent1 & tx_parent2 & tx_parent4)
 - 3: spend tx_double_spend as normal (ignoring the not spendable flag)
 - 4: mark tx_double_spend as not conflicting
 - 5: mark tx_parent1 & tx_parent2 & tx_parent4 as spendable again
*/
// ProcessConflicting returns:
//   - losingTxHashesMap: hashes of txs displaced by the winners (the immediate
//     counter-conflicting set from GetCounterConflicting). Used by callers to
//     mark losers in subtrees / drop them from upstream paths.
//   - allMarkedConflicting: every hash marked Conflicting=true during this run,
//     in BFS order — losers + every descendant the cascade reached. Callers
//     (notably block assembly) need this superset to populate a conflictingMap
//     so the queue→subtree dequeue path can reject children of conflicting
//     parents that arrive after the cascade has run.
func ProcessConflicting(ctx context.Context, s Store, blockHeight uint32, conflictingTxHashes []chainhash.Hash,
	processedConflictingHashesMap map[chainhash.Hash]bool) (losingTxHashesMap txmap.TxMap, allMarkedConflicting []chainhash.Hash, err error) {
	ctx, _, deferFn := tracing.Tracer("utxo").Start(ctx, "ProcessConflicting")

	defer deferFn()

	// State for the deferred compensating rollback. Each commit phase flips a flag; the
	// deferred block reads them on the way out and undoes whatever happened — see #4561.
	// allMarkedHashes mirrors the allMarkedConflicting named return but is read by the
	// deferred block; a `return nil, nil, err` in the error paths clobbers the named
	// return before the deferred runs, so we keep a parallel copy that survives.
	var (
		step1Committed        bool
		step2Committed        bool
		step4Committed        bool
		step5Failed           bool // distinct from "not committed" — rollback is intentionally skipped
		affectedParentSpends  []*Spend
		markedAsNotSpendable  []chainhash.Hash
		step3SuccessfulSpends []*Spend
		allMarkedHashes       []chainhash.Hash
	)

	defer func() {
		if err == nil || !step1Committed {
			return
		}

		// Step 5 (SetLocked false) is the last simple state update. Steps 1-4 are correct
		// at this point; rolling back would re-introduce conflicting flags and unspend the
		// winner — strictly worse. Surface the error and let the operator unlock manually.
		if step5Failed {
			return
		}

		rollbackErr := rollbackProcessConflicting(ctx, s, conflictingTxHashes,
			allMarkedHashes, markedAsNotSpendable, step3SuccessfulSpends, blockHeight,
			step2Committed, step4Committed)
		if rollbackErr != nil {
			err = errors.NewProcessingError("[ProcessConflicting] MANUAL INTERVENTION REQUIRED: original=%v rollback=%v", err, rollbackErr)
		}

		losingTxHashesMap = nil
		allMarkedConflicting = nil
	}()

	// 0. Get the transactions, check they are conflicting
	winningTxs := make([]*bt.Tx, len(conflictingTxHashes))

	// losingTxHashesPerConflictingTx is a slice of slices, each slice contains the hashes of the transactions that are conflicting
	// with the winning transaction at the same index in the winningTxs slice
	losingTxHashesPerConflictingTx := make([][]chainhash.Hash, len(conflictingTxHashes))
	losingTxHashesPerConflictingTxCount := atomic.Int64{}

	g, gCtx := errgroup.WithContext(ctx)

	for idx, txHash := range conflictingTxHashes {
		idx := idx
		txHash := txHash

		if txHash.Equal(subtree.CoinbasePlaceholderHashValue) {
			// the counter-conflicting tx is frozen, we should not process anything further
			return nil, nil, errors.NewProcessingError("[ProcessConflicting][%s] tx is frozen", txHash.String())
		}

		g.Go(func() error {
			txMeta, err := s.Get(gCtx, &txHash, fields.Tx, fields.BlockIDs, fields.Conflicting)
			if err != nil {
				return errors.NewProcessingError("[ProcessConflicting][%s] error getting tx", txHash.String(), err)
			}

			// the transaction should be marked as conflicting, otherwise it shouldn't be in this process
			// unless it was already processed in this run, then it will be in the processedConflictingHashesMap.
			// This can occur when a transaction is in multiple forks, and we are moving back from one fork to another
			// and the transaction was already processed in the previous fork.
			if !txMeta.Conflicting && !processedConflictingHashesMap[txHash] {
				return errors.NewProcessingError("[ProcessConflicting][%s] tx is not conflicting", txHash.String())
			}

			// get the counter conflicting transactions for the current transaction
			// this includes all the children of the conflicting transaction
			if losingTxHashesPerConflictingTx[idx], err = s.GetCounterConflicting(gCtx, txHash); err != nil {
				return errors.NewProcessingError("[ProcessConflicting][%s] error getting counter conflicting txs", txHash.String(), err)
			}

			winningTxs[idx] = txMeta.Tx

			losingTxHashesPerConflictingTxCount.Add(int64(len(losingTxHashesPerConflictingTx[idx])))

			return nil
		})
	}

	if err = g.Wait(); err != nil {
		return nil, nil, err
	}

	// create a unique list of all the losing tx hashes
	losingTxHashesMap = txmap.NewSplitSwissMap(int(losingTxHashesPerConflictingTxCount.Load()))

	for _, hashes := range losingTxHashesPerConflictingTx {
		for _, hash := range hashes {
			// an error will be returned if the hash already exists in the map
			// we don't really care, we just need the unique hashes
			_ = losingTxHashesMap.Put(hash, 1)
		}
	}

	losingTxHashes := losingTxHashesMap.Keys()

	// - 1: mark all losingTxHashesPerConflictingTx as conflicting + all its spending transactions recursively.
	//   allMarkedConflicting is the BFS expansion: every hash now flagged Conflicting=true. Forwarded to callers so
	//   the block-assembly conflictingMap can include the cascaded descendants (not just the immediate losers).
	affectedParentSpends, allMarkedHashes, err = MarkConflictingRecursively(ctx, s, losingTxHashes)
	if err != nil {
		return nil, nil, err
	}

	allMarkedConflicting = allMarkedHashes
	step1Committed = true

	// - 2: un-spend txa, marking the input txs as not spendable (txp & txq)
	if err = s.Unspend(ctx, affectedParentSpends, true); err != nil {
		return nil, nil, errors.NewProcessingError("error unspending affected parent spends", err)
	}

	step2Committed = true

	// get the unique hashes of the transactions that were marked as not spendable
	markedAsNotSpendableHashesUnique := make(map[chainhash.Hash]struct{})
	for _, spend := range affectedParentSpends {
		markedAsNotSpendableHashesUnique[*spend.TxID] = struct{}{}
	}

	markedAsNotSpendable = make([]chainhash.Hash, 0, len(markedAsNotSpendableHashesUnique))
	for hash := range markedAsNotSpendableHashesUnique {
		markedAsNotSpendable = append(markedAsNotSpendable, hash)
	}

	// - 3: spend tx_double_spend as normal (ignoring the not spendable flag)
	var tErr *errors.Error

	for _, tx := range winningTxs {
		spends, spendErr := s.Spend(ctx, tx, blockHeight, IgnoreFlags{
			IgnoreConflicting: true,
			IgnoreLocked:      true,
		})
		// Capture per-input partial successes regardless of overall outcome so the rollback
		// can undo them via Unspend(false) (parents at step 3 entry were unlocked-by-us, so
		// the unspend MUST NOT relock).
		for _, sp := range spends {
			if sp != nil && sp.Err == nil {
				step3SuccessfulSpends = append(step3SuccessfulSpends, sp)
			}
		}

		if spendErr != nil {
			if errors.As(spendErr, &tErr) {
				for _, spend := range spends {
					if spend.Err != nil {
						tErr.SetWrappedErr(spend.Err)
					}
				}
			}

			err = spendErr
			return nil, nil, err
		}
	}

	// - 4: mark txb as not conflicting
	if _, _, err = s.SetConflicting(ctx, conflictingTxHashes, false); err != nil {
		return nil, nil, err
	}

	step4Committed = true

	// - 5: mark txp & txq as spendable again. Step 5 is a near-final state update; rolling
	// the entire commit back now would re-introduce the very inconsistencies we just fixed.
	// Retry with bounded back-off and surface any persistent failure for operator action.
	if err = setLockedWithRetry(ctx, s, markedAsNotSpendable, false); err != nil {
		step5Failed = true
		return nil, nil, err
	}

	return losingTxHashesMap, allMarkedConflicting, nil
}

// rollbackProcessConflicting reverses the committed phases of ProcessConflicting in
// reverse-of-forward order. It is best-effort: each sub-step's error is collected via
// errors.Join so subsequent sub-steps still run. If the caller sees a non-nil return
// the UTXO store may be in an inconsistent state — see ProcessConflicting deferred
// block which tags this as MANUAL INTERVENTION REQUIRED.
func rollbackProcessConflicting(ctx context.Context, s Store, conflictingTxHashes,
	allMarkedHashes, markedAsNotSpendable []chainhash.Hash,
	step3SuccessfulSpends []*Spend, blockHeight uint32, step2Committed, step4Committed bool) error {
	var rollbackErr error

	// 1. Undo step 4 first (re-mark winners as conflicting) so the system briefly observes
	// "everything is conflicting" rather than "winner accepted but parents still missing
	// their spend record".
	if step4Committed {
		if _, _, e := s.SetConflicting(ctx, conflictingTxHashes, true); e != nil {
			rollbackErr = errors.Join(rollbackErr, errors.NewProcessingError("rollback step 4 (re-mark winners conflicting) failed", e))
		}
	}

	// 2. Undo step 3 partial spends. Pass flagAsLocked=false: step 3 used IgnoreLocked, so
	// re-locking here is meaningless — the parents are still locked from step 2 and that
	// lock will be cleared together at step 5 of the rollback (SetLocked false below).
	if len(step3SuccessfulSpends) > 0 {
		if e := s.Unspend(ctx, step3SuccessfulSpends, false); e != nil {
			rollbackErr = errors.Join(rollbackErr, errors.NewProcessingError("rollback step 3 (unspend partial winning spends) failed", e))
		}
	}

	// 3. Undo step 2: re-spend every tx the cascade marked conflicting so the original
	// spending_data is restored on affectedParentSpends. We iterate allMarkedHashes — not
	// just losingTxHashes — because MarkConflictingRecursively does a BFS that reaches
	// spending descendants of the counter-conflicting set, and step 2's Unspend covered
	// the parents of that whole cascade. Skipping descendants would leave their parent
	// UTXOs unspent and the store in a torn state. Parents are still locked here, so we
	// MUST set IgnoreLocked; the cascade is still flagged conflicting, so IgnoreConflicting.
	// A descendant's body may be unfetchable (pruned, frozen placeholder, or missing) —
	// log via rollbackErr and continue rather than abort the rest of the unwind.
	if step2Committed {
		for _, h := range allMarkedHashes {
			h := h

			if h.Equal(subtree.CoinbasePlaceholderHashValue) {
				continue
			}

			txMeta, e := s.Get(ctx, &h, fields.Tx)
			if e != nil {
				rollbackErr = errors.Join(rollbackErr, errors.NewProcessingError("rollback step 2 (fetch tx %s) failed", h.String(), e))
				continue
			}

			if txMeta == nil || txMeta.Tx == nil {
				rollbackErr = errors.Join(rollbackErr, errors.NewProcessingError("rollback step 2 (tx %s has no body)", h.String()))
				continue
			}

			if _, e := s.Spend(ctx, txMeta.Tx, blockHeight, IgnoreFlags{IgnoreConflicting: true, IgnoreLocked: true}); e != nil {
				rollbackErr = errors.Join(rollbackErr, errors.NewProcessingError("rollback step 2 (re-spend tx %s) failed", h.String(), e))
			}
		}
	}

	// 4. Undo step 1: clear the conflicting flag on every hash MarkConflictingRecursively
	// added (cascaded children included).
	if len(allMarkedHashes) > 0 {
		if _, _, e := s.SetConflicting(ctx, allMarkedHashes, false); e != nil {
			rollbackErr = errors.Join(rollbackErr, errors.NewProcessingError("rollback step 1 (clear conflicting flag) failed", e))
		}
	}

	// 5. Undo the lock applied at step 2 (only attempted if step 2 actually committed).
	if step2Committed && len(markedAsNotSpendable) > 0 {
		if e := s.SetLocked(ctx, markedAsNotSpendable, false); e != nil {
			rollbackErr = errors.Join(rollbackErr, errors.NewProcessingError("rollback step 2 lock (SetLocked false) failed", e))
		}
	}

	return rollbackErr
}

// setLockedWithRetry retries SetLocked with a bounded back-off — a deliberate exception
// to "always roll back". By the time we reach step 5 every other phase is committed and
// correct; the only inconsistency a SetLocked failure introduces is parents that are
// still locked. Rolling back here would re-introduce conflicting markers and unspend the
// winner — strictly worse than retrying a simple state update.
func setLockedWithRetry(ctx context.Context, s Store, hashes []chainhash.Hash, value bool) error {
	var err error

	for _, delay := range step5RetryDelays {
		if delay > 0 {
			select {
			case <-ctx.Done():
				return errors.NewProcessingError("setLockedWithRetry aborted by context", ctx.Err())
			case <-time.After(delay):
			}
		}

		if err = s.SetLocked(ctx, hashes, value); err == nil {
			return nil
		}
	}

	return err
}

// MarkConflictingRecursively marks the given transactions as conflicting, and iteratively marks all their spending
// children as conflicting too using breadth-first traversal.
//
// Parameters:
//   - ctx: The context for managing request-scoped values, cancellation signals, and deadlines.
//   - s: The UTXO store interface used to interact with the underlying data store.
//   - hashes: A slice of transaction hashes to be marked as conflicting.
//
// Returns:
//   - A slice of pointers to Spend structs representing the affected parent spends.
//   - A slice of all transaction hashes that were marked conflicting by this call,
//     including the input hashes and every descendant reached via BFS. Insertion
//     order is BFS order (input level first, then each descendant level) — callers
//     can rely on this for deterministic logs, traces, and eviction ordering.
//   - An error if any issues occur during the process.
func MarkConflictingRecursively(ctx context.Context, s Store, hashes []chainhash.Hash) ([]*Spend, []chainhash.Hash, error) {
	ctx, _, deferFn := tracing.Tracer("utxo").Start(ctx, "MarkConflictingRecursively")

	defer deferFn()

	var allAffectedSpends []*Spend
	toProcess := hashes

	visited := make(map[chainhash.Hash]struct{}, len(hashes))
	markedOrder := make([]chainhash.Hash, 0, len(hashes))
	for _, h := range hashes {
		if _, ok := visited[h]; !ok {
			visited[h] = struct{}{}
			markedOrder = append(markedOrder, h)
		}
	}

	for len(toProcess) > 0 {
		affectedParentSpends, spendingChildTxs, err := s.SetConflicting(ctx, toProcess, true)
		if err != nil {
			return nil, nil, err
		}

		allAffectedSpends = append(allAffectedSpends, affectedParentSpends...)

		// filter out already-visited hashes to prevent infinite loops
		nextBatch := spendingChildTxs[:0]
		for _, child := range spendingChildTxs {
			if _, ok := visited[child]; !ok {
				visited[child] = struct{}{}
				markedOrder = append(markedOrder, child)
				nextBatch = append(nextBatch, child)
			}
		}
		toProcess = nextBatch
	}

	return allAffectedSpends, markedOrder, nil
}

func GetAndLockChildren(ctx context.Context, s Store, hash chainhash.Hash) ([]chainhash.Hash, error) {
	ctx, _, deferFn := tracing.Tracer("utxo").Start(ctx, "GetAndLockChildren")

	defer deferFn()

	if hash.Equal(subtree.CoinbasePlaceholderHashValue) {
		// skip the coinbase placeholder hash
		return nil, errors.NewProcessingError("[GetAndLockChildren][%s] tx is frozen", hash.String())
	}

	visited := make(map[chainhash.Hash]struct{})
	visited[hash] = struct{}{}
	currentLevel := []chainhash.Hash{hash}

	for len(currentLevel) > 0 {
		results := make([]*meta.Data, len(currentLevel))
		g, gCtx := errgroup.WithContext(ctx)

		for i, current := range currentLevel {
			i := i
			current := current
			g.Go(func() error {
				if err := s.SetLocked(gCtx, []chainhash.Hash{current}, true); err != nil {
					return err
				}
				txMeta, err := s.Get(gCtx, &current, fields.Utxos)
				if err != nil {
					return err
				}
				results[i] = txMeta
				return nil
			})
		}

		if err := g.Wait(); err != nil {
			return nil, err
		}

		var nextLevel []chainhash.Hash
		for _, txMeta := range results {
			if txMeta == nil {
				continue
			}

			if txMeta.SpendingDatas != nil {
				for _, spendingData := range txMeta.SpendingDatas {
					if spendingData != nil {
						child := *spendingData.TxID
						if _, ok := visited[child]; ok {
							continue
						}

						if child.Equal(subtree.CoinbasePlaceholderHashValue) {
							return nil, errors.NewProcessingError("[GetAndLockChildren][%s] tx is frozen", child.String())
						}

						visited[child] = struct{}{}
						nextLevel = append(nextLevel, child)
					}
				}
			}
		}
		currentLevel = nextLevel
	}

	// exclude the root hash from the result
	delete(visited, hash)

	children := make([]chainhash.Hash, 0, len(visited))
	for child := range visited {
		children = append(children, child)
	}

	return children, nil
}

func GetConflictingChildren(ctx context.Context, s Store, hash chainhash.Hash) ([]chainhash.Hash, error) {
	ctx, _, deferFn := tracing.Tracer("utxo").Start(ctx, "GetConflictingChildren")

	defer deferFn()

	if hash.Equal(subtree.CoinbasePlaceholderHashValue) {
		// skip the coinbase placeholder hash
		return nil, nil
	}

	visited := make(map[chainhash.Hash]struct{})
	visited[hash] = struct{}{}
	currentLevel := []chainhash.Hash{hash}

	for len(currentLevel) > 0 {
		results := make([]*meta.Data, len(currentLevel))
		g, gCtx := errgroup.WithContext(ctx)

		for i, current := range currentLevel {
			i := i
			current := current
			g.Go(func() error {
				txMeta, err := s.Get(gCtx, &current, fields.Utxos, fields.ConflictingChildren)
				if err != nil {
					return err
				}
				results[i] = txMeta
				return nil
			})
		}

		if err := g.Wait(); err != nil {
			return nil, err
		}

		var nextLevel []chainhash.Hash
		for _, txMeta := range results {
			if txMeta == nil {
				continue
			}

			if txMeta.ConflictingChildren != nil {
				for _, child := range txMeta.ConflictingChildren {
					if _, ok := visited[child]; !ok {
						visited[child] = struct{}{}
						nextLevel = append(nextLevel, child)
					}
				}
			}

			if txMeta.SpendingDatas != nil {
				for _, spendingData := range txMeta.SpendingDatas {
					if spendingData != nil {
						child := *spendingData.TxID
						if _, ok := visited[child]; !ok {
							visited[child] = struct{}{}
							nextLevel = append(nextLevel, child)
						}
					}
				}
			}
		}
		currentLevel = nextLevel
	}

	// exclude the root hash from the result
	delete(visited, hash)

	conflictingChildren := make([]chainhash.Hash, 0, len(visited))
	for child := range visited {
		conflictingChildren = append(conflictingChildren, child)
	}

	return conflictingChildren, nil
}

func GetCounterConflictingTxHashes(ctx context.Context, s Store, txHash chainhash.Hash) ([]chainhash.Hash, error) {
	ctx, _, deferFn := tracing.Tracer("utxo").Start(ctx, "GetCounterConflictingTxHashes")

	defer deferFn()

	txMeta, err := s.Get(ctx, &txHash, fields.Tx)
	if err != nil {
		return nil, err
	}

	counterConflictingMap := make(map[chainhash.Hash]struct{})
	counterConflictingMap[txHash] = struct{}{}

	// get the unique parent txs
	parentTxs := make(map[chainhash.Hash][]*chainhash.Hash)

	for _, input := range txMeta.Tx.Inputs {
		// get the parent tx
		parentTxs[*input.PreviousTxIDChainHash()] = nil
	}

	for parentTx := range parentTxs {
		parentTxHash := &parentTx

		parentTxMeta, err := s.Get(ctx, parentTxHash, fields.Utxos)
		if err != nil {
			return nil, err
		}

		spendingTxIDs := make([]*chainhash.Hash, len(parentTxMeta.SpendingDatas))

		for idx, spendingData := range parentTxMeta.SpendingDatas {
			if spendingData == nil {
				continue
			}

			spendingTxIDs[idx] = spendingData.TxID
		}

		parentTxs[*parentTxHash] = spendingTxIDs
	}

	for _, input := range txMeta.Tx.Inputs {
		parenTxIDS, ok := parentTxs[*input.PreviousTxIDChainHash()]
		if ok {
			// check the length of the spending txs, if it's less than the index, then the input is not spent
			if len(parenTxIDS) <= int(input.PreviousTxOutIndex) {
				// throw an error
				return nil, errors.NewProcessingError("[GetCounterConflictingTxHashes][%s] cannot process counter conflicting, input %d of %s is out of range (len: %d, %v)", txHash.String(), input.PreviousTxOutIndex, input.PreviousTxIDChainHash().String(), len(parenTxIDS), parenTxIDS)
			}

			spendingTxID := parenTxIDS[input.PreviousTxOutIndex]
			if spendingTxID != nil {
				counterConflictingMap[*spendingTxID] = struct{}{}

				childHashes, err := s.GetConflictingChildren(ctx, *spendingTxID)
				if err != nil {
					return nil, err
				}

				for _, childHash := range childHashes {
					if childHash.Equal(subtree.FrozenBytesTxHash) {
						return nil, errors.NewProcessingError("[GetCounterConflictingTxHashes][%s] tx has frozen child", spendingTxID.String())
					}

					counterConflictingMap[childHash] = struct{}{}
				}
			}
		}
	}

	counterConflicting := make([]chainhash.Hash, 0, len(counterConflictingMap))

	for child := range counterConflictingMap {
		counterConflicting = append(counterConflicting, child)
	}

	// fmt.Printf("counterConflicting: %v\n", counterConflicting)

	return counterConflicting, nil
}
