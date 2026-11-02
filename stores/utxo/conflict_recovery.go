package utxo

import (
	"bytes"
	"context"
	"encoding/json"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	txmap "github.com/bsv-blockchain/go-tx-map"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"golang.org/x/sync/errgroup"
)

// ConflictRecovery is one operation's durable checklist. The before-image is
// immutable after publication; only Phase changes. No process-local addresses
// or transaction objects are serialized.
type ConflictRecovery struct {
	Version        int
	Phase          string
	Transactions   []ConflictTransaction
	OriginalSpends []*Spend
	WinningSpends  []*Spend
	Parents        []chainhash.Hash
	// Winners are the transactions the operation promotes. They are never part of Losers.
	Winners []chainhash.Hash
	Losers  []chainhash.Hash
}

type ConflictTransaction struct {
	Hash        chainhash.Hash
	Conflicting bool
}

const (
	ConflictRecoveryPlanning    = "planning"
	ConflictRecoveryMutating    = "mutating"
	ConflictRecoveryRollingBack = "rolling_back"
	ConflictRecoveryFinished    = "finished"
	ConflictRecoveryPrepared    = "prepared"
	ConflictRecoveryApplied     = "applied"
	ConflictRecoveryRolledBack  = "rolled_back"
)

func EncodeConflictRecovery(r *ConflictRecovery) ([]byte, error) {
	if err := r.Validate(); err != nil {
		return nil, err
	}
	return json.Marshal(r)
}

func DecodeConflictRecovery(data []byte) (*ConflictRecovery, error) {
	if len(data) == 0 {
		return nil, nil
	}
	var r ConflictRecovery

	if err := json.Unmarshal(data, &r); err != nil {
		return nil, errors.NewStorageError("invalid conflict recovery checklist", err)
	}
	if err := r.Validate(); err != nil {
		return nil, err
	}
	return &r, nil
}

func (r *ConflictRecovery) Validate() error {
	if r == nil || r.Version != 1 {
		return errors.NewStorageError("unsupported conflict recovery checklist version")
	}
	switch r.Phase {
	case ConflictRecoveryPlanning, ConflictRecoveryPrepared, ConflictRecoveryMutating, ConflictRecoveryApplied, ConflictRecoveryRolledBack, ConflictRecoveryRollingBack, ConflictRecoveryFinished:
	default:
		return errors.NewStorageError("unknown conflict recovery phase %q", r.Phase)
	}
	for _, list := range [][]*Spend{r.OriginalSpends, r.WinningSpends} {
		for _, sp := range list {
			if sp == nil || sp.TxID == nil || sp.UTXOHash == nil || (sp.SpendingData != nil && sp.SpendingData.TxID == nil) {
				return errors.NewStorageError("invalid spend in conflict recovery checklist")
			}
		}
	}
	return nil
}

// prepareConflictRecovery snapshots the whole cascade before SetConflicting or
// Unspend can erase any edge. Parents already locked by another holder stay out of
// this operation's lock and unlock list. The reads run concurrently, bounded by
// conflictingWalkFanOut; the checklist keeps discovery order.
func prepareConflictRecovery(ctx context.Context, s Store, hashes []chainhash.Hash, winningSpends []*Spend) (*ConflictRecovery, error) {
	r := &ConflictRecovery{Version: 1, Phase: ConflictRecoveryPrepared, WinningSpends: winningSpends}

	// Keep only what the checklist needs from each transaction, not its body: a cascade can be wide.
	type txSnapshot struct {
		spends      []*Spend
		conflicting bool
	}

	snapshots := make([]txSnapshot, len(hashes))
	if err := forEachConflictRead(ctx, len(hashes), func(ctx context.Context, i int) error {
		m, err := s.Get(ctx, &hashes[i], fields.Tx, fields.Conflicting)
		if err != nil {
			return err
		}

		if m == nil || m.Tx == nil {
			return errors.NewProcessingError("cannot snapshot conflict transaction %s", hashes[i].String())
		}

		spends, err := GetSpends(m.Tx)
		if err != nil {
			return err
		}

		snapshots[i] = txSnapshot{spends: spends, conflicting: m.Conflicting}

		return nil
	}); err != nil {
		return nil, err
	}

	seen := make(map[outpoint]struct{})

	addSpends := func(spends []*Spend) {
		for _, sp := range spends {
			key := outpoint{*sp.TxID, sp.Vout}
			if _, ok := seen[key]; ok {
				continue
			}

			seen[key] = struct{}{}
			r.OriginalSpends = append(r.OriginalSpends, sp.Clone())
		}
	}

	for i, h := range hashes {
		r.Transactions = append(r.Transactions, ConflictTransaction{Hash: h, Conflicting: snapshots[i].conflicting})
		addSpends(snapshots[i].spends)
	}

	// Winning-only inputs also need before-images (including originally unspent inputs).
	addSpends(winningSpends)

	if err := forEachConflictRead(ctx, len(r.OriginalSpends), func(ctx context.Context, i int) error {
		original := r.OriginalSpends[i]

		response, err := s.GetSpend(ctx, original)
		if err != nil {
			return err
		}

		if response == nil || response.Status == int(Status_NOT_FOUND) || response.Status == int(Status_FROZEN) {
			return errors.NewProcessingError("cannot snapshot conflict outpoint %s:%d", original.TxID, original.Vout)
		}

		original.SpendingData = response.SpendingData.Clone()

		return nil
	}); err != nil {
		return nil, err
	}

	members := make(map[chainhash.Hash]struct{}, len(hashes))
	for _, h := range hashes {
		members[h] = struct{}{}
	}

	winning := make(map[outpoint]struct{}, len(winningSpends))
	for _, sp := range winningSpends {
		winning[outpoint{*sp.TxID, sp.Vout}] = struct{}{}
	}

	// An input of a conflicting cascade member can belong to a transaction outside the cascade. That
	// spend is not the cascade's, so the operation must neither unspend nor restore it. A winning input
	// with an outside owner is different: a validator spent it after discovery, so plan again.
	var parents hashSet

	kept := r.OriginalSpends[:0]
	for _, original := range r.OriginalSpends {
		if original.SpendingData != nil {
			if _, member := members[*original.SpendingData.TxID]; !member {
				if _, ok := winning[outpoint{*original.TxID, original.Vout}]; ok {
					return nil, errors.NewProcessingError("conflict spender %s at %s:%d changed during preparation; retry required", original.SpendingData.TxID.String(), original.TxID.String(), original.Vout)
				}

				continue
			}
		}

		kept = append(kept, original)
		parents.add(*original.TxID)
	}

	r.OriginalSpends = kept

	locked := make([]bool, len(parents.order))
	if err := forEachConflictRead(ctx, len(parents.order), func(ctx context.Context, i int) error {
		m, err := s.Get(ctx, &parents.order[i], fields.Locked)
		if err != nil {
			return err
		}

		if m == nil {
			return errors.NewProcessingError("conflict parent %s is missing", parents.order[i].String())
		}

		locked[i] = m.Locked

		return nil
	}); err != nil {
		return nil, err
	}

	// A lock held by someone else (a validator, or a lock an older build leaked) is not this
	// operation's to take or release. Leave it out of the lock set instead of failing every attempt.
	for i, h := range parents.order {
		if !locked[i] {
			r.Parents = append(r.Parents, h)
		}
	}

	return r, nil
}

// outpoint identifies one transaction output.
type outpoint struct {
	hash  chainhash.Hash
	index uint32
}

// conflictTransactionHashes lists the hashes of every transaction in the checklist.
func conflictTransactionHashes(r *ConflictRecovery) []chainhash.Hash {
	hashes := make([]chainhash.Hash, len(r.Transactions))
	for i, tx := range r.Transactions {
		hashes[i] = tx.Hash
	}

	return hashes
}

// forEachConflictRead runs read for every index in [0, n), at most conflictingWalkFanOut at a
// time. Each call writes only its own index, so callers keep a deterministic order.
func forEachConflictRead(ctx context.Context, n int, read func(ctx context.Context, i int) error) error {
	g, gCtx := errgroup.WithContext(ctx)
	g.SetLimit(conflictingWalkFanOut)

	for i := 0; i < n; i++ {
		// Some stores ignore ctx in their reads, so do not start more reads after a failure.
		if gCtx.Err() != nil {
			break
		}

		g.Go(func() error {
			// g.Go can wait for a slot that the failing read frees, so check again here.
			if err := gCtx.Err(); err != nil {
				return err
			}

			return read(gCtx, i)
		})
	}

	if err := g.Wait(); err != nil {
		return err
	}

	// errgroup reports errors only from reads that ran. A cancelled ctx can stop the loop or the reads
	// with every index unread, which must not pass for success.
	return ctx.Err()
}

// rollbackConflictRecovery restores recorded owners, never every transaction
// visited by the cascade. Unexpected spenders cause an error, not an overwrite.
//
// A child outside the checklist can attach to a checklist transaction at any time. The rollback
// never touches that child's inputs, so it needs no before-image. When its parent ends Conflicting,
// the child is marked Conflicting too, and cascaded returns it with its descendants.
func rollbackConflictRecovery(ctx context.Context, s Store, r *ConflictRecovery, height uint32, id chainhash.Hash) (cascaded []chainhash.Hash, err error) {
	// A saved applied operation can have usable winners. Disable all recorded
	// transactions before removing any winning spend during compensation.
	hashes := conflictTransactionHashes(r)
	known := make(map[chainhash.Hash]struct{}, len(hashes))
	for _, h := range hashes {
		known[h] = struct{}{}
	}
	// SetConflicting reports the spenders of a transaction's outputs. Aerospike reports them on every
	// call; SQL only while the transaction is not yet Conflicting, because its GetSpend reports an
	// output of a Conflicting transaction as CONFLICTING (ConflictRecoverySetConflictingChildren pins
	// both). So collect the children outside the checklist from this first mark: it runs while the
	// applied winners and restored losers are not Conflicting. A later mark can miss them on SQL.
	var outside hashSet

	if len(hashes) > 0 {
		_, children, err := s.SetConflicting(ctx, hashes, true)
		if err != nil {
			return nil, err
		}
		for _, child := range children {
			if _, ok := known[child]; !ok {
				outside.add(child)
			}
		}
	}
	// A retry after a failed rollback finds the winners already Conflicting, so on SQL the mark above
	// reports none of their children. Read the winners' spenders directly: only a winner, applied and
	// not Conflicting, can have gained a child outside the checklist. The reads run in parallel, so
	// each one fills its own slot and the merge happens after they finish. A winner the checklist
	// names but does not hold was applied by an earlier checklist of the block, and this rollback
	// leaves it as it is, so its children stay too.
	spenders := make([][]chainhash.Hash, len(r.Winners))

	if err := forEachConflictRead(ctx, len(r.Winners), func(ctx context.Context, i int) error {
		if _, ok := known[r.Winners[i]]; !ok {
			return nil
		}
		m, err := s.Get(ctx, &r.Winners[i], fields.Utxos)
		if err != nil {
			return err
		}
		if m == nil {
			return nil
		}
		for _, sd := range m.SpendingDatas {
			if sd != nil && sd.TxID != nil {
				if _, ok := known[*sd.TxID]; !ok {
					spenders[i] = append(spenders[i], *sd.TxID)
				}
			}
		}
		return nil
	}); err != nil {
		return nil, err
	}
	for _, children := range spenders {
		outside.add(children...)
	}
	var failures []error

	if err := s.Unspend(ctx, cloneConflictSpends(r.WinningSpends)); err != nil {
		failures = append(failures, err)
	}

	displaced, leave, release, err := displacedConflictOwners(ctx, s, r, known)
	if err != nil {
		return nil, err
	}

	if len(release) > 0 {
		if err := s.Unspend(ctx, release); err != nil {
			failures = append(failures, err)
		}
	}

	var restore []*Spend

	for i, original := range r.OriginalSpends {
		if original.SpendingData != nil && !leave[i] {
			restore = append(restore, original.Clone())
		}
	}

	if err := s.RestoreConflictSpends(ctx, restore, height); err != nil {
		failures = append(failures, err)
	}

	// Verify before making any loser usable again. A failed restoration keeps
	// the cascade conflicting and the checklist pending.
	mismatches := make([]error, len(r.OriginalSpends))
	verifyErr := forEachConflictRead(ctx, len(r.OriginalSpends), func(ctx context.Context, i int) error {
		original := r.OriginalSpends[i]
		if leave[i] {
			return nil
		}

		owned, err := conflictOwnerMatches(ctx, s, original)
		if err != nil {
			mismatches[i] = err
		} else if !owned {
			mismatches[i] = errors.NewProcessingError("conflict rollback ownership mismatch at %s:%d", original.TxID, original.Vout)
		}

		// Collect every mismatch: one failure must not cancel the other checks.
		return nil
	})

	// An outpoint whose read never ran is unverified, not a match.
	if verifyErr != nil {
		failures = append(failures, verifyErr)
	}
	for _, mismatch := range mismatches {
		if mismatch != nil {
			failures = append(failures, mismatch)
		}
	}
	if len(failures) != 0 {
		return nil, capRollbackFailures(failures)
	}
	endsConflicting := make(map[chainhash.Hash]struct{}, len(r.Transactions))
	for _, value := range []bool{true, false} {
		var hashes []chainhash.Hash

		for _, tx := range r.Transactions {
			if (tx.Conflicting || displaced.has(tx.Hash)) == value {
				hashes = append(hashes, tx.Hash)
				if value {
					endsConflicting[tx.Hash] = struct{}{}
				}
			}
		}
		if len(hashes) != 0 {
			if _, _, err := s.SetConflicting(ctx, hashes, value); err != nil {
				return nil, err
			}
		}
	}
	// A child of a transaction that ends Conflicting spends a conflicting output.
	var spendsConflicting []chainhash.Hash

	for _, child := range outside.order {
		m, err := s.Get(ctx, &child, fields.TxInpoints)
		if err != nil {
			return nil, err
		}
		if m == nil {
			continue
		}
		for _, parent := range m.TxInpoints.GetParentTxHashes() {
			if _, ok := endsConflicting[parent]; ok {
				spendsConflicting = append(spendsConflicting, child)
				break
			}
		}
	}
	if len(spendsConflicting) > 0 {
		if _, cascaded, err = MarkConflictingRecursively(ctx, s, spendsConflicting); err != nil {
			return cascaded, err
		}
	}
	return cascaded, s.SetConflictLocks(ctx, id, r.Parents, false)
}

// displacedConflictOwners finds the inputs that the rollback must leave as they are, and the
// checklist transactions that cannot get their inputs back. The operation freed the inputs of its
// losers, and the rollback frees the inputs of its winners that were free at planning. The validator
// can give such a freed input to a new transaction before the rollback verifies it. That spend is
// legitimate, so the rollback leaves it, and leave marks the outpoint. A recorded owner that lost an
// input this way, and every checklist transaction that spends that owner's outputs, ends Conflicting
// and gets none of its inputs back. release lists their inputs that still point at them.
//
// A loser's input counts only when no winner spends it. The new owner must be outside the checklist
// and not Conflicting. Any other unexpected owner still fails the verification.
func displacedConflictOwners(ctx context.Context, s Store, r *ConflictRecovery, members map[chainhash.Hash]struct{}) (displaced hashSet, leave []bool, release []*Spend, err error) {
	winning := make(map[outpoint]struct{}, len(r.WinningSpends))
	for _, sp := range r.WinningSpends {
		winning[outpoint{*sp.TxID, sp.Vout}] = struct{}{}
	}

	owners := make([]*chainhash.Hash, len(r.OriginalSpends))
	taken := make([]bool, len(r.OriginalSpends))
	leave = make([]bool, len(r.OriginalSpends))

	if err := forEachConflictRead(ctx, len(r.OriginalSpends), func(ctx context.Context, i int) error {
		original := r.OriginalSpends[i]

		response, err := s.GetSpend(ctx, original)
		if err != nil {
			return err
		}

		if response == nil || response.SpendingData == nil || response.SpendingData.TxID == nil {
			return nil
		}

		owner := *response.SpendingData.TxID
		owners[i] = &owner

		if original.SpendingData == nil {
			// A winning input that was free at planning. The rollback gives it back, and a
			// transaction outside the checklist can take it before the verification.
			if _, member := members[owner]; member {
				return nil
			}

			m, err := s.Get(ctx, &owner, fields.Conflicting)
			if errors.Is(err, errors.ErrTxNotFound) || errors.Is(err, errors.ErrNotFound) {
				return nil
			}

			if err != nil {
				return err
			}

			leave[i] = m != nil && !m.Conflicting

			return nil
		}

		if _, ok := winning[outpoint{*original.TxID, original.Vout}]; ok || owner.IsEqual(original.SpendingData.TxID) {
			return nil
		}

		if _, member := members[owner]; member {
			return nil
		}

		m, err := s.Get(ctx, &owner, fields.Conflicting)
		// A spend can precede its owner's Create. Leave a missing owner to the
		// verification pass so the remaining original spends are still restored.
		if errors.Is(err, errors.ErrTxNotFound) || errors.Is(err, errors.ErrNotFound) {
			return nil
		}
		if err != nil {
			return err
		}

		taken[i] = m != nil && !m.Conflicting

		return nil
	}); err != nil {
		return displaced, nil, nil, err
	}

	for i, original := range r.OriginalSpends {
		if taken[i] {
			displaced.add(*original.SpendingData.TxID)
		}
	}

	if len(displaced.order) == 0 {
		return displaced, leave, nil, nil
	}

	for changed := true; changed; {
		changed = false

		for _, original := range r.OriginalSpends {
			if original.SpendingData != nil && displaced.has(*original.TxID) && !displaced.has(*original.SpendingData.TxID) {
				displaced.add(*original.SpendingData.TxID)
				changed = true
			}
		}
	}

	for i, original := range r.OriginalSpends {
		if original.SpendingData == nil || !displaced.has(*original.SpendingData.TxID) {
			continue
		}

		leave[i] = true

		if owners[i] != nil && owners[i].IsEqual(original.SpendingData.TxID) {
			release = append(release, original.Clone())
		}
	}

	return displaced, leave, release, nil
}

func cloneConflictSpends(spends []*Spend) []*Spend {
	out := make([]*Spend, len(spends))
	for i, sp := range spends {
		out[i] = sp.Clone()
	}
	return out
}

func verifyConflictSpends(ctx context.Context, s Store, spends []*Spend) error {
	return forEachConflictRead(ctx, len(spends), func(ctx context.Context, i int) error {
		original := spends[i]

		owned, err := conflictOwnerMatches(ctx, s, original)
		if err != nil {
			return err
		}

		if !owned {
			return errors.NewProcessingError("conflict ownership changed at %s:%d", original.TxID, original.Vout)
		}

		return nil
	})
}

// conflictOwnerMatches reports whether the outpoint of original still has the spender the checklist recorded.
func conflictOwnerMatches(ctx context.Context, s Store, original *Spend) (bool, error) {
	got, err := s.GetSpend(ctx, original)
	if err != nil {
		return false, err
	}

	return got != nil && got.Status != int(Status_NOT_FOUND) && bytes.Equal(got.SpendingData.Bytes(), original.SpendingData.Bytes()), nil
}

// RollbackConflictIntent also handles stale forward intents after a reorg.
// Legacy records cannot establish the prior state and remain pending for repair.
//
// mayBeMarked lists every transaction whose Conflicting flag the rollback may have changed: the
// checklist transactions, and the children outside the checklist that it marked because their parent
// ends Conflicting. It is empty when the rollback stops before it changes a flag, for example when a
// guard refuses it. Callers evict these transactions from the txmeta cache, also on error.
func RollbackConflictIntent(ctx context.Context, s Store, intent ConflictIntent, guards ...CurrentChainGuard) (mayBeMarked []chainhash.Hash, err error) {
	r, err := s.LoadConflictRecovery(ctx, intent.IntentID())
	if err != nil {
		return nil, err
	}
	return rollbackConflictIntent(ctx, s, intent, r, guards...)
}

// rollbackConflictIntent is RollbackConflictIntent with the checklist already loaded.
func rollbackConflictIntent(ctx context.Context, s Store, intent ConflictIntent, r *ConflictRecovery, guards ...CurrentChainGuard) (mayBeMarked []chainhash.Hash, err error) {
	if r == nil {
		return nil, errors.NewProcessingError("legacy conflict intent %s has no recovery checklist; manual repair required", intent.IntentID().String())
	}
	var protected []*chainProtectedConflictStore

	for _, guard := range guards {
		if p := guard.protectForward(ctx, s, r.Winners, intent.BlockHash); p != nil {
			s = p
			protected = append(protected, p)
		}
	}
	if r.Phase == ConflictRecoveryRolledBack {
		return nil, nil
	}
	if r.Phase == ConflictRecoveryPlanning || r.Phase == ConflictRecoveryPrepared {
		// Nothing was mutated. Taking the locks is all or nothing, but a crash can stop the operation
		// before or after it took them, and another holder can have one. Release only what it owns.
		if err := s.ReleaseConflictLocks(ctx, intent.IntentID(), r.Parents); err != nil {
			return nil, err
		}
		r.Phase = ConflictRecoveryRolledBack
		return nil, s.SaveConflictRecovery(ctx, intent.IntentID(), r)
	}
	hashes := conflictTransactionHashes(r)

	for _, p := range protected {
		if err := p.checkUnconfirmed(ctx, hashes); err != nil {
			return nil, err
		}
	}
	// Do not compensate under a lock that another operation has taken over.
	if err := s.SetConflictLocks(ctx, intent.IntentID(), r.Parents, true); err != nil {
		return nil, err
	}
	r.Phase = ConflictRecoveryRollingBack
	if err = s.SaveConflictRecovery(ctx, intent.IntentID(), r); err != nil {
		return nil, err
	}
	cascaded, err := rollbackConflictRecovery(ctx, s, r, intent.BlockHeight, intent.IntentID())
	mayBeMarked = append(hashes, cascaded...)

	if err != nil {
		return mayBeMarked, err
	}
	r.Phase = ConflictRecoveryRolledBack
	return mayBeMarked, s.SaveConflictRecovery(ctx, intent.IntentID(), r)
}

func finishConflictRecovery(ctx context.Context, s Store, intent ConflictIntent, r *ConflictRecovery, deferCompletion bool) (txmap.TxMap, []chainhash.Hash, error) {
	if r.Phase != ConflictRecoveryFinished {
		if err := s.SetConflictLocks(ctx, intent.IntentID(), r.Parents, false); err != nil {
			return nil, nil, err
		}
		r.Phase = ConflictRecoveryFinished
		if err := s.SaveConflictRecovery(ctx, intent.IntentID(), r); err != nil {
			return nil, nil, err
		}
	}
	result := txmap.NewSplitSwissMap(len(r.Losers))
	for _, h := range r.Losers {
		_ = result.Put(h, 1)
	}
	if !deferCompletion {
		if err := CompleteConflictIntent(ctx, s, intent.IntentID()); err != nil {
			return nil, nil, err
		}
	}
	return result, r.Losers, nil
}

// CompleteConflictIntentsForBlock is called only after subtree and queue work
// for the block has succeeded. Prepared or failed operations remain pending.
func CompleteConflictIntentsForBlock(ctx context.Context, s Store, blockHash chainhash.Hash) error {
	return CompleteConflictIntentsForBlocks(ctx, s, []chainhash.Hash{blockHash})
}

// CompleteConflictIntentsForBlocks completes the finished forward intents of all the given
// blocks. A forward intent is keyed by its block, so this needs no scan of the WAL. The same rules
// apply as for CompleteConflictIntentsForBlock.
func CompleteConflictIntentsForBlocks(ctx context.Context, s Store, blockHashes []chainhash.Hash) error {
	for _, blockHash := range blockHashes {
		id := ForwardConflictIntentID(blockHash)
		r, err := s.LoadConflictRecovery(ctx, id)
		if err != nil {
			return err
		}
		if r != nil && r.Phase == ConflictRecoveryFinished {
			if err := CompleteConflictIntent(ctx, s, id); err != nil {
				return err
			}
		}
	}
	return nil
}
