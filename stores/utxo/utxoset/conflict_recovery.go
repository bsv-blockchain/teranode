package utxoset

import (
	"context"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	spendpkg "github.com/bsv-blockchain/teranode/stores/utxo/spend"
	"github.com/jackc/pgx/v5"
)

// A forward conflict resolution keeps its checklist (utxo.ConflictRecovery) on its own intent
// row, in the recovery column. The checklist is the before-image of every owner and flag the
// resolution changes, so a crash at any step can be rolled back to exactly that state.

const recoveryUpdateSQL = `UPDATE conflict_intents SET recovery = $2 WHERE intent_id = $1`

const recoverySelectSQL = `SELECT recovery FROM conflict_intents WHERE intent_id = $1`

// SaveConflictRecovery publishes the checklist and its phase in one write. The intent must
// already exist: a checklist with no intent row would never be replayed.
func (s *Store) SaveConflictRecovery(ctx context.Context, id chainhash.Hash, r *utxo.ConflictRecovery) error {
	data, err := utxo.EncodeConflictRecovery(r)
	if err != nil {
		return err
	}

	tag, err := s.pool.Exec(ctx, recoveryUpdateSQL, id[:], data)
	if err != nil {
		return errors.NewStorageError("[utxoset][SaveConflictRecovery] intent %s", id.String(), err)
	}

	if tag.RowsAffected() != 1 {
		return errors.NewStorageError("[utxoset][SaveConflictRecovery] conflict recovery intent %s is missing", id.String())
	}

	return nil
}

// LoadConflictRecovery returns the intent's checklist, and nil for an absent intent or one an
// older build wrote without a checklist.
func (s *Store) LoadConflictRecovery(ctx context.Context, id chainhash.Hash) (*utxo.ConflictRecovery, error) {
	var data []byte

	err := s.pool.QueryRow(ctx, recoverySelectSQL, id[:]).Scan(&data)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}

	if err != nil {
		return nil, errors.NewStorageError("[utxoset][LoadConflictRecovery] intent %s", id.String(), err)
	}

	return utxo.DecodeConflictRecovery(data)
}

// RestoreConflictSpends spends each recorded UTXO again for the spender the record names, and
// only that UTXO, through the store's one spend statement.
//
// A UTXO that the named spender already took is accepted, which makes a replay idempotent. A
// UTXO that a different transaction took is refused with that transaction's ID, so a restore
// never overwrites an owner the checklist did not record. Conflicting and locked marks are
// waived, as for every spend conflict resolution makes: it marks the transactions itself.
//
// The records carry no UTXO hash here, and none is needed. This store finds a UTXO by its
// outpoint and authorises the spend by the full parent ID, so the record's outpoint and spender
// are the whole claim. A reassigned UTXO, whose only authentication is the new output, is
// refused (claimMismatch), on the first write and on a replay alike: a record presents no output.
//
// Every record is written in one database transaction. A refusal rolls back the others, so a
// failed restore leaves the store as it found it and the caller can retry or roll back.
func (s *Store) RestoreConflictSpends(ctx context.Context, spends []*utxo.Spend, height uint32) error {
	if len(spends) == 0 {
		return nil
	}

	for _, sp := range spends {
		if sp == nil || sp.TxID == nil || sp.SpendingData == nil || sp.SpendingData.TxID == nil {
			return errors.NewProcessingError("[utxoset][RestoreConflictSpends] invalid conflict restoration spend")
		}
	}

	if err := s.ensureSpendJournalPartition(ctx, height); err != nil {
		return err
	}

	plan := planRecordedSpends(spends, height, utxo.IgnoreFlags{IgnoreConflicting: true, IgnoreLocked: true})

	dbTx, err := s.pool.Begin(ctx)
	if err != nil {
		return errors.NewStorageError("[utxoset][RestoreConflictSpends] begin", err)
	}

	defer func() { _ = dbTx.Rollback(ctx) }()

	if err = s.runSpendPlan(ctx, dbTx, plan); err != nil {
		return err
	}

	for _, records := range plan.perItem {
		if sp := records[0]; sp.Err != nil {
			if sp.ConflictingTxID != nil {
				return errors.NewProcessingError("[utxoset][RestoreConflictSpends] %s:%d is spent by %s, not by the recorded %s",
					sp.TxID.String(), sp.Vout, sp.ConflictingTxID.String(), sp.SpendingData.TxID.String(), sp.Err)
			}

			return errors.NewProcessingError("[utxoset][RestoreConflictSpends] %s:%d cannot be restored", sp.TxID.String(), sp.Vout, sp.Err)
		}
	}

	if err = dbTx.Commit(ctx); err != nil {
		return errors.NewStorageError("[utxoset][RestoreConflictSpends] commit", err)
	}

	return nil
}

// planRecordedSpends is planSpends for spend records rather than transactions: one item per
// record, with no transaction, so nothing is decorated and the record itself is the answer.
func planRecordedSpends(spends []*utxo.Spend, height uint32, flags utxo.IgnoreFlags) *spendPlan {
	n := len(spends)
	mask := spendGuardMask(flags)

	p := &spendPlan{
		leaves:    make([]int16, 0, n),
		ukeys:     make([][16]byte, 0, n),
		txids:     make([][]byte, 0, n),
		idx:       make([]int32, 0, n),
		heights:   make([]int32, 0, n),
		spenders:  make([][]byte, 0, n),
		masks:     make([]int16, 0, n),
		owner:     make([]int, 0, n),
		ownerVin:  make([]int, 0, n),
		vins:      make([]int32, 0, n),
		perItem:   make([][]*utxo.Spend, n),
		itemTxs:   make([]*bt.Tx, n),
		skipClaim: make([]bool, n),
	}

	for i, sp := range spends {
		parent := *sp.TxID
		spender := *sp.SpendingData.TxID

		p.perItem[i] = []*utxo.Spend{{
			TxID:         &parent,
			Vout:         sp.Vout,
			UTXOHash:     sp.UTXOHash,
			SpendingData: spendpkg.NewSpendingData(&spender, sp.SpendingData.Vin),
		}}

		p.leaves = append(p.leaves, LeafFor(parent[:]))
		p.ukeys = append(p.ukeys, Pack(parent[:], sp.Vout))
		p.txids = append(p.txids, parent[:])
		p.idx = append(p.idx, int32(i))              //nolint:gosec // bounded by the record count
		p.heights = append(p.heights, int32(height)) //nolint:gosec // block heights are far below 2^31
		p.spenders = append(p.spenders, spender[:])
		p.masks = append(p.masks, mask)
		p.owner = append(p.owner, i)
		p.ownerVin = append(p.ownerVin, 0)
		p.vins = append(p.vins, int32(sp.SpendingData.Vin)) //nolint:gosec // an input index fits int32
	}

	p.sortRows()

	return p
}

// SetConflictLocks is a no-op. A conflict resolution's Unspend here never locks the parents it
// restores (the forward path passes no lock flag), so the operation owns no parent lock to take
// or release, and touching the locked flag would release a lock that validation holds.
func (s *Store) SetConflictLocks(_ context.Context, _ chainhash.Hash, _ []chainhash.Hash, _ bool) error {
	return nil
}

// ReleaseConflictLocks is a no-op for the same reason as SetConflictLocks.
func (s *Store) ReleaseConflictLocks(_ context.Context, _ chainhash.Hash, _ []chainhash.Hash) error {
	return nil
}
