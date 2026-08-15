package packedsql

import (
	"context"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	spendpkg "github.com/bsv-blockchain/teranode/stores/utxo/spend"
	"github.com/bsv-blockchain/teranode/util"
	"github.com/jackc/pgx/v5"
)

func (s *Store) GetCounterConflicting(ctx context.Context, txHash chainhash.Hash) ([]chainhash.Hash, error) {
	return utxo.GetCounterConflictingTxHashes(ctx, s, txHash, 0)
}

func (s *Store) GetConflictingChildren(ctx context.Context, txHash chainhash.Hash) ([]chainhash.Hash, error) {
	return utxo.GetConflictingChildren(ctx, s, txHash, s.settings.UtxoStore.ConflictingChildrenMaxNodes)
}

func (s *Store) SetConflicting(ctx context.Context, txHashes []chainhash.Hash, value bool) ([]*utxo.Spend, []chainhash.Hash, error) {
	var newDAH *int64

	if value && s.settings.GetUtxoStoreBlockHeightRetention() > 0 {
		dah := int64(s.GetBlockHeight()) + 1 + int64(s.settings.GetUtxoStoreBlockHeightRetention())
		newDAH = &dah
	}

	affectedParentSpends := make([]*utxo.Spend, 0, len(txHashes))
	spendingTxHashes := make([]chainhash.Hash, 0, len(txHashes))

	dbTx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return nil, nil, errors.NewStorageError("packedsql: failed to begin SetConflicting transaction", err)
	}

	defer func() { _ = dbTx.Rollback(ctx) }()

	for i := range txHashes {
		txHash := txHashes[i]

		md, err := s.Get(ctx, &txHash, fields.Tx, fields.Utxos)
		if err != nil {
			return nil, nil, err
		}

		if value {
			_, err = dbTx.Exec(ctx,
				`UPDATE packed_txs SET flags = flags | 4,
				   delete_at_height = CASE WHEN preserve_until IS NOT NULL THEN delete_at_height
				                           ELSE COALESCE(delete_at_height, $2::bigint) END
				 WHERE hash = $1`, txHash[:], newDAH)
		} else {
			_, err = dbTx.Exec(ctx,
				`UPDATE packed_txs SET flags = flags & ~4, delete_at_height = NULL WHERE hash = $1`, txHash[:])
		}

		if err != nil {
			return nil, nil, errors.NewStorageError("packedsql: failed to set conflicting flag for %s", txHash, err)
		}

		for _, input := range md.Tx.Inputs {
			if _, err = dbTx.Exec(ctx,
				`INSERT INTO conflicting_children (hash, child_hash, created_at) VALUES ($1, $2, $3) ON CONFLICT DO NOTHING`,
				input.PreviousTxIDChainHash()[:], txHash[:], md.CreatedAt); err != nil {
				return nil, nil, errors.NewStorageError("packedsql: failed to insert conflicting child for %s", txHash, err)
			}
		}

		for vin, input := range md.Tx.Inputs {
			utxoHash, err := util.UTXOHashFromInput(input)
			if err != nil {
				return nil, nil, err
			}

			affectedParentSpends = append(affectedParentSpends, &utxo.Spend{
				TxID:         input.PreviousTxIDChainHash(),
				Vout:         input.PreviousTxOutIndex,
				UTXOHash:     utxoHash,
				SpendingData: spendpkg.NewSpendingData(&txHash, vin),
			})
		}

		for _, sd := range md.SpendingDatas {
			if sd != nil && sd.TxID != nil && !sd.TxID.IsEqual(&subtree.FrozenBytesTxHash) {
				spendingTxHashes = append(spendingTxHashes, *sd.TxID)
			}
		}
	}

	if err = dbTx.Commit(ctx); err != nil {
		return nil, nil, errors.NewStorageError("packedsql: failed to commit SetConflicting transaction", err)
	}

	return affectedParentSpends, spendingTxHashes, nil
}

func (s *Store) RemoveFromConflictingChildren(ctx context.Context, removals []utxo.ConflictingChildRemoval) error {
	if len(removals) == 0 {
		return nil
	}

	dbTx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return errors.NewStorageError("packedsql: failed to begin RemoveFromConflictingChildren transaction", err)
	}

	defer func() { _ = dbTx.Rollback(ctx) }()

	for _, r := range removals {
		if r.ParentHash == nil || r.ChildHash == nil {
			return errors.NewInvalidArgumentError("packedsql: parent and child hash must be non-nil")
		}

		if _, err = dbTx.Exec(ctx,
			`DELETE FROM conflicting_children WHERE hash = $1 AND child_hash = $2`,
			r.ParentHash[:], r.ChildHash[:]); err != nil {
			return errors.NewStorageError("packedsql: failed to remove conflicting child", err)
		}
	}

	if err = dbTx.Commit(ctx); err != nil {
		return errors.NewStorageError("packedsql: failed to commit RemoveFromConflictingChildren transaction", err)
	}

	return nil
}

func (s *Store) SetLocked(ctx context.Context, txHashes []chainhash.Hash, value bool) error {
	if len(txHashes) == 0 {
		return nil
	}

	hashBytes := make([][]byte, len(txHashes))
	for i := range txHashes {
		hashBytes[i] = txHashes[i][:]
	}

	if value {
		if _, err := s.pool.Exec(ctx,
			`UPDATE packed_txs SET flags = flags | 8, delete_at_height = NULL WHERE hash = ANY($1::bytea[])`,
			hashBytes); err != nil {
			return errors.NewStorageError("packedsql: failed to set locked flag", err)
		}

		return nil
	}

	var newDAH *int64

	if retention := s.settings.GetUtxoStoreBlockHeightRetention(); retention > 0 {
		dah := int64(s.GetBlockHeight()) + 1 + int64(retention)
		newDAH = &dah
	}

	if _, err := s.pool.Exec(ctx,
		`UPDATE packed_txs SET flags = flags & ~8,
		   delete_at_height = CASE
		     WHEN $2::bigint IS NOT NULL AND preserve_until IS NULL AND (flags & 4) = 0
		          AND unmined_since IS NULL
		          AND octet_length(coalesce(block_refs, ''::bytea)) > 0
		          AND spent_count >= page0_count AND pages_spent >= pages_total
		     THEN $2 ELSE delete_at_height END
		 WHERE hash = ANY($1::bytea[])`,
		hashBytes, newDAH); err != nil {
		return errors.NewStorageError("packedsql: failed to clear locked flag", err)
	}

	return nil
}

func (s *Store) RemoveBlockIDs(ctx context.Context, removals []utxo.BlockIDsRemoval) error {
	if len(removals) == 0 {
		return nil
	}

	dbTx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return errors.NewStorageError("packedsql: failed to begin RemoveBlockIDs transaction", err)
	}

	defer func() { _ = dbTx.Rollback(ctx) }()

	for _, r := range removals {
		if r.TxHash == nil {
			return errors.NewInvalidArgumentError("packedsql: txHash must be non-nil")
		}

		var blockRefs []byte

		err = dbTx.QueryRow(ctx,
			`SELECT block_refs FROM packed_txs WHERE hash = $1 FOR UPDATE`, r.TxHash[:]).Scan(&blockRefs)
		if err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				continue
			}

			return errors.NewStorageError("packedsql: RemoveBlockIDs lookup failed for %s", r.TxHash, err)
		}

		for _, blockID := range r.BlockIDs {
			blockRefs = removeBlockRef(blockRefs, blockID)
		}

		if _, err = dbTx.Exec(ctx,
			`UPDATE packed_txs SET block_refs = $2 WHERE hash = $1`, r.TxHash[:], blockRefs); err != nil {
			return errors.NewStorageError("packedsql: RemoveBlockIDs update failed for %s", r.TxHash, err)
		}
	}

	if err = dbTx.Commit(ctx); err != nil {
		return errors.NewStorageError("packedsql: failed to commit RemoveBlockIDs transaction", err)
	}

	return nil
}
