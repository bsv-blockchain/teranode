package sql

import (
	"context"

	"github.com/bitcoin-sv/ubsv/errors"
	utxostore "github.com/bitcoin-sv/ubsv/stores/utxo"
)

func (s *Store) FreezeUTXOs(ctx context.Context, spends []*utxostore.Spend) error {
	txHashIDMap := make(map[string]int)

	// check whether the UTXOs are already spent or frozen
	for _, spend := range spends {
		q := `
            SELECT t.id, o.frozen, o.spending_transaction_id
            FROM outputs AS o, transactions AS t
            WHERE t.hash = $1
              AND o.transaction_id = t.id AND o.idx = $2
        `

		var id int

		var spendingTxID []byte

		var frozen bool

		if err := s.db.QueryRowContext(ctx, q, spend.TxID[:], spend.Vout).Scan(&id, &frozen, &spendingTxID); err != nil {
			return err
		}

		if spendingTxID != nil {
			return errors.NewSpentError("transaction %s:%d already spent by %s", spend.SpendingTxID, spend.Vout, spendingTxID)
		}

		if frozen {
			return errors.NewFrozenError("transaction %s:%d already frozen", spend.SpendingTxID, spend.Vout)
		}

		txHashIDMap[spend.TxID.String()] = id
	}

	// if not, freeze the UTXO
	for _, spend := range spends {
		id := txHashIDMap[spend.TxID.String()]

		q := `UPDATE outputs SET frozen = true WHERE transaction_id = $1 AND idx = $2 AND spending_transaction_id IS NULL`
		if _, err := s.db.ExecContext(ctx, q, id, spend.Vout); err != nil {
			return err
		}
	}

	return nil
}

func (s *Store) UnFreezeUTXOs(ctx context.Context, spends []*utxostore.Spend) error {
	txHashIDMap := make(map[string]int)

	// check whether the UTXOs are already spent or frozen
	for _, spend := range spends {
		q := `
            SELECT t.id, o.frozen
            FROM outputs AS o, transactions AS t
            WHERE t.hash = $1
              AND o.transaction_id = t.id AND o.idx = $2
        `

		var id int

		var frozen bool

		if err := s.db.QueryRowContext(ctx, q, spend.TxID[:], spend.Vout).Scan(&id, &frozen); err != nil {
			return err
		}

		if !frozen {
			return errors.NewFrozenError("transaction %s:%d is not frozen", spend.SpendingTxID, spend.Vout)
		}

		txHashIDMap[spend.TxID.String()] = id
	}

	for _, spend := range spends {
		id := txHashIDMap[spend.TxID.String()]

		q := `UPDATE outputs SET frozen = false WHERE transaction_id = $1 AND idx = $2 AND spending_transaction_id IS NULL AND frozen = true`
		if _, err := s.db.ExecContext(ctx, q, id, spend.Vout); err != nil {
			return err
		}
	}

	return nil
}

func (s *Store) ReAssignUTXO(ctx context.Context, utxo *utxostore.Spend, newUtxo *utxostore.Spend) error {
	return nil
}
