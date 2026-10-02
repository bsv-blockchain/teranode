package utxoset

import (
	"testing"

	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/stretchr/testify/require"
)

// TestCreateHonoursStateFlags closes a SILENT wrong answer, which is a worse class of
// bug than the unimplemented methods around it.
//
// createIn took its options as `_` and discarded them. CreateOptions carries Frozen,
// Conflicting and Locked; schema.go defines FlagFrozen, FlagConflicting and FlagLocked;
// and spend.go:198-200 checks all three when deciding whether a spend may proceed. Every
// piece was present except the wire from the option to the row, so a transaction created
// as conflicting got ordinary spendable outputs and every downstream guard passed
// quietly, because the bits it inspects were zero.
//
// An unimplemented method stops the node and gets noticed. This answered wrongly and
// carried on, which is how consensus bugs start. The store-agnostic conformance suite
// caught it; nothing else did, and that suite is switched off by default.
func TestCreateHonoursStateFlags(t *testing.T) {
	for _, tc := range []struct {
		name string
		opt  utxo.CreateOption
		bit  int16
	}{
		{"conflicting", utxo.WithConflicting(true), FlagConflicting},
		{"frozen", utxo.WithFrozen(true), FlagFrozen},
		{"locked", utxo.WithLocked(true), FlagLocked},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, ctx := newTestStore(t)

			tx := mkTx(t, 1, 4_000)
			_, err := s.Create(ctx, tx, 100, tc.opt)
			require.NoError(t, err)

			txid := tx.TxIDChainHash()

			var flags int16

			require.NoError(t, s.pool.QueryRow(ctx,
				`SELECT flags FROM utxo WHERE txid = $1`, txid[:]).Scan(&flags))

			require.NotZero(t, flags&tc.bit,
				"the %s option must reach the row, or every guard that reads it passes silently", tc.name)
		})
	}
}
