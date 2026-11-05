package subtreeprocessor

import (
	"context"
	"testing"

	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/bsv-blockchain/teranode/util/cohort"
	"github.com/stretchr/testify/require"
)

// TestProcessCoinbaseUtxosCohortStamp checks the label processCoinbaseUtxos
// writes on the coinbase record, against a real sqlitememory UTXO store. With
// UtxoStore.CohortStamping on, the coinbase is created mined, so it carries the
// BornMined sentinel; with the flag off, the label stays Unset.
func TestProcessCoinbaseUtxosCohortStamp(t *testing.T) {
	for _, tc := range []struct {
		name     string
		stamping bool
		want     cohort.ID
	}{
		{name: "on", stamping: true, want: cohort.BornMined},
		{name: "off", stamping: false, want: cohort.Unset},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			stp, block := newMoveForwardCommitBoundaryProcessor(t)

			stp.settings.UtxoStore.CohortStamping = tc.stamping

			require.NoError(t, stp.processCoinbaseUtxos(ctx, block))

			got, err := stp.utxoStore.Get(ctx, block.CoinbaseTx.TxIDChainHash(), fields.Cohort)
			require.NoError(t, err)
			require.Equal(t, tc.want, cohort.ID(got.Cohort))
		})
	}
}
