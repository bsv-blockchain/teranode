package blockvalidation

import (
	"context"
	"fmt"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/bsv-blockchain/teranode/util/cohort"
	"github.com/stretchr/testify/require"
)

// TestQuickValidateCohort_BlockCreatesAreBornMined: a transaction the quick path creates as part
// of a block is stored with the born-mined cohort when cohort stamping is on, through both the
// list write and the one-at-a-time fallback, and with no cohort when it is off. (issue 556)
func TestQuickValidateCohort_BlockCreatesAreBornMined(t *testing.T) {
	cases := []struct {
		name     string
		stamping bool
		oneByOne bool
		want     cohort.ID
	}{
		{name: "list, stamping on", stamping: true, want: cohort.BornMined},
		{name: "one by one, stamping on", stamping: true, oneByOne: true, want: cohort.BornMined},
		{name: "list, stamping off", want: cohort.Unset},
		{name: "one by one, stamping off", oneByOne: true, want: cohort.Unset},
	}

	for i, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			bv, recorder, cleanup := newListHarness(t, fmt.Sprintf("cohort_born_mined_%d", i), false)
			defer cleanup()

			bv.settings.UtxoStore.CohortStamping = tc.stamping

			ctx := context.Background()
			root, key := seedRoot(t, recorder.Store, 2, "COHORT_BORN_MINED_KEY")

			txs := []*bt.Tx{
				spendOf(t, key, root, 0, 90_000),
				spendOf(t, key, root, 1, 90_000),
			}

			block := &model.Block{Height: 100, ID: 42}
			subtreeIdxs := []int{0, 1}

			if tc.oneByOne {
				require.NoError(t, bv.applyOneByOne(ctx, block, txs, subtreeIdxs, false, false, func(*bt.Tx) {}))
			} else {
				txids := make([]chainhash.Hash, len(txs))
				for n, tx := range txs {
					txids[n] = *tx.TxIDChainHash()
				}

				require.NoError(t, bv.applyList(ctx, block, txs, txids, subtreeIdxs, false, false, func(*bt.Tx) {}))
			}

			for n, tx := range txs {
				md, err := recorder.Store.Get(ctx, tx.TxIDChainHash(), fields.Cohort)
				require.NoError(t, err, "tx %d must be in the store", n)
				require.Equal(t, uint32(tc.want), md.Cohort, "tx %d cohort", n)
			}
		})
	}
}
