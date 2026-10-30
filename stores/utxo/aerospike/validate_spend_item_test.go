package aerospike

import (
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	spendpkg "github.com/bsv-blockchain/teranode/stores/utxo/spend"
	"github.com/stretchr/testify/require"
)

// TestValidateSpendItemRequiresSpendingTxID: the replay-marker check on both
// spend paths is keyed on the spender's txid, and the expression path adds its
// marker clause only when it has one. A spend with spending data but no txid
// must be refused before it is dispatched, not written through unchecked.
func TestValidateSpendItemRequiresSpendingTxID(t *testing.T) {
	s := &Store{}
	parent := chainhash.HashH([]byte("parent"))
	spender := chainhash.HashH([]byte("spender"))

	for _, tc := range []struct {
		name         string
		spendingData *spendpkg.SpendingData
		wantErr      bool
	}{
		{name: "spending data with a txid", spendingData: spendpkg.NewSpendingData(&spender, 0)},
		{name: "no spending data", wantErr: true},
		{name: "spending data without a txid", spendingData: &spendpkg.SpendingData{Vin: 0}, wantErr: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := s.validateSpendItem(&batchSpend{spend: &utxo.Spend{TxID: &parent, SpendingData: tc.spendingData}})
			if tc.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
		})
	}
}
