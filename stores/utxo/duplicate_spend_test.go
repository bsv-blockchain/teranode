package utxo

import (
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	terrors "github.com/bsv-blockchain/teranode/errors"
	spendpkg "github.com/bsv-blockchain/teranode/stores/utxo/spend"
	"github.com/stretchr/testify/require"
)

type duplicateSpendTestItem struct {
	spend *Spend
	err   error
}

func TestFilterConflictingDuplicateSpendClaims(t *testing.T) {
	parentTxID := chainhash.HashH([]byte("parent"))
	utxoHash := chainhash.HashH([]byte("utxo"))
	winnerTxID := chainhash.HashH([]byte("winner"))
	loserTxID := chainhash.HashH([]byte("loser"))

	winner := &duplicateSpendTestItem{
		spend: &Spend{
			TxID:         &parentTxID,
			Vout:         3,
			UTXOHash:     &utxoHash,
			SpendingData: spendpkg.NewSpendingData(&winnerTxID, 0),
		},
	}
	idempotentDuplicate := &duplicateSpendTestItem{
		spend: &Spend{
			TxID:         &parentTxID,
			Vout:         3,
			UTXOHash:     &utxoHash,
			SpendingData: spendpkg.NewSpendingData(&winnerTxID, 0),
		},
	}
	loser := &duplicateSpendTestItem{
		spend: &Spend{
			TxID:         &parentTxID,
			Vout:         3,
			UTXOHash:     &utxoHash,
			SpendingData: spendpkg.NewSpendingData(&loserTxID, 0),
		},
	}

	filtered := FilterConflictingDuplicateSpendClaims(
		[]*duplicateSpendTestItem{winner, idempotentDuplicate, loser},
		func(item *duplicateSpendTestItem) *Spend {
			return item.spend
		},
		func(item *duplicateSpendTestItem, err error) {
			item.err = err
		},
	)

	require.Equal(t, []*duplicateSpendTestItem{winner, idempotentDuplicate}, filtered)
	require.NoError(t, winner.err)
	require.NoError(t, idempotentDuplicate.err)
	require.ErrorIs(t, loser.err, terrors.ErrSpent)

	var errData *terrors.UtxoSpentErrData
	var terr *terrors.Error
	require.ErrorAs(t, loser.err, &terr)
	require.True(t, terrors.AsData(terr, &errData))
	require.Equal(t, winner.spend.SpendingData.Bytes(), errData.SpendingData.Bytes())
}
