package teraslab

import (
	"context"
	"encoding/binary"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/spend"
	teraslab "github.com/icellan/teraslab/client/go"
	"github.com/stretchr/testify/require"
)

// TestSpendRejectsZeroBlockHeight pins the blockHeight==0 guard that both
// reference backends enforce (sql.go:1814, aerospike/spend.go:302). A height of
// 0 is a programming error; the store must fail loud rather than forward
// CurrentBlockHeight=0 to the server and run maturity/retention math against it.
// The guard must precede any client/batcher use, so a zero-value Store suffices.
func TestSpendRejectsZeroBlockHeight(t *testing.T) {
	s := &Store{}
	tx := bt.NewTx()

	_, err := s.Spend(context.Background(), tx, 0)
	require.Error(t, err, "blockHeight 0 must be rejected, matching the SQL/Aerospike backends")
	require.Contains(t, err.Error(), "blockHeight must be greater than zero")

	// A non-zero height with no inputs is the existing no-op success path, so the
	// guard is specific to height 0 rather than rejecting every call.
	_, err = s.Spend(context.Background(), tx, 1)
	require.NoError(t, err)
}

// TestFinalizeSpendResults covers the per-transaction mapping the spend batcher
// applies after a (possibly multi-tx) SpendBatch response: success block IDs and
// per-input error/conflicting-txid routing, plus the hasError / needsRollback
// decision that drives per-tx atomic rollback. This is the logic that must stay
// scoped to a single transaction even when many txs share one SpendBatch RPC.
func TestFinalizeSpendResults(t *testing.T) {
	mkResults := func(n int) []*utxo.Spend {
		r := make([]*utxo.Spend, n)
		for i := range r {
			h := chainhash.Hash{}
			h[0] = byte(i + 1)
			r[i] = &utxo.Spend{TxID: &h, Vout: uint32(i)}
		}
		return r
	}

	t.Run("all inputs succeed → no error, no rollback", func(t *testing.T) {
		res := mkResults(2)
		hasErr, rollback := finalizeSpendResults(res, nil)
		require.False(t, hasErr)
		require.False(t, rollback)
		require.NoError(t, res[0].Err)
		require.NoError(t, res[1].Err)
	})

	t.Run("double-spend on one input → ErrSpent + conflicting txid, needs rollback", func(t *testing.T) {
		res := mkResults(2)
		// conflicting spending data: txid in first 32 bytes, vin in [32:36]
		conflictTxID := &chainhash.Hash{}
		conflictTxID[0] = 0xAB
		var sd teraslab.SpendingData
		copy(sd[:32], conflictTxID[:])
		binary.LittleEndian.PutUint32(sd[32:36], 3)

		errs := map[int]*teraslab.BatchItemError{
			1: {ItemIndex: 1, Code: teraslab.ErrCodeAlreadySpent, Data: sd[:]},
		}
		hasErr, rollback := finalizeSpendResults(res, errs)

		require.True(t, hasErr)
		require.True(t, rollback, "AlreadySpent must trigger rollback of the sibling input")
		// sibling input 0 succeeded
		require.Nil(t, res[0].Err)
		// input 1 failed with ErrSpent + conflicting txid
		require.Error(t, res[1].Err)
		require.ErrorIs(t, res[1].Err, errors.ErrSpent)
		require.NotNil(t, res[1].ConflictingTxID)
		require.Equal(t, conflictTxID.String(), res[1].ConflictingTxID.String())
	})

	t.Run("transient/internal error → hasError but no rollback", func(t *testing.T) {
		res := mkResults(1)
		errs := map[int]*teraslab.BatchItemError{
			0: {ItemIndex: 0, Code: teraslab.ErrCodeInternal},
		}
		hasErr, rollback := finalizeSpendResults(res, errs)
		require.True(t, hasErr)
		require.False(t, rollback, "internal/transient errors must not roll back (idempotent retry)")
	})

	_ = spend.NewSpendingData
}
