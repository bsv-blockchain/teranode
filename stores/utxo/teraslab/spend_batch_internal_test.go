package teraslab

import (
	"encoding/binary"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/spend"
	teraslab "github.com/icellan/teraslab/client/go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

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

	t.Run("all inputs succeed → block IDs set, no error, no rollback", func(t *testing.T) {
		res := mkResults(2)
		hasErr, rollback := finalizeSpendResults(res,
			map[int][]uint32{0: {7}, 1: {7}}, nil)
		assert.False(t, hasErr)
		assert.False(t, rollback)
		assert.Equal(t, []uint32{7}, res[0].BlockIDs)
		assert.Equal(t, []uint32{7}, res[1].BlockIDs)
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
		hasErr, rollback := finalizeSpendResults(res, map[int][]uint32{0: {7}}, errs)

		assert.True(t, hasErr)
		assert.True(t, rollback, "AlreadySpent must trigger rollback of the sibling input")
		// sibling input 0 succeeded
		assert.Equal(t, []uint32{7}, res[0].BlockIDs)
		assert.Nil(t, res[0].Err)
		// input 1 failed with ErrSpent + conflicting txid
		require.Error(t, res[1].Err)
		assert.ErrorIs(t, res[1].Err, errors.ErrSpent)
		require.NotNil(t, res[1].ConflictingTxID)
		assert.Equal(t, conflictTxID.String(), res[1].ConflictingTxID.String())
	})

	t.Run("transient/internal error → hasError but no rollback", func(t *testing.T) {
		res := mkResults(1)
		errs := map[int]*teraslab.BatchItemError{
			0: {ItemIndex: 0, Code: teraslab.ErrCodeInternal},
		}
		hasErr, rollback := finalizeSpendResults(res, nil, errs)
		assert.True(t, hasErr)
		assert.False(t, rollback, "internal/transient errors must not roll back (idempotent retry)")
	})

	_ = spend.NewSpendingData
}
