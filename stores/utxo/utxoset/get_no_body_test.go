package utxoset

import (
	"context"
	"testing"

	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/stretchr/testify/require"
)

// TestMetadataReadsLeaveTheBodyOut follows the parent-output fix. Get and BatchDecorate read
// and parsed the body of every transaction they answered, whatever the caller asked for, so a
// metadata read near the tip, where nearly every transaction still has its body, paid for the
// whole transaction. A read that names only metadata now leaves the body out, as the sql store
// does. A read that names the body, or no field, still reads it.
//
// The body is overwritten with bytes that do not decode: a read that touches it fails.
func TestMetadataReadsLeaveTheBodyOut(t *testing.T) {
	t.Run("direct", func(t *testing.T) {
		s, ctx := newTestStore(t)
		requireMetadataReadsLeaveTheBodyOut(t, s, ctx)
	})

	t.Run("through the get batcher", func(t *testing.T) {
		s, ctx := newTestStoreWith(t, func(ts *settings.Settings) { ts.UtxoStore.GetBatcherSize = 10 })
		require.NotNil(t, s.getBatcher)
		requireMetadataReadsLeaveTheBodyOut(t, s, ctx)
	})
}

func requireMetadataReadsLeaveTheBodyOut(t *testing.T, s *Store, ctx context.Context) {
	t.Helper()

	tx := mkTx(t, 2, 5_000)
	_, err := s.Create(ctx, tx, 100)
	require.NoError(t, err)

	h := tx.TxIDChainHash()

	tag, err := s.pool.Exec(ctx, `UPDATE tx_body SET raw_tx = '\xff'::bytea WHERE txid = $1`, h[:])
	require.NoError(t, err)
	require.Equal(t, int64(1), tag.RowsAffected())

	got, err := s.Get(ctx, h, fields.Fee, fields.SizeInBytes, fields.TxInpoints, fields.Conflicting, fields.BlockIDs)
	require.NoError(t, err, "a metadata read touched the body")
	require.Nil(t, got.Tx)
	require.Equal(t, uint64(tx.Size()), got.SizeInBytes)

	items := []*utxo.UnresolvedMetaData{{Hash: *h}}
	require.NoError(t, s.BatchDecorate(ctx, items, fields.Fee, fields.Conflicting, fields.BlockIDs))
	require.NoError(t, items[0].Err, "a metadata decorate touched the body")
	require.Nil(t, items[0].Data.Tx)

	_, err = s.Get(ctx, h, fields.Tx)
	require.Error(t, err, "a read that names the body still reads it")

	_, err = s.Get(ctx, h)
	require.Error(t, err, "a read that names no field still reads everything")

	items = []*utxo.UnresolvedMetaData{{Hash: *h}}
	require.NoError(t, s.BatchDecorate(ctx, items, fields.Outputs))
	require.Error(t, items[0].Err, "a decorate that names the outputs still reads the body")
}
