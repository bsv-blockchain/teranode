package aerospike_test

import (
	"context"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	teranodeaerospike "github.com/bsv-blockchain/teranode/stores/utxo/aerospike"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/lane"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/require"
)

// The restart window on Aerospike is every record last updated at or after its start, found by
// walking the primary index with the last-update time as a filter. A record written before the
// start is not returned; an old parent a spend inside the window updated is, and so is a page
// record written inside it, as not the transaction's own.
func TestReadLaneWindowFiltersOnLastUpdate(t *testing.T) {
	ctx := context.Background()
	tSettings := test.CreateBaseTestSettings(t)
	tSettings.UtxoStore.LaneMigration = settings.LaneMigrationShadow

	_, store, _, deferFn := initAerospike(t, tSettings, ulogger.NewErrorTestLogger(t))
	t.Cleanup(deferFn)

	untouched := laneParent(t, 11, 2)
	_, err := store.Create(ctx, untouched, 100, utxo.WithLanePosition(lane.Pack(3, 1)))
	require.NoError(t, err)

	spentParent := laneParent(t, 12, 2)
	_, err = store.Create(ctx, spentParent, 100, utxo.WithLanePosition(lane.Pack(3, 2)))
	require.NoError(t, err)

	// Last-update times are kept to the millisecond; a second either side keeps the clocks clear.
	time.Sleep(time.Second)

	from := time.Now()

	time.Sleep(time.Second)

	fresh := laneParent(t, 13, 2)
	_, err = store.Create(ctx, fresh, 101, utxo.WithLanePosition(lane.Pack(4, 7)))
	require.NoError(t, err)

	child := laneChild(t, spentParent, 0)
	_, err = store.Create(ctx, child, 101, utxo.WithLanePosition(lane.Pack(4, 8)))
	require.NoError(t, err)

	_, err = store.Spend(ctx, child, 101)
	require.NoError(t, err)

	paged := laneParent(t, 14, tSettings.UtxoStore.UtxoBatchSize+2)
	_, err = store.Create(ctx, paged, 101, utxo.WithLanePosition(lane.Pack(4, 9)))
	require.NoError(t, err)

	got := map[chainhash.Hash]utxo.LaneWindowRecord{}
	notMaster := 0

	st, err := store.ReadLaneWindow(ctx, utxo.LaneWindow{From: from}, func(batch []utxo.LaneWindowRecord) error {
		for _, r := range batch {
			if r.NotMaster {
				notMaster++

				continue
			}

			got[r.Hash] = r
		}

		return nil
	})
	require.NoError(t, err)

	require.NotContains(t, got, *untouched.TxIDChainHash(), "written before the window")
	require.Contains(t, got, *spentParent.TxIDChainHash(), "an old parent a spend inside the window updated")
	require.Contains(t, got, *fresh.TxIDChainHash())
	require.Contains(t, got, *child.TxIDChainHash())
	require.Contains(t, got, *paged.TxIDChainHash())
	require.Len(t, got, 4)
	require.Equal(t, 1, notMaster, "the page record of the paged transaction")
	require.Equal(t, uint64(5), st.Records)
	require.False(t, st.MarginKnown)
	require.Positive(t, st.Bytes)

	require.Equal(t, lane.Pack(3, 2), got[*spentParent.TxIDChainHash()].LanePosition)
	require.Equal(t, lane.Pack(4, 7), got[*fresh.TxIDChainHash()].LanePosition)
	require.Positive(t, got[*fresh.TxIDChainHash()].CreatedAt)

	lut, err := store.LaneLastUpdate(ctx, *spentParent.TxIDChainHash())
	require.NoError(t, err)
	require.True(t, lut.After(from), "the spend moved the parent's last-update time to %s", lut)

	lut, err = store.LaneLastUpdate(ctx, *untouched.TxIDChainHash())
	require.NoError(t, err)
	require.True(t, lut.Before(from), "%s", lut)

	// A margin reaching back before the first writes takes in every record.
	all := 0
	_, err = store.ReadLaneWindow(ctx, utxo.LaneWindow{From: from, Margin: time.Hour}, func(batch []utxo.LaneWindowRecord) error {
		all += len(batch)

		return nil
	})
	require.NoError(t, err)
	require.Equal(t, 6, all)
}

// A cancel during the read returns at once with the cancel, and hands the caller no more batches,
// though partition queries may still be walking the index.
func TestReadLaneWindowReturnsOnCancel(t *testing.T) {
	tSettings := test.CreateBaseTestSettings(t)
	tSettings.UtxoStore.LaneMigration = settings.LaneMigrationShadow

	_, store, _, deferFn := initAerospike(t, tSettings, ulogger.NewErrorTestLogger(t))
	t.Cleanup(deferFn)

	// Batches of four, so the read has many to hand over after the cancel.
	t.Cleanup(teranodeaerospike.SetLaneWindowBatch(4))

	from := time.Now().Add(-time.Second)

	for i := range 64 {
		_, err := store.Create(context.Background(), laneParent(t, uint32(100+i), 1), 101, utxo.WithLanePosition(lane.Pack(4, uint64(i+1)))) //nolint:gosec // a test seed
		require.NoError(t, err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	calls := 0

	_, err := store.ReadLaneWindow(ctx, utxo.LaneWindow{From: from}, func([]utxo.LaneWindowRecord) error {
		calls++

		// The partition queries fill the results buffer meanwhile, so batches are waiting when the
		// cancel comes.
		time.Sleep(200 * time.Millisecond)
		cancel()

		return nil
	})
	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, 1, calls, "no batch after the cancel")
}
