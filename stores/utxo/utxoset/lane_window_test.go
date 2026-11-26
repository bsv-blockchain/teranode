package utxoset

import (
	"context"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/util/lane"
	"github.com/stretchr/testify/require"
)

// readWindow reads the restart window from fromHeight with a one-block margin and returns its
// records by hash.
func readWindow(t *testing.T, s *Store, ctx context.Context, fromHeight uint32) (map[chainhash.Hash]utxo.LaneWindowRecord, utxo.LaneWindowStats) {
	t.Helper()

	got := map[chainhash.Hash]utxo.LaneWindowRecord{}

	st, err := s.ReadLaneWindow(ctx, utxo.LaneWindow{FromHeight: fromHeight, MarginBlocks: 1}, func(batch []utxo.LaneWindowRecord) error {
		for _, r := range batch {
			_, dup := got[r.Hash]
			require.False(t, dup, "%s read twice", r.Hash)
			got[r.Hash] = r
		}

		return nil
	})
	require.NoError(t, err)

	return got, st
}

func createAt(t *testing.T, s *Store, ctx context.Context, sats uint64, height uint32, pos uint64) *bt.Tx {
	t.Helper()

	tx := mkTx(t, 2, sats)

	var opts []utxo.CreateOption
	if pos != lane.None {
		opts = append(opts, utxo.WithLanePosition(pos))
	}

	_, err := s.Create(ctx, tx, height, opts...)
	require.NoError(t, err)

	return tx
}

// The window is tx_body from the start height less the margin, then tx_body_carry, each with its
// identity row. An older parent a spend inside the window touched is not read: tx_body is
// partitioned by when a transaction was created, and a spend writes no body.
func TestReadLaneWindowReadsTheRowsCreatedInsideIt(t *testing.T) {
	s, ctx := newLaneStore(t)

	old := createAt(t, s, ctx, 1_001, 100, lane.Pack(3, 1))
	margin := createAt(t, s, ctx, 1_002, 199, lane.Pack(3, 2))
	inside := createAt(t, s, ctx, 1_003, 200, lane.Pack(3, 3))
	later := createAt(t, s, ctx, 1_004, 250, lane.Pack(4, 9))
	noPosition := createAt(t, s, ctx, 1_005, 210, lane.None)
	gone := createAt(t, s, ctx, 1_006, 220, lane.Pack(3, 5))
	dropIdentityRow(t, s, ctx, gone)

	loser := createAt(t, s, ctx, 1_007, 230, lane.Pack(3, 6))
	_, err := s.pool.Exec(ctx, `UPDATE tx_ident SET flags = flags | $2 WHERE txid = $1`, hashBytes(loser), FlagConflicting)
	require.NoError(t, err)

	coinbase := mkTx(t, 1, 1_009)
	_, err = s.Create(ctx, coinbase, 240, utxo.WithSetCoinbase(true), utxo.WithLanePosition(lane.Pack(3, 7)))
	require.NoError(t, err)

	// A spend of the old parent inside the window: the spender is read, the parent is not.
	child := spendOneOutput(t, s, ctx, old, 0, 260)

	// A transaction that waited past the body horizon has its body in tx_body_carry.
	carried := createAt(t, s, ctx, 1_008, 40, lane.Pack(5, 1))
	_, err = s.pool.Exec(ctx, `INSERT INTO tx_body_carry (txid, raw_tx) VALUES ($1, $2)`, hashBytes(carried), carried.Bytes())
	require.NoError(t, err)

	got, st := readWindow(t, s, ctx, 200)

	want := []*bt.Tx{margin, inside, later, noPosition, gone, loser, coinbase, child, carried}
	require.Len(t, got, len(want))
	require.Equal(t, uint64(len(want)), st.Records)
	require.NotContains(t, got, *old.TxIDChainHash(), "an old parent a spend touched is not read")

	for _, tx := range want {
		require.Contains(t, got, *tx.TxIDChainHash())
	}

	require.True(t, got[*margin.TxIDChainHash()].InMargin)
	require.Equal(t, uint64(1), st.MarginRecords)
	require.True(t, st.MarginKnown)
	require.False(t, got[*inside.TxIDChainHash()].InMargin)

	require.Equal(t, lane.Pack(3, 3), got[*inside.TxIDChainHash()].LanePosition)
	require.Equal(t, lane.Pack(4, 9), got[*later.TxIDChainHash()].LanePosition)
	require.Equal(t, lane.Pack(5, 1), got[*carried.TxIDChainHash()].LanePosition)
	require.Equal(t, lane.None, got[*noPosition.TxIDChainHash()].LanePosition)
	require.Positive(t, got[*inside.TxIDChainHash()].CreatedAt)

	require.True(t, got[*gone.TxIDChainHash()].Missing)
	require.False(t, got[*inside.TxIDChainHash()].Missing)
	require.True(t, got[*loser.TxIDChainHash()].Conflicting)
	require.False(t, got[*inside.TxIDChainHash()].Conflicting)
	require.True(t, got[*coinbase.TxIDChainHash()].Coinbase)
	require.False(t, got[*inside.TxIDChainHash()].Coinbase)
	require.Positive(t, st.Bytes)
}

// A window bigger than one page is read in pages, each row once.
func TestReadLaneWindowPagesThroughTheKey(t *testing.T) {
	s, ctx := newLaneStore(t)

	defer func(n int) { laneWindowPage = n }(laneWindowPage)

	laneWindowPage = 3

	for i := range 10 {
		createAt(t, s, ctx, uint64(2_000+i), 300+uint32(i%4), lane.Pack(1, uint64(i+1)))
	}

	for i := range 4 {
		carried := createAt(t, s, ctx, uint64(3_000+i), 10, lane.Pack(2, uint64(i+1)))
		_, err := s.pool.Exec(ctx, `INSERT INTO tx_body_carry (txid, raw_tx) VALUES ($1, $2)`, hashBytes(carried), carried.Bytes())
		require.NoError(t, err)
	}

	got, st := readWindow(t, s, ctx, 300)
	require.Len(t, got, 14)
	require.Equal(t, uint64(14), st.Records)
}

// Without lane_position on tx_ident the window cannot be judged, and the read says so.
func TestReadLaneWindowNeedsTheLaneColumn(t *testing.T) {
	s, ctx := newTestStore(t)

	_, err := s.ReadLaneWindow(ctx, utxo.LaneWindow{FromHeight: 1}, func([]utxo.LaneWindowRecord) error { return nil })
	require.Error(t, err)
}

// An error from the caller stops the read and is returned.
func TestReadLaneWindowStopsOnTheCallersError(t *testing.T) {
	s, ctx := newLaneStore(t)

	createAt(t, s, ctx, 4_001, 500, lane.Pack(1, 1))

	stop := context.Canceled

	_, err := s.ReadLaneWindow(ctx, utxo.LaneWindow{FromHeight: 500}, func([]utxo.LaneWindowRecord) error { return stop })
	require.ErrorIs(t, err, stop)
}
