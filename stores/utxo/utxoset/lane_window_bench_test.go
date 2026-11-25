package utxoset

import (
	"context"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/stretchr/testify/require"
)

// laneWindowShape is the store a restart window read is measured against, scaled down: each block
// writes perBlock transactions, tx_body holds bodyBlocks of them, tx_ident also holds oldIdent rows
// of transactions whose bodies are gone, the window is windowBlocks (the cap) and the last
// unminedBlocks are waiting to be mined, which is what today's reload reads.
type laneWindowShape struct {
	name          string
	perBlock      int
	bodyBlocks    int
	windowBlocks  int
	unminedBlocks int
	oldIdent      int
}

// The mainnet shape: 140 transactions a second is about 84,000 a block; scaled by 1/42 to 2,000.
// Bodies are kept 288 blocks and the cap is 144. tx_ident holds four windows more of older
// transactions whose outputs are unspent. The design-rate shape: a cap of 8 blocks against a
// retention of 10, every block written at one rate, scaled to 50,000 a block. Both keep one block
// waiting.
var laneWindowShapes = []laneWindowShape{
	{name: "mainnet", perBlock: 2_000, bodyBlocks: 288, windowBlocks: 144, unminedBlocks: 1, oldIdent: 4 * 144 * 2_000},
	{name: "design_rate", perBlock: 50_000, bodyBlocks: 10, windowBlocks: 8, unminedBlocks: 1},
}

// BenchmarkLaneRestartWindowRead loads a store at each shape with SQL, then times today's reload
// read (the unmined iterator) and the lanes restart window read, once each, warm.
//
//	go test -run '^$' -bench BenchmarkLaneRestartWindowRead -benchtime 1x -timeout 60m ./stores/utxo/utxoset/
func BenchmarkLaneRestartWindowRead(b *testing.B) {
	for _, shape := range laneWindowShapes {
		b.Run(shape.name, func(b *testing.B) {
			s, ctx := benchStoreWith(b, func(ts *settings.Settings) { ts.UtxoStore.LaneMigration = settings.LaneMigrationShadow })

			benchLaneWindow(b, s, ctx, shape)
		})
	}
}

func benchLaneWindow(b *testing.B, s *Store, ctx context.Context, shape laneWindowShape) {
	const firstHeight = 10_000

	tip := uint32(firstHeight + shape.bodyBlocks - 1)

	inpoints := subtree.NewTxInpointsFromPacked([]chainhash.Hash{{1}}, []uint32{0})
	inpointBytes, err := inpoints.Serialize()
	require.NoError(b, err)

	load := time.Now()

	for h := uint32(firstHeight); h <= tip; h++ {
		require.NoError(b, s.ensureTxBodyPartition(ctx, h))

		unmined := h > tip-uint32(shape.unminedBlocks)

		_, err = s.pool.Exec(ctx, `
WITH t AS (
  SELECT i, decode(md5('w' || $1::int || '-' || i) || md5('x' || $1::int || '-' || i), 'hex') AS txid
    FROM generate_series(1, $2::int) i
), b AS (
  INSERT INTO tx_body (created_height, txid, raw_tx)
  SELECT $1, txid, decode(repeat('ab', 300), 'hex') FROM t
)
INSERT INTO tx_ident (leaf, txid, created_height, off_chain_since, fee, size_in_bytes, tx_inpoints, locktime,
                      created_at, flags, lane_position)
SELECT get_byte(txid, 0) & 7, txid, $1, CASE WHEN $3 THEN $1 END, 500, 300, $4, 0,
       $5::bigint + i, 0, ($1::bigint << 20) + i
  FROM t`, int32(h), shape.perBlock, unmined, inpointBytes, time.Now().UnixMilli()) //nolint:gosec // test heights
		require.NoError(b, err)
	}

	if shape.oldIdent > 0 {
		_, err = s.pool.Exec(ctx, `
INSERT INTO tx_ident (leaf, txid, created_height, off_chain_since, fee, size_in_bytes, tx_inpoints, locktime,
                      created_at, flags, lane_position)
SELECT get_byte(txid, 0) & 7, txid, 100, NULL, 500, 300, $2, 0, 0, 0, i
  FROM (SELECT i, decode(md5('o' || i) || md5('p' || i), 'hex') AS txid FROM generate_series(1, $1::int) i) o`,
			shape.oldIdent, inpointBytes)
		require.NoError(b, err)
	}

	_, err = s.pool.Exec(ctx, `VACUUM ANALYZE tx_body`)
	require.NoError(b, err)
	_, err = s.pool.Exec(ctx, `VACUUM ANALYZE tx_ident`)
	require.NoError(b, err)

	b.Logf("%s: loaded %d blocks of %d, %d old identity rows, in %s", shape.name, shape.bodyBlocks, shape.perBlock, shape.oldIdent, time.Since(load))

	b.ResetTimer()

	todayStart := time.Now()

	it, err := s.GetUnminedTxIterator()
	require.NoError(b, err)

	today := 0

	for {
		batch, err := it.Next(ctx)
		require.NoError(b, err)

		if batch == nil {
			break
		}

		today += len(batch)
	}

	require.NoError(b, it.Close())

	todayTook := time.Since(todayStart)

	// The first grant at or above the cap height was handed out at that height.
	from := tip - uint32(shape.windowBlocks) + 1

	st, err := s.ReadLaneWindow(ctx, utxo.LaneWindow{FromHeight: from, MarginBlocks: 1}, func([]utxo.LaneWindowRecord) error { return nil })
	require.NoError(b, err)

	b.StopTimer()

	require.Equal(b, shape.unminedBlocks*shape.perBlock, today)
	require.Equal(b, uint64((shape.windowBlocks+1)*shape.perBlock), st.Records)

	b.ReportMetric(float64(today), "today_records")
	b.ReportMetric(todayTook.Seconds(), "today_s")
	b.ReportMetric(float64(st.Records), "window_records")
	b.ReportMetric(st.Duration.Seconds(), "window_s")
	b.ReportMetric(float64(st.Bytes)/float64(st.Records), "window_bytes/record")
	b.ReportMetric(float64(st.MarginRecords), "margin_records")
	b.ReportMetric(float64(st.Records)/float64(today), "read_ratio")
	b.ReportMetric(float64(st.Records)/st.Duration.Seconds(), "window_records/s")

	b.Logf("%s: today read %d in %s; window read %d (%d in the margin), %d bytes, in %s: ratio %.1f, %.0f records/s",
		shape.name, today, todayTook, st.Records, st.MarginRecords, st.Bytes, st.Duration, float64(st.Records)/float64(today),
		float64(st.Records)/st.Duration.Seconds())
}
