package aerospike_test

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	as "github.com/bsv-blockchain/aerospike-client-go/v8"
	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	teranodeaerospike "github.com/bsv-blockchain/teranode/stores/utxo/aerospike"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/lane"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/bsv-blockchain/teranode/util/uaerospike"
	"github.com/stretchr/testify/require"
)

// laneWindowShape is the store a restart window read is measured against, scaled down: oldRecords
// written before the window, a margin of marginRecords, windowBlocks of perBlock records of which
// the last unminedBlocks are waiting to be mined (what today's reload reads), and touchedOld of the
// old records updated inside the window, as a spend of an old parent updates it.
type laneWindowShape struct {
	name          string
	perBlock      int
	windowBlocks  int
	unminedBlocks int
	oldRecords    int
	touchedOld    int
}

// The mainnet shape: 84,000 transactions a block, scaled by 1/84 to 1,000; a cap of 144 blocks;
// four windows of older records; and, as an upper bound, one older parent spent per window
// transaction. The five-minute margin is about half a block at a steady rate. The design-rate
// shape: a cap of 8 blocks against a retention of 10, scaled to 25,000 a block, its parents inside
// the window.
var laneWindowShapes = []laneWindowShape{
	{name: "mainnet", perBlock: 1_000, windowBlocks: 144, unminedBlocks: 1, oldRecords: 4 * 144 * 1_000, touchedOld: 144 * 1_000},
	{name: "design_rate", perBlock: 25_000, windowBlocks: 8, unminedBlocks: 1, oldRecords: 2 * 25_000},
}

// BenchmarkLaneRestartWindowRead loads an Aerospike at each shape through the store, then times
// today's reload read (the unmined iterator), the lanes restart window read, and a read whose
// window is empty, which is the primary index walk alone. Once each, warm.
//
//	go test -run '^$' -bench BenchmarkLaneRestartWindowRead -benchtime 1x -timeout 60m ./stores/utxo/aerospike/
func BenchmarkLaneRestartWindowRead(b *testing.B) {
	for _, shape := range laneWindowShapes {
		b.Run(shape.name, func(b *testing.B) {
			tSettings := test.CreateBaseTestSettings(b)
			tSettings.UtxoStore.LaneMigration = settings.LaneMigrationShadow

			store, _, deferFn := initAerospikeBench(b, tSettings, ulogger.TestLogger{})
			b.Cleanup(deferFn)

			benchLaneWindow(b, store.GetClient(), store, shape)
		})
	}
}

// benchLaneWindowWrite creates n transactions starting at seed, mined unless unmined, across 32
// writers.
func benchLaneWindowWrite(b *testing.B, store *teranodeaerospike.Store, seed *atomic.Uint32, n int, unmined bool) []*bt.Tx {
	ctx := context.Background()
	txs := make([]*bt.Tx, n)

	var (
		wg   sync.WaitGroup
		next atomic.Int64
		fail atomic.Value
	)

	for range 32 {
		wg.Add(1)

		go func() {
			defer wg.Done()

			for {
				i := next.Add(1) - 1
				if i >= int64(n) {
					return
				}

				s := seed.Add(1)

				tx := bt.NewTx()
				_ = tx.From("00000000000000000000000000000000000000000000000000000000000000cc", s,
					"76a914000000000000000000000000000000000000000088ac", 1_000_000)
				_ = tx.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 1_000)
				_ = tx.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 1_000)

				opts := []utxo.CreateOption{utxo.WithLanePosition(lane.Pack(1, uint64(s)))}
				if !unmined {
					opts = append(opts, utxo.WithMinedBlockInfo(utxo.MinedBlockInfo{BlockID: 7, BlockHeight: 100, OnLongestChain: true}))
				}

				if _, err := store.Create(ctx, tx, 100, opts...); err != nil {
					fail.Store(err)

					return
				}

				txs[i] = tx
			}
		}()
	}

	wg.Wait()

	if err, ok := fail.Load().(error); ok {
		require.NoError(b, err)
	}

	return txs
}

func benchLaneWindow(b *testing.B, client *uaerospike.Client, store *teranodeaerospike.Store, shape laneWindowShape) {
	ctx := context.Background()

	var seed atomic.Uint32

	load := time.Now()

	old := benchLaneWindowWrite(b, store, &seed, shape.oldRecords, false)

	time.Sleep(1100 * time.Millisecond)

	marginStart := time.Now()

	time.Sleep(10 * time.Millisecond)
	benchLaneWindowWrite(b, store, &seed, shape.perBlock/2, false)
	time.Sleep(1100 * time.Millisecond)

	from := time.Now()

	time.Sleep(10 * time.Millisecond)
	benchLaneWindowWrite(b, store, &seed, (shape.windowBlocks-shape.unminedBlocks)*shape.perBlock, false)
	benchLaneWindowWrite(b, store, &seed, shape.unminedBlocks*shape.perBlock, true)

	policy := as.NewWritePolicy(0, as.TTLDontUpdate)

	for _, tx := range old[:shape.touchedOld] {
		key, err := as.NewKey(store.GetNamespace(), store.GetName(), tx.TxIDChainHash()[:])
		require.NoError(b, err)
		require.NoError(b, client.Touch(policy, key))
	}

	total := shape.oldRecords + shape.perBlock/2 + shape.windowBlocks*shape.perBlock
	b.Logf("%s: loaded %d records in %s", shape.name, total, time.Since(load))

	b.ResetTimer()

	todayStart := time.Now()

	it, err := store.GetUnminedTxIterator()
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

	st, err := store.ReadLaneWindow(ctx, utxo.LaneWindow{From: from, Margin: from.Sub(marginStart)}, func([]utxo.LaneWindowRecord) error { return nil })
	require.NoError(b, err)

	noMargin, err := store.ReadLaneWindow(ctx, utxo.LaneWindow{From: from}, func([]utxo.LaneWindowRecord) error { return nil })
	require.NoError(b, err)

	walk, err := store.ReadLaneWindow(ctx, utxo.LaneWindow{From: time.Now().Add(time.Hour)}, func([]utxo.LaneWindowRecord) error { return nil })
	require.NoError(b, err)

	b.StopTimer()

	require.Equal(b, shape.unminedBlocks*shape.perBlock, today)
	require.Equal(b, uint64(shape.perBlock/2+shape.windowBlocks*shape.perBlock+shape.touchedOld), st.Records)
	require.Equal(b, uint64(shape.windowBlocks*shape.perBlock+shape.touchedOld), noMargin.Records)
	require.Zero(b, walk.Records)

	b.ReportMetric(float64(today), "today_records")
	b.ReportMetric(todayTook.Seconds(), "today_s")
	b.ReportMetric(float64(st.Records), "window_records")
	b.ReportMetric(st.Duration.Seconds(), "window_s")
	b.ReportMetric(float64(st.Bytes)/float64(st.Records), "window_bytes/record")
	b.ReportMetric(float64(st.Records-noMargin.Records), "margin_records")
	b.ReportMetric(float64(st.Records)/float64(today), "read_ratio")
	b.ReportMetric(walk.Duration.Seconds(), "index_walk_s")
	b.ReportMetric(float64(total)/walk.Duration.Seconds(), "index_entries/s")

	b.Logf("%s: %d records; today read %d in %s; window read %d (%d by the margin, %d old parents touched), %d bytes, in %s: ratio %.1f, %.0f records/s; empty window (index walk) %s, %.0f entries/s",
		shape.name, total, today, todayTook, st.Records, st.Records-noMargin.Records, shape.touchedOld, st.Bytes, st.Duration,
		float64(st.Records)/float64(today), float64(st.Records)/st.Duration.Seconds(), walk.Duration, float64(total)/walk.Duration.Seconds())
}
