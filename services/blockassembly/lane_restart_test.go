package blockassembly

import (
	"context"
	"net/url"
	"testing"
	"time"

	"github.com/RoaringBitmap/roaring/v2/roaring64"
	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	utxoaerospike "github.com/bsv-blockchain/teranode/stores/utxo/aerospike"
	"github.com/bsv-blockchain/teranode/stores/utxo/utxoset"
	testAerospike "github.com/bsv-blockchain/teranode/test/utils/aerospike"
	"github.com/bsv-blockchain/teranode/test/utils/postgres"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/lane"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

// laneTestStore is a store the dry run can read the window of.
type laneTestStore interface {
	utxo.Store
	utxo.LaneWindowReader
}

func laneShadowSettings(t *testing.T) *settings.Settings {
	t.Helper()

	ts := test.CreateBaseTestSettings(t)
	ts.UtxoStore.LaneMigration = settings.LaneMigrationShadow

	return ts
}

func newLaneUtxosetStore(t *testing.T) laneTestStore {
	t.Helper()

	dsn, cleanup, err := postgres.SetupTestPostgresContainer()
	test.SkipIfContainerUnavailable(t, err)
	require.NoError(t, err)
	t.Cleanup(func() { _ = cleanup() })

	u, err := url.Parse(dsn)
	require.NoError(t, err)

	s, err := utxoset.New(context.Background(), ulogger.TestLogger{}, laneShadowSettings(t), u)
	require.NoError(t, err)

	t.Cleanup(func() { _ = s.Close(context.Background()) })

	return s
}

func newLaneAerospikeStore(t *testing.T) laneTestStore {
	t.Helper()

	aerospikeURL, cleanup, err := testAerospike.InitAerospikeContainer()
	test.SkipIfContainerUnavailable(t, err)
	require.NoError(t, err)
	t.Cleanup(func() { _ = cleanup() })

	u, err := url.Parse(aerospikeURL)
	require.NoError(t, err)

	s, err := utxoaerospike.New(context.Background(), ulogger.TestLogger{}, laneShadowSettings(t), u)
	require.NoError(t, err)

	return s
}

// laneTestTx is a distinct transaction with two outputs.
func laneTestTx(t *testing.T, seed uint32) *bt.Tx {
	t.Helper()

	tx := bt.NewTx()
	require.NoError(t, tx.From("00000000000000000000000000000000000000000000000000000000000000bb", seed,
		"76a914000000000000000000000000000000000000000088ac", 1_000_000))
	require.NoError(t, tx.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 1_000))
	require.NoError(t, tx.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 1_000))

	return tx
}

func laneTestChild(t *testing.T, parent *bt.Tx) *bt.Tx {
	t.Helper()

	child := bt.NewTx()
	require.NoError(t, child.FromUTXOs(&bt.UTXO{
		TxIDHash:      parent.TxIDChainHash(),
		Vout:          0,
		LockingScript: parent.Outputs[0].LockingScript,
		Satoshis:      parent.Outputs[0].Satoshis,
	}))
	require.NoError(t, child.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 500))

	return child
}

func laneTestHash(id uint32) chainhash.Hash {
	var h chainhash.Hash

	h[0], h[1], h[2], h[3], h[31] = byte(id), byte(id>>8), byte(id>>16), byte(id>>24), 0xab

	return h
}

// laneTestIndex is a lane index of main-chain blocks 900 to 1010, block ID equal to height, whose
// rows take: lane 1 positions 1 to 10 in block 905 (folded into the floor at tip 1010), 20 in
// block 1003, 61 in block 1004 and 60 in block 1008.
func laneTestIndex(t *testing.T) *lane.Index {
	t.Helper()

	idx := lane.NewIndex(lane.DefaultSettleDepth)

	takes := map[uint32][]uint64{905: {1, 2, 3, 4, 5, 6, 7, 8, 9, 10}, 1003: {20}, 1004: {61}, 1008: {60}}

	for h := uint32(900); h <= 1010; h++ {
		var rows []lane.MapRow

		if ps, ok := takes[h]; ok {
			var err error

			rows, err = lane.BuildMapRows(1, roaring64.BitmapOf(ps...))
			require.NoError(t, err)
		}

		require.NoError(t, idx.AddBlock(lane.IndexBlock{ID: h, Height: h, Hash: laneTestHash(h), Parent: laneTestHash(h - 1)}, rows))
	}

	require.NoError(t, idx.SetTip(laneTestHash(1010)))

	return idx
}

func laneTestIDs(from, to uint32) map[uint32]bool {
	ids := map[uint32]bool{}
	for h := from; h <= to; h++ {
		ids[h] = true
	}

	return ids
}

// TestLaneRestartDryRunCounts runs the dry run of a reset to block 1005 while the main chain's tip
// is 1010, with a cap of 10 blocks (cap height 995), on each store, and checks every count. Lane 1's
// first grant at or above 995 starts at 15; lane 2's at 1,000; lane 9 has none.
//
//   - K (1,30) and L (1,35) are unmined: kept. K is in today's pool, L is not (lanes_only).
//   - X (1,60) was taken by block 1008, above the target: kept, as today's reset reload keeps it.
//   - Y (1,61) was taken by block 1004: mined on the target's chain. Today's pool has it
//     (today_only_mined).
//   - F (1,5) is below lane 1's floor: mined_floor, though also below the lane's first grant.
//   - I (2,500) is from an idle grant older than the cap, written inside the window: capped. Today's
//     pool has it (today_only_capped).
//   - N (9,5) is in a lane with no grant inside the cap: capped.
//   - C (1,40) lost a double-spend: conflicting. Z has no position, and is in today's pool.
//   - E (1,17) is written inside the window's margin: kept, and on utxoset counted in the margin.
//   - P (1,20) is an old parent, taken by block 1003, whose output a child Ch (1,45) spends inside
//     the window. Aerospike reads P, because the spend moved its last-update time, and drops it as
//     mined; utxoset does not read it. Ch is kept.
//   - B (1,16) was written before the window and is read by neither store.
//   - A (1,80) was written after today's reload began: after_reload.
//   - M (1,70) is in today's pool and in no store: the window cannot have read it.
func TestLaneRestartDryRunCounts(t *testing.T) {
	if testing.Short() {
		t.Skip("needs Postgres and Aerospike containers")
	}

	for _, tc := range []struct {
		name  string
		store func(t *testing.T) laneTestStore
		// readsOldParent: the store reads P, whose last-update time the spend moved.
		readsOldParent bool
		marginKnown    bool
	}{
		{"utxoset", newLaneUtxosetStore, false, true},
		{"aerospike", newLaneAerospikeStore, true, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			store := tc.store(t)

			create := func(seed uint32, tx *bt.Tx, height uint32, pos uint64) *bt.Tx {
				if tx == nil {
					tx = laneTestTx(t, seed)
				}

				return createLaneTestTx(t, store, tx, height, pos, true)
			}

			// Before the window: on Aerospike, more than the margin before the first grant's time;
			// on utxoset, below its height less the margin.
			p := create(1, nil, 900, lane.Pack(1, 20))
			create(2, nil, 990, lane.Pack(1, 16))

			handedAt := time.Now().Add(laneRestartMargin + time.Second)

			time.Sleep(2 * time.Second)

			k := create(3, nil, 1000, lane.Pack(1, 30))
			l := create(4, nil, 1000, lane.Pack(1, 35))
			x := create(5, nil, 1000, lane.Pack(1, 60))
			y := create(6, nil, 1000, lane.Pack(1, 61))
			create(7, nil, 1000, lane.Pack(1, 5))
			i := create(8, nil, 1000, lane.Pack(2, 500))
			create(9, nil, 1000, lane.Pack(9, 5))
			c := create(10, nil, 1000, lane.Pack(1, 40))
			z := create(11, nil, 1000, lane.None)
			create(12, nil, 995, lane.Pack(1, 17))

			// The child spends P and is created in one write.
			createLaneTestTx(t, store, laneTestChild(t, p), 1000, lane.Pack(1, 45), false)

			_, _, err := store.SetConflicting(ctx, []chainhash.Hash{*c.TxIDChainHash()}, true)
			require.NoError(t, err)

			started := time.Now()

			time.Sleep(20 * time.Millisecond)
			create(13, nil, 1000, lane.Pack(1, 80))

			m := laneTestTx(t, 14)

			today := &laneRestartToday{target: laneTestHash(1005), targetHeight: 1005, ids: laneTestIDs(900, 1005), started: started}
			today.noteRead(9)

			var pool []*utxo.UnminedTransaction

			for _, kept := range []struct {
				tx  *bt.Tx
				pos uint64
			}{{k, lane.Pack(1, 30)}, {x, lane.Pack(1, 60)}, {y, lane.Pack(1, 61)}, {i, lane.Pack(2, 500)}, {z, lane.None}, {m, lane.Pack(1, 70)}} {
				pool = append(pool, &utxo.UnminedTransaction{Node: &subtree.Node{Hash: *kept.tx.TxIDChainHash()}, LanePosition: kept.pos})
			}

			today.expect(len(pool))
			today.keep(pool)

			idx := laneTestIndex(t)

			deps := laneRestartDeps{
				logger:    ulogger.TestLogger{},
				window:    store,
				retention: 10,
				grants: func(_ context.Context, height uint32) ([]lane.CapGrant, error) {
					require.Equal(t, uint32(995), height)

					return []lane.CapGrant{
						{Lane: 1, First: 15, Height: 996, HandedAt: handedAt},
						{Lane: 2, First: 1_000, Height: 998, HandedAt: handedAt.Add(time.Minute)},
					}, nil
				},
				view: func(_ context.Context, _ uint32, ids map[uint32]bool) *lane.ChainView {
					return idx.Snapshot().ViewOfIDs(ids)
				},
			}
			deps.lastUpdate, _ = utxo.As[utxo.LaneLastUpdateReader](store)

			res := runLaneRestartDryRun(ctx, deps, today)
			require.Equal(t, laneRunDone, res.outcome)

			minedRow := uint64(1)
			read := uint64(12)

			if tc.readsOldParent {
				minedRow++
				read++
			}

			require.Equal(t, map[string]uint64{
				laneRecKeep:        5,
				laneRecMinedRow:    minedRow,
				laneRecMinedFloor:  1,
				laneRecCapped:      2,
				laneRecConflicting: 1,
				laneRecNoPosition:  1,
				laneRecAfterReload: 1,
			}, res.records)
			require.Equal(t, read, res.stats.Records)
			require.Equal(t, tc.marginKnown, res.stats.MarginKnown)

			if tc.marginKnown {
				require.Equal(t, uint64(1), res.stats.MarginRecords, "E, written one block below the first grant")
			}

			require.Equal(t, map[string]uint64{
				laneSampleBoth:            2,
				laneSampleLanesOnly:       3,
				laneSampleTodayMined:      1,
				laneSampleTodayCapped:     1,
				laneSampleTodayNoPosition: 1,
				laneSampleTodayNotRead:    1,
			}, res.sample)

			require.Equal(t, uint64(9), res.todayRead)
			require.Equal(t, uint64(6), res.todayKept)
			require.Equal(t, uint64(1), res.sampleRate)
			require.Equal(t, handedAt, res.window.From)
			require.Equal(t, uint32(996), res.window.FromHeight)

			// Blocks 995 to 1005; only 1003 and 1004 took positions.
			require.Equal(t, uint64(11), res.capBlocks)
			require.Equal(t, uint64(9), res.capBlocksWithoutRows)

			_ = l
		})
	}
}

// createLaneTestTx writes tx at height with lane position pos, spending its inputs unless
// createOnly.
func createLaneTestTx(t *testing.T, store utxo.Store, tx *bt.Tx, height uint32, pos uint64, createOnly bool) *bt.Tx {
	t.Helper()

	var opts []utxo.CreateOption
	if pos != lane.None {
		opts = append(opts, utxo.WithLanePosition(pos))
	}

	if createOnly {
		opts = append(opts, utxo.WithCreateOnly())
	}

	_, _, err := store.SpendAndCreate(context.Background(), tx, height, opts...)
	require.NoError(t, err)

	return tx
}

// fakeLaneWindow is a window reader for the cases that need no store.
type fakeLaneWindow struct {
	records []utxo.LaneWindowRecord
	called  bool
	block   bool
}

func (f *fakeLaneWindow) ReadLaneWindow(ctx context.Context, _ utxo.LaneWindow, fn func([]utxo.LaneWindowRecord) error) (utxo.LaneWindowStats, error) {
	f.called = true

	if f.block {
		<-ctx.Done()

		return utxo.LaneWindowStats{}, ctx.Err()
	}

	return utxo.LaneWindowStats{Records: uint64(len(f.records))}, fn(f.records)
}

func laneTestToday(t *testing.T, pool ...*utxo.UnminedTransaction) *laneRestartToday {
	t.Helper()

	today := &laneRestartToday{target: laneTestHash(1010), targetHeight: 1010, ids: laneTestIDs(900, 1010), started: time.Now()}
	today.noteRead(uint64(len(pool)))
	today.expect(len(pool))
	today.keep(pool)

	return today
}

// With no grant handed out inside the cap no position is inside it: the window is not read, and
// today's positioned pool is capped.
func TestLaneRestartDryRunWithNoQualifyingGrant(t *testing.T) {
	window := &fakeLaneWindow{}

	positioned := &utxo.UnminedTransaction{Node: &subtree.Node{Hash: laneTestHash(1)}, LanePosition: lane.Pack(1, 5)}
	unpositioned := &utxo.UnminedTransaction{Node: &subtree.Node{Hash: laneTestHash(2)}}

	res := runLaneRestartDryRun(context.Background(), laneRestartDeps{
		logger:    ulogger.TestLogger{},
		window:    window,
		retention: 144,
		grants:    func(context.Context, uint32) ([]lane.CapGrant, error) { return nil, nil },
		view:      func(context.Context, uint32, map[uint32]bool) *lane.ChainView { panic("no view is needed") },
	}, laneTestToday(t, positioned, unpositioned))

	require.Equal(t, laneRunNoGrant, res.outcome)
	require.False(t, window.called)
	require.Equal(t, map[string]uint64{laneSampleTodayCapped: 1, laneSampleTodayNoPosition: 1}, res.sample)
}

// Without a view of the target's chain a positioned record is not judged, and is counted
// not_ready.
func TestLaneRestartDryRunWithoutAView(t *testing.T) {
	window := &fakeLaneWindow{records: []utxo.LaneWindowRecord{
		{Hash: laneTestHash(1), LanePosition: lane.Pack(1, 30), CreatedAt: 1},
		{Hash: laneTestHash(2), CreatedAt: 1},
		{Hash: laneTestHash(3), NotMaster: true},
		{Hash: laneTestHash(4), Missing: true},
	}}

	res := runLaneRestartDryRun(context.Background(), laneRestartDeps{
		logger:    ulogger.TestLogger{},
		window:    window,
		retention: 144,
		grants: func(context.Context, uint32) ([]lane.CapGrant, error) {
			return []lane.CapGrant{{Lane: 1, First: 1, Height: 900, HandedAt: time.Now()}}, nil
		},
		view: func(context.Context, uint32, map[uint32]bool) *lane.ChainView { return nil },
	}, laneTestToday(t))

	require.Equal(t, laneRunDone, res.outcome)
	require.Equal(t, map[string]uint64{laneRecNotReady: 1, laneRecNoPosition: 1, laneRecNotMaster: 1, laneRecMissing: 1}, res.records)
}

// A store that cannot read the window is counted, and nothing is read.
func TestLaneRestartDryRunUnsupportedStore(t *testing.T) {
	res := runLaneRestartDryRun(context.Background(), laneRestartDeps{logger: ulogger.TestLogger{}}, laneTestToday(t))
	require.Equal(t, laneRunUnsupported, res.outcome)
}

// A run cancelled during its read, as the next reload cancels it, is counted cancelled.
func TestLaneRestartDryRunCancelled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	window := &fakeLaneWindow{block: true}

	go func() {
		time.Sleep(50 * time.Millisecond)
		cancel()
	}()

	res := runLaneRestartDryRun(ctx, laneRestartDeps{
		logger:    ulogger.TestLogger{},
		window:    window,
		retention: 144,
		grants: func(context.Context, uint32) ([]lane.CapGrant, error) {
			return []lane.CapGrant{{Lane: 1, First: 1, Height: 900, HandedAt: time.Now()}}, nil
		},
		view: func(context.Context, uint32, map[uint32]bool) *lane.ChainView { return nil },
	}, laneTestToday(t))

	require.Equal(t, laneRunCancelled, res.outcome)
}

// Both sides sample by the same rule, so a transaction absent from one sample is absent from that
// side, and the sample stays near its target however large the pool.
func TestLaneRestartSampleRate(t *testing.T) {
	today := &laneRestartToday{}
	today.expect(10 * laneRestartSampleTarget)
	require.Equal(t, uint64(10), today.rate)

	today.expect(laneRestartSampleTarget)
	require.Equal(t, uint64(1), today.rate)

	today.expect(laneRestartSampleTarget + 1)
	require.Equal(t, uint64(2), today.rate)
}

// At off a reload gathers nothing and starts no run; at shadow each reload path starts one with
// what it read and kept. The SQL store cannot read the window, so the run ends unsupported.
func TestLaneRestartDryRunStartsAfterAReloadOnlyAtShadow(t *testing.T) {
	initLaneRestartMetrics()

	for _, diskSort := range []bool{false, true} {
		for _, shadow := range []bool{false, true} {
			name := map[bool]string{false: "in memory", true: "disk sort"}[diskSort] + "/" + map[bool]string{false: "off", true: "shadow"}[shadow]

			t.Run(name, func(t *testing.T) {
				ba, utxoStore, cleanup := setupDiskSortTest(t)
				defer cleanup()

				ctx := context.Background()

				for _, tx := range generateTestTransactionsForDiskSort(t, 5) {
					_, _, err := utxoStore.SpendAndCreate(ctx, tx, 1, utxo.WithCreateOnly())
					require.NoError(t, err)
				}

				ba.settings.BlockAssembly.UnminedTxDiskSortEnabled = diskSort
				ba.settings.BlockAssembly.OnRestartValidateParentChain = false

				if shadow {
					ba.settings.UtxoStore.LaneMigration = settings.LaneMigrationShadow
				}

				before := testutil.ToFloat64(prometheusLaneRestartRuns.WithLabelValues(laneRunUnsupported))

				require.NoError(t, ba.loadUnminedTransactions(ctx, false))
				ba.stopLaneRestartDryRun()

				ba.laneRestart.mu.Lock()
				last := ba.laneRestart.last
				started := ba.laneRestart.cancel != nil
				ba.laneRestart.mu.Unlock()

				if !shadow {
					require.False(t, started, "no run at off")
					require.Nil(t, last)
					require.Equal(t, before, testutil.ToFloat64(prometheusLaneRestartRuns.WithLabelValues(laneRunUnsupported)))

					return
				}

				require.NotNil(t, last)
				require.Equal(t, laneRunUnsupported, last.outcome)
				require.Equal(t, uint64(5), last.todayRead)
				require.Equal(t, uint64(5), last.todayKept)
				require.Equal(t, before+1, testutil.ToFloat64(prometheusLaneRestartRuns.WithLabelValues(laneRunUnsupported)))
			})
		}
	}
}
