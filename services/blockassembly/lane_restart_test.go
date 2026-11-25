package blockassembly

import (
	"context"
	"net/url"
	"reflect"
	"testing"
	"time"

	"github.com/RoaringBitmap/roaring/v2/roaring64"
	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	utxoaerospike "github.com/bsv-blockchain/teranode/stores/utxo/aerospike"
	"github.com/bsv-blockchain/teranode/stores/utxo/utxoset"
	testAerospike "github.com/bsv-blockchain/teranode/test/utils/aerospike"
	"github.com/bsv-blockchain/teranode/test/utils/postgres"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/lane"
	"github.com/bsv-blockchain/teranode/util/lanereader"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/mock"
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

// TestLaneRestartDryRunCounts dry-runs a reset to block 1005 (tip 1010, cap 10, cap height 995) on
// each store and checks every count. Lane 1's first grant inside the cap starts at 15, lane 2's at
// 1,000; lane 9 has none. Each record's case is beside its write.
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
			// on utxoset, below its height less the margin. P is an old parent block 1003 took; B is
			// read by neither store.
			p := create(1, nil, 900, lane.Pack(1, 20))
			create(2, nil, 990, lane.Pack(1, 16))

			handedAt := time.Now().Add(laneRestartMargin + time.Second)

			time.Sleep(2 * time.Second)

			k := create(3, nil, 1000, lane.Pack(1, 30))  // unmined: keep, and in today's pool
			create(4, nil, 1000, lane.Pack(1, 35))       // unmined: keep, not in today's pool (lanes_only)
			x := create(5, nil, 1000, lane.Pack(1, 60))  // block 1008 took it, above the target: keep
			y := create(6, nil, 1000, lane.Pack(1, 61))  // block 1004 took it: mined_row, and in today's pool
			create(7, nil, 1000, lane.Pack(1, 5))        // below lane 1's floor: mined_floor before capped
			i := create(8, nil, 1000, lane.Pack(2, 500)) // an idle grant older than the cap: capped
			create(9, nil, 1000, lane.Pack(9, 5))        // a lane with no grant inside the cap: capped
			c := create(10, nil, 1000, lane.Pack(1, 40)) // a double-spend's loser: conflicting
			z := create(11, nil, 1000, lane.None)        // no_position, and in today's pool
			create(12, nil, 995, lane.Pack(1, 17))       // in the margin: keep, counted in utxoset's margin

			// The child spends P inside the window, which moves P's last-update time: Aerospike
			// reads P and drops it as mined, utxoset does not read it. The child is kept.
			createLaneTestTx(t, store, laneTestChild(t, p), 1000, lane.Pack(1, 45), false)

			_, _, err := store.SetConflicting(ctx, []chainhash.Hash{*c.TxIDChainHash()}, true)
			require.NoError(t, err)

			started := time.Now()

			time.Sleep(20 * time.Millisecond)
			create(13, nil, 1000, lane.Pack(1, 80)) // after today's reload began: after_reload

			m := laneTestTx(t, 14) // in today's pool and in no store: today_only_not_read

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

			requireLaneRestartPublished(t, res, map[string]float64{
				laneLastReadRecords:          float64(read),
				laneLastTodayReadRecords:     9,
				laneLastTodayKept:            6,
				laneLastWouldKeep:            5,
				laneLastReadRatio:            float64(read) / 9,
				laneLastCapBlocks:            11,
				laneLastCapBlocksWithoutRows: 9,
				laneLastSampleRate:           1,
			})
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

// The run's outcomes that need no store: no grant inside the cap, no view of the target's chain, a
// store that cannot read the window, a run cancelled during its read, and a sample rate above one.
func TestLaneRestartDryRunOutcomes(t *testing.T) {
	grants := func(context.Context, uint32) ([]lane.CapGrant, error) {
		return []lane.CapGrant{{Lane: 1, First: 15, Height: 900, HandedAt: time.Now()}}, nil
	}
	noView := func(context.Context, uint32, map[uint32]bool) *lane.ChainView { return nil }
	idx := laneTestIndex(t)
	view := func(_ context.Context, _ uint32, ids map[uint32]bool) *lane.ChainView {
		return idx.Snapshot().ViewOfIDs(ids)
	}
	tx := func(id uint32, pos uint64) *utxo.UnminedTransaction {
		return &utxo.UnminedTransaction{Node: &subtree.Node{Hash: laneTestHash(id)}, LanePosition: pos}
	}

	for _, tc := range []struct {
		name    string
		window  *fakeLaneWindow
		grants  func(context.Context, uint32) ([]lane.CapGrant, error)
		view    func(context.Context, uint32, map[uint32]bool) *lane.ChainView
		pool    []*utxo.UnminedTransaction
		rate    int // today's expected pool size, which sets the sample rate; 0 for the pool's size
		cancel  bool
		outcome string
		records map[string]uint64
		sample  map[string]uint64
	}{
		{
			// No position is inside the cap: the window is not read, and today's positioned pool
			// is capped.
			name:    "no grant inside the cap",
			window:  &fakeLaneWindow{},
			grants:  func(context.Context, uint32) ([]lane.CapGrant, error) { return nil, nil },
			view:    func(context.Context, uint32, map[uint32]bool) *lane.ChainView { panic("no view is needed") },
			pool:    []*utxo.UnminedTransaction{tx(1, lane.Pack(1, 5)), tx(2, lane.None)},
			outcome: laneRunNoGrant,
			records: map[string]uint64{},
			sample:  map[string]uint64{laneSampleTodayCapped: 1, laneSampleTodayNoPosition: 1},
		},
		{
			// A positioned record is not judged without a view of the target's chain.
			name: "no view",
			window: &fakeLaneWindow{records: []utxo.LaneWindowRecord{
				{Hash: laneTestHash(1), LanePosition: lane.Pack(1, 30), CreatedAt: 1},
				{Hash: laneTestHash(2), CreatedAt: 1},
				{Hash: laneTestHash(3), NotMaster: true},
				{Hash: laneTestHash(4), Missing: true},
			}},
			grants:  grants,
			view:    noView,
			outcome: laneRunDone,
			records: map[string]uint64{laneRecNotReady: 1, laneRecNoPosition: 1, laneRecNotMaster: 1, laneRecMissing: 1},
			sample:  map[string]uint64{},
		},
		{
			name:    "a store that cannot read the window",
			outcome: laneRunUnsupported,
			records: map[string]uint64{},
			sample:  map[string]uint64{},
		},
		{
			name:    "cancelled during the read, as the next reload cancels it",
			window:  &fakeLaneWindow{block: true},
			grants:  grants,
			view:    noView,
			cancel:  true,
			outcome: laneRunCancelled,
			records: map[string]uint64{},
			sample:  map[string]uint64{},
		},
		{
			// At one in two only even IDs (laneTestHash puts the ID in the hash's low bytes) are in
			// either side's sample: kept record 3 and today's transaction 5 are outside it.
			name: "a sample rate of two",
			window: &fakeLaneWindow{records: []utxo.LaneWindowRecord{
				{Hash: laneTestHash(2), LanePosition: lane.Pack(1, 30), CreatedAt: 1},
				{Hash: laneTestHash(3), LanePosition: lane.Pack(1, 31), CreatedAt: 1},
			}},
			grants:  grants,
			view:    view,
			pool:    []*utxo.UnminedTransaction{tx(4, lane.Pack(1, 32)), tx(5, lane.Pack(1, 33))},
			rate:    laneRestartSampleTarget + 1,
			outcome: laneRunDone,
			records: map[string]uint64{laneRecKeep: 2},
			sample:  map[string]uint64{laneSampleLanesOnly: 1, laneSampleTodayNotRead: 1},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			if tc.cancel {
				time.AfterFunc(50*time.Millisecond, cancel)
			}

			today := &laneRestartToday{target: laneTestHash(1010), targetHeight: 1010, ids: laneTestIDs(900, 1010), started: time.Now()}
			today.noteRead(uint64(len(tc.pool)))
			today.expect(max(tc.rate, len(tc.pool)))
			today.keep(tc.pool)

			deps := laneRestartDeps{logger: ulogger.TestLogger{}, retention: 144, grants: tc.grants, view: tc.view}
			if tc.window != nil {
				deps.window = tc.window
			}

			res := runLaneRestartDryRun(ctx, deps, today)

			require.Equal(t, tc.outcome, res.outcome)
			require.Equal(t, tc.records, res.records)
			require.Equal(t, tc.sample, res.sample)

			if tc.outcome == laneRunNoGrant {
				require.False(t, tc.window.called, "no window is read")
			}
		})
	}
}

// laneRestartViewSource is a lane index source with one block, genesis, and no rows.
type laneRestartViewSource struct{ header *model.BlockHeader }

func (s laneRestartViewSource) GetBestBlockHeader(context.Context) (*model.BlockHeader, *model.BlockHeaderMeta, error) {
	return s.header, &model.BlockHeaderMeta{}, nil
}

func (s laneRestartViewSource) GetBlockHeaders(context.Context, *chainhash.Hash, uint64) ([]*model.BlockHeader, []*model.BlockHeaderMeta, error) {
	return []*model.BlockHeader{s.header}, []*model.BlockHeaderMeta{{}}, nil
}

func (laneRestartViewSource) LaneMapRows(context.Context, uint32) ([]lane.MapRow, error) {
	return nil, nil
}

// The production view waits for the process's lane index to reach the target's height and gives
// up at the deadline, so a run never judges against an index behind its target.
func TestLaneRestartReaderViewWaitsForTheTarget(t *testing.T) {
	defer func(d time.Duration) { laneRestartViewWait = d }(laneRestartViewWait)

	laneRestartViewWait = 1500 * time.Millisecond

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ts := test.CreateBaseTestSettings(t)
	ts.UtxoStore.LaneMigration = settings.LaneMigrationShadow

	genesis := ts.ChainCfgParams.GenesisBlock.Header
	header := &model.BlockHeader{
		Version:        uint32(genesis.Version), //nolint:gosec // a block version
		HashPrevBlock:  &chainhash.Hash{},
		HashMerkleRoot: &genesis.MerkleRoot,
		Timestamp:      uint32(genesis.Timestamp.Unix()), //nolint:gosec // a block time
		Bits:           model.NBit{},
		Nonce:          genesis.Nonce,
	}

	require.NotNil(t, lanereader.Start(ctx, ulogger.TestLogger{}, ts, laneRestartViewSource{header: header}))

	defer lanereader.Start(ctx, ulogger.TestLogger{}, test.CreateBaseTestSettings(t), nil)

	start := time.Now()
	require.Nil(t, laneRestartReaderView(ctx, 1, map[uint32]bool{0: true}), "the index's tip, height 0, is below the target")
	require.GreaterOrEqual(t, time.Since(start), laneRestartViewWait)

	v := laneRestartReaderView(ctx, 0, map[uint32]bool{0: true})
	require.NotNil(t, v)
	require.True(t, v.IDSet())

	cancel()
	require.Nil(t, laneRestartReaderView(ctx, 1, map[uint32]bool{0: true}), "a cancelled run stops waiting")
}

// A run from a reload reads its grants from the blockchain client and waits for the process's lane
// index, and judges with the UTXO store's window read where the store has one.
func TestLaneRestartDepsComeFromTheAssembler(t *testing.T) {
	ba, _, cleanup := setupDiskSortTest(t)
	defer cleanup()

	client := &blockchain.Mock{}
	client.On("LaneCapGrants", mock.Anything, uint32(7)).Return([]lane.CapGrant{{Lane: 3, First: 1}}, nil)
	ba.blockchainClient = client

	deps := ba.laneRestartDeps()

	got, err := deps.grants(context.Background(), 7)
	require.NoError(t, err)
	require.Equal(t, []lane.CapGrant{{Lane: 3, First: 1}}, got)
	client.AssertExpectations(t)

	require.Equal(t, reflect.ValueOf(laneRestartReaderView).Pointer(), reflect.ValueOf(deps.view).Pointer(), "the process's lane index")
	require.Nil(t, deps.window, "the SQL store cannot read the window")
	require.Equal(t, ba.settings.UtxoStore.UnminedTxRetention, deps.retention)
}

// requireLaneRestartPublished publishes res and checks the last-run gauges, and that the records and
// sample counters each rose by res's counts.
func requireLaneRestartPublished(t *testing.T, res laneRestartResult, last map[string]float64) {
	t.Helper()

	initLaneRestartMetrics()

	before := map[string]float64{}
	for _, o := range laneRecOutcomes {
		before["r"+o] = testutil.ToFloat64(prometheusLaneRestartRecords.WithLabelValues(o))
	}

	for _, k := range laneSampleKinds {
		before["s"+k] = testutil.ToFloat64(prometheusLaneRestartSample.WithLabelValues(k))
	}

	bytesBefore := testutil.ToFloat64(prometheusLaneRestartBytes)

	publishLaneRestart(res)

	for m, v := range last {
		require.InDelta(t, v, testutil.ToFloat64(prometheusLaneRestartLast.WithLabelValues(m)), 1e-9, m)
	}

	for _, o := range laneRecOutcomes {
		require.InDelta(t, float64(res.records[o]), testutil.ToFloat64(prometheusLaneRestartRecords.WithLabelValues(o))-before["r"+o], 1e-9, o)
	}

	for _, k := range laneSampleKinds {
		require.InDelta(t, float64(res.sample[k]), testutil.ToFloat64(prometheusLaneRestartSample.WithLabelValues(k))-before["s"+k], 1e-9, k)
	}

	require.InDelta(t, float64(res.stats.Bytes), testutil.ToFloat64(prometheusLaneRestartBytes)-bytesBefore, 1e-9)
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
