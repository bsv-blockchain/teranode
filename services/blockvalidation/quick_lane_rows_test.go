package blockvalidation

import (
	"context"
	"fmt"
	"net/url"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	bec "github.com/bsv-blockchain/go-sdk/primitives/ec"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/settings"
	blobmemory "github.com/bsv-blockchain/teranode/stores/blob/memory"
	blockchain_store "github.com/bsv-blockchain/teranode/stores/blockchain"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	utxoaerospike "github.com/bsv-blockchain/teranode/stores/utxo/aerospike"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/bsv-blockchain/teranode/stores/utxo/meta"
	"github.com/bsv-blockchain/teranode/stores/utxo/utxoset"
	testAerospike "github.com/bsv-blockchain/teranode/test/utils/aerospike"
	tpostgres "github.com/bsv-blockchain/teranode/test/utils/postgres"
	"github.com/bsv-blockchain/teranode/test/utils/transactions"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/lane"
	"github.com/bsv-blockchain/teranode/util/lanereader"
	testutil "github.com/bsv-blockchain/teranode/util/test"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

// quickLaneSource hands out consecutive positions in one lane, as one grant does.
type quickLaneSource struct {
	lane uint8
	next atomic.Uint64
}

func (s *quickLaneSource) Next(context.Context) (uint64, error) {
	return lane.Pack(s.lane, s.next.Add(1)), nil
}

// quickLaneStore is a UTXO store the quick-validation lane tests run on. outpointOnly is the
// block's below-checkpoint mode on that store: utxoset nets the block (BV-QUICK-032); Aerospike
// does not support outpoint-only spends, so it takes the ordinary quick path.
type quickLaneStore struct {
	name         string
	outpointOnly bool
	open         func(t *testing.T, tSettings *settings.Settings) utxo.Store
}

func quickLaneStores() []quickLaneStore {
	return []quickLaneStore{
		{name: "utxoset", outpointOnly: true, open: openQuickLaneUtxoset},
		{name: "aerospike udf", open: func(t *testing.T, s *settings.Settings) utxo.Store { return openQuickLaneAerospike(t, s, false) }},
		{name: "aerospike native", open: func(t *testing.T, s *settings.Settings) utxo.Store { return openQuickLaneAerospike(t, s, true) }},
		{name: "aerospike one at a time", open: func(t *testing.T, s *settings.Settings) utxo.Store {
			return listRefusingStore{openQuickLaneAerospike(t, s, false)}
		}},
		{name: "aerospike partial retry", open: func(t *testing.T, s *settings.Settings) utxo.Store {
			return &flakyTxStore{Store: openQuickLaneAerospike(t, s, false), spendOnly: true}
		}},
	}
}

// listRefusingStore refuses every list, as a store refuses a malformed one, so quick validation
// applies the block one transaction at a time (applyOneByOne).
type listRefusingStore struct {
	utxo.Store
}

func (s listRefusingStore) Unwrap() utxo.Store { return s.Store }

func (listRefusingStore) SpendAndCreateMulti(_ context.Context, txs []*bt.Tx, _ uint32, _ ...utxo.CreateOption) ([]utxo.SpendAndCreateMultiResult, error) {
	_, err := utxo.PrepareSpendAndCreateMulti(append(txs, txs...))

	return nil, err
}

func openQuickLaneUtxoset(t *testing.T, tSettings *settings.Settings) utxo.Store {
	t.Helper()

	dsn, cleanup, err := tpostgres.SetupTestPostgresContainer()
	if err != nil {
		testutil.SkipIfContainerUnavailable(t, err)
		t.Fatalf("postgres container unavailable: %v", err)
	}

	t.Cleanup(func() { _ = cleanup() })

	storeURL, err := url.Parse(dsn)
	require.NoError(t, err)

	store, err := utxoset.New(context.Background(), ulogger.TestLogger{}, tSettings, storeURL)
	require.NoError(t, err)

	t.Cleanup(func() { _ = store.Close(context.Background()) })

	require.NoError(t, store.SetBlockHeight(100))

	return store
}

// openQuickLaneAerospike opens an Aerospike store on a fresh container. native asks for the fork
// server's native operations; on the stock image the store's probe falls back to the Lua UDFs, so
// AEROSPIKE_CONTAINER_IMAGE names the fork image to run them natively.
func openQuickLaneAerospike(t *testing.T, tSettings *settings.Settings, native bool) utxo.Store {
	t.Helper()

	aerospikeURL, cleanup, err := testAerospike.InitAerospikeContainer()
	testutil.SkipIfContainerUnavailable(t, err)
	require.NoError(t, err)

	t.Cleanup(func() { _ = cleanup() })

	u, err := url.Parse(aerospikeURL)
	require.NoError(t, err)

	tSettings.Aerospike.UseNativeTeranodeOps = native

	var store *utxoaerospike.Store

	// The namespace can refuse writes for a few seconds after the port opens.
	require.Eventually(t, func() bool {
		store, err = utxoaerospike.New(context.Background(), ulogger.TestLogger{}, tSettings, u)

		return err == nil
	}, time.Minute, time.Second, "open the aerospike store")

	store.SetExternalStore(blobmemory.New())

	require.NoError(t, store.SetBlockHeight(100))

	return store
}

// newQuickLaneHarness is a BlockValidation at shadow over store, which takes its positions from
// lane 7 of source, with a checkpoint above the fixture blocks.
func newQuickLaneHarness(t *testing.T, kind quickLaneStore) (*BlockValidation, utxo.Store, *quickLaneSource) {
	t.Helper()

	tSettings := quickTestSettings(t, true)
	tSettings.UtxoStore.LaneMigration = settings.LaneMigrationShadow

	store := kind.open(t, tSettings)

	source := &quickLaneSource{lane: 7}

	taker, ok := utxo.As[utxo.LanePositionTaker](store)
	require.True(t, ok)
	taker.SetLanePositionSource(source)

	bv := newQuickTestBlockValidation(t, tSettings, store)

	return bv, store, source
}

// quickLaneBlock is a block of two subtrees over root: subtree 0 holds the coinbase and a0 to a2,
// subtree 1 holds a3 and a4. a1 spends a0 and a3 spends a1, inside the block, so on utxoset their
// outputs are netted and never become coins; the others spend root.
func quickLaneBlock(t *testing.T, key *bec.PrivateKey, root *bt.Tx) ([][]*bt.Tx, []*bt.Tx, *model.Block) {
	t.Helper()

	a0 := spendOf(t, key, root, 0, 90_000)
	outKey, _ := bec.PrivateKeyFromBytes([]byte("netted-out"))
	a1 := spendOf(t, outKey, a0, 0, 80_000)
	a2 := spendOf(t, key, root, 1, 90_000)
	a3 := spendOf(t, outKey, a1, 0, 70_000)
	a4 := spendOf(t, key, root, 2, 90_000)

	layout := [][]*bt.Tx{{a0, a1, a2}, {a3, a4}}

	return layout, []*bt.Tx{a0, a1, a2, a3, a4}, netBlockFor(layout)
}

// quickLaneBatch is the batch of subtrees from to to of block, extended by the production stage.
func quickLaneBatch(t *testing.T, bv *BlockValidation, block *model.Block, layout [][]*bt.Tx, from, to int, outpointOnly bool) *SubtreeProcessingBatch {
	t.Helper()

	batch := &SubtreeProcessingBatch{
		subtreeData:  make([]*subtreepkg.Data, to-from),
		txRanges:     make([][2]int, to-from),
		batchStart:   from,
		batchEnd:     to,
		outpointOnly: outpointOnly,
	}

	parents := map[chainhash.Hash]*bt.Tx{}

	for i := 0; i < from; i++ {
		for _, tx := range layout[i] {
			parents[*tx.TxIDChainHash()] = tx
		}
	}

	for i := from; i < to; i++ {
		data := layout[i]
		if i == 0 {
			data = append([]*bt.Tx{nil}, data...)
		}

		batch.subtreeData[i-from] = &subtreepkg.Data{Txs: data}
	}

	require.NoError(t, bv.extendBatch(context.Background(), block, batch, parents))

	return batch
}

// quickValidateAttempt applies the batches of subtrees from to to of block as one attempt of quick
// validation, and returns the context the attempt collected its positions on.
func quickValidateAttempt(t *testing.T, bv *BlockValidation, block *model.Block, layout [][]*bt.Tx, kind quickLaneStore, from, to int) context.Context {
	t.Helper()

	ctx := bv.withQuickLanePositions(context.Background())
	require.NotNil(t, quickLanePositionsFrom(ctx))

	for i := from; i < to; i++ {
		require.NoError(t, bv.createAndSpendUTXOsForBatch(ctx, block, quickLaneBatch(t, bv, block, layout, i, i+1, kind.outpointOnly)))
	}

	return ctx
}

// storedLanePositions reads each transaction's lane position from the store.
func storedLanePositions(t *testing.T, store utxo.Store, txs []*bt.Tx) []uint64 {
	t.Helper()

	positions := make([]uint64, len(txs))

	for i, tx := range txs {
		md, err := store.Get(context.Background(), tx.TxIDChainHash(), fields.LanePosition)
		require.NoError(t, err)
		require.True(t, lane.Valid(md.LanePosition), "transaction %d is stored with a lane position", i)

		positions[i] = md.LanePosition
	}

	return positions
}

// rowsOf are the map rows of a block whose transactions took positions.
func rowsOf(t *testing.T, positions []uint64) []lane.MapRow {
	t.Helper()

	b := lane.NewBitmapsBuilder()
	for _, v := range positions {
		b.Add(v)
	}

	rows, err := lane.BuildBlockRows([]lane.LaneBitmaps{b.Subtree(chainhash.Hash{}).Lanes})
	require.NoError(t, err)

	return rows.Rows
}

// A block stored by quick validation at shadow gets rows listing exactly the positions its
// records were written with, and a redone block, cut off after its first subtree, gets the same
// rows and gives no record a second position.
func TestQuickValidationLaneRowsAreTheRecordsPositions(t *testing.T) {
	for _, kind := range quickLaneStores() {
		t.Run(kind.name, func(t *testing.T) {
			bv, store, _ := newQuickLaneHarness(t, kind)

			t.Run("one attempt", func(t *testing.T) {
				root, key := seedRoot(t, store, 4, "QUICK_LANE_ONE_"+kind.name)
				layout, txs, block := quickLaneBlock(t, key, root)

				// a1's spend fails once after its create, so the retry carries its parent a0 back
				// as existing; a0's position comes from the first pass.
				flaky, partial := store.(*flakyTxStore)
				if partial {
					flaky.fail = *txs[1].TxIDChainHash()
					flaky.failTimes.Store(1)
				}

				mismatches := laneMetric(t, "teranode_lane_row_count_mismatches_total", "", "")

				ctx := quickValidateAttempt(t, bv, block, layout, kind, 0, len(layout))
				positions := storedLanePositions(t, store, txs)

				if partial {
					require.LessOrEqual(t, flaky.failTimes.Load(), int32(0), "the fault fired")
				}

				require.Equal(t, rowsOf(t, positions), bv.quickLaneRows(ctx, block))
				require.NotContains(t, positions, storedLanePositions(t, store, []*bt.Tx{root})[0], "the rows hold the block's records only")
				require.Equal(t, mismatches, laneMetric(t, "teranode_lane_row_count_mismatches_total", "", ""), "the rows hold every transaction besides the coinbase")
			})

			t.Run("cut off after the first subtree and redone", func(t *testing.T) {
				root, key := seedRoot(t, store, 4, "QUICK_LANE_REDO_"+kind.name)
				layout, txs, block := quickLaneBlock(t, key, root)

				quickValidateAttempt(t, bv, block, layout, kind, 0, 1)
				first := storedLanePositions(t, store, txs[:3])

				redo := quickValidateAttempt(t, bv, block, layout, kind, 0, len(layout))
				positions := storedLanePositions(t, store, txs)
				require.Equal(t, first, positions[:3], "a record keeps the position it was written with")
				require.Equal(t, rowsOf(t, positions), bv.quickLaneRows(redo, block))

				again := quickValidateAttempt(t, bv, block, layout, kind, 0, len(layout))
				require.Equal(t, positions, storedLanePositions(t, store, txs))
				require.Equal(t, rowsOf(t, positions), bv.quickLaneRows(again, block), "a block found whole gets the same rows")
			})
		})
	}
}

// A transaction whose create is skipped as unspendable has no record and no position, so the rows
// leave it out and the count does not expect it; one stored before the block keeps its position.
func TestQuickValidationLaneRowsLeaveOutTransactionsStoredWithoutARecord(t *testing.T) {
	kind := quickLaneStores()[1]
	bv, store, _ := newQuickLaneHarness(t, kind)

	bv.settings.BlockValidation.QuickValidateSkipUtxoLock = true
	bv.settings.BlockValidation.SkipUnspendableTxStorageDuringCatchup = true

	root, key := seedRoot(t, store, 3, "QUICK_LANE_UNSPENDABLE")

	opReturnOf := func(vout uint32, data string) *bt.Tx {
		tx := bt.NewTx()
		require.NoError(t, tx.FromUTXOs(&bt.UTXO{
			TxIDHash:      root.TxIDChainHash(),
			Vout:          vout,
			LockingScript: root.Outputs[vout].LockingScript,
			Satoshis:      root.Outputs[vout].Satoshis,
		}))
		tx.Inputs[0].UnlockingScript = bscript.NewFromBytes([]byte{0x00})
		require.NoError(t, tx.AddOpReturnOutput([]byte(data)))

		return tx
	}

	opReturn := opReturnOf(0, "quick-lane-unspendable")
	storedBefore := opReturnOf(2, "quick-lane-stored-before")
	spendable := spendOf(t, key, root, 1, 90_000)

	_, _, err := store.SpendAndCreate(context.Background(), storedBefore, 100, utxo.WithCreateOnly())
	require.NoError(t, err)

	layout := [][]*bt.Tx{{opReturn, spendable, storedBefore}}
	block := netBlockFor(layout)

	mismatches := laneMetric(t, "teranode_lane_row_count_mismatches_total", "", "")

	ctx := quickValidateAttempt(t, bv, block, layout, kind, 0, 1)

	_, err = store.Get(context.Background(), opReturn.TxIDChainHash())
	require.ErrorIs(t, err, errors.ErrTxNotFound, "the unspendable transaction is stored without a record")

	require.Equal(t, rowsOf(t, storedLanePositions(t, store, []*bt.Tx{spendable, storedBefore})), bv.quickLaneRows(ctx, block))
	require.Equal(t, mismatches, laneMetric(t, "teranode_lane_row_count_mismatches_total", "", ""))
}

// At off an attempt collects nothing and the block gets no rows.
func TestQuickValidationLaneRowsAtOff(t *testing.T) {
	bv := &BlockValidation{logger: ulogger.TestLogger{}, settings: testutil.CreateBaseTestSettings(t)}
	require.Equal(t, settings.LaneMigrationOff, bv.settings.UtxoStore.LaneMigration)

	ctx := context.Background()
	require.Equal(t, ctx, bv.withQuickLanePositions(ctx))
	require.Nil(t, quickLanePositionsFrom(ctx))

	quickLanePositionsFrom(ctx).add(lane.Pack(7, 1))
	quickLanePositionsFrom(ctx).addCreated([]utxo.SpendAndCreateMultiResult{{Status: utxo.MultiTxCreated}})
	quickLanePositionsFrom(ctx).addNoRecord(1)
	quickLanePositionsFrom(ctx).fail(errors.NewProcessingError("read failed"))

	require.Nil(t, bv.quickLaneRows(ctx, &model.Block{Subtrees: []*chainhash.Hash{{}}, TransactionCount: 2}))
}

// decorateFailingStore fails every BatchDecorate with whole, or, when whole is nil, every item
// with item, and counts the calls when calls is set. With stall set it waits for its context first.
type decorateFailingStore struct {
	utxo.Store
	whole, item error
	calls       *atomic.Int32
	stall       bool
	// late, with stall, answers without error once the context has ended, as a store that does
	// not follow its context answers after the bound.
	late bool
	// deadline, when set, receives the deadline of the context the store was given.
	deadline chan time.Time
	// hold, when set, blocks every call until it is closed, whatever the context says, as an
	// Aerospike batch read does.
	hold chan struct{}
}

func (s decorateFailingStore) BatchDecorate(ctx context.Context, items []*utxo.UnresolvedMetaData, _ ...fields.FieldName) error {
	if s.calls != nil {
		s.calls.Add(1)
	}

	if s.deadline != nil {
		d, _ := ctx.Deadline()
		s.deadline <- d
	}

	if s.hold != nil {
		<-s.hold
	}

	if s.stall {
		<-ctx.Done()

		if s.late {
			return nil
		}

		return ctx.Err()
	}

	if s.whole != nil {
		return s.whole
	}

	for _, it := range items {
		it.Err = s.item
	}

	return nil
}

// The collection takes created records only, counts a record without a position, holds its
// positions in per-lane bitmaps past the pending bound, and a failed read of existing records
// stores the block without rows, counted, rather than failing it.
func TestQuickLanePositionsCollect(t *testing.T) {
	tSettings := testutil.CreateBaseTestSettings(t)
	tSettings.UtxoStore.LaneMigration = settings.LaneMigrationShadow

	bv := &BlockValidation{logger: ulogger.TestLogger{}, settings: tSettings}
	block := &model.Block{Subtrees: []*chainhash.Hash{{}}, TransactionCount: 1 + quickLanePendingMax + 4}

	ctx := bv.withQuickLanePositions(context.Background())
	q := quickLanePositionsFrom(ctx)
	require.NotNil(t, q)

	q.addCreated([]utxo.SpendAndCreateMultiResult{
		{Status: utxo.MultiTxCreated, Meta: &meta.Data{LanePosition: lane.Pack(7, 1)}},
		{Status: utxo.MultiTxExisted, Meta: &meta.Data{LanePosition: lane.Pack(7, 2)}},
		{Status: utxo.MultiTxFailed, Meta: &meta.Data{LanePosition: lane.Pack(7, 3)}},
		{Status: utxo.MultiTxCreated},
		{Status: utxo.MultiTxCreated, Meta: &meta.Data{LanePosition: lane.Pack(9, 5)}},
	})

	many := make([]uint64, quickLanePendingMax, quickLanePendingMax+2)
	for i := range many {
		many[i] = lane.Pack(7, uint64(10+i)) //nolint:gosec // test index
	}

	q.add(many...)
	require.Less(t, q.npending, quickLanePendingMax, "the pending positions went into the bitmaps at the bound")
	require.NotEmpty(t, q.lanes)

	unpositioned := laneTxsWithoutPosition(t)
	fewer := laneMetric(t, "teranode_lane_row_count_mismatches_total", "direction", lane.MismatchFewer)

	rows := bv.quickLaneRows(ctx, block)
	require.Equal(t, rowsOf(t, append(many, lane.Pack(7, 1), lane.Pack(9, 5))), rows)
	require.Equal(t, unpositioned+1, laneTxsWithoutPosition(t), "the created record without a position")
	require.Equal(t, fewer+1, laneMetric(t, "teranode_lane_row_count_mismatches_total", "direction", lane.MismatchFewer))

	for _, store := range []decorateFailingStore{
		{whole: errors.NewStorageError("decorate failed")},
		{item: errors.NewStorageError("record unreadable")},
	} {
		bv = &BlockValidation{logger: ulogger.TestLogger{}, settings: tSettings, utxoStore: store}
		failures := laneMetric(t, "teranode_lane_row_insert_failures_total", "", "")
		unpositioned = laneTxsWithoutPosition(t)

		ctx = bv.withQuickLanePositions(context.Background())
		quickLanePositionsFrom(ctx).add(lane.Pack(7, 1), lane.None)
		bv.readExistingLanePositions(ctx, quickLanePositionsFrom(ctx), []*chainhash.Hash{{0x01}})

		require.Nil(t, bv.quickLaneRows(ctx, block))
		require.Equal(t, failures+1, laneMetric(t, "teranode_lane_row_insert_failures_total", "", ""))
		require.Equal(t, unpositioned+1, laneTxsWithoutPosition(t), "a record without a position is counted though the rows failed")
	}

	// A read that stalls ends at quickLaneReadTimeout, 30 s by default, and the block goes
	// without rows.
	timeout := quickLaneReadTimeout
	require.Equal(t, 30*time.Second, timeout)

	quickLaneReadTimeout = 50 * time.Millisecond

	t.Cleanup(func() { quickLaneReadTimeout = timeout })

	deadline := make(chan time.Time, 1)

	bv = &BlockValidation{logger: ulogger.TestLogger{}, settings: tSettings, utxoStore: decorateFailingStore{stall: true, deadline: deadline}}
	ctx = bv.withQuickLanePositions(context.Background())
	quickLanePositionsFrom(ctx).add(lane.Pack(7, 1))

	start := time.Now()
	bv.readExistingLanePositions(ctx, quickLanePositionsFrom(ctx), []*chainhash.Hash{{0x01}})
	require.WithinDuration(t, start.Add(quickLaneReadTimeout), <-deadline, 20*time.Millisecond, "the store is given the bound")
	require.Nil(t, bv.quickLaneRows(ctx, block))

	bv = &BlockValidation{logger: ulogger.TestLogger{}, settings: tSettings, utxoStore: decorateFailingStore{stall: true, late: true}}
	ctx = bv.withQuickLanePositions(context.Background())
	quickLanePositionsFrom(ctx).add(lane.Pack(7, 1))
	bv.readExistingLanePositions(ctx, quickLanePositionsFrom(ctx), []*chainhash.Hash{{0x01}})
	require.Nil(t, bv.quickLaneRows(ctx, block), "an answer after the bound does not count")

	// A store that ignores its context does not hold the block past the bound.
	hold := make(chan struct{})
	t.Cleanup(func() { close(hold) })

	bv = &BlockValidation{logger: ulogger.TestLogger{}, settings: tSettings, utxoStore: decorateFailingStore{hold: hold}}
	ctx = bv.withQuickLanePositions(context.Background())

	start = time.Now()
	bv.readExistingLanePositions(ctx, quickLanePositionsFrom(ctx), []*chainhash.Hash{{0x01}})
	require.Less(t, time.Since(start), 2*quickLaneReadTimeout, "the read returns at the bound")
	require.True(t, quickLanePositionsFrom(ctx).hasFailed())

	// Reads stuck in such a store hold at most the shared slots: a later attempt starts no read,
	// times out, and the slots serve again once the store answers.
	var started atomic.Int32

	stuck := make(chan struct{})
	bv = &BlockValidation{logger: ulogger.TestLogger{}, settings: tSettings, utxoStore: decorateFailingStore{hold: stuck, calls: &started}}
	stuckHashes := make([]*chainhash.Hash, quickLaneReadConcurrency*quickLaneReadChunk)

	for i := range stuckHashes {
		stuckHashes[i] = &chainhash.Hash{byte(i), byte(i >> 8)}
	}

	ctx = bv.withQuickLanePositions(context.Background())
	bv.readExistingLanePositions(ctx, quickLanePositionsFrom(ctx), stuckHashes)
	require.Eventually(t, func() bool { return started.Load() == quickLaneReadConcurrency }, time.Second, time.Millisecond)

	ctx = bv.withQuickLanePositions(context.Background())
	bv.readExistingLanePositions(ctx, quickLanePositionsFrom(ctx), stuckHashes[:1])
	require.True(t, quickLanePositionsFrom(ctx).hasFailed(), "no slot was free")
	require.Equal(t, int32(quickLaneReadConcurrency), started.Load(), "no ninth read started")

	close(stuck)

	require.Eventually(t, func() bool {
		ctx = bv.withQuickLanePositions(context.Background())
		bv.readExistingLanePositions(ctx, quickLanePositionsFrom(ctx), stuckHashes[:1])

		return !quickLanePositionsFrom(ctx).hasFailed()
	}, time.Second, 10*time.Millisecond, "the slots serve again")

	// A read whose deadline has already passed reaches no store.
	var none atomic.Int32

	bv = &BlockValidation{logger: ulogger.TestLogger{}, settings: tSettings, utxoStore: decorateFailingStore{calls: &none}}
	expired, cancelExpired := context.WithCancel(bv.withQuickLanePositions(context.Background()))
	cancelExpired()

	for range 20 {
		bv.readExistingLanePositions(expired, &quickLanePositions{pending: map[uint8][]uint64{}, lanes: lane.LaneBitmaps{}}, stuckHashes[:3*quickLaneReadChunk])
	}

	time.Sleep(50 * time.Millisecond)
	require.Zero(t, none.Load(), "no store call after the deadline")

	// A coinbase-only block has nothing to build and records no gate time.
	gates := laneMetric(t, "teranode_lane_gate_seconds", "stage", lane.GateStageBuild)
	ctx = bv.withQuickLanePositions(context.Background())
	require.Nil(t, bv.quickLaneRows(ctx, &model.Block{TransactionCount: 1}))
	require.Equal(t, gates, laneMetric(t, "teranode_lane_gate_seconds", "stage", lane.GateStageBuild))

	// After a failed read no further chunk is read, in this batch or a later one. The first read
	// fails; the rest hold until released, so the reads in flight are the slots' worth.
	var calls atomic.Int32

	release := make(chan struct{})
	bv = &BlockValidation{logger: ulogger.TestLogger{}, settings: tSettings, utxoStore: firstFailsStore{calls: &calls, release: release}}
	ctx = bv.withQuickLanePositions(context.Background())
	hashes := make([]*chainhash.Hash, 40*quickLaneReadChunk)

	for i := range hashes {
		hashes[i] = &chainhash.Hash{byte(i), byte(i >> 8)}
	}

	bv.readExistingLanePositions(ctx, quickLanePositionsFrom(ctx), hashes)
	require.True(t, quickLanePositionsFrom(ctx).hasFailed())
	close(release)
	require.LessOrEqual(t, calls.Load(), int32(quickLaneReadConcurrency), "the reads stop once one has failed")

	// Reads already started may still land on the first store; a later batch reaches none.
	var later atomic.Int32

	bv = &BlockValidation{logger: ulogger.TestLogger{}, settings: tSettings, utxoStore: decorateFailingStore{calls: &later}}
	bv.readExistingLanePositions(ctx, quickLanePositionsFrom(ctx), hashes)
	require.Zero(t, later.Load(), "a later batch reads nothing")

	// A record not found is one without a position; the rest of the rows still go.
	bv = &BlockValidation{logger: ulogger.TestLogger{}, settings: tSettings, utxoStore: decorateFailingStore{item: errors.NewTxNotFoundError("gone")}}
	unpositioned = laneTxsWithoutPosition(t)

	ctx = bv.withQuickLanePositions(context.Background())
	quickLanePositionsFrom(ctx).add(lane.Pack(7, 1))
	bv.readExistingLanePositions(ctx, quickLanePositionsFrom(ctx), []*chainhash.Hash{{0x01}})

	require.Equal(t, rowsOf(t, []uint64{lane.Pack(7, 1)}), bv.quickLaneRows(ctx, block))
	require.Equal(t, unpositioned+1, laneTxsWithoutPosition(t))
}

// firstFailsStore fails its first BatchDecorate and holds every later one until release is closed.
type firstFailsStore struct {
	utxo.Store
	calls   *atomic.Int32
	release chan struct{}
}

func (s firstFailsStore) BatchDecorate(context.Context, []*utxo.UnresolvedMetaData, ...fields.FieldName) error {
	if s.calls.Add(1) == 1 {
		return errors.NewStorageError("decorate failed")
	}

	<-s.release

	return errors.NewStorageError("decorate failed")
}

// laneTxsWithoutPosition is teranode_lane_txs_without_position_total for quick validation.
func laneTxsWithoutPosition(t *testing.T) float64 {
	t.Helper()

	return laneServiceMetric(t, "teranode_lane_txs_without_position_total", laneQuickValidationService)
}

// laneCounter is the sum of a counter's samples.
func laneCounter(t *testing.T, metric string) float64 {
	t.Helper()

	families, err := prometheus.DefaultGatherer.Gather()
	require.NoError(t, err)

	var sum float64

	for _, f := range families {
		if f.GetName() != metric {
			continue
		}

		for _, m := range f.GetMetric() {
			sum += m.GetCounter().GetValue()
		}
	}

	return sum
}

// laneServiceMetric is a counter's value for one service label.
func laneServiceMetric(t *testing.T, metric, service string) float64 {
	t.Helper()

	families, err := prometheus.DefaultGatherer.Gather()
	require.NoError(t, err)

	var sum float64

	for _, f := range families {
		if f.GetName() != metric {
			continue
		}

		for _, m := range f.GetMetric() {
			for _, l := range m.GetLabel() {
				if l.GetName() == "service" && l.GetValue() == service && m.GetCounter() != nil {
					sum += m.GetCounter().GetValue()
				}
			}
		}
	}

	return sum
}

// commitBlock stores a quick-validated block with its rows, which the lane reader and the pruner's
// lane rule then judge, and stores it without rows when its positions cannot be read.
func TestQuickValidatedBlockCommit(t *testing.T) {
	t.Run("rows reach the reader and the pruner", func(t *testing.T) {
		f := newQuickLaneCommitFixture(t, "QUICK_LANE_READER")
		bv, store, client, kind, layout, txs, block := f.bv, f.store, f.client, f.kind, f.layout, f.txs, f.block
		blockChainStore := f.blockChainStore

		ctx := context.Background()
		logger := ulogger.TestLogger{}

		attempt := quickValidateAttempt(t, bv, block, layout, kind, 0, len(layout))
		require.NoError(t, bv.commitBlock(attempt, block, "", "test"))

		positions := storedLanePositions(t, store, txs)

		stored, err := client.LaneMapRows(ctx, block.ID)
		require.NoError(t, err)
		require.Equal(t, rowsOf(t, positions), stored, "the rows went with the block")

		// The reader settles a block once it is the coinbase maturity deep (1 in the test settings);
		// three more blocks put it past that.
		parent := block
		for height := uint32(2); height <= 4; height++ {
			next := quickLaneChainBlock(t, parent.Hash(), height, &model.Block{TransactionCount: 1})
			_, _, err = blockChainStore.StoreBlock(ctx, next, "")
			require.NoError(t, err)

			parent = next
		}

		readerCtx, cancel := context.WithCancel(ctx)
		defer cancel()

		r := lanereader.Start(readerCtx, logger, bv.settings, client)
		require.NotNil(t, r)
		require.Eventually(t, func() bool { return r.Snapshot() != nil && r.Snapshot().TipHeight() == 4 }, 30*time.Second, 50*time.Millisecond)

		v := r.MainView()

		for i, pos := range positions {
			require.True(t, v.Lookup(pos).Mined, "transaction %d of the quick-validated block is mined", i)
		}

		judge := r.PruneJudge(lanereader.PruneStoreUtxoset, bv.settings.GlobalBlockHeightRetention)
		defer judge.Done()

		// a0 is spent by a1, both in the quick-validated block.
		verdict := judge.Judge(lanereader.TodayDelete, &lanereader.PruneCandidate{
			ParentPos: positions[0],
			Spenders:  []lanereader.PruneSpender{{Pos: positions[1]}},
		})
		require.Equal(t, lanereader.PruneVerdict{Lanes: lanereader.PruneDelete, Reason: lanereader.ReasonSettled}, verdict)
	})

	// Full validation of the same block, finding the subtree files quick validation wrote, saves no
	// lane bitmaps and stores the block without rows. Whichever stores it first, the block ends with
	// its records' rows, under the ID it was stored with, and a redo changes nothing.
	for _, fullFirst := range []bool{false, true} {
		t.Run(fmt.Sprintf("full validation stores the block first=%t", fullFirst), func(t *testing.T) {
			f := newQuickLaneCommitFixture(t, fmt.Sprintf("QUICK_LANE_RACE_%t", fullFirst))
			bv, store, client, kind, layout, txs, block := f.bv, f.store, f.client, f.kind, f.layout, f.txs, f.block

			bv.subtreeStore = blobmemory.New()

			ctx := context.Background()
			failures := laneMetric(t, "teranode_lane_row_insert_failures_total", "block", lane.BlockPeer)
			absent := laneMetric(t, "teranode_lane_missing_bitmaps_total", "reason", lane.MissingBitmapsAbsent)

			// Full validation's own copy of the block, with no ID: the store gives it one.
			full := &model.Block{Header: block.Header, CoinbaseTx: block.CoinbaseTx, Subtrees: block.Subtrees, TransactionCount: block.TransactionCount, Height: block.Height}
			storeInFull := func() error { return client.AddBlock(ctx, full, "legacy", bv.buildAddBlockOpts(ctx, full)...) }

			attempt := quickValidateAttempt(t, bv, block, layout, kind, 0, len(layout))

			if fullFirst {
				require.NoError(t, storeInFull())
			}

			require.NoError(t, bv.commitBlock(attempt, block, "", "test"))

			if !fullFirst {
				require.Error(t, storeInFull(), "the block is stored already")
			}

			require.Equal(t, absent+float64(len(block.Subtrees)), laneMetric(t, "teranode_lane_missing_bitmaps_total", "reason", lane.MissingBitmapsAbsent), "full validation found no lane bitmaps")

			_, meta, err := client.GetBlockHeader(ctx, block.Hash())
			require.NoError(t, err)

			want := rowsOf(t, storedLanePositions(t, store, txs))

			stored, err := client.LaneMapRows(ctx, meta.ID)
			require.NoError(t, err)
			require.Equal(t, want, stored)

			redo := quickValidateAttempt(t, bv, block, layout, kind, 0, len(layout))
			require.NoError(t, bv.commitBlock(redo, block, "", "test"))

			stored, err = client.LaneMapRows(ctx, meta.ID)
			require.NoError(t, err)
			require.Equal(t, want, stored, "a redo changes no row")
			require.Equal(t, failures, laneMetric(t, "teranode_lane_row_insert_failures_total", "block", lane.BlockPeer), "no insert failed")
		})
	}

	// A late insert runs after the unlock, whether the unlock succeeds or fails, and after its
	// caller gave up, on a context of its own with a deadline. One that fails is counted, and the
	// commit still runs to its end.
	for _, c := range []struct {
		name                     string
		insertFails, unlockFails bool
	}{
		{name: "a late insert"},
		{name: "a late insert that fails", insertFails: true},
		{name: "a late insert after a failed unlock", unlockFails: true},
	} {
		t.Run(c.name, func(t *testing.T) {
			saved := postCommitUnlockRetryBackoff
			postCommitUnlockRetryBackoff = time.Millisecond

			t.Cleanup(func() { postCommitUnlockRetryBackoff = saved })

			f := newQuickLaneCommitFixture(t, "QUICK_LANE_LATE_"+c.name)
			bv, store, client, kind, layout, txs, block := f.bv, f.store, f.client, f.kind, f.layout, f.txs, f.block

			ctx := context.Background()
			full := &model.Block{Header: block.Header, CoinbaseTx: block.CoinbaseTx, Subtrees: block.Subtrees, TransactionCount: block.TransactionCount, Height: block.Height}
			require.NoError(t, client.AddBlock(ctx, full, "legacy"), "stored first, without rows")

			attempt := quickValidateAttempt(t, bv, block, layout, kind, 0, len(layout))

			// The outputs to unlock, so the commit's unlock reaches the store.
			subtree, err := subtreepkg.NewTreeByLeafCount(4)
			require.NoError(t, err)
			require.NoError(t, subtree.AddCoinbaseNode())
			require.NoError(t, subtree.AddNode(*txs[0].TxIDChainHash(), 0, 0))

			block.SubtreeSlices = []*subtreepkg.Subtree{subtree}

			locks := &lockRecordingStore{Store: store}
			if c.unlockFails {
				locks.err = errors.NewStorageError("unlock failed")
			}

			bv.utxoStore = locks

			late := &lateInsertClient{ClientI: client, unlocks: &locks.unlocks}
			if c.insertFails {
				late.err = errors.NewServiceError("insert failed")
			}

			bv.blockchainClient = late
			failures := laneMetric(t, "teranode_lane_row_insert_failures_total", "block", lane.BlockPeer)

			gaveUp, cancel := context.WithCancel(attempt)
			cancel()

			start := time.Now()
			err = bv.commitBlock(gaveUp, block, "", "test")

			if c.unlockFails {
				require.Error(t, err)
				require.True(t, isBlockCommitted(err), "the block is stored")
			} else {
				require.NoError(t, err, "the block is stored either way")

				_, cached := bv.blockExistsCache.Get(*block.Hash())
				require.True(t, cached, "the commit ran on to its end")
			}

			require.Equal(t, 1, late.calls, "one insert")
			require.Positive(t, late.unlocksBefore, "the unlock ran before the insert")
			require.True(t, late.live, "the insert runs although the caller gave up")
			require.WithinDuration(t, start.Add(quickLaneLateInsertTimeout), late.deadline, 5*time.Second)

			_, meta, err := client.GetBlockHeader(ctx, block.Hash())
			require.NoError(t, err)

			rows, err := client.LaneMapRows(ctx, meta.ID)
			require.NoError(t, err)

			if c.insertFails {
				require.Empty(t, rows)
				require.Equal(t, failures+1, laneMetric(t, "teranode_lane_row_insert_failures_total", "block", lane.BlockPeer))

				return
			}

			require.Equal(t, rowsOf(t, storedLanePositions(t, store, txs)), rows)
			require.Equal(t, failures, laneMetric(t, "teranode_lane_row_insert_failures_total", "block", lane.BlockPeer))
		})
	}

	t.Run("a commit with no rows sends no late insert", func(t *testing.T) {
		f := newQuickLaneCommitFixture(t, "QUICK_LANE_LATE_NONE")
		bv, client, kind, layout, block := f.bv, f.client, f.kind, f.layout, f.block

		ctx := context.Background()
		full := &model.Block{Header: block.Header, CoinbaseTx: block.CoinbaseTx, Subtrees: block.Subtrees, TransactionCount: block.TransactionCount, Height: block.Height}
		require.NoError(t, client.AddBlock(ctx, full, "legacy"), "stored first, without rows")

		quickValidateAttempt(t, bv, block, layout, kind, 0, len(layout))

		late := &lateInsertClient{ClientI: client}
		bv.blockchainClient = late

		// A context with no position collection builds no rows.
		require.NoError(t, bv.commitBlock(ctx, block, "", "test"))
		require.Zero(t, late.calls)
	})

	t.Run("a failed position read stores the block without rows", func(t *testing.T) {
		f := newQuickLaneCommitFixture(t, "QUICK_LANE_READ_FAILS")
		bv, store, client, kind, layout, block := f.bv, f.store, f.client, f.kind, f.layout, f.block

		ctx := context.Background()

		// The first attempt is cut off after its first subtree, so the redo finds those records.
		quickValidateAttempt(t, bv, block, layout, kind, 0, 1)

		bv.utxoStore = decorateFailingStore{Store: store, whole: errors.NewStorageError("decorate failed")}

		redo := quickValidateAttempt(t, bv, block, layout, kind, 0, len(layout))
		require.NoError(t, bv.commitBlock(redo, block, "", "test"))

		exists, err := client.GetBlockExists(ctx, block.Hash())
		require.NoError(t, err)
		require.True(t, exists, "the block is stored")

		rows, err := client.LaneMapRows(ctx, block.ID)
		require.NoError(t, err)
		require.Empty(t, rows)
	})
}

// lateInsertClient records the context the late lane rows insert ran on, and fails it with err.
type lateInsertClient struct {
	blockchain.ClientI
	err      error
	calls    int
	deadline time.Time
	live     bool
	// unlocks, when set, counts the unlocks, and unlocksBefore is its value when the insert began.
	unlocks       *atomic.Int32
	unlocksBefore int32
}

func (c *lateInsertClient) InsertLaneMapRows(ctx context.Context, hash *chainhash.Hash, rows []lane.MapRow) error {
	c.calls++
	c.deadline, _ = ctx.Deadline()

	if c.unlocks != nil {
		c.unlocksBefore = c.unlocks.Load()
	}
	c.live = ctx.Err() == nil

	if c.err != nil {
		return c.err
	}

	return c.ClientI.InsertLaneMapRows(ctx, hash, rows)
}

// lockRecordingStore counts each unlock, and fails it with err.
type lockRecordingStore struct {
	utxo.Store
	err     error
	unlocks atomic.Int32
}

func (s *lockRecordingStore) SetLocked(context.Context, []chainhash.Hash, bool) error {
	s.unlocks.Add(1)

	return s.err
}

// withQuickLaneBlockchain gives bv a blockchain client over a fresh SQLite blockchain store.
func withQuickLaneBlockchain(t *testing.T, bv *BlockValidation) (blockchain_store.Store, blockchain.ClientI) {
	t.Helper()

	blockChainStore, err := blockchain_store.NewStore(ulogger.TestLogger{}, &url.URL{Scheme: "sqlitememory"}, bv.settings)
	require.NoError(t, err)

	client, err := blockchain.NewLocalClient(ulogger.TestLogger{}, bv.settings, blockChainStore, nil, nil)
	require.NoError(t, err)

	bv.blockchainClient = client

	return blockChainStore, client
}

// quickLaneCommitFixture is a quick-validation block over a utxoset store at shadow, as block 1 of a
// real SQLite blockchain store, with its ID reserved, ready for an attempt and commitBlock.
type quickLaneCommitFixture struct {
	bv              *BlockValidation
	store           utxo.Store
	blockChainStore blockchain_store.Store
	client          blockchain.ClientI
	kind            quickLaneStore
	layout          [][]*bt.Tx
	txs             []*bt.Tx
	block           *model.Block
}

func newQuickLaneCommitFixture(t *testing.T, seed string) quickLaneCommitFixture {
	t.Helper()

	initPrometheusMetrics()

	f := quickLaneCommitFixture{kind: quickLaneStores()[0]}
	f.bv, f.store, _ = newQuickLaneHarness(t, f.kind)
	f.blockChainStore, f.client = withQuickLaneBlockchain(t, f.bv)

	root, key := seedRoot(t, f.store, 4, seed)

	var fixture *model.Block

	f.layout, f.txs, fixture = quickLaneBlock(t, key, root)
	f.block = quickLaneFirstBlock(t, f.bv, f.client, fixture)

	return f
}

// quickLaneFirstBlock is fixture as block 1 on genesis, with the ID quick validation reserves for it.
func quickLaneFirstBlock(t *testing.T, bv *BlockValidation, client blockchain.ClientI, fixture *model.Block) *model.Block {
	t.Helper()

	block := quickLaneChainBlock(t, bv.settings.ChainCfgParams.GenesisHash, 1, fixture)

	id, err := client.AssignBlockID(context.Background(), block.Hash())
	require.NoError(t, err)

	block.ID, err = blockIDToUint32(id, block.Hash().String())
	require.NoError(t, err)

	return block
}

// quickLaneChainBlock is fixture as a block at height on parent, with a header that meets the
// regtest target.
func quickLaneChainBlock(t *testing.T, parent *chainhash.Hash, height uint32, fixture *model.Block) *model.Block {
	t.Helper()

	coinbase := transactions.Create(t,
		transactions.WithCoinbaseData(height, "/quick-lane/"),
		transactions.WithP2PKHOutputs(1, 50*100_000_000, quickLaneCoinbaseKey(t)),
	)

	nBits, err := model.NewNBitFromString("207fffff")
	require.NoError(t, err)

	header := &model.BlockHeader{
		Version:        1,
		HashPrevBlock:  parent,
		HashMerkleRoot: coinbase.TxIDChainHash(),
		Timestamp:      uint32(time.Now().Unix()) + height, //nolint:gosec // a test timestamp
		Bits:           *nBits,
	}

	for ok := false; !ok; header.Nonce++ {
		ok, _, _ = header.HasMetTargetDifficulty()
		if ok {
			break
		}
	}

	subtrees := fixture.Subtrees
	for i := range subtrees {
		subtrees[i] = &chainhash.Hash{byte(height), byte(i + 1)}
	}

	return &model.Block{
		Header:           header,
		CoinbaseTx:       coinbase,
		Subtrees:         subtrees,
		TransactionCount: fixture.TransactionCount,
		Height:           height,
	}
}

func quickLaneCoinbaseKey(t *testing.T) *bec.PublicKey {
	t.Helper()

	_, publicKey := bec.PrivateKeyFromBytes([]byte("quick-lane-coinbase"))

	return publicKey
}

// A cold allocator at the default grant settings hands out four netted ranges' positions back to
// back, as quick validation takes them, with no take failing or waiting near the 5 s take wait.
func TestQuickValidationTakesARangesPositionsWithoutStalling(t *testing.T) {
	tSettings := testutil.CreateBaseTestSettings(t)
	logger := ulogger.TestLogger{}

	blockChainStore, err := blockchain_store.NewStore(logger, &url.URL{Scheme: "sqlitememory"}, tSettings)
	require.NoError(t, err)

	client, err := blockchain.NewLocalClient(logger, tSettings, blockChainStore, nil, nil)
	require.NoError(t, err)

	a := lane.NewAllocator(client, "quick-lane-test", lane.Timings{
		Renew:         tSettings.BlockChain.LaneLeaseRenewInterval,
		Stop:          tSettings.BlockChain.LaneLeaseStopAfter,
		GrantDuration: tSettings.BlockChain.LaneGrantDuration,
	}, logger)

	t.Cleanup(func() { _ = a.Close() })

	ctx := context.Background()

	const ranges, perRange = 4, defaultNetRange

	var longest time.Duration

	grants, waits := laneCounter(t, "teranode_lane_grants_total"), laneCounter(t, "teranode_lane_waits_total")
	start := time.Now()
	seen := map[uint64]struct{}{}

	for range ranges * perRange {
		took := time.Now()

		v, err := a.Next(ctx)
		require.NoError(t, err)

		longest = max(longest, time.Since(took))

		_, dup := seen[v]
		require.False(t, dup, "a position is handed out once")

		seen[v] = struct{}{}
	}

	t.Logf("took %d positions in %v, longest take %v, %v grants, %v takes waited for one", ranges*perRange, time.Since(start), longest,
		laneCounter(t, "teranode_lane_grants_total")-grants, laneCounter(t, "teranode_lane_waits_total")-waits)

	require.Less(t, longest, time.Second, "no take waits anywhere near the allocator's 5 s take wait")
}

// BenchmarkQuickLaneRows is what quick validation adds per block at shadow, on blocks the size of
// mainnet blocks 635,141 and 804,142, the largest of 2019 to 2021 and the largest below 945,000.
func BenchmarkQuickLaneRows(b *testing.B) {
	tSettings := testutil.CreateBaseTestSettings(b)
	tSettings.UtxoStore.LaneMigration = settings.LaneMigrationShadow

	bv := &BlockValidation{logger: ulogger.TestLogger{}, settings: tSettings}

	for _, blk := range []struct {
		name string
		txs  int
	}{
		{"635141", 1_324_314},
		{"804142", 7_126_502},
	} {
		b.Run(blk.name, func(b *testing.B) {
			block := &model.Block{Subtrees: []*chainhash.Hash{{}}, TransactionCount: uint64(blk.txs) + 1} //nolint:gosec // a test count

			list := make([]utxo.SpendAndCreateMultiResult, defaultNetRange)
			metas := make([]meta.Data, defaultNetRange)

			for i := range list {
				list[i] = utxo.SpendAndCreateMultiResult{Status: utxo.MultiTxCreated, Meta: &metas[i]}
			}

			var heap uint64

			var rowBytes int

			b.ReportAllocs()
			b.ResetTimer()

			for range b.N {
				ctx := bv.withQuickLanePositions(context.Background())
				q := quickLanePositionsFrom(ctx)

				var position uint64

				for done := 0; done < blk.txs; done += len(list) {
					n := min(len(list), blk.txs-done)

					for i := range n {
						position++
						if position%10_000 == 0 {
							position++
						}

						metas[i].LanePosition = lane.Pack(7, position)
					}

					q.addCreated(list[:n])
				}

				rows := bv.quickLaneRows(ctx, block)

				b.StopTimer()

				heap = 0
				for _, bm := range q.lanes {
					heap += bm.GetSizeInBytes()
				}

				rowBytes = 0
				for _, r := range rows {
					rowBytes += lane.RowFixedBytes + len(r.HoleData)
				}

				b.StartTimer()
			}

			b.ReportMetric(float64(heap)/(1<<20), "bitmap-MB")
			b.ReportMetric(float64(rowBytes), "row-bytes")
		})
	}
}
