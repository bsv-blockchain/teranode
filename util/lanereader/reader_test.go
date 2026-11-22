package lanereader

import (
	"context"
	"net/url"
	"runtime"
	"testing"
	"time"

	"github.com/RoaringBitmap/roaring/v2/roaring64"
	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/model"
	blockchainstore "github.com/bsv-blockchain/teranode/stores/blockchain"
	"github.com/bsv-blockchain/teranode/stores/blockchain/options"
	blockchainsql "github.com/bsv-blockchain/teranode/stores/blockchain/sql"
	"github.com/bsv-blockchain/teranode/test/utils/postgres"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/lane"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

const coinbaseHex = "01000000010000000000000000000000000000000000000000000000000000000000000000ffffffff17030100002f6d312d65752f29c267ffea1adb87f33b398fffffffff03ac505763000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88acaa505763000000001976a9143c22b6d9ba7b50b6d6e615c69d11ecb2ba3db14588acaa505763000000001976a914b7177c7deb43f3869eabc25cfd9f618215f34d5588ac00000000"

// engines runs fn against a real SQLite blockchain store and a real Postgres one in a container.
func engines(t *testing.T, fn func(t *testing.T, store blockchainstore.Store)) {
	for _, engine := range []string{"sqlite", "postgres"} {
		t.Run(engine, func(t *testing.T) {
			fn(t, newStore(t, engine))
		})
	}
}

func newStore(t *testing.T, engine string) blockchainstore.Store {
	t.Helper()

	storeURL := "sqlitememory:///"

	if engine == "postgres" {
		connStr, teardown, err := postgres.SetupTestPostgresContainer()
		if err != nil {
			t.Skipf("PostgreSQL container not available: %v", err)
		}

		t.Cleanup(func() { _ = teardown() })

		storeURL = connStr
	}

	u, err := url.Parse(storeURL)
	require.NoError(t, err)

	s, err := blockchainsql.New(ulogger.TestLogger{}, u, test.CreateBaseTestSettings(t))
	require.NoError(t, err)

	t.Cleanup(func() { _ = s.Close(context.Background()) })

	return s
}

// chain stores blocks for a test.
type chain struct {
	t       *testing.T
	store   blockchainstore.Store
	nonce   uint32
	genesis *model.BlockHeader
}

func newChain(t *testing.T, store blockchainstore.Store) *chain {
	header, _, err := store.GetBestBlockHeader(context.Background())
	require.NoError(t, err)

	return &chain{t: t, store: store, nonce: 1_000, genesis: header}
}

// stored is a block a test stored.
type stored struct {
	hash   chainhash.Hash
	id     uint32
	height uint32
}

// add stores a block on parent (genesis when nil) with rows.
func (c *chain) add(parent *stored, rows ...[]lane.MapRow) *stored {
	c.t.Helper()

	coinbase, err := bt.NewTxFromString(coinbaseHex)
	require.NoError(c.t, err)

	bits, err := model.NewNBitFromString("207fffff")
	require.NoError(c.t, err)

	prev, height := c.genesis.Hash(), uint32(1)
	if parent != nil {
		prev, height = &parent.hash, parent.height+1
	}

	c.nonce++

	blk := &model.Block{
		Header: &model.BlockHeader{
			Version:        1,
			Timestamp:      1729259727 + c.nonce,
			Nonce:          c.nonce,
			HashPrevBlock:  prev,
			HashMerkleRoot: &chainhash.Hash{},
			Bits:           *bits,
		},
		Height:           height,
		CoinbaseTx:       coinbase,
		TransactionCount: 1,
	}

	var all []lane.MapRow
	for _, r := range rows {
		all = append(all, r...)
	}

	var opts []options.StoreBlockOption
	if len(all) > 0 {
		opts = append(opts, options.WithLaneMapRows(all))
	}

	id, h, err := c.store.StoreBlock(context.Background(), blk, "", opts...)
	require.NoError(c.t, err)

	return &stored{hash: *blk.Hash(), id: uint32(id), height: h} //nolint:gosec // test block IDs are small
}

// line stores n blocks on parent without rows and returns the last.
func (c *chain) line(parent *stored, n int) *stored {
	for range n {
		parent = c.add(parent)
	}

	return parent
}

func takes(t *testing.T, laneNum uint8, positions ...uint64) []lane.MapRow {
	t.Helper()

	bm := roaring64.New()
	bm.AddMany(positions)

	rows, err := lane.BuildMapRows(laneNum, bm)
	require.NoError(t, err)

	return rows
}

// follow runs one follower step and requires it to reach the best block.
func follow(t *testing.T, r *Reader) {
	t.Helper()

	header, _, err := r.source.GetBestBlockHeader(context.Background())
	require.NoError(t, err)

	tip, err := r.step(context.Background(), chainhash.Hash{})
	require.NoError(t, err)
	require.Equal(t, *header.Hash(), tip)
}

func answers(reader, kind string, via lane.Via) float64 {
	initMetrics()

	return testutil.ToFloat64(answerCounters[reader][kind][via])
}

// requireCounted runs fn and requires it to count exactly one answer, of kind via via.
func requireCounted(t *testing.T, reader, kind string, via lane.Via, fn func()) {
	t.Helper()

	before := answers(reader, kind, via)

	total := func() float64 {
		var sum float64

		for _, k := range kinds {
			for _, v := range []lane.Via{lane.ViaNone, lane.ViaRow, lane.ViaFloor} {
				sum += answers(reader, k, v)
			}
		}

		return sum
	}

	all := total()

	fn()

	require.Equal(t, before+1, answers(reader, kind, via), "reader %s kind %s via %s", reader, kind, via)
	require.Equal(t, all+1, total(), "exactly one answer counted")
}

// failingSource fails the test on any call.
type failingSource struct{ t *testing.T }

func (f failingSource) GetBestBlockHeader(context.Context) (*model.BlockHeader, *model.BlockHeaderMeta, error) {
	f.t.Error("GetBestBlockHeader called at off")

	return nil, nil, nil
}

func (f failingSource) GetBlockHeaders(context.Context, *chainhash.Hash, uint64) ([]*model.BlockHeader, []*model.BlockHeaderMeta, error) {
	f.t.Error("GetBlockHeaders called at off")

	return nil, nil, nil
}

func (f failingSource) LaneMapRows(context.Context, uint32) ([]lane.MapRow, error) {
	f.t.Error("LaneMapRows called at off")

	return nil, nil
}

// At off there is no Reader: nothing is read, no goroutine runs, and every method does nothing.
func TestReaderAtOffDoesNothing(t *testing.T) {
	tSettings := test.CreateBaseTestSettings(t)
	require.False(t, tSettings.UtxoStore.LaneMigration.Stamps())

	goroutines := runtime.NumGoroutine()

	r := Start(t.Context(), ulogger.TestLogger{}, tSettings, failingSource{t: t})
	require.Nil(t, r)
	require.Nil(t, Current())

	time.Sleep(50 * time.Millisecond)
	require.LessOrEqual(t, runtime.NumGoroutine(), goroutines)

	require.Nil(t, r.Snapshot())
	require.Nil(t, r.MainView())
	require.Nil(t, r.BlockParents(t.Context(), chainhash.Hash{}))

	_, ok := r.Prepare(t.Context(), chainhash.Hash{})
	require.False(t, ok)

	r.CompareHeight(ReaderValidator, chainhash.Hash{}, lane.Pack(1, 1), true, 1)
	r.CompareBlocks(ReaderBlockParent, nil, chainhash.Hash{}, lane.Pack(1, 1), []uint32{1})
	r.CompareDuplicates(nil, chainhash.Hash{}, 1, false)
}

// Each kind of comparison is counted once, under its reader and what answered.
func TestReaderCountsEachKind(t *testing.T) {
	engines(t, func(t *testing.T, store blockchainstore.Store) {
		c := newChain(t, store)

		// b1 takes lane 3's 1 to 10; b2 takes 11 to 20 but not 15; b3 has no rows.
		b1 := c.add(nil, takes(t, 3, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10))
		b2 := c.add(b1, takes(t, 3, 11, 12, 13, 14, 16, 17, 18, 19, 20))
		b3 := c.add(b2)

		r := newReader(ulogger.TestLogger{}, store, 100)

		requireCounted(t, ReaderValidator, KindNotReady, lane.ViaNone, func() {
			r.CompareHeight(ReaderValidator, chainhash.Hash{}, lane.Pack(3, 5), true, b1.height)
		})

		follow(t, r)

		h := chainhash.Hash{1}

		// The validator's form: today mined at a height, or not.
		cases := []struct {
			kind        string
			via         lane.Via
			pos         uint64
			todayMined  bool
			todayHeight uint32
		}{
			{KindAgree, lane.ViaRow, lane.Pack(3, 5), true, b1.height},
			{KindAgree, lane.ViaNone, lane.Pack(3, 15), false, 0},
			{KindLanesOnly, lane.ViaRow, lane.Pack(3, 5), false, 0},
			{KindTodayOnly, lane.ViaNone, lane.Pack(3, 15), true, b2.height},
			{KindDifferentBlock, lane.ViaRow, lane.Pack(3, 5), true, b2.height},
			{KindNoPosition, lane.ViaNone, 0, true, b1.height},
			{KindNoRows, lane.ViaNone, lane.Pack(3, 99), true, b3.height},
			{KindNotLoaded, lane.ViaNone, lane.Pack(3, 99), true, 5_000},
		}

		for _, tc := range cases {
			requireCounted(t, ReaderValidator, tc.kind, tc.via, func() {
				r.CompareHeight(ReaderValidator, h, tc.pos, tc.todayMined, tc.todayHeight)
			})
		}

		// The block-list form, on the main chain.
		v := r.MainView()
		listCases := []struct {
			kind  string
			via   lane.Via
			pos   uint64
			today []uint32
		}{
			{KindAgree, lane.ViaRow, lane.Pack(3, 5), []uint32{b1.id}},
			{KindAgree, lane.ViaNone, lane.Pack(3, 15), nil},
			{KindLanesOnly, lane.ViaRow, lane.Pack(3, 5), nil},
			{KindTodayOnly, lane.ViaNone, lane.Pack(3, 15), []uint32{b2.id}},
			{KindDifferentBlock, lane.ViaRow, lane.Pack(3, 5), []uint32{b2.id}},
			{KindNoPosition, lane.ViaNone, 0, []uint32{b1.id}},
			{KindNoRows, lane.ViaNone, lane.Pack(3, 99), []uint32{b3.id}},
			{KindNotLoaded, lane.ViaNone, lane.Pack(3, 99), []uint32{b3.id + 1_000}},
		}

		for _, tc := range listCases {
			requireCounted(t, ReaderSubtreeAlreadyMined, tc.kind, tc.via, func() {
				r.CompareBlocks(ReaderSubtreeAlreadyMined, v, h, tc.pos, tc.today)
			})
		}

		requireCounted(t, ReaderBlockParent, KindNotReady, lane.ViaNone, func() {
			r.CompareBlocks(ReaderBlockParent, nil, h, lane.Pack(3, 5), []uint32{b1.id})
		})
	})
}

// A reorg moves the answer: once the fork is the main chain, its block's rows answer and the old
// block's do not.
func TestReaderFollowsAReorg(t *testing.T) {
	engines(t, func(t *testing.T, store blockchainstore.Store) {
		c := newChain(t, store)

		b1 := c.add(nil)
		b2 := c.add(b1, takes(t, 4, 5))

		r := newReader(ulogger.TestLogger{}, store, 100)
		follow(t, r)

		require.Equal(t, lane.Answer{Mined: true, Via: lane.ViaRow, BlockID: b2.id, Height: 2}, r.MainView().Lookup(lane.Pack(4, 5)))

		// A rival to b2 takes 6, and its child makes it the longer chain.
		rival := c.add(b1, takes(t, 4, 6))
		c.add(rival)
		follow(t, r)

		v := r.MainView()
		require.Equal(t, lane.Answer{}, v.Lookup(lane.Pack(4, 5)))
		require.Equal(t, lane.Answer{Mined: true, Via: lane.ViaRow, BlockID: rival.id, Height: 2}, v.Lookup(lane.Pack(4, 6)))

		requireCounted(t, ReaderValidator, KindAgree, lane.ViaRow, func() {
			r.CompareHeight(ReaderValidator, chainhash.Hash{}, lane.Pack(4, 6), true, 2)
		})
	})
}

// Below the floor a position is answered by the floor, and a listed one is not.
func TestReaderFloorAnswersBelowIt(t *testing.T) {
	engines(t, func(t *testing.T, store blockchainstore.Store) {
		c := newChain(t, store)

		b1 := c.add(nil, takes(t, 2, 10, 11, 13))
		c.line(b1, 3)

		r := newReader(ulogger.TestLogger{}, store, 2)
		follow(t, r)

		s := r.Snapshot()
		require.Equal(t, uint32(2), s.AnchorHeight())
		require.Equal(t, 0, s.Stats().Rows)

		v := r.MainView()
		require.Equal(t, lane.Answer{Mined: true, Via: lane.ViaFloor}, v.Lookup(lane.Pack(2, 11)))
		require.Equal(t, lane.Answer{}, v.Lookup(lane.Pack(2, 12)), "a hole stays listed")

		requireCounted(t, ReaderValidator, KindAgree, lane.ViaFloor, func() {
			r.CompareHeight(ReaderValidator, chainhash.Hash{}, lane.Pack(2, 13), true, b1.height)
		})
	})
}

// A deleted block's rows stay in the map but are never used, and a fork block's rows count only on
// the fork's own chain.
func TestReaderUsesOnlyRowsOfBlocksOnTheJudgedChain(t *testing.T) {
	engines(t, func(t *testing.T, store blockchainstore.Store) {
		c := newChain(t, store)

		b1 := c.add(nil)
		deleted := c.add(b1, takes(t, 5, 7))
		require.NoError(t, store.DeleteBlock(context.Background(), &deleted.hash))

		rows, err := store.LaneMapRows(context.Background(), deleted.id)
		require.NoError(t, err)
		require.NotEmpty(t, rows, "DeleteBlock leaves the block's rows")

		b2 := c.add(b1)
		c.add(b2)

		fork := c.add(b1, takes(t, 5, 8))

		r := newReader(ulogger.TestLogger{}, store, 100)
		follow(t, r)

		v := r.MainView()
		require.Equal(t, lane.Answer{}, v.Lookup(lane.Pack(5, 7)), "the deleted block's row")
		require.Equal(t, lane.Answer{}, v.Lookup(lane.Pack(5, 8)), "the fork block's row, off the main chain")

		forkView, ok := r.Prepare(context.Background(), fork.hash)
		require.True(t, ok)
		require.Equal(t, lane.Answer{Mined: true, Via: lane.ViaRow, BlockID: fork.id, Height: 2}, forkView.Lookup(lane.Pack(5, 8)))
		require.Equal(t, lane.Answer{}, forkView.Lookup(lane.Pack(5, 7)))
	})
}

// Block validation loads a block's ancestry itself, before the follower has moved.
func TestReaderPrepareLoadsAnAncestryTheFollowerHasNotSeen(t *testing.T) {
	engines(t, func(t *testing.T, store blockchainstore.Store) {
		c := newChain(t, store)

		b1 := c.add(nil)

		r := newReader(ulogger.TestLogger{}, store, 100)
		follow(t, r)

		b2 := c.add(b1, takes(t, 6, 1, 2))
		b3 := c.add(b2, takes(t, 6, 3))

		v, ok := r.Prepare(context.Background(), b3.hash)
		require.True(t, ok)
		require.Equal(t, lane.Answer{Mined: true, Via: lane.ViaRow, BlockID: b2.id, Height: 2}, v.Lookup(lane.Pack(6, 2)))

		// A block re-taking b2's position 2 is counted as a duplicate on its chain.
		b4 := c.add(b3, takes(t, 6, 2, 4))

		v, ok = r.Prepare(context.Background(), b4.hash)
		require.True(t, ok)

		requireCounted(t, ReaderBlockDuplicate, KindAgree, lane.ViaRow, func() {
			r.CompareDuplicates(v, b4.hash, b4.id, true)
		})
		requireCounted(t, ReaderBlockDuplicate, KindLanesOnly, lane.ViaRow, func() {
			r.CompareDuplicates(v, b4.hash, b4.id, false)
		})

		v, ok = r.Prepare(context.Background(), b3.hash)
		require.True(t, ok)

		requireCounted(t, ReaderBlockDuplicate, KindTodayOnly, lane.ViaRow, func() {
			r.CompareDuplicates(v, b3.hash, b3.id, true)
		})
		requireCounted(t, ReaderBlockDuplicate, KindNoRows, lane.ViaRow, func() {
			r.CompareDuplicates(v, b1.hash, b1.id, false)
		})
	})
}

// The parent check's shadow holds parents told of before the block's chain is loaded and answers
// them once it is.
func TestBlockParentsAnswersHeldParentsOnceTheChainIsIn(t *testing.T) {
	engines(t, func(t *testing.T, store blockchainstore.Store) {
		c := newChain(t, store)

		b1 := c.add(nil)

		r := newReader(ulogger.TestLogger{}, store, 100)
		follow(t, r)

		b2 := c.add(b1, takes(t, 8, 9))

		before := answers(ReaderBlockParent, KindAgree, lane.ViaRow)

		bp := r.BlockParents(context.Background(), b2.hash)
		bp.ObserveParent(chainhash.Hash{2}, lane.Pack(8, 9), []uint32{b2.id})

		require.Eventually(t, func() bool {
			return answers(ReaderBlockParent, KindAgree, lane.ViaRow) == before+1
		}, 5*time.Second, 10*time.Millisecond)

		bp.ObserveParent(chainhash.Hash{3}, lane.Pack(8, 9), []uint32{b2.id})
		require.Equal(t, before+2, answers(ReaderBlockParent, KindAgree, lane.ViaRow))
	})
}

// A reorg past rows already folded into the floors rebuilds the index on the new chain.
func TestReaderRebuildsOnAReorgPastTheAnchor(t *testing.T) {
	engines(t, func(t *testing.T, store blockchainstore.Store) {
		c := newChain(t, store)

		b1 := c.add(nil)
		old := c.add(b1, takes(t, 9, 1))
		c.line(old, 3)

		r := newReader(ulogger.TestLogger{}, store, 2)
		follow(t, r)
		require.Equal(t, lane.Answer{Mined: true, Via: lane.ViaFloor}, r.MainView().Lookup(lane.Pack(9, 1)))

		rebuilds := testutil.ToFloat64(prometheusLaneReaderRebuilds.WithLabelValues("deep_reorg"))

		// A rival chain from b1, longer than the old one, takes 2 and not 1.
		rival := c.add(b1, takes(t, 9, 2))
		c.line(rival, 5)
		follow(t, r)

		require.Equal(t, rebuilds+1, testutil.ToFloat64(prometheusLaneReaderRebuilds.WithLabelValues("deep_reorg")))

		v := r.MainView()
		require.Equal(t, lane.Answer{}, v.Lookup(lane.Pack(9, 1)))
		require.Equal(t, lane.Answer{Mined: true, Via: lane.ViaFloor}, v.Lookup(lane.Pack(9, 2)))
	})
}
