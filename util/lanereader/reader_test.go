package lanereader

import (
	"context"
	"net/url"
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	"github.com/RoaringBitmap/roaring/v2/roaring64"
	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/settings"
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

// add stores a block on parent (genesis when nil) with rows. It holds a transaction besides its
// coinbase, so with no rows it stands for a block quick validation took.
func (c *chain) add(parent *stored, rows ...[]lane.MapRow) *stored {
	c.t.Helper()

	return c.addBlock(parent, 2, rows...)
}

// addCoinbaseOnly stores a block of only its coinbase on parent, which has no rows to hold.
func (c *chain) addCoinbaseOnly(parent *stored) *stored {
	c.t.Helper()

	return c.addBlock(parent, 1)
}

func (c *chain) addBlock(parent *stored, txCount uint64, rows ...[]lane.MapRow) *stored {
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
		TransactionCount: txCount,
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

func (f failingSource) GetBlockHeader(context.Context, *chainhash.Hash) (*model.BlockHeader, *model.BlockHeaderMeta, error) {
	f.t.Error("GetBlockHeader called at off")

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

	_, ok := prepareView(r, t.Context(), chainhash.Hash{})
	require.False(t, ok)

	initMetrics()

	total := func() float64 {
		var sum float64

		for _, reader := range readers {
			for _, k := range kinds {
				for _, v := range []lane.Via{lane.ViaNone, lane.ViaRow, lane.ViaFloor} {
					sum += answers(reader, k, v)
				}
			}
		}

		return sum
	}

	before := total()

	r.BlockParents(t.Context(), chainhash.Hash{}).ObserveParent(chainhash.Hash{}, lane.Pack(1, 1), []uint32{1}, false)
	r.CompareHeight(ReaderValidator, chainhash.Hash{}, lane.Pack(1, 1), true, 1)
	r.CompareBlocks(ReaderBlockParent, nil, chainhash.Hash{}, lane.Pack(1, 1), []uint32{1})
	r.compareDuplicates(nil, KindNotReady, chainhash.Hash{}, 1, false)
	r.ShadowDuplicates(t.Context(), chainhash.Hash{}, 1, false)

	require.Equal(t, before, total(), "a nil reader counts nothing")
}

// Each kind of comparison is counted once, under its reader and what answered.
func TestReaderCountsEachKind(t *testing.T) {
	engines(t, func(t *testing.T, store blockchainstore.Store) {
		c := newChain(t, store)

		// b1 takes lane 3's 1 to 10; b2 takes 11 to 20 but not 15; b3 has no rows.
		b1 := c.add(nil, takes(t, 3, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10))
		b2 := c.add(b1, takes(t, 3, 11, 12, 13, 14, 16, 17, 18, 19, 20))
		b3 := c.add(b2)
		// b4 holds only its coinbase, so it has nothing for a row to list.
		b4 := c.addCoinbaseOnly(b3)

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
			// Two blocks on the chain: the row's block first or second still agrees, and the
			// first block decides no_rows.
			{KindAgree, lane.ViaRow, lane.Pack(3, 5), []uint32{b1.id, b2.id}},
			{KindAgree, lane.ViaRow, lane.Pack(3, 5), []uint32{b2.id, b1.id}},
			{KindNoRows, lane.ViaNone, lane.Pack(3, 99), []uint32{b3.id, b2.id}},
			{KindTodayOnly, lane.ViaNone, lane.Pack(3, 99), []uint32{b2.id, b3.id}},
			// A block of only its coinbase has complete rows, so a position no row takes is
			// judged, not no_rows. Only its coinbase can hold one, which the parent-check
			// coinbase kind counts apart once it lands.
			{KindTodayOnly, lane.ViaNone, lane.Pack(3, 99), []uint32{b4.id}},
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

// A block's rows commit with it and today's mined write lands seconds later: until that write has
// finished, a lanes_only answer from the best block's row is counted mined_pending.
func TestReaderCountsTheMinedWriteWindowApart(t *testing.T) {
	engines(t, func(t *testing.T, store blockchainstore.Store) {
		ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
		defer cancel()

		c := newChain(t, store)

		b1 := c.add(nil, takes(t, 3, 5))
		b2 := c.add(b1, takes(t, 3, 6))
		require.NoError(t, store.SetBlockMinedSet(ctx, &b1.hash, nil))

		r := newReader(ulogger.TestLogger{}, store, 100)
		clock := storedAt(t, store, b2.hash)
		r.now = func() time.Time { return clock }
		follow(t, r)

		// b2 is the best block, and its mined write has not finished.
		requireCounted(t, ReaderValidator, KindMinedPending, lane.ViaRow, func() {
			r.CompareHeight(ReaderValidator, chainhash.Hash{}, lane.Pack(3, 6), false, 0)
		})
		requireCounted(t, ReaderSubtreePrefetch, KindMinedPending, lane.ViaRow, func() {
			r.CompareBlocks(ReaderSubtreePrefetch, r.MainView(), chainhash.Hash{}, lane.Pack(3, 6), nil)
		})

		// b1's has finished, so its row against today's unmined is a disagreement.
		requireCounted(t, ReaderValidator, KindLanesOnly, lane.ViaRow, func() {
			r.CompareHeight(ReaderValidator, chainhash.Hash{}, lane.Pack(3, 5), false, 0)
		})

		// Once b2's has finished too, so is its row's.
		require.NoError(t, store.SetBlockMinedSet(ctx, &b2.hash, nil))

		_, err := r.step(ctx, b2.hash)
		require.NoError(t, err)

		requireCounted(t, ReaderValidator, KindLanesOnly, lane.ViaRow, func() {
			r.CompareHeight(ReaderValidator, chainhash.Hash{}, lane.Pack(3, 6), false, 0)
		})

		// A cleared flag on the same best block, as a reorg's clear leaves it, counts pending again.
		require.NoError(t, store.ClearBlockMinedSet(ctx, &b2.hash))

		_, err = r.step(ctx, b2.hash)
		require.NoError(t, err)

		requireCounted(t, ReaderValidator, KindMinedPending, lane.ViaRow, func() {
			r.CompareHeight(ReaderValidator, chainhash.Hash{}, lane.Pack(3, 6), false, 0)
		})

		require.NoError(t, store.SetBlockMinedSet(ctx, &b2.hash, nil))

		// b3 arrives with its mined write unfinished; then a read of its flag fails, or comes back
		// empty. The failure is counted and leaves no block pending.
		b3 := c.add(b2, takes(t, 3, 7))
		stored3 := storedAt(t, store, b3.hash)
		clock = stored3

		for _, failing := range []Source{headerFailingSource{store}, headerMissingSource{store}} {
			r.source = store

			_, err = r.step(ctx, b2.hash)
			require.NoError(t, err)

			requireCounted(t, ReaderValidator, KindMinedPending, lane.ViaRow, func() {
				r.CompareHeight(ReaderValidator, chainhash.Hash{}, lane.Pack(3, 7), false, 0)
			})

			r.source = failing
			errorsBefore := testutil.ToFloat64(prometheusLaneReaderFollowErrors)

			_, err = r.step(ctx, b3.hash)
			require.NoError(t, err)
			require.Equal(t, errorsBefore+1, testutil.ToFloat64(prometheusLaneReaderFollowErrors))

			requireCounted(t, ReaderValidator, KindLanesOnly, lane.ViaRow, func() {
				r.CompareHeight(ReaderValidator, chainhash.Hash{}, lane.Pack(3, 7), false, 0)
			})
		}

		// b3 pending, then invalidated so b2 is best again: a view still holding b3 counts its row
		// lanes_only, since b3 is no longer the best block.
		r.source = store

		_, err = r.step(ctx, b3.hash)
		require.NoError(t, err)

		held := r.MainView()

		requireCounted(t, ReaderSubtreePrefetch, KindMinedPending, lane.ViaRow, func() {
			r.CompareBlocks(ReaderSubtreePrefetch, held, chainhash.Hash{}, lane.Pack(3, 7), nil)
		})

		_, err = store.InvalidateBlock(ctx, &b3.hash)
		require.NoError(t, err)

		_, err = r.step(ctx, b3.hash)
		require.NoError(t, err)

		requireCounted(t, ReaderSubtreePrefetch, KindLanesOnly, lane.ViaRow, func() {
			r.CompareBlocks(ReaderSubtreePrefetch, held, chainhash.Hash{}, lane.Pack(3, 7), nil)
		})

		// b4 is best and its write unfinished. Its rows count pending until a minute after it was
		// stored, with no poll needed, as when the best block cannot be read.
		b4 := c.add(b2, takes(t, 3, 8))
		stored4 := storedAt(t, store, b4.hash)
		clock = stored4

		_, err = r.step(ctx, b2.hash)
		require.NoError(t, err)

		clock = stored4.Add(59 * time.Second)

		requireCounted(t, ReaderValidator, KindMinedPending, lane.ViaRow, func() {
			r.CompareHeight(ReaderValidator, chainhash.Hash{}, lane.Pack(3, 8), false, 0)
		})

		clock = stored4.Add(60 * time.Second)

		requireCounted(t, ReaderValidator, KindLanesOnly, lane.ViaRow, func() {
			r.CompareHeight(ReaderValidator, chainhash.Hash{}, lane.Pack(3, 8), false, 0)
		})

		// b5 replaces b4 as best and is invalidated: b4 is best again, still past its minute.
		b5 := c.add(b4, takes(t, 3, 9))

		_, err = r.step(ctx, b4.hash)
		require.NoError(t, err)

		_, err = store.InvalidateBlock(ctx, &b5.hash)
		require.NoError(t, err)

		_, err = r.step(ctx, b5.hash)
		require.NoError(t, err)

		requireCounted(t, ReaderValidator, KindLanesOnly, lane.ViaRow, func() {
			r.CompareHeight(ReaderValidator, chainhash.Hash{}, lane.Pack(3, 8), false, 0)
		})
	})
}

// storedAt is when the store saved block hash, the start of its mined write's minute.
func storedAt(t *testing.T, store blockchainstore.Store, hash chainhash.Hash) time.Time {
	t.Helper()

	_, meta, err := store.GetBlockHeader(t.Context(), &hash)
	require.NoError(t, err)

	return time.Unix(int64(meta.Timestamp), 0)
}

// headerFailingSource is a Source whose GetBlockHeader fails.
type headerFailingSource struct{ blockchainstore.Store }

func (headerFailingSource) GetBlockHeader(context.Context, *chainhash.Hash) (*model.BlockHeader, *model.BlockHeaderMeta, error) {
	return nil, nil, errors.NewServiceUnavailableError("blockchain service down")
}

// headerMissingSource is a Source whose GetBlockHeader returns no metadata and no error.
type headerMissingSource struct{ blockchainstore.Store }

func (headerMissingSource) GetBlockHeader(context.Context, *chainhash.Hash) (*model.BlockHeader, *model.BlockHeaderMeta, error) {
	return nil, nil, nil
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

		// A store still reporting the replaced block's height is not a disagreement: its block at
		// that height, off the main chain now, took the position.
		requireCounted(t, ReaderValidator, KindOffChain, lane.ViaNone, func() {
			r.CompareHeight(ReaderValidator, chainhash.Hash{}, lane.Pack(4, 5), true, 2)
		})

		// Judged by block list, the replaced block is simply off the chain.
		requireCounted(t, ReaderSubtreePrefetch, KindAgree, lane.ViaNone, func() {
			r.CompareBlocks(ReaderSubtreePrefetch, v, chainhash.Hash{}, lane.Pack(4, 5), []uint32{b2.id})
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

		// Today's block at the anchor's height is settled too; one above it is not.
		requireCounted(t, ReaderValidator, KindAgree, lane.ViaFloor, func() {
			r.CompareHeight(ReaderValidator, chainhash.Hash{}, lane.Pack(2, 13), true, s.AnchorHeight())
		})
		requireCounted(t, ReaderValidator, KindDifferentBlock, lane.ViaFloor, func() {
			r.CompareHeight(ReaderValidator, chainhash.Hash{}, lane.Pack(2, 13), true, s.AnchorHeight()+1)
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

		forkView, ok := prepareView(r, context.Background(), fork.hash)
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

		v, ok := prepareView(r, context.Background(), b3.hash)
		require.True(t, ok)
		require.Equal(t, lane.Answer{Mined: true, Via: lane.ViaRow, BlockID: b2.id, Height: 2}, v.Lookup(lane.Pack(6, 2)))

		// A block re-taking b2's position 2 is counted as a duplicate on its chain.
		b4 := c.add(b3, takes(t, 6, 2, 4))

		v, ok = prepareView(r, context.Background(), b4.hash)
		require.True(t, ok)

		requireCounted(t, ReaderBlockDuplicate, KindAgree, lane.ViaRow, func() {
			r.compareDuplicates(v, KindNotReady, b4.hash, b4.id, true)
		})
		requireCounted(t, ReaderBlockDuplicate, KindLanesOnly, lane.ViaRow, func() {
			r.compareDuplicates(v, KindNotReady, b4.hash, b4.id, false)
		})

		v, ok = prepareView(r, context.Background(), b3.hash)
		require.True(t, ok)

		// setMined found a duplicate the index cannot see: its earlier block may be older than the
		// index's start.
		requireCounted(t, ReaderBlockDuplicate, KindNotLoaded, lane.ViaNone, func() {
			r.compareDuplicates(v, KindNotReady, b3.hash, b3.id, true)
		})
		requireCounted(t, ReaderBlockDuplicate, KindNoRows, lane.ViaNone, func() {
			r.compareDuplicates(v, KindNotReady, b1.hash, b1.id, false)
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
		bp.ObserveParent(chainhash.Hash{2}, lane.Pack(8, 9), []uint32{b2.id}, false)

		require.Eventually(t, func() bool {
			return answers(ReaderBlockParent, KindAgree, lane.ViaRow) == before+1
		}, 5*time.Second, 10*time.Millisecond)

		bp.ObserveParent(chainhash.Hash{3}, lane.Pack(8, 9), []uint32{b2.id}, false)
		require.Equal(t, before+2, answers(ReaderBlockParent, KindAgree, lane.ViaRow))

		// Block rows leave the coinbase out, so its position is in no row: a coinbase parent counts
		// coinbase, where an ordinary parent at such a position counts today_only.
		requireCounted(t, ReaderBlockParent, KindCoinbase, lane.ViaNone, func() {
			bp.ObserveParent(chainhash.Hash{4}, lane.Pack(8, 10), []uint32{b2.id}, true)
		})
		requireCounted(t, ReaderBlockParent, KindTodayOnly, lane.ViaNone, func() {
			bp.ObserveParent(chainhash.Hash{5}, lane.Pack(8, 10), []uint32{b2.id}, false)
		})

		// A coinbase parent needs no chain, so it is counted before the chain's rows are in.
		requireCounted(t, ReaderBlockParent, KindCoinbase, lane.ViaNone, func() {
			r.BlockParents(context.Background(), b2.hash).ObserveParent(chainhash.Hash{6}, lane.Pack(8, 10), []uint32{b2.id}, true)
		})
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

// A fork that leaves the main chain below the anchor cannot use the floors, and a parent mined in
// a folded block on it is counted not_loaded, not as a disagreement: its rows are no longer in
// memory.
func TestReaderCountsAFoldedAncestorOnAForkBelowTheAnchorNotLoaded(t *testing.T) {
	engines(t, func(t *testing.T, store blockchainstore.Store) {
		c := newChain(t, store)

		b1 := c.add(nil, takes(t, 2, 50))
		c.line(b1, 4)

		r := newReader(ulogger.TestLogger{}, store, 2)
		follow(t, r)
		require.True(t, r.Snapshot().Folded(b1.id))

		fork := c.add(b1)

		v, ok := prepareView(r, context.Background(), fork.hash)
		require.True(t, ok)
		require.False(t, v.FloorValid())

		requireCounted(t, ReaderBlockParent, KindNotLoaded, lane.ViaNone, func() {
			r.CompareBlocks(ReaderBlockParent, v, chainhash.Hash{}, lane.Pack(2, 50), []uint32{b1.id})
		})
	})
}

// A load waiting for another gives up when its caller's context ends.
func TestReaderPrepareGivesUpWaitingForAnotherLoad(t *testing.T) {
	store := newStore(t, "sqlite")
	c := newChain(t, store)
	b1 := c.add(nil)

	r := newReader(ulogger.TestLogger{}, store, 100)
	follow(t, r)

	b2 := c.add(b1)

	r.loading <- struct{}{} // another load holds the token

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	start := time.Now()
	_, ok := prepareView(r, ctx, b2.hash)
	require.False(t, ok)
	require.Less(t, time.Since(start), 5*time.Second)

	<-r.loading

	_, ok = prepareView(r, context.Background(), b2.hash)
	require.True(t, ok)
}

// setMined's backlog can reach a block already folded into the floors, whose rows' bitmaps have
// left memory: the duplicate check counts it not_loaded, whatever the verdict.
func TestReaderCountsTheDuplicateCheckOfAFoldedBlockNotLoaded(t *testing.T) {
	engines(t, func(t *testing.T, store blockchainstore.Store) {
		c := newChain(t, store)

		b1 := c.add(nil, takes(t, 2, 50))
		c.line(b1, 4)

		r := newReader(ulogger.TestLogger{}, store, 2)
		follow(t, r)
		require.True(t, r.Snapshot().Folded(b1.id))

		for _, verdict := range []bool{false, true} {
			requireCounted(t, ReaderBlockDuplicate, KindNotLoaded, lane.ViaNone, func() {
				r.compareDuplicates(r.MainView(), KindNotReady, b1.hash, b1.id, verdict)
			})
		}
	})
}

// Subtree validation's check judges on the block IDs of the chain it validates against, so a
// block a reorg replaced, still on that chain, answers by its row.
func TestReaderJudgesOnTheChainOfTheCallersBlockIDs(t *testing.T) {
	engines(t, func(t *testing.T, store blockchainstore.Store) {
		c := newChain(t, store)

		b1 := c.add(nil)
		b2 := c.add(b1, takes(t, 4, 5))

		r := newReader(ulogger.TestLogger{}, store, 100)
		follow(t, r)

		rival := c.add(b1)
		c.add(rival)
		follow(t, r)

		requireCounted(t, ReaderSubtreeAlreadyMined, KindAgree, lane.ViaRow, func() {
			r.CompareBlocks(ReaderSubtreeAlreadyMined, r.ViewOfIDs(map[uint32]bool{b1.id: true, b2.id: true}), chainhash.Hash{}, lane.Pack(4, 5), []uint32{b2.id})
		})
		requireCounted(t, ReaderSubtreeAlreadyMined, KindLanesOnly, lane.ViaRow, func() {
			r.CompareBlocks(ReaderSubtreeAlreadyMined, r.ViewOfIDs(map[uint32]bool{b1.id: true, b2.id: true}), chainhash.Hash{}, lane.Pack(4, 5), nil)
		})
	})
}

// countingSource counts the header reads a Reader makes.
type countingSource struct {
	Source
	headerReads int
}

func (c *countingSource) GetBlockHeaders(ctx context.Context, h *chainhash.Hash, n uint64) ([]*model.BlockHeader, []*model.BlockHeaderMeta, error) {
	c.headerReads++

	return c.Source.GetBlockHeaders(ctx, h, n)
}

// A fork whose older ancestry the index has forgotten cannot say a block is not on its chain: a
// parent mined in such a block, and a duplicate check finding nothing, count not_loaded.
func TestReaderCountsMissesOnADisconnectedForkNotLoaded(t *testing.T) {
	engines(t, func(t *testing.T, store blockchainstore.Store) {
		c := newChain(t, store)

		b1 := c.add(nil, takes(t, 2, 50, 60))
		b5 := c.line(b1, 4)

		r := newReader(ulogger.TestLogger{}, store, 2)
		follow(t, r) // anchor 3

		// A fork from b1, loaded for its validation, then the main chain moves on: the fork's
		// blocks at or below the new anchor are forgotten, f5 stays with no parent loaded.
		f := c.line(b1, 3)
		f5 := c.add(f, takes(t, 2, 50))

		_, ok := prepareView(r, context.Background(), f5.hash)
		require.True(t, ok)

		c.add(b5)
		follow(t, r) // anchor 4

		v, ok := prepareView(r, context.Background(), f5.hash)
		require.True(t, ok)
		require.True(t, v.Disconnected())

		requireCounted(t, ReaderBlockParent, KindNotLoaded, lane.ViaNone, func() {
			r.CompareBlocks(ReaderBlockParent, v, chainhash.Hash{}, lane.Pack(2, 60), []uint32{b1.id})
		})
		requireCounted(t, ReaderBlockDuplicate, KindNotLoaded, lane.ViaNone, func() {
			r.compareDuplicates(v, KindNotReady, f5.hash, f5.id, true)
		})
	})
}

// A block below the index's start counts not_loaded at once, without walking back for an
// ancestor the index cannot hold.
func TestReaderCountsABlockBelowItsStartNotLoadedWithoutAWalk(t *testing.T) {
	store := newStore(t, "sqlite")
	c := newChain(t, store)

	b1 := c.add(nil)
	b2 := c.add(b1, takes(t, 3, 1))
	c.line(b2, 8)

	source := &countingSource{Source: store}
	r := newReader(ulogger.TestLogger{}, source, 100)
	r.startBlocks = 3
	follow(t, r)
	require.Greater(t, r.Snapshot().StartHeight(), b2.height)

	reads := source.headerReads

	requireCounted(t, ReaderBlockDuplicate, KindNotLoaded, lane.ViaNone, func() {
		r.ShadowDuplicates(context.Background(), b2.hash, b2.id, false)
	})
	require.Equal(t, reads+1, source.headerReads, "one header read, no walk")

	state, _ := r.Snapshot().Block(b2.id)
	require.Equal(t, lane.BlockNotLoaded, state, "nothing below the start is loaded")
}

// Once a reorg's losing block leaves the index, a store still reporting its height cannot be told
// from the winner at that height: it counts not_loaded, not a disagreement.
func TestReaderCountsAForgottenForkHeightNotLoaded(t *testing.T) {
	engines(t, func(t *testing.T, store blockchainstore.Store) {
		c := newChain(t, store)

		b1 := c.add(nil)
		lost := c.add(b1, takes(t, 4, 5))

		r := newReader(ulogger.TestLogger{}, store, 2)
		follow(t, r)

		won := c.add(b1, takes(t, 4, 6))
		c.line(won, 3)
		follow(t, r)

		state, _ := r.Snapshot().Block(lost.id)
		require.Equal(t, lane.BlockNotLoaded, state, "the losing block is forgotten")
		require.True(t, r.Snapshot().ForkAt(2))

		requireCounted(t, ReaderValidator, KindNotLoaded, lane.ViaNone, func() {
			r.CompareHeight(ReaderValidator, chainhash.Hash{}, lane.Pack(4, 5), true, 2)
		})

		// The winner's own position at that height still agrees, below the floor.
		requireCounted(t, ReaderValidator, KindAgree, lane.ViaFloor, func() {
			r.CompareHeight(ReaderValidator, chainhash.Hash{}, lane.Pack(4, 6), true, 2)
		})
	})
}

// While a reorg's loser is still loaded with its rows, it is judged: a position neither it nor the
// winner took is a disagreement. A loaded loser with no rows cannot be judged.
func TestReaderJudgesALoadedForkHeightByItsRows(t *testing.T) {
	engines(t, func(t *testing.T, store blockchainstore.Store) {
		c := newChain(t, store)

		b1 := c.add(nil)
		c.add(b1, takes(t, 4, 5))
		lostNoRows := c.add(b1)

		r := newReader(ulogger.TestLogger{}, store, 100)
		follow(t, r)

		_, ok := prepareView(r, context.Background(), lostNoRows.hash)
		require.True(t, ok)

		won := c.add(b1, takes(t, 4, 6))
		c.line(won, 2)
		follow(t, r)

		require.False(t, r.Snapshot().ForkAt(3))

		// At height 2 the losers are one with rows and one without: the height cannot be judged.
		requireCounted(t, ReaderValidator, KindNotLoaded, lane.ViaNone, func() {
			r.CompareHeight(ReaderValidator, chainhash.Hash{}, lane.Pack(4, 7), true, 2)
		})
	})
}

// Only the loser with rows at a height: a position it did not take is judged, today_only.
func TestReaderCountsAPositionNoLoadedBlockAtTheHeightTookTodayOnly(t *testing.T) {
	engines(t, func(t *testing.T, store blockchainstore.Store) {
		c := newChain(t, store)

		b1 := c.add(nil)
		c.add(b1, takes(t, 4, 5))

		r := newReader(ulogger.TestLogger{}, store, 100)
		follow(t, r)

		won := c.add(b1, takes(t, 4, 6))
		c.line(won, 2)
		follow(t, r)

		require.False(t, r.Snapshot().ForkAt(2), "the loaded loser has rows")

		requireCounted(t, ReaderValidator, KindTodayOnly, lane.ViaNone, func() {
			r.CompareHeight(ReaderValidator, chainhash.Hash{}, lane.Pack(4, 7), true, 2)
		})
	})
}

// Subtree validation's block-ID set covers a recent window. A parent mined in a folded block
// outside it, answered by the floor, agrees, rather than reading as off the chain.
func TestReaderTreatsAFoldedBlockOutsideTheCallersWindowAsUnknown(t *testing.T) {
	engines(t, func(t *testing.T, store blockchainstore.Store) {
		c := newChain(t, store)

		b1 := c.add(nil, takes(t, 2, 50))
		b2 := c.add(b1)
		b3 := c.add(b2)
		b4 := c.add(b3)
		b5 := c.add(b4)

		r := newReader(ulogger.TestLogger{}, store, 2)
		follow(t, r) // anchor 3: b1 folded

		window := map[uint32]bool{b3.id: true, b4.id: true, b5.id: true}

		requireCounted(t, ReaderSubtreeAlreadyMined, KindAgree, lane.ViaFloor, func() {
			r.CompareBlocks(ReaderSubtreeAlreadyMined, r.ViewOfIDs(window), chainhash.Hash{}, lane.Pack(2, 50), []uint32{b1.id})
		})
	})
}

// On a chain given as a window of block IDs, a parent mined only in a rival block, loaded and not
// yet folded, is off the chain: both answers say not mined, and agree.
func TestReaderCountsAForkMinedParentOnAnIDWindowAgree(t *testing.T) {
	engines(t, func(t *testing.T, store blockchainstore.Store) {
		c := newChain(t, store)

		b1 := c.add(nil)
		b2 := c.add(b1)
		rival := c.add(b1, takes(t, 2, 70))

		r := newReader(ulogger.TestLogger{}, store, 100)
		follow(t, r)

		_, ok := prepareView(r, context.Background(), rival.hash)
		require.True(t, ok)

		requireCounted(t, ReaderSubtreeAlreadyMined, KindAgree, lane.ViaNone, func() {
			r.CompareBlocks(ReaderSubtreeAlreadyMined, r.ViewOfIDs(map[uint32]bool{b1.id: true, b2.id: true}), chainhash.Hash{}, lane.Pack(2, 70), []uint32{rival.id})
		})
	})
}

// failingHeaders fails every header read once failing is set.
type failingHeaders struct {
	Source
	failing bool
}

func (f *failingHeaders) GetBlockHeaders(ctx context.Context, h *chainhash.Hash, n uint64) ([]*model.BlockHeader, []*model.BlockHeaderMeta, error) {
	if f.failing {
		return nil, nil, errors.NewServiceError("blockchain service down")
	}

	return f.Source.GetBlockHeaders(ctx, h, n)
}

func prepareFailures(reason string) float64 {
	initMetrics()

	return testutil.ToFloat64(prometheusLaneReaderPrepareFailures.WithLabelValues(reason))
}

// A failed read while loading a block's ancestry is counted as a prepare failure, and the block's
// checks not_ready.
func TestReaderCountsAFailedAncestryReadAsAPrepareFailure(t *testing.T) {
	store := newStore(t, "sqlite")
	c := newChain(t, store)

	b1 := c.add(nil)

	source := &failingHeaders{Source: store}
	r := newReader(ulogger.TestLogger{}, source, 100)
	follow(t, r)

	b2 := c.add(b1)
	source.failing = true

	before := prepareFailures("read_error")

	requireCounted(t, ReaderBlockDuplicate, KindNotReady, lane.ViaNone, func() {
		r.ShadowDuplicates(context.Background(), b2.hash, b2.id, false)
	})
	require.Equal(t, before+1, prepareFailures("read_error"))
}

// A walk that finds no held block within its limit counts not_loaded and a prepare failure, and
// a later check of the same block does not walk again until the tip moves.
func TestReaderRemembersABlockWithNoHeldAncestor(t *testing.T) {
	store := newStore(t, "sqlite")
	c := newChain(t, store)

	b1 := c.add(nil)
	c.add(nil) // a side block, so block IDs and heights differ
	parent := c.line(b1, 7)
	tip := c.add(parent)
	require.NotEqual(t, tip.id, tip.height)

	source := &countingSource{Source: store}
	r := newReader(ulogger.TestLogger{}, source, 100)
	r.startBlocks, r.maxWalk, r.walkBatch = 3, 2, 2
	follow(t, r)
	require.Equal(t, tip.height-2, r.Snapshot().StartHeight())

	// A fork from b1, its tip above the index's start and below the main tip, with more blocks
	// back to a held one than the walk looks at.
	fork := c.line(b1, 7)

	before := prepareFailures("no_held_ancestor")

	requireCounted(t, ReaderBlockDuplicate, KindNotLoaded, lane.ViaNone, func() {
		r.ShadowDuplicates(context.Background(), fork.hash, fork.id, false)
	})
	require.Equal(t, before+1, prepareFailures("no_held_ancestor"))

	reads := source.headerReads

	requireCounted(t, ReaderBlockDuplicate, KindNotLoaded, lane.ViaNone, func() {
		r.ShadowDuplicates(context.Background(), fork.hash, fork.id, false)
	})
	require.Equal(t, reads, source.headerReads, "the same block at the same tip is not walked again")

	// A reorg to a sibling of the tip, at the same height: the block is walked again.
	sibling := c.add(parent)
	_, err := store.InvalidateBlock(context.Background(), &tip.hash)
	require.NoError(t, err)
	follow(t, r)
	require.Equal(t, sibling.id, r.Snapshot().TipID())
	require.Equal(t, tip.height, r.Snapshot().TipHeight())

	reads = source.headerReads

	r.ShadowDuplicates(context.Background(), fork.hash, fork.id, false)
	require.Greater(t, source.headerReads, reads, "after a same-height reorg the walk runs again")

	// Once the tip moves up, the block is walked again.
	c.add(sibling)
	follow(t, r)

	reads = source.headerReads

	r.ShadowDuplicates(context.Background(), fork.hash, fork.id, false)
	require.Greater(t, source.headerReads, reads, "after the tip moved the walk runs again")
}

// Two checks of the same unreachable block that overlap walk once: the second, waiting for the
// load token, finds the first's result when it gets it.
func TestReaderWalksAnUnreachableBlockOnceForOverlappingChecks(t *testing.T) {
	store := newStore(t, "sqlite")
	c := newChain(t, store)

	b1 := c.add(nil)
	c.line(b1, 8)

	source := &countingSource{Source: store}
	r := newReader(ulogger.TestLogger{}, source, 100)
	r.startBlocks, r.maxWalk, r.walkBatch = 3, 2, 2
	follow(t, r)

	fork := c.line(b1, 7)

	// Hold the load token, start the first check, and let it queue behind the token.
	require.True(t, r.lockLoad(context.Background()))

	reads := source.headerReads
	done := make(chan string, 2)

	for range 2 {
		go func() {
			_, kind := r.prepare(context.Background(), fork.hash)
			done <- kind
		}()
	}

	time.Sleep(50 * time.Millisecond)
	r.unlockLoad()

	require.Equal(t, KindNotLoaded, <-done)
	require.Equal(t, KindNotLoaded, <-done)
	require.Equal(t, reads+1, source.headerReads, "one walk, not two")
}

// failingRows fails every row read with a processing error, as a stored row that fails its check
// does.
type failingRows struct {
	Source
	failing bool
}

func (f *failingRows) LaneMapRows(ctx context.Context, id uint32) ([]lane.MapRow, error) {
	if f.failing {
		return nil, errors.NewServiceError("reading the lane map rows of block %d", id, errors.NewProcessingError("stored lane map row of block %d", id))
	}

	return f.Source.LaneMapRows(ctx, id)
}

// A processing error in an ancestry load is a read error, not a block below the index's start:
// it counts as a prepare failure, and the block's checks not_ready.
func TestReaderCountsAProcessingErrorInAnAncestryLoadAsAReadError(t *testing.T) {
	store := newStore(t, "sqlite")
	c := newChain(t, store)

	b1 := c.add(nil)

	source := &failingRows{Source: store}
	r := newReader(ulogger.TestLogger{}, source, 100)
	follow(t, r)

	b2 := c.add(b1, takes(t, 3, 1))
	source.failing = true

	before := prepareFailures("read_error")

	requireCounted(t, ReaderBlockDuplicate, KindNotReady, lane.ViaNone, func() {
		r.ShadowDuplicates(context.Background(), b2.hash, b2.id, false)
	})
	require.Equal(t, before+1, prepareFailures("read_error"))
}

// A block whose chain cannot be loaded has its held and later parents counted not_ready.
func TestBlockParentsCountsNotReadyWhenTheChainCannotLoad(t *testing.T) {
	store := newStore(t, "sqlite")
	c := newChain(t, store)
	c.add(nil)

	r := newReader(ulogger.TestLogger{}, store, 100)
	follow(t, r)

	before := answers(ReaderBlockParent, KindNotReady, lane.ViaNone)

	bp := r.BlockParents(context.Background(), chainhash.Hash{9, 9})
	bp.ObserveParent(chainhash.Hash{2}, lane.Pack(8, 9), []uint32{1}, false)

	require.Eventually(t, func() bool {
		return answers(ReaderBlockParent, KindNotReady, lane.ViaNone) == before+1
	}, 5*time.Second, 10*time.Millisecond)

	bp.ObserveParent(chainhash.Hash{3}, lane.Pack(8, 9), []uint32{1}, false)
	require.Equal(t, before+2, answers(ReaderBlockParent, KindNotReady, lane.ViaNone))
}

// badRows hands back a row the index refuses for block id bad.
type badRows struct {
	Source
	bad uint32
}

func (b *badRows) LaneMapRows(ctx context.Context, id uint32) ([]lane.MapRow, error) {
	if id == b.bad {
		return []lane.MapRow{{Lane: 3, First: 5, Last: 9, FirstHole: 6, Tag: lane.RowTagRoaring64, HoleData: []byte{1, 2, 3}}}, nil
	}

	return b.Source.LaneMapRows(ctx, id)
}

// A row the index refuses fails the follower's step, and the block is not loaded.
func TestReaderStepFailsOnARowTheIndexRefuses(t *testing.T) {
	store := newStore(t, "sqlite")
	c := newChain(t, store)

	b1 := c.add(nil)

	source := &badRows{Source: store, bad: ^uint32(0)}
	r := newReader(ulogger.TestLogger{}, source, 100)
	follow(t, r)

	b2 := c.add(b1)
	source.bad = b2.id

	_, err := r.step(context.Background(), chainhash.Hash{})
	require.Error(t, err)
	require.Contains(t, err.Error(), "does not parse", "the refused row's error, not a later one")

	state, _ := r.Snapshot().Block(b2.id)
	require.Equal(t, lane.BlockNotLoaded, state)
}

// A new tip with no held block within the walk's reach, as after a fast catch-up between polls,
// rebuilds the index on it instead of leaving it on the old tip.
func TestReaderRebuildsWhenTheNewTipIsOutOfReach(t *testing.T) {
	store := newStore(t, "sqlite")
	c := newChain(t, store)

	b1 := c.add(nil)

	r := newReader(ulogger.TestLogger{}, store, 100)
	r.maxWalk, r.walkBatch = 2, 2
	follow(t, r)

	b2 := c.add(b1, takes(t, 6, 3))
	tip := c.line(b2, 4)

	rebuilds := testutil.ToFloat64(prometheusLaneReaderRebuilds.WithLabelValues("gap"))

	follow(t, r)

	require.Equal(t, rebuilds+1, testutil.ToFloat64(prometheusLaneReaderRebuilds.WithLabelValues("gap")))
	require.Equal(t, tip.height, r.Snapshot().TipHeight())
	require.Equal(t, lane.Answer{Mined: true, Via: lane.ViaRow, BlockID: b2.id, Height: b2.height}, r.MainView().Lookup(lane.Pack(6, 3)))
}

// prepareView is prepare reporting whether a view came back, for the tests.
func prepareView(r *Reader, ctx context.Context, hash chainhash.Hash) (*lane.ChainView, bool) {
	v, _ := r.prepare(ctx, hash)

	return v, v != nil
}

// A block block validation loaded before the follower moved the tip is not a reorg's loser: a
// parent the store says is mined at its height counts not_loaded, not off_chain, until the tip
// moves.
func TestReaderDoesNotCountANewBlockAboveTheTipAsOffChain(t *testing.T) {
	store := newStore(t, "sqlite")
	c := newChain(t, store)

	b1 := c.add(nil)

	r := newReader(ulogger.TestLogger{}, store, 100)
	follow(t, r)

	b2 := c.add(b1, takes(t, 4, 5))

	r.ShadowDuplicates(context.Background(), b2.hash, b2.id, false) // loads b2 ahead of the follower

	requireCounted(t, ReaderValidator, KindNotLoaded, lane.ViaNone, func() {
		r.CompareHeight(ReaderValidator, chainhash.Hash{}, lane.Pack(4, 5), true, b2.height)
	})

	follow(t, r)

	requireCounted(t, ReaderValidator, KindAgree, lane.ViaRow, func() {
		r.CompareHeight(ReaderValidator, chainhash.Hash{}, lane.Pack(4, 5), true, b2.height)
	})
}

// gatedHeaders holds every header read until release is closed.
type gatedHeaders struct {
	Source
	release chan struct{}
}

func (g *gatedHeaders) GetBlockHeaders(ctx context.Context, h *chainhash.Hash, n uint64) ([]*model.BlockHeader, []*model.BlockHeaderMeta, error) {
	<-g.release

	return g.Source.GetBlockHeaders(ctx, h, n)
}

// While a block's chain loads, its parents are held up to maxPendingParents; one more counts
// not_ready at once, and the held ones are each answered once the chain is in.
func TestBlockParentsHoldsUpToItsCapWhileTheChainLoads(t *testing.T) {
	store := newStore(t, "sqlite")
	c := newChain(t, store)

	b1 := c.add(nil)

	source := &gatedHeaders{Source: store, release: make(chan struct{})}
	close(source.release)

	r := newReader(ulogger.TestLogger{}, source, 100)
	follow(t, r)

	b2 := c.add(b1, takes(t, 8, 9))
	source.release = make(chan struct{})

	notReady := answers(ReaderBlockParent, KindNotReady, lane.ViaNone)
	agree := answers(ReaderBlockParent, KindAgree, lane.ViaRow)

	bp := r.BlockParents(context.Background(), b2.hash)

	const held = 100_000 // the cap the design states, written out so a changed cap fails here

	for i := 0; i <= held; i++ {
		bp.ObserveParent(chainhash.Hash{2}, lane.Pack(8, 9), []uint32{b2.id}, false)
	}

	require.Equal(t, notReady+1, answers(ReaderBlockParent, KindNotReady, lane.ViaNone), "one past the cap counts not_ready")
	require.Equal(t, agree, answers(ReaderBlockParent, KindAgree, lane.ViaRow), "the held ones wait for the chain")

	close(source.release)

	require.Eventually(t, func() bool {
		return answers(ReaderBlockParent, KindAgree, lane.ViaRow) == agree+held
	}, 10*time.Second, 10*time.Millisecond)

	time.Sleep(50 * time.Millisecond)
	require.Equal(t, agree+held, answers(ReaderBlockParent, KindAgree, lane.ViaRow), "each held parent answered once")
}

// A load for block validation publishes the index's size, though the tip has not moved.
func TestPrepareUpdatesTheSizeGauges(t *testing.T) {
	store := newStore(t, "sqlite")
	c := newChain(t, store)

	b1 := c.add(nil)

	r := newReader(ulogger.TestLogger{}, store, 100)
	follow(t, r)

	c.add(b1)
	fork := c.add(b1, takes(t, 3, 4)) // a side block; the best tip stays the first block at its height

	_, ok := prepareView(r, context.Background(), fork.hash)
	require.True(t, ok)

	st := r.Snapshot().Stats()
	require.Equal(t, float64(st.Blocks), testutil.ToFloat64(prometheusLaneReaderIndexBlocks))
	require.Equal(t, float64(st.Rows), testutil.ToFloat64(prometheusLaneReaderIndexRows))
	require.Equal(t, float64(st.Bytes), testutil.ToFloat64(prometheusLaneReaderIndexBytes))
}

// The parent check's shadow gives up on its own deadline while another load holds the token, and
// counts its parents not_ready.
func TestBlockParentsGivesUpOnItsOwnDeadline(t *testing.T) {
	store := newStore(t, "sqlite")
	c := newChain(t, store)

	b1 := c.add(nil)

	r := newReader(ulogger.TestLogger{}, store, 100)
	follow(t, r)

	b2 := c.add(b1)

	old := prepareTimeout
	prepareTimeout = 100 * time.Millisecond

	t.Cleanup(func() { prepareTimeout = old })

	r.loading <- struct{}{} // another load holds the token, and never lets go

	t.Cleanup(func() { <-r.loading })

	before := answers(ReaderBlockParent, KindNotReady, lane.ViaNone)

	bp := r.BlockParents(context.Background(), b2.hash)
	bp.ObserveParent(chainhash.Hash{2}, lane.Pack(8, 9), []uint32{b2.id}, false)

	require.Eventually(t, func() bool {
		return answers(ReaderBlockParent, KindNotReady, lane.ViaNone) == before+1
	}, 3*time.Second, 10*time.Millisecond)
}

// hangingSource holds the named read until its context ends.
type hangingSource struct {
	Source
	hang atomic.Value // the method name to hold, or ""
}

func (h *hangingSource) hold(ctx context.Context, method string) error {
	if name, _ := h.hang.Load().(string); name != method {
		return nil
	}

	<-ctx.Done()

	return ctx.Err()
}

func (h *hangingSource) GetBestBlockHeader(ctx context.Context) (*model.BlockHeader, *model.BlockHeaderMeta, error) {
	if err := h.hold(ctx, "best"); err != nil {
		return nil, nil, err
	}

	return h.Source.GetBestBlockHeader(ctx)
}

func (h *hangingSource) GetBlockHeaders(ctx context.Context, hash *chainhash.Hash, n uint64) ([]*model.BlockHeader, []*model.BlockHeaderMeta, error) {
	if err := h.hold(ctx, "headers"); err != nil {
		return nil, nil, err
	}

	return h.Source.GetBlockHeaders(ctx, hash, n)
}

func (h *hangingSource) LaneMapRows(ctx context.Context, id uint32) ([]lane.MapRow, error) {
	if err := h.hold(ctx, "rows"); err != nil {
		return nil, err
	}

	return h.Source.LaneMapRows(ctx, id)
}

// within runs fn and fails the test if it has not returned within five seconds.
func within(t *testing.T, fn func()) {
	t.Helper()

	done := make(chan struct{})

	go func() {
		defer close(done)

		fn()
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("a hung read was not given up")
	}
}

// Each read the follower and a block's ancestry load make has its own deadline, so a hung read
// ends without the caller's context ending, gives the load token back, and the next step
// succeeds.
func TestReaderGivesUpAHungRead(t *testing.T) {
	saved := readTimeout
	readTimeout = 50 * time.Millisecond

	t.Cleanup(func() { readTimeout = saved })

	tokenFree := func(t *testing.T, r *Reader) {
		t.Helper()

		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()

		require.True(t, r.lockLoad(ctx), "the load token is given back")
		r.unlockLoad()
	}

	for _, method := range []string{"best", "headers", "rows"} {
		t.Run("follower "+method, func(t *testing.T) {
			store := newStore(t, "sqlite")
			c := newChain(t, store)
			c.line(c.add(nil, takes(t, 2, 7)), 3)

			source := &hangingSource{Source: store}
			source.hang.Store(method)

			r := newReader(ulogger.TestLogger{}, source, 100)

			within(t, func() {
				_, err := r.step(context.Background(), chainhash.Hash{})
				require.Error(t, err)
			})
			tokenFree(t, r)

			source.hang.Store("")
			follow(t, r)
		})
	}

	t.Run("ancestry walk", func(t *testing.T) {
		store := newStore(t, "sqlite")
		c := newChain(t, store)

		b1 := c.add(nil)
		c.line(b1, 8)

		source := &hangingSource{Source: store}
		r := newReader(ulogger.TestLogger{}, source, 100)
		r.startBlocks = 3
		follow(t, r)

		fork := c.line(b1, 7)

		source.hang.Store("headers")

		within(t, func() {
			_, kind := r.prepare(context.Background(), fork.hash)
			require.Equal(t, KindNotReady, kind)
		})
		tokenFree(t, r)

		source.hang.Store("")

		_, kind := r.prepare(context.Background(), fork.hash)
		require.NotEqual(t, KindNotReady, kind)
	})
}

// A walk back whose batch is full and ends at genesis has reached the start of the chain: it loads
// the blocks it read rather than asking for genesis's parent.
func TestReaderWalkEndsAtGenesisInAFullBatch(t *testing.T) {
	store := newStore(t, "sqlite")
	c := newChain(t, store)

	c.line(c.add(nil), 11) // the main chain, 12 blocks

	r := newReader(ulogger.TestLogger{}, store, 100)
	r.startBlocks = 3
	follow(t, r)
	require.Equal(t, uint32(10), r.Snapshot().StartHeight())

	// A fork of 10 blocks from genesis: the walk from its tip reads them and genesis, 11 headers,
	// exactly one batch.
	fork := c.line(c.add(nil, takes(t, 4, 3)), 9)
	require.Equal(t, uint32(10), fork.height)

	r.maxWalk, r.walkBatch = 11, 11

	v, kind := r.prepare(context.Background(), fork.hash)
	require.Empty(t, kind)
	require.NotNil(t, v)
	require.True(t, v.Lookup(lane.Pack(4, 3)).Mined, "the fork's rows are loaded")
}

// While the source connects, the Reader is already the process's, and its parent checks count
// not_ready; a failed connect is retried, and once one succeeds the Reader follows the chain.
func TestReaderCountsNotReadyWhileItConnects(t *testing.T) {
	store := newStore(t, "sqlite")
	c := newChain(t, store)
	b1 := c.add(nil, takes(t, 2, 7))

	tSettings := test.CreateBaseTestSettings(t)
	tSettings.UtxoStore.LaneMigration = settings.LaneMigrationShadow

	t.Cleanup(func() { Start(context.Background(), nil, test.CreateBaseTestSettings(t), nil) })

	var attempts atomic.Int32

	release := make(chan struct{})

	r := StartConnecting(t.Context(), ulogger.TestLogger{}, tSettings, func(ctx context.Context) (Source, error) {
		if attempts.Add(1) == 1 {
			return nil, errors.NewServiceError("not yet")
		}

		select {
		case <-release:
			return store, nil
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}, 10*time.Millisecond)

	require.Same(t, r, Current())

	requireCounted(t, ReaderValidator, KindNotReady, lane.ViaNone, func() {
		Current().CompareHeight(ReaderValidator, chainhash.Hash{}, lane.Pack(2, 7), true, b1.height)
	})

	require.Eventually(t, func() bool { return attempts.Load() >= 2 }, 5*time.Second, 5*time.Millisecond, "a failed connect is retried")

	close(release)

	require.Eventually(t, func() bool { return r.Snapshot() != nil }, 5*time.Second, 10*time.Millisecond)

	requireCounted(t, ReaderValidator, KindAgree, lane.ViaRow, func() {
		Current().CompareHeight(ReaderValidator, chainhash.Hash{}, lane.Pack(2, 7), true, b1.height)
	})
}
