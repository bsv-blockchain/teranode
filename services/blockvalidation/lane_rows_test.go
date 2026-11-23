package blockvalidation

import (
	"context"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/RoaringBitmap/roaring/v2/roaring64"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/stores/blob"
	blobmemory "github.com/bsv-blockchain/teranode/stores/blob/memory"
	bloboptions "github.com/bsv-blockchain/teranode/stores/blob/options"
	blockchainoptions "github.com/bsv-blockchain/teranode/stores/blockchain/options"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/lane"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

func lanePositions(positions ...uint64) *roaring64.Bitmap {
	bm := roaring64.New()
	bm.AddMany(positions)

	return bm
}

// laneMetric sums a lane metric's samples whose labels include the given pair, or all of them
// when label is "": a counter's value, or a histogram's observation count.
func laneMetric(t *testing.T, metric, label, value string) float64 {
	t.Helper()

	families, err := prometheus.DefaultGatherer.Gather()
	require.NoError(t, err)

	var sum float64

	for _, f := range families {
		if f.GetName() != metric {
			continue
		}

		for _, m := range f.GetMetric() {
			match := label == ""

			for _, l := range m.GetLabel() {
				if l.GetName() == label && l.GetValue() == value {
					match = true
				}
			}

			if match && m.GetCounter() != nil {
				sum += m.GetCounter().GetValue()
			}

			if match && m.GetHistogram() != nil {
				sum += float64(m.GetHistogram().GetSampleCount())
			}
		}
	}

	return sum
}

// laneRowsFixture is a block of two subtrees whose lane bitmaps subtree validation saved: lane 7
// taken whole from 1 to 4 across both, and lane 9 with a hole at 2. Six members besides the
// coinbase.
func laneRowsFixture(t *testing.T, mode settings.LaneMigration) (*BlockValidation, *model.Block) {
	t.Helper()

	tSettings := test.CreateBaseTestSettings(t)
	tSettings.UtxoStore.LaneMigration = mode

	store := blobmemory.New()

	first, second := chainhash.Hash{0x51}, chainhash.Hash{0x52}

	for hash, lanes := range map[chainhash.Hash]lane.LaneBitmaps{
		first:  {7: lanePositions(1, 2), 9: lanePositions(1)},
		second: {7: lanePositions(3, 4), 9: lanePositions(3)},
	} {
		data, err := lane.EncodeSubtreeBitmaps(lane.SubtreeBitmaps{SubtreeHash: hash, Lanes: lanes})
		require.NoError(t, err)
		require.NoError(t, store.Set(context.Background(), hash[:], fileformat.FileTypeSubtreeLaneBitmaps, data))
	}

	u := &BlockValidation{logger: ulogger.TestLogger{}, settings: tSettings, subtreeStore: store}

	block := &model.Block{Subtrees: []*chainhash.Hash{&first, &second}, TransactionCount: 7}

	return u, block
}

func TestLaneRowsAreTheBlockMembersPerLane(t *testing.T) {
	u, block := laneRowsFixture(t, settings.LaneMigrationShadow)

	mismatches := laneMetric(t, "teranode_lane_row_count_mismatches_total", "", "")
	builds := laneMetric(t, "teranode_lane_gate_seconds", "stage", lane.GateStageBuild)

	rows := u.laneRows(context.Background(), block)
	require.Len(t, rows, 2)
	require.Equal(t, lane.MapRow{Lane: 7, First: 1, Last: 4, Tag: lane.RowTagRange}, rows[0])
	require.Equal(t, uint8(9), rows[1].Lane)
	require.Equal(t, uint64(2), rows[1].FirstHole)

	taken, err := rows[1].Check()
	require.NoError(t, err)
	require.Equal(t, []uint64{1, 3}, taken.ToArray())

	require.Equal(t, mismatches, laneMetric(t, "teranode_lane_row_count_mismatches_total", "", ""), "six members for seven transactions with the coinbase is no mismatch")
	require.Equal(t, builds+1, laneMetric(t, "teranode_lane_gate_seconds", "stage", lane.GateStageBuild), "the build stage is timed")

	// The rows go to AddBlock with the block.
	opts := blockchainoptions.ProcessStoreBlockOptions(u.buildAddBlockOpts(context.Background(), block)...)
	require.Equal(t, rows, opts.LaneMapRows)
}

func TestLaneRowsAreNotBuiltAtOff(t *testing.T) {
	u, block := laneRowsFixture(t, settings.LaneMigrationOff)

	require.Nil(t, u.laneRows(context.Background(), block))
	require.Nil(t, u.buildAddBlockOpts(context.Background(), block), "AddBlock gets the options it got without lanes")
}

func TestLaneRowsAreNotBuiltWithoutASubtreeStore(t *testing.T) {
	u, block := laneRowsFixture(t, settings.LaneMigrationShadow)
	u.subtreeStore = nil

	require.Nil(t, u.laneRows(context.Background(), block))
}

func TestLaneRowsCountAMismatchAndStillGoWithTheBlock(t *testing.T) {
	u, block := laneRowsFixture(t, settings.LaneMigrationShadow)

	for _, tc := range []struct {
		txs       uint64
		direction string
	}{
		{txs: 9, direction: lane.MismatchFewer}, // two transactions without a position
		{txs: 5, direction: lane.MismatchMore},
		{txs: 0, direction: lane.MismatchMore}, // no count at all, not a wrap past zero
	} {
		block.TransactionCount = tc.txs
		before := laneMetric(t, "teranode_lane_row_count_mismatches_total", "direction", tc.direction)

		require.Len(t, u.laneRows(context.Background(), block), 2)
		require.Equal(t, before+1, laneMetric(t, "teranode_lane_row_count_mismatches_total", "direction", tc.direction))
	}
}

// A block whose bitmaps files were all read but hold no position, as for subtrees of transactions
// cached before they had positions, has no rows to store, so AddBlock gets none, and it counts as
// fewer when it has transactions besides its coinbase. It is not counted as missing bitmaps.
func TestLaneRowsOfABlockWhoseBitmapsHoldNoPosition(t *testing.T) {
	u, block := laneRowsFixture(t, settings.LaneMigrationShadow)

	for _, h := range block.Subtrees {
		empty, err := lane.EncodeSubtreeBitmaps(lane.SubtreeBitmaps{SubtreeHash: *h})
		require.NoError(t, err)
		require.NoError(t, u.subtreeStore.Set(context.Background(), h[:], fileformat.FileTypeSubtreeLaneBitmaps, empty, withOverwrite()...))
	}

	block.TransactionCount = 3

	fewer := laneMetric(t, "teranode_lane_row_count_mismatches_total", "direction", lane.MismatchFewer)
	absent := laneMetric(t, "teranode_lane_missing_bitmaps_total", "reason", lane.MissingBitmapsAbsent)

	require.Nil(t, u.laneRows(context.Background(), block), "nil, so AddBlock is given no rows at all")
	require.Equal(t, fewer+1, laneMetric(t, "teranode_lane_row_count_mismatches_total", "direction", lane.MismatchFewer))
	require.Equal(t, absent, laneMetric(t, "teranode_lane_missing_bitmaps_total", "reason", lane.MissingBitmapsAbsent))
}

func TestLaneRowsSkipABlockWithMissingBitmaps(t *testing.T) {
	t.Run("absent", func(t *testing.T) {
		u, block := laneRowsFixture(t, settings.LaneMigrationShadow)

		third := chainhash.Hash{0x53}
		block.Subtrees = append(block.Subtrees, &third)

		before := laneMetric(t, "teranode_lane_missing_bitmaps_total", "reason", lane.MissingBitmapsAbsent)

		require.Nil(t, u.laneRows(context.Background(), block))
		require.Equal(t, before+1, laneMetric(t, "teranode_lane_missing_bitmaps_total", "reason", lane.MissingBitmapsAbsent))
	})

	t.Run("unreadable or another subtree's", func(t *testing.T) {
		u, block := laneRowsFixture(t, settings.LaneMigrationShadow)

		corrupt := *block.Subtrees[0]
		require.NoError(t, u.subtreeStore.Set(context.Background(), corrupt[:], fileformat.FileTypeSubtreeLaneBitmaps, []byte("LNSB"), withOverwrite()...))

		other, err := lane.EncodeSubtreeBitmaps(lane.SubtreeBitmaps{SubtreeHash: chainhash.Hash{0x99}, Lanes: lane.LaneBitmaps{7: lanePositions(3)}})
		require.NoError(t, err)

		wrong := *block.Subtrees[1]
		require.NoError(t, u.subtreeStore.Set(context.Background(), wrong[:], fileformat.FileTypeSubtreeLaneBitmaps, other, withOverwrite()...))

		before := laneMetric(t, "teranode_lane_missing_bitmaps_total", "reason", lane.MissingBitmapsUnreadable)

		require.Nil(t, u.laneRows(context.Background(), block))
		require.Equal(t, before+2, laneMetric(t, "teranode_lane_missing_bitmaps_total", "reason", lane.MissingBitmapsUnreadable))

		// The block is still stored, by the options it has without lanes.
		require.Nil(t, u.buildAddBlockOpts(context.Background(), block))
	})
}

func withOverwrite() []bloboptions.FileOption {
	return []bloboptions.FileOption{bloboptions.WithAllowOverwrite(true)}
}

// hungStore never returns from a Get, whatever its context does, until released: the file subtree
// store does not stop a read for its context once the read holds its read permit, so a hung disk
// looks like this.
type hungStore struct {
	blob.Store
	release chan struct{}
	once    sync.Once
	gets    atomic.Int32
}

func (s *hungStore) Get(_ context.Context, _ []byte, _ fileformat.FileType, _ ...bloboptions.FileOption) ([]byte, error) {
	s.gets.Add(1)
	<-s.release

	return nil, errors.NewStorageError("released")
}

// unhang lets every Get return, once.
func (s *hungStore) unhang() {
	s.once.Do(func() { close(s.release) })
}

// hungLaneRowsBlock is a shadow block of count subtrees in a store whose reads hang.
func hungLaneRowsBlock(t *testing.T, count int) (*BlockValidation, *model.Block, *hungStore) {
	t.Helper()

	u, block := laneRowsFixture(t, settings.LaneMigrationShadow)

	store := &hungStore{Store: u.subtreeStore, release: make(chan struct{})}
	t.Cleanup(store.unhang)

	u.subtreeStore = store

	block.Subtrees = nil
	for i := 0; i < count; i++ {
		h := chainhash.Hash{byte(i), 0xee}
		block.Subtrees = append(block.Subtrees, &h)
	}

	return u, block, store
}

// A hung subtree store holds up storing the block for at most the read deadline, even with more
// subtrees than reads run at once: every subtree not read by then is counted as timed out, not as an
// unreadable file, and the block goes without rows.
func TestLaneRowsGiveUpOnAHungSubtreeStore(t *testing.T) {
	const subtrees = laneBitmapReadConcurrency + 4

	u, block, store := hungLaneRowsBlock(t, subtrees)

	saved := laneBitmapReadTimeout
	laneBitmapReadTimeout = 50 * time.Millisecond

	t.Cleanup(func() { laneBitmapReadTimeout = saved })

	timedOut := laneMetric(t, "teranode_lane_missing_bitmaps_total", "reason", lane.MissingBitmapsTimedOut)
	unreadable := laneMetric(t, "teranode_lane_missing_bitmaps_total", "reason", lane.MissingBitmapsUnreadable)

	baseline := runtime.NumGoroutine()

	started := time.Now()
	require.Nil(t, u.laneRows(context.Background(), block))
	require.Less(t, time.Since(started), time.Second)

	require.Equal(t, timedOut+subtrees, laneMetric(t, "teranode_lane_missing_bitmaps_total", "reason", lane.MissingBitmapsTimedOut))
	require.Equal(t, unreadable, laneMetric(t, "teranode_lane_missing_bitmaps_total", "reason", lane.MissingBitmapsUnreadable))

	// The hung reads now finish, after laneRows has returned. Each reports without blocking and
	// frees its slot, and no read starts after the deadline: only the reads that held a slot then
	// ever reach the store, and every read goroutine ends.
	require.Equal(t, int32(laneBitmapReadConcurrency), store.gets.Load(), "the reads in flight at the deadline")

	// While those reads still hang, only their goroutines are left: the dispatcher has stopped.
	// A plain loop, not require.Eventually, which runs its condition in a goroutine of its own.
	hungOnly := baseline + laneBitmapReadConcurrency

	settle := time.Now().Add(5 * time.Second)
	for runtime.NumGoroutine() > hungOnly && time.Now().Before(settle) {
		time.Sleep(10 * time.Millisecond)
	}

	require.LessOrEqual(t, runtime.NumGoroutine(), hungOnly, "the dispatcher does not wait on a slot a hung read holds")

	store.unhang()

	// A plain loop, not require.Eventually, which runs its condition in a goroutine of its own.
	deadline := time.Now().Add(5 * time.Second)
	for runtime.NumGoroutine() > baseline && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}

	require.LessOrEqual(t, runtime.NumGoroutine(), baseline, "every late read reports and ends")
	require.Equal(t, int32(laneBitmapReadConcurrency), store.gets.Load(), "no read starts after the deadline")
}

// Reads stuck on a hung disk keep the file store's read permits, which every file read in the
// process shares, until the disk answers. The slots bound them across blocks, not per block: once
// one block has left laneBitmapReadConcurrency reads stuck, the next block starts no read at all,
// times out and goes without rows, so the stuck reads never hold more permits than that. Once they
// return, blocks read again.
func TestLaneRowsLeaveAtMostTheSlotsStuckAcrossBlocks(t *testing.T) {
	const subtrees = laneBitmapReadConcurrency + 4

	u, block, store := hungLaneRowsBlock(t, subtrees)

	saved := laneBitmapReadTimeout
	laneBitmapReadTimeout = 50 * time.Millisecond

	t.Cleanup(func() { laneBitmapReadTimeout = saved })

	require.Nil(t, u.laneRows(context.Background(), block))
	require.Equal(t, int32(laneBitmapReadConcurrency), store.gets.Load(), "the first block leaves a full pool of reads stuck")

	timedOut := laneMetric(t, "teranode_lane_missing_bitmaps_total", "reason", lane.MissingBitmapsTimedOut)

	next := &model.Block{Subtrees: block.Subtrees, TransactionCount: block.TransactionCount}

	started := time.Now()
	require.Nil(t, u.laneRows(context.Background(), next))
	require.Less(t, time.Since(started), time.Second)
	require.Equal(t, int32(laneBitmapReadConcurrency), store.gets.Load(), "the next block starts no read while the stuck ones hold every slot")
	require.Equal(t, timedOut+subtrees, laneMetric(t, "teranode_lane_missing_bitmaps_total", "reason", lane.MissingBitmapsTimedOut))

	// The disk answers: the stuck reads return and free their slots, and a block reads again.
	store.unhang()

	settle := time.Now().Add(5 * time.Second)
	for len(u.laneBitmapReadSlots()) > 0 && time.Now().Before(settle) {
		time.Sleep(10 * time.Millisecond)
	}

	require.Empty(t, u.laneBitmapReadSlots(), "every slot is free once the stuck reads return")

	require.Nil(t, u.laneRows(context.Background(), next), "the store now answers with an error, so still no rows")
	require.Equal(t, int32(laneBitmapReadConcurrency+subtrees), store.gets.Load(), "but every subtree was read")
}

// A block of more subtrees than are read at once still gets its rows, well inside the deadline:
// each finished read frees its slot for the next.
func TestLaneRowsReadMoreSubtreesThanRunAtOnce(t *testing.T) {
	const subtrees = laneBitmapReadConcurrency + 1

	u, block := laneRowsFixture(t, settings.LaneMigrationShadow)

	block.Subtrees = nil
	for i := 0; i < subtrees; i++ {
		h := chainhash.Hash{byte(i), 0xdd}
		data, err := lane.EncodeSubtreeBitmaps(lane.SubtreeBitmaps{SubtreeHash: h, Lanes: lane.LaneBitmaps{7: lanePositions(uint64(i) + 1)}})
		require.NoError(t, err)
		require.NoError(t, u.subtreeStore.Set(context.Background(), h[:], fileformat.FileTypeSubtreeLaneBitmaps, data))

		block.Subtrees = append(block.Subtrees, &h)
	}

	block.TransactionCount = subtrees + 1

	started := time.Now()
	rows := u.laneRows(context.Background(), block)
	require.Less(t, time.Since(started), time.Second)
	require.Equal(t, []lane.MapRow{{Lane: 7, First: 1, Last: subtrees, Tag: lane.RowTagRange}}, rows)
}

// The caller ending its context ends the reads too, before the deadline.
func TestLaneRowsStopWhenTheCallerGivesUp(t *testing.T) {
	u, block, _ := hungLaneRowsBlock(t, 2)

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	started := time.Now()
	require.Nil(t, u.laneRows(ctx, block))
	require.Less(t, time.Since(started), time.Second, "well before the 5 s deadline")
}

func TestLaneBitmapReadTimeoutIsFiveSeconds(t *testing.T) {
	require.Equal(t, 5*time.Second, laneBitmapReadTimeout)
}

// partlyHungStore hangs the reads of the subtrees it names and serves the rest.
type partlyHungStore struct {
	blob.Store
	hung    map[chainhash.Hash]bool
	release chan struct{}
}

func (s *partlyHungStore) Get(ctx context.Context, key []byte, fileType fileformat.FileType, opts ...bloboptions.FileOption) ([]byte, error) {
	if s.hung[chainhash.Hash(key)] {
		<-s.release
		return nil, errors.NewStorageError("released")
	}

	return s.Store.Get(ctx, key, fileType, opts...)
}

// When some reads finish and others hang, only the hung subtrees count as timed out.
func TestLaneRowsCountOnlyTheHungSubtreesAsTimedOut(t *testing.T) {
	u, block := laneRowsFixture(t, settings.LaneMigrationShadow)

	third := chainhash.Hash{0x53}
	block.Subtrees = append(block.Subtrees, &third)

	store := &partlyHungStore{Store: u.subtreeStore, hung: map[chainhash.Hash]bool{third: true}, release: make(chan struct{})}
	t.Cleanup(func() { close(store.release) })

	u.subtreeStore = store

	saved := laneBitmapReadTimeout
	laneBitmapReadTimeout = 200 * time.Millisecond

	t.Cleanup(func() { laneBitmapReadTimeout = saved })

	timedOut := laneMetric(t, "teranode_lane_missing_bitmaps_total", "reason", lane.MissingBitmapsTimedOut)

	require.Nil(t, u.laneRows(context.Background(), block))
	require.Equal(t, timedOut+1, laneMetric(t, "teranode_lane_missing_bitmaps_total", "reason", lane.MissingBitmapsTimedOut))
}

// stallingStore answers a Get only when its context ends, as a network store that honours its
// context does.
type stallingStore struct {
	blob.Store
}

func (s *stallingStore) Get(ctx context.Context, _ []byte, _ fileformat.FileType, _ ...bloboptions.FileOption) ([]byte, error) {
	<-ctx.Done()
	return nil, ctx.Err()
}

// The reads run under the deadline's context, so a store that honours it ends them at the deadline
// and every read goroutine ends with them.
func TestLaneRowsEndTheReadsOfAStoreThatHonoursTheDeadline(t *testing.T) {
	u, block := laneRowsFixture(t, settings.LaneMigrationShadow)
	u.subtreeStore = &stallingStore{Store: u.subtreeStore}

	saved := laneBitmapReadTimeout
	laneBitmapReadTimeout = 50 * time.Millisecond

	t.Cleanup(func() { laneBitmapReadTimeout = saved })

	baseline := runtime.NumGoroutine()

	require.Nil(t, u.laneRows(context.Background(), block))

	// A plain loop, not require.Eventually, which runs its condition in a goroutine of its own.
	deadline := time.Now().Add(5 * time.Second)
	for runtime.NumGoroutine() > baseline && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}

	require.LessOrEqual(t, runtime.NumGoroutine(), baseline, "every read ends with the deadline")
}

// With the caller's context already ended, a free slot and the ended context are both ready on
// every pass of the dispatcher, and its select picks between them at random. No read may start.
// One pass picks the ended context half the time, so the scenario runs 30 times: a dispatcher
// that could start a read gets past this test about once in a billion runs.
func TestLaneRowsStartNoReadOnceTheCallerHasGivenUp(t *testing.T) {
	for run := 0; run < 30; run++ {
		u, block, store := hungLaneRowsBlock(t, laneBitmapReadConcurrency+4)

		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		baseline := runtime.NumGoroutine()

		require.Nil(t, u.laneRows(ctx, block))

		// laneRows returns at once; let the dispatcher and any read it started run to the end
		// before counting. A plain loop, not require.Eventually, which runs its condition in a
		// goroutine.
		store.unhang()

		deadline := time.Now().Add(5 * time.Second)
		for runtime.NumGoroutine() > baseline && time.Now().Before(deadline) {
			time.Sleep(time.Millisecond)
		}

		require.LessOrEqual(t, runtime.NumGoroutine(), baseline, "run %d", run)
		require.Zero(t, store.gets.Load(), "run %d: no read starts after the context ended", run)
	}
}
