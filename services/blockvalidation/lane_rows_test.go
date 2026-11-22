package blockvalidation

import (
	"context"
	"testing"
	"time"

	"github.com/RoaringBitmap/roaring/v2/roaring64"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
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

// stallingStore holds every Get until its context ends, as a stalled subtree store would.
type stallingStore struct {
	blob.Store
}

func (s *stallingStore) Get(ctx context.Context, _ []byte, _ fileformat.FileType, _ ...bloboptions.FileOption) ([]byte, error) {
	<-ctx.Done()
	return nil, ctx.Err()
}

// A stalled subtree store holds up storing the block for at most the read deadline; the block then
// goes without rows, counted as timed out, not as unreadable files.
func TestLaneRowsGiveUpOnAStalledSubtreeStore(t *testing.T) {
	u, block := laneRowsFixture(t, settings.LaneMigrationShadow)
	u.subtreeStore = &stallingStore{Store: u.subtreeStore}

	saved := laneBitmapReadTimeout
	laneBitmapReadTimeout = 50 * time.Millisecond

	t.Cleanup(func() { laneBitmapReadTimeout = saved })

	timedOut := laneMetric(t, "teranode_lane_missing_bitmaps_total", "reason", lane.MissingBitmapsTimedOut)
	unreadable := laneMetric(t, "teranode_lane_missing_bitmaps_total", "reason", lane.MissingBitmapsUnreadable)

	started := time.Now()
	require.Nil(t, u.laneRows(context.Background(), block))
	require.Less(t, time.Since(started), 2*time.Second)

	require.Equal(t, timedOut+2, laneMetric(t, "teranode_lane_missing_bitmaps_total", "reason", lane.MissingBitmapsTimedOut))
	require.Equal(t, unreadable, laneMetric(t, "teranode_lane_missing_bitmaps_total", "reason", lane.MissingBitmapsUnreadable))
}
