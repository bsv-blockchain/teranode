package sql

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/bsv-blockchain/teranode/util/lane"
	"github.com/stretchr/testify/require"
)

// recordTestGrant writes one hand-out row the way a grant is recorded, at height.
func recordTestGrant(t *testing.T, s *SQL, l uint8, first, last uint64, height uint32) {
	t.Helper()

	ctx := context.Background()

	tx, err := s.db.BeginTx(ctx, nil)
	require.NoError(t, err)

	_, err = s.laneExec(ctx, tx, recordGrantSQL, int(l), lane.ToSigned(first), lane.ToSigned(last), "test", int64(height), nil,
		sql.NullInt64{}, sql.NullInt64{})
	require.NoError(t, err)
	require.NoError(t, tx.Commit())
}

// Each lane's first grant at or above the height is found by walking the lane's grants down its
// primary key, across pages; a lane whose grants are all older has none.
func TestLaneCapGrants(t *testing.T) {
	runLaneHandoutTests(t, "first grant at or above the height", func(t *testing.T, s *SQL) {
		// lane 1: grants at heights 90 to 105; the first at or above 100 starts at 2,001
		for i, h := range []uint32{90, 95, 100, 105} {
			recordTestGrant(t, s, 1, uint64(i)*1_000+1, uint64(i+1)*1_000, h)
		}

		// lane 2: only an idle grant older than the cap
		recordTestGrant(t, s, 2, 1, 10_000, 50)

		// lane 3: more grants than one page of the walk, heights 1 to 600
		for i := range 600 {
			recordTestGrant(t, s, 3, uint64(i)*10+1, uint64(i+1)*10, uint32(i+1))
		}

		// lane 4: every grant above the height, so its oldest
		recordTestGrant(t, s, 4, 1, 100, 200)
		recordTestGrant(t, s, 4, 101, 200, 201)

		// lane 255, the last lane
		recordTestGrant(t, s, 255, 1, 50, 150)

		got, err := s.LaneCapGrants(context.Background(), 100)
		require.NoError(t, err)
		require.Len(t, got, 4)
		require.Equal(t, uint8(255), got[3].Lane)

		require.Equal(t, uint8(1), got[0].Lane)
		require.Equal(t, uint64(2_001), got[0].First)
		require.Equal(t, uint32(100), got[0].Height)
		require.WithinDuration(t, time.Now(), got[0].HandedAt, time.Hour)

		require.Equal(t, uint8(3), got[1].Lane)
		require.Equal(t, uint64(991), got[1].First, "lane 3's grant at height 100")
		require.Equal(t, uint32(100), got[1].Height)

		require.Equal(t, uint8(4), got[2].Lane)
		require.Equal(t, uint64(1), got[2].First)

		require.True(t, got[0].Below(lane.Pack(1, 2_000)))
		require.False(t, got[0].Below(lane.Pack(1, 2_001)))

		none, err := s.LaneCapGrants(context.Background(), 1_000)
		require.NoError(t, err)
		require.Empty(t, none)

		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		_, err = s.LaneCapGrants(ctx, 100)
		require.Error(t, err, "a read that cannot run")
	})

	// A hand-out row whose values do not fit is refused, not passed on.
	for name, row := range map[string]struct {
		first  int64
		height int64
	}{
		"no first position":    {0, 200},
		"first above the lane": {int64(lane.MaxPosition) + 1, 200},
		"height above 32 bits": {1, int64(1) << 33},
	} {
		runLaneHandoutTests(t, "refuses "+name, func(t *testing.T, s *SQL) {
			ctx := context.Background()

			tx, err := s.db.BeginTx(ctx, nil)
			require.NoError(t, err)

			_, err = s.laneExec(ctx, tx, recordGrantSQL, 6, row.first, row.first+10, "test", row.height, nil, sql.NullInt64{}, sql.NullInt64{})
			require.NoError(t, err)
			require.NoError(t, tx.Commit())

			_, err = s.LaneCapGrants(ctx, 100)
			require.Error(t, err)
		})
	}
}
