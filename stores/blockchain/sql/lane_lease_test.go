package sql

import (
	"context"
	"database/sql"
	"math/rand/v2"
	"net/url"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/bsv-blockchain/teranode/stores/blockchain/options"
	"github.com/bsv-blockchain/teranode/test/utils/postgres"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util"
	"github.com/bsv-blockchain/teranode/util/lane"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/bsv-blockchain/teranode/util/usql"
	"github.com/stretchr/testify/require"
)

const leaseTestGrantMin = 10_000

// runLaneHandoutTests runs fn against SQLite in memory and, unless -short, Postgres.
func runLaneHandoutTests(t *testing.T, name string, fn func(t *testing.T, s *SQL)) {
	t.Helper()

	t.Run(name+"/sqlite", func(t *testing.T) {
		storeURL, err := url.Parse("sqlitememory:///")
		require.NoError(t, err)

		s, err := New(ulogger.TestLogger{}, storeURL, test.CreateBaseTestSettings(t))
		require.NoError(t, err)

		fn(t, s)
	})

	t.Run(name+"/postgres", func(t *testing.T) {
		if testing.Short() {
			t.Skip("skipping postgres-backed test in -short mode")
		}

		s, cleanup := setupLanePostgresStore(t)
		defer cleanup()

		fn(t, s)
	})
}

func setupLanePostgresStore(t *testing.T) (*SQL, func()) {
	t.Helper()

	connStr, teardown, err := postgres.SetupTestPostgresContainer()
	if err != nil {
		t.Skipf("PostgreSQL container not available: %v", err)
	}

	storeURL, err := url.Parse(connStr)
	require.NoError(t, err)

	s, err := New(ulogger.TestLogger{}, storeURL, test.CreateBaseTestSettings(t))
	require.NoError(t, err)

	return s, func() { _ = teardown() }
}

func leaseOpen(holder string) options.LaneLeaseOpen {
	return options.LaneLeaseOpen{Holder: holder, Height: 7, GrantMin: leaseTestGrantMin, CloseAfter: 3 * time.Minute}
}

func leaseRenew(id lane.LeaseID, g lane.Grant) options.LaneLeaseRenew {
	return options.LaneLeaseRenew{Lease: id, Lane: g.Lane, Height: 7, GrantMin: leaseTestGrantMin, GrantDuration: 2 * time.Minute}
}

func countRows(t *testing.T, s *SQL, query string, args ...any) int {
	t.Helper()

	var n int
	require.NoError(t, s.db.QueryRowContext(t.Context(), query, args...).Scan(&n))

	return n
}

// ageLeases moves every lease's renewed_at back by d, as if no renewal had come for that long.
func ageLeases(t *testing.T, s *SQL, d time.Duration) {
	t.Helper()

	var err error
	if s.engine == util.Postgres {
		_, err = s.db.ExecContext(t.Context(), `UPDATE lane_lease SET renewed_at = renewed_at - $1::bigint * interval '1 microsecond'`, d.Microseconds())
	} else {
		_, err = s.db.ExecContext(t.Context(), `UPDATE lane_lease SET renewed_at = strftime('%Y-%m-%d %H:%M:%f', renewed_at, $1)`, sqliteOffset(-d))
	}

	require.NoError(t, err)
}

func TestOpenLaneLeaseGrantsTheMinimumOnAFreshLane(t *testing.T) {
	runLaneHandoutTests(t, "fresh lane", func(t *testing.T, s *SQL) {
		id, g, _, err := s.OpenLaneLease(t.Context(), leaseOpen("a"))
		require.NoError(t, err)
		require.False(t, id.IsZero())
		require.Equal(t, lane.Grant{Lane: 0, First: 1, Last: leaseTestGrantMin}, g)

		require.Equal(t, 1, countRows(t, s, `SELECT COUNT(*) FROM lane_handout WHERE lease_id = $1`, s.leaseParam(id)))
		require.Equal(t, 1, countRows(t, s, `SELECT COUNT(*) FROM lane_lease WHERE lane = 0 AND lease_id = $1 AND closed_at IS NULL`, s.leaseParam(id)))
	})
}

func TestOpenLaneLeaseStartsAboveTheOldRanges(t *testing.T) {
	runLaneHandoutTests(t, "old ranges", func(t *testing.T, s *SQL) {
		base := time.Date(2026, 11, 7, 0, 0, 0, 0, time.UTC)

		// Lane n's old range ends at (n+1) × 10,000,000 and was handed out at a scrambled time, so
		// the two oldest are not the two lowest lanes.
		handedAt := make(map[uint8]time.Time, 254)

		for n := 0; n < 254; n++ {
			at := base.Add(time.Duration((n*37)%254) * time.Minute)
			handedAt[uint8(n)] = at //nolint:gosec // n is below 254

			_, err := s.db.ExecContext(t.Context(),
				`INSERT INTO lane_handout (lane, first, last, holder, height, handed_at) VALUES ($1, $2, $3, 'old', 1, $4)`,
				n, int64(n)*10_000_000+1, int64(n+1)*10_000_000, at)
			require.NoError(t, err)
		}

		lanes := make([]uint8, 0, 254)
		for l := range handedAt {
			lanes = append(lanes, l)
		}

		sort.Slice(lanes, func(i, j int) bool { return handedAt[lanes[i]].Before(handedAt[lanes[j]]) })

		for i := 0; i < 2; i++ {
			_, g, _, err := s.OpenLaneLease(t.Context(), leaseOpen("new"))
			require.NoError(t, err)
			require.Equal(t, lanes[i], g.Lane, "open %d takes the lane idle longest", i)
			require.Equal(t, uint64(g.Lane+1)*10_000_000+1, g.First, "the grant starts one above the lane's old range")
		}
	})
}

func TestOpenLaneLeaseNeverGrantsLanes254And255(t *testing.T) {
	runLaneHandoutTests(t, "254 and 255", func(t *testing.T, s *SQL) {
		for i := 0; i < 254; i++ {
			_, g, _, err := s.OpenLaneLease(t.Context(), leaseOpen("a"))
			require.NoError(t, err)
			require.Less(t, int(g.Lane), 254)
		}

		_, _, _, err := s.OpenLaneLease(t.Context(), leaseOpen("a"))
		require.Error(t, err)

		require.Equal(t, 254, countRows(t, s, `SELECT COUNT(DISTINCT lane) FROM lane_lease`))
	})
}

func TestRenewLaneLeaseSizesTheGrantByRate(t *testing.T) {
	runLaneHandoutTests(t, "rate", func(t *testing.T, s *SQL) {
		id, g, _, err := s.OpenLaneLease(t.Context(), leaseOpen("a"))
		require.NoError(t, err)

		r := leaseRenew(id, g)
		r.Rate, r.WantGrant = 90_000, true

		g2, granted, closed, err := s.RenewLaneLease(t.Context(), r)
		require.NoError(t, err)
		require.True(t, granted)
		require.False(t, closed)
		require.Equal(t, lane.Grant{Lane: g.Lane, First: g.Last + 1, Last: g.Last + 10_800_000}, g2)

		r.Rate = 50

		g3, granted, _, err := s.RenewLaneLease(t.Context(), r)
		require.NoError(t, err)
		require.True(t, granted)
		require.Equal(t, uint64(leaseTestGrantMin), g3.Len())
		require.Equal(t, g2.Last+1, g3.First)
	})
}

func TestRenewLaneLeaseWithoutAGrantWritesNoHandoutRow(t *testing.T) {
	runLaneHandoutTests(t, "no grant", func(t *testing.T, s *SQL) {
		id, g, _, err := s.OpenLaneLease(t.Context(), leaseOpen("a"))
		require.NoError(t, err)

		r := leaseRenew(id, g)
		r.Rate = 100

		_, granted, closed, err := s.RenewLaneLease(t.Context(), r)
		require.NoError(t, err)
		require.False(t, granted)
		require.False(t, closed)
		require.Equal(t, 1, countRows(t, s, `SELECT COUNT(*) FROM lane_handout`))
	})
}

func TestRenewLaneLeaseRecordsTheSettlesPairOnTheNextHandoutRow(t *testing.T) {
	runLaneHandoutTests(t, "settles", func(t *testing.T, s *SQL) {
		id, g, _, err := s.OpenLaneLease(t.Context(), leaseOpen("a"))
		require.NoError(t, err)

		r := leaseRenew(id, g)
		r.WantGrant = true

		g2, _, _, err := s.RenewLaneLease(t.Context(), r)
		require.NoError(t, err)

		r.Settles = lane.Settles{First: g.First, LastTaken: 9_000}

		g3, _, _, err := s.RenewLaneLease(t.Context(), r)
		require.NoError(t, err)

		var first, lastTaken sql.NullInt64

		require.NoError(t, s.db.QueryRowContext(t.Context(),
			`SELECT settles_first, settles_last_taken FROM lane_handout WHERE lane = $1 AND first = $2`, int(g3.Lane), int64(g3.First)).Scan(&first, &lastTaken)) //nolint:gosec // test positions are small
		require.Equal(t, int64(g.First), first.Int64) //nolint:gosec // test positions are small
		require.Equal(t, int64(9_000), lastTaken.Int64)

		require.NoError(t, s.db.QueryRowContext(t.Context(),
			`SELECT settles_first, settles_last_taken FROM lane_handout WHERE lane = $1 AND first = $2`, int(g2.Lane), int64(g2.First)).Scan(&first, &lastTaken)) //nolint:gosec // test positions are small
		require.False(t, first.Valid, "a grant asked for with nothing to settle carries no pair")
	})
}

func TestSweepRecordsTheNewestHandoutOnALapsedClose(t *testing.T) {
	runLaneHandoutTests(t, "lapsed", func(t *testing.T, s *SQL) {
		id, g, _, err := s.OpenLaneLease(t.Context(), leaseOpen("a"))
		require.NoError(t, err)

		r := leaseRenew(id, g)
		r.WantGrant = true
		r.Settles = lane.Settles{First: 1, LastTaken: 10}

		g2, _, _, err := s.RenewLaneLease(t.Context(), r)
		require.NoError(t, err)

		// A second lease on another lane takes a grant after this one, so "newest" must mean this
		// lease's newest hand-out, not the table's.
		_, _, _, err = s.OpenLaneLease(t.Context(), leaseOpen("b"))
		require.NoError(t, err)

		ageLeases(t, s, time.Hour)

		n, err := s.CloseStaleLaneLeases(t.Context(), 30*time.Minute)
		require.NoError(t, err)
		require.Equal(t, 2, n)

		var (
			kind                       string
			last                       int64
			settlesFirst, settlesTaken sql.NullInt64
		)

		require.NoError(t, s.db.QueryRowContext(t.Context(),
			`SELECT kind, last, settles_first, settles_last_taken FROM lane_close WHERE lease_id = $1`, s.leaseParam(id)).Scan(&kind, &last, &settlesFirst, &settlesTaken))
		require.Equal(t, "lapsed", kind)
		require.Equal(t, int64(g2.Last), last, "the lease's newest hand-out") //nolint:gosec // test positions are small
		require.False(t, settlesFirst.Valid, "a lapsed close has no settles pair")
		require.False(t, settlesTaken.Valid)
	})
}

// An open closes stale leases before it chooses a lane, and reports how many, so the service can
// count them with the sweep's lapsed closes.
func TestOpenReportsTheStaleLeasesItCloses(t *testing.T) {
	runLaneHandoutTests(t, "open-lapsed", func(t *testing.T, s *SQL) {
		for _, holder := range []string{"a", "b"} {
			_, _, _, err := s.OpenLaneLease(t.Context(), leaseOpen(holder))
			require.NoError(t, err)
		}

		ageLeases(t, s, time.Hour)

		open := leaseOpen("c")
		open.CloseAfter = 30 * time.Minute

		_, _, lapsed, err := s.OpenLaneLease(t.Context(), open)
		require.NoError(t, err)
		require.Equal(t, 2, lapsed)
		require.Equal(t, 2, countRows(t, s, `SELECT COUNT(*) FROM lane_close WHERE kind = 'lapsed'`))
	})
}

func TestRenewAndReleaseOfAClosedLeaseAreRefused(t *testing.T) {
	runLaneHandoutTests(t, "closed", func(t *testing.T, s *SQL) {
		id, g, _, err := s.OpenLaneLease(t.Context(), leaseOpen("a"))
		require.NoError(t, err)

		already, err := s.ReleaseLaneLease(t.Context(), options.LaneLeaseRelease{Lease: id, Lane: g.Lane, LastTaken: g.First})
		require.NoError(t, err)
		require.False(t, already)

		r := leaseRenew(id, g)
		r.WantGrant = true

		_, granted, closed, err := s.RenewLaneLease(t.Context(), r)
		require.NoError(t, err)
		require.True(t, closed)
		require.False(t, granted)
		require.Equal(t, 1, countRows(t, s, `SELECT COUNT(*) FROM lane_handout`), "a closed lease gets no grant")

		r.WantGrant = false

		_, _, closed, err = s.RenewLaneLease(t.Context(), r)
		require.NoError(t, err)
		require.True(t, closed)

		already, err = s.ReleaseLaneLease(t.Context(), options.LaneLeaseRelease{Lease: id, Lane: g.Lane})
		require.NoError(t, err)
		require.True(t, already)
		require.Equal(t, 1, countRows(t, s, `SELECT COUNT(*) FROM lane_close WHERE lease_id = $1`, s.leaseParam(id)))
	})
}

func TestReleaseAndSweepRaceWriteOneCloseRow(t *testing.T) {
	runLaneHandoutTests(t, "race", func(t *testing.T, s *SQL) {
		for i := 0; i < 50; i++ {
			id, g, _, err := s.OpenLaneLease(t.Context(), leaseOpen("a"))
			require.NoError(t, err)

			var (
				wg       sync.WaitGroup
				already  bool
				swept    int
				relErr   error
				sweepErr error
			)

			wg.Add(2)

			go func() {
				defer wg.Done()

				already, relErr = s.ReleaseLaneLease(context.Background(), options.LaneLeaseRelease{Lease: id, Lane: g.Lane, LastTaken: g.First})
			}()

			go func() {
				defer wg.Done()

				swept, sweepErr = s.CloseStaleLaneLeases(context.Background(), 0)
			}()

			wg.Wait()
			require.NoError(t, relErr)
			require.NoError(t, sweepErr)
			require.Equal(t, 1, countRows(t, s, `SELECT COUNT(*) FROM lane_close WHERE lease_id = $1`, s.leaseParam(id)))

			var kind string
			require.NoError(t, s.db.QueryRowContext(t.Context(), `SELECT kind FROM lane_close WHERE lease_id = $1`, s.leaseParam(id)).Scan(&kind))

			if already {
				require.Equal(t, "lapsed", kind)
				require.Equal(t, 1, swept)
			} else {
				require.Equal(t, "clean", kind)
				require.Equal(t, 0, swept)
			}
		}
	})
}

// TestTwoLeasesOnOneLaneNeverSharePosition is the end-state test of the store: leases open, take
// grants, release or lapse and lanes pass between them, and no two hand-outs in a lane ever overlap.
func TestTwoLeasesOnOneLaneNeverSharePosition(t *testing.T) {
	runLaneHandoutTests(t, "end state", func(t *testing.T, s *SQL) {
		const (
			workers = 8
			loops   = 200
		)

		var wg sync.WaitGroup

		errs := make(chan error, workers)

		for w := 0; w < workers; w++ {
			wg.Add(1)

			go func(seed uint64) {
				defer wg.Done()

				rng := rand.New(rand.NewPCG(seed, seed)) //nolint:gosec // test randomness
				ctx := context.Background()

				for i := 0; i < loops; i++ {
					id, g, _, err := s.OpenLaneLease(ctx, leaseOpen("w"))
					if err != nil {
						errs <- err
						return
					}

					taken := g.First
					cur := g

					for k := 0; k < 2+rng.IntN(2); k++ {
						r := leaseRenew(id, cur)
						r.WantGrant, r.Rate = true, 100
						r.Settles = lane.Settles{First: cur.First, LastTaken: taken}

						next, granted, closed, err := s.RenewLaneLease(ctx, r)
						if err != nil {
							errs <- err
							return
						}

						if closed {
							break
						}

						if granted {
							cur = next
							taken = next.First
						}
					}

					switch rng.IntN(3) {
					case 0:
						if _, err := s.CloseStaleLaneLeases(ctx, 0); err != nil {
							errs <- err
							return
						}
					default:
						if _, err := s.ReleaseLaneLease(ctx, options.LaneLeaseRelease{Lease: id, Lane: cur.Lane, LastTaken: taken}); err != nil {
							errs <- err
							return
						}
					}
				}
			}(uint64(w + 1)) //nolint:gosec // w is small
		}

		wg.Wait()
		close(errs)

		for err := range errs {
			require.NoError(t, err)
		}

		type row struct {
			lane        int
			first, last int64
			leaseID     string
			handedAt    time.Time
		}

		rows, err := s.db.QueryContext(t.Context(), `SELECT lane, first, last, lease_id, handed_at FROM lane_handout`)
		require.NoError(t, err)

		var all []row

		for rows.Next() {
			var (
				r  row
				id []byte
			)

			require.NoError(t, rows.Scan(&r.lane, &r.first, &r.last, &id, &r.handedAt))
			r.leaseID = string(id)
			all = append(all, r)
		}

		require.NoError(t, rows.Err())
		require.NoError(t, rows.Close())
		require.Greater(t, len(all), workers*loops)

		byLane := map[int][]row{}
		for _, r := range all {
			require.Less(t, r.lane, 254)
			byLane[r.lane] = append(byLane[r.lane], r)
		}

		for l, rs := range byLane {
			sort.Slice(rs, func(i, j int) bool { return rs[i].first < rs[j].first })

			for i := 1; i < len(rs); i++ {
				require.Greater(t, rs[i].first, rs[i-1].last, "lane %d: two hand-outs overlap", l)
			}

			sort.SliceStable(rs, func(i, j int) bool { return rs[i].handedAt.Before(rs[j].handedAt) })

			for i := 1; i < len(rs); i++ {
				if rs[i].handedAt.Equal(rs[i-1].handedAt) {
					continue
				}

				require.Greater(t, rs[i].first, rs[i-1].first, "lane %d: a later hand-out starts below an earlier one", l)
			}
		}

		closes, err := s.db.QueryContext(t.Context(), `SELECT c.lease_id, c.last, (SELECT COUNT(*) FROM lane_handout h WHERE h.lease_id = c.lease_id AND c.last BETWEEN h.first AND h.last) FROM lane_close c`)
		require.NoError(t, err)

		n := 0

		for closes.Next() {
			var (
				id     []byte
				last   int64
				inside int
			)

			require.NoError(t, closes.Scan(&id, &last, &inside))
			require.Equal(t, 1, inside, "close row last %d is not inside its own lease's grants", last)

			n++
		}

		require.NoError(t, closes.Err())
		require.NoError(t, closes.Close())
		require.Positive(t, n)
	})
}

func TestSweepClosesOnlyLeasesPastTheCloseTime(t *testing.T) {
	runLaneHandoutTests(t, "close time", func(t *testing.T, s *SQL) {
		for i := 0; i < 2; i++ {
			_, _, _, err := s.OpenLaneLease(t.Context(), leaseOpen("old"))
			require.NoError(t, err)
		}

		ageLeases(t, s, 10*time.Minute)

		// This open's own sweep must close nothing, so the sweep below is the one under test.
		open := leaseOpen("new")
		open.CloseAfter = time.Hour

		fresh, _, lapsed, err := s.OpenLaneLease(t.Context(), open)
		require.NoError(t, err)
		require.Zero(t, lapsed)

		closed, err := s.CloseStaleLaneLeases(t.Context(), 3*time.Minute)
		require.NoError(t, err)
		require.Equal(t, 2, closed)
		require.Equal(t, 1, countRows(t, s, `SELECT COUNT(*) FROM lane_lease WHERE lease_id = $1 AND closed_at IS NULL`, s.leaseParam(fresh)))
	})
}

func TestLaneLeaseDDLIsANoOpOnACurrentSchema(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping postgres-backed test in -short mode")
	}

	dbURL, db := newSchemaTestDB(t)

	s, err := New(ulogger.TestLogger{}, dbURL, test.CreateBaseTestSettings(t))
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close(context.Background()) })

	// A database a soak of the range hand-out left behind: lane_handout without the lease columns,
	// and no lease or close table. The schema setup must bring it up to date.
	for _, stmt := range []string{
		`DROP TABLE lane_lease`,
		`DROP TABLE lane_close`,
		`ALTER TABLE lane_handout DROP COLUMN lease_id`,
		`ALTER TABLE lane_handout DROP COLUMN settles_first`,
		`ALTER TABLE lane_handout DROP COLUMN settles_last_taken`,
	} {
		_, err = db.Exec(stmt)
		require.NoError(t, err, stmt)
	}

	require.NoError(t, createPostgresSchema(ulogger.TestLogger{}, db, true))

	var n int
	require.NoError(t, db.QueryRow(`SELECT COUNT(*) FROM information_schema.columns WHERE table_name = 'lane_handout' AND column_name IN ('lease_id', 'settles_first', 'settles_last_taken')`).Scan(&n))
	require.Equal(t, 3, n)

	for _, table := range []string{"lane_lease", "lane_close"} {
		var exists bool
		require.NoError(t, db.QueryRow(`SELECT to_regclass($1) IS NOT NULL`, table).Scan(&exists))
		require.True(t, exists, table)
	}

	// With everything in place, a role without CREATE runs no DDL at all.
	for _, stmt := range []string{
		`CREATE ROLE lease_no_ddl LOGIN PASSWORD 'lease_no_ddl'`,
		`REVOKE CREATE ON SCHEMA public FROM PUBLIC`,
		`GRANT USAGE ON SCHEMA public TO lease_no_ddl`,
		`GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO lease_no_ddl`,
	} {
		_, err = db.Exec(stmt)
		require.NoError(t, err, stmt)
	}

	nodeURL := *dbURL
	nodeURL.User = url.UserPassword("lease_no_ddl", "lease_no_ddl")

	nodeDB, err := usql.Open("postgres", nodeURL.String())
	require.NoError(t, err)
	t.Cleanup(func() { _ = nodeDB.Close() })

	_, err = nodeDB.Exec(`ALTER TABLE lane_handout ADD COLUMN IF NOT EXISTS lease_id UUID`)
	require.Error(t, err, "the role must not own the table for this test to mean anything")

	require.NoError(t, createPostgresSchema(ulogger.TestLogger{}, nodeDB, true))
}

// An exhausted lane, one whose last hand-out reached the last position, is never chosen, even when it
// is the lane closed longest ago.
func TestOpenLaneLeaseSkipsAnExhaustedLane(t *testing.T) {
	runLaneHandoutTests(t, "exhausted", func(t *testing.T, s *SQL) {
		id, g, _, err := s.OpenLaneLease(t.Context(), leaseOpen("a"))
		require.NoError(t, err)
		require.Equal(t, uint8(0), g.Lane)

		r := leaseRenew(id, g)
		r.WantGrant, r.Rate = true, lane.MaxPosition

		last, _, _, err := s.RenewLaneLease(t.Context(), r)
		require.NoError(t, err)
		require.Equal(t, lane.MaxPosition, last.Last, "a grant is capped at the last position")

		_, err = s.ReleaseLaneLease(t.Context(), options.LaneLeaseRelease{Lease: id, Lane: g.Lane, LastTaken: g.First})
		require.NoError(t, err)

		// Every other lane has a hand-out from after lane 0 closed, so lane 0 is the one closed
		// longest ago.
		for l := 1; l < 254; l++ {
			_, err = s.db.ExecContext(t.Context(),
				`INSERT INTO lane_handout (lane, first, last, holder, height, handed_at) VALUES ($1, 1, 10, 'old', 1, $2)`,
				l, time.Date(2099, 1, 1, 0, 0, 0, 0, time.UTC))
			require.NoError(t, err)
		}

		_, g2, _, err := s.OpenLaneLease(t.Context(), leaseOpen("b"))
		require.NoError(t, err)
		require.NotEqual(t, uint8(0), g2.Lane, "the exhausted lane is skipped")
		require.Equal(t, uint64(11), g2.First)
	})
}
