package sql

import (
	"context"
	"database/sql"
	"fmt"
	"math/rand/v2"
	"net/url"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/settings"
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

// leasableLanes is lanes 0 to 253, written out so a change to the store's own count is caught.
const leasableLanes = 254

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

// An open whose first attempt commits but reports a retriable error (a lost COMMIT reply) is
// retried by RetryTx. The retry must take a new lease id, or the two committed leases share one id
// and, once both lapse, their two lapsed closes collide on lane_close's primary key: the sweep, and
// every open that sweeps first, would then fail for good.
func TestOpenLaneLeaseRetryAfterALostCommitTakesANewID(t *testing.T) {
	runLaneHandoutTests(t, "lost commit", func(t *testing.T, s *SQL) {
		// Postgres retries are off unless postgres_retryEnabled is set; this is the retrying case.
		s.db.SetRetryConfig(usql.RetryConfig{MaxAttempts: 3, BaseDelay: time.Millisecond, Enabled: true})

		attempts := 0
		openLeaseAttemptHook = func(tx *sql.Tx) error {
			attempts++
			if attempts > 1 {
				return nil
			}

			require.NoError(t, tx.Commit())

			return errors.NewServiceError("connection reset by peer")
		}

		defer func() { openLeaseAttemptHook = nil }()

		id, _, _, err := s.OpenLaneLease(t.Context(), leaseOpen("a"))
		require.NoError(t, err)
		require.Equal(t, 2, attempts, "the lost commit must be retried")

		openLeaseAttemptHook = nil

		require.Equal(t, 2, countRows(t, s, `SELECT COUNT(*) FROM lane_lease WHERE closed_at IS NULL`))
		require.Equal(t, 2, countRows(t, s, `SELECT COUNT(DISTINCT lease_id) FROM lane_lease`), "each attempt takes its own lease id")
		require.Equal(t, 1, countRows(t, s, `SELECT COUNT(*) FROM lane_lease WHERE lease_id = $1`, s.leaseParam(id)), "the returned id is the committed retry's")

		ageLeases(t, s, time.Hour)

		closed, err := s.CloseStaleLaneLeases(t.Context(), 30*time.Minute)
		require.NoError(t, err)
		require.Equal(t, 2, closed)

		_, _, _, err = s.OpenLaneLease(t.Context(), leaseOpen("b"))
		require.NoError(t, err)
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

		// No one grant can use up a lane (settings.MaxLaneGrant), so an older hand-out takes lane 0
		// to within ten positions of its end, and the next grant takes the rest.
		_, err = s.db.ExecContext(t.Context(),
			`INSERT INTO lane_handout (lane, first, last, holder, height, handed_at) VALUES (0, $1, $2, 'a', 1, $3)`,
			lane.ToSigned(g.Last+1), lane.ToSigned(lane.MaxPosition-10), time.Now())
		require.NoError(t, err)

		r := leaseRenew(id, g)
		r.WantGrant, r.Rate = true, lane.MaxPosition

		last, _, _, err := s.RenewLaneLease(t.Context(), r)
		require.NoError(t, err)
		require.Equal(t, lane.MaxPosition-9, last.First)
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

// A lane with no lease row can be exhausted by hand-outs from before the lease. However long ago
// its last hand-out, an open skips it.
func TestOpenLaneLeaseSkipsAnExhaustedLaneWithNoLease(t *testing.T) {
	runLaneHandoutTests(t, "exhausted without lease", func(t *testing.T, s *SQL) {
		_, err := s.db.ExecContext(t.Context(),
			`INSERT INTO lane_handout (lane, first, last, holder, height, handed_at) VALUES (0, 1, $1, 'old', 1, $2)`,
			int64(lane.MaxPosition), time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC))
		require.NoError(t, err)

		useUpLanesFrom(t, s, 1)

		_, g, _, err := s.OpenLaneLease(t.Context(), leaseOpen("a"))
		require.NoError(t, err)
		require.Equal(t, uint8(1), g.Lane, "the exhausted lane 0 is skipped although its hand-out is the oldest")
		require.Equal(t, uint64(20_000_001), g.First)
	})
}

// useUpLanesFrom inserts one hand-out in each lane from first to 253, stamped a day ahead, so
// none of them is never-used and each is a later pick than any lease closed now.
func useUpLanesFrom(t *testing.T, s *SQL, first int) {
	t.Helper()

	at := time.Now().Add(24 * time.Hour)

	for n := first; n < grantableLanes; n++ {
		_, err := s.db.ExecContext(t.Context(),
			`INSERT INTO lane_handout (lane, first, last, holder, height, handed_at) VALUES ($1, $2, $3, 'old', 1, $4)`,
			n, int64(n)*10_000_000+1, int64(n+1)*10_000_000, at)
		require.NoError(t, err)
	}
}

func TestRenewAndReleaseOfAnOldLeaseLeaveTheLaneNewLeaseAlone(t *testing.T) {
	runLaneHandoutTests(t, "old lease on reused lane", func(t *testing.T, s *SQL) {
		oldID, oldGrant, _, err := s.OpenLaneLease(t.Context(), leaseOpen("old"))
		require.NoError(t, err)
		require.Equal(t, uint8(0), oldGrant.Lane)

		// Lane 0 is the only closed lane and every other lane has been used, so the next open
		// must reuse lane 0.
		useUpLanesFrom(t, s, 1)
		ageLeases(t, s, 10*time.Minute)

		closed, err := s.CloseStaleLaneLeases(t.Context(), 3*time.Minute)
		require.NoError(t, err)
		require.Equal(t, 1, closed)

		newID, newGrant, _, err := s.OpenLaneLease(t.Context(), leaseOpen("new"))
		require.NoError(t, err)
		require.Equal(t, uint8(0), newGrant.Lane, "the new lease reuses the lane")
		require.NotEqual(t, oldID, newID)

		handouts := countRows(t, s, `SELECT COUNT(*) FROM lane_handout WHERE lane = 0`)

		r := leaseRenew(oldID, oldGrant)
		r.WantGrant = true

		_, granted, closedLease, err := s.RenewLaneLease(t.Context(), r)
		require.NoError(t, err)
		require.True(t, closedLease, "the old lease is not the lane's current one")
		require.False(t, granted)
		require.Equal(t, handouts, countRows(t, s, `SELECT COUNT(*) FROM lane_handout WHERE lane = 0`), "no grant for the old lease")

		already, err := s.ReleaseLaneLease(t.Context(), options.LaneLeaseRelease{Lease: oldID, Lane: oldGrant.Lane, LastTaken: oldGrant.First})
		require.NoError(t, err)
		require.True(t, already)

		require.Equal(t, 1, countRows(t, s, `SELECT COUNT(*) FROM lane_lease WHERE lane = 0 AND lease_id = $1 AND closed_at IS NULL`, s.leaseParam(newID)),
			"the new lease is still open")
		require.Equal(t, 0, countRows(t, s, `SELECT COUNT(*) FROM lane_close WHERE lease_id = $1 AND kind = $2`, s.leaseParam(newID), closeClean))
	})
}

func TestOpenLaneLeaseTakesTheLaneClosedLongestAgo(t *testing.T) {
	runLaneHandoutTests(t, "closed longest ago", func(t *testing.T, s *SQL) {
		ids := make([]lane.LeaseID, 3)
		grants := make([]lane.Grant, 3)

		for i := range ids {
			var err error

			ids[i], grants[i], _, err = s.OpenLaneLease(t.Context(), leaseOpen("a"))
			require.NoError(t, err)
			require.Equal(t, uint8(i), grants[i].Lane) //nolint:gosec // i is below 3
		}

		// Lane 1 closes first, then lane 0; lane 2 stays open. Lane 0 is the lower number, so
		// only the closing time can make lane 1 the choice.
		for _, i := range []int{1, 0} {
			already, err := s.ReleaseLaneLease(t.Context(), options.LaneLeaseRelease{Lease: ids[i], Lane: grants[i].Lane, LastTaken: grants[i].First})
			require.NoError(t, err)
			require.False(t, already)

			time.Sleep(20 * time.Millisecond)
		}

		useUpLanesFrom(t, s, 3)

		_, g, _, err := s.OpenLaneLease(t.Context(), leaseOpen("b"))
		require.NoError(t, err)
		require.Equal(t, uint8(1), g.Lane, "the lane closed longest ago comes first")
	})
}

// checkLaneInvariants checks what must hold however the lease calls interleave: no position is
// handed out twice in a lane, a later hand-out never starts below an earlier one, a lease has at
// most one close row and its last lies inside the lease's own grants, and a lane's lease row has a
// close row exactly when it is closed.
func checkLaneInvariants(t *testing.T, s *SQL) {
	t.Helper()

	type row struct {
		first, last int64
		handedAt    time.Time
	}

	rows, err := s.db.QueryContext(t.Context(), `SELECT lane, first, last, handed_at FROM lane_handout`)
	require.NoError(t, err)

	byLane := map[int][]row{}

	for rows.Next() {
		var (
			l int
			r row
		)

		require.NoError(t, rows.Scan(&l, &r.first, &r.last, &r.handedAt))
		require.Less(t, l, leasableLanes)
		byLane[l] = append(byLane[l], r)
	}

	require.NoError(t, rows.Err())
	require.NoError(t, rows.Close())

	for l, rs := range byLane {
		sort.Slice(rs, func(i, j int) bool { return rs[i].first < rs[j].first })

		for i := 1; i < len(rs); i++ {
			require.Greater(t, rs[i].first, rs[i-1].last, "lane %d: two hand-outs overlap", l)
		}

		sort.SliceStable(rs, func(i, j int) bool { return rs[i].handedAt.Before(rs[j].handedAt) })

		for i := 1; i < len(rs); i++ {
			require.True(t, rs[i].handedAt.Equal(rs[i-1].handedAt) || rs[i].first > rs[i-1].first, "lane %d: a later hand-out starts below an earlier one", l)
		}
	}

	require.Zero(t, countRows(t, s, `SELECT COUNT(*) FROM lane_close c WHERE (SELECT COUNT(*) FROM lane_handout h WHERE h.lease_id = c.lease_id AND c.last BETWEEN h.first AND h.last) <> 1`),
		"a close row's last lies outside its own lease's grants")
	require.Zero(t, countRows(t, s, `SELECT COUNT(*) - COUNT(DISTINCT lease_id) FROM lane_close`), "a lease has two close rows")
	require.Zero(t, countRows(t, s, `SELECT COUNT(*) FROM lane_lease l WHERE (l.closed_at IS NOT NULL) <> EXISTS (SELECT 1 FROM lane_close c WHERE c.lease_id = l.lease_id)`),
		"a lease row is closed without a close row, or open with one")
}

// leaseModel is what the lease tables should hold after a sequence of lease calls, kept by the
// test from the rules alone.
type leaseModel struct {
	s         *SQL
	leases    []*modelLease
	current   map[uint8]*modelLease // each lane's lease row
	high      map[uint8]uint64      // each lane's newest hand-out, 0 for none
	prefillAt map[uint8]time.Time   // when the hand-outs from before the lease were made
	handouts  []modelHandout        // the hand-out rows with a lease
	closes    []modelClose
}

type modelLease struct {
	id     lane.LeaseID
	lane   uint8
	holder string
	closed bool
	age    time.Duration // how far ageLeases has moved its last renewal back
	grants []lane.Grant
}

type modelHandout struct {
	lane                     uint8
	first, last              int64
	holder                   string
	height                   int64
	lease                    string
	settlesFirst, settlesTkn sql.NullInt64
}

type modelClose struct {
	lease                    string
	lane                     uint8
	kind                     string
	last                     int64
	settlesFirst, settlesTkn sql.NullInt64
}

type modelLeaseRow struct {
	lane          uint8
	lease, holder string
	closed        bool
}

func (m *leaseModel) key(id lane.LeaseID) string {
	if m.s.engine == util.Postgres {
		return id.String()
	}

	return string(id[:])
}

func (m *leaseModel) exhausted(l uint8) bool { return m.high[l] >= lane.MaxPosition }

func (m *leaseModel) stale(ls *modelLease) bool { return ls.closed || m.current[ls.lane] != ls }

// grant is the next grant in lane l: size positions from one above the lane's newest hand-out.
func (m *leaseModel) grant(l uint8, size uint64) lane.Grant {
	g := lane.Grant{Lane: l, First: m.high[l] + 1, Last: lane.MaxPosition}
	if size-1 < lane.MaxPosition-g.First {
		g.Last = g.First + size - 1
	}

	return g
}

func (m *leaseModel) record(ls *modelLease, g lane.Grant, height uint32, st lane.Settles) {
	m.high[g.Lane] = g.Last
	ls.grants = append(ls.grants, g)
	m.handouts = append(m.handouts, modelHandout{g.Lane, lane.ToSigned(g.First), lane.ToSigned(g.Last), ls.holder, int64(height), m.key(ls.id), settlesFirst(st), settlesLastTaken(st)})
}

// lapsing is the open leases a sweep with close time closeAfter closes.
func (m *leaseModel) lapsing(closeAfter time.Duration) []*modelLease {
	var out []*modelLease

	for _, ls := range m.current {
		if !ls.closed && ls.age >= closeAfter {
			out = append(out, ls)
		}
	}

	return out
}

func (m *leaseModel) lapse(swept []*modelLease) {
	for _, ls := range swept {
		ls.closed = true
		last := ls.grants[len(ls.grants)-1].Last
		m.closes = append(m.closes, modelClose{m.key(ls.id), ls.lane, closeLapsed, lane.ToSigned(last), sql.NullInt64{}, sql.NullInt64{}})
	}
}

// choose is the lanes an open may take: the lowest never-used lane, else the lane closed longest
// ago, skipping exhausted lanes. Leases the open's own sweep closes share one close time, so any of
// them may come first; closedAt is the close time the store recorded for each earlier close.
func (m *leaseModel) choose(swept []*modelLease, closedAt map[uint8]time.Time) []uint8 {
	type candidate struct {
		lane  uint8
		since time.Time
	}

	justClosed := map[uint8]bool{}
	for _, ls := range swept {
		justClosed[ls.lane] = true
	}

	var cands []candidate

	for n := 0; n < leasableLanes; n++ {
		l := uint8(n) //nolint:gosec // n is below 254
		ls := m.current[l]

		switch {
		case ls == nil && m.high[l] == 0:
			return []uint8{l}
		case m.exhausted(l) || (ls != nil && !ls.closed && !justClosed[l]):
		case justClosed[l]:
			cands = append(cands, candidate{l, time.Date(9999, 1, 1, 0, 0, 0, 0, time.UTC)})
		case ls != nil:
			cands = append(cands, candidate{l, closedAt[l]})
		default:
			cands = append(cands, candidate{l, m.prefillAt[l]})
		}
	}

	if len(cands) == 0 {
		return nil
	}

	sort.SliceStable(cands, func(a, b int) bool { return cands[a].since.Before(cands[b].since) })

	if !justClosed[cands[0].lane] {
		return []uint8{cands[0].lane}
	}

	var out []uint8

	for _, c := range cands {
		if justClosed[c.lane] {
			out = append(out, c.lane)
		}
	}

	return out
}

// scanAll reads every row of query with scan.
func scanAll[T any](t *testing.T, s *SQL, query string, scan func(rows *sql.Rows, v *T, id *[]byte) error, setID func(v *T, id []byte)) []T {
	t.Helper()

	rows, err := s.db.QueryContext(t.Context(), query)
	require.NoError(t, err)

	var out []T

	for rows.Next() {
		var (
			v  T
			id []byte
		)

		require.NoError(t, scan(rows, &v, &id))
		setID(&v, id)
		out = append(out, v)
	}

	require.NoError(t, rows.Err())
	require.NoError(t, rows.Close())

	return out
}

// checkTables compares every lease, hand-out and close row with the model.
func (m *leaseModel) checkTables(t *testing.T) {
	t.Helper()

	require.ElementsMatch(t, m.handouts, scanAll(t, m.s,
		`SELECT lane, first, last, holder, height, lease_id, settles_first, settles_last_taken FROM lane_handout WHERE lease_id IS NOT NULL`,
		func(rows *sql.Rows, h *modelHandout, id *[]byte) error {
			return rows.Scan(&h.lane, &h.first, &h.last, &h.holder, &h.height, id, &h.settlesFirst, &h.settlesTkn)
		},
		func(h *modelHandout, id []byte) { h.lease = string(id) }), "lane_handout")

	require.ElementsMatch(t, m.closes, scanAll(t, m.s,
		`SELECT lease_id, lane, kind, last, settles_first, settles_last_taken FROM lane_close`,
		func(rows *sql.Rows, c *modelClose, id *[]byte) error {
			return rows.Scan(id, &c.lane, &c.kind, &c.last, &c.settlesFirst, &c.settlesTkn)
		},
		func(c *modelClose, id []byte) { c.lease = string(id) }), "lane_close")

	want := make([]modelLeaseRow, 0, len(m.current))
	for l, ls := range m.current {
		want = append(want, modelLeaseRow{l, m.key(ls.id), ls.holder, ls.closed})
	}

	require.ElementsMatch(t, want, scanAll(t, m.s,
		`SELECT lane, lease_id, holder, closed_at IS NOT NULL FROM lane_lease`,
		func(rows *sql.Rows, r *modelLeaseRow, id *[]byte) error {
			return rows.Scan(&r.lane, id, &r.holder, &r.closed)
		},
		func(r *modelLeaseRow, id []byte) { r.lease = string(id) }), "lane_lease")
}

// closeTimes reads the close time the store recorded for each closed lane lease.
func (m *leaseModel) closeTimes(t *testing.T) map[uint8]time.Time {
	t.Helper()

	type closedLane struct {
		lane uint8
		at   time.Time
	}

	out := map[uint8]time.Time{}

	for _, c := range scanAll(t, m.s, `SELECT lane, closed_at FROM lane_lease WHERE closed_at IS NOT NULL`,
		func(rows *sql.Rows, c *closedLane, _ *[]byte) error { return rows.Scan(&c.lane, &c.at) },
		func(*closedLane, []byte) {}) {
		out[c.lane] = c.at
	}

	return out
}

// prefill makes lanes free to 253 used before the lease, so leases must pass a few lanes between
// them. Three keep positions, at scrambled times; the rest are exhausted and older still, so an
// open that took an exhausted lane would take one of them first.
func (m *leaseModel) prefill(t *testing.T, rng *rand.Rand, free int) {
	t.Helper()

	usable := map[int]bool{}
	for len(usable) < 3 {
		usable[free+rng.IntN(leasableLanes-free)] = true
	}

	for n := free; n < leasableLanes; n++ {
		first, last := int64(n)*10_000_000+1, lane.ToSigned(lane.MaxPosition)
		at := time.Date(1999, 1, 1, 0, n, 0, 0, time.UTC)

		if usable[n] {
			last, at = int64(n+1)*10_000_000, time.Date(2000, 1, 1, 0, rng.IntN(1000), n, 0, time.UTC)
		}

		_, err := m.s.db.ExecContext(t.Context(),
			`INSERT INTO lane_handout (lane, first, last, holder, height, handed_at) VALUES ($1, $2, $3, 'old', 1, $4)`, n, first, last, at)
		require.NoError(t, err)

		m.high[uint8(n)], m.prefillAt[uint8(n)] = lane.FromSigned(last), at //nolint:gosec // n is below 254
	}
}

// TestLaneLeaseStoreKeepsItsInvariants drives seeded random sequences of opens, renewals with and
// without a grant, releases, sweeps and the passing of time against the store. After every call it
// checks the store's answer and every lease, hand-out and close row against a model kept from the
// rules, and then the invariants. Odd seeds leave only a few lanes usable, so lanes pass from one
// lease to the next while the old holder still renews and releases.
func TestLaneLeaseStoreKeepsItsInvariants(t *testing.T) {
	runLaneHandoutTests(t, "invariants", func(t *testing.T, s *SQL) {
		for seed := uint64(1); seed <= 6; seed++ {
			for _, table := range []string{"lane_close", "lane_handout", "lane_lease"} {
				_, err := s.db.ExecContext(t.Context(), `DELETE FROM `+table)
				require.NoError(t, err)
			}

			runLeaseModel(t, s, seed, 150)
		}
	})
}

func runLeaseModel(t *testing.T, s *SQL, seed uint64, steps int) {
	rng := rand.New(rand.NewPCG(seed, seed)) //nolint:gosec // test randomness
	m := &leaseModel{s: s, current: map[uint8]*modelLease{}, high: map[uint8]uint64{}, prefillAt: map[uint8]time.Time{}}

	var ops []string

	defer func() {
		if t.Failed() {
			t.Logf("seed %d, ops:\n%s", seed, strings.Join(ops, "\n"))
		}
	}()

	if seed%2 == 1 {
		m.prefill(t, rng, 1+rng.IntN(3))
	}

	ctx := t.Context()
	closeAfters := []time.Duration{90 * time.Second, 150 * time.Second, time.Hour, time.Hour}
	rates := []uint64{0, 3, 100, 90_000, 0, 3, 100, 90_000, lane.MaxPosition}

	// pick returns a lease that is still open nine times in ten, else any lease ever opened.
	pick := func() *modelLease {
		var open []*modelLease

		for _, ls := range m.leases {
			if !m.stale(ls) {
				open = append(open, ls)
			}
		}

		if len(open) > 0 && rng.IntN(10) < 9 {
			return open[rng.IntN(len(open))]
		}

		return m.leases[rng.IntN(len(m.leases))]
	}

	// pair is a settles pair inside one of the lease's grants, or none.
	pair := func(ls *modelLease) lane.Settles {
		if rng.IntN(3) == 0 {
			return lane.Settles{}
		}

		g := ls.grants[rng.IntN(len(ls.grants))]

		return lane.Settles{First: g.First, LastTaken: g.First + rng.Uint64N(g.Len())}
	}

	for step := 0; step < steps; step++ {
		op := rng.IntN(100)
		if len(m.leases) == 0 {
			op = 0
		}

		switch {
		case op < 20:
			req := options.LaneLeaseOpen{
				Holder: fmt.Sprintf("h%d", step), Height: rng.Uint32N(1000), GrantMin: []uint64{1, 7, leaseTestGrantMin}[rng.IntN(3)],
				CloseAfter: closeAfters[rng.IntN(len(closeAfters))],
			}
			swept := m.lapsing(req.CloseAfter)
			allowed := m.choose(swept, m.closeTimes(t))
			ops = append(ops, fmt.Sprintf("%d open %+v -> lanes %v", step, req, allowed))

			id, g, lapsed, err := s.OpenLaneLease(ctx, req)
			if allowed == nil {
				require.ErrorIs(t, err, errors.ErrServiceError, "no lane is free")
				break
			}

			require.NoError(t, err)
			require.Contains(t, allowed, g.Lane)
			require.Equal(t, m.grant(g.Lane, req.GrantMin), g)
			require.Equal(t, len(swept), lapsed)
			m.lapse(swept)

			ls := &modelLease{id: id, lane: g.Lane, holder: req.Holder}
			m.leases = append(m.leases, ls)
			m.current[g.Lane] = ls
			m.record(ls, g, req.Height, lane.Settles{})

		case op < 55:
			ls := pick()
			req := options.LaneLeaseRenew{
				Lease: ls.id, Lane: ls.lane, Rate: rates[rng.IntN(len(rates))], WantGrant: rng.IntN(3) > 0, Settles: pair(ls),
				Height: rng.Uint32N(1000), GrantMin: []uint64{1, leaseTestGrantMin}[rng.IntN(2)], GrantDuration: 2 * time.Minute,
			}
			ops = append(ops, fmt.Sprintf("%d renew lane %d %+v", step, ls.lane, req))

			stale := m.stale(ls)

			g, granted, closed, err := s.RenewLaneLease(ctx, req)
			if !stale && req.WantGrant && m.exhausted(ls.lane) {
				require.ErrorIs(t, err, errors.ErrServiceError, "an exhausted lane grants nothing")
				break
			}

			require.NoError(t, err)
			require.Equal(t, stale, closed, "a renewal is refused exactly when its lease is stale")
			require.Equal(t, !stale && req.WantGrant, granted)

			if stale {
				break
			}

			ls.age = 0

			if req.WantGrant {
				size := req.GrantMin // two minutes at the rate, at least the minimum, at most MaxLaneGrant
				if req.Rate > settings.MaxLaneGrant/120 {
					size = settings.MaxLaneGrant
				} else if req.Rate > 0 {
					size = max(size, req.Rate*120)
				}

				require.Equal(t, m.grant(ls.lane, size), g)
				m.record(ls, g, req.Height, req.Settles)
			}

		case op < 70:
			ls := pick()
			g := ls.grants[rng.IntN(len(ls.grants))]
			req := options.LaneLeaseRelease{Lease: ls.id, Lane: ls.lane, LastTaken: g.First + rng.Uint64N(g.Len()), Settles: pair(ls)}
			ops = append(ops, fmt.Sprintf("%d release lane %d %+v", step, ls.lane, req))

			stale := m.stale(ls)

			already, err := s.ReleaseLaneLease(ctx, req)
			require.NoError(t, err)
			require.Equal(t, stale, already, "a release is refused exactly when its lease is stale")

			if !stale {
				ls.closed = true
				m.closes = append(m.closes, modelClose{m.key(ls.id), ls.lane, closeClean, lane.ToSigned(req.LastTaken), settlesFirst(req.Settles), settlesLastTaken(req.Settles)})
			}

		case op < 85:
			d := time.Duration(1+rng.IntN(3)) * time.Minute
			ops = append(ops, fmt.Sprintf("%d age %s", step, d))

			ageLeases(t, s, d)

			for _, ls := range m.current {
				ls.age += d
			}

		default:
			closeAfter := closeAfters[rng.IntN(len(closeAfters))]
			ops = append(ops, fmt.Sprintf("%d sweep %s", step, closeAfter))

			swept := m.lapsing(closeAfter)

			n, err := s.CloseStaleLaneLeases(ctx, closeAfter)
			require.NoError(t, err)
			require.Equal(t, len(swept), n)
			m.lapse(swept)
		}

		m.checkTables(t)
		checkLaneInvariants(t, s)

		// SQLite keeps times to the millisecond, and two closes in one would tie on their close time.
		time.Sleep(2 * time.Millisecond)
	}
}

// grantSize sizes on whole seconds of the duration, so under one second every grant is the minimum
// (and nothing divides by zero). No grant holds more than settings.MaxLaneGrant, whatever the rate
// or the minimum.
func TestGrantSize(t *testing.T) {
	const capRate = settings.MaxLaneGrant / 120

	for _, c := range []struct {
		name    string
		rate    uint64
		d       time.Duration
		minimum uint64
		want    uint64
	}{
		{"under one second gives the minimum", 90_000, 500 * time.Millisecond, 10_000, 10_000},
		{"zero rate gives the minimum", 0, 2 * time.Minute, 10_000, 10_000},
		{"rate times whole seconds", 100, 2*time.Minute + 900*time.Millisecond, 10, 12_000},
		{"never below the minimum", 3, 2 * time.Minute, 10_000, 10_000},
		{"rate exactly at the cap", capRate, 2 * time.Minute, 1, capRate * 120},
		{"rate one above the cap", capRate + 1, 2 * time.Minute, 1, settings.MaxLaneGrant},
		{"huge rate is capped", lane.MaxPosition, 2 * time.Minute, 1, settings.MaxLaneGrant},
		{"huge minimum is capped", 0, 2 * time.Minute, lane.MaxPosition, settings.MaxLaneGrant},
	} {
		t.Run(c.name, func(t *testing.T) {
			require.Equal(t, c.want, grantSize(c.rate, c.d, c.minimum))
		})
	}
}

// An open asked for a first grant above settings.MaxLaneGrant gets MaxLaneGrant, so a misconfigured
// blockchain_laneGrantMin cannot use up a lane on its first grant.
func TestOpenLaneLeaseCapsTheFirstGrant(t *testing.T) {
	runLaneHandoutTests(t, "first grant cap", func(t *testing.T, s *SQL) {
		req := leaseOpen("a")
		req.GrantMin = lane.MaxPosition

		_, g, _, err := s.OpenLaneLease(t.Context(), req)
		require.NoError(t, err)
		require.Equal(t, settings.MaxLaneGrant, g.Last-g.First+1)
	})
}

// A renewal that gets the lease row before a sweep keeps its lease, though the lease was stale when
// the sweep started: the staleness test sits in the sweep's UPDATE, which Postgres re-checks on the
// row the renewal committed. A held row lock fixes the order: the renewal queues first, then the
// sweep, and both go when the lock is let go. SQLite runs every lease call under one mutex, so
// there the two cannot overlap at all.
func TestARenewalThatGetsTheRowFirstKeepsItsLease(t *testing.T) {
	runLaneHandoutTests(t, "renewal before sweep", func(t *testing.T, s *SQL) {
		if s.engine != util.Postgres {
			t.Skip("lease calls never overlap on SQLite")
		}

		ctx := t.Context()

		id, g, _, err := s.OpenLaneLease(ctx, leaseOpen("a"))
		require.NoError(t, err)

		ageLeases(t, s, 10*time.Minute)

		hold, err := s.db.BeginTx(ctx, nil)
		require.NoError(t, err)

		defer func() { _ = hold.Rollback() }()

		_, err = hold.ExecContext(ctx, `SELECT 1 FROM lane_lease WHERE lane = $1 FOR UPDATE`, int(g.Lane))
		require.NoError(t, err)

		waiting := func(n int) func() bool {
			return func() bool {
				return countRows(t, s, `SELECT COUNT(*) FROM pg_stat_activity WHERE datname = current_database() AND wait_event_type = 'Lock'`) == n
			}
		}

		type renewed struct {
			closed bool
			err    error
		}

		renewDone := make(chan renewed, 1)

		go func() {
			_, _, closed, err := s.RenewLaneLease(context.Background(), leaseRenew(id, g))
			renewDone <- renewed{closed, err}
		}()

		require.Eventually(t, waiting(1), 10*time.Second, 5*time.Millisecond, "the renewal waits on the row")

		type swept struct {
			n   int
			err error
		}

		sweepDone := make(chan swept, 1)

		go func() {
			n, err := s.CloseStaleLaneLeases(context.Background(), 3*time.Minute)
			sweepDone <- swept{n, err}
		}()

		require.Eventually(t, waiting(2), 10*time.Second, 5*time.Millisecond, "the sweep waits behind it")
		require.NoError(t, hold.Rollback())

		r := <-renewDone
		require.NoError(t, r.err)
		require.False(t, r.closed, "the renewal got the row first")

		sw := <-sweepDone
		require.NoError(t, sw.err)
		require.Equal(t, 0, sw.n, "the sweep re-checks staleness on the renewed row")

		require.Equal(t, 1, countRows(t, s, `SELECT COUNT(*) FROM lane_lease WHERE lease_id = $1 AND closed_at IS NULL`, s.leaseParam(id)))
		require.Equal(t, 0, countRows(t, s, `SELECT COUNT(*) FROM lane_close WHERE lease_id = $1`, s.leaseParam(id)))
	})
}
