package sql

import (
	"net/url"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/bsv-blockchain/teranode/stores/blockchain/options"
	"github.com/bsv-blockchain/teranode/test/utils/postgres"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/lane"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/require"
)

const laneTestSize = 1000

var laneTestFreeAfter = 24 * time.Hour

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

func laneReq(holder string, now time.Time) options.LaneRangeRequest {
	return options.LaneRangeRequest{Holder: holder, Size: laneTestSize, Height: 7, FreeAfter: laneTestFreeAfter, Now: now}
}

func laneReqFor(holder string, l uint8, now time.Time) options.LaneRangeRequest {
	r := laneReq(holder, now)
	r.WantLane, r.HasLane = l, true

	return r
}

func TestReserveLaneRange(t *testing.T) {
	t0 := time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC)

	runLaneHandoutTests(t, "fresh holder gets lane 0 then positions are monotonic", func(t *testing.T, s *SQL) {
		r1, err := s.ReserveLaneRange(t.Context(), laneReq("a", t0))
		require.NoError(t, err)
		require.Equal(t, lane.Range{Lane: 0, First: 1, Last: laneTestSize}, r1)

		r2, err := s.ReserveLaneRange(t.Context(), laneReqFor("a", r1.Lane, t0.Add(time.Minute)))
		require.NoError(t, err)
		require.Equal(t, lane.Range{Lane: 0, First: laneTestSize + 1, Last: 2 * laneTestSize}, r2)
	})

	runLaneHandoutTests(t, "two holders get different lanes", func(t *testing.T, s *SQL) {
		a, err := s.ReserveLaneRange(t.Context(), laneReq("a", t0))
		require.NoError(t, err)
		b, err := s.ReserveLaneRange(t.Context(), laneReq("b", t0))
		require.NoError(t, err)
		require.NotEqual(t, a.Lane, b.Lane)
	})

	// A process that lost its host name but kept its data folder names its saved
	// lane under a new holder. It keeps the lane, because otherwise every deploy of
	// such a pod would take another lane until the 256 ran out. The range still
	// starts above every position handed out, so the two lives cannot share one.
	runLaneHandoutTests(t, "a named lane is granted whoever held it last", func(t *testing.T, s *SQL) {
		a, err := s.ReserveLaneRange(t.Context(), laneReq("a", t0))
		require.NoError(t, err)

		b, err := s.ReserveLaneRange(t.Context(), laneReqFor("b", a.Lane, t0.Add(time.Hour)))
		require.NoError(t, err)
		require.Equal(t, a.Lane, b.Lane)
		require.Equal(t, a.Last+1, b.First)
	})

	runLaneHandoutTests(t, "same holder re-asking gets its lane", func(t *testing.T, s *SQL) {
		a, err := s.ReserveLaneRange(t.Context(), laneReq("a", t0))
		require.NoError(t, err)
		_, err = s.ReserveLaneRange(t.Context(), laneReq("b", t0))
		require.NoError(t, err)

		again, err := s.ReserveLaneRange(t.Context(), laneReqFor("a", a.Lane, t0.Add(time.Hour)))
		require.NoError(t, err)
		require.Equal(t, a.Lane, again.Lane)
	})

	runLaneHandoutTests(t, "never-used requested lane is granted from position 1", func(t *testing.T, s *SQL) {
		r, err := s.ReserveLaneRange(t.Context(), laneReqFor("a", 200, t0))
		require.NoError(t, err)
		require.Equal(t, lane.Range{Lane: 200, First: 1, Last: laneTestSize}, r)
	})

	runLaneHandoutTests(t, "a named lane continues above old positions, past or inside the idle window", func(t *testing.T, s *SQL) {
		a, err := s.ReserveLaneRange(t.Context(), laneReqFor("a", 5, t0))
		require.NoError(t, err)

		b, err := s.ReserveLaneRange(t.Context(), laneReqFor("b", 5, t0.Add(laneTestFreeAfter)))
		require.NoError(t, err)
		require.Equal(t, uint8(5), b.Lane)
		require.Equal(t, a.Last+1, b.First)

		// The earlier holder asks again inside the window b's hand-out started. Its
		// lane is still granted, and the range starts above b's, so a and b overlap
		// in the lane with distinct positions.
		c, err := s.ReserveLaneRange(t.Context(), laneReqFor("a", 5, t0.Add(laneTestFreeAfter+time.Hour)))
		require.NoError(t, err)
		require.Equal(t, uint8(5), c.Lane)
		require.Equal(t, b.Last+1, c.First)
	})

	runLaneHandoutTests(t, "no-lane request fails with no free lane then picks the longest idle lane", func(t *testing.T, s *SQL) {
		// Use every lane once, lane l at t0+l seconds, so lane 0 is idle longest.
		for l := 0; l <= lane.MaxLane; l++ {
			_, err := s.ReserveLaneRange(t.Context(), laneReqFor("h", uint8(l), t0.Add(time.Duration(l)*time.Second)))
			require.NoError(t, err)
		}

		// Still inside the window of every lane.
		_, err := s.ReserveLaneRange(t.Context(), laneReq("new", t0.Add(time.Hour)))
		require.Error(t, err)
		require.Contains(t, err.Error(), "no free lane")

		// Only lanes 0 and 1 are idle past the window; lane 0 is idle longest.
		r, err := s.ReserveLaneRange(t.Context(), laneReq("new", t0.Add(laneTestFreeAfter+time.Second+500*time.Millisecond)))
		require.NoError(t, err)
		require.Equal(t, uint8(0), r.Lane)
		require.Equal(t, uint64(laneTestSize+1), r.First)
	})

	// Lanes are handed out in reverse, so the longest idle lane is the highest number, not the
	// lowest. A scan that took the first free lane would get this wrong.
	runLaneHandoutTests(t, "a no-lane request takes the longest idle lane, not the lowest number", func(t *testing.T, s *SQL) {
		for l := lane.MaxLane; l >= 0; l-- {
			at := t0.Add(time.Duration(lane.MaxLane-l) * time.Second)

			_, err := s.ReserveLaneRange(t.Context(), laneReqFor("h", uint8(l), at)) //nolint:gosec // l is at most 255
			require.NoError(t, err)
		}

		// Only lanes 255 and 254 are idle past the window, and 255 has been idle longest.
		r, err := s.ReserveLaneRange(t.Context(), laneReq("new", t0.Add(laneTestFreeAfter+time.Second+500*time.Millisecond)))
		require.NoError(t, err)
		require.Equal(t, uint8(lane.MaxLane), r.Lane)
	})

	// With every lane used, the free-lane scan does reach an exhausted lane. Handing it out would
	// give a first position that does not fit in seven bytes.
	runLaneHandoutTests(t, "the free-lane scan skips an exhausted lane that is idle longest", func(t *testing.T, s *SQL) {
		exhaust := laneReqFor("a", 3, t0)
		exhaust.Size = lane.MaxPosition
		_, err := s.ReserveLaneRange(t.Context(), exhaust)
		require.NoError(t, err)

		// Every other lane is used later, lane 0 first, so lane 3 is the longest idle of all.
		for l := 0; l <= lane.MaxLane; l++ {
			if l == 3 {
				continue
			}

			_, err = s.ReserveLaneRange(t.Context(), laneReqFor("h", uint8(l), t0.Add(time.Hour+time.Duration(l)*time.Second))) //nolint:gosec // l is at most 255
			require.NoError(t, err)
		}

		r, err := s.ReserveLaneRange(t.Context(), laneReq("new", t0.Add(laneTestFreeAfter+2*time.Hour)))
		require.NoError(t, err)
		require.Equal(t, uint8(0), r.Lane, "lane 0 is the longest idle lane that is not exhausted")
		require.LessOrEqual(t, r.Last, lane.MaxPosition)
	})

	runLaneHandoutTests(t, "an exhausted lane is never handed out again", func(t *testing.T, s *SQL) {
		req := laneReqFor("a", 3, t0)
		req.Size = lane.MaxPosition
		r, err := s.ReserveLaneRange(t.Context(), req)
		require.NoError(t, err)
		require.Equal(t, lane.MaxPosition, r.Last)

		// Even the same holder, long after the window, is moved off lane 3.
		again, err := s.ReserveLaneRange(t.Context(), laneReqFor("a", 3, t0.Add(1000*time.Hour)))
		require.NoError(t, err)
		require.NotEqual(t, uint8(3), again.Lane)
	})

	runLaneHandoutTests(t, "a range is capped at the last position", func(t *testing.T, s *SQL) {
		req := laneReqFor("a", 3, t0)
		req.Size = lane.MaxPosition - 10
		_, err := s.ReserveLaneRange(t.Context(), req)
		require.NoError(t, err)

		r, err := s.ReserveLaneRange(t.Context(), laneReqFor("a", 3, t0))
		require.NoError(t, err)
		require.Equal(t, lane.MaxPosition, r.Last)
		require.Equal(t, lane.MaxPosition-9, r.First)
	})

	runLaneHandoutTests(t, "high lanes round-trip through signed storage", func(t *testing.T, s *SQL) {
		r, err := s.ReserveLaneRange(t.Context(), laneReqFor("a", 255, t0))
		require.NoError(t, err)
		next, err := s.ReserveLaneRange(t.Context(), laneReqFor("a", 255, t0))
		require.NoError(t, err)
		require.Equal(t, r.Last+1, next.First)
	})

	runLaneHandoutTests(t, "invalid requests are rejected", func(t *testing.T, s *SQL) {
		_, err := s.ReserveLaneRange(t.Context(), laneReq("", t0))
		require.Error(t, err)

		req := laneReq("a", t0)
		req.Size = 0
		_, err = s.ReserveLaneRange(t.Context(), req)
		require.Error(t, err)
	})

	runLaneHandoutTests(t, "concurrent requests never share a lane or overlap", func(t *testing.T, s *SQL) {
		const workers = 32

		var (
			wg      sync.WaitGroup
			mu      sync.Mutex
			results = make(map[int]lane.Range)
			errs    []error
		)

		// Even workers ask for no lane, odd workers all ask for lane 100.
		for i := 0; i < workers; i++ {
			wg.Add(1)

			go func(i int) {
				defer wg.Done()

				req := laneReq("holder-"+strconv.Itoa(i), t0)
				if i%2 == 1 {
					req = laneReqFor("holder-"+strconv.Itoa(i), 100, t0)
				}

				r, err := s.ReserveLaneRange(t.Context(), req)

				mu.Lock()
				defer mu.Unlock()

				if err != nil {
					errs = append(errs, err)
					return
				}

				results[i] = r
			}(i)
		}

		wg.Wait()
		require.Empty(t, errs)
		require.Len(t, results, workers)

		noLaneLanes := map[uint8]bool{}
		firsts := map[uint8]map[uint64]bool{}

		for i, r := range results {
			if i%2 == 0 {
				require.False(t, noLaneLanes[r.Lane], "two no-lane requests got lane %d", r.Lane)
				noLaneLanes[r.Lane] = true
			}

			if firsts[r.Lane] == nil {
				firsts[r.Lane] = map[uint64]bool{}
			}

			// Every range has the same size and starts on a size boundary + 1, so
			// distinct firsts mean no overlap.
			require.False(t, firsts[r.Lane][r.First], "lane %d first %d handed out twice", r.Lane, r.First)
			require.Equal(t, uint64(laneTestSize), r.Len())
			require.Equal(t, uint64(1), r.First%laneTestSize)
			firsts[r.Lane][r.First] = true
		}
	})
}
