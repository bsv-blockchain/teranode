package lane

import (
	"context"
	"math/rand/v2"
	"sync"
	"testing"
	"time"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

// A returned position goes out before the grant's next new one, and once given out again it is
// no longer held.
func TestAllocatorHandsOutAReturnedPositionBeforeTheNextNewOne(t *testing.T) {
	a := newTestAllocator(t, newFakeLeases(1000), quiet, nil)

	for i := uint64(1); i <= 3; i++ {
		require.Equal(t, Pack(0, i), next(t, a))
	}

	before := testutil.ToFloat64(prometheusLaneReused)

	require.True(t, a.Return(Pack(0, 2)))
	require.Equal(t, Pack(0, 2), next(t, a), "the returned position first")
	require.Equal(t, Pack(0, 4), next(t, a), "then the next new one")
	require.Equal(t, Pack(0, 5), next(t, a), "a reused position is not given out a second time")
	require.InDelta(t, 1, testutil.ToFloat64(prometheusLaneReused)-before, 0)
}

// A position waiting in the returned set is taken back once. A second return of it, while it
// waits, would hand it to two records.
func TestAllocatorTakesAPositionBackOnlyOnce(t *testing.T) {
	a := newTestAllocator(t, newFakeLeases(1000), quiet, nil)

	for i := 0; i < 3; i++ {
		next(t, a)
	}

	require.True(t, a.Return(Pack(0, 2)))
	require.False(t, a.Return(Pack(0, 2)), "already waiting")
	require.Equal(t, Pack(0, 2), next(t, a))
	require.Equal(t, Pack(0, 4), next(t, a), "given out once, not twice")

	// Given out again and returned again by the record that took it the second time, it comes
	// back again: each return answers one hand-out.
	require.True(t, a.Return(Pack(0, 2)))
	require.Equal(t, Pack(0, 2), next(t, a))
}

// Only a position of the current grant that has been given out comes back. Anything else, a
// position not yet given out, another lane's, None, or one of a grant the allocator has left,
// stays burned.
func TestAllocatorRefusesAReturnFromOutsideTheCurrentGrant(t *testing.T) {
	src := newFakeLeases(10)
	a := newTestAllocator(t, src, quiet, nil)

	for i := 0; i < 3; i++ {
		next(t, a)
	}

	require.False(t, a.Return(Pack(0, 4)), "not given out yet")
	require.False(t, a.Return(Pack(0, 9)), "not given out yet")
	require.False(t, a.Return(Pack(1, 2)), "another lane")
	require.False(t, a.Return(None))
	require.False(t, a.Return(Pack(0, 0)))

	for i := 0; i < 7; i++ {
		next(t, a)
	}

	require.Eventually(t, a.hasSpareGrant, 5*time.Second, time.Millisecond)
	require.Equal(t, Pack(0, 11), next(t, a), "on the spare now")

	require.False(t, a.Return(Pack(0, 5)), "a position of the grant left behind")
	require.Equal(t, Pack(0, 12), next(t, a))
}

// Leaving a grant drops the positions returned to it. They stay burned, below the grant's final
// number, rather than going into a later grant, which would date a record by a grant it did not
// come from.
func TestAllocatorDropsReturnedPositionsWhenItLeavesAnAgedGrant(t *testing.T) {
	clk := newStepClock()
	src := newFakeLeases(100)
	a := newTestAllocator(t, src, Timings{Renew: time.Hour, Stop: 10 * time.Minute, GrantDuration: 2 * time.Minute}, clk)

	for i := uint64(1); i <= 50; i++ {
		if i == 50 {
			clk.step(time.Minute) // the spare is requested a minute in, so it is still fresh below
		}

		next(t, a)
	}

	require.Eventually(t, a.hasSpareGrant, 5*time.Second, time.Millisecond)
	require.True(t, a.Return(Pack(0, 7)))

	clk.step(time.Minute)
	require.Equal(t, Pack(0, 101), next(t, a), "the aged grant is left, its returned position with it")
	require.Equal(t, Pack(0, 102), next(t, a))

	require.NoError(t, a.Close())

	src.mu.Lock()
	defer src.mu.Unlock()

	require.Equal(t, Settles{First: 1, LastTaken: 50}, src.releases[0].Settles, "settled at the highest position taken")
}

// A used-up grant with returned positions stays current until they are given out, and the grant
// still settles at its last position.
func TestAllocatorGivesOutReturnedPositionsOfAUsedUpGrantBeforeTheSpare(t *testing.T) {
	src := newFakeLeases(10)
	a := newTestAllocator(t, src, quiet, nil)

	for i := 0; i < 10; i++ {
		next(t, a)
	}

	require.Eventually(t, a.hasSpareGrant, 5*time.Second, time.Millisecond)
	require.True(t, a.Return(Pack(0, 4)))
	require.True(t, a.Return(Pack(0, 9)))

	got := []uint64{next(t, a), next(t, a)}
	require.ElementsMatch(t, []uint64{Pack(0, 4), Pack(0, 9)}, got)
	require.Equal(t, Pack(0, 11), next(t, a), "then the spare")

	require.NoError(t, a.Close())

	src.mu.Lock()
	defer src.mu.Unlock()

	require.Equal(t, uint64(11), src.releases[0].LastTaken)
	require.Equal(t, Settles{First: 1, LastTaken: 10}, src.releases[0].Settles)
}

// Reuse changes neither the lease's last taken, which the release carries, nor the highest
// position given out in the lane, which guards the next grant, nor the count the rate is made
// from.
func TestAllocatorReuseLeavesTheReleaseAndTheHighestTakenAlone(t *testing.T) {
	src := newFakeLeases(1000)
	a := newTestAllocator(t, src, quiet, nil)

	for i := 0; i < 7; i++ {
		next(t, a)
	}

	read := func() (leaseTaken, highest, taken, nextNew uint64) {
		a.mu.Lock()
		defer a.mu.Unlock()

		return a.leaseTaken, a.highest[0], a.taken, a.next
	}

	lt, hi, tk, nx := read()

	require.True(t, a.Return(Pack(0, 3)))
	require.True(t, a.Return(Pack(0, 6)))
	next(t, a)
	next(t, a)

	lt2, hi2, tk2, nx2 := read()
	require.Equal(t, []uint64{lt, hi, tk, nx}, []uint64{lt2, hi2, tk2, nx2})

	require.NoError(t, a.Close())

	src.mu.Lock()
	defer src.mu.Unlock()

	require.Equal(t, uint64(7), src.releases[0].LastTaken)
}

// A lease the service closed takes its grant's returned positions with it.
func TestAllocatorDropsReturnedPositionsWhenItsLeaseIsClosed(t *testing.T) {
	src := newFakeLeases(1000)
	a := newTestAllocator(t, src, Timings{Renew: 10 * time.Millisecond, Stop: time.Hour, GrantDuration: time.Hour}, nil)

	for i := 0; i < 3; i++ {
		next(t, a)
	}

	require.True(t, a.Return(Pack(0, 2)))

	src.closeAll()
	require.Eventually(t, func() bool { return src.openCount() == 2 }, 5*time.Second, time.Millisecond, "a new lease is opened")

	_, n := Unpack(next(t, a))
	require.Greater(t, n, uint64(1000), "from the new lease's grant, not the returned position")
	require.False(t, a.Return(Pack(0, 2)), "a position of the closed lease")
}

// Return and Next on the reuse path allocate nothing.
func TestReturnAndReuseDoNotAllocate(t *testing.T) {
	a := newTestAllocator(t, newFakeLeases(10_000_000), quiet, nil)
	next(t, a)

	ctx := context.Background()

	require.Zero(t, testing.AllocsPerRun(1000, func() {
		v, err := a.Next(ctx)
		if err != nil {
			t.Fatal(err)
		}

		if !a.Return(v) {
			t.Fatal("not taken back")
		}

		if _, err = a.Next(ctx); err != nil {
			t.Fatal(err)
		}
	}))
}

// The returned set has a fixed size; a return past it stays burned.
func TestAllocatorBoundsTheReturnedSet(t *testing.T) {
	a := newTestAllocator(t, newFakeLeases(10_000), quiet, nil)

	for i := 0; i < maxReturned+1; i++ {
		next(t, a)
	}

	for i := uint64(1); i <= maxReturned; i++ {
		require.True(t, a.Return(Pack(0, i)))
	}

	require.False(t, a.Return(Pack(0, maxReturned+1)), "full")
}

// Many takers return about a third of what they take, as failed creates would. No position is
// ever held by two takers at once, and a position Return refused, which stays burned, is never
// given out again. Run under -race.
func TestAllocatorNeverHandsOutAPositionTwiceUnderConcurrentReturns(t *testing.T) {
	a := newTestAllocator(t, newFakeLeases(500), Timings{Renew: 5 * time.Millisecond, Stop: time.Hour, GrantDuration: time.Hour}, nil)

	var (
		mu       sync.Mutex
		held     = map[uint64]bool{}
		dupes    []uint64
		returned int
	)

	var wg sync.WaitGroup

	for w := 0; w < 8; w++ {
		wg.Add(1)

		go func(seed uint64) {
			defer wg.Done()

			rng := rand.New(rand.NewPCG(seed, seed))

			for i := 0; i < 2000; i++ {
				v, err := a.Next(context.Background())
				if err != nil {
					t.Error(err)
					return
				}

				mu.Lock()
				if held[v] {
					dupes = append(dupes, v)
				}
				held[v] = true
				mu.Unlock()

				if rng.IntN(3) != 0 {
					continue // written: the position is held for good
				}

				// Released before the return, so a taker that gets it from Next is not seen as a
				// second holder; held again if the return is refused and the position burns.
				mu.Lock()
				delete(held, v)
				mu.Unlock()

				if a.Return(v) {
					mu.Lock()
					returned++
					mu.Unlock()

					continue
				}

				mu.Lock()
				held[v] = true
				mu.Unlock()
			}
		}(uint64(w))
	}

	wg.Wait()

	require.Empty(t, dupes)
	require.Positive(t, returned)
}

// A store whose write ends after the allocator closed does not get its position counted as
// returned: a closed allocator gives out nothing, so it takes nothing back.
func TestAllocatorRefusesAReturnAfterClose(t *testing.T) {
	a := newTestAllocator(t, newFakeLeases(1000), quiet, nil)

	v := next(t, a)
	next(t, a)

	require.NoError(t, a.Close())
	require.False(t, a.Return(v))
}

// A returned position waits behind the same stop as a new one: once Stop has passed since the last
// good renewal, the service may give the lane away, so Next hands out nothing, returned or new, even
// though the grant is young.
func TestAllocatorHandsOutNoReturnedPositionAfterStop(t *testing.T) {
	clk := newStepClock()
	src := newFakeLeases(1_000_000)
	a := newTestAllocator(t, src, Timings{Renew: 10 * time.Millisecond, Stop: 2 * time.Minute, GrantDuration: time.Hour}, clk)
	a.takeWait = 200 * time.Millisecond

	v := next(t, a)
	next(t, a)
	src.setDown(true)

	require.True(t, a.Return(v))

	clk.step(2 * time.Minute)

	_, err := a.Next(context.Background())
	require.ErrorIs(t, err, errors.ErrServiceUnavailable, "the returned position stays put after Stop")
}

// settleAgedGrant takes positions 1 to 50 of a grant of 100, hands back the given positions, lets the
// grant age out, takes half the spare so the next grant is asked for, and returns the settles pair
// that request carried for the grant left.
func settleAgedGrant(t *testing.T, returned ...uint64) Settles {
	t.Helper()

	clk := newStepClock()
	src := newFakeLeases(100)
	a := newTestAllocator(t, src, Timings{Renew: time.Hour, Stop: 10 * time.Minute, GrantDuration: 2 * time.Minute}, clk)

	for i := uint64(1); i <= 50; i++ {
		if i == 50 {
			clk.step(time.Minute) // the spare is requested a minute in, so it is still fresh below
		}

		next(t, a)
	}

	require.Eventually(t, a.hasSpareGrant, 5*time.Second, time.Millisecond)

	for _, n := range returned {
		require.True(t, a.Return(Pack(0, n)))
	}

	clk.step(time.Minute)
	require.Equal(t, Pack(0, 101), next(t, a), "the aged grant is left")

	for i := 0; i < 49; i++ {
		next(t, a)
	}

	require.Eventually(t, func() bool { return len(src.grantRequests()) == 2 }, 5*time.Second, time.Millisecond,
		"the next grant is asked for at half the spare")

	return src.grantRequests()[1].Settles
}

// When the allocator leaves a grant, its final number drops past the returned positions still at the
// top of the taken range, so it names only positions that may carry a record. A returned position
// below a used one changes nothing.
func TestAllocatorSettlesAGrantBelowReturnedPositionsAtItsTop(t *testing.T) {
	require.Equal(t, Settles{First: 1, LastTaken: 50}, settleAgedGrant(t), "nothing returned")
	require.Equal(t, Settles{First: 1, LastTaken: 49}, settleAgedGrant(t, 50), "the top position returned")
	require.Equal(t, Settles{First: 1, LastTaken: 48}, settleAgedGrant(t, 49, 50), "the top two returned")
	require.Equal(t, Settles{First: 1, LastTaken: 48}, settleAgedGrant(t, 50, 49), "in either order")
	require.Equal(t, Settles{First: 1, LastTaken: 50}, settleAgedGrant(t, 49), "below a used position")
	require.Equal(t, Settles{First: 1, LastTaken: 48}, settleAgedGrant(t, 50, 20, 49), "only the run at the top")
}

// A grant every taken position of which came back settles at zero: it gave out nothing that may
// carry a record.
func TestAllocatorSettlesAGrantWhoseTakesAllCameBackAtZero(t *testing.T) {
	all := make([]uint64, 50)
	for i := range all {
		all[i] = uint64(i) + 1 //nolint:gosec // i counts up from 0
	}

	require.Equal(t, Settles{First: 1, LastTaken: 0}, settleAgedGrant(t, all...))
}

// The release's last-taken number follows the same rule for the grant still current.
func TestAllocatorReleasesBelowReturnedPositionsAtTheTop(t *testing.T) {
	release := func(t *testing.T, taken uint64, returned ...uint64) uint64 {
		t.Helper()

		src := newFakeLeases(1000)
		a := newTestAllocator(t, src, quiet, nil)

		for i := uint64(0); i < taken; i++ {
			next(t, a)
		}

		for _, n := range returned {
			require.True(t, a.Return(Pack(0, n)))
		}

		require.NoError(t, a.Close())

		src.mu.Lock()
		defer src.mu.Unlock()

		require.Len(t, src.releases, 1)

		return src.releases[0].LastTaken
	}

	require.Equal(t, uint64(7), release(t, 7))
	require.Equal(t, uint64(6), release(t, 7, 7), "the top position returned")
	require.Equal(t, uint64(5), release(t, 7, 6, 7), "the top two returned")
	require.Equal(t, uint64(7), release(t, 7, 3), "below a used position")
	require.Equal(t, uint64(0), release(t, 3, 1, 2, 3), "every position returned: the lease took nothing")
}

// A release whose current grant gave back everything it took falls back to the number the lease had
// before that grant, which itself was lowered when that grant was left.
func TestAllocatorReleaseFallsBackPastACurrentGrantThatCameBackWhole(t *testing.T) {
	clk := newStepClock()
	src := newFakeLeases(10)
	a := newTestAllocator(t, src, Timings{Renew: time.Hour, Stop: 10 * time.Minute, GrantDuration: 2 * time.Minute}, clk)

	for i := uint64(1); i <= 10; i++ {
		if i == 5 {
			clk.step(time.Minute) // the spare is requested a minute in, so it is still fresh below
		}

		next(t, a)
	}

	require.Eventually(t, a.hasSpareGrant, 5*time.Second, time.Millisecond)
	require.True(t, a.Return(Pack(0, 10)))

	// The grant ages out with 10 still waiting, so it settles at 9 and the lease's number drops to 9.
	clk.step(time.Minute)
	require.Equal(t, Pack(0, 11), next(t, a), "the aged grant is left for the spare")
	require.Equal(t, Pack(0, 12), next(t, a))
	require.True(t, a.Return(Pack(0, 12)))
	require.True(t, a.Return(Pack(0, 11)))

	require.NoError(t, a.Close())

	src.mu.Lock()
	defer src.mu.Unlock()

	require.Len(t, src.releases, 1)
	require.Equal(t, uint64(9), src.releases[0].LastTaken, "the spare came back whole; the grant before it settled at 9")
	require.Equal(t, Settles{First: 1, LastTaken: 9}, src.releases[0].Settles, "the grant left is settled by the release")
}

// A returned top position that a take reuses before the grant settles carries a record again, so the
// settle and the release still name it: only positions still waiting in the returned set lower them.
func TestAllocatorSettlesAtAReturnedTopPositionThatWasReused(t *testing.T) {
	t.Run("the release", func(t *testing.T) {
		src := newFakeLeases(1000)
		a := newTestAllocator(t, src, quiet, nil)

		for i := 0; i < 7; i++ {
			next(t, a)
		}

		require.True(t, a.Return(Pack(0, 7)))
		require.Equal(t, Pack(0, 7), next(t, a), "the returned top position is reused")

		require.NoError(t, a.Close())

		src.mu.Lock()
		defer src.mu.Unlock()

		require.Len(t, src.releases, 1)
		require.Equal(t, uint64(7), src.releases[0].LastTaken, "a record carries 7 again")
	})

	t.Run("an aged grant", func(t *testing.T) {
		clk := newStepClock()
		src := newFakeLeases(100)
		a := newTestAllocator(t, src, Timings{Renew: time.Hour, Stop: 10 * time.Minute, GrantDuration: 2 * time.Minute}, clk)

		for i := uint64(1); i <= 50; i++ {
			if i == 50 {
				clk.step(time.Minute) // the spare is requested a minute in, so it is still fresh below
			}

			next(t, a)
		}

		require.Eventually(t, a.hasSpareGrant, 5*time.Second, time.Millisecond)
		require.True(t, a.Return(Pack(0, 50)))
		require.Equal(t, Pack(0, 50), next(t, a), "the returned top position is reused")

		clk.step(time.Minute)
		require.Equal(t, Pack(0, 101), next(t, a), "the aged grant is left")

		for i := 0; i < 49; i++ {
			next(t, a)
		}

		require.Eventually(t, func() bool { return len(src.grantRequests()) == 2 }, 5*time.Second, time.Millisecond)
		require.Equal(t, Settles{First: 1, LastTaken: 50}, src.grantRequests()[1].Settles, "a record carries 50 again")
	})
}
