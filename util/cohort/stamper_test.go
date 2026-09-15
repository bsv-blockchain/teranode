package cohort

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// fixedClock is a clock the test drives by hand.
type fixedClock struct {
	sec atomic.Int64
}

func newFixedClock(sec int64) *fixedClock {
	c := &fixedClock{}
	c.sec.Store(sec)

	return c
}

func (c *fixedClock) now() time.Time {
	return time.Unix(c.sec.Load(), 0).UTC()
}

func (c *fixedClock) set(sec int64) {
	c.sec.Store(sec)
}

func TestStamperFrozenClock(t *testing.T) {
	clock := newFixedClock(int64(GenesisTime) + 1000)
	stamper := NewStamper(WithClock(clock.now))

	require.Equal(t, Unset, stamper.Floor())

	for i := 0; i < 3; i++ {
		got, err := stamper.Stamp()
		require.NoError(t, err)
		require.Equal(t, GenesisTime+1000, got)
	}

	// Nothing has been mapped, so nothing was skewed.
	require.Equal(t, uint64(0), stamper.SkewedStamps())
}

func TestStamperClockAdvances(t *testing.T) {
	clock := newFixedClock(int64(GenesisTime) + 1000)
	stamper := NewStamper(WithClock(clock.now))

	first, err := stamper.Stamp()
	require.NoError(t, err)

	clock.set(int64(GenesisTime) + 1001)

	second, err := stamper.Stamp()
	require.NoError(t, err)

	require.Equal(t, GenesisTime+1000, first)
	require.Equal(t, GenesisTime+1001, second)
	require.Equal(t, uint64(0), stamper.SkewedStamps())
}

func TestStamperClockHasNotAdvancedPastFloor(t *testing.T) {
	clock := newFixedClock(int64(GenesisTime) + 1000)
	stamper := NewStamper(WithClock(clock.now))

	// The current second has just been mapped to a block.
	stamper.ObserveMapped(GenesisTime + 1000)
	require.Equal(t, GenesisTime+1000, stamper.Floor())

	// The clock has not moved on, so stamps have to go above the floor.
	got, err := stamper.Stamp()
	require.NoError(t, err)
	require.Equal(t, GenesisTime+1001, got)
	require.Equal(t, uint64(1), stamper.SkewedStamps())

	got, err = stamper.Stamp()
	require.NoError(t, err)
	require.Equal(t, GenesisTime+1001, got)
	require.Equal(t, uint64(2), stamper.SkewedStamps())

	// Once the clock catches up, stamping is normal again.
	clock.set(int64(GenesisTime) + 1002)

	got, err = stamper.Stamp()
	require.NoError(t, err)
	require.Equal(t, GenesisTime+1002, got)
	require.Equal(t, uint64(2), stamper.SkewedStamps())
}

func TestStamperBackwardClock(t *testing.T) {
	clock := newFixedClock(int64(GenesisTime) + 5000)
	stamper := NewStamper(WithClock(clock.now))

	stamper.ObserveMapped(GenesisTime + 5000)

	// The clock jumps back an hour.
	clock.set(int64(GenesisTime) + 1400)

	got, err := stamper.Stamp()
	require.NoError(t, err)
	require.Equal(t, GenesisTime+5001, got, "a backward clock must never stamp back into mapped territory")
	require.Equal(t, uint64(1), stamper.SkewedStamps())
}

func TestStamperClockBeforeGenesis(t *testing.T) {
	clock := newFixedClock(0)
	stamper := NewStamper(WithClock(clock.now))

	// Nothing mapped yet, and the clock is unusable, so there is nothing to
	// count up from.
	_, err := stamper.Stamp()
	require.Error(t, err)
	require.Equal(t, uint64(0), stamper.SkewedStamps())

	// With a floor, the same broken clock still yields a usable stamp.
	stamper.ObserveMapped(GenesisTime + 7)

	got, err := stamper.Stamp()
	require.NoError(t, err)
	require.Equal(t, GenesisTime+8, got)
	require.Equal(t, uint64(1), stamper.SkewedStamps())
}

func TestStamperClockBeyondRange(t *testing.T) {
	// A clock past 2106 is as unrepresentable as one before genesis, and the
	// error must not claim the opposite direction.
	clock := newFixedClock(int64(MaxClock) + 1)
	stamper := NewStamper(WithClock(clock.now))

	_, err := stamper.Stamp()
	require.Error(t, err)
	require.Contains(t, err.Error(), "outside the cohort range")
	require.NotContains(t, err.Error(), "before genesis")
	require.Equal(t, uint64(0), stamper.SkewedStamps())

	// With a floor, the same unusable clock still hands out a stamp.
	stamper.ObserveMapped(GenesisTime + 11)

	got, err := stamper.Stamp()
	require.NoError(t, err)
	require.Equal(t, GenesisTime+12, got)
	require.Equal(t, uint64(1), stamper.SkewedStamps())
}

func TestStamperClockSpaceExhausted(t *testing.T) {
	clock := newFixedClock(int64(MaxClock))
	stamper := NewStamper(WithClock(clock.now))

	stamper.ObserveMapped(MaxClock)

	_, err := stamper.Stamp()
	require.Error(t, err)
}

func TestStamperObserveMappedIgnoresSyntheticAndSentinels(t *testing.T) {
	clock := newFixedClock(int64(GenesisTime) + 1000)
	stamper := NewStamper(WithClock(clock.now))

	// Synthetic and sentinel IDs are not points in time. A synthetic ID sits in
	// the sub-genesis range, so if it were allowed to set the floor it would
	// leave the floor far below every clock cohort and switch the guard off.
	for _, id := range []ID{Unset, Historical, BornMined, FirstSynthetic, ID(999_999), LastSynthetic} {
		stamper.ObserveMapped(id)
		require.Equal(t, Unset, stamper.Floor(), "%s must not move the floor", id)
	}

	// A clock ID raises it.
	stamper.ObserveMapped(GenesisTime + 1000)
	require.Equal(t, GenesisTime+1000, stamper.Floor())

	// And synthetic IDs still leave it exactly where it is.
	for _, id := range []ID{Unset, Historical, BornMined, FirstSynthetic, LastSynthetic} {
		stamper.ObserveMapped(id)
		require.Equal(t, GenesisTime+1000, stamper.Floor(), "%s must not move the floor", id)
	}
}

func TestStamperObserveMappedNeverLowersTheFloor(t *testing.T) {
	stamper := NewStamper()

	stamper.ObserveMapped(GenesisTime + 2000)
	require.Equal(t, GenesisTime+2000, stamper.Floor())

	stamper.ObserveMapped(GenesisTime + 1000)
	require.Equal(t, GenesisTime+2000, stamper.Floor())

	stamper.ObserveMapped(GenesisTime + 2001)
	require.Equal(t, GenesisTime+2001, stamper.Floor())
}

func TestStamperConcurrentStampsAreMonotonic(t *testing.T) {
	clock := newFixedClock(int64(GenesisTime) + 1000)
	stamper := NewStamper(WithClock(clock.now))

	const (
		goroutines = 8
		stamps     = 200
	)

	var (
		wg   sync.WaitGroup
		stop atomic.Bool
	)

	// Failures are collected rather than asserted inside the goroutines, since
	// require would abort the wrong goroutine.
	failures := make(chan string, goroutines*stamps)

	// Seed the floor deterministically before any goroutine starts. The mapper
	// goroutine below also raises it, but whether it gets scheduled at all
	// before the stampers finish is up to the runtime, so the closing assertion
	// on Floor() cannot depend on it.
	stamper.ObserveMapped(ID(clock.sec.Load()))

	// A ticker goroutine walking the clock forward, and a mapper goroutine
	// raising the floor, both while stamps are being handed out.
	wg.Add(2)

	go func() {
		defer wg.Done()

		for !stop.Load() {
			clock.set(clock.sec.Load() + 1)
		}
	}()

	go func() {
		defer wg.Done()

		for !stop.Load() {
			stamper.ObserveMapped(ID(clock.sec.Load()))
		}
	}()

	var stampers sync.WaitGroup

	for i := 0; i < goroutines; i++ {
		stampers.Add(1)

		go func() {
			defer stampers.Done()

			last := Unset

			for j := 0; j < stamps; j++ {
				got, err := stamper.Stamp()
				if err != nil {
					failures <- "stamp failed: " + err.Error()

					return
				}

				if !got.IsClock() {
					failures <- "stamp is not a clock cohort: " + got.String()
				}

				if got < last {
					failures <- "stamps went backwards: " + last.String() + " then " + got.String()
				}

				last = got
			}
		}()
	}

	stampers.Wait()
	stop.Store(true)
	wg.Wait()
	close(failures)

	for failure := range failures {
		require.Fail(t, failure)
	}

	// Only clock cohorts were ever fed in, so the floor is one, and it never
	// went backwards from the value seeded above.
	require.True(t, stamper.Floor().IsClock())
	require.GreaterOrEqual(t, uint32(stamper.Floor()), uint32(GenesisTime))
}

func TestCanMap(t *testing.T) {
	now := time.Unix(int64(GenesisTime)+10_000, 0).UTC()

	tests := []struct {
		name   string
		id     ID
		minAge time.Duration
		want   bool
	}{
		{name: "unset is never mappable", id: Unset, minAge: DefaultMinMapAge},
		{name: "historical is never mappable", id: Historical, minAge: DefaultMinMapAge},
		{name: "born mined is never mappable", id: BornMined, minAge: DefaultMinMapAge},
		{name: "first synthetic is exempt", id: FirstSynthetic, minAge: DefaultMinMapAge, want: true},
		{name: "last synthetic is exempt", id: LastSynthetic, minAge: time.Hour, want: true},
		{name: "clock cohort well past the age", id: GenesisTime + 9_000, minAge: DefaultMinMapAge, want: true},
		{name: "clock cohort exactly at the age", id: GenesisTime + 10_000 - 7, minAge: DefaultMinMapAge, want: true},
		{name: "clock cohort one second short", id: GenesisTime + 10_000 - 6, minAge: DefaultMinMapAge},
		{name: "clock cohort is the current second", id: GenesisTime + 10_000, minAge: DefaultMinMapAge},
		{name: "clock cohort in the future", id: GenesisTime + 10_001, minAge: DefaultMinMapAge},
		{name: "current second with a zero age", id: GenesisTime + 10_000, minAge: 0, want: true},
		{name: "future second with a zero age", id: GenesisTime + 10_001, minAge: 0},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			require.Equal(t, test.want, CanMap(test.id, now, test.minAge))
		})
	}
}

func TestDefaultMinMapAge(t *testing.T) {
	require.Equal(t, 7*time.Second, DefaultMinMapAge)
}
