package alarm

import (
	"math"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

var t0 = time.Date(2026, 10, 28, 10, 0, 0, 0, time.UTC)

func feed(b *Baseline, id string, start time.Time, n int, step time.Duration, value func(i int, at time.Time) float64) time.Time {
	at := start

	for i := 0; i < n; i++ {
		b.Observe(id, value(i, at), at)
		at = at.Add(step)
	}

	return at
}

func constant(v float64) func(int, time.Time) float64 {
	return func(int, time.Time) float64 { return v }
}

func TestNotReadyBeforeWarmup(t *testing.T) {
	b := NewBaseline(10 * time.Minute)

	feed(b, "rate", t0, 100, 2*time.Second, constant(1000))

	mean, tol, ready := b.Normal("rate")
	require.False(t, ready)
	require.InDelta(t, 1000, mean, 1e-6)
	require.Greater(t, tol, 0.0)

	c := NewBaseline(10 * time.Minute)

	feed(c, "rate", t0, 10, 66*time.Second, constant(1000))

	_, _, ready = c.Normal("rate")
	require.False(t, ready)
}

func TestReadyAfterWarmupAndStable(t *testing.T) {
	b := NewBaseline(10 * time.Minute)

	feed(b, "rate", t0, 400, 2*time.Second, func(i int, _ time.Time) float64 {
		if i%2 == 0 {
			return 990
		}

		return 1010
	})

	mean, tol, ready := b.Normal("rate")
	require.True(t, ready)
	require.InEpsilon(t, 1000, mean, 0.01)
	require.InDelta(t, 0.05*mean, tol, 1e-9)
}

func TestToleranceUsesStandardDeviationAndMinimum(t *testing.T) {
	b := NewBaseline(time.Minute)

	feed(b, "wide", t0, 20000, 2*time.Second, func(i int, _ time.Time) float64 {
		if i%2 == 0 {
			return 0
		}

		return 200
	})
	feed(b, "zero", t0, 2000, 2*time.Second, constant(0))

	_, tol, ready := b.Normal("wide")
	require.True(t, ready)
	require.InDelta(t, 300, tol, 15)

	mean, tol, ready := b.Normal("zero")
	require.True(t, ready)
	require.Equal(t, 0.0, mean)
	require.Equal(t, 1e-9, tol)
}

func TestUnknownMetric(t *testing.T) {
	b := NewBaseline(time.Minute)

	mean, tol, ready := b.Normal("nope")
	require.Equal(t, 0.0, mean)
	require.Equal(t, 0.0, tol)
	require.False(t, ready)
}

func TestSpikeDoesNotShiftMeanMuch(t *testing.T) {
	b := NewBaseline(10 * time.Minute)

	at := feed(b, "rate", t0, 400, 2*time.Second, func(i int, _ time.Time) float64 {
		if i%2 == 0 {
			return 990
		}

		return 1010
	})

	meanBefore, tolBefore, _ := b.Normal("rate")
	b.Observe("rate", 100000, at)

	mean, tol, ready := b.Normal("rate")
	require.True(t, ready)
	require.InEpsilon(t, meanBefore, mean, 0.01)
	require.InEpsilon(t, tolBefore, tol, 0.10)
}

func TestSustainedLevelShiftEventuallyBecomesNormal(t *testing.T) {
	b := NewBaseline(10 * time.Minute)
	step := 2 * time.Second
	alpha := 1 - math.Exp2(-float64(step)/float64(time.Hour))

	at := feed(b, "rate", t0, 400, step, constant(100))

	prev, _, _ := b.Normal("rate")

	for i := 0; i < 12*3600/2; i++ {
		_, tolBefore, _ := b.Normal("rate")
		b.Observe("rate", 200, at)
		at = at.Add(step)

		mean, _, _ := b.Normal("rate")
		require.GreaterOrEqual(t, mean, prev)
		require.LessOrEqual(t, mean-prev, alpha*8*tolBefore+1e-9)

		prev = mean
	}

	mean, _, ready := b.Normal("rate")
	require.True(t, ready)
	require.InEpsilon(t, 200, mean, 0.05)
}

func TestFirstObservationsAreNotClamped(t *testing.T) {
	b := NewBaseline(time.Minute)

	b.Observe("m", 1e6, t0)

	mean, _, _ := b.Normal("m")
	require.Equal(t, 1e6, mean)

	b.Observe("m", 5, t0.Add(2*time.Second))

	alpha := 1 - math.Exp2(-2.0/3600)
	mean, _, _ = b.Normal("m")
	require.InEpsilon(t, 1e6+alpha*(5-1e6), mean, 1e-12)
}

func TestClampDoesNotApplyBeforeFiveObservations(t *testing.T) {
	b := NewBaseline(time.Minute)
	alpha := 1 - math.Exp2(-2.0/3600)
	at := t0

	for i := 0; i < 4; i++ {
		b.Observe("m", 1000, at)
		at = at.Add(2 * time.Second)
	}

	b.Observe("m", 100000, at)
	at = at.Add(2 * time.Second)

	mean, _, _ := b.Normal("m")
	require.InEpsilon(t, 1000+alpha*99000, mean, 1e-12)

	_, tol, _ := b.Normal("m")
	b.Observe("m", 100000000, at)

	after, _, _ := b.Normal("m")
	require.LessOrEqual(t, after-mean, alpha*8*tol+1e-9)
	require.Greater(t, after, mean)
}

func TestIrregularSamplingWeightsByTime(t *testing.T) {
	step := func(_ int, at time.Time) float64 {
		if at.Sub(t0) < 2*time.Hour {
			return 10
		}

		return 20
	}

	fast := NewBaseline(10 * time.Minute)
	slow := NewBaseline(10 * time.Minute)

	feed(fast, "m", t0, 4*3600/2, 2*time.Second, step)
	feed(slow, "m", t0, 4*3600/20, 20*time.Second, step)

	fm, _, fr := fast.Normal("m")
	sm, _, sr := slow.Normal("m")
	require.True(t, fr)
	require.True(t, sr)
	require.InEpsilon(t, 17.5, fm, 0.01)
	require.InEpsilon(t, 17.5, sm, 0.01)
	require.InEpsilon(t, fm, sm, 0.01)
}

func TestNaNAndInfIgnored(t *testing.T) {
	b := NewBaseline(time.Minute)

	b.Observe("m", math.NaN(), t0)
	b.Observe("m", math.Inf(1), t0)
	b.Observe("m", math.Inf(-1), t0)

	_, _, ready := b.Normal("m")
	require.False(t, ready)
	require.False(t, b.Calibration(t0).Ready)
	require.Empty(t, b.State().Metrics)

	feed(b, "m", t0, 100, 2*time.Second, constant(5))
	b.Observe("m", math.NaN(), t0.Add(time.Hour))

	mean, _, ready := b.Normal("m")
	require.True(t, ready)
	require.InDelta(t, 5, mean, 1e-9)
	require.Equal(t, 100, b.State().Metrics["m"].Count)
}

func TestCalibrationFraction(t *testing.T) {
	b := NewBaseline(10 * time.Minute)

	c := b.Calibration(t0)
	require.False(t, c.Ready)
	require.Equal(t, 0.0, c.Fraction)
	require.True(t, c.Until.IsZero())

	feed(b, "a", t0, 2, time.Second, constant(1))
	feed(b, "b", t0.Add(4*time.Minute), 2, time.Second, constant(1))

	c = b.Calibration(t0.Add(5 * time.Minute))
	require.False(t, c.Ready)
	require.InDelta(t, 0.1, c.Fraction, 1e-9)

	c = b.Calibration(t0.Add(30 * time.Minute))
	require.False(t, c.Ready)
	require.Equal(t, 1.0, c.Fraction)
}

func TestCalibrationUntil(t *testing.T) {
	b := NewBaseline(10 * time.Minute)

	feed(b, "a", t0, 400, 2*time.Second, constant(1))
	feed(b, "b", t0.Add(4*time.Minute), 2, time.Second, constant(1))

	now := t0.Add(13 * time.Minute)
	c := b.Calibration(now)
	require.False(t, c.Ready)
	require.Equal(t, t0.Add(14*time.Minute), c.Until)

	feed(b, "b", t0.Add(4*time.Minute+2*time.Second), 400, 2*time.Second, constant(1))

	c = b.Calibration(t0.Add(15 * time.Minute))
	require.True(t, c.Ready)
	require.True(t, c.Until.IsZero())
	require.Equal(t, 1.0, c.Fraction)
}

func TestParseFloors(t *testing.T) {
	m, err := ParseFloors([]string{" tps = 100 ", "", "hit_rate=0.5", "  "})
	require.NoError(t, err)
	require.Equal(t, map[string]float64{"tps": 100, "hit_rate": 0.5}, m)

	m, err = ParseFloors(nil)
	require.NoError(t, err)
	require.Empty(t, m)
}

func TestParseFloorsRejectsBadEntry(t *testing.T) {
	bad := []string{"tps", "tps=", "=5", "tps=abc", "tps=NaN", "tps=Inf", "tps=-Inf", "a=1=2"}

	for _, e := range bad {
		_, err := ParseFloors([]string{"ok=1", e})
		require.Error(t, err, e)
		require.Contains(t, err.Error(), strconv.Quote(e), e)
	}

	_, err := ParseFloors([]string{"tps=1", "tps=2"})
	require.Error(t, err)
	require.Contains(t, err.Error(), "tps=2")
}

func TestBaselineStateRoundTrip(t *testing.T) {
	b := NewBaseline(10 * time.Minute)

	feed(b, "a", t0, 100, 2*time.Second, func(i int, _ time.Time) float64 { return float64(i % 7) })

	st := b.State()
	r := RestoreBaseline(st, 10*time.Minute)

	am, at, ar := b.Normal("a")
	rm, rt, rr := r.Normal("a")
	require.Equal(t, am, rm)
	require.Equal(t, at, rt)
	require.Equal(t, ar, rr)
	require.Equal(t, st, r.State())
}

func TestConcurrentUseIsRaceFree(t *testing.T) {
	b := NewBaseline(time.Minute)

	var wg sync.WaitGroup

	wg.Add(1)

	go func() {
		defer wg.Done()

		feed(b, "a", t0, 2000, time.Second, constant(3))
	}()

	for i := 0; i < 3; i++ {
		wg.Add(1)

		go func() {
			defer wg.Done()

			for j := 0; j < 500; j++ {
				b.Normal("a")
				b.Calibration(t0.Add(time.Duration(j) * time.Second))
				b.State()
			}
		}()
	}

	wg.Wait()
}

func TestCheckFloorKeys(t *testing.T) {
	known := map[string]float64{}
	for _, r := range DefaultRules {
		if r.FloorKey != "" {
			known[r.FloorKey] = 1
		}
	}

	require.Len(t, known, 8)
	require.NoError(t, CheckFloorKeys(known))
	require.NoError(t, CheckFloorKeys(nil))

	err := CheckFloorKeys(map[string]float64{"cache.hitRate": 0.9, "zeta": 1, "alpha": 2})
	require.Error(t, err)
	require.Contains(t, err.Error(), "alpha, zeta")
}
