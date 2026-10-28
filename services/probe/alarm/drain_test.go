package alarm

import (
	"math"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

var drainStart = time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)

func at(secs float64) time.Time {
	return drainStart.Add(time.Duration(secs * float64(time.Second)))
}

func TestShrinkingBacklogGivesEstimate(t *testing.T) {
	d := NewDrain(5 * time.Minute)

	for s := 0; s <= 120; s += 2 {
		d.Observe(at(float64(s)), 2200-10*float64(s))
	}

	secs, growing, ok := d.Seconds()

	require.True(t, ok)
	require.False(t, growing)
	require.InDelta(t, 100.0, secs, 1e-6)
}

func TestGrowingBacklogReportsGrowing(t *testing.T) {
	d := NewDrain(5 * time.Minute)

	for s := 0; s <= 120; s += 2 {
		d.Observe(at(float64(s)), 100+5*float64(s))
	}

	secs, growing, ok := d.Seconds()

	require.True(t, growing)
	require.False(t, ok)
	require.Zero(t, secs)
}

func TestFlatBacklogNotOK(t *testing.T) {
	d := NewDrain(5 * time.Minute)

	for s := 0; s <= 120; s += 2 {
		d.Observe(at(float64(s)), 500)
	}

	secs, growing, ok := d.Seconds()

	require.False(t, ok)
	require.False(t, growing)
	require.Zero(t, secs)
}

func TestNearlyFlatBacklogNotOK(t *testing.T) {
	d := NewDrain(5 * time.Minute)

	for s := 0; s <= 120; s += 2 {
		d.Observe(at(float64(s)), 500-0.001*float64(s))
	}

	secs, growing, ok := d.Seconds()

	require.False(t, ok)
	require.False(t, growing)
	require.Zero(t, secs)
}

func TestEmptyBacklogIsZero(t *testing.T) {
	d := NewDrain(5 * time.Minute)

	for s := 0; s <= 120; s += 2 {
		d.Observe(at(float64(s)), math.Max(0, 100-10*float64(s)))
	}

	secs, growing, ok := d.Seconds()

	require.True(t, ok)
	require.False(t, growing)
	require.Zero(t, secs)
}

func TestNotEnoughDataIsNotOK(t *testing.T) {
	d := NewDrain(5 * time.Minute)

	secs, growing, ok := d.Seconds()
	require.False(t, ok)
	require.False(t, growing)
	require.Zero(t, secs)

	d.Observe(at(0), 1000)

	_, growing, ok = d.Seconds()
	require.False(t, ok)
	require.False(t, growing)

	for s := 2; s <= 28; s += 2 {
		d.Observe(at(float64(s)), 1000-10*float64(s))
	}

	_, growing, ok = d.Seconds()
	require.False(t, ok)
	require.False(t, growing)

	d.Observe(at(30), 700)

	secs, growing, ok = d.Seconds()
	require.True(t, ok)
	require.False(t, growing)
	require.InDelta(t, 70.0, secs, 1e-6)
}

func TestWindowDropsOldObservations(t *testing.T) {
	d := NewDrain(5 * time.Minute)

	for s := 0; s <= 600; s += 2 {
		d.Observe(at(float64(s)), 10000-10*float64(s))
	}

	for s := 602; s <= 900; s += 2 {
		d.Observe(at(float64(s)), 4000+10*float64(s-600))
	}

	secs, growing, ok := d.Seconds()

	require.True(t, growing)
	require.False(t, ok)
	require.Zero(t, secs)
}

func TestNoisyShrinkingBacklogStillEstimates(t *testing.T) {
	d := NewDrain(5 * time.Minute)

	const (
		n          = 61.0
		stepSecs   = 2.0
		meanX      = stepSecs * (n - 1) / 2
		sxx        = stepSecs * stepSecs * n * (n*n - 1) / 12
		spikeUp    = 30.0
		spikeUpAt  = 10
		spikeDown  = -12.0
		spikeDnAt  = 45
		trueSlope  = -10.0
		altNoise   = 5.0
		biasSum    = spikeUp*(stepSecs*spikeUpAt-meanX) + spikeDown*(stepSecs*spikeDnAt-meanX)
		lsSlope    = trueSlope + biasSum/sxx
		newestSamp = 2200 + trueSlope*stepSecs*(n-1) + altNoise
		expected   = newestSamp / -lsSlope
	)

	for i := 0; i < int(n); i++ {
		noise := altNoise
		if i%2 == 1 {
			noise = -altNoise
		}

		switch i {
		case spikeUpAt:
			noise += spikeUp
		case spikeDnAt:
			noise += spikeDown
		}

		s := stepSecs * float64(i)
		d.Observe(at(s), 2200+trueSlope*s+noise)
	}

	secs, growing, ok := d.Seconds()

	require.True(t, ok)
	require.False(t, growing)
	require.InEpsilon(t, expected, secs, 1e-6)
	require.InEpsilon(t, 100.0, secs, 0.10)
}

func TestLeastSquaresNotEndpointSlope(t *testing.T) {
	d := NewDrain(5 * time.Minute)

	const (
		n         = 150.0
		stepSecs  = 2.0
		start     = 2000.0
		fall      = -5.0
		outlier   = 400.0
		lastX     = stepSecs * (n - 1)
		meanX     = lastX / 2
		sxx       = stepSecs * stepSecs * n * (n*n - 1) / 12
		lsSlope   = fall + outlier*(lastX-meanX)/sxx
		newest    = start + fall*lastX + outlier
		expected  = newest / -lsSlope
		endpoint  = (newest - start) / lastX
		endpointS = newest / -endpoint
	)

	for i := 0; i < int(n); i++ {
		s := stepSecs * float64(i)
		v := start + fall*s

		if i == int(n)-1 {
			v += outlier
		}

		d.Observe(at(s), v)
	}

	secs, growing, ok := d.Seconds()

	require.True(t, ok)
	require.False(t, growing)
	require.InEpsilon(t, expected, secs, 0.005)
	require.Greater(t, math.Abs(secs-endpointS)/endpointS, 0.10)
}

func TestFastObservationsStillSpanFullWindow(t *testing.T) {
	d := NewDrain(5 * time.Minute)

	const (
		start = 5000.0
		fall  = 10.0
	)

	var newest float64

	for i := 0; i <= 1800; i++ {
		ts := drainStart.Add(time.Duration(i) * 200 * time.Millisecond)
		secs := float64(i) * 0.2
		newest = start - fall*secs
		d.Observe(ts, newest)
	}

	require.Less(t, len(d.obs), 330)

	span := d.obs[len(d.obs)-1].t.Sub(d.obs[0].t)
	require.GreaterOrEqual(t, span, 295*time.Second)

	secs, growing, ok := d.Seconds()

	require.True(t, ok)
	require.False(t, growing)
	require.InEpsilon(t, newest/fall, secs, 0.01)
}

func TestObservationWithinOneSecondReplacesNewest(t *testing.T) {
	d := NewDrain(5 * time.Minute)

	d.Observe(at(0), 100)
	d.Observe(at(0.5), 200)
	require.Len(t, d.obs, 1)
	require.Equal(t, at(0.5), d.obs[0].t)
	require.Equal(t, 200.0, d.obs[0].backlog)

	d.Observe(at(0.5), 999)
	d.Observe(at(0.2), 999)
	require.Len(t, d.obs, 1)
	require.Equal(t, 200.0, d.obs[0].backlog)

	d.Observe(at(1.0), 300)
	require.Len(t, d.obs, 2)

	d.Observe(at(1.9), 400)
	require.Len(t, d.obs, 2)
	require.Equal(t, 400.0, d.obs[1].backlog)
	require.Equal(t, at(1.9), d.obs[1].t)

	d.Observe(at(2.0), 500)
	require.Len(t, d.obs, 3)
}

func TestInvalidObservationsIgnored(t *testing.T) {
	d := NewDrain(5 * time.Minute)

	d.Observe(at(0), 1000)
	d.Observe(at(2), math.NaN())
	d.Observe(at(4), math.Inf(1))
	d.Observe(at(6), math.Inf(-1))
	d.Observe(at(8), -5)
	require.Len(t, d.obs, 1)

	d.Observe(at(10), 900)
	d.Observe(at(10), 5000)
	d.Observe(at(5), 5000)
	require.Len(t, d.obs, 2)

	for s := 20; s <= 60; s += 10 {
		d.Observe(at(float64(s)), 1000-10*float64(s))
	}

	secs, growing, ok := d.Seconds()

	require.True(t, ok)
	require.False(t, growing)
	require.Greater(t, secs, 0.0)
	require.Len(t, d.obs, 7)
}

func TestObservationCountIsBounded(t *testing.T) {
	d := NewDrain(5 * time.Minute)

	for i := 0; i < 200000; i++ {
		d.Observe(drainStart.Add(time.Duration(i)*time.Millisecond), 1000)
	}

	require.LessOrEqual(t, len(d.obs), 600)
	require.Equal(t, drainStart.Add(199999*time.Millisecond), d.obs[len(d.obs)-1].t)
}

func TestConcurrentReadersAreRaceFree(t *testing.T) {
	d := NewDrain(5 * time.Minute)

	var wg sync.WaitGroup

	for r := 0; r < 4; r++ {
		wg.Add(1)

		go func() {
			defer wg.Done()

			for i := 0; i < 500; i++ {
				d.Seconds()
			}
		}()
	}

	for s := 0; s <= 400; s += 2 {
		d.Observe(at(float64(s)), 5000-10*float64(s))
	}

	wg.Wait()

	secs, _, ok := d.Seconds()

	require.True(t, ok)
	require.InDelta(t, 100.0, secs, 1e-6)
}
