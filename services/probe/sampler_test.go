package probe

import (
	"context"
	"encoding/json"
	"fmt"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/services/probe/snapshot"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/stretchr/testify/require"
)

type fakeCollector struct {
	name string
	fn   func(ctx context.Context, call int) (snapshot.Patch, error)

	mu    sync.Mutex
	calls []time.Time
}

func newFake(name string, fn func(ctx context.Context, call int) (snapshot.Patch, error)) *fakeCollector {
	return &fakeCollector{name: name, fn: fn}
}

func (f *fakeCollector) Name() string {
	return f.name
}

func (f *fakeCollector) Collect(ctx context.Context) (snapshot.Patch, error) {
	f.mu.Lock()
	f.calls = append(f.calls, time.Now())
	n := len(f.calls)
	f.mu.Unlock()

	return f.fn(ctx, n)
}

func (f *fakeCollector) Calls() int {
	f.mu.Lock()
	defer f.mu.Unlock()

	return len(f.calls)
}

func (f *fakeCollector) CallTimes() []time.Time {
	f.mu.Lock()
	defer f.mu.Unlock()

	return append([]time.Time(nil), f.calls...)
}

func okPatch(p snapshot.Patch) func(context.Context, int) (snapshot.Patch, error) {
	return func(context.Context, int) (snapshot.Patch, error) { return p, nil }
}

type fakeClock struct {
	mu sync.Mutex
	t  time.Time
}

func newFakeClock() *fakeClock {
	return &fakeClock{t: time.Date(2026, 10, 28, 10, 0, 0, 0, time.UTC)}
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.t
}

func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.t = c.t.Add(d)
}

type recordingLogger struct {
	ulogger.TestLogger

	mu    sync.Mutex
	lines []string
}

func (l *recordingLogger) Errorf(format string, args ...interface{}) {
	l.mu.Lock()
	defer l.mu.Unlock()

	l.lines = append(l.lines, fmt.Sprintf(format, args...))
}

func (l *recordingLogger) Warnf(format string, args ...interface{}) {
	l.mu.Lock()
	defer l.mu.Unlock()

	l.lines = append(l.lines, "WARN "+fmt.Sprintf(format, args...))
}

func (l *recordingLogger) Infof(format string, args ...interface{}) {
	l.mu.Lock()
	defer l.mu.Unlock()

	l.lines = append(l.lines, "INFO "+fmt.Sprintf(format, args...))
}

func (l *recordingLogger) Lines() []string {
	l.mu.Lock()
	defer l.mu.Unlock()

	return append([]string(nil), l.lines...)
}

func startSampler(t *testing.T, s *Sampler) context.CancelFunc {
	t.Helper()

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})

	go func() {
		s.Run(ctx)
		close(done)
	}()

	t.Cleanup(func() {
		cancel()
		<-done
	})

	return cancel
}

func waitForUpdate(t *testing.T, s *Sampler, within time.Duration, match func(snapshot.Snapshot) bool) snapshot.Snapshot {
	t.Helper()

	deadline := time.After(within)

	for {
		select {
		case snap := <-s.Updates():
			if match(snap) {
				return snap
			}
		case <-deadline:
			require.FailNow(t, "no matching update published in time")
		}
	}
}

func anyUpdate(snapshot.Snapshot) bool {
	return true
}

func TestSlowCollectorRunsLessOften(t *testing.T) {
	fast := newFake("fast", okPatch(func(s *snapshot.Snapshot) {
		s.Cache.HitRate = snapshot.Reading{Value: 0.9, Status: snapshot.StatusOK}
	}))
	slow := newFake("slow", okPatch(func(s *snapshot.Snapshot) {
		s.Stores.Aerospike.SizeGB = snapshot.Reading{Value: 7, Status: snapshot.StatusOK}
	}))

	s := NewSampler([]Collector{fast}, []Collector{slow}, 20*time.Millisecond, 200*time.Millisecond, "node-1")
	startSampler(t, s)

	require.Eventually(t, func() bool { return fast.Calls() >= 40 }, 5*time.Second, 5*time.Millisecond)

	fastCalls := fast.Calls()
	slowCalls := slow.Calls()

	require.GreaterOrEqual(t, slowCalls, 2)
	require.LessOrEqual(t, slowCalls, fastCalls/4)

	snap := waitForUpdate(t, s, time.Second, anyUpdate)
	require.Equal(t, "node-1", snap.Node)
	require.Equal(t, snapshot.Reading{Value: 0.9, Status: snapshot.StatusOK}, snap.Cache.HitRate)
	require.Equal(t, snapshot.Reading{Value: 7, Status: snapshot.StatusOK}, snap.Stores.Aerospike.SizeGB)
}

func TestFailingCollectorMarksNoData(t *testing.T) {
	clock := newFakeClock()
	t0 := clock.Now()

	failErr := newFake("fail-err", func(_ context.Context, call int) (snapshot.Patch, error) {
		if call == 1 {
			return func(s *snapshot.Snapshot) {
				s.Cache.HitRate = snapshot.Reading{Value: 0.9, Status: snapshot.StatusOK, Note: "live note"}
				s.Flow(snapshot.FlowTxMeta).Rate = snapshot.Reading{Value: 5, Status: snapshot.StatusOK}
				s.FSM = "RUNNING"
				s.Tip = 100
				s.Chain.BlocksWaiting = []uint32{7}
			}, nil
		}

		return func(s *snapshot.Snapshot) { s.Cache.HitRate.Value = 0.1 }, errors.NewServiceError("source down")
	})

	failNil := newFake("fail-nil", func(_ context.Context, call int) (snapshot.Patch, error) {
		if call == 1 {
			return func(s *snapshot.Snapshot) {
				s.Validator.QueueFill = snapshot.Reading{Value: 0.4, Status: snapshot.StatusWarn}
			}, nil
		}

		return nil, nil
	})

	dead := newFake("dead", func(context.Context, int) (snapshot.Patch, error) {
		return nil, errors.NewServiceError("never up")
	})

	s := NewSampler([]Collector{failErr, failNil, dead}, nil, 20*time.Millisecond, time.Hour, "n", WithClock(clock.Now))
	startSampler(t, s)

	first := waitForUpdate(t, s, 2*time.Second, anyUpdate)
	require.Equal(t, t0, first.Time)
	require.Equal(t, snapshot.Reading{Value: 0.9, Status: snapshot.StatusOK, Note: "live note"}, first.Cache.HitRate)
	require.Equal(t, snapshot.Reading{Value: 0.4, Status: snapshot.StatusWarn}, first.Validator.QueueFill)

	clock.Advance(30 * time.Second)

	stale := waitForUpdate(t, s, 2*time.Second, func(snap snapshot.Snapshot) bool { return snap.Time.Equal(t0.Add(30 * time.Second)) })

	require.Equal(t, snapshot.Reading{Value: 0.9, Status: snapshot.StatusNoData, AgeSec: 30}, stale.Cache.HitRate)
	require.Equal(t, snapshot.Reading{Value: 5, Status: snapshot.StatusNoData, AgeSec: 30}, stale.Flow(snapshot.FlowTxMeta).Rate)
	require.Equal(t, snapshot.Reading{Value: 0.4, Status: snapshot.StatusNoData, AgeSec: 30}, stale.Validator.QueueFill)
	require.Equal(t, "RUNNING", stale.FSM)
	require.Equal(t, uint32(100), stale.Tip)
	require.Equal(t, []uint32{7}, stale.Chain.BlocksWaiting)

	nodata := snapshot.Reading{Status: snapshot.StatusNoData}
	require.Equal(t, snapshot.AssemblyStage{
		Paused: nodata, TipLagBlocks: nodata, QueueHeadAgeSec: nodata, Transactions: nodata, IngestQueueFill: nodata, SubtreeFill: nodata,
	}, stale.Assembly)
	require.Equal(t, snapshot.IngressStage{
		PropagationTxPerSec: nodata, SubtreeIntakePerSec: nodata, Peers: nodata,
		RejectedPerSec: nodata, SubtreeQueue: nodata, SubtreeQueueOldestSec: nodata,
	}, stale.Ingress)
	require.Equal(t, nodata, stale.Flow(snapshot.FlowBlocks).Rate)
	require.Equal(t, nodata, stale.Validator.ErrorsPerSec)
	require.Equal(t, nodata, stale.Stores.Postgres.SizeGB)

	require.GreaterOrEqual(t, dead.Calls(), 2)
}

func TestHungCollectorDoesNotStallTick(t *testing.T) {
	release := make(chan struct{})
	t.Cleanup(func() { close(release) })

	hung := newFake("hung", func(context.Context, int) (snapshot.Patch, error) {
		<-release
		return nil, nil
	})
	ok := newFake("ok", okPatch(func(s *snapshot.Snapshot) {
		s.Cache.Fill = snapshot.Reading{Value: 0.5, Status: snapshot.StatusOK}
	}))

	fastEvery := 250 * time.Millisecond

	s := NewSampler([]Collector{hung, ok}, nil, fastEvery, time.Hour, "n")
	start := time.Now()
	startSampler(t, s)

	prev := start

	for i := 0; i < 4; i++ {
		snap := waitForUpdate(t, s, 2*fastEvery, anyUpdate)
		require.LessOrEqual(t, time.Since(prev), 2*fastEvery)
		require.Equal(t, snapshot.Reading{Value: 0.5, Status: snapshot.StatusOK}, snap.Cache.Fill)

		prev = time.Now()
	}

	require.Equal(t, 1, hung.Calls())
}

func TestPanicRecovered(t *testing.T) {
	logger := &recordingLogger{}

	bad := newFake("exploder", func(context.Context, int) (snapshot.Patch, error) {
		panic("boom")
	})
	good := newFake("good", okPatch(func(s *snapshot.Snapshot) {
		s.Cache.HitRate = snapshot.Reading{Value: 0.8, Status: snapshot.StatusOK}
	}))

	s := NewSampler([]Collector{bad, good}, nil, 20*time.Millisecond, time.Hour, "n", WithLogger(logger))
	startSampler(t, s)

	require.Eventually(t, func() bool { return bad.Calls() >= 3 }, 5*time.Second, 5*time.Millisecond)

	snap := waitForUpdate(t, s, time.Second, anyUpdate)
	require.Equal(t, snapshot.Reading{Value: 0.8, Status: snapshot.StatusOK}, snap.Cache.HitRate)
	require.GreaterOrEqual(t, s.ConsecutivePanics(), 1)

	lines := logger.Lines()
	require.NotEmpty(t, lines)

	for _, line := range lines {
		require.Contains(t, line, "exploder")
		require.Contains(t, line, "boom")
		require.NotContains(t, line, "\n")
	}
}

func TestPatchPanicRecovered(t *testing.T) {
	logger := &recordingLogger{}

	bad := newFake("bad-patch", okPatch(func(s *snapshot.Snapshot) {
		var m map[string]int
		m["x"] = 1
	}))
	good := newFake("good", okPatch(func(s *snapshot.Snapshot) {
		s.Cache.HitRate = snapshot.Reading{Value: 0.8, Status: snapshot.StatusOK}
	}))

	s := NewSampler([]Collector{bad, good}, nil, 20*time.Millisecond, time.Hour, "n", WithLogger(logger))
	startSampler(t, s)

	snap := waitForUpdate(t, s, 2*time.Second, anyUpdate)
	require.Equal(t, snapshot.Reading{Value: 0.8, Status: snapshot.StatusOK}, snap.Cache.HitRate)
	require.GreaterOrEqual(t, s.ConsecutivePanics(), 1)
	require.NotEmpty(t, logger.Lines())
	require.Contains(t, logger.Lines()[0], "bad-patch")
}

func TestHistoryWindow(t *testing.T) {
	clock := newFakeClock()
	fastEvery := 2 * time.Second

	s := NewSampler(nil, nil, fastEvery, 15*time.Second, "n", WithClock(clock.Now))

	for i := 0; i < 400; i++ {
		clock.Advance(fastEvery)
		s.fastTick(context.Background())
	}

	now := clock.Now()

	all := s.History(10 * time.Minute)
	require.Len(t, all, 301)
	require.Equal(t, now.Add(-10*time.Minute), all[0].Time)
	require.Equal(t, now, all[len(all)-1].Time)

	for i := 1; i < len(all); i++ {
		require.True(t, all[i-1].Time.Before(all[i].Time))
	}

	require.Len(t, s.History(time.Minute), 31)
	require.Len(t, s.History(time.Hour), 301)
	require.Empty(t, s.History(0))
	require.LessOrEqual(t, s.historyLen(), 301)

	all[0].Flows[0].Rate.Value = 99
	require.NotEqual(t, float64(99), s.History(10 * time.Minute)[0].Flows[0].Rate.Value)

	clock.Advance(20 * time.Minute)
	require.Empty(t, s.History(10*time.Minute))

	s.fastTick(context.Background())
	require.Len(t, s.History(10*time.Minute), 1)
}

func TestHistoryWindowHoldsTenMinutesUnderTriggers(t *testing.T) {
	clock := newFakeClock()
	fastEvery := 2 * time.Second
	triggerStep := 200 * time.Millisecond

	s := NewSampler(nil, nil, fastEvery, 15*time.Second, "n", WithClock(clock.Now))

	steps := int(12 * time.Minute / triggerStep)

	for i := 1; i <= steps; i++ {
		clock.Advance(triggerStep)
		s.fastTick(context.Background())

		if i%int(fastEvery/triggerStep) == 0 {
			s.fastTick(context.Background())
		}
	}

	now := clock.Now()

	all := s.History(10 * time.Minute)
	require.Len(t, all, int(10*time.Minute/fastEvery)+1)
	require.False(t, all[0].Time.Before(now.Add(-10*time.Minute)))
	require.LessOrEqual(t, all[0].Time.Sub(now.Add(-10*time.Minute)), fastEvery)
	require.Equal(t, now, all[len(all)-1].Time)

	for i := 1; i < len(all); i++ {
		gap := all[i].Time.Sub(all[i-1].Time)
		require.LessOrEqual(t, gap, fastEvery, i)

		if i < len(all)-1 {
			require.GreaterOrEqual(t, gap, fastEvery-triggerStep, i)
		}
	}

	require.Len(t, s.History(time.Minute), int(time.Minute/fastEvery)+1)
	require.LessOrEqual(t, s.historyLen(), int(10*time.Minute/fastEvery)+1)
}

func TestHistoryKeepsEveryRegularTickDespiteJitter(t *testing.T) {
	clock := newFakeClock()
	fastEvery := 2 * time.Second
	start := clock.Now()

	s := NewSampler(nil, nil, fastEvery, 15*time.Second, "n", WithClock(clock.Now))

	var ticks []time.Time

	for k := 1; k <= 400; k++ {
		jitter := time.Duration(k%3-1) * 30 * time.Millisecond
		at := start.Add(time.Duration(k)*fastEvery + jitter)
		clock.Advance(at.Sub(clock.Now()))
		s.fastTick(context.Background())

		ticks = append(ticks, at)
	}

	now := clock.Now()

	for _, d := range []time.Duration{time.Minute, 5 * time.Minute} {
		var want []time.Time

		for _, at := range ticks {
			if !at.Before(now.Add(-d)) {
				want = append(want, at)
			}
		}

		got := s.History(d)
		require.Len(t, got, len(want), d)

		for i := range want {
			require.Equal(t, want[i], got[i].Time, d)
		}
	}
}

func TestNoGoroutinePileUpWithHungCollector(t *testing.T) {
	release := make(chan struct{})
	t.Cleanup(func() { close(release) })

	hung := newFake("hung", func(context.Context, int) (snapshot.Patch, error) {
		<-release
		return nil, nil
	})
	ok := newFake("ok", okPatch(func(*snapshot.Snapshot) {}))

	s := NewSampler([]Collector{hung, ok}, nil, 20*time.Millisecond, time.Hour, "n")
	startSampler(t, s)

	require.Eventually(t, func() bool { return ok.Calls() >= 3 }, 5*time.Second, 5*time.Millisecond)

	before := runtime.NumGoroutine()
	base := ok.Calls()

	require.Eventually(t, func() bool { return ok.Calls() >= base+20 }, 5*time.Second, 5*time.Millisecond)

	require.LessOrEqual(t, runtime.NumGoroutine(), before+2)
	require.Equal(t, 1, hung.Calls())
}

func TestHungCollectorServesStalePatchWithAge(t *testing.T) {
	clock := newFakeClock()
	t0 := clock.Now()

	release := make(chan struct{})
	t.Cleanup(func() { close(release) })

	src := newFake("src", func(_ context.Context, call int) (snapshot.Patch, error) {
		if call == 1 {
			return func(s *snapshot.Snapshot) {
				s.Cache.HitRate = snapshot.Reading{Value: 0.8, Status: snapshot.StatusOK}
				s.FSM = "RUNNING"
			}, nil
		}

		<-release

		return nil, nil
	})

	s := NewSampler([]Collector{src}, nil, 100*time.Millisecond, time.Hour, "n", WithClock(clock.Now))
	startSampler(t, s)

	first := waitForUpdate(t, s, 2*time.Second, anyUpdate)
	require.Equal(t, snapshot.Reading{Value: 0.8, Status: snapshot.StatusOK}, first.Cache.HitRate)

	clock.Advance(10 * time.Second)

	snap := waitForUpdate(t, s, 2*time.Second, func(snap snapshot.Snapshot) bool { return snap.Time.Equal(t0.Add(10 * time.Second)) })
	require.Equal(t, snapshot.Reading{Value: 0.8, Status: snapshot.StatusNoData, AgeSec: 10}, snap.Cache.HitRate)
	require.Equal(t, "RUNNING", snap.FSM)

	clock.Advance(5 * time.Second)

	snap = waitForUpdate(t, s, 2*time.Second, func(snap snapshot.Snapshot) bool { return snap.Time.Equal(t0.Add(15 * time.Second)) })
	require.Equal(t, snapshot.Reading{Value: 0.8, Status: snapshot.StatusNoData, AgeSec: 15}, snap.Cache.HitRate)
	require.Equal(t, 2, src.Calls())
}

func TestSourcesReportHealthPerCollector(t *testing.T) {
	clock := newFakeClock()
	t0 := clock.Now()

	healthy := newFake("a", okPatch(func(*snapshot.Snapshot) {}))
	failing := newFake("b", func(context.Context, int) (snapshot.Patch, error) {
		return nil, errors.NewProcessingError("down")
	})
	flaky := newFake("d", func(_ context.Context, call int) (snapshot.Patch, error) {
		if call == 1 {
			return func(*snapshot.Snapshot) {}, nil
		}

		return nil, errors.NewProcessingError("down")
	})
	slow := newFake("c", okPatch(func(*snapshot.Snapshot) {}))

	s := NewSampler([]Collector{healthy, failing, flaky}, []Collector{slow}, 100*time.Millisecond, time.Hour, "n", WithClock(clock.Now))
	startSampler(t, s)

	require.Eventually(t, func() bool { return slow.Calls() == 1 && flaky.Calls() >= 2 }, 2*time.Second, 10*time.Millisecond)

	clock.Advance(10 * time.Second)

	snap := waitForUpdate(t, s, 2*time.Second, func(snap snapshot.Snapshot) bool {
		return snap.Time.Equal(t0.Add(10*time.Second)) && len(snap.Sources) == 4 && snap.Sources[3].OK
	})

	require.Equal(t, []snapshot.SourceHealth{
		{Name: "a", OK: true},
		{Name: "b", OK: false, AgeSec: 10},
		{Name: "d", OK: false, AgeSec: 10},
		{Name: "c", OK: true},
	}, snap.Sources)
	require.Equal(t, 1, slow.Calls())
}

func TestNeverSucceededSourceAgesFromStart(t *testing.T) {
	clock := newFakeClock()
	t0 := clock.Now()

	failing := newFake("aerospike", func(context.Context, int) (snapshot.Patch, error) {
		return nil, errors.NewProcessingError("refused")
	})

	s := NewSampler([]Collector{failing}, nil, 100*time.Millisecond, time.Hour, "n", WithClock(clock.Now))
	startSampler(t, s)

	first := waitForUpdate(t, s, 2*time.Second, anyUpdate)
	require.Equal(t, []snapshot.SourceHealth{{Name: "aerospike", OK: false}}, first.Sources)

	clock.Advance(61 * time.Second)

	snap := waitForUpdate(t, s, 2*time.Second, func(snap snapshot.Snapshot) bool { return snap.Time.Equal(t0.Add(61 * time.Second)) })
	require.Equal(t, []snapshot.SourceHealth{{Name: "aerospike", OK: false, AgeSec: 61}}, snap.Sources)
}

func TestSlicesNeverNull(t *testing.T) {
	withAlarm := newFake("alarm", okPatch(func(s *snapshot.Snapshot) {
		s.Alarms = []snapshot.Alarm{{ID: "a1", Stage: "cache"}}
		s.Chain.BlocksWaiting = nil
	}))

	cases := map[string][]Collector{
		"no collectors":   nil,
		"alarm with nils": {withAlarm},
	}

	for name, fast := range cases {
		t.Run(name, func(t *testing.T) {
			s := NewSampler(fast, nil, time.Hour, time.Hour, "n")
			startSampler(t, s)

			published := waitForUpdate(t, s, 2*time.Second, anyUpdate)

			views := map[string]snapshot.Snapshot{
				"published": published,
				"latest":    s.Latest(),
				"history":   s.History(time.Minute)[0],
			}

			for view, snap := range views {
				out, err := json.Marshal(snap)
				require.NoError(t, err)

				js := string(out)
				require.NotContains(t, js, "null", view)
				require.Contains(t, js, `"blocksWaiting":[]`, view)
				require.Contains(t, js, `"flows":[{`, view)

				if fast == nil {
					require.Contains(t, js, `"alarms":[]`, view)
					require.Contains(t, js, `"sources":[]`, view)
				} else {
					require.Contains(t, js, `"drilldowns":[]`, view)
				}
			}
		})
	}
}

func TestTriggerRefreshesEarlyAndCoalesces(t *testing.T) {
	c := newFake("c", okPatch(func(*snapshot.Snapshot) {}))

	s := NewSampler([]Collector{c}, nil, time.Hour, time.Hour, "n")

	trig := make(chan struct{})
	s.Trigger(trig)
	startSampler(t, s)

	require.Eventually(t, func() bool { return c.Calls() == 1 }, 2*time.Second, 5*time.Millisecond)

	for i := 0; i < 50; i++ {
		trig <- struct{}{}
	}

	require.Eventually(t, func() bool { return c.Calls() >= 2 }, 2*time.Second, 5*time.Millisecond)
	require.Never(t, func() bool { return c.Calls() > 3 }, 700*time.Millisecond, 10*time.Millisecond)

	before := c.Calls()
	trig <- struct{}{}

	require.Eventually(t, func() bool { return c.Calls() == before+1 }, 2*time.Second, 5*time.Millisecond)

	times := c.CallTimes()
	for i := 1; i < len(times); i++ {
		require.GreaterOrEqual(t, times[i].Sub(times[i-1]), 180*time.Millisecond)
	}
}

func TestUpdatesLatestWinsDoesNotBlock(t *testing.T) {
	var n atomic.Int64

	c := newFake("counter", func(context.Context, int) (snapshot.Patch, error) {
		v := float64(n.Add(1))

		return func(s *snapshot.Snapshot) {
			s.Cache.EntriesMillions = snapshot.Reading{Value: v, Status: snapshot.StatusOK}
		}, nil
	})

	s := NewSampler([]Collector{c}, nil, 10*time.Millisecond, time.Hour, "n")
	startSampler(t, s)

	require.Eventually(t, func() bool { return c.Calls() >= 30 }, 5*time.Second, 5*time.Millisecond)

	snap := <-s.Updates()
	require.GreaterOrEqual(t, snap.Cache.EntriesMillions.Value, float64(20))
}

func TestLatestReturnsDeepCopy(t *testing.T) {
	normal := 0.5
	lag := int64(12)
	drain := 30.0

	c := newFake("c", okPatch(func(s *snapshot.Snapshot) {
		n := normal
		l := lag
		d := drain
		s.Cache.HitRate = snapshot.Reading{Value: 0.9, Status: snapshot.StatusOK, Normal: &n}
		s.Flow(snapshot.FlowTxInput).Lag = &l
		s.Chain.BlocksWaiting = []uint32{1, 2}
		s.Alarms = []snapshot.Alarm{{ID: "a1", Drilldowns: []snapshot.Link{{Label: "l", Path: "/p"}}}}
		s.DrainSeconds = &d
	}))

	s := NewSampler([]Collector{c}, nil, time.Hour, time.Hour, "n")
	startSampler(t, s)

	published := waitForUpdate(t, s, 2*time.Second, anyUpdate)
	want := s.Latest()
	require.Equal(t, published, want)

	mutate := func(snap snapshot.Snapshot) {
		*snap.Cache.HitRate.Normal = 99
		*snap.Flow(snapshot.FlowTxInput).Lag = 99
		snap.Flows[0].Rate.Value = 99
		snap.Chain.BlocksWaiting[0] = 99
		snap.Alarms[0].ID = "mutated"
		snap.Alarms[0].Drilldowns[0].Path = "/mutated"
		*snap.DrainSeconds = 99
		snap.Sources[0].Name = "mutated"
	}

	mutate(s.Latest())
	mutate(published)
	mutate(s.History(time.Minute)[0])

	got := s.Latest()
	require.Equal(t, normal, *got.Cache.HitRate.Normal)
	require.Equal(t, lag, *got.Flow(snapshot.FlowTxInput).Lag)
	require.Equal(t, snapshot.StatusNoData, got.Flows[0].Rate.Status)
	require.Equal(t, float64(0), got.Flows[0].Rate.Value)
	require.Equal(t, []uint32{1, 2}, got.Chain.BlocksWaiting)
	require.Equal(t, "a1", got.Alarms[0].ID)
	require.Equal(t, "/p", got.Alarms[0].Drilldowns[0].Path)
	require.Equal(t, drain, *got.DrainSeconds)
	require.Equal(t, []snapshot.SourceHealth{{Name: "c", OK: true}}, got.Sources)

	hist := s.History(time.Minute)[0]
	require.Equal(t, "c", hist.Sources[0].Name)
	require.Equal(t, "a1", hist.Alarms[0].ID)
	require.Equal(t, normal, *hist.Cache.HitRate.Normal)
}

func TestConsecutivePanicsResets(t *testing.T) {
	var panicking atomic.Bool

	panicking.Store(true)

	fast := newFake("flaky", func(context.Context, int) (snapshot.Patch, error) {
		if panicking.Load() {
			panic("flaky")
		}

		return func(*snapshot.Snapshot) {}, nil
	})
	slow := newFake("steady", okPatch(func(*snapshot.Snapshot) {}))

	s := NewSampler([]Collector{fast}, []Collector{slow}, time.Hour, 50*time.Millisecond, "n", WithLogger(&recordingLogger{}))

	trig := make(chan struct{})
	s.Trigger(trig)
	startSampler(t, s)

	waitForUpdate(t, s, 2*time.Second, anyUpdate)
	require.Equal(t, 1, s.ConsecutivePanics())

	tick := func() {
		trig <- struct{}{}

		waitForUpdate(t, s, 2*time.Second, anyUpdate)
	}

	tick()
	require.Equal(t, 2, s.ConsecutivePanics())

	tick()
	require.Equal(t, 3, s.ConsecutivePanics())
	require.Greater(t, slow.Calls(), 1)

	panicking.Store(false)
	tick()
	require.Equal(t, 0, s.ConsecutivePanics())

	panicking.Store(true)
	tick()
	require.Equal(t, 1, s.ConsecutivePanics())
}

func TestLateReturnOfHungCollector(t *testing.T) {
	clock := newFakeClock()

	release := make(chan struct{})

	src := newFake("src", func(_ context.Context, call int) (snapshot.Patch, error) {
		switch call {
		case 1:
			return func(s *snapshot.Snapshot) {
				s.Cache.HitRate = snapshot.Reading{Value: 0.5, Status: snapshot.StatusOK}
			}, nil
		case 2:
			<-release

			return func(s *snapshot.Snapshot) {
				s.Cache.HitRate = snapshot.Reading{Value: 0.99, Status: snapshot.StatusOK}
			}, nil
		default:
			return func(s *snapshot.Snapshot) {
				s.Cache.HitRate = snapshot.Reading{Value: 0.7, Status: snapshot.StatusOK}
			}, nil
		}
	})

	s := NewSampler([]Collector{src}, nil, 100*time.Millisecond, time.Hour, "n", WithClock(clock.Now))
	startSampler(t, s)

	waitForUpdate(t, s, 2*time.Second, func(snap snapshot.Snapshot) bool { return snap.Cache.HitRate.Value == 0.5 })

	clock.Advance(10 * time.Second)

	stale := waitForUpdate(t, s, 2*time.Second, func(snap snapshot.Snapshot) bool {
		return snap.Cache.HitRate.Status == snapshot.StatusNoData
	})
	require.Equal(t, snapshot.Reading{Value: 0.5, Status: snapshot.StatusNoData, AgeSec: 10}, stale.Cache.HitRate)

	close(release)

	var seen []float64

	waitForUpdate(t, s, 2*time.Second, func(snap snapshot.Snapshot) bool {
		seen = append(seen, snap.Cache.HitRate.Value)
		return snap.Cache.HitRate.Value == 0.7
	})

	require.NotContains(t, seen, 0.99)
	require.Equal(t, snapshot.Reading{Value: 0.7, Status: snapshot.StatusOK}, s.Latest().Cache.HitRate)
	require.GreaterOrEqual(t, src.Calls(), 3)
}

func TestRunReturnsDespiteHungCollector(t *testing.T) {
	release := make(chan struct{})

	hung := newFake("hung", func(context.Context, int) (snapshot.Patch, error) {
		<-release
		return nil, nil
	})
	ok := newFake("ok", okPatch(func(*snapshot.Snapshot) {}))

	baseline := runtime.NumGoroutine()

	s := NewSampler([]Collector{hung, ok}, []Collector{ok}, 20*time.Millisecond, 20*time.Millisecond, "n")

	trig := make(chan struct{})
	s.Trigger(trig)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})

	go func() {
		s.Run(ctx)
		close(done)
	}()

	require.Eventually(t, func() bool { return hung.Calls() == 1 && ok.Calls() >= 3 }, 5*time.Second, 5*time.Millisecond)

	cancel()

	select {
	case <-done:
	case <-time.After(1500 * time.Millisecond):
		require.FailNow(t, "Run did not return after cancel")
	}

	close(release)

	require.True(t, goroutinesSettleTo(baseline, 5*time.Second), "goroutines leaked after Run returned")
}

func goroutinesSettleTo(limit int, within time.Duration) bool {
	deadline := time.Now().Add(within)

	for time.Now().Before(deadline) {
		if runtime.NumGoroutine() <= limit {
			return true
		}

		runtime.Gosched()
		time.Sleep(10 * time.Millisecond)
	}

	return runtime.NumGoroutine() <= limit
}

func TestNoReadingEverHasEmptyStatus(t *testing.T) {
	dead := newFake("dead", func(context.Context, int) (snapshot.Patch, error) {
		return nil, errors.NewServiceError("never up")
	})
	partial := newFake("partial", okPatch(func(s *snapshot.Snapshot) {
		s.Cache.HitRate = snapshot.Reading{Value: 0.9, Status: snapshot.StatusOK}
	}))

	cases := map[string][]Collector{
		"no collectors":       nil,
		"never succeeded":     {dead},
		"one partial success": {dead, partial},
	}

	for name, fast := range cases {
		t.Run(name, func(t *testing.T) {
			s := NewSampler(fast, nil, time.Hour, time.Hour, "n")
			startSampler(t, s)

			published := waitForUpdate(t, s, 2*time.Second, anyUpdate)

			count := 0

			published.EachReading(func(key string, r *snapshot.Reading) {
				count++

				require.NotEmpty(t, r.Status, key)

				if key != "cache.hitRate" || len(fast) < 2 {
					require.Equal(t, snapshot.StatusNoData, r.Status, key)
				}
			})

			require.Equal(t, 75, count)

			if len(fast) == 2 {
				require.Equal(t, snapshot.Reading{Value: 0.9, Status: snapshot.StatusOK}, published.Cache.HitRate)
			}
		})
	}
}

func TestSamplerLatestBeforeFirstTick(t *testing.T) {
	s := NewSampler(nil, nil, time.Second, time.Second, "node-x")

	snap := s.Latest()
	require.Equal(t, "node-x", snap.Node)
	require.Len(t, snap.Flows, len(snapshot.NewSnapshot("").Flows))

	out, err := json.Marshal(snap)
	require.NoError(t, err)
	require.False(t, strings.Contains(string(out), "null"))
}

func runUntilStopped(t *testing.T, s *Sampler, started <-chan struct{}) time.Duration {
	t.Helper()

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})

	go func() {
		s.Run(ctx)
		close(done)
	}()

	<-started

	begin := time.Now()

	cancel()

	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("Run did not return")
	}

	return time.Since(begin)
}

func TestRunWaitsForAnInFlightCallThatEndsSoon(t *testing.T) {
	started := make(chan struct{})
	finished := make(chan struct{})

	var once sync.Once

	slow := newFake("p2p", func(context.Context, int) (snapshot.Patch, error) {
		once.Do(func() { close(started) })
		time.Sleep(300 * time.Millisecond)
		close(finished)

		return nil, errors.NewServiceError("p2p unavailable")
	})

	s := NewSampler([]Collector{slow}, nil, time.Hour, time.Hour, "n")

	runUntilStopped(t, s, started)

	select {
	case <-finished:
	default:
		t.Fatal("Run returned before the in-flight call ended")
	}
}

func TestRunStopsWaitingForAHungCallAfterTheBound(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})

	defer close(release)

	var once sync.Once

	hung := newFake("p2p", func(context.Context, int) (snapshot.Patch, error) {
		once.Do(func() { close(started) })
		<-release

		return nil, errors.NewServiceError("p2p unavailable")
	})

	s := NewSampler([]Collector{hung}, nil, time.Hour, time.Hour, "n")

	waited := runUntilStopped(t, s, started)

	require.GreaterOrEqual(t, waited, shutdownWait)
	require.Less(t, waited, shutdownWait+2*time.Second)
}

func TestSourceFailureLoggedOncePerStateChange(t *testing.T) {
	logger := &recordingLogger{}

	flaky := newFake("flaky", func(_ context.Context, call int) (snapshot.Patch, error) {
		if call >= 3 && call <= 6 {
			return nil, errors.NewServiceError("dial postgres://probe:secretpw@db:5432/teranode refused\nretry later")
		}

		return okPatch(func(s *snapshot.Snapshot) {
			s.Cache.HitRate = snapshot.Reading{Value: 0.8, Status: snapshot.StatusOK}
		})(context.Background(), call)
	})

	s := NewSampler([]Collector{flaky}, nil, 20*time.Millisecond, time.Hour, "n", WithLogger(logger))
	startSampler(t, s)

	require.Eventually(t, func() bool { return flaky.Calls() >= 10 }, 5*time.Second, 5*time.Millisecond)

	var warns, infos []string

	for _, line := range logger.Lines() {
		require.NotContains(t, line, "\n")
		require.NotContains(t, line, "secretpw")

		switch {
		case strings.HasPrefix(line, "WARN "):
			warns = append(warns, line)
		case strings.HasPrefix(line, "INFO "):
			infos = append(infos, line)
		}
	}

	require.Len(t, warns, 1)
	require.Contains(t, warns[0], "flaky")
	require.Contains(t, warns[0], "refused")
	require.Len(t, infos, 1)
	require.Contains(t, infos[0], "flaky")
}

func TestStaleChainFieldsCarryTheSourceAge(t *testing.T) {
	clock := newFakeClock()

	chain := newFake("chain", func(_ context.Context, call int) (snapshot.Patch, error) {
		if call == 1 {
			return func(s *snapshot.Snapshot) {
				s.FSM = "RUNNING"
				s.Tip = 1000
			}, nil
		}

		return nil, errors.NewServiceError("blockchain unavailable")
	})

	kafka := newFake("kafka", func(_ context.Context, call int) (snapshot.Patch, error) {
		if call == 1 {
			return func(s *snapshot.Snapshot) {
				lag := int64(3)
				s.Flow(snapshot.FlowBlocks).Lag = &lag
			}, nil
		}

		return nil, errors.NewServiceError("broker down")
	})

	s := NewSampler([]Collector{chain}, []Collector{kafka}, 20*time.Millisecond, 20*time.Millisecond, "n", WithClock(clock.Now))
	startSampler(t, s)

	sourceMap := func(snap snapshot.Snapshot) map[string]snapshot.SourceHealth {
		out := map[string]snapshot.SourceHealth{}
		for _, src := range snap.Sources {
			out[src.Name] = src
		}

		return out
	}

	waitForUpdate(t, s, 2*time.Second, func(snap snapshot.Snapshot) bool {
		h := sourceMap(snap)
		return snap.FSM == "RUNNING" && snap.Flow(snapshot.FlowBlocks).Lag != nil && !h["chain"].OK && !h["kafka"].OK
	})

	clock.Advance(45 * time.Second)

	stale := waitForUpdate(t, s, 2*time.Second, func(snap snapshot.Snapshot) bool {
		return snap.Time.Equal(clock.Now())
	})

	health := sourceMap(stale)

	require.False(t, health["chain"].OK)
	require.InDelta(t, 45, health["chain"].AgeSec, 0.001)
	require.False(t, health["kafka"].OK)
	require.InDelta(t, 45, health["kafka"].AgeSec, 0.001)
	require.Equal(t, "RUNNING", stale.FSM)
	require.Equal(t, uint32(1000), stale.Tip)
}
