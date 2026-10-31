package alarm

import (
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/services/probe/snapshot"
	"github.com/stretchr/testify/require"
)

type fakeClock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *fakeClock) now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.t
}

func (c *fakeClock) advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.t = c.t.Add(d)
}

func alarmAt(id string, at time.Time) snapshot.Alarm {
	return snapshot.Alarm{ID: id, Stage: "cache", Title: id, Severity: "amber", RaisedAt: at}
}

func openTemp(t *testing.T, opts ...StoreOption) (*Store, string) {
	t.Helper()

	path := filepath.Join(t.TempDir(), "sub", "probe.json")
	s, err := OpenStore(path, opts...)
	require.NoError(t, err)

	return s, path
}

func TestStoreSurvivesRestart(t *testing.T) {
	s, path := openTemp(t)

	b := NewBaseline(10 * time.Minute)
	feed(b, "a", t0, 100, 2*time.Second, constant(1000))

	require.NoError(t, s.SaveBaseline(b.State()))
	require.NoError(t, s.SaveSnooze("global", t0.Add(time.Hour)))
	require.NoError(t, s.AppendAlarm(alarmAt("a1", t0.Add(time.Minute))))
	require.NoError(t, s.AppendAlarm(alarmAt("a2", t0.Add(2*time.Minute))))
	require.NoError(t, s.Close())

	s2, err := OpenStore(path)
	require.NoError(t, err)

	defer s2.Close()

	st, ok, err := s2.LoadBaseline()
	require.NoError(t, err)
	require.True(t, ok)

	r := RestoreBaseline(st, 10*time.Minute)
	now := t0.Add(5 * time.Minute)
	require.Equal(t, b.Calibration(now), r.Calibration(now))
	require.InDelta(t, 0.5, r.Calibration(now).Fraction, 1e-9)

	feed(r, "a", t0.Add(200*time.Second), 400, 2*time.Second, constant(1000))

	_, _, ready := r.Normal("a")
	require.True(t, ready)

	sn := s2.Snoozes(t0)
	require.Equal(t, map[string]time.Time{"global": t0.Add(time.Hour)}, sn)

	al := s2.AlarmsSince(t0)
	require.Len(t, al, 2)
	require.Equal(t, "a1", al[0].ID)
	require.Equal(t, "a2", al[1].ID)
}

func TestCorruptStoreIsTyped(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.json")
	require.NoError(t, os.WriteFile(path, []byte("{not json"), 0o600))

	_, err := OpenStore(path)
	require.Error(t, err)
	require.True(t, errors.Is(err, ErrCorruptStore))

	dir := filepath.Join(t.TempDir(), "isdir")
	require.NoError(t, os.MkdirAll(dir, 0o700))

	_, err = OpenStore(dir)
	require.Error(t, err)
	require.False(t, errors.Is(err, ErrCorruptStore))
}

func TestMissingFileIsEmptyStore(t *testing.T) {
	s, _ := openTemp(t)

	_, ok, err := s.LoadBaseline()
	require.NoError(t, err)
	require.False(t, ok)
	require.Empty(t, s.Snoozes(t0))
	require.Empty(t, s.AlarmsSince(time.Time{}))
}

func TestStoreAtomicWrite(t *testing.T) {
	s, path := openTemp(t)

	require.NoError(t, s.AppendAlarm(alarmAt("old", t0)))
	require.NoError(t, s.Flush())

	before, err := os.ReadFile(path)
	require.NoError(t, err)

	s.beforeRename = func() error { return errors.NewStorageError("crash") }

	require.NoError(t, s.AppendAlarm(alarmAt("new", t0.Add(time.Second))))
	require.Error(t, s.Flush())

	after, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, before, after)

	s2, err := OpenStore(path)
	require.NoError(t, err)

	al := s2.AlarmsSince(time.Time{})
	require.Len(t, al, 1)
	require.Equal(t, "old", al[0].ID)
}

func TestPruneDropsOldAlarms(t *testing.T) {
	s, _ := openTemp(t)
	now := t0.Add(10 * 24 * time.Hour)

	require.NoError(t, s.AppendAlarm(alarmAt("eight", now.Add(-8*24*time.Hour))))
	require.NoError(t, s.AppendAlarm(alarmAt("six", now.Add(-6*24*time.Hour))))
	require.NoError(t, s.Prune(now, 7*24*time.Hour))

	al := s.AlarmsSince(time.Time{})
	require.Len(t, al, 1)
	require.Equal(t, "six", al[0].ID)
}

func TestAlarmsSinceOrderedAndFiltered(t *testing.T) {
	s, _ := openTemp(t)

	require.NoError(t, s.AppendAlarm(alarmAt("c", t0.Add(3*time.Minute))))
	require.NoError(t, s.AppendAlarm(alarmAt("a", t0.Add(time.Minute))))
	require.NoError(t, s.AppendAlarm(alarmAt("b", t0.Add(2*time.Minute))))

	al := s.AlarmsSince(t0.Add(2 * time.Minute))
	require.Len(t, al, 2)
	require.Equal(t, "b", al[0].ID)
	require.Equal(t, "c", al[1].ID)
}

func TestSnoozeExpires(t *testing.T) {
	s, path := openTemp(t)

	require.NoError(t, s.SaveSnooze("global", t0.Add(time.Hour)))
	require.NoError(t, s.SaveSnooze("other", t0.Add(3*time.Hour)))
	require.Len(t, s.Snoozes(t0), 2)

	sn := s.Snoozes(t0.Add(2 * time.Hour))
	require.Equal(t, map[string]time.Time{"other": t0.Add(3 * time.Hour)}, sn)
	require.Len(t, s.Snoozes(t0), 1)

	require.NoError(t, s.ClearSnooze("other"))
	require.Empty(t, s.Snoozes(t0))

	s2, err := OpenStore(path)
	require.NoError(t, err)
	require.Empty(t, s2.Snoozes(t0))
}

func TestCorruptFileReturnsError(t *testing.T) {
	path := filepath.Join(t.TempDir(), "probe.json")
	require.NoError(t, os.WriteFile(path, []byte("{not json"), 0o600))

	_, err := OpenStore(path)
	require.Error(t, err)
	require.Contains(t, err.Error(), path)

	got, rerr := os.ReadFile(path)
	require.NoError(t, rerr)
	require.Equal(t, "{not json", string(got))
}

func TestEmptyFileIsEmptyStore(t *testing.T) {
	path := filepath.Join(t.TempDir(), "probe.json")
	require.NoError(t, os.WriteFile(path, nil, 0o600))

	s, err := OpenStore(path)
	require.NoError(t, err)

	_, ok, err := s.LoadBaseline()
	require.NoError(t, err)
	require.False(t, ok)
	require.Empty(t, s.AlarmsSince(time.Time{}))
}

func TestFileModeIs0600(t *testing.T) {
	s, path := openTemp(t)

	require.NoError(t, s.AppendAlarm(alarmAt("a", t0)))
	require.NoError(t, s.Flush())

	fi, err := os.Stat(path)
	require.NoError(t, err)
	require.Equal(t, os.FileMode(0o600), fi.Mode().Perm())

	di, err := os.Stat(filepath.Dir(path))
	require.NoError(t, err)
	require.Equal(t, os.FileMode(0o700), di.Mode().Perm())
}

func TestWritesAreCoalesced(t *testing.T) {
	clk := &fakeClock{t: t0}
	s, path := openTemp(t, WithClock(clk.now), withTimer((&fakeTimers{}).after))

	count := func() int {
		r, err := OpenStore(path)
		require.NoError(t, err)

		return len(r.AlarmsSince(time.Time{}))
	}

	require.NoError(t, s.AppendAlarm(alarmAt("1", t0)))
	require.Equal(t, 1, count())

	clk.advance(400 * time.Millisecond)
	require.NoError(t, s.AppendAlarm(alarmAt("2", t0.Add(time.Second))))
	require.Equal(t, 1, count())

	clk.advance(400 * time.Millisecond)
	require.NoError(t, s.SaveBaseline(BaselineState{Metrics: map[string]MetricState{"m": {Count: 1}}}))
	require.Equal(t, 1, count())

	clk.advance(300 * time.Millisecond)
	require.NoError(t, s.AppendAlarm(alarmAt("3", t0.Add(2*time.Second))))
	require.Equal(t, 3, count())

	clk.advance(10 * time.Millisecond)
	require.NoError(t, s.AppendAlarm(alarmAt("4", t0.Add(3*time.Second))))
	require.Equal(t, 3, count())
	require.NoError(t, s.Flush())
	require.Equal(t, 4, count())

	clk.advance(10 * time.Millisecond)
	require.NoError(t, s.AppendAlarm(alarmAt("5", t0.Add(4*time.Second))))
	require.Equal(t, 4, count())
	require.NoError(t, s.Close())
	require.Equal(t, 5, count())
}

func TestSnoozeChangePersistsImmediately(t *testing.T) {
	clk := &fakeClock{t: t0}
	s, path := openTemp(t, WithClock(clk.now))

	require.NoError(t, s.AppendAlarm(alarmAt("1", t0)))
	clk.advance(time.Millisecond)
	require.NoError(t, s.AppendAlarm(alarmAt("2", t0)))
	require.NoError(t, s.SaveSnooze("global", t0.Add(time.Hour)))

	r, err := OpenStore(path)
	require.NoError(t, err)
	require.Len(t, r.AlarmsSince(time.Time{}), 2)
	require.Len(t, r.Snoozes(t0), 1)
}

func TestStoreConcurrentUseIsRaceFree(t *testing.T) {
	s, _ := openTemp(t)

	var wg sync.WaitGroup

	for g := 0; g < 4; g++ {
		wg.Add(1)

		go func() {
			defer wg.Done()

			for i := 0; i < 50; i++ {
				at := t0.Add(time.Duration(i) * time.Second)
				_ = s.AppendAlarm(alarmAt("x", at))
				_ = s.SaveBaseline(BaselineState{})
				_ = s.SaveSnooze("g", at.Add(time.Hour))
				s.Snoozes(at)
				s.AlarmsSince(t0)
				_, _, _ = s.LoadBaseline()
				_ = s.Prune(at, time.Hour)
			}
		}()
	}

	wg.Wait()
	require.NoError(t, s.Close())
}

type fakeTimers struct {
	mu      sync.Mutex
	delays  []time.Duration
	funcs   []func()
	stopped int
}

func (f *fakeTimers) after(d time.Duration, fn func()) func() bool {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.delays = append(f.delays, d)
	f.funcs = append(f.funcs, fn)

	return func() bool {
		f.mu.Lock()
		defer f.mu.Unlock()

		f.stopped++

		return true
	}
}

func (f *fakeTimers) armed() int {
	f.mu.Lock()
	defer f.mu.Unlock()

	return len(f.funcs)
}

func (f *fakeTimers) fire(i int) {
	f.mu.Lock()
	fn := f.funcs[i]
	f.mu.Unlock()

	fn()
}

func diskCount(t *testing.T, path string) int {
	t.Helper()

	r, err := OpenStore(path)
	require.NoError(t, err)

	return len(r.AlarmsSince(time.Time{}))
}

func TestTrailingFlushPersistsWithinOneSecond(t *testing.T) {
	clk := &fakeClock{t: t0}
	ft := &fakeTimers{}
	s, path := openTemp(t, WithClock(clk.now), withTimer(ft.after))

	require.NoError(t, s.AppendAlarm(alarmAt("1", t0)))
	require.Equal(t, 1, diskCount(t, path))
	require.Equal(t, 0, ft.armed())

	clk.advance(300 * time.Millisecond)
	require.NoError(t, s.AppendAlarm(alarmAt("2", t0.Add(time.Second))))
	require.Equal(t, 1, ft.armed())
	require.Equal(t, 700*time.Millisecond, ft.delays[0])

	clk.advance(100 * time.Millisecond)
	require.NoError(t, s.AppendAlarm(alarmAt("3", t0.Add(2*time.Second))))
	require.Equal(t, 1, ft.armed())
	require.Equal(t, 1, diskCount(t, path))

	clk.advance(600 * time.Millisecond)
	ft.fire(0)
	require.Equal(t, 3, diskCount(t, path))

	clk.advance(10 * time.Millisecond)
	require.NoError(t, s.AppendAlarm(alarmAt("4", t0.Add(3*time.Second))))
	require.Equal(t, 2, ft.armed())
}

func TestCloseStopsTimerAndFlushes(t *testing.T) {
	clk := &fakeClock{t: t0}
	ft := &fakeTimers{}
	s, path := openTemp(t, WithClock(clk.now), withTimer(ft.after))

	require.NoError(t, s.AppendAlarm(alarmAt("1", t0)))
	clk.advance(100 * time.Millisecond)
	require.NoError(t, s.AppendAlarm(alarmAt("2", t0.Add(time.Second))))
	require.Equal(t, 1, ft.armed())
	require.Equal(t, 1, diskCount(t, path))

	require.NoError(t, s.Close())
	require.Equal(t, 1, ft.stopped)
	require.Equal(t, 2, diskCount(t, path))

	ft.fire(0)
	require.Equal(t, 2, diskCount(t, path))
}

func TestFailedTrailingFlushStaysDirtyAndIsReported(t *testing.T) {
	clk := &fakeClock{t: t0}
	ft := &fakeTimers{}
	s, path := openTemp(t, WithClock(clk.now), withTimer(ft.after))

	require.NoError(t, s.AppendAlarm(alarmAt("1", t0)))
	clk.advance(100 * time.Millisecond)
	require.NoError(t, s.AppendAlarm(alarmAt("2", t0.Add(time.Second))))

	s.beforeRename = func() error { return errors.NewStorageError("disk full") }

	clk.advance(900 * time.Millisecond)
	ft.fire(0)
	require.Equal(t, 1, diskCount(t, path))
	require.True(t, s.dirty)

	s.beforeRename = nil

	err := s.Flush()
	require.Error(t, err)
	require.Contains(t, err.Error(), "disk full")
	require.Equal(t, 2, diskCount(t, path))

	require.NoError(t, s.Flush())
	require.NoError(t, s.Close())
}
