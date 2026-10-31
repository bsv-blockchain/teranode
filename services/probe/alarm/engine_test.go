package alarm

import (
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/services/probe/snapshot"
	"github.com/stretchr/testify/require"
)

const (
	tick   = 2 * time.Second
	warmup = 10 * time.Minute

	keyLatency    = "validator.latencyP99Sec"
	keyQueueFill  = "validator.queueFill"
	keyHitRate    = "cache.hitRate"
	keyFill       = "cache.fill"
	keyPrevGen    = "cache.previousGenFrac"
	keyAerospike  = "stores.aerospike.readP99Sec"
	keyTipLag     = "assembly.tipLagBlocks"
	keyHeadAge    = "assembly.queueHeadAgeSec"
	keyPersister  = "chain.persisterBehind"
	keySubtreeAge = "ingress.subtreeQueueOldestSec"
)

var e0 = time.Date(2026, 10, 28, 12, 0, 0, 0, time.UTC)

type rig struct {
	t     *testing.T
	e     *Engine
	b     *Baseline
	st    *Store
	path  string
	clock time.Time
	mu    sync.Mutex
}

func newRig(t *testing.T, floors map[string]float64, opts ...EngineOption) *rig {
	return newRigRules(t, DefaultRules, floors, opts...)
}

func newRigRules(t *testing.T, rules []Rule, floors map[string]float64, opts ...EngineOption) *rig {
	r := &rig{t: t, path: filepath.Join(t.TempDir(), "probe.json"), clock: e0}
	r.open(rules, floors, opts...)

	return r
}

func (r *rig) open(rules []Rule, floors map[string]float64, opts ...EngineOption) {
	st, err := OpenStore(r.path)
	require.NoError(r.t, err)

	r.t.Cleanup(func() { _ = st.Close() })

	r.st = st
	r.b = NewBaseline(warmup)
	r.e = NewEngine(rules, r.b, st, floors, 15*time.Minute, append([]EngineOption{WithNow(r.now)}, opts...)...)
}

func (r *rig) now() time.Time {
	r.mu.Lock()
	defer r.mu.Unlock()

	return r.clock
}

func (r *rig) setClock(at time.Time) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.clock = at
}

func blank(fsm string) *snapshot.Snapshot {
	s := snapshot.NewSnapshot("n")
	s.FSM = fsm

	s.EachReading(func(_ string, rd *snapshot.Reading) {
		rd.Status = snapshot.StatusNoData
		rd.Note = "no data"
	})

	return &s
}

func snap(fsm string, values map[string]float64) *snapshot.Snapshot {
	s := blank(fsm)

	for key, v := range values {
		rd, ok := s.Lookup(key)
		if !ok {
			panic("unknown key " + key)
		}

		*rd = snapshot.Reading{Value: v, Status: snapshot.StatusOK}
	}

	return s
}

func (r *rig) eval(at time.Time, values map[string]float64) *snapshot.Snapshot {
	return r.evalFSM(at, "RUNNING", values)
}

func (r *rig) evalFSM(at time.Time, fsm string, values map[string]float64) *snapshot.Snapshot {
	s := snap(fsm, values)
	r.e.Evaluate(at, s)

	return s
}

func (r *rig) evalSnap(at time.Time, s *snapshot.Snapshot) *snapshot.Snapshot {
	r.e.Evaluate(at, s)

	return s
}

func (r *rig) run(from, to time.Time, values map[string]float64) *snapshot.Snapshot {
	var last *snapshot.Snapshot

	for at := from; !at.After(to); at = at.Add(tick) {
		last = r.eval(at, values)
	}

	return last
}

func readyBaseline(b *Baseline, id string, v float64, end time.Time) {
	for at := end.Add(-warmup); !at.After(end); at = at.Add(tick) {
		b.Observe(id, v, at)
	}
}

func find(s *snapshot.Snapshot, id string) *snapshot.Alarm {
	for i := range s.Alarms {
		if s.Alarms[i].ID == id {
			return &s.Alarms[i]
		}
	}

	return nil
}

func reading(t *testing.T, s *snapshot.Snapshot, key string) *snapshot.Reading {
	rd, ok := s.Lookup(key)
	require.True(t, ok)

	return rd
}

func sec(n int) time.Duration {
	return time.Duration(n) * time.Second
}

func merge(ms ...map[string]float64) map[string]float64 {
	out := map[string]float64{}

	for _, m := range ms {
		for k, v := range m {
			out[k] = v
		}
	}

	return out
}

func startOf(s *snapshot.Snapshot) []string {
	var out []string

	for _, a := range s.Alarms {
		if a.StartOfChain {
			out = append(out, a.ID)
		}
	}

	return out
}

func TestAmberAfter30sOutsideTolerance(t *testing.T) {
	r := newRig(t, nil)
	readyBaseline(r.b, "validator-latency-p99", 1.0, e0)

	start := e0.Add(tick)
	bad := map[string]float64{keyLatency: 1.075}

	s := r.run(start, start.Add(sec(28)), bad)
	require.Nil(t, find(s, "validator-latency-p99"))
	require.Equal(t, snapshot.StatusOK, reading(t, s, keyLatency).Status)

	s = r.eval(start.Add(sec(29)), bad)
	require.Nil(t, find(s, "validator-latency-p99"))

	s = r.eval(start.Add(sec(30)), bad)
	a := find(s, "validator-latency-p99")
	require.NotNil(t, a)
	require.Equal(t, "warn", a.Severity)
	require.Equal(t, "validator", a.Stage)
	require.Equal(t, start.Add(sec(30)), a.RaisedAt)
	require.Equal(t, snapshot.StatusWarn, reading(t, s, keyLatency).Status)
	require.Equal(t, 1.075, reading(t, s, keyLatency).Value)

	s = r.run(start.Add(sec(32)), start.Add(sec(120)), bad)
	require.Equal(t, "warn", find(s, "validator-latency-p99").Severity)
}

func TestRedAfter60sBeyondFloor(t *testing.T) {
	r := newRig(t, map[string]float64{keyLatency: 2.0})

	bad := map[string]float64{keyLatency: 3.0}

	s := r.run(e0, e0.Add(sec(28)), bad)
	require.Nil(t, find(s, "validator-latency-p99"))

	s = r.eval(e0.Add(sec(30)), bad)
	require.Equal(t, "warn", find(s, "validator-latency-p99").Severity)

	s = r.run(e0.Add(sec(32)), e0.Add(sec(58)), bad)
	require.Equal(t, "warn", find(s, "validator-latency-p99").Severity)

	s = r.eval(e0.Add(sec(59)), bad)
	require.Equal(t, "warn", find(s, "validator-latency-p99").Severity)

	s = r.eval(e0.Add(sec(60)), bad)
	a := find(s, "validator-latency-p99")
	require.Equal(t, "crit", a.Severity)
	require.Equal(t, e0.Add(sec(30)), a.RaisedAt)
	require.Equal(t, snapshot.StatusCrit, reading(t, s, keyLatency).Status)
}

func TestClearNeedsContinuousNormal60s(t *testing.T) {
	r := newRig(t, map[string]float64{keyLatency: 2.0})

	bad := map[string]float64{keyLatency: 3.0}
	good := map[string]float64{keyLatency: 1.0}

	s := r.run(e0, e0.Add(sec(30)), bad)
	require.NotNil(t, find(s, "validator-latency-p99"))

	s = r.run(e0.Add(sec(32)), e0.Add(sec(78)), good)
	require.NotNil(t, find(s, "validator-latency-p99"))
	require.Equal(t, snapshot.StatusWarn, reading(t, s, keyLatency).Status)

	s = r.eval(e0.Add(sec(80)), bad)
	require.NotNil(t, find(s, "validator-latency-p99"))

	s = r.run(e0.Add(sec(82)), e0.Add(sec(92)), good)
	require.NotNil(t, find(s, "validator-latency-p99"))

	s = r.run(e0.Add(sec(94)), e0.Add(sec(140)), good)
	require.NotNil(t, find(s, "validator-latency-p99"))

	s = r.eval(e0.Add(sec(141)), good)
	require.NotNil(t, find(s, "validator-latency-p99"))

	s = r.eval(e0.Add(sec(142)), good)
	require.Nil(t, find(s, "validator-latency-p99"))
	require.Equal(t, snapshot.StatusOK, reading(t, s, keyLatency).Status)
}

func TestColdCacheShowsWarmingNotAlarm(t *testing.T) {
	floors := map[string]float64{keyHitRate: 0.9}

	t.Run("fill under ten percent", func(t *testing.T) {
		r := newRig(t, floors)

		s := r.run(e0, e0.Add(sec(300)), map[string]float64{keyHitRate: 0.5, keyFill: 0.05})
		hit := reading(t, s, keyHitRate)
		require.Equal(t, snapshot.StatusIdle, hit.Status)
		require.Equal(t, "Warming", hit.Note)
		require.Equal(t, 0.5, hit.Value)
		require.Nil(t, find(s, "cache-hit-rate"))
		require.Empty(t, s.Alarms)
	})

	t.Run("fill missing and no previous generation", func(t *testing.T) {
		r := newRig(t, floors)

		s := r.run(e0, e0.Add(sec(300)), map[string]float64{keyHitRate: 0.5, keyPrevGen: 0})
		require.Equal(t, "Warming", reading(t, s, keyHitRate).Note)
		require.Empty(t, s.Alarms)
	})

	t.Run("fill present wins over previous generation", func(t *testing.T) {
		r := newRig(t, floors)

		values := map[string]float64{keyHitRate: 0.5, keyFill: 0.9, keyPrevGen: 0}

		r.run(e0, e0.Add(sec(300)), values)
		s := r.eval(e0.Add(sec(302)), values)
		require.Equal(t, snapshot.StatusCrit, reading(t, s, keyHitRate).Status)
		require.NotNil(t, find(s, "cache-hit-rate"))
	})

	t.Run("first thirty samples warm", func(t *testing.T) {
		r := newRig(t, floors)

		values := map[string]float64{keyHitRate: 0.5, keyFill: 0.9}

		var s *snapshot.Snapshot

		for i := 0; i < 30; i++ {
			s = r.eval(e0.Add(time.Duration(i)*tick), values)
			require.Equal(t, "Warming", reading(t, s, keyHitRate).Note, "sample %d", i+1)
		}

		first := e0.Add(30 * tick)
		s = r.eval(first, values)
		require.NotEqual(t, "Warming", reading(t, s, keyHitRate).Note)
		require.Nil(t, find(s, "cache-hit-rate"))

		s = r.eval(first.Add(sec(29)), values)
		require.Nil(t, find(s, "cache-hit-rate"))

		s = r.eval(first.Add(sec(30)), values)
		require.Equal(t, "warn", find(s, "cache-hit-rate").Severity)
	})

	t.Run("warming clears an active alarm", func(t *testing.T) {
		r := newRig(t, floors)

		values := map[string]float64{keyHitRate: 0.5, keyFill: 0.9}

		s := r.run(e0, e0.Add(sec(120)), values)
		require.NotNil(t, find(s, "cache-hit-rate"))

		s = r.eval(e0.Add(sec(122)), map[string]float64{keyHitRate: 0.5, keyFill: 0.05})
		require.Nil(t, find(s, "cache-hit-rate"))
		require.Equal(t, "Warming", reading(t, s, keyHitRate).Note)
	})
}

func TestCalibratingUsesFloorsOnly(t *testing.T) {
	r := newRig(t, map[string]float64{keyLatency: 2.0})

	r.run(e0, e0.Add(sec(40)), map[string]float64{keyLatency: 1.0})

	start := e0.Add(sec(42))
	s := r.run(start, start.Add(sec(120)), map[string]float64{keyLatency: 1.9})

	mean, tol, ready := r.b.Normal("validator-latency-p99")
	require.False(t, ready)
	require.Greater(t, 1.9, mean+tol)

	rd := reading(t, s, keyLatency)
	require.Nil(t, find(s, "validator-latency-p99"))
	require.Equal(t, snapshot.StatusOK, rd.Status)
	require.Equal(t, "Calibrating", rd.Note)
	require.Nil(t, rd.Normal)
	require.False(t, s.Calibration.Ready)

	floorStart := start.Add(sec(122))
	s = r.run(floorStart, floorStart.Add(sec(28)), map[string]float64{keyLatency: 2.5})
	require.Nil(t, find(s, "validator-latency-p99"))

	s = r.eval(floorStart.Add(sec(30)), map[string]float64{keyLatency: 2.5})
	require.Equal(t, "warn", find(s, "validator-latency-p99").Severity)
}

func warmCache(r *rig, until time.Time, extra map[string]float64) {
	r.run(until.Add(-30*tick), until.Add(-tick), merge(map[string]float64{keyHitRate: 0.97, keyFill: 0.9}, extra))
}

func TestStartOfChainIsFirstBadStageInCauseOrder(t *testing.T) {
	t.Run("throughput", func(t *testing.T) {
		floors := map[string]float64{keyHitRate: 0.9, keyAerospike: 0.01, keyHeadAge: 5, keyLatency: 2}
		r := newRig(t, floors)

		healthy := map[string]float64{keyHitRate: 0.97, keyFill: 0.9, keyAerospike: 0.001, keyHeadAge: 1, keyLatency: 0.5}
		warmCache(r, e0, healthy)

		stage2 := merge(healthy, map[string]float64{keyAerospike: 0.05, keyHeadAge: 10})
		s := r.run(e0, e0.Add(sec(30)), stage2)
		require.NotNil(t, find(s, "assembly-queue-head-age"))
		require.Equal(t, []string{"aerospike-read-p99"}, startOf(s))

		s = r.run(e0.Add(sec(32)), e0.Add(sec(62)), merge(healthy, map[string]float64{keyLatency: 3, keyHeadAge: 10}))
		require.NotNil(t, find(s, "validator-latency-p99"))
		require.NotNil(t, find(s, "aerospike-read-p99"))
		require.Equal(t, []string{"aerospike-read-p99"}, startOf(s))

		all := merge(stage2, map[string]float64{keyHitRate: 0.5, keyLatency: 3})
		s = r.run(e0.Add(sec(64)), e0.Add(sec(94)), all)
		require.Equal(t, []string{"cache-hit-rate"}, startOf(s))
	})

	t.Run("catchup", func(t *testing.T) {
		floors := map[string]float64{keyPersister: 10, keySubtreeAge: 60, keyQueueFill: 0.8}
		r := newRig(t, floors)

		bad := map[string]float64{keySubtreeAge: 120, keyQueueFill: 0.9}
		s := r.evalFSM(e0, "CATCHINGBLOCKS", bad)
		require.Equal(t, []string{CatchupInProgressID}, startOf(s))

		require.NoError(t, r.e.Snooze(CatchupInProgressID, time.Hour))

		for at := e0.Add(tick); !at.After(e0.Add(sec(30))); at = at.Add(tick) {
			s = r.evalFSM(at, "CATCHINGBLOCKS", bad)
		}

		require.NotNil(t, find(s, "validator-queue-fill"))
		require.True(t, find(s, CatchupInProgressID).Snoozed)
		require.Equal(t, []string{"subtree-queue-age"}, startOf(s))

		worse := merge(bad, map[string]float64{keyPersister: 50})
		for at := e0.Add(sec(32)); !at.After(e0.Add(sec(62))); at = at.Add(tick) {
			s = r.evalFSM(at, "CATCHINGBLOCKS", worse)
		}

		require.Equal(t, []string{"persister-behind"}, startOf(s))
	})
}

func TestCatchupChainUsedWhenFSMCatchingBlocks(t *testing.T) {
	floors := map[string]float64{keyHitRate: 0.9, keyQueueFill: 0.8}
	r := newRig(t, floors)

	warmCache(r, e0, nil)

	bad := map[string]float64{keyHitRate: 0.5, keyFill: 0.9, keyQueueFill: 0.9}
	s := r.run(e0, e0.Add(sec(30)), bad)
	require.Nil(t, find(s, CatchupInProgressID))
	require.Equal(t, []string{"cache-hit-rate"}, startOf(s))

	at := e0.Add(sec(32))
	s = r.evalFSM(at, "CATCHINGBLOCKS", bad)

	c := find(s, CatchupInProgressID)
	require.NotNil(t, c)
	require.Equal(t, "warn", c.Severity)
	require.Equal(t, "chain", c.Stage)
	require.Equal(t, at, c.RaisedAt)
	require.NotEmpty(t, c.Title)
	require.Equal(t, []string{CatchupInProgressID}, startOf(s))

	require.NoError(t, r.e.Snooze(CatchupInProgressID, time.Hour))

	s = r.evalFSM(at.Add(tick), "CATCHINGBLOCKS", bad)
	require.True(t, find(s, CatchupInProgressID).Snoozed)
	require.NotNil(t, find(s, "cache-hit-rate"))
	require.Equal(t, []string{"validator-queue-fill"}, startOf(s))

	s = r.eval(at.Add(2*tick), bad)
	require.Nil(t, find(s, CatchupInProgressID))
	require.Equal(t, []string{"cache-hit-rate"}, startOf(s))
}

func TestCatchupNumberingForValidatorRules(t *testing.T) {
	rules := []Rule{
		DefaultRules[3],
		{
			ID: "zz-subtree", Stage: "ingress", Reading: keySubtreeAge,
			Chain: ChainCatchup, ChainOrder: 3, FloorKey: keySubtreeAge, TextID: "subtree-queue-age",
		},
	}
	require.Equal(t, "validator-queue-fill", rules[0].ID)

	floors := map[string]float64{keyQueueFill: 0.8, keySubtreeAge: 60}
	r := newRigRules(t, rules, floors)

	require.NoError(t, r.e.Snooze(CatchupInProgressID, 24*time.Hour))

	var s *snapshot.Snapshot

	for at := e0; !at.After(e0.Add(sec(30))); at = at.Add(tick) {
		s = r.evalFSM(at, "CATCHINGBLOCKS", map[string]float64{keyQueueFill: 0.9, keySubtreeAge: 1})
	}

	require.Equal(t, []string{"validator-queue-fill"}, startOf(s))

	for at := e0.Add(sec(32)); !at.After(e0.Add(sec(62))); at = at.Add(tick) {
		s = r.evalFSM(at, "CATCHINGBLOCKS", map[string]float64{keyQueueFill: 0.9, keySubtreeAge: 120})
	}

	require.NotNil(t, find(s, "zz-subtree"))
	require.Equal(t, []string{"zz-subtree"}, startOf(s))

	s = r.eval(e0.Add(sec(64)), map[string]float64{keyQueueFill: 0.9, keySubtreeAge: 120})
	require.Equal(t, []string{"validator-queue-fill"}, startOf(s))
}

func TestSnoozeSuppressesAndPersists(t *testing.T) {
	floors := map[string]float64{keyLatency: 2.0}
	r := newRig(t, floors)

	bad := map[string]float64{keyLatency: 3.0}
	s := r.run(e0, e0.Add(sec(30)), bad)
	require.Equal(t, []string{"validator-latency-p99"}, startOf(s))

	snoozeAt := e0.Add(sec(31))
	r.setClock(snoozeAt)
	require.NoError(t, r.e.Snooze("validator-latency-p99", 0))

	s = r.eval(e0.Add(sec(32)), bad)
	a := find(s, "validator-latency-p99")
	require.NotNil(t, a)
	require.True(t, a.Snoozed)
	require.False(t, a.StartOfChain)
	require.Equal(t, snapshot.StatusWarn, reading(t, s, keyLatency).Status)

	require.NoError(t, r.st.Close())
	r.open(DefaultRules, floors)

	until := snoozeAt.Add(15 * time.Minute)
	restart := until.Add(-sec(60))
	s = r.run(restart, restart.Add(sec(30)), bad)
	a = find(s, "validator-latency-p99")
	require.NotNil(t, a)
	require.True(t, a.Snoozed)
	require.Empty(t, startOf(s))

	s = r.eval(until.Add(-sec(1)), bad)
	require.True(t, find(s, "validator-latency-p99").Snoozed)

	s = r.eval(until, bad)
	a = find(s, "validator-latency-p99")
	require.False(t, a.Snoozed)
	require.True(t, a.StartOfChain)

	require.NoError(t, r.e.Snooze("validator-latency-p99", time.Hour))
	s = r.eval(until.Add(tick), bad)
	require.True(t, find(s, "validator-latency-p99").Snoozed)

	require.NoError(t, r.e.Unsnooze("validator-latency-p99"))
	s = r.eval(until.Add(2*tick), bad)
	require.False(t, find(s, "validator-latency-p99").Snoozed)
}

func TestEveryRuleHasText(t *testing.T) {
	ids := map[string]bool{}

	for _, rule := range append(append([]Rule{}, DefaultRules...), catchupRule) {
		require.False(t, ids[rule.ID], rule.ID)
		ids[rule.ID] = true

		title, body, ok := Text(rule.TextID)
		require.True(t, ok, rule.TextID)
		require.NotEmpty(t, strings.TrimSpace(title), rule.TextID)
		require.NotEmpty(t, strings.TrimSpace(body), rule.TextID)
		require.NotEmpty(t, rule.Drilldowns, rule.ID)

		for _, l := range rule.Drilldowns {
			require.True(t, strings.HasPrefix(l.Path, "#/stage/"), l.Path)
		}

		for _, sentence := range strings.Split(body, ". ") {
			require.LessOrEqual(t, len(strings.Fields(sentence)), 20, sentence)
		}
	}

	for _, stage := range stageOrder {
		require.True(t, ids[SourceUnreachableID+stage], stage)
	}

	_, body, _ := Text("cache-hit-rate")
	require.Contains(t, body, "full")
	require.Contains(t, body, "hit rate")
	require.Contains(t, body, "eviction rate")

	_, body, _ = Text("aerospike-read-p99")
	require.Contains(t, body, "enable-benchmarks-read")

	_, _, ok := Text("no-such-text")
	require.False(t, ok)

	r := newRig(t, map[string]float64{keyLatency: 2.0})
	s := r.run(e0, e0.Add(sec(30)), map[string]float64{keyLatency: 3.0})
	a := find(s, "validator-latency-p99")
	title, body, _ := Text("validator-latency-p99")
	require.Equal(t, title, a.Title)
	require.Equal(t, body, a.Body)
	require.Equal(t, "validator-latency-p99", a.TextID)
	require.Equal(t, DefaultRules[2].Drilldowns, a.Drilldowns)
}

func TestNodataDoesNotFlapAnActiveAlarm(t *testing.T) {
	r := newRig(t, map[string]float64{keyLatency: 2.0})

	bad := map[string]float64{keyLatency: 3.0}
	r.run(e0, e0.Add(sec(30)), bad)

	gap := e0.Add(sec(32))
	s := r.run(gap, gap.Add(sec(58)), nil)
	require.NotNil(t, find(s, "validator-latency-p99"))
	require.Equal(t, snapshot.StatusNoData, reading(t, s, keyLatency).Status)

	s = r.eval(gap.Add(sec(59)), nil)
	require.NotNil(t, find(s, "validator-latency-p99"))

	back := gap.Add(sec(60))
	s = r.eval(back, bad)
	a := find(s, "validator-latency-p99")
	require.NotNil(t, a)
	require.Equal(t, e0.Add(sec(30)), a.RaisedAt)
	require.Equal(t, "warn", a.Severity)
	require.Len(t, r.st.AlarmsSince(time.Time{}), 1)

	s = r.run(back.Add(tick), back.Add(sec(58)), bad)
	require.Equal(t, "warn", find(s, "validator-latency-p99").Severity)

	s = r.eval(back.Add(sec(60)), bad)
	require.Equal(t, "crit", find(s, "validator-latency-p99").Severity)
	require.Len(t, r.st.AlarmsSince(time.Time{}), 2)

	gap2 := back.Add(sec(62))
	s = r.run(gap2, gap2.Add(sec(58)), nil)
	require.NotNil(t, find(s, "validator-latency-p99"))

	s = r.eval(gap2.Add(sec(60)), nil)
	require.Nil(t, find(s, "validator-latency-p99"))
}

func TestNodataResetsEscalationAndClearTimers(t *testing.T) {
	t.Run("red timer restarts after a gap", func(t *testing.T) {
		r := newRig(t, map[string]float64{keyLatency: 2.0})

		bad := map[string]float64{keyLatency: 3.0}
		s := r.run(e0, e0.Add(sec(30)), bad)
		require.Equal(t, "warn", find(s, "validator-latency-p99").Severity)

		r.run(e0.Add(sec(32)), e0.Add(sec(50)), nil)

		resume := e0.Add(sec(52))
		s = r.run(resume, resume.Add(sec(58)), bad)
		require.Equal(t, "warn", find(s, "validator-latency-p99").Severity)

		s = r.eval(resume.Add(sec(59)), bad)
		require.Equal(t, "warn", find(s, "validator-latency-p99").Severity)

		s = r.eval(resume.Add(sec(60)), bad)
		require.Equal(t, "crit", find(s, "validator-latency-p99").Severity)
	})

	t.Run("clear timer restarts after a gap", func(t *testing.T) {
		r := newRig(t, map[string]float64{keyLatency: 2.0})

		good := map[string]float64{keyLatency: 1.0}
		r.run(e0, e0.Add(sec(30)), map[string]float64{keyLatency: 3.0})

		normal := e0.Add(sec(32))
		r.run(normal, normal.Add(sec(10)), good)

		gap := normal.Add(sec(12))
		s := r.run(gap, gap.Add(sec(54)), nil)
		require.NotNil(t, find(s, "validator-latency-p99"))

		after := gap.Add(sec(55))
		require.GreaterOrEqual(t, after.Sub(normal), sec(60))

		s = r.eval(after, good)
		require.NotNil(t, find(s, "validator-latency-p99"))

		s = r.run(after.Add(tick), after.Add(sec(58)), good)
		require.NotNil(t, find(s, "validator-latency-p99"))

		s = r.eval(after.Add(sec(60)), good)
		require.Nil(t, find(s, "validator-latency-p99"))
	})
}

func health(sources ...snapshot.SourceHealth) *snapshot.Snapshot {
	s := blank("RUNNING")
	s.Sources = sources

	return s
}

func down(name string, age float64) snapshot.SourceHealth {
	return snapshot.SourceHealth{Name: name, OK: false, AgeSec: age}
}

func up(name string) snapshot.SourceHealth {
	return snapshot.SourceHealth{Name: name, OK: true}
}

func sourceAlarms(s *snapshot.Snapshot) []string {
	var out []string

	for _, a := range s.Alarms {
		if strings.HasPrefix(a.ID, SourceUnreachableID) {
			out = append(out, a.ID)
		}
	}

	return out
}

func TestSourceUnreachableAlarmPerStage(t *testing.T) {
	r := newRig(t, nil)

	stale := func(age float64) *snapshot.Snapshot {
		s := health(down("aerospike", age), down("chain", age), down("kafka", 60), up("metrics"), up("postgres"))
		reading(t, s, "ingress.peers").AgeSec = 500
		s.Flow(snapshot.FlowTxInput).Rate.AgeSec = 500

		return s
	}

	var s *snapshot.Snapshot

	for at := e0; !at.After(e0.Add(sec(28))); at = at.Add(tick) {
		s = r.evalSnap(at, stale(61+at.Sub(e0).Seconds()))
	}

	require.Empty(t, s.Alarms)

	s = r.evalSnap(e0.Add(sec(29)), stale(90))
	require.Empty(t, s.Alarms)

	s = r.evalSnap(e0.Add(sec(30)), stale(91))
	require.Equal(t, []string{SourceUnreachableID + "chain", SourceUnreachableID + "stores"}, sourceAlarms(s))

	for _, stage := range []string{"chain", "stores"} {
		a := find(s, SourceUnreachableID+stage)
		require.Equal(t, "warn", a.Severity)
		require.Equal(t, stage, a.Stage)
		require.False(t, a.StartOfChain)
		require.NotEmpty(t, a.Title)
	}

	recovered := e0.Add(sec(32))

	for at := recovered; !at.After(recovered.Add(sec(58))); at = at.Add(tick) {
		s = r.evalSnap(at, health(up("aerospike"), up("chain")))
	}

	require.NotNil(t, find(s, SourceUnreachableID+"stores"))

	s = r.evalSnap(recovered.Add(sec(60)), health(up("aerospike"), up("chain")))
	require.Empty(t, sourceAlarms(s))
}

func TestNeverReachableSourceRaisesAlarm(t *testing.T) {
	r := newRig(t, nil)

	var s *snapshot.Snapshot

	for at := e0; !at.After(e0.Add(sec(28))); at = at.Add(tick) {
		s = r.evalSnap(at, health(down("aerospike", 61+at.Sub(e0).Seconds())))
	}

	require.Empty(t, sourceAlarms(s))

	s = r.evalSnap(e0.Add(sec(29)), health(down("aerospike", 90)))
	require.Empty(t, sourceAlarms(s))

	s = r.evalSnap(e0.Add(sec(30)), health(down("aerospike", 91)))
	require.Equal(t, []string{SourceUnreachableID + "stores"}, sourceAlarms(s))
}

func TestUnreachableP2PRaisesChainSourceAlarm(t *testing.T) {
	r := newRig(t, nil)

	var s *snapshot.Snapshot

	for at := e0; !at.After(e0.Add(sec(29))); at = at.Add(tick) {
		s = r.evalSnap(at, health(up("chain"), up("metrics"), down("p2p", 61+at.Sub(e0).Seconds())))
	}

	require.Empty(t, sourceAlarms(s))

	s = r.evalSnap(e0.Add(sec(30)), health(up("chain"), up("metrics"), down("p2p", 91)))
	require.Equal(t, []string{SourceUnreachableID + "chain"}, sourceAlarms(s))
}

func TestSourceMappedToTwoStagesAlarmsBoth(t *testing.T) {
	t.Run("custom map", func(t *testing.T) {
		r := newRig(t, nil, WithSourceStages(map[string][]string{"x": {"cache", "stores"}}))

		var s *snapshot.Snapshot

		for at := e0; !at.After(e0.Add(sec(30))); at = at.Add(tick) {
			s = r.evalSnap(at, health(down("x", 100), down("aerospike", 100)))
		}

		require.Equal(t, []string{SourceUnreachableID + "cache", SourceUnreachableID + "stores"}, sourceAlarms(s))
	})

	t.Run("default map", func(t *testing.T) {
		r := newRig(t, nil)

		var s *snapshot.Snapshot

		for at := e0; !at.After(e0.Add(sec(30))); at = at.Add(tick) {
			s = r.evalSnap(at, health(down("metrics", 100), down("kafka", 100)))
		}

		got := sourceAlarms(s)
		sort.Strings(got)
		require.Equal(t, []string{
			SourceUnreachableID + "assembly", SourceUnreachableID + "cache",
			SourceUnreachableID + "ingress", SourceUnreachableID + "validator",
		}, got)
	})
}

func TestFarOutsideToleranceGoesRed(t *testing.T) {
	t.Run("far", func(t *testing.T) {
		r := newRig(t, nil)
		readyBaseline(r.b, "validator-latency-p99", 1.0, e0)

		start := e0.Add(tick)
		far := map[string]float64{keyLatency: 5.0}

		s := r.run(start, start.Add(sec(28)), far)
		require.Nil(t, find(s, "validator-latency-p99"))

		s = r.eval(start.Add(sec(30)), far)
		require.Equal(t, "warn", find(s, "validator-latency-p99").Severity)

		s = r.run(start.Add(sec(32)), start.Add(sec(58)), far)
		require.Equal(t, "warn", find(s, "validator-latency-p99").Severity)

		s = r.eval(start.Add(sec(59)), far)
		require.Equal(t, "warn", find(s, "validator-latency-p99").Severity)

		s = r.eval(start.Add(sec(60)), far)
		require.Equal(t, "crit", find(s, "validator-latency-p99").Severity)
	})

	t.Run("near stays amber", func(t *testing.T) {
		r := newRig(t, nil)
		readyBaseline(r.b, "validator-latency-p99", 1.0, e0)

		start := e0.Add(tick)
		s := r.run(start, start.Add(sec(180)), map[string]float64{keyLatency: 1.075})
		require.Equal(t, "warn", find(s, "validator-latency-p99").Severity)
	})
}

func TestNormalIsSetWhenBaselineReady(t *testing.T) {
	r := newRig(t, nil)

	s := r.eval(e0, map[string]float64{keyLatency: 1.0})
	rd := reading(t, s, keyLatency)
	require.Nil(t, rd.Normal)
	require.Equal(t, "Calibrating", rd.Note)

	s = snap("RUNNING", map[string]float64{keyAerospike: 0.001})
	reading(t, s, keyAerospike).Note = "sampled"
	r.evalSnap(e0, s)
	require.Equal(t, "sampled", reading(t, s, keyAerospike).Note)

	readyBaseline(r.b, "validator-latency-p99", 1.0, e0.Add(warmup))

	s = snap("RUNNING", map[string]float64{keyLatency: 1.0})
	reading(t, s, keyLatency).Note = "Calibrating"
	r.evalSnap(e0.Add(warmup).Add(tick), s)

	rd = reading(t, s, keyLatency)
	require.NotNil(t, rd.Normal)
	require.InDelta(t, 1.0, *rd.Normal, 1e-9)
	require.Empty(t, rd.Note)
	require.Equal(t, snapshot.StatusOK, rd.Status)
}

func TestAlarmsNeverNilAndSorted(t *testing.T) {
	r := newRig(t, map[string]float64{keyAerospike: 0.01, keyTipLag: 2, keyLatency: 2})

	s := blank("RUNNING")
	s.Alarms = []snapshot.Alarm{{ID: "stale"}}
	r.evalSnap(e0, s)
	require.NotNil(t, s.Alarms)
	require.Empty(t, s.Alarms)

	s = blank("RUNNING")
	r.evalSnap(e0, s)
	require.NotNil(t, s.Alarms)

	crit := map[string]float64{keyAerospike: 0.05, keyTipLag: 5}
	r.run(e0, e0.Add(sec(30)), crit)

	s = r.run(e0.Add(sec(32)), e0.Add(sec(62)), merge(crit, map[string]float64{keyLatency: 3}))

	ids := make([]string, 0, len(s.Alarms))
	for _, a := range s.Alarms {
		ids = append(ids, a.ID+"/"+a.Severity)
	}

	require.Equal(t, []string{"aerospike-read-p99/crit", "assembly-tip-lag/crit", "validator-latency-p99/warn"}, ids)
}

func TestAlarmLogRecordsRaiseAndSeverityChange(t *testing.T) {
	r := newRig(t, map[string]float64{keyLatency: 2.0})

	bad := map[string]float64{keyLatency: 3.0}
	r.run(e0, e0.Add(sec(28)), bad)
	require.Empty(t, r.st.AlarmsSince(time.Time{}))

	r.eval(e0.Add(sec(30)), bad)
	log := r.st.AlarmsSince(time.Time{})
	require.Len(t, log, 1)
	require.Equal(t, "validator-latency-p99", log[0].ID)
	require.Equal(t, "warn", log[0].Severity)
	require.Equal(t, e0.Add(sec(30)), log[0].RaisedAt)
	require.NotEmpty(t, log[0].Title)

	r.run(e0.Add(sec(32)), e0.Add(sec(58)), bad)
	require.Len(t, r.st.AlarmsSince(time.Time{}), 1)

	r.run(e0.Add(sec(60)), e0.Add(sec(120)), bad)
	log = r.st.AlarmsSince(time.Time{})
	require.Len(t, log, 2)
	require.Equal(t, "crit", log[1].Severity)
	require.Equal(t, e0.Add(sec(30)), log[1].RaisedAt)

	r.run(e0.Add(sec(122)), e0.Add(sec(182)), map[string]float64{keyLatency: 1})
	s := r.eval(e0.Add(sec(184)), map[string]float64{keyLatency: 1})
	require.Empty(t, s.Alarms)
	require.Len(t, r.st.AlarmsSince(time.Time{}), 2)

	again := e0.Add(sec(186))
	r.run(again, again.Add(sec(30)), bad)
	log = r.st.AlarmsSince(time.Time{})
	require.Len(t, log, 3)
	require.Equal(t, again.Add(sec(30)), log[2].RaisedAt)
	require.Equal(t, "warn", log[2].Severity)
}

func TestAlarmLogPrunedDaily(t *testing.T) {
	r := newRig(t, nil, WithLogRetention(time.Hour))

	require.NoError(t, r.st.AppendAlarm(snapshot.Alarm{ID: "old", RaisedAt: e0.Add(-2 * time.Hour)}))
	r.eval(e0, nil)
	require.Empty(t, r.st.AlarmsSince(time.Time{}))

	require.NoError(t, r.st.AppendAlarm(snapshot.Alarm{ID: "old", RaisedAt: e0.Add(-2 * time.Hour)}))
	r.eval(e0.Add(time.Hour), nil)
	require.Len(t, r.st.AlarmsSince(time.Time{}), 1)

	r.eval(e0.Add(24*time.Hour-time.Second), nil)
	require.Len(t, r.st.AlarmsSince(time.Time{}), 1)

	r.eval(e0.Add(24*time.Hour), nil)
	require.Empty(t, r.st.AlarmsSince(time.Time{}))
}

func TestBaselineSavedAtMostOncePerMinute(t *testing.T) {
	r := newRig(t, nil)

	count := func() int {
		state, ok, err := r.st.LoadBaseline()
		require.NoError(t, err)
		require.True(t, ok)

		return state.Metrics["validator-latency-p99"].Count
	}

	_, ok, err := r.st.LoadBaseline()
	require.NoError(t, err)
	require.False(t, ok)

	values := map[string]float64{keyLatency: 1}

	r.eval(e0, values)
	require.Equal(t, 1, count())

	r.run(e0.Add(tick), e0.Add(sec(58)), values)
	r.eval(e0.Add(sec(59)), values)
	require.Equal(t, 1, count())

	r.eval(e0.Add(sec(60)), values)
	require.Equal(t, 32, count())

	r.eval(e0.Add(sec(119)), values)
	require.Equal(t, 32, count())

	require.NoError(t, r.e.FlushState())
	require.Equal(t, 33, count())

	reopened, err := OpenStore(r.path)
	require.NoError(t, err)

	state, ok, err := reopened.LoadBaseline()
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, 33, state.Metrics["validator-latency-p99"].Count)
}

func TestFloorForBadWhenLowRuleIsAMinimum(t *testing.T) {
	r := newRig(t, map[string]float64{keyHitRate: 0.9, keyLatency: 2})

	warmCache(r, e0, nil)

	s := r.run(e0, e0.Add(sec(120)), map[string]float64{keyHitRate: 0.99, keyFill: 0.9, keyLatency: 0.5})
	require.Empty(t, s.Alarms)

	s = r.run(e0.Add(sec(122)), e0.Add(sec(152)), map[string]float64{keyHitRate: 0.85, keyFill: 0.9, keyLatency: 3})
	require.Equal(t, "warn", find(s, "cache-hit-rate").Severity)
	require.Equal(t, "warn", find(s, "validator-latency-p99").Severity)
}

func TestEngineConcurrentSnoozeIsRaceFree(t *testing.T) {
	r := newRig(t, map[string]float64{keyLatency: 2.0})

	var (
		wg   sync.WaitGroup
		errs = make([]error, 4)
	)

	for g := 0; g < len(errs); g++ {
		wg.Add(1)

		go func(g int) {
			defer wg.Done()

			for i := 0; i < 50 && errs[g] == nil; i++ {
				r.setClock(e0.Add(time.Duration(i) * time.Millisecond))

				if i%2 == 0 {
					errs[g] = r.e.Snooze("validator-latency-p99", time.Minute)
				} else {
					errs[g] = r.e.Unsnooze("validator-latency-p99")
				}
			}
		}(g)
	}

	s := r.run(e0, e0.Add(sec(200)), map[string]float64{keyLatency: 3.0})

	wg.Wait()

	for _, err := range errs {
		require.NoError(t, err)
	}

	require.NotNil(t, find(s, "validator-latency-p99"))
	require.NoError(t, r.e.FlushState())
}

func TestFlushStateReportsStoreErrors(t *testing.T) {
	r := newRig(t, map[string]float64{keyLatency: 2.0})

	failures := 1
	r.st.beforeRename = func() error {
		if failures == 0 {
			return nil
		}

		failures--

		return errors.NewProcessingError("disk full")
	}

	r.eval(e0, map[string]float64{keyLatency: 1.0})
	require.Equal(t, 0, failures)

	r.run(e0.Add(tick), e0.Add(sec(32)), map[string]float64{keyLatency: 3.0})

	err := r.e.FlushState()
	require.Error(t, err)
	require.Contains(t, err.Error(), "disk full")

	require.NoError(t, r.e.FlushState())

	reopened, err := OpenStore(r.path)
	require.NoError(t, err)
	require.Len(t, reopened.AlarmsSince(time.Time{}), 1)
}

func noisyBaseline(b *Baseline, id string, mean, noise float64, end time.Time) {
	i := 0

	for at := end.Add(-warmup); !at.After(end); at = at.Add(tick) {
		v := mean + noise
		if i%2 == 1 {
			v = mean - noise
		}

		b.Observe(id, v, at)
		i++
	}
}

func TestSustainedFiveTimesFaultStaysCritAndNeverClears(t *testing.T) {
	r := newRig(t, nil)
	noisyBaseline(r.b, "validator-latency-p99", 1.0, 0.05, e0)

	start := e0.Add(tick)
	fault := map[string]float64{keyLatency: 5.0}

	for at := start; !at.After(start.Add(15 * time.Minute)); at = at.Add(tick) {
		s := r.eval(at, fault)
		elapsed := at.Sub(start)

		a := find(s, "validator-latency-p99")

		switch {
		case elapsed < sec(30):
			require.Nil(t, a, elapsed)
		case elapsed < sec(60):
			require.Equal(t, "warn", a.Severity, elapsed)
		default:
			require.NotNil(t, a, elapsed)
			require.Equal(t, "crit", a.Severity, elapsed)
			require.Equal(t, snapshot.StatusCrit, reading(t, s, keyLatency).Status)
		}
	}
}

func TestTwoTimesFaultGoesRed(t *testing.T) {
	r := newRig(t, nil)
	noisyBaseline(r.b, "validator-latency-p99", 1.0, 0.1, e0)

	mean, tol, ready := r.b.Normal("validator-latency-p99")
	require.True(t, ready)
	require.InDelta(t, 1.0, mean, 0.1)
	require.LessOrEqual(t, tol, 0.3)
	require.Less(t, mean+3*tol, 2.0)

	start := e0.Add(tick)
	fault := map[string]float64{keyLatency: 2.0}

	s := r.run(start, start.Add(sec(58)), fault)
	require.Equal(t, "warn", find(s, "validator-latency-p99").Severity)

	s = r.eval(start.Add(sec(59)), fault)
	require.Equal(t, "warn", find(s, "validator-latency-p99").Severity)

	s = r.eval(start.Add(sec(60)), fault)
	require.Equal(t, "crit", find(s, "validator-latency-p99").Severity)

	s = r.run(start.Add(sec(62)), start.Add(10*time.Minute), fault)
	require.Equal(t, "crit", find(s, "validator-latency-p99").Severity)
}

func TestBaselineNotFedWhileAlarmActive(t *testing.T) {
	r := newRig(t, map[string]float64{keyHitRate: 0.9})
	noisyBaseline(r.b, "validator-latency-p99", 1.0, 0.05, e0)

	mean, tol, ready := r.b.Normal("validator-latency-p99")
	require.True(t, ready)

	count := r.b.State().Metrics["validator-latency-p99"].Count

	start := e0.Add(tick)
	s := r.run(start, start.Add(10*time.Minute), map[string]float64{keyLatency: 5.0, keyHitRate: 0.5, keyFill: 0.05})
	require.Equal(t, "crit", find(s, "validator-latency-p99").Severity)
	require.Equal(t, "Warming", reading(t, s, keyHitRate).Note)

	gotMean, gotTol, gotReady := r.b.Normal("validator-latency-p99")
	require.True(t, gotReady)
	require.Equal(t, mean, gotMean)
	require.Equal(t, tol, gotTol)
	require.Equal(t, count, r.b.State().Metrics["validator-latency-p99"].Count)
	require.Equal(t, mean, *reading(t, s, keyLatency).Normal)

	_, learned := r.b.State().Metrics["cache-hit-rate"]
	require.False(t, learned)

	r2 := newRig(t, nil)
	noisyBaseline(r2.b, "validator-latency-p99", 1.0, 0.05, e0)
	count2 := r2.b.State().Metrics["validator-latency-p99"].Count
	mean2, _, _ := r2.b.Normal("validator-latency-p99")

	r2.run(start, start.Add(sec(20)), map[string]float64{keyLatency: 1.2})
	r2.eval(start.Add(sec(22)), map[string]float64{keyLatency: 1.0})
	require.Equal(t, count2, r2.b.State().Metrics["validator-latency-p99"].Count)

	gotMean2, _, _ := r2.b.Normal("validator-latency-p99")
	require.Equal(t, mean2, gotMean2)

	r2.eval(start.Add(sec(24)), map[string]float64{keyLatency: 1.0})
	require.Equal(t, count2+1, r2.b.State().Metrics["validator-latency-p99"].Count)
}

func TestZeroMeanCountRuleToleratesOneBlock(t *testing.T) {
	r := newRig(t, nil)
	readyBaseline(r.b, "assembly-tip-lag", 0, e0)

	mean, tol, ready := r.b.Normal("assembly-tip-lag")
	require.True(t, ready)
	require.Equal(t, 0.0, mean)
	require.Less(t, tol, 1e-6)

	start := e0.Add(tick)
	s := r.run(start, start.Add(10*time.Minute), map[string]float64{keyTipLag: 1})
	require.Nil(t, find(s, "assembly-tip-lag"))
	require.Equal(t, snapshot.StatusOK, reading(t, s, keyTipLag).Status)

	r2 := newRig(t, nil)
	readyBaseline(r2.b, "assembly-tip-lag", 0, e0)

	s = r2.run(start, start.Add(sec(28)), map[string]float64{keyTipLag: 5})
	require.Nil(t, find(s, "assembly-tip-lag"))

	s = r2.eval(start.Add(sec(30)), map[string]float64{keyTipLag: 5})
	require.Equal(t, "warn", find(s, "assembly-tip-lag").Severity)

	s = r2.run(start.Add(sec(32)), start.Add(sec(60)), map[string]float64{keyTipLag: 5})
	require.Equal(t, "crit", find(s, "assembly-tip-lag").Severity)
}

func TestMinToleranceAppliesToRedThreshold(t *testing.T) {
	r := newRig(t, nil)
	readyBaseline(r.b, "assembly-tip-lag", 0, e0)

	start := e0.Add(tick)
	s := r.run(start, start.Add(5*time.Minute), map[string]float64{keyTipLag: 3})
	require.Equal(t, "warn", find(s, "assembly-tip-lag").Severity)

	r2 := newRig(t, nil)
	readyBaseline(r2.b, "assembly-tip-lag", 0, e0)

	s = r2.run(start, start.Add(sec(58)), map[string]float64{keyTipLag: 3.5})
	require.Equal(t, "warn", find(s, "assembly-tip-lag").Severity)

	s = r2.eval(start.Add(sec(60)), map[string]float64{keyTipLag: 3.5})
	require.Equal(t, "crit", find(s, "assembly-tip-lag").Severity)

	want := map[string]float64{
		"assembly-tip-lag":        1,
		"persister-behind":        2,
		"assembly-queue-head-age": 5,
		"validator-queue-fill":    0.05,
		"subtree-queue-age":       5,
	}

	for _, rule := range DefaultRules {
		require.Equal(t, want[rule.ID], rule.MinTolerance, rule.ID)
	}
}
