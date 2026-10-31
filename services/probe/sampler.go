package probe

import (
	"context"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"time"

	"github.com/bsv-blockchain/teranode/pkg/urlutil"
	"github.com/bsv-blockchain/teranode/services/probe/guard"
	"github.com/bsv-blockchain/teranode/services/probe/snapshot"
	"github.com/bsv-blockchain/teranode/ulogger"
)

const (
	historyWindow    = 10 * time.Minute
	maxCallTimeout   = 10 * time.Second
	minTriggerGap    = 200 * time.Millisecond
	shutdownWait     = time.Second
	defaultFastEvery = 2 * time.Second
	defaultSlowEvery = 15 * time.Second
)

type Collector interface {
	Name() string
	Collect(ctx context.Context) (snapshot.Patch, error)
}

type Option func(*Sampler)

func WithLogger(logger ulogger.Logger) Option {
	return func(s *Sampler) {
		if logger != nil {
			s.logger = logger
		}
	}
}

func WithClock(now func() time.Time) Option {
	return func(s *Sampler) {
		if now != nil {
			s.now = now
		}
	}
}

func withRecorder(rec guard.Recorder) Option {
	return func(s *Sampler) {
		if rec != nil {
			s.recorder = rec
		}
	}
}

type entry struct {
	collector Collector
	name      string
	done      chan struct{}
	good      snapshot.Patch
	goodAt    time.Time
	touched   []string
	stale     bool
	failing   bool
}

type outcome struct {
	entry    *entry
	patch    snapshot.Patch
	err      error
	panicked bool
}

type Sampler struct {
	fast      []*entry
	slow      []*entry
	fastEvery time.Duration
	slowEvery time.Duration
	node      string
	logger    ulogger.Logger
	recorder  guard.Recorder
	now       func() time.Time

	kick     chan struct{}
	updates  chan snapshot.Snapshot
	stopped  chan struct{}
	stopOnce sync.Once

	mu         sync.Mutex
	latest     snapshot.Snapshot
	ring       []snapshot.Snapshot
	ringNext   int
	ringCap    int
	slotStart  time.Time
	fastPanics int
	slowPanics int
	started    time.Time
}

func NewSampler(fast, slow []Collector, fastEvery, slowEvery time.Duration, node string, opts ...Option) *Sampler {
	if fastEvery <= 0 {
		fastEvery = defaultFastEvery
	}

	if slowEvery <= 0 {
		slowEvery = defaultSlowEvery
	}

	s := &Sampler{
		fast:      newEntries(fast),
		slow:      newEntries(slow),
		fastEvery: fastEvery,
		slowEvery: slowEvery,
		node:      node,
		now:       time.Now,
		kick:      make(chan struct{}, 1),
		updates:   make(chan snapshot.Snapshot, 1),
		stopped:   make(chan struct{}),
		ringCap:   int(historyWindow/fastEvery) + 1,
	}

	for _, opt := range opts {
		opt(s)
	}

	if s.logger == nil {
		s.logger = ulogger.New("probe")
	}

	if s.recorder == nil {
		logger := s.logger
		s.recorder = guard.RecorderFunc(func(component string, value any) {
			logger.Errorf("[Probe] %s panicked: %s", component, oneLine(value))
		})
	}

	s.started = s.now()
	s.latest = snapshot.NewSnapshot(node)
	normalise(&s.latest)

	return s
}

func newEntries(collectors []Collector) []*entry {
	entries := make([]*entry, 0, len(collectors))

	for _, c := range collectors {
		entries = append(entries, &entry{collector: c, name: c.Name()})
	}

	return entries
}

func (s *Sampler) Updates() <-chan snapshot.Snapshot {
	return s.updates
}

func (s *Sampler) Latest() snapshot.Snapshot {
	s.mu.Lock()
	defer s.mu.Unlock()

	return cloneSnapshot(s.latest)
}

func (s *Sampler) ConsecutivePanics() int {
	s.mu.Lock()
	defer s.mu.Unlock()

	return max(s.fastPanics, s.slowPanics)
}

func (s *Sampler) History(d time.Duration) []snapshot.Snapshot {
	if d <= 0 {
		return []snapshot.Snapshot{}
	}

	d = min(d, historyWindow)

	s.mu.Lock()
	defer s.mu.Unlock()

	cutoff := s.now().Add(-d)
	out := make([]snapshot.Snapshot, 0, len(s.ring))

	for i := range s.ring {
		snap := s.ring[(s.ringNext+i)%len(s.ring)]
		if snap.Time.Before(cutoff) {
			continue
		}

		out = append(out, cloneSnapshot(snap))
	}

	return out
}

func (s *Sampler) historyLen() int {
	s.mu.Lock()
	defer s.mu.Unlock()

	return len(s.ring)
}

func (s *Sampler) Trigger(ch <-chan struct{}) {
	go func() {
		defer guard.Recover(s.recorder, "sampler trigger")

		for {
			select {
			case _, ok := <-ch:
				if !ok {
					return
				}

				select {
				case s.kick <- struct{}{}:
				default:
				}
			case <-s.stopped:
				return
			}
		}
	}()
}

func (s *Sampler) Run(ctx context.Context) {
	defer s.stopOnce.Do(func() { close(s.stopped) })

	s.mu.Lock()
	s.started = s.now()
	s.mu.Unlock()

	var loops sync.WaitGroup

	loops.Add(1)

	go func() {
		defer guard.Recover(s.recorder, "sampler slow loop")
		defer loops.Done()

		s.slowLoop(ctx)
	}()

	s.fastLoop(ctx)
	loops.Wait()
	s.awaitInFlight(shutdownWait)
}

func (s *Sampler) fastLoop(ctx context.Context) {
	last := time.Now()
	s.fastTick(ctx)

	ticker := time.NewTicker(s.fastEvery)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		case <-s.kick:
			if !s.waitTriggerGap(ctx, last) {
				return
			}

			select {
			case <-s.kick:
			default:
			}

			ticker.Reset(s.fastEvery)
		}

		last = time.Now()
		s.fastTick(ctx)
	}
}

func (s *Sampler) waitTriggerGap(ctx context.Context, last time.Time) bool {
	wait := minTriggerGap - time.Since(last)
	if wait <= 0 {
		return true
	}

	timer := time.NewTimer(wait)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

func (s *Sampler) slowLoop(ctx context.Context) {
	if len(s.slow) == 0 {
		return
	}

	s.slowTick(ctx)

	ticker := time.NewTicker(s.slowEvery)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.slowTick(ctx)
		}
	}
}

func (s *Sampler) fastTick(ctx context.Context) {
	panicked := s.runGroup(ctx, s.fast, callTimeout(s.fastEvery))

	s.mu.Lock()

	snap, buildPanicked := s.build()
	s.fastPanics = nextStreak(s.fastPanics, panicked || buildPanicked)
	s.latest = snap
	s.record(cloneSnapshot(snap))
	out := cloneSnapshot(snap)

	s.mu.Unlock()

	s.offer(out)
}

func (s *Sampler) slowTick(ctx context.Context) {
	panicked := s.runGroup(ctx, s.slow, callTimeout(s.slowEvery))

	s.mu.Lock()
	s.slowPanics = nextStreak(s.slowPanics, panicked)
	s.mu.Unlock()
}

func nextStreak(streak int, panicked bool) int {
	if panicked {
		return streak + 1
	}

	return 0
}

func callTimeout(every time.Duration) time.Duration {
	return min(every, maxCallTimeout)
}

func (s *Sampler) runGroup(ctx context.Context, entries []*entry, timeout time.Duration) bool {
	if len(entries) == 0 {
		return false
	}

	results := make(chan outcome, len(entries))
	launched := make(map[*entry]bool, len(entries))

	s.mu.Lock()

	for _, e := range entries {
		if e.done != nil {
			e.stale = true
			continue
		}

		s.launch(ctx, e, timeout, results)
		launched[e] = true
	}

	s.mu.Unlock()

	if len(launched) == 0 {
		return false
	}

	timer := time.NewTimer(timeout)
	defer timer.Stop()

	panicked := false

	for len(launched) > 0 {
		select {
		case res := <-results:
			delete(launched, res.entry)

			if s.accept(res) {
				panicked = true
			}
		case <-timer.C:
			s.markStale(launched)

			return panicked
		case <-ctx.Done():
			s.markStale(launched)

			return panicked
		}
	}

	return panicked
}

func (s *Sampler) markStale(pending map[*entry]bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	for e := range pending {
		e.stale = true
	}
}

func (s *Sampler) launch(ctx context.Context, e *entry, timeout time.Duration, results chan<- outcome) {
	done := make(chan struct{})
	e.done = done

	callCtx, cancel := context.WithTimeout(ctx, timeout)

	go func() {
		defer guard.Recover(s.recorder, "sampler collector call")
		defer cancel()

		res := s.call(callCtx, e)

		s.mu.Lock()
		if e.done == done {
			e.done = nil
		}
		s.mu.Unlock()

		close(done)

		results <- res
	}()
}

func (s *Sampler) call(ctx context.Context, e *entry) (res outcome) {
	defer func() {
		if r := recover(); r != nil {
			s.logger.Errorf("[Probe] collector %s panicked: %s", e.name, oneLine(r))

			res = outcome{entry: e, panicked: true}
		}
	}()

	patch, err := e.collector.Collect(ctx)

	return outcome{entry: e, patch: patch, err: err}
}

func (s *Sampler) accept(res outcome) bool {
	e := res.entry

	if res.panicked || res.err != nil || res.patch == nil {
		s.mu.Lock()
		e.stale = true
		first := !e.failing && !res.panicked
		e.failing = e.failing || !res.panicked
		s.mu.Unlock()

		if first {
			s.logger.Warnf("[Probe] source %s has no data: %s", e.name, failureText(res.err))
		}

		return res.panicked
	}

	touched, ok := s.touchedReadings(e.name, res.patch)

	s.mu.Lock()
	defer s.mu.Unlock()

	if !ok {
		e.stale = true

		return true
	}

	if e.failing {
		s.logger.Infof("[Probe] source %s is back", e.name)
	}

	e.good = res.patch
	e.goodAt = s.now()
	e.touched = touched
	e.stale = false
	e.failing = false

	return false
}

func (s *Sampler) touchedReadings(name string, patch snapshot.Patch) ([]string, bool) {
	scratch := snapshot.NewSnapshot("")
	if !s.apply(name, patch, &scratch) {
		return nil, false
	}

	blank := snapshot.NewSnapshot("")
	before := make(map[string]snapshot.Reading)

	blank.EachReading(func(key string, r *snapshot.Reading) {
		before[key] = *r
	})

	var touched []string

	scratch.EachReading(func(key string, r *snapshot.Reading) {
		if prev, found := before[key]; !found || !reflect.DeepEqual(prev, *r) {
			touched = append(touched, key)
		}
	})

	return touched, true
}

func (s *Sampler) apply(name string, patch snapshot.Patch, snap *snapshot.Snapshot) (ok bool) {
	defer func() {
		if r := recover(); r != nil {
			s.logger.Errorf("[Probe] patch from collector %s panicked: %s", name, oneLine(r))

			ok = false
		}
	}()

	patch(snap)

	return true
}

func (s *Sampler) build() (snapshot.Snapshot, bool) {
	now := s.now()
	snap := snapshot.NewSnapshot(s.node)
	panicked := false

	for _, group := range [][]*entry{s.fast, s.slow} {
		for _, e := range group {
			if e.good == nil {
				continue
			}

			if !s.apply(e.name, e.good, &snap) {
				e.good = nil
				e.touched = nil
				panicked = true

				continue
			}

			if !e.stale {
				continue
			}

			age := now.Sub(e.goodAt).Seconds()

			for _, key := range e.touched {
				if r, found := snap.Lookup(key); found {
					r.Status = snapshot.StatusNoData
					r.AgeSec = age
					r.Note = ""
				}
			}
		}
	}

	snap.Time = now
	snap.Node = s.node
	snap.Sources = s.sourceHealth(now)
	normalise(&snap)

	return snap, panicked
}

func (s *Sampler) sourceHealth(now time.Time) []snapshot.SourceHealth {
	out := make([]snapshot.SourceHealth, 0, len(s.fast)+len(s.slow))

	for _, group := range [][]*entry{s.fast, s.slow} {
		for _, e := range group {
			h := snapshot.SourceHealth{Name: e.name, OK: e.good != nil && !e.stale}

			if !h.OK {
				since := s.started
				if !e.goodAt.IsZero() {
					since = e.goodAt
				}

				h.AgeSec = max(now.Sub(since).Seconds(), 0)
			}

			out = append(out, h)
		}
	}

	return out
}

func (s *Sampler) record(snap snapshot.Snapshot) {
	if len(s.ring) > 0 && snap.Time.Sub(s.slotStart) < s.fastEvery {
		s.ring[(s.ringNext-1+len(s.ring))%len(s.ring)] = snap

		return
	}

	if len(s.ring) == 0 {
		s.slotStart = snap.Time.Add(-s.fastEvery / 2)
	} else {
		s.slotStart = s.slotStart.Add(snap.Time.Sub(s.slotStart) / s.fastEvery * s.fastEvery)
	}

	if len(s.ring) < s.ringCap {
		s.ring = append(s.ring, snap)

		return
	}

	s.ring[s.ringNext] = snap
	s.ringNext = (s.ringNext + 1) % s.ringCap
}

func (s *Sampler) offer(snap snapshot.Snapshot) {
	select {
	case s.updates <- snap:
		return
	default:
	}

	select {
	case <-s.updates:
	default:
	}

	select {
	case s.updates <- snap:
	default:
	}
}

func (s *Sampler) awaitInFlight(limit time.Duration) {
	s.mu.Lock()

	var pending []chan struct{}

	for _, group := range [][]*entry{s.fast, s.slow} {
		for _, e := range group {
			if e.done != nil {
				pending = append(pending, e.done)
			}
		}
	}

	s.mu.Unlock()

	timer := time.NewTimer(limit)
	defer timer.Stop()

	for _, done := range pending {
		select {
		case <-done:
		case <-timer.C:
			return
		}
	}
}

func normalise(snap *snapshot.Snapshot) {
	if snap.Alarms == nil {
		snap.Alarms = []snapshot.Alarm{}
	}

	if snap.Flows == nil {
		snap.Flows = []snapshot.Flow{}
	}

	if snap.Chain.BlocksWaiting == nil {
		snap.Chain.BlocksWaiting = []uint32{}
	}

	if snap.Sources == nil {
		snap.Sources = []snapshot.SourceHealth{}
	}

	for i := range snap.Alarms {
		if snap.Alarms[i].Drilldowns == nil {
			snap.Alarms[i].Drilldowns = []snapshot.Link{}
		}
	}

	snap.EachReading(func(_ string, r *snapshot.Reading) {
		if r.Status == "" {
			r.Status = snapshot.StatusNoData
		}
	})
}

func cloneSnapshot(src snapshot.Snapshot) snapshot.Snapshot {
	dst := src

	dst.Flows = append(make([]snapshot.Flow, 0, len(src.Flows)), src.Flows...)

	for i := range dst.Flows {
		if lag := dst.Flows[i].Lag; lag != nil {
			v := *lag
			dst.Flows[i].Lag = &v
		}
	}

	dst.Alarms = append(make([]snapshot.Alarm, 0, len(src.Alarms)), src.Alarms...)

	for i := range dst.Alarms {
		dst.Alarms[i].Drilldowns = append(make([]snapshot.Link, 0, len(src.Alarms[i].Drilldowns)), src.Alarms[i].Drilldowns...)
	}

	dst.Chain.BlocksWaiting = append(make([]uint32, 0, len(src.Chain.BlocksWaiting)), src.Chain.BlocksWaiting...)
	dst.Sources = append(make([]snapshot.SourceHealth, 0, len(src.Sources)), src.Sources...)

	if src.DrainSeconds != nil {
		v := *src.DrainSeconds
		dst.DrainSeconds = &v
	}

	dst.EachReading(func(_ string, r *snapshot.Reading) {
		if r.Normal != nil {
			v := *r.Normal
			r.Normal = &v
		}
	})

	return dst
}

func failureText(err error) string {
	if err == nil {
		return "no result"
	}

	return urlutil.RedactText(oneLine(err))
}

func oneLine(v any) string {
	return strings.Join(strings.Fields(fmt.Sprint(v)), " ")
}
