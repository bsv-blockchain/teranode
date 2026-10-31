package alarm

import (
	"math"
	"sort"
	"sync"
	"time"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/services/probe/snapshot"
)

const (
	amberAfter          = 30 * time.Second
	redAfter            = 60 * time.Second
	clearAfter          = 60 * time.Second
	nodataGrace         = 60 * time.Second
	staleAgeSec         = 60.0
	baselineSaveEvery   = time.Minute
	pruneEvery          = 24 * time.Hour
	defaultLogRetention = 168 * time.Hour
	farTolerances       = 3.0
	warmingFill         = 0.10
	warmingSamples      = 30

	fsmCatchingBlocks = "CATCHINGBLOCKS"
	keyCacheHitRate   = "cache.hitRate"
	keyCacheFill      = "cache.fill"
	keyCachePrevGen   = "cache.previousGenFrac"

	noteWarming     = "Warming"
	noteCalibrating = "Calibrating"

	severityWarn = "warn"
	severityCrit = "crit"
)

type EngineOption func(*Engine)

func WithLogRetention(d time.Duration) EngineOption {
	return func(e *Engine) { e.retention = d }
}

func WithNow(now func() time.Time) EngineOption {
	return func(e *Engine) { e.now = now }
}

var SourceStages = map[string][]string{
	"metrics":   {"ingress", "validator", "cache", "assembly"},
	"chain":     {"chain"},
	"p2p":       {"chain"},
	"kafka":     {"ingress"},
	"aerospike": {"stores"},
	"postgres":  {"stores"},
}

func WithSourceStages(m map[string][]string) EngineOption {
	return func(e *Engine) { e.sourceStages = copyStages(m) }
}

func copyStages(m map[string][]string) map[string][]string {
	out := make(map[string][]string, len(m))

	for name, stages := range m {
		out[name] = append([]string(nil), stages...)
	}

	return out
}

type ruleState struct {
	active      bool
	severity    string
	raisedAt    time.Time
	warnSince   time.Time
	critSince   time.Time
	normalSince time.Time
	nodataSince time.Time
}

type Engine struct {
	mu            sync.Mutex
	rules         []Rule
	all           []Rule
	baseline      *Baseline
	store         *Store
	floors        map[string]float64
	snoozeDefault time.Duration
	retention     time.Duration
	now           func() time.Time
	states        map[string]*ruleState
	samples       map[string]int
	sourceStages  map[string][]string
	lastSave      time.Time
	lastPrune     time.Time
	err           error
}

func NewEngine(rules []Rule, b *Baseline, st *Store, floors map[string]float64, snoozeDefault time.Duration, opts ...EngineOption) *Engine {
	e := &Engine{
		rules:         append([]Rule(nil), rules...),
		baseline:      b,
		store:         st,
		floors:        make(map[string]float64, len(floors)),
		snoozeDefault: snoozeDefault,
		retention:     defaultLogRetention,
		now:           time.Now,
		states:        map[string]*ruleState{},
		samples:       map[string]int{},
		sourceStages:  copyStages(SourceStages),
	}

	for k, v := range floors {
		e.floors[k] = v
	}

	e.all = append(append([]Rule(nil), e.rules...), catchupRule)

	for _, o := range opts {
		o(e)
	}

	return e
}

func (e *Engine) Snooze(id string, d time.Duration) error {
	if d <= 0 {
		d = e.snoozeDefault
	}

	return e.store.SaveSnooze(id, e.now().Add(d))
}

func (e *Engine) Unsnooze(id string) error {
	return e.store.ClearSnooze(id)
}

func (e *Engine) FlushState() error {
	e.mu.Lock()
	defer e.mu.Unlock()

	pending := e.err
	e.err = nil

	var errs []error

	for _, err := range []error{pending, e.store.SaveBaseline(e.baseline.State()), e.store.Flush()} {
		if err != nil {
			errs = append(errs, err)
		}
	}

	if len(errs) == 0 {
		return nil
	}

	return errors.Join(errs...)
}

func (e *Engine) Evaluate(now time.Time, s *snapshot.Snapshot) {
	e.mu.Lock()
	defer e.mu.Unlock()

	down := e.unreachableStages(s)

	for _, r := range e.rules {
		if r.Reading == "" {
			e.advance(now, r, e.state(r.ID), down[r.Stage], false)
			continue
		}

		e.judgeReading(now, r, s)
	}

	e.judgeCatchup(now, s.FSM == fsmCatchingBlocks)

	chain := ChainThroughput
	if s.FSM == fsmCatchingBlocks {
		chain = ChainCatchup
	}

	s.Alarms = e.alarms(e.store.Snoozes(now), chain)
	s.Calibration = e.baseline.Calibration(now)

	e.maintain(now)
}

func (e *Engine) state(id string) *ruleState {
	st, ok := e.states[id]
	if !ok {
		st = &ruleState{}
		e.states[id] = st
	}

	return st
}

func available(r *snapshot.Reading) bool {
	return r.Status != snapshot.StatusNoData && r.Status != ""
}

func lookupAvailable(s *snapshot.Snapshot, key string) (*snapshot.Reading, bool) {
	r, ok := s.Lookup(key)
	if !ok || !available(r) {
		return nil, false
	}

	return r, true
}

func (e *Engine) judgeReading(now time.Time, r Rule, s *snapshot.Snapshot) {
	st := e.state(r.ID)

	rd, ok := lookupAvailable(s, r.Reading)
	if !ok {
		e.noData(now, st)
		return
	}

	st.nodataSince = time.Time{}

	prior := e.samples[r.ID]
	e.samples[r.ID]++

	if r.Reading == keyCacheHitRate && cacheWarming(s, prior) {
		rd.Status = snapshot.StatusIdle
		rd.Note = noteWarming
		*st = ruleState{}

		return
	}

	mean, tol, ready := e.baseline.Normal(r.ID)
	warn, crit := e.breach(r, rd.Value, mean, tol, ready)

	if !warn && !st.active && st.warnSince.IsZero() && st.critSince.IsZero() {
		e.baseline.Observe(r.ID, rd.Value, now)
		mean, _, ready = e.baseline.Normal(r.ID)
	}

	if ready {
		m := mean
		rd.Normal = &m

		if rd.Note == noteCalibrating {
			rd.Note = ""
		}
	} else {
		rd.Normal = nil

		if rd.Note == "" {
			rd.Note = noteCalibrating
		}
	}

	e.advance(now, r, st, warn, crit)

	if st.active {
		rd.Status = snapshot.Status(st.severity)
	}
}

func cacheWarming(s *snapshot.Snapshot, prior int) bool {
	if prior < warmingSamples {
		return true
	}

	if fill, ok := lookupAvailable(s, keyCacheFill); ok {
		return fill.Value < warmingFill
	}

	prev, ok := lookupAvailable(s, keyCachePrevGen)

	return ok && prev.Value == 0
}

func (e *Engine) breach(r Rule, v, mean, tol float64, ready bool) (warn, crit bool) {
	var beyondFloor, outside, far bool

	if floor, ok := e.floors[r.FloorKey]; ok && r.FloorKey != "" {
		beyondFloor = v > floor
		if r.BadWhenLow {
			beyondFloor = v < floor
		}
	}

	if ready {
		tol = math.Max(tol, r.MinTolerance)

		dev := v - mean
		if r.BadWhenLow {
			dev = mean - v
		}

		outside = dev > tol
		far = dev > farTolerances*tol
	}

	return outside || beyondFloor, far || beyondFloor
}

func track(since *time.Time, cond bool, now time.Time) {
	switch {
	case !cond:
		*since = time.Time{}
	case since.IsZero():
		*since = now
	}
}

func (e *Engine) advance(now time.Time, r Rule, st *ruleState, warn, crit bool) {
	track(&st.warnSince, warn, now)
	track(&st.critSince, crit, now)

	target := ""

	switch {
	case crit && now.Sub(st.critSince) >= redAfter:
		target = severityCrit
	case warn && now.Sub(st.warnSince) >= amberAfter:
		target = severityWarn
	}

	if !st.active {
		if target != "" {
			st.active = true
			st.severity = target
			st.raisedAt = now
			st.normalSince = time.Time{}
			e.log(r, st)
		}

		return
	}

	if warn {
		st.normalSince = time.Time{}

		if target != "" && target != st.severity {
			st.severity = target
			e.log(r, st)
		}

		return
	}

	if st.normalSince.IsZero() {
		st.normalSince = now
	}

	if now.Sub(st.normalSince) >= clearAfter {
		*st = ruleState{}
	}
}

func (e *Engine) noData(now time.Time, st *ruleState) {
	if st.nodataSince.IsZero() {
		st.nodataSince = now
	}

	st.warnSince = time.Time{}
	st.critSince = time.Time{}
	st.normalSince = time.Time{}

	if st.active && now.Sub(st.nodataSince) >= nodataGrace {
		*st = ruleState{}
	}
}

func (e *Engine) judgeCatchup(now time.Time, catching bool) {
	st := e.state(catchupRule.ID)

	if !catching {
		*st = ruleState{}
		return
	}

	if st.active {
		return
	}

	st.active = true
	st.severity = severityWarn
	st.raisedAt = now
	e.log(catchupRule, st)
}

func (e *Engine) unreachableStages(s *snapshot.Snapshot) map[string]bool {
	out := map[string]bool{}

	for _, src := range s.Sources {
		if src.OK || src.AgeSec <= staleAgeSec {
			continue
		}

		for _, stage := range e.sourceStages[src.Name] {
			out[stage] = true
		}
	}

	return out
}

func (e *Engine) alarm(r Rule, st *ruleState) snapshot.Alarm {
	title, body, _ := Text(r.TextID)

	return snapshot.Alarm{
		ID:         r.ID,
		Stage:      r.Stage,
		Title:      title,
		Body:       body,
		TextID:     r.TextID,
		Severity:   st.severity,
		RaisedAt:   st.raisedAt,
		Drilldowns: append([]snapshot.Link{}, r.Drilldowns...),
	}
}

func (e *Engine) log(r Rule, st *ruleState) {
	e.keep(e.store.AppendAlarm(e.alarm(r, st)))
}

func (e *Engine) keep(err error) {
	if err != nil {
		e.err = err
	}
}

type ranked struct {
	alarm  snapshot.Alarm
	order  int
	member bool
}

func severityRank(sev string) int {
	if sev == severityCrit {
		return 0
	}

	return 1
}

func (e *Engine) alarms(snoozes map[string]time.Time, chain string) []snapshot.Alarm {
	list := make([]ranked, 0, len(e.all))

	for _, r := range e.all {
		st, ok := e.states[r.ID]
		if !ok || !st.active {
			continue
		}

		a := e.alarm(r, st)
		_, a.Snoozed = snoozes[r.ID]

		order, member := r.orderIn(chain)
		if !member {
			order = math.MaxInt
		}

		list = append(list, ranked{alarm: a, order: order, member: member})
	}

	sort.SliceStable(list, func(i, j int) bool {
		a, b := list[i], list[j]

		if ra, rb := severityRank(a.alarm.Severity), severityRank(b.alarm.Severity); ra != rb {
			return ra < rb
		}

		if a.order != b.order {
			return a.order < b.order
		}

		return a.alarm.ID < b.alarm.ID
	})

	start := -1

	for i, x := range list {
		if !x.member || x.alarm.Snoozed {
			continue
		}

		if start < 0 || x.order < list[start].order || (x.order == list[start].order && x.alarm.ID < list[start].alarm.ID) {
			start = i
		}
	}

	out := make([]snapshot.Alarm, len(list))

	for i, x := range list {
		out[i] = x.alarm
		out[i].StartOfChain = i == start
	}

	return out
}

func (e *Engine) maintain(now time.Time) {
	if e.lastSave.IsZero() || now.Sub(e.lastSave) >= baselineSaveEvery {
		e.keep(e.store.SaveBaseline(e.baseline.State()))
		e.lastSave = now
	}

	if e.lastPrune.IsZero() || now.Sub(e.lastPrune) >= pruneEvery {
		e.keep(e.store.Prune(now, e.retention))
		e.lastPrune = now
	}
}
