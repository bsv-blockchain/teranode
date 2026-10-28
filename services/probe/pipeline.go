package probe

import (
	"context"
	"strings"
	"sync"
	"time"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/services/probe/alarm"
	"github.com/bsv-blockchain/teranode/services/probe/collect"
	"github.com/bsv-blockchain/teranode/services/probe/snapshot"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/ulogger"
)

const (
	defaultStateFile     = "probe_state.json"
	maxConsecutivePanics = 5
	collectorTimeout     = 3 * time.Second
	metricsScrapeTimeout = 2 * time.Second
	drainWindow          = 5 * time.Minute
	flushEvery           = time.Minute
)

type collectorSet struct {
	fast     []Collector
	slow     []Collector
	triggers []<-chan struct{}
	closers  []func()
}

type pipeline struct {
	sampler *Sampler
	engine  *alarm.Engine
	store   *alarm.Store
	drain   *alarm.Drain
	logger  ulogger.Logger
	hook    func(*snapshot.Snapshot)
	every   time.Duration
	out     chan snapshot.Snapshot

	failed   chan struct{}
	failOnce sync.Once

	streak    int
	lastFlush time.Time

	mu     sync.Mutex
	latest snapshot.Snapshot
}

func statePath(s *settings.Settings) string {
	if s.Probe.StatePath != "" {
		return s.Probe.StatePath
	}

	return defaultStateFile
}

func kafkaHosts(raw string) []string {
	hosts := []string{}

	for _, h := range strings.Split(raw, ",") {
		if h = strings.TrimSpace(h); h != "" {
			hosts = append(hosts, h)
		}
	}

	return hosts
}

func newPipeline(s *Server, set collectorSet) (*pipeline, error) {
	cfg := s.settings.Probe

	floors, err := alarm.ParseFloors(cfg.Floors)
	if err != nil {
		return nil, errors.NewConfigurationError("probe_floors is invalid", err)
	}

	store, err := alarm.OpenStore(statePath(s.settings))
	if err != nil {
		return nil, errors.NewServiceError("probe could not open its state store", err)
	}

	baseline := alarm.NewBaseline(cfg.BaselineWarmup)

	state, found, err := store.LoadBaseline()

	switch {
	case err != nil:
		s.logger.Warnf("[Probe] could not load the saved baseline, starting calibration again: %s", oneLine(err))
	case found:
		baseline = alarm.RestoreBaseline(state, cfg.BaselineWarmup)
	}

	opts := []alarm.EngineOption{alarm.WithNow(s.now)}
	if cfg.AlarmLogRetention > 0 {
		opts = append(opts, alarm.WithLogRetention(cfg.AlarmLogRetention))
	}

	engine := alarm.NewEngine(alarm.DefaultRules, baseline, store, floors, cfg.SnoozeDefault, opts...)

	sampler := NewSampler(set.fast, set.slow, cfg.SampleInterval, cfg.SlowSampleInterval, s.nodeName(),
		WithLogger(s.logger), WithClock(s.now))

	return &pipeline{
		sampler:   sampler,
		engine:    engine,
		store:     store,
		drain:     alarm.NewDrain(drainWindow),
		logger:    s.logger,
		hook:      s.processHook,
		every:     sampler.fastEvery,
		out:       make(chan snapshot.Snapshot, 1),
		failed:    make(chan struct{}),
		lastFlush: time.Now(),
		latest:    sampler.Latest(),
	}, nil
}

func (s *Server) defaultCollectors(ctx context.Context) collectorSet {
	var set collectorSet

	var src collect.MetricSource

	if targets := s.settings.Probe.MetricsTargets; len(targets) == 0 {
		src = collect.NewLocalSource()
	} else {
		src = collect.NewHTTPSource(targets, metricsScrapeTimeout)
	}

	set.fast = append(set.fast, collect.NewMetricsCollector(src, collect.NewRates(), s.settings))

	if s.blockchainClient != nil && s.p2pClient != nil {
		chain := collect.NewChain(s.blockchainClient, s.p2pClient)
		set.fast = append(set.fast, chain)
		set.triggers = append(set.triggers, chain.Events(ctx))
	} else {
		s.logger.Warnf("[Probe] chain collector disabled: blockchain or p2p client missing")
	}

	if hosts := kafkaHosts(s.settings.Kafka.Hosts); len(hosts) > 0 {
		k, err := collect.NewKafka(hosts, collect.TopicsFromSettings(s.settings.Kafka), collectorTimeout)
		if err != nil {
			s.logger.Warnf("[Probe] kafka collector disabled: %s", oneLine(err))
		} else {
			set.slow = append(set.slow, k)
			set.closers = append(set.closers, k.Close)
		}
	}

	if a, err := collect.NewAerospike(collect.AerospikeURL(s.settings), collectorTimeout); err != nil {
		s.logger.Warnf("[Probe] aerospike collector disabled: %s", oneLine(err))
	} else {
		set.slow = append(set.slow, a)
		set.closers = append(set.closers, a.Close)
	}

	pg, err := collect.NewPostgres(collect.PostgresURL(s.settings), collectorTimeout, s.settings.Probe.PostgresDiskCapacityGB)
	if err != nil {
		s.logger.Warnf("[Probe] postgres collector disabled: %s", oneLine(err))
	} else {
		set.slow = append(set.slow, pg)
		set.closers = append(set.closers, pg.Close)
	}

	return set
}

func (p *pipeline) Latest() snapshot.Snapshot {
	p.mu.Lock()
	defer p.mu.Unlock()

	return cloneSnapshot(p.latest)
}

func (p *pipeline) knows(id string) bool {
	if id == alarm.CatchupInProgressID {
		return true
	}

	for _, r := range alarm.DefaultRules {
		if r.ID == id {
			return true
		}
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	for _, a := range p.latest.Alarms {
		if a.ID == id {
			return true
		}
	}

	return false
}

func (p *pipeline) run(ctx context.Context) {
	ticker := time.NewTicker(p.every)
	defer ticker.Stop()

	p.publish(p.Latest())

	for {
		select {
		case <-ctx.Done():
			return
		case snap := <-p.sampler.Updates():
			p.handle(snap)
		case <-ticker.C:
		}

		if p.streak >= maxConsecutivePanics || p.sampler.ConsecutivePanics() >= maxConsecutivePanics {
			p.failOnce.Do(func() { close(p.failed) })

			return
		}

		p.maybeFlush()
	}
}

func (p *pipeline) handle(snap snapshot.Snapshot) {
	defer func() {
		if r := recover(); r != nil {
			p.streak++
			p.logger.Errorf("[Probe] pipeline panicked: %s", oneLine(r))
		}
	}()

	if p.hook != nil {
		p.hook(&snap)
	}

	now := snap.Time
	p.engine.Evaluate(now, &snap)

	backlog := float64(len(snap.Chain.BlocksWaiting))
	if q := snap.Ingress.SubtreeQueue; q.Status != snapshot.StatusNoData && q.Status != "" {
		backlog += q.Value
	}

	p.drain.Observe(now, backlog)

	snap.DrainSeconds = nil
	snap.DrainGrowing = false

	secs, growing, ok := p.drain.Seconds()

	switch {
	case growing:
		snap.DrainGrowing = true
	case ok:
		snap.DrainSeconds = &secs
	}

	normalise(&snap)

	p.mu.Lock()
	p.latest = cloneSnapshot(snap)
	p.mu.Unlock()

	p.streak = 0
	p.publish(snap)
}

func (p *pipeline) publish(snap snapshot.Snapshot) {
	select {
	case p.out <- snap:
		return
	default:
	}

	select {
	case <-p.out:
	default:
	}

	select {
	case p.out <- snap:
	default:
	}
}

func (p *pipeline) maybeFlush() {
	if time.Since(p.lastFlush) < flushEvery {
		return
	}

	p.lastFlush = time.Now()

	if err := p.engine.FlushState(); err != nil {
		p.logger.Warnf("[Probe] could not save probe state: %s", oneLine(err))
	}
}

func (p *pipeline) close() error {
	var errs []error

	for _, err := range []error{p.engine.FlushState(), p.store.Close()} {
		if err != nil {
			errs = append(errs, err)
		}
	}

	if len(errs) == 0 {
		return nil
	}

	return errors.Join(errs...)
}
