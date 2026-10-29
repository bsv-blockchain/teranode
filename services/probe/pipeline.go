package probe

import (
	"context"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/services/probe/alarm"
	"github.com/bsv-blockchain/teranode/services/probe/collect"
	"github.com/bsv-blockchain/teranode/services/probe/guard"
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
	defaultFlushEvery    = time.Minute
	corruptSuffixLayout  = "20060102T150405Z"
	maxCorruptNames      = 1000
	fsmCatchingBlocks    = "CATCHINGBLOCKS"
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
	fault   func(point string)
	faults  *faultCounter
	signal  *stopSignal
	every   time.Duration
	out     chan snapshot.Snapshot

	flushEvery time.Duration
	lastFlush  time.Time

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

func usesLocalMetrics(targets []string) bool {
	return len(collect.MetricsTargetURLs(targets)) == 0
}

func kafkaInMemory(k settings.KafkaSettings) bool {
	memory, real := k.Scheme == "memory", false

	for _, u := range []*url.URL{
		k.ValidatorTxsConfig, k.TxMetaConfig, k.LegacyInvConfig, k.BlocksFinalConfig, k.RejectedTxConfig,
		k.InvalidBlocksConfig, k.InvalidSubtreesConfig, k.TxPolicyRejectedConfig, k.SubtreesConfig, k.BlocksConfig,
	} {
		if u == nil {
			continue
		}

		switch u.Scheme {
		case "memory":
			memory = true
		case "kafka":
			real = true
		}
	}

	return memory && !real
}

func checkWritable(dir string) error {
	f, err := os.CreateTemp(dir, ".probe-write-check-*")
	if err != nil {
		return err
	}

	name := f.Name()

	if err := f.Close(); err != nil {
		_ = os.Remove(name)

		return err
	}

	return os.Remove(name)
}

func (s *Server) openStore() (*alarm.Store, string, error) {
	path := statePath(s.settings)
	dir := filepath.Dir(path)

	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, "", errors.NewConfigurationError("probe state directory %s cannot be created", dir, err)
	}

	if err := checkWritable(dir); err != nil {
		return nil, "", errors.NewConfigurationError("probe state directory %s is not writable", dir, err)
	}

	store, err := alarm.OpenStore(path)
	if err == nil {
		return store, "", nil
	}

	if !errors.Is(err, alarm.ErrCorruptStore) {
		return nil, "", errors.NewServiceError("probe could not open its state store", err)
	}

	moved, err := s.moveCorrupt(path)
	if err != nil {
		return nil, "probe could not move its corrupt state file aside: " + oneLine(err), nil
	}

	s.logger.Warnf("[Probe] state file was corrupt; moved it to %s and started with an empty store", moved)

	store, err = alarm.OpenStore(path)
	if err != nil {
		return nil, "", errors.NewServiceError("probe could not open a fresh state store", err)
	}

	return store, "", nil
}

func moveAside(oldPath, newPath string) error {
	if err := os.Link(oldPath, newPath); err != nil {
		if errors.Is(err, os.ErrExist) {
			return os.ErrExist
		}

		_, statErr := os.Lstat(newPath)

		switch {
		case statErr == nil:
			return os.ErrExist
		case !errors.Is(statErr, os.ErrNotExist):
			return statErr
		}

		return os.Rename(oldPath, newPath)
	}

	return os.Remove(oldPath)
}

func (s *Server) moveCorrupt(path string) (string, error) {
	base := path + ".corrupt-" + s.now().UTC().Format(corruptSuffixLayout)

	for i := 0; i < maxCorruptNames; i++ {
		name := base
		if i > 0 {
			name = base + "-" + strconv.Itoa(i)
		}

		err := s.rename(path, name)
		if err == nil {
			return name, nil
		}

		if !errors.Is(err, os.ErrExist) {
			return "", err
		}
	}

	return "", errors.NewProcessingError("no free name for the corrupt state file after %d attempts", maxCorruptNames)
}

func newPipeline(s *Server, set collectorSet, store *alarm.Store, f *faults) (*pipeline, error) {
	cfg := s.settings.Probe

	floors, err := alarm.ParseFloors(cfg.Floors)
	if err != nil {
		return nil, errors.NewConfigurationError("probe_floors is invalid", err)
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
		WithLogger(s.logger), WithClock(s.now), withRecorder(f.pipeline.fatal()))

	return &pipeline{
		sampler:    sampler,
		engine:     engine,
		store:      store,
		drain:      alarm.NewDrain(drainWindow),
		logger:     s.logger,
		hook:       s.processHook,
		fault:      s.fault,
		faults:     f.pipeline,
		signal:     f.signal,
		every:      sampler.fastEvery,
		out:        make(chan snapshot.Snapshot, 1),
		flushEvery: s.flushEvery,
		lastFlush:  time.Now(),
		latest:     sampler.Latest(),
	}, nil
}

func (s *Server) defaultCollectors(ctx context.Context, rec guard.Recorder) collectorSet {
	var set collectorSet

	var src collect.MetricSource

	if targets := s.settings.Probe.MetricsTargets; usesLocalMetrics(targets) {
		src = collect.NewLocalSource()
	} else {
		src = collect.NewHTTPSource(targets, metricsScrapeTimeout)
	}

	set.fast = append(set.fast, collect.NewMetricsCollector(src, collect.NewRates(), s.settings))

	if s.blockchainClient != nil {
		chain := collect.NewChain(s.blockchainClient)
		set.fast = append(set.fast, chain)
		set.triggers = append(set.triggers, chain.Events(ctx, rec))
	} else {
		s.logger.Warnf("[Probe] chain collector disabled: blockchain client missing")
	}

	if s.p2pClient != nil {
		set.fast = append(set.fast, collect.NewPeers(s.p2pClient))
	} else {
		s.logger.Warnf("[Probe] p2p collector disabled: p2p client missing")
	}

	if kafkaInMemory(s.settings.Kafka) {
		s.logger.Infof("[Probe] kafka collector disabled: the node uses in-memory Kafka")
	} else if hosts := kafkaHosts(s.settings.Kafka.Hosts); len(hosts) > 0 {
		k, err := collect.NewKafka(hosts, collect.TopicsFromSettings(s.settings.Kafka), collectorTimeout)
		if err != nil {
			s.logger.Warnf("[Probe] kafka collector disabled: %s", oneLine(err))
		} else {
			k.Watch(collect.WatchedTopics(s.settings.Kafka))

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

	p.safely("initial publish", func() {
		p.fault(faultPublish)
		p.publish(p.Latest())
	})

	for {
		handled, clean := false, true

		select {
		case <-ctx.Done():
			return
		case snap := <-p.sampler.Updates():
			handled = true
			clean = p.handle(snap)
		case <-ticker.C:
		}

		if !p.maybeFlush() {
			clean = false
		}

		if handled && clean {
			p.faults.reset()
		}

		if p.sampler.ConsecutivePanics() >= maxConsecutivePanics {
			p.signal.fail(probeStoppedMessage + " in the sampler collectors")
		}

		if p.signal.stopped() {
			return
		}
	}
}

func (p *pipeline) safely(component string, fn func()) (ok bool) {
	defer guard.Recover(p.faults, component)

	fn()

	return true
}

func (p *pipeline) handle(snap snapshot.Snapshot) (clean bool) {
	defer guard.Recover(p.faults, "pipeline")

	if p.hook != nil {
		p.hook(&snap)
	}

	now := snap.Time
	p.engine.Evaluate(now, &snap)

	backlog := blockBacklog(&snap)
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

	p.publish(snap)

	return true
}

func blockBacklog(s *snapshot.Snapshot) float64 {
	blocks := float64(len(s.Chain.BlocksWaiting))
	if c := s.Chain.BlocksWaitingCount; c.Status != snapshot.StatusNoData && c.Status != "" {
		blocks = c.Value
	}

	if p := s.Chain.PeerHeight; s.FSM == fsmCatchingBlocks && p.Status == snapshot.StatusOK && p.Value > float64(s.Tip) {
		blocks = max(blocks, p.Value-float64(s.Tip))
	}

	return blocks
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

func (p *pipeline) maybeFlush() bool {
	if time.Since(p.lastFlush) < p.flushEvery {
		return true
	}

	p.lastFlush = time.Now()

	return p.safely("state flush", func() {
		p.fault(faultFlush)

		if err := p.engine.FlushState(); err != nil {
			p.logger.Warnf("[Probe] could not save probe state: %s", oneLine(err))
		}
	})
}

func (p *pipeline) close() error {
	p.fault(faultClose)

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
