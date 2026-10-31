package probe

import (
	"context"
	"net"
	"net/http"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/services/p2p"
	"github.com/bsv-blockchain/teranode/services/probe/alarm"
	"github.com/bsv-blockchain/teranode/services/probe/guard"
	"github.com/bsv-blockchain/teranode/services/probe/snapshot"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/health"
)

const (
	readHeaderTimeout   = 10 * time.Second
	shutdownTimeout     = 5 * time.Second
	defaultNodeName     = "node"
	probeStoppedMessage = "probe stopped after 5 consecutive panics"
	probeStoppedHealth  = "probe stopped after repeated failures; the node must be restarted to resume probe monitoring; the node itself is not affected"
)

type Server struct {
	logger           ulogger.Logger
	settings         *settings.Settings
	blockchainClient blockchain.ClientI
	p2pClient        p2p.ClientI

	collectors    func(ctx context.Context, rec guard.Recorder) collectorSet
	now           func() time.Time
	processHook   func(*snapshot.Snapshot)
	wsAuthTimeout time.Duration
	wsWriteWait   time.Duration
	flushEvery    time.Duration
	rename        func(oldPath, newPath string) error
	listen        func(network, address string) (net.Listener, error)
	hook          atomic.Pointer[func(point string)]
	logClock      func() time.Time

	mu       sync.Mutex
	listener net.Listener
	run      *probeRun
	stopped  string
}

type probeRun struct {
	pipe    *pipeline
	hub     *Hub
	httpSrv *http.Server
	cancel  context.CancelFunc
	wg      sync.WaitGroup
	closers []func()
	logger  ulogger.Logger
	faults  *faults

	once sync.Once
	err  error
}

func New(logger ulogger.Logger, tSettings *settings.Settings, bc blockchain.ClientI, p2pc p2p.ClientI) *Server {
	s := &Server{
		logger:           logger,
		settings:         tSettings,
		blockchainClient: bc,
		p2pClient:        p2pc,
		now:              time.Now,
		wsAuthTimeout:    defaultWSAuthTimeout,
		wsWriteWait:      defaultWSWriteWait,
		flushEvery:       defaultFlushEvery,
		rename:           moveAside,
		listen:           net.Listen,
	}

	s.collectors = s.defaultCollectors

	return s
}

func (s *Server) Init(_ context.Context) error {
	if s.settings.Probe.AuthToken == "" {
		return errors.NewConfigurationError("probe_authToken must be set; the probe refuses to start without a token")
	}

	if s.settings.Probe.HTTPListenAddress == "" {
		return errors.NewConfigurationError("probe_httpListenAddress must not be empty")
	}

	floors, err := alarm.ParseFloors(s.settings.Probe.Floors)
	if err != nil {
		return errors.NewConfigurationError("probe_floors is invalid", err)
	}

	if err := alarm.CheckFloorKeys(floors); err != nil {
		return errors.NewConfigurationError("probe_floors is invalid", err)
	}

	return nil
}

func (s *Server) Addr() string {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.listener == nil {
		return ""
	}

	return s.listener.Addr().String()
}

func (s *Server) faultClock() func() time.Time {
	if s.logClock != nil {
		return s.logClock
	}

	return s.now
}

func (s *Server) nodeName() string {
	name, err := os.Hostname()
	if err != nil || name == "" {
		return defaultNodeName
	}

	return name
}

func (s *Server) current() *probeRun {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.run
}

func (s *Server) pipe() *pipeline {
	if run := s.current(); run != nil {
		return run.pipe
	}

	return nil
}

func (s *Server) sampler() *Sampler {
	if p := s.pipe(); p != nil {
		return p.sampler
	}

	return nil
}

func (s *Server) hubClients() int {
	if run := s.current(); run != nil {
		return run.hub.count()
	}

	return 0
}

func (s *Server) Start(ctx context.Context, readyCh chan<- struct{}) error {
	var closeOnce sync.Once

	defer closeOnce.Do(func() { close(readyCh) })

	if s.settings.Probe.AuthToken == "" {
		return errors.NewConfigurationError("probe_authToken must be set; the probe refuses to start without a token")
	}

	store, stopReason, err := s.openStore()
	if err != nil {
		return err
	}

	if stopReason != "" {
		return s.selfStop(nil, stopReason)
	}

	f := newFaults(s.logger)
	f.setClock(s.faultClock())
	runCtx, cancel := context.WithCancel(ctx)
	set := s.collectors(runCtx, f.collectors)

	pipe, err := newPipeline(s, set, store, f)
	if err != nil {
		cancel()
		closeAll(set.closers, f.pipeline)

		if closeErr := store.Close(); closeErr != nil {
			s.logger.Warnf("[Probe] could not close the state store: %s", oneLine(closeErr))
		}

		return err
	}

	ln, err := s.listen("tcp", s.settings.Probe.HTTPListenAddress)
	if err != nil {
		cancel()

		if closeErr := pipe.close(); closeErr != nil {
			s.logger.Warnf("[Probe] could not close the state store: %s", oneLine(closeErr))
		}

		closeAll(set.closers, f.pipeline)

		return errors.NewServiceError("probe failed to listen on %s", s.settings.Probe.HTTPListenAddress, err) // urlsafe: a host:port listen address, no userinfo
	}

	hub := NewHub(pipe.out)
	hub.logger = s.logger
	hub.writeWait = s.wsWriteWait
	hub.faults = f.hub
	hub.fault = s.fault

	run := &probeRun{
		pipe:    pipe,
		hub:     hub,
		cancel:  cancel,
		closers: set.closers,
		logger:  s.logger,
		faults:  f,
		httpSrv: &http.Server{
			Handler:           s.newRouter(),
			ReadHeaderTimeout: readHeaderTimeout,
		},
	}

	s.mu.Lock()
	s.listener = ln
	s.run = run
	s.stopped = ""
	s.mu.Unlock()

	run.launch(runCtx, set.triggers)

	s.logger.Infof("[Probe] listening on %s", ln.Addr().String())

	closeOnce.Do(func() { close(readyCh) })

	serveErr := make(chan error, 1)

	go func() {
		defer guard.Recover(f.pipeline.fatal(), "http server")

		serveErr <- run.httpSrv.Serve(ln)
	}()

	select {
	case err = <-serveErr:
		if errors.Is(err, http.ErrServerClosed) {
			return run.stop()
		}

		return s.selfStop(run, "probe http server failed: "+oneLine(err))
	case <-ctx.Done():
		return run.stop()
	case <-f.signal.failed:
		return s.selfStop(run, f.signal.Reason())
	}
}

func (s *Server) selfStop(run *probeRun, reason string) error {
	s.mu.Lock()
	s.stopped = probeStoppedHealth
	s.mu.Unlock()

	s.logger.Errorf("[Probe] %s; the probe has shut down, the node keeps running, and the node must be restarted to resume probe monitoring", reason)

	if run == nil {
		return nil
	}

	if err := run.stop(); err != nil {
		s.logger.Warnf("[Probe] cleanup after self-stop failed: %s", oneLine(err))
	}

	return nil
}

func (r *probeRun) launch(ctx context.Context, triggers []<-chan struct{}) {
	for _, ch := range triggers {
		r.pipe.sampler.Trigger(ch)
	}

	r.wg.Add(3)

	go func() {
		defer guard.Recover(r.faults.pipeline.fatal(), "sampler")
		defer r.wg.Done()

		r.pipe.sampler.Run(ctx)
	}()

	go func() {
		defer guard.Recover(r.faults.pipeline.fatal(), "pipeline")
		defer r.wg.Done()

		r.pipe.run(ctx)
	}()

	go func() {
		defer guard.Recover(r.faults.hub.fatal(), "websocket hub")
		defer r.wg.Done()

		r.hub.Run(ctx)
	}()
}

func (r *probeRun) stop() error {
	r.once.Do(func() {
		r.cancel()

		shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
		defer cancel()

		var errs []error

		if err := r.httpSrv.Shutdown(shutdownCtx); err != nil && !errors.Is(err, http.ErrServerClosed) {
			_ = r.httpSrv.Close()

			errs = append(errs, errors.NewServiceError("probe http server shutdown failed", err))
		}

		r.wg.Wait()

		r.closeState()
		closeAll(r.closers, r.faults.pipeline)

		if len(errs) > 0 {
			r.err = errors.Join(errs...)
		}
	})

	return r.err
}

func (r *probeRun) closeState() {
	defer guard.Recover(r.faults.pipeline, "state close")

	if err := r.pipe.close(); err != nil {
		r.logger.Warnf("[Probe] could not save probe state on shutdown: %s", oneLine(err))
	}
}

func closeAll(closers []func(), rec guard.Recorder) {
	for _, c := range closers {
		func() {
			defer guard.Recover(rec, "collector close")

			c()
		}()
	}
}

func (s *Server) Stop(_ context.Context) error {
	run := s.current()
	if run == nil {
		return nil
	}

	return run.stop()
}

func (s *Server) Health(ctx context.Context, checkLiveness bool) (int, string, error) {
	s.mu.Lock()
	stopped := s.stopped
	s.mu.Unlock()

	if stopped != "" {
		return http.StatusOK, stopped, nil
	}

	if checkLiveness {
		return http.StatusOK, "OK", nil
	}

	checks := make([]health.Check, 0, 1)

	if s.blockchainClient != nil {
		checks = append(checks, health.Check{Name: "BlockchainClient", Check: s.blockchainClient.Health})
	}

	return health.CheckAll(ctx, checkLiveness, checks)
}
