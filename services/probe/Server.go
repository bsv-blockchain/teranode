// Package probe provides the read-only Probe service that serves a live operator dashboard.
package probe

import (
	"context"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/services/p2p"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/health"
)

const (
	readHeaderTimeout = 10 * time.Second
	shutdownTimeout   = 5 * time.Second
)

type Server struct {
	logger           ulogger.Logger
	settings         *settings.Settings
	blockchainClient blockchain.ClientI
	p2pClient        p2p.ClientI

	mu       sync.Mutex
	listener net.Listener
	httpSrv  *http.Server
}

func New(logger ulogger.Logger, tSettings *settings.Settings, bc blockchain.ClientI, p2pc p2p.ClientI) *Server {
	return &Server{
		logger:           logger,
		settings:         tSettings,
		blockchainClient: bc,
		p2pClient:        p2pc,
	}
}

func (s *Server) Init(_ context.Context) error {
	if s.settings.Probe.AuthToken == "" {
		return errors.NewConfigurationError("probe_authToken must be set; the probe refuses to start without a token")
	}

	if s.settings.Probe.HTTPListenAddress == "" {
		return errors.NewConfigurationError("probe_httpListenAddress must not be empty")
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

func (s *Server) Start(ctx context.Context, readyCh chan<- struct{}) error {
	var closeOnce sync.Once

	defer closeOnce.Do(func() { close(readyCh) })

	ln, err := net.Listen("tcp", s.settings.Probe.HTTPListenAddress)
	if err != nil {
		return errors.NewServiceError("probe failed to listen on %s", s.settings.Probe.HTTPListenAddress, err)
	}

	srv := &http.Server{
		Handler:           s.newRouter(),
		ReadHeaderTimeout: readHeaderTimeout,
	}

	s.mu.Lock()
	s.listener = ln
	s.httpSrv = srv
	s.mu.Unlock()

	s.logger.Infof("[Probe] listening on %s", ln.Addr().String())

	closeOnce.Do(func() { close(readyCh) })

	serveErr := make(chan error, 1)

	go func() { serveErr <- srv.Serve(ln) }()

	select {
	case err = <-serveErr:
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			return errors.NewServiceError("probe http server failed", err)
		}

		return nil
	case <-ctx.Done():
		return s.shutdown()
	}
}

func (s *Server) Stop(_ context.Context) error {
	return s.shutdown()
}

func (s *Server) shutdown() error {
	s.mu.Lock()
	srv := s.httpSrv
	s.mu.Unlock()

	if srv == nil {
		return nil
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()

	if err := srv.Shutdown(shutdownCtx); err != nil && !errors.Is(err, http.ErrServerClosed) {
		_ = srv.Close()

		return errors.NewServiceError("probe http server shutdown failed", err)
	}

	return nil
}

func (s *Server) Health(ctx context.Context, checkLiveness bool) (int, string, error) {
	if checkLiveness {
		return http.StatusOK, "OK", nil
	}

	checks := make([]health.Check, 0, 1)

	if s.blockchainClient != nil {
		checks = append(checks, health.Check{Name: "BlockchainClient", Check: s.blockchainClient.Health})
	}

	return health.CheckAll(ctx, checkLiveness, checks)
}
