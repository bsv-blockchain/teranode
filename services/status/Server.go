package status

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"sync"

	"github.com/bitcoin-sv/ubsv/model"
	"github.com/bitcoin-sv/ubsv/services/status/status_api"
	"github.com/bitcoin-sv/ubsv/ulogger"
	"github.com/bitcoin-sv/ubsv/util"
	"github.com/bitcoin-sv/ubsv/util/servicemanager"
	"github.com/labstack/echo/v4"
	"github.com/labstack/echo/v4/middleware"
	"github.com/ordishs/gocore"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type Server struct {
	status_api.UnimplementedStatusAPIServer
	logger          ulogger.Logger
	wsCh            chan interface{}
	e               *echo.Echo
	httpAddr        string
	name            string
	statusItems     map[string]*model.AnnounceStatusRequest
	statusItemMutex sync.RWMutex
}

func Enabled() bool {
	_, found := gocore.Config().Get("status_grpcListenAddress")
	return found
}

// New will return a server instance with the logger stored within it
func New(logger ulogger.Logger) *Server {
	return &Server{
		logger:      logger,
		wsCh:        make(chan interface{}, 100),
		statusItems: make(map[string]*model.AnnounceStatusRequest),
	}
}

func (s *Server) Health(ctx context.Context) (int, string, error) {
	return 0, "", nil
}

func (s *Server) Init(ctx context.Context) error {
	s.httpAddr, _ = gocore.Config().Get("status_httpListenAddress")
	if s.httpAddr == "" {
		return errors.New("status_httpListenAddress is required")
	}

	e := echo.New()
	e.HideBanner = true
	e.HidePort = true

	e.Use(middleware.Recover())

	e.Use(middleware.CORSWithConfig(middleware.CORSConfig{
		AllowOrigins: []string{"*"},
		AllowMethods: []string{echo.GET},
	}))

	e.GET("/ws", s.HandleWebSocket(s.wsCh))

	s.e = e

	s.name, _ = gocore.Config().Get("asset_clientName", "unknown")

	return nil
}

// Start function
func (s *Server) Start(ctx context.Context) error {
	g, ctx := errgroup.WithContext(ctx)

	g.Go(func() error {
		return s.startHTTP(ctx)
	})

	g.Go(func() error {
		return util.StartGRPCServer(ctx, s.logger, "status", func(server *grpc.Server) {
			status_api.RegisterStatusAPIServer(server, s)
		})
	})

	if err := g.Wait(); err != nil {
		return err
	}

	return nil
}

func (s *Server) Stop(_ context.Context) error {
	_ = s.e.Shutdown(context.Background())
	return nil
}

func (s *Server) startHTTP(ctx context.Context) error {
	mode := "HTTPS"
	if level, _ := gocore.Config().GetInt("securityLevelHTTP", 0); level == 0 {
		mode = "HTTP"
	}

	s.logger.Infof("Status %s service listening on %s", mode, s.httpAddr)

	go func() {
		<-ctx.Done()
		s.logger.Infof("[Status] %s (impl) service shutting down", mode)
		err := s.e.Shutdown(ctx)
		if err != nil {
			s.logger.Errorf("[Status] %s (impl) service shutdown error: %s", mode, err)
		}
	}()

	var err error

	if mode == "HTTP" {
		servicemanager.AddListenerInfo(fmt.Sprintf("Status HTTP listening on %s", s.httpAddr))
		err = s.e.Start(s.httpAddr)

	} else {

		certFile, found := gocore.Config().Get("server_certFile")
		if !found {
			return errors.New("server_certFile is required for HTTPS")
		}
		keyFile, found := gocore.Config().Get("server_keyFile")
		if !found {
			return errors.New("server_keyFile is required for HTTPS")
		}

		servicemanager.AddListenerInfo(fmt.Sprintf("Status HTTPS listening on %s", s.httpAddr))
		err = s.e.StartTLS(s.httpAddr, certFile, keyFile)
	}

	if err != http.ErrServerClosed {
		return err
	}

	return nil
}

func (s *Server) HealthGRPC(_ context.Context, _ *emptypb.Empty) (*status_api.HealthResponse, error) {
	return &status_api.HealthResponse{
		Ok:        true,
		Timestamp: timestamppb.Now(),
	}, nil
}

func (s *Server) AnnounceStatus(ctx context.Context, req *model.AnnounceStatusRequest) (*emptypb.Empty, error) {
	if req.ClusterName == "" {
		req.ClusterName = s.name
	}

	s.statusItemMutex.Lock()
	s.statusItems[req.ClusterName+req.Type+req.Subtype] = req
	s.statusItemMutex.Unlock()

	go func() {
		s.wsCh <- req
	}()

	return &emptypb.Empty{}, nil
}
