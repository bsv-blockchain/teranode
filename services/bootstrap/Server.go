package bootstrap

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"sort"
	"sync"
	"time"

	bootstrap_api "github.com/bitcoin-sv/ubsv/services/bootstrap/bootstrap_api"
	"github.com/bitcoin-sv/ubsv/ulogger"
	"github.com/bitcoin-sv/ubsv/util"
	"github.com/bitcoin-sv/ubsv/util/servicemanager"
	"github.com/labstack/echo/v4"
	"github.com/labstack/echo/v4/middleware"
	"github.com/ordishs/go-utils"
	"github.com/ordishs/gocore"
	"google.golang.org/grpc"
	"google.golang.org/grpc/peer"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// Server type carries the logger within it
type Server struct {
	bootstrap_api.UnimplementedBootstrapAPIServer
	mu          sync.RWMutex
	logger      ulogger.Logger
	subscribers map[chan *bootstrap_api.Notification]*bootstrap_api.Info
	grpc        *grpc.Server
	e           *echo.Echo
	discoveryCh chan *bootstrap_api.Notification
}

func Enabled() bool {
	_, found := gocore.Config().Get("bootstrap_grpcListenAddress")
	return found
}

// NewServer will return a server instance with the logger stored within it
func NewServer(logger ulogger.Logger) *Server {
	initPrometheusMetrics()
	return &Server{
		logger:      logger,
		subscribers: make(map[chan *bootstrap_api.Notification]*bootstrap_api.Info),
	}
}

func (v *Server) Health(ctx context.Context) (int, string, error) {
	return 0, "", nil
}

func (s *Server) Init(_ context.Context) (err error) {
	// Create a discovery channel

	s.discoveryCh = make(chan *bootstrap_api.Notification, 10)

	// Set up the HTTP server
	e := echo.New()
	e.HideBanner = true
	e.HidePort = true

	e.Use(middleware.CORSWithConfig(middleware.CORSConfig{
		AllowOrigins: []string{"*"},
		AllowMethods: []string{echo.GET},
	}))

	e.GET("/asset-ws", s.HandleWebSocket())

	e.GET("/nodes", func(c echo.Context) error {
		type node struct {
			Source           string `json:"source"`
			AssetGRPCAddress string `json:"AssetGRPCAddress"`
			AssetHTTPAddress string `json:"AssetHTTPAddress"`
			ConnectedAt      string `json:"connectedAt"`
			Name             string `json:"name"`
			IP               string `json:"ip"`
		}

		nodes := make([]*node, 0, len(s.subscribers))

		s.mu.RLock()
		for _, s := range s.subscribers {
			nodes = append(nodes, &node{
				Source:           s.Source,
				AssetGRPCAddress: s.AssetGRPCAddress,
				AssetHTTPAddress: s.AssetHTTPAddress,
				ConnectedAt:      utils.ISOFormat(s.ConnectedAt.AsTime()),
				IP:               s.Ip,
				Name:             s.Name,
			})
		}
		s.mu.RUnlock()

		// Sort the list by time connected
		sort.Slice(nodes, func(i, j int) bool {
			return nodes[i].ConnectedAt < nodes[j].ConnectedAt
		})

		return c.JSONPretty(200, nodes, "  ")
	})

	// Store a reference to the echo server in the server struct
	s.e = e

	return nil
}

// Start function
func (s *Server) Start(ctx context.Context) (err error) {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	go func() {
		for {
			select {
			case <-ctx.Done():
				// Get a read lock and then copy all of the subscriber channels, then release lock...
				s.mu.RLock()
				subs := make([]chan *bootstrap_api.Notification, 0, len(s.subscribers))

				for ch := range s.subscribers {
					subs = append(subs, ch)
				}
				s.mu.RUnlock()

				// Now close each subscriber channel
				for _, ch := range subs {
					close(ch)
				}

				if s.grpc != nil {
					s.grpc.GracefulStop()
				}
				// s.grpc.Stop()

				return // returning not to leak the goroutine

			case <-ticker.C:
				s.BroadcastNotification(&bootstrap_api.Notification{
					Type: bootstrap_api.Type_PING,
				})
			}
		}
	}()

	go func() {
		addr, httpOk := gocore.Config().Get("bootstrap_httpListenAddress")
		if !httpOk {
			s.logger.Infof("[BootstrapServer] HTTP service not configured")
			return
		}

		mode := "HTTPS"
		if level, _ := gocore.Config().GetInt("securityLevelHTTP", 0); level == 0 {
			mode = "HTTP"
		}

		s.logger.Infof("BootstrapServer %s service listening on %s", mode, addr)

		var err error

		if mode == "HTTP" {
			servicemanager.AddListenerInfo(fmt.Sprintf("Bootstrap HTTP listening on %s", addr))
			err = s.e.Start(addr)

		} else {
			certFile, found := gocore.Config().Get("server_certFile")
			if !found {
				s.logger.Errorf("server_certFile is required for HTTPS")
				return
			}

			keyFile, found := gocore.Config().Get("server_keyFile")
			if !found {
				s.logger.Errorf("server_keyFile is required for HTTPS")
				return
			}

			servicemanager.AddListenerInfo(fmt.Sprintf("Bootstrap HTTPS listening on %s", addr))
			err = s.e.StartTLS(addr, certFile, keyFile)
		}

		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			s.logger.Errorf("[BootstrapServer] %s (impl) service error: %s", mode, err)
		}

		<-ctx.Done()

		s.logger.Infof("[BootstrapServer] %s (impl) service shutting down", mode)
		err = s.e.Shutdown(ctx)
		if err != nil {
			s.logger.Errorf("[BootstrapServer] %s (impl) service shutdown error: %s", mode, err)
		}
	}()

	// this will block
	if err = util.StartGRPCServer(ctx, s.logger, "bootstrap", func(server *grpc.Server) {
		bootstrap_api.RegisterBootstrapAPIServer(server, s)
		s.grpc = server
	}); err != nil {
		return err
	}

	return nil
}

func (s *Server) Stop(_ context.Context) error {
	return nil
}

func (s *Server) HealthGRPC(_ context.Context, _ *emptypb.Empty) (*bootstrap_api.HealthResponse, error) {
	prometheusHealth.Inc()
	return &bootstrap_api.HealthResponse{
		Ok:        true,
		Timestamp: timestamppb.New(time.Now()),
	}, nil
}

// Connect Subscribe to this service and receive updates whenever a peer is added or removed
func (s *Server) Connect(info *bootstrap_api.Info, stream bootstrap_api.BootstrapAPI_ConnectServer) error {
	prometheusConnect.Inc()
	p, _ := peer.FromContext(stream.Context())
	info.Ip = p.Addr.String()

	ch := make(chan *bootstrap_api.Notification)

	s.mu.RLock()
	if info.AssetGRPCAddress == "" {
		s.logger.Infof("[Bootstrap] New Anonymous Subscription received [%s] (Total=%d).", info.Source, len(s.subscribers)+1)
	} else {
		s.logger.Infof("[Bootstrap] New Subscription received [%s / %s] (Total=%d).", info.Source, info.AssetGRPCAddress, len(s.subscribers)+1)
	}

	// Set the time this node connected
	info.ConnectedAt = timestamppb.New(time.Now())

	// Send this new subscriber all the existing subscribers...
	for _, info := range s.subscribers {
		if info.AssetGRPCAddress != "" {
			if err := stream.Send(&bootstrap_api.Notification{
				Type: bootstrap_api.Type_ADD,
				Info: info,
			}); err != nil {
				return err
			}
		}
	}

	s.mu.RUnlock()

	// Send all existing subscribers this new address...
	s.BroadcastNotification(&bootstrap_api.Notification{
		Type: bootstrap_api.Type_ADD,
		Info: info,
	})

	// Now add this subscriber to the list...
	s.mu.Lock()
	s.subscribers[ch] = info
	s.mu.Unlock()

	defer func() {
		s.mu.Lock()
		delete(s.subscribers, ch)
		s.mu.Unlock()

		safeClose(ch)

		s.mu.RLock()
		if info.AssetGRPCAddress == "" {
			s.logger.Infof("[Bootstrap] Anonymous Subscription removed [%s] (Total=%d).", info.Source, len(s.subscribers))
		} else {
			s.logger.Infof("[Bootstrap] Subscription removed [%s / %s] (Total=%d).", info.Source, info.AssetGRPCAddress, len(s.subscribers))
		}
		s.mu.RUnlock()

		// Send all existing subscribers this dead address...
		s.BroadcastNotification(&bootstrap_api.Notification{
			Type: bootstrap_api.Type_REMOVE,
			Info: info,
		})
	}()

	for {
		select {
		case <-stream.Context().Done():
			return nil
		case notification := <-ch:
			err := stream.Send(notification)
			if err != nil {
				return err
			}
		}
	}
}

func (s *Server) GetNodes(_ context.Context, _ *emptypb.Empty) (*bootstrap_api.NodeList, error) {
	prometheusGetNodes.Inc()
	s.mu.RLock()
	defer s.mu.RUnlock()

	list := make([]*bootstrap_api.Info, 0, len(s.subscribers))

	for _, s := range s.subscribers {
		list = append(list, s)
	}

	return &bootstrap_api.NodeList{
		Nodes: list,
	}, nil
}

func (s *Server) BroadcastNotification(notification *bootstrap_api.Notification) {
	prometheusBroadcastNotification.Inc()
	defer func() {
		_ = recover()
	}()

	s.mu.RLock()
	defer s.mu.RUnlock()

	// Send the notification to the discovery channel for websocket client
	s.discoveryCh <- notification

	// Send the notification to all GRPC subscribers
	for ch := range s.subscribers {
		select {
		case ch <- notification:
		default:
			// If the channel is full, skip sending to this subscriber
		}
	}
}

func safeClose[T any](ch chan T) {
	defer func() {
		_ = recover()
	}()

	close(ch)
}
