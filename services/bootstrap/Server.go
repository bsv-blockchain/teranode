package bootstrap

import (
	"context"
	"sync"
	"time"

	bootstrap_api "github.com/bitcoin-sv/ubsv/services/bootstrap/bootstrap_api"
	"github.com/bitcoin-sv/ubsv/util"
	"github.com/ordishs/go-utils"
	"github.com/ordishs/gocore"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// Server type carries the logger within it
type Server struct {
	bootstrap_api.UnimplementedBootstrapAPIServer
	mu          sync.RWMutex
	logger      utils.Logger
	subscribers map[chan *bootstrap_api.Notification]*bootstrap_api.Info
	grpc        *grpc.Server
}

func Enabled() bool {
	_, found := gocore.Config().Get("bootstrap_grpcAddress")
	return found
}

// NewServer will return a server instance with the logger stored within it
func NewServer(logger utils.Logger) *Server {
	return &Server{
		logger:      logger,
		subscribers: make(map[chan *bootstrap_api.Notification]*bootstrap_api.Info),
	}
}

func (s *Server) Init(_ context.Context) (err error) {
	return nil
}

// Start function
func (s *Server) Start(ctx context.Context) (err error) {
	ticker := time.NewTicker(10 * time.Second)

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

				s.grpc.GracefulStop()
				// s.grpc.Stop()

				return // returning not to leak the goroutine

			case <-ticker.C:
				s.BroadcastNotification(&bootstrap_api.Notification{
					Type: bootstrap_api.Type_PING,
				})
			}
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

func (s *Server) Health(_ context.Context, _ *emptypb.Empty) (*bootstrap_api.HealthResponse, error) {
	return &bootstrap_api.HealthResponse{
		Ok:        true,
		Timestamp: timestamppb.New(time.Now()),
	}, nil
}

// Connect Subscribe to this service and receive updates whenever a peer is added or removed
func (s *Server) Connect(info *bootstrap_api.Info, stream bootstrap_api.BootstrapAPI_ConnectServer) error {
	ch := make(chan *bootstrap_api.Notification)

	s.mu.RLock()
	if info.BlobServerGrpcAddress == "" {
		s.logger.Infof("[Bootstrap] New Anonymous Subscription received (Total=%d).", len(s.subscribers)+1)
	} else {
		s.logger.Infof("[Bootstrap] New Subscription received [%s] (Total=%d).", info.BlobServerGrpcAddress, len(s.subscribers)+1)
	}

	// Send this new subscriber all the existing subscribers...
	for _, info := range s.subscribers {
		if info.BlobServerGrpcAddress != "" {
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
		if info.BlobServerGrpcAddress == "" {
			s.logger.Infof("[Bootstrap] Anonymous Subscription removed (Total=%d).", len(s.subscribers))
		} else {
			s.logger.Infof("[Bootstrap] Subscription removed [%s] (Total=%d).", info.BlobServerGrpcAddress, len(s.subscribers))
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

func (s *Server) BroadcastNotification(notification *bootstrap_api.Notification) {
	s.mu.RLock()
	defer s.mu.RUnlock()

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
