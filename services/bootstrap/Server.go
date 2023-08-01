package bootstrap

import (
	"context"
	"errors"
	"fmt"
	"net"
	"time"

	bootstrap_api "github.com/TAAL-GmbH/ubsv/services/bootstrap/bootstrap_api"
	"github.com/ordishs/go-utils"
	"github.com/ordishs/gocore"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type void struct{}

type subscriber struct {
	subscription          bootstrap_api.BootstrapAPI_ConnectServer
	done                  chan struct{}
	blobServerGrpcAddress string
}

// Server type carries the logger within it
type Server struct {
	bootstrap_api.UnimplementedBootstrapAPIServer
	logger            utils.Logger
	grpcServer        *grpc.Server
	newSubscriptions  chan subscriber
	deadSubscriptions chan subscriber
	subscribers       map[subscriber]void
	notifications     chan *bootstrap_api.Notification
}

func Enabled() bool {
	_, found := gocore.Config().Get("bootstrap_grpcAddress")
	return found
}

// NewServer will return a server instance with the logger stored within it
func NewServer() *Server {
	return &Server{
		logger:            gocore.Log("bootS"),
		newSubscriptions:  make(chan subscriber, 10),
		deadSubscriptions: make(chan subscriber, 10),
		subscribers:       make(map[subscriber]void),
		notifications:     make(chan *bootstrap_api.Notification, 100),
	}
}

// Start function
func (s *Server) Start(ctx context.Context) error {
	address, ok := gocore.Config().Get("bootstrap_grpcAddress")
	if !ok {
		return errors.New("no bootstrap_grpcAddress setting found")
	}

	var err error
	s.grpcServer, err = utils.GetGRPCServer(&utils.ConnectionOptions{
		MaxMessageSize: 10_000,
	})
	if err != nil {
		return fmt.Errorf("could not create GRPC server [%w]", err)
	}

	gocore.SetAddress(address)

	lis, err := net.Listen("tcp", address)
	if err != nil {
		return fmt.Errorf("GRPC server failed to listen [%w]", err)
	}

	bootstrap_api.RegisterBootstrapAPIServer(s.grpcServer, s)

	// Register reflection service on gRPC server.
	reflection.Register(s.grpcServer)

	ticker := time.NewTicker(10 * time.Second)

	go func() {
		for {
			select {
			case <-ctx.Done():
				// Close all subscriptions
				for sub := range s.subscribers {
					safeClose(sub.done)
				}
				return // returning not to leak the goroutine

			case <-ticker.C:
				for sub := range s.subscribers {
					go func(sub subscriber) {
						if err = sub.subscription.Send(&bootstrap_api.Notification{
							Type: bootstrap_api.Type_PING,
						}); err != nil {
							s.deadSubscriptions <- sub
						}
					}(sub)
				}

			case notification := <-s.notifications:
				if notification.Info.BlobServerGrpcAddress != "" {
					for sub := range s.subscribers {
						go func(sub subscriber) {
							if err = sub.subscription.Send(notification); err != nil {
								s.deadSubscriptions <- sub
							}
						}(sub)
					}
				}

			case newSubscriber := <-s.newSubscriptions:
				// Send this new subscription a list of the currently known subscribers
				for sub := range s.subscribers {
					go func(sub subscriber) {
						if sub.blobServerGrpcAddress != "" {
							if err = newSubscriber.subscription.Send(&bootstrap_api.Notification{
								Type: bootstrap_api.Type_ADD,
								Info: &bootstrap_api.Info{
									BlobServerGrpcAddress: sub.blobServerGrpcAddress,
								},
							}); err != nil {
								s.deadSubscriptions <- sub
							}
						}
					}(sub)
				}

				// Notify all the other subscribers of the new subscription if it is advertising a blobServerGrpcAddress
				if newSubscriber.blobServerGrpcAddress != "" {
					for sub := range s.subscribers {
						go func(sub subscriber) {
							if err = sub.subscription.Send(&bootstrap_api.Notification{
								Type: bootstrap_api.Type_ADD,
								Info: &bootstrap_api.Info{
									BlobServerGrpcAddress: newSubscriber.blobServerGrpcAddress,
								},
							}); err != nil {
								s.deadSubscriptions <- sub
							}
						}(sub)
					}
				}

				// Add the newSubscriber to our map
				s.subscribers[newSubscriber] = void{}

				if newSubscriber.blobServerGrpcAddress == "" {
					s.logger.Infof("[Bootstrap] New Anonymous Subscription received (Total=%d).", len(s.subscribers))
				} else {
					s.logger.Infof("[Bootstrap] New Subscription received [%s] (Total=%d).", newSubscriber.blobServerGrpcAddress, len(s.subscribers))
				}

			case deadSubscriber := <-s.deadSubscriptions:
				delete(s.subscribers, deadSubscriber)
				safeClose(deadSubscriber.done)

				// Notify all the remaining subscription of the removed subscription if it had a blobServerGrpcAddress
				if deadSubscriber.blobServerGrpcAddress != "" {
					for sub := range s.subscribers {
						go func(sub subscriber) {
							if err = sub.subscription.Send(&bootstrap_api.Notification{
								Type: bootstrap_api.Type_REMOVE,
								Info: &bootstrap_api.Info{
									BlobServerGrpcAddress: deadSubscriber.blobServerGrpcAddress,
								},
							}); err != nil {
								s.deadSubscriptions <- sub
							}
						}(sub)
					}
				}

				if deadSubscriber.blobServerGrpcAddress == "" {
					s.logger.Infof("[Bootstrap] Anonymous Subscription removed (Total=%d).", len(s.subscribers))
				} else {
					s.logger.Infof("[Bootstrap] Subscription removed [%s] (Total=%d).", deadSubscriber.blobServerGrpcAddress, len(s.subscribers))
				}
			}
		}
	}()

	s.logger.Infof("Bootstrap GRPC service listening on %s", address)

	if err = s.grpcServer.Serve(lis); err != nil {
		return fmt.Errorf("GRPC server failed [%w]", err)
	}

	return nil
}

func (s *Server) Stop(ctx context.Context) {
	_, cancel := context.WithCancel(ctx)
	defer cancel()

	s.grpcServer.GracefulStop()
}

func (s *Server) Health(_ context.Context, _ *emptypb.Empty) (*bootstrap_api.HealthResponse, error) {
	return &bootstrap_api.HealthResponse{
		Ok:        true,
		Timestamp: timestamppb.New(time.Now()),
	}, nil
}

// Connect Subscribe to this service and receive updates whenever a peer is added or removed
func (s *Server) Connect(info *bootstrap_api.Info, stream bootstrap_api.BootstrapAPI_ConnectServer) error {
	// Keep this subscription alive without endless loop - use a channel that blocks forever.
	ch := make(chan struct{})

	s.newSubscriptions <- subscriber{
		subscription:          stream,
		blobServerGrpcAddress: info.BlobServerGrpcAddress,
		done:                  ch,
	}

	<-ch

	return nil
}

func safeClose[T any](ch chan T) {
	defer func() {
		_ = recover()
	}()

	close(ch)
}
