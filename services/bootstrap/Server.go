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
)

type void struct{}

type subscriber struct {
	subscription  bootstrap_api.BootstrapAPI_ConnectServer
	done          chan struct{}
	localAddress  string
	remoteAddress string
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
func (v *Server) Start() error {
	address, ok := gocore.Config().Get("bootstrap_grpcAddress")
	if !ok {
		return errors.New("no bootstrap_grpcAddress setting found")
	}

	var err error
	v.grpcServer, err = utils.GetGRPCServer(&utils.ConnectionOptions{
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

	bootstrap_api.RegisterBootstrapAPIServer(v.grpcServer, v)

	// Register reflection service on gRPC server.
	reflection.Register(v.grpcServer)

	ticker := time.NewTicker(10 * time.Second)

	go func() {
		for {
			select {
			case <-ticker.C:
				for sub := range v.subscribers {
					go func(s subscriber) {
						if err := s.subscription.Send(&bootstrap_api.Notification{
							Type: bootstrap_api.Type_PING,
						}); err != nil {
							v.deadSubscriptions <- s
						}
					}(sub)
				}

			case notification := <-v.notifications:
				for sub := range v.subscribers {
					go func(s subscriber) {
						if err := s.subscription.Send(notification); err != nil {
							v.deadSubscriptions <- s
						}
					}(sub)
				}

			case newSubscriber := <-v.newSubscriptions:
				// Send this new subscription a list of the currently known subscribers
				for sub := range v.subscribers {
					go func(s subscriber) {
						if err := newSubscriber.subscription.Send(&bootstrap_api.Notification{
							Type: bootstrap_api.Type_ADD,
							Info: &bootstrap_api.Info{
								LocalAddress:  s.localAddress,
								RemoteAddress: s.remoteAddress,
							},
						}); err != nil {
							v.deadSubscriptions <- s
						}
					}(sub)
				}

				// Notify all the other subscribers of the new subscription
				for sub := range v.subscribers {
					go func(s subscriber) {
						if err := s.subscription.Send(&bootstrap_api.Notification{
							Type: bootstrap_api.Type_ADD,
							Info: &bootstrap_api.Info{
								LocalAddress:  newSubscriber.localAddress,
								RemoteAddress: newSubscriber.remoteAddress,
							},
						}); err != nil {
							v.deadSubscriptions <- s
						}
					}(sub)
				}

				// Add the newSubscriber to our map
				v.subscribers[newSubscriber] = void{}

				v.logger.Infof("[Bootstrap] New Subscription received (Total=%d).", len(v.subscribers))

			case deadSubscriber := <-v.deadSubscriptions:
				delete(v.subscribers, deadSubscriber)
				close(deadSubscriber.done)

				// Notify all the remaining subscription of the removed subscription
				for sub := range v.subscribers {
					go func(s subscriber) {
						if err := s.subscription.Send(&bootstrap_api.Notification{
							Type: bootstrap_api.Type_REMOVE,
							Info: &bootstrap_api.Info{
								LocalAddress:  deadSubscriber.localAddress,
								RemoteAddress: deadSubscriber.remoteAddress,
							},
						}); err != nil {
							v.deadSubscriptions <- s
						}
					}(sub)
				}

				v.logger.Infof("[Bootstrap] Subscription removed (Total=%d).", len(v.subscribers))
			}
		}
	}()

	v.logger.Infof("Bootstrap GRPC service listening on %s", address)

	if err = v.grpcServer.Serve(lis); err != nil {
		return fmt.Errorf("GRPC server failed [%w]", err)
	}

	return nil
}

func (s *Server) Stop(ctx context.Context) {
	_, cancel := context.WithCancel(ctx)
	defer cancel()

	s.grpcServer.GracefulStop()
}

// Subscribe to this service and receive updates whenever a peer is added or removed
func (s *Server) Connect(info *bootstrap_api.Info, stream bootstrap_api.BootstrapAPI_ConnectServer) error {
	// Keep this subscription alive without endless loop - use a channel that blocks forever.
	ch := make(chan struct{})

	s.newSubscriptions <- subscriber{
		subscription:  stream,
		localAddress:  info.LocalAddress,
		remoteAddress: info.RemoteAddress,
		done:          ch,
	}

	<-ch

	return nil
}
