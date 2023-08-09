package grpc_impl

import (
	"context"
	"fmt"
	"net"
	"strings"
	"time"

	blobserver_api "github.com/TAAL-GmbH/ubsv/services/blobserver/blobserver_api"
	"github.com/TAAL-GmbH/ubsv/services/blobserver/repository"
	"github.com/TAAL-GmbH/ubsv/services/blockchain"
	"github.com/ordishs/go-utils"
	"github.com/ordishs/gocore"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
	"google.golang.org/protobuf/types/known/emptypb"

	"google.golang.org/protobuf/types/known/timestamppb"
)

var (
	baseURL string
)

type subscriber struct {
	subscription blobserver_api.BlobServerAPI_SubscribeServer
	source       string
	done         chan struct{}
}

type GRPC struct {
	blobserver_api.UnimplementedBlobServerAPIServer
	logger            utils.Logger
	repository        *repository.Repository
	grpcServer        *grpc.Server
	blockchainClient  blockchain.ClientI
	newSubscriptions  chan subscriber
	deadSubscriptions chan subscriber
	subscribers       map[subscriber]bool
	notifications     chan *blobserver_api.Notification
}

func init() {
	baseURL, _ = gocore.Config().Get("blobserver_baseURL")

	if baseURL == "" {
		remoteAddress, err := utils.GetPublicIPAddress()
		if err != nil {
			panic(err)
		}

		blobServerAddress, _ := gocore.Config().Get("blobserver_httpAddress")

		port := strings.Split(blobServerAddress, ":")[1]

		baseURL = fmt.Sprintf("http://%s:%s", remoteAddress, port)
	}
}

func New(logger utils.Logger, repo *repository.Repository) (*GRPC, error) {
	// TODO: change logger name
	//logger := gocore.Log("b_grpc", logger.GetLogLevel())

	grpcServer, err := utils.GetGRPCServer(&utils.ConnectionOptions{
		OpenTracing: gocore.Config().GetBool("use_open_tracing", true),
		Prometheus:  gocore.Config().GetBool("use_prometheus_grpc_metrics", true),
	})
	if err != nil {
		return nil, fmt.Errorf("could not create GRPC server [%w]", err)
	}

	g := &GRPC{
		logger:            logger,
		repository:        repo,
		grpcServer:        grpcServer,
		newSubscriptions:  make(chan subscriber, 10),
		deadSubscriptions: make(chan subscriber, 10),
		subscribers:       make(map[subscriber]bool),
		notifications:     make(chan *blobserver_api.Notification, 100),
	}

	blobserver_api.RegisterBlobServerAPIServer(grpcServer, g)

	// Register reflection service on gRPC server
	reflection.Register(g.grpcServer)

	return g, nil
}

func (g *GRPC) Init(ctx context.Context) (err error) {
	g.blockchainClient, err = blockchain.NewClient(ctx)
	if err != nil {
		return fmt.Errorf("could not create blockchain client [%w]", err)
	}

	return nil
}

func (g *GRPC) Start(ctx context.Context, addr string) error {
	// Subscribe to the blockchain service
	blockchainSubscription, err := g.blockchainClient.Subscribe(ctx, "blobserver")
	if err != nil {
		return err
	}

	go func() {
		for {
			select {
			case <-ctx.Done():
				g.logger.Infof("BlobServer GRPC service shutting down")
				return
			case notification := <-blockchainSubscription:
				if notification == nil {
					continue
				}

				g.logger.Debugf("Sending %s notification: %s to %d subscribers", blobserver_api.Type(notification.Type).String(), notification.Hash.String(), len(g.subscribers))

				g.notifications <- &blobserver_api.Notification{
					Type:    blobserver_api.Type(notification.Type),
					Hash:    notification.Hash[:],
					BaseUrl: baseURL,
				}
			}
		}
	}()

	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case notification := <-g.notifications:
				for sub := range g.subscribers {
					go func(s subscriber) {
						if err := s.subscription.Send(notification); err != nil {
							g.deadSubscriptions <- s
						}
					}(sub)
				}

			case s := <-g.newSubscriptions:
				g.subscribers[s] = true
				g.logger.Infof("[BlobServer] New Subscription received [%s] (Total=%d).", s.source, len(g.subscribers))

			case s := <-g.deadSubscriptions:
				delete(g.subscribers, s)
				close(s.done)
				g.logger.Infof("[BlobServer] Subscription removed [%s] (Total=%d).", s.source, len(g.subscribers))
			}
		}
	}()

	lis, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("GRPC server failed to listen [%w]", err)
	}

	g.logger.Infof("BlobServer GRPC service listening on %s", addr)

	return g.grpcServer.Serve(lis)
}

func (g *GRPC) Stop(ctx context.Context) error {
	g.grpcServer.GracefulStop()
	return nil
}

func (g *GRPC) Health(_ context.Context, _ *emptypb.Empty) (*blobserver_api.HealthResponse, error) {
	return &blobserver_api.HealthResponse{
		Ok:        true,
		Timestamp: timestamppb.New(time.Now()),
	}, nil
}

func (g *GRPC) Subscribe(req *blobserver_api.SubscribeRequest, sub blobserver_api.BlobServerAPI_SubscribeServer) error {
	// Keep this subscription alive without endless loop - use a channel that blocks forever.
	ch := make(chan struct{})

	g.newSubscriptions <- subscriber{
		subscription: sub,
		source:       req.Source,
		done:         ch,
	}

	<-ch

	return nil
}
