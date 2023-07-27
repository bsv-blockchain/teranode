package bootstrap

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync"
	"time"

	bootstrap_api "github.com/TAAL-GmbH/ubsv/services/bootstrap/bootstrap_api"
	"github.com/ordishs/go-utils"
	"github.com/ordishs/gocore"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type Peer struct {
	ID      string
	Address string
}

// Server type carries the logger within it
type Server struct {
	bootstrap_api.UnimplementedBootstrapAPIServer
	logger     utils.Logger
	Peers      map[string]*Peer
	mu         sync.RWMutex
	grpcServer *grpc.Server
}

func Enabled() bool {
	_, found := gocore.Config().Get("bootstrap_grpcAddress")
	return found
}

// NewServer will return a server instance with the logger stored within it
func NewServer() *Server {
	return &Server{
		logger: gocore.Log("boots"),
		Peers:  make(map[string]*Peer),
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

	v.logger.Infof("Bootstrap GRPC service listening on %s", address)

	if err = v.grpcServer.Serve(lis); err != nil {
		return fmt.Errorf("GRPC server failed [%w]", err)
	}

	return nil
}

func (v *Server) Stop(ctx context.Context) {
	_, cancel := context.WithCancel(ctx)
	defer cancel()

	v.grpcServer.GracefulStop()
}

func (v *Server) Health(_ context.Context, _ *emptypb.Empty) (*bootstrap_api.HealthResponse, error) {
	return &bootstrap_api.HealthResponse{
		Ok:        true,
		Timestamp: timestamppb.New(time.Now()),
	}, nil
}

func (s *Server) GetPeers(ctx context.Context, in *emptypb.Empty) (*bootstrap_api.PeerList, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	pl := &bootstrap_api.PeerList{}

	for _, peer := range s.Peers {
		pl.Peers = append(pl.Peers, &bootstrap_api.Peer{
			Id:      peer.ID,
			Address: peer.Address,
		})
	}

	return pl, nil
}

func (s *Server) AddPeer(ctx context.Context, peer *bootstrap_api.Peer) (*emptypb.Empty, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.Peers[peer.Id] = &Peer{
		ID:      peer.Id,
		Address: peer.Address,
	}

	return &emptypb.Empty{}, nil
}
