package utxostore

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/TAAL-GmbH/ubs/tracing"
	"github.com/TAAL-GmbH/ubs/utxostore/utxostore_api"
	"github.com/TAAL-GmbH/ubs/validator/validator_api"
	grpc_prometheus "github.com/grpc-ecosystem/go-grpc-prometheus"
	"github.com/ordishs/go-utils"
	"github.com/ordishs/gocore"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

var (
	empty = [32]byte{}
)

// Server type carries the logger within it
type Server struct {
	validator_api.UnsafeValidatorAPIServer
	logger     utils.Logger
	grpcServer *grpc.Server
	mu         sync.Mutex
	store      map[[32]byte][32]byte
}

// NewServer will return a server instance with the logger stored within it
func NewServer(logger utils.Logger) *Server {
	return &Server{
		logger: logger,
		store:  make(map[[32]byte][32]byte),
	}
}

// StartGRPCServer function
func (s *Server) StartGRPCServer() error {

	address, ok := gocore.Config().Get("validator_grpcAddress") //, "localhost:8001")
	if !ok {
		return errors.New("no validator_grpcAddress setting found")
	}

	// LEVEL 0 - no security / no encryption
	var opts []grpc.ServerOption
	_, prometheusOn := gocore.Config().Get("prometheusEndpoint")
	if prometheusOn {
		opts = append(opts,
			grpc.ChainStreamInterceptor(grpc_prometheus.StreamServerInterceptor),
			grpc.ChainUnaryInterceptor(grpc_prometheus.UnaryServerInterceptor),
		)
	}

	s.grpcServer = grpc.NewServer(tracing.AddGRPCServerOptions(opts)...)

	gocore.SetAddress(address)

	lis, err := net.Listen("tcp", address)
	if err != nil {
		return fmt.Errorf("GRPC server failed to listen [%w]", err)
	}

	validator_api.RegisterValidatorAPIServer(s.grpcServer, s)

	// Register reflection service on gRPC server.
	reflection.Register(s.grpcServer)

	s.logger.Infof("GRPC server listening on %s", address)

	if err = s.grpcServer.Serve(lis); err != nil {
		return fmt.Errorf("GRPC server failed [%w]", err)
	}

	return nil
}

func (s *Server) Health(_ context.Context, _ *emptypb.Empty) (*validator_api.HealthResponse, error) {
	return &validator_api.HealthResponse{
		Ok:        true,
		Timestamp: timestamppb.New(time.Now()),
	}, nil
}

func (s *Server) Store(_ context.Context, req *utxostore_api.StoreRequest) (*utxostore_api.StoreResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	spendingTxID, found := s.store[[32]byte(req.UxtoHash)]
	if found {
		if compareHash(spendingTxID, empty) {
			return &utxostore_api.StoreResponse{
				Status: utxostore_api.Status_OK,
			}, nil
		}

		return &utxostore_api.StoreResponse{
			Status: utxostore_api.Status_SPENT,
		}, nil

	}

	s.store[[32]byte(req.UxtoHash)] = empty

	return &utxostore_api.StoreResponse{
		Status: utxostore_api.Status_OK,
	}, nil
}

func (s *Server) Spend(_ context.Context, req *utxostore_api.SpendRequest) (*utxostore_api.SpendResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	spendingTxID, found := s.store[[32]byte(req.UxtoHash)]
	if found {
		if compareHash(spendingTxID, empty) {
			s.store[[32]byte(req.UxtoHash)] = [32]byte(req.SpendingTxid)

			return &utxostore_api.SpendResponse{
				Status: utxostore_api.Status_OK,
			}, nil
		}

		if compareHash(spendingTxID, [32]byte(req.SpendingTxid)) {
			return &utxostore_api.SpendResponse{
				Status: utxostore_api.Status_OK,
			}, nil
		}
	}
	return &utxostore_api.SpendResponse{
		Status:       utxostore_api.Status_SPENT,
		SpendingTxid: spendingTxID[:],
	}, nil
}

func (s *Server) Reset(_ context.Context, req *utxostore_api.ResetRequest) (*utxostore_api.ResetResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	spendingTxID, found := s.store[[32]byte(req.UxtoHash)]
	if found {
		if !compareHash(spendingTxID, empty) {
			s.store[[32]byte(req.UxtoHash)] = empty
		}

		return &utxostore_api.ResetResponse{
			Status: utxostore_api.Status_OK,
		}, nil
	}

	return &utxostore_api.ResetResponse{
		Status: utxostore_api.Status_NOT_FOUND,
	}, nil
}

func compareHash(a, b [32]byte) bool {
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}

	return true
}
