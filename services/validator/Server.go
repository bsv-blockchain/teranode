package validator

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"time"

	"github.com/TAAL-GmbH/ubsv/services/validator/validator_api"
	"github.com/TAAL-GmbH/ubsv/tracing"
	grpc_prometheus "github.com/grpc-ecosystem/go-grpc-prometheus"
	"github.com/libsv/go-bt/v2"
	"github.com/ordishs/go-utils"
	"github.com/ordishs/gocore"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/reflection"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// Server type carries the logger within it
type Server struct {
	validator_api.UnsafeValidatorAPIServer
	logger     utils.Logger
	grpcServer *grpc.Server
}

func Enabled() bool {
	_, found := gocore.Config().Get("validator_grpcAddress")
	return found
}

// NewServer will return a server instance with the logger stored within it
func NewServer(logger utils.Logger) *Server {
	return &Server{
		logger: logger,
	}
}

// Start function
func (v *Server) Start() error {

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

	v.grpcServer = grpc.NewServer(tracing.AddGRPCServerOptions(opts)...)

	gocore.SetAddress(address)

	lis, err := net.Listen("tcp", address)
	if err != nil {
		return fmt.Errorf("GRPC server failed to listen [%w]", err)
	}

	validator_api.RegisterValidatorAPIServer(v.grpcServer, v)

	// Register reflection service on gRPC server.
	reflection.Register(v.grpcServer)

	v.logger.Infof("GRPC server listening on %s", address)

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

func (v *Server) Health(_ context.Context, _ *emptypb.Empty) (*validator_api.HealthResponse, error) {
	return &validator_api.HealthResponse{
		Ok:        true,
		Timestamp: timestamppb.New(time.Now()),
	}, nil
}

func (v *Server) ValidateTransaction(stream validator_api.ValidatorAPI_ValidateTransactionServer) error {
	transactionData := bytes.Buffer{}

	for {
		log.Print("waiting to receive more data")

		req, err := stream.Recv()
		if err == io.EOF {
			log.Print("no more data")
			break
		}
		if err != nil {
			return v.logError(status.Errorf(codes.Unknown, "cannot receive chunk data: %v", err))
		}

		chunk := req.GetTransactionData()

		_, err = transactionData.Write(chunk)
		if err != nil {
			return v.logError(status.Errorf(codes.Internal, "cannot write chunk data: %v", err))
		}
	}

	var tx bt.Tx
	if _, err := tx.ReadFrom(bytes.NewReader(transactionData.Bytes())); err != nil {
		return v.logError(status.Errorf(codes.Internal, "cannot read transaction data: %v", err))
	}

	return stream.SendAndClose(&validator_api.ValidateTransactionResponse{
		Valid: true,
	})
}

func (v *Server) logError(err error) error {
	if err != nil {
		v.logger.Errorf("%v", err)
	}
	return err
}
