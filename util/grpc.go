package util

import (
	"context"
	"fmt"
	"net"

	"github.com/ordishs/go-utils"
	"github.com/ordishs/gocore"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

func StartGRPCServer(ctx context.Context, logger utils.Logger, serviceName string, register func(server *grpc.Server)) error {
	grpcAddress := fmt.Sprintf("%s_grpcAddress", serviceName)
	address, ok := gocore.Config().Get(grpcAddress)
	if !ok {
		return fmt.Errorf("[%s] no setting %s found", serviceName, grpcAddress)
	}

	grpcServer, err := utils.GetGRPCServer(&utils.ConnectionOptions{
		OpenTracing: gocore.Config().GetBool("use_open_tracing", true),
		Prometheus:  gocore.Config().GetBool("use_prometheus_grpc_metrics", true),
	})
	if err != nil {
		return fmt.Errorf("[%s] could not create GRPC server [%w]", serviceName, err)
	}

	// Register reflection service on gRPC server.
	reflection.Register(grpcServer)

	gocore.SetAddress(address)

	lis, err := net.Listen("tcp", address)
	if err != nil {
		return fmt.Errorf("[%s] GRPC server failed to listen [%w]", serviceName, err)
	}

	register(grpcServer)

	logger.Infof("[%s] GRPC service listening on %s", serviceName, address)

	go func() {
		<-ctx.Done()
		logger.Infof("[%s] GRPC service shutting down", serviceName)
		grpcServer.GracefulStop()
	}()

	if err = grpcServer.Serve(lis); err != nil {
		return fmt.Errorf("[%s] GRPC server failed [%w]", serviceName, err)
	}

	return nil
}
