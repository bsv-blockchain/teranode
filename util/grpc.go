package util

import (
	"context"
	"fmt"
	"net"
	"time"

	"github.com/bitcoin-sv/ubsv/ulogger"
	"github.com/bitcoin-sv/ubsv/util/servicemanager"
	"github.com/ordishs/gocore"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

func StartGRPCServer(ctx context.Context, l ulogger.Logger, serviceName string, register func(server *grpc.Server), maxConnectionAge ...time.Duration) error {
	grpcAddress := fmt.Sprintf("%s_grpcListenAddress", serviceName)
	address, ok := gocore.Config().Get(grpcAddress)
	if !ok {
		return fmt.Errorf("[%s] no setting %s found", serviceName, grpcAddress)
	}

	securityLevel, _ := gocore.Config().GetInt("securityLevelGRPC", 0)

	var certFile, keyFile string

	if securityLevel > 0 {
		var found bool

		certFile, found = gocore.Config().Get("server_certFile")
		if !found {
			return fmt.Errorf("server_certFile is required for security level %d", securityLevel)
		}
		keyFile, found = gocore.Config().Get("server_keyFile")
		if !found {
			return fmt.Errorf("server_keyFile is required for security level %d", securityLevel)
		}
	}

	connectionOptions := &ConnectionOptions{
		OpenTracing:   gocore.Config().GetBool("use_open_tracing", true),
		Prometheus:    gocore.Config().GetBool("use_prometheus_grpc_metrics", true),
		SecurityLevel: securityLevel,
		CertFile:      certFile,
		KeyFile:       keyFile,
	}

	if len(maxConnectionAge) > 0 {
		connectionOptions.MaxConnectionAge = maxConnectionAge[0]
	}

	grpcServer, err := getGRPCServer(connectionOptions)
	if err != nil {
		return fmt.Errorf("[%s] could not create GRPC server [%w]", serviceName, err)
	}

	// Register reflection service on gRPC server.
	reflection.Register(grpcServer)

	gocore.SetAddress(address)

	if securityLevel == 0 {
		servicemanager.AddListenerInfo(fmt.Sprintf("%s GRPC listening on %s", serviceName, address))
	} else {
		servicemanager.AddListenerInfo(fmt.Sprintf("%s GRPCS listening on %s", serviceName, address))
	}

	lis, err := net.Listen("tcp", address)
	if err != nil {
		return fmt.Errorf("[%s] GRPC server failed to listen [%w]", serviceName, err)
	}

	register(grpcServer)

	l.Infof("[%s] GRPC service listening on %s", serviceName, address)

	go func() {
		<-ctx.Done()
		l.Infof("[%s] GRPC service shutting down", serviceName)
		grpcServer.GracefulStop()
	}()

	if err = grpcServer.Serve(lis); err != nil {
		return fmt.Errorf("[%s] GRPC server failed [%w]", serviceName, err)
	}

	return nil
}
