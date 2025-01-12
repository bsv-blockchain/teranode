package util

import (
	"context"
	"fmt"
	"net"
	"time"

	"github.com/bitcoin-sv/teranode/errors"
	"github.com/bitcoin-sv/teranode/settings"
	"github.com/bitcoin-sv/teranode/ulogger"
	"github.com/bitcoin-sv/teranode/util/servicemanager"
	"github.com/ordishs/gocore"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

func StartGRPCServer(ctx context.Context, l ulogger.Logger, tSettings *settings.Settings, serviceName string, grpcListenerAddress string, register func(server *grpc.Server), maxConnectionAge ...time.Duration) error {
	address := grpcListenerAddress

	securityLevel := tSettings.SecurityLevelGRPC

	var certFile, keyFile string

	if securityLevel > 0 {
		certFile = tSettings.ServerCertFile
		if certFile == "" {
			return errors.NewConfigurationError("server_certFile is required for security level %d", securityLevel)
		}

		keyFile = tSettings.ServerKeyFile
		if keyFile == "" {
			return errors.NewConfigurationError("server_keyFile is required for security level %d", securityLevel)
		}
	}

	connectionOptions := &ConnectionOptions{
		SecurityLevel: securityLevel,
		CertFile:      certFile,
		KeyFile:       keyFile,
	}

	if len(maxConnectionAge) > 0 {
		connectionOptions.MaxConnectionAge = maxConnectionAge[0]
	}

	grpcServer, err := getGRPCServer(connectionOptions, tSettings)
	if err != nil {
		return errors.NewConfigurationError("[%s] could not create GRPC server", serviceName, err)
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
		return errors.NewServiceError("[%s] GRPC server failed to listen", serviceName, err)
	}

	register(grpcServer)

	l.Infof("[%s] GRPC service listening on %s", serviceName, address)

	go func() {
		<-ctx.Done()
		l.Infof("[%s] GRPC service shutting down", serviceName)
		grpcServer.GracefulStop()
	}()

	if err = grpcServer.Serve(lis); err != nil {
		return errors.NewServiceError("[%s] GRPC server failed [%w]", serviceName, err)
	}

	return nil
}
