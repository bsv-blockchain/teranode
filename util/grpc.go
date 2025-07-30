package util

import (
	"context"
	"fmt"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/bitcoin-sv/teranode/errors"
	"github.com/bitcoin-sv/teranode/settings"
	"github.com/bitcoin-sv/teranode/ulogger"
	"github.com/bitcoin-sv/teranode/util/servicemanager"
	"github.com/ordishs/gocore"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

// AuthOptions contains configuration for API authentication
type AuthOptions struct {
	// API key for authentication
	APIKey string

	// Map of method names that require authentication
	ProtectedMethods map[string]bool
}

func StartGRPCServer(ctx context.Context, l ulogger.Logger, tSettings *settings.Settings, serviceName string, grpcListenerAddress string, register func(server *grpc.Server), authOptions *AuthOptions, maxConnectionAge ...time.Duration) error {
	listener, address, _, err := GetListener(tSettings.Context, serviceName, "", grpcListenerAddress)
	if err != nil {
		return errors.NewServiceError("[%s] GRPC server failed to listen", serviceName, err)
	}

	defer RemoveListener(tSettings.Context, serviceName, "")

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

	// Create server options
	var serverOptions []grpc.ServerOption

	// Add authentication interceptor if auth options are provided
	if authOptions != nil && authOptions.APIKey != "" {
		authInterceptor := CreateAuthInterceptor(authOptions.APIKey, authOptions.ProtectedMethods)
		serverOptions = append(serverOptions, grpc.UnaryInterceptor(authInterceptor))
	}

	connectionOptions := &ConnectionOptions{
		SecurityLevel: securityLevel,
		CertFile:      certFile,
		KeyFile:       keyFile,
	}

	if len(maxConnectionAge) > 0 {
		connectionOptions.MaxConnectionAge = maxConnectionAge[0]
	}

	grpcServer, err := getGRPCServer(connectionOptions, serverOptions, tSettings)
	if err != nil {
		return errors.NewConfigurationError("[%s] could not create GRPC server", serviceName, err)
	}

	// Register reflection service on gRPC server.
	reflection.Register(grpcServer)

	if securityLevel == 0 {
		servicemanager.AddListenerInfo(fmt.Sprintf("%s GRPC listening on %s", serviceName, address))
	} else {
		servicemanager.AddListenerInfo(fmt.Sprintf("%s GRPCS listening on %s", serviceName, address))
	}

	register(grpcServer)

	l.Infof("[%s] GRPC service listening on %s", serviceName, address)

	go func() {
		<-ctx.Done()
		l.Infof("[%s] GRPC service shutting down", serviceName)
		grpcServer.GracefulStop()
	}()

	if err = grpcServer.Serve(listener); err != nil {
		return errors.NewServiceError("[%s] GRPC server failed [%w]", serviceName, err)
	}

	return nil
}

var listeners sync.Map

func GetListener(settingsContext string, serviceName string, schema string, listenerAddress string) (net.Listener, string, string, error) {
	key := listenerKey(settingsContext, serviceName, schema)

	if val, ok := listeners.Load(key); ok {
		lis, ok := val.(net.Listener)
		if !ok {
			return nil, "", "", errors.NewServiceError("[%s] Invalid listener type stored in map", serviceName)
		}
		listenAddress, clientAddress := addresses(schema, lis)
		return lis, listenAddress, clientAddress, nil
	}

	lis, err := net.Listen("tcp", listenerAddress)
	if err != nil {
		return nil, "", "", errors.NewServiceError("[%s] failed to start a new listener", serviceName, err)
	}

	listenAddress, clientAddress := addresses(schema, lis)

	gocore.SetAddress(listenAddress)

	listeners.Store(key, lis)

	return lis, listenAddress, clientAddress, nil
}

func RemoveListener(settingsContext string, serviceName string, schema string) {
	key := listenerKey(settingsContext, serviceName, schema)

	if val, ok := listeners.Load(key); ok {
		lis, ok := val.(net.Listener)
		if ok {
			lis.Close()
		}
		listeners.Delete(key)
	}
}

func CleanupListeners(settingsContext string) []string {
	var keys []string
	listeners.Range(func(k, v interface{}) bool {
		if !strings.Contains(k.(string), settingsContext) {
			return true
		}

		if lis, ok := v.(net.Listener); ok {
			lis.Close()
		}

		keys = append(keys, k.(string))
		return true
	})

	for _, key := range keys {
		listeners.Delete(key)
	}

	return keys
}

func listenerKey(settingsContext string, serviceName string, schema string) string {
	// take off :// from the end of the schema string if it is there
	if strings.HasSuffix(schema, "://") {
		schema = schema[:len(schema)-3]
	}

	return fmt.Sprintf("%s!%s!%s", settingsContext, serviceName, schema)
}

func addresses(schema string, listener net.Listener) (string, string) {
	listenAddress := listener.Addr().String()
	clientAddress := listenAddress

	if tcpAddr, ok := listener.Addr().(*net.TCPAddr); ok {
		if tcpAddr.IP == nil || tcpAddr.IP.IsUnspecified() {
			listenAddress = fmt.Sprintf("0.0.0.0:%d", tcpAddr.Port)

			// the schema may be empty (in the case of grpc) or it may be "http://" or "https://"
			clientAddress = fmt.Sprintf("%slocalhost:%d", schema, tcpAddr.Port)
		}
	}

	return listenAddress, clientAddress
}
