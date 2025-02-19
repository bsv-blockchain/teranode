//go:build !test_all

package main

import (
	"fmt"
	"log/slog"
	"net"

	"github.com/bitcoin-sv/teranode/test/utils/tconfig"
	"github.com/bitcoin-sv/teranode/test/utils/tstore"
	"github.com/bitcoin-sv/teranode/ulogger"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

// To start the TStore server
//
//	go run test/utils/cmd/tstore/main.go --tconfig-file=./test/utils/cmd/tstore/localhost.env
func main() {
	tconfig := tconfig.LoadTConfig(nil, tconfig.LoadConfigLocalSystem())
	tconfigYAML := tconfig.StringYAML()
	fmt.Printf("\n###  Config for tstore server  ###\n\n%v", tconfigYAML)

	logger := ulogger.New("tstore-server", ulogger.WithLevel("DEBUG"))
	tstoreService := tstore.NewTStoreService(tconfig.LocalSystem.TStoreRootDir, logger)

	// Listen on a specific port.
	lis, err := net.Listen("tcp", tconfig.LocalSystem.TStoreURL)
	if err != nil {
		slog.Error("Failed to listen %v %v", tconfig.LocalSystem.TStoreURL, err)
	}

	slog.Info("gRPC server listening on", "TStoreURL", tconfig.LocalSystem.TStoreURL)

	// Create a new gRPC server.
	grpcServer := grpc.NewServer()

	// Register the FileSystem service with the server.
	tstore.RegisterTStoreServer(grpcServer, tstoreService)
	reflection.Register(grpcServer)

	// Start serving.
	if err := grpcServer.Serve(lis); err != nil {
		slog.Error("Failed to serve", "error", err)
	}
}
