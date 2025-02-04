//go:build !test_all

package main

import (
	"context"
	"fmt"
	"log/slog"
	"net/url"
	"os"
	"strconv"

	"github.com/bitcoin-sv/teranode/stores/blob"
	"github.com/bitcoin-sv/teranode/stores/blob/options"
	"github.com/bitcoin-sv/teranode/ulogger"
)

// To start the blob server
//
//	BSERVER_URL="localhost:8080" BSERVER_SUBDIR=foo BSERVER_HASHPREFIX=2 go run test/utils/cmd/blob/server/main.go
func main() {
	slog.Info("Mini program to start a blob http server")
	slog.Info("Use environment variables BSERVER_URL BSERVER_SUBDIR BSERVER_HASHPREFIX to configure the server")

	serverURL := os.Getenv("BSERVER_URL")
	subdir := os.Getenv("BSERVER_SUBDIR")
	hPrefixStr := os.Getenv("BSERVER_HASHPREFIX")

	// Default serverURL if not set
	if len(serverURL) < 1 {
		serverURL = "localhost:8080"
	}

	// Default subdir if not set
	if len(subdir) < 1 {
		subdir = "default_blob_dir"
	}

	// Default prefixLength if not set
	prefixLength := 0

	if len(hPrefixStr) > 0 {
		if v, err := strconv.Atoi(hPrefixStr); err == nil {
			prefixLength = v
		} else {
			slog.Default().Warn("bad value of", "BSERVER_HASHPREFIX", hPrefixStr)
		}
	}

	// Create a blob server, with relative path, and eventually with hash prefix
	logger := ulogger.New("test-blob-server")
	serverStoreURL, err := url.Parse(fmt.Sprintf("file://./%s", subdir))

	if err != nil {
		slog.Default().Error("failed to parse relative path", "subdir", subdir)
		panic(err)
	}

	slog.Info("Creating blob server", "BSERVER_URL", serverURL, "BSERVER_SUBDIR", subdir, "BSERVER_HASHPREFIX", hPrefixStr)

	var blobServer *blob.HTTPBlobServer
	if prefixLength > 0 {
		blobServer, err = blob.NewHTTPBlobServer(logger, serverStoreURL, options.WithHashPrefix(prefixLength))
	} else {
		blobServer, err = blob.NewHTTPBlobServer(logger, serverStoreURL)
	}

	if err != nil || blobServer == nil {
		slog.Default().Error("failed to create blob server")
		panic(err)
	}

	if err := blobServer.Start(context.Background(), serverURL); err != nil {
		slog.Default().Error("failed to start blob server")
		panic(err)
	}
}
