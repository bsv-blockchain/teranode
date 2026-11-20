//go:build !test_all

package main

import (
	"context"
	"fmt"
	"log/slog"
	"net/url"
	"os"
	"strconv"

	"github.com/bsv-blockchain/teranode/stores/blob"
	"github.com/bsv-blockchain/teranode/stores/blob/options"
	"github.com/bsv-blockchain/teranode/ulogger"
)

// To start the blob server
//
//	BSERVER_URL="localhost:8080" BSERVER_SUBDIR=foo BSERVER_HASHPREFIX=2 go run test/utils/cmd/blob/server/main.go
func main() {
	slog.Info("Mini program to start a blob http server")
	slog.Info("Use environment variables BSERVER_URL BSERVER_SUBDIR BSERVER_HASHPREFIX BSERVER_TOKEN to configure the server")

	serverURL := os.Getenv("BSERVER_URL")
	subdir := os.Getenv("BSERVER_SUBDIR")
	hPrefixStr := os.Getenv("BSERVER_HASHPREFIX")
	logLevel := os.Getenv("BSERVER_LOGLEVEL")

	// Shared secret required on POST, PATCH and DELETE. Empty leaves the server read-only.
	authToken := os.Getenv("BSERVER_TOKEN")

	// Default serverURL if not set
	if len(serverURL) < 1 {
		serverURL = ":8080"
	}

	// Default subdir if not set
	if len(subdir) < 1 {
		subdir = "default_blob_dir"
	}

	// Default logLevel if not set
	if len(logLevel) < 1 {
		logLevel = "DEBUG"
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
	logger := ulogger.New("test-blob-server", ulogger.WithLevel(logLevel))
	serverStoreURL, err := url.Parse(fmt.Sprintf("file://./%s", subdir))

	if err != nil {
		slog.Default().Error("failed to parse relative path", "subdir", subdir)
		panic(err)
	}

	slog.Info("Creating blob server", "BSERVER_URL", serverURL, "BSERVER_SUBDIR", subdir, "BSERVER_HASHPREFIX", hPrefixStr)

	var blobServer *blob.HTTPBlobServer
	if prefixLength > 0 {
		blobServer, err = blob.NewHTTPBlobServer(logger, serverStoreURL, authToken, options.WithHashPrefix(prefixLength))
	} else {
		blobServer, err = blob.NewHTTPBlobServer(logger, serverStoreURL, authToken)
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
