// Package main provides a CLI tool for analyzing scheduled blob deletions and their associated blocks.
package main

import (
	"context"
	"flag"
	"os"

	"github.com/bsv-blockchain/teranode/cmd/subtreeanalyzer/subtreeanalyzer"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/ulogger"
)

func main() {
	flag.Parse()

	logger := ulogger.New("subtreeanalyzer")
	s := settings.NewSettings()

	ctx := context.Background()

	if err := subtreeanalyzer.Analyze(ctx, logger, s); err != nil {
		logger.Fatalf("analysis failed: %v", err)
		os.Exit(1)
	}
}
