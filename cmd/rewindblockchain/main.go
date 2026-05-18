// Package main provides a CLI tool that rewinds the blockchain DB, UTXO
// store, and subtree blob storage back to Block Assembly's persisted height.
// It is a repair tool for nodes that have drifted out of sync (UTXO state
// no longer matches the on-disk subtree files and blockchain DB tip).
//
// The node process must be stopped before running this tool; the tool will
// abort unless the FSM state stored in the blockchain DB reads IDLE (or
// --force-not-idle is passed).
package main

import (
	"context"
	"flag"
	"fmt"
	"os"

	"github.com/bsv-blockchain/teranode/cmd/rewindblockchain/rewindblockchain"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/ulogger"
)

func main() {
	var (
		targetHeight int64
		dryRun       bool
		assumeYes    bool
		forceNotIdle bool
		forceDeep    bool
		verify       bool
		concurrency  int
		showVersion  bool
	)

	flag.Int64Var(&targetHeight, "target-height", -1, "Target height to rewind to (default: read state[\"BlockAssembler\"])")
	flag.BoolVar(&dryRun, "dry-run", false, "Log actions but do not modify any store")
	flag.BoolVar(&assumeYes, "assume-yes", false, "Skip interactive confirmation prompt")
	flag.BoolVar(&forceNotIdle, "force-not-idle", false, "Proceed even if FSM is not IDLE (DANGEROUS)")
	flag.BoolVar(&forceDeep, "force-deep", false, "Allow rewind deeper than 100 blocks (coinbase-maturity risk)")
	flag.BoolVar(&verify, "verify", false, "Run post-rewind consistency checks")
	flag.IntVar(&concurrency, "concurrency", 0, "Subtree-load concurrency (0 = use settings.BlockAssembly.MoveBackBlockConcurrency or 4)")
	flag.BoolVar(&showVersion, "version", false, "Print version and exit")

	flag.Usage = func() {
		fmt.Fprintf(flag.CommandLine.Output(), "Usage: %s [flags]\n\n", os.Args[0])
		fmt.Fprintln(flag.CommandLine.Output(), "Rewinds the Teranode blockchain DB, UTXO store, and subtree blob storage")
		fmt.Fprintln(flag.CommandLine.Output(), "back to Block Assembly's persisted height. The node process must be stopped.")
		fmt.Fprintln(flag.CommandLine.Output())
		flag.PrintDefaults()
	}

	flag.Parse()

	if showVersion {
		fmt.Println("rewindblockchain dev")
		return
	}

	logger := ulogger.New("rewindblockchain")
	s := settings.NewSettings()

	ctx := context.Background()

	opts := rewindblockchain.Options{
		TargetHeight: targetHeight,
		DryRun:       dryRun,
		AssumeYes:    assumeYes,
		ForceNotIdle: forceNotIdle,
		ForceDeep:    forceDeep,
		Verify:       verify,
		Concurrency:  concurrency,
		Stdin:        os.Stdin,
		Stdout:       os.Stdout,
	}

	if _, err := rewindblockchain.Rewind(ctx, logger, s, opts); err != nil {
		logger.Errorf("rewind failed: %v", err)
		os.Exit(1)
	}
}
