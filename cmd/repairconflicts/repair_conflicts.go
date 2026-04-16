// Package repairconflicts repairs inconsistent conflicting transaction state in the UTXO store.
// Run this while the node is running with the FSM in IDLE (e.g. after BlockAssembler
// transitions to IDLE due to a parent-chain repair request). The command connects to the
// live blockchain gRPC service, so the node process must be up; after the repair completes,
// restart the node (or transition the FSM back out of IDLE) to resume block assembly.
package repairconflicts

import (
	"context"
	"fmt"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	utxofactory "github.com/bsv-blockchain/teranode/stores/utxo/factory"
	"github.com/bsv-blockchain/teranode/ulogger"
)

// blockchainAdapter wraps blockchain.ClientI to implement utxo.BlockchainQuerier.
type blockchainAdapter struct {
	client blockchain.ClientI
}

func (a *blockchainAdapter) GetBestBlockHeaderInfo(ctx context.Context) (utxo.BlockHeaderInfo, error) {
	header, meta, err := a.client.GetBestBlockHeader(ctx)
	if err != nil {
		return utxo.BlockHeaderInfo{}, err
	}
	if header == nil || meta == nil {
		return utxo.BlockHeaderInfo{}, errors.NewProcessingError("GetBestBlockHeader returned nil header or meta")
	}
	return utxo.BlockHeaderInfo{Hash: header.Hash(), Height: meta.Height}, nil
}

func (a *blockchainAdapter) GetBlockHeaderIDs(ctx context.Context, blockHash *chainhash.Hash, numberOfHeaders uint64) ([]uint32, error) {
	return a.client.GetBlockHeaderIDs(ctx, blockHash, numberOfHeaders)
}

// RepairConflicts detects and fixes inconsistent conflicting transaction state in the UTXO store.
// Run this while the node is up and the FSM is in IDLE — the command dials the blockchain gRPC
// service to fetch header data, so stopping the node first will cause startup to fail with a
// connection error. After the repair completes, restart the node to resume block assembly.
// dryRun=true reports issues without writing any changes.
func RepairConflicts(ctx context.Context, logger ulogger.Logger, tSettings *settings.Settings, dryRun bool) error {
	store, err := utxofactory.NewStore(ctx, logger, tSettings, "RepairConflicts", false)
	if err != nil {
		return errors.NewConfigurationError("failed to create UTXO store", err)
	}

	blockchainClient, err := blockchain.NewClient(ctx, logger, tSettings, "RepairConflicts")
	if err != nil {
		return errors.NewConfigurationError("failed to create blockchain client", err)
	}

	adapter := &blockchainAdapter{client: blockchainClient}

	progress := func(format string, args ...interface{}) {
		fmt.Printf(format+"\n", args...)
	}

	report, err := utxo.RepairConflictingChains(ctx, store, adapter, dryRun, progress)
	if err != nil {
		return errors.NewProcessingError("repair failed", err)
	}

	if dryRun {
		fmt.Println("Dry run — no changes written.")
	}

	fmt.Printf("Repair report:\n")
	fmt.Printf("  UnminedSince inconsistencies fixed: %d\n", report.UnminedSinceFixed)
	fmt.Printf("  Case A (loser not marked) fixed:    %d\n", report.CaseAFixed)
	fmt.Printf("  Case C (inverted winner/loser) fixed: %d\n", report.CaseCFixed)

	if len(report.Errors) > 0 {
		fmt.Printf("  Non-fatal errors encountered: %d\n", len(report.Errors))
		for _, e := range report.Errors {
			fmt.Printf("    - %v\n", e)
		}
	}

	return nil
}
