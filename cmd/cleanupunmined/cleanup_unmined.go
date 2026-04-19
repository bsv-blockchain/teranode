// Package cleanupunmined removes inconsistent unmined transaction state from
// the UTXO store: conflicting-unmined records and non-conflicting unmined
// records whose parents are mined on an orphaned fork. Run this while the node
// is running with the FSM in IDLE (BlockAssembler parks itself in IDLE when it
// detects such inconsistencies). The command connects to the live blockchain
// gRPC service, so the node process must be up; after the cleanup completes,
// transition the FSM back out of IDLE (e.g. teranode-cli setfsmstate
// --fsmstate RUNNING) to resume block assembly. No node restart is required.
package cleanupunmined

import (
	"context"
	"fmt"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/services/blockassembly"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	utxofactory "github.com/bsv-blockchain/teranode/stores/utxo/factory"
	"github.com/bsv-blockchain/teranode/ulogger"
)

// blockchainAdapter implements utxo.BlockchainQuerier. GetBestBlockHeaderInfo
// uses Block Assembly's persisted CurrentBlock — not the blockchain service's
// best — because BA is the validateParentChain consumer and cleanup must
// classify parents against the exact chain BA will evaluate on its next
// loadUnminedTransactions pass. The two views can drift after a reorg: BA
// persists its tip in the blockchain DB state table and resumes from there
// on startup, while the blockchain service may have advanced or retreated.
// BA's gRPC stays reachable while the FSM is in IDLE (only write entry
// points are gated by frozenForRepair); if we cannot read BA's state, we
// fail rather than silently operate on a mismatched chain view.
type blockchainAdapter struct {
	client   blockchain.ClientI
	baClient blockassembly.ClientI
	logger   ulogger.Logger
}

func (a *blockchainAdapter) GetBestBlockHeaderInfo(ctx context.Context) (utxo.BlockHeaderInfo, error) {
	state, err := a.baClient.GetBlockAssemblyState(ctx)
	if err != nil {
		return utxo.BlockHeaderInfo{}, errors.NewProcessingError("failed to query block assembly state", err)
	}
	if state == nil || state.CurrentHash == "" {
		return utxo.BlockHeaderInfo{}, errors.NewProcessingError("block assembly state missing CurrentHash")
	}
	hash, hashErr := chainhash.NewHashFromStr(state.CurrentHash)
	if hashErr != nil {
		return utxo.BlockHeaderInfo{}, errors.NewProcessingError("block assembly CurrentHash is not a valid hash: %s", state.CurrentHash, hashErr)
	}
	a.logger.Infof("[blockchainAdapter] using Block Assembly's CurrentBlock as best-chain anchor: %s @ height %d",
		state.CurrentHash, state.CurrentHeight)
	return utxo.BlockHeaderInfo{Hash: hash, Height: state.CurrentHeight}, nil
}

func (a *blockchainAdapter) GetBlockHeaderIDs(ctx context.Context, blockHash *chainhash.Hash, numberOfHeaders uint64) ([]uint32, error) {
	return a.client.GetBlockHeaderIDs(ctx, blockHash, numberOfHeaders)
}

// CleanupUnmined removes inconsistent unmined state from the UTXO store:
//   - records with Conflicting=true AND UnminedSince>0
//   - non-conflicting unmined records whose parents are mined on a block that
//     is not on the best chain (orphaned fork)
//
// Run this while the node is up and the FSM is in IDLE — the command dials
// the blockchain gRPC service to fetch header data, so stopping the node first
// will cause startup to fail with a connection error. Block assembly resumes
// automatically once the FSM leaves IDLE.
//
// dryRun=true reports what would be deleted without writing any changes.
// skipUnminedSinceScan=true skips step 0 (the unmined_since fixup pass). Only
// use it when step 0 has run cleanly at least once since the last change to
// the store.
func CleanupUnmined(ctx context.Context, logger ulogger.Logger, tSettings *settings.Settings, dryRun, skipUnminedSinceScan bool) error {
	store, err := utxofactory.NewStore(ctx, logger, tSettings, "CleanupUnmined", false)
	if err != nil {
		return errors.NewConfigurationError("failed to create UTXO store", err)
	}

	blockchainClient, err := blockchain.NewClient(ctx, logger, tSettings, "CleanupUnmined")
	if err != nil {
		return errors.NewConfigurationError("failed to create blockchain client", err)
	}

	// Block Assembly client is required — cleanup classifies orphan-parent
	// children against BA's current chain view, so drift between BA and the
	// blockchain service after a reorg does not cause us to miss or
	// mis-delete records. BA's gRPC stays reachable in IDLE; if we can't
	// dial it, fail loudly.
	baClient, err := blockassembly.NewClient(ctx, logger, tSettings)
	if err != nil {
		return errors.NewConfigurationError("failed to create block assembly client (required for best-chain alignment)", err)
	}

	adapter := &blockchainAdapter{
		client:   blockchainClient,
		baClient: baClient,
		logger:   logger,
	}

	progress := func(format string, args ...interface{}) {
		fmt.Printf(format+"\n", args...)
	}

	report, err := utxo.CleanupUnmined(ctx, store, adapter, dryRun, progress, utxo.CleanupOptions{
		SkipUnminedSinceScan: skipUnminedSinceScan,
	})
	if err != nil {
		return errors.NewProcessingError("cleanup failed", err)
	}

	if dryRun {
		fmt.Println("Dry run — no changes written.")
	}

	fmt.Printf("Cleanup report:\n")
	fmt.Printf("  UnminedSince inconsistencies fixed:       %d\n", report.UnminedSinceFixed)
	fmt.Printf("  Conflicting-unmined records deleted:      %d\n", report.ConflictingUnminedDeleted)
	fmt.Printf("  Orphan-parent unmined children deleted:   %d\n", report.OrphanParentUnminedDeleted)

	return nil
}
