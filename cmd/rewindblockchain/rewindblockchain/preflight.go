package rewindblockchain

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"strings"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/services/blockassembly"
)

// preflightResult holds everything the phases need to know up-front.
type preflightResult struct {
	target      uint32
	tip         uint32
	targetHash  *chainhash.Hash
	deleteList  []blockToDelete
	deleteByID  map[uint32]struct{}
	deleteByHsh map[chainhash.Hash]struct{}

	// persisterHeight is state["BlockPersisterHeight"] as read before any
	// mutation, and persisterHeightKnown says whether it decoded. Phase 3
	// refuses to raise it; Phase 4 asserts it never went up.
	persisterHeight      uint32
	persisterHeightKnown bool

	// utxoMarkerStale is true when the UTXO persister's lastProcessed marker is
	// above the target or unparseable — the states Phase 3 resets under
	// --reset-utxo-persister (both force a rebuild from genesis). The confirm
	// prompt keys its reset line on this, not on utxoMarkerHash, since the
	// unparseable and unresolved cases reset the persister with no hash to show.
	utxoMarkerStale bool

	// utxoMarkerHash is the hash of the main-chain block at the marker's height,
	// resolved via GetBlockHeadersByHeight while that block is still on the main
	// chain (nil when the marker is at/below target, unreadable, unparseable, or
	// its height cannot be resolved). Phase 3 uses it to clean up the stale
	// utxo-set files when --reset-utxo-persister deletes the marker. Resolving
	// through the main chain, not deleteList, avoids picking a fork block that
	// shares the marker height (deleteList spans all branches, ordered by id).
	utxoMarkerHash *chainhash.Hash
}

// blockToDelete is one row from the enumeration query, preserving processing
// order (descending height, descending id).
type blockToDelete struct {
	id     uint32
	hash   *chainhash.Hash
	height uint32
}

// preflight validates gates, resolves the target height, and enumerates the
// block rows that will be deleted.
func (e *env) preflight(ctx context.Context) (*preflightResult, error) {
	// 1. FSM state check.
	fsmState, err := e.blockchainStore.GetFSMState(ctx)
	if err != nil {
		return nil, errors.NewStorageError("failed to read FSM state", err)
	}

	normalised := strings.ToUpper(strings.TrimSpace(fsmState))
	if normalised != "IDLE" && normalised != "" {
		if !e.opts.ForceNotIdle {
			return nil, errors.NewProcessingError("FSM state is %q, expected IDLE; pass --force-not-idle to override", fsmState)
		}
		e.logger.Warnf("FSM state is %q but --force-not-idle given; proceeding anyway", fsmState)
	}

	// 2. Resolve target height.
	target, targetHash, err := e.resolveTarget(ctx)
	if err != nil {
		return nil, err
	}

	// 3. Read current tip.
	_, tipMeta, err := e.blockchainStore.GetBestBlockHeader(ctx)
	if err != nil {
		return nil, errors.NewStorageError("failed to read best block", err)
	}

	tip := tipMeta.Height

	e.logger.Infof("current tip height=%d; target height=%d", tip, target)

	if tip < target {
		return nil, errors.NewProcessingError("current tip %d is below requested target %d; refusing", tip, target)
	}

	// 4. Depth guard.
	if tip-target > coinbaseMaturity && !e.opts.ForceDeep {
		return nil, errors.NewProcessingError("rewind depth %d exceeds coinbase maturity (%d); pass --force-deep to override", tip-target, coinbaseMaturity)
	}

	// 5. Capture the pre-run block persister height. Reading it here means a
	// genuine storage failure aborts before Phase 0 mutates anything.
	persisterHeight, persisterHeightKnown, err := e.readBlockPersisterHeight(ctx)
	if err != nil {
		return nil, err
	}

	switch {
	case !persisterHeightKnown:
		e.logger.Infof("state[%s] not set or undecodable; it will be left untouched", blockPersisterHeightKey)
	case persisterHeight <= target:
		e.logger.Infof("state[%s]=%d (target %d); it will be left untouched", blockPersisterHeightKey, persisterHeight, target)
	default:
		e.logger.Infof("state[%s]=%d (target %d); it will be rewritten down to the target", blockPersisterHeightKey, persisterHeight, target)
	}

	e.warnOnPersisterAnomalies(ctx, target, persisterHeight, persisterHeightKnown)

	// 6. Enumerate blocks above target.
	deleteList, err := e.enumerateBlocksAboveTarget(ctx, target)
	if err != nil {
		return nil, err
	}

	byID := make(map[uint32]struct{}, len(deleteList))
	byHash := make(map[chainhash.Hash]struct{}, len(deleteList))
	for _, b := range deleteList {
		byID[b.id] = struct{}{}
		byHash[*b.hash] = struct{}{}
	}

	// 6b. Read and report the UTXO persister's lastProcessed marker now, before
	// any destructive phase — the operator needs the plan before confirming, not
	// after Phase 2. Resolve the marker block's hash through the main chain while
	// it is still there, so Phase 3 can clean up its stale utxo-set files.
	utxoMarkerStale, utxoMarkerHash := e.reportUTXOPersisterPlan(ctx, target)

	res := &preflightResult{
		target:               target,
		tip:                  tip,
		targetHash:           targetHash,
		deleteList:           deleteList,
		deleteByID:           byID,
		deleteByHsh:          byHash,
		persisterHeight:      persisterHeight,
		persisterHeightKnown: persisterHeightKnown,
		utxoMarkerStale:      utxoMarkerStale,
		utxoMarkerHash:       utxoMarkerHash,
	}

	// 7. Interactive confirmation.
	if !e.opts.AssumeYes && !e.opts.DryRun {
		if err = e.confirmPrompt(res); err != nil {
			return nil, err
		}
	}

	return res, nil
}

// warnOnPersisterAnomalies cross-checks the state key against the persister's
// real position and speaks up about the two states an operator needs to know
// before confirming a destructive run.
//
// The tool cannot repair either of them: this guard only stops it from creating
// the first, and the second is what made someone reach for a rewind in the first
// place. Both are diagnostics — never fatal.
func (e *env) warnOnPersisterAnomalies(ctx context.Context, target, persisterHeight uint32, persisterHeightKnown bool) {
	realHeight, realKnown := e.blockPersisterGroundTruth(ctx)
	if !realKnown {
		return
	}

	// An inflated key: the blocks table says the persister is at realHeight, the
	// key claims higher. Nothing legitimate writes a value above ground truth,
	// so this is the fingerprint of a pre-fix run of this tool.
	if persisterHeightKnown && persisterHeight > realHeight {
		e.logger.Warnf("state[%s]=%d is ahead of the real block persister position %d (lowest unpersisted block is %d): the value was inflated by an earlier run of this tool (issue 1340) or another writer, and this run cannot repair it — delete the state row and let block persister republish it on next start",
			blockPersisterHeightKey, persisterHeight, realHeight, realHeight+1)
	}

	// A large lag: the rewind will not move where the persister resumes, and the
	// pruner stays gated there until it catches up.
	if target > realHeight && target-realHeight > coinbaseMaturity {
		e.logger.Warnf("block persister is %d blocks behind the target (real position %d, target %d): it will resume from %d after the rewind and the pruner stays gated there",
			target-realHeight, realHeight, target, realHeight+1)
	}
}

// reportUTXOPersisterPlan reads the UTXO persister's lastProcessed marker and
// logs what Phase 3 will do with it — before the destructive phases run, so the
// operator sees "the persister will rebuild from genesis" (or "will be left as
// is") at confirmation time rather than after Phase 2. It returns:
//   - stale: whether the marker is above the target OR unparseable, the states
//     Phase 3 resets under --reset-utxo-persister (both force a genesis rebuild).
//     The confirm prompt keys its reset line on this.
//   - hash: the main-chain block at the marker's height, resolved via
//     GetBlockHeadersByHeight (as the persister itself resolves heights) while
//     the block is still on the main chain, so Phase 3 can clean up its stale
//     utxo-set files; nil when the marker is at/below target, unreadable,
//     unparseable, or its height can't be resolved (e.g. above the tip).
//
// Resolving through the main chain rather than deleteList matters: deleteList
// spans all branches ordered by (height DESC, id DESC), so a fork block sharing
// the marker height could be picked and the cleanup would target the wrong
// hash, leaving the real set behind. Diagnostics only — never fatal.
func (e *env) reportUTXOPersisterPlan(ctx context.Context, target uint32) (bool, *chainhash.Hash) {
	height, known, parseErr := e.readUTXOPersisterMarker(ctx)

	switch {
	case parseErr:
		if e.opts.ResetUTXOPersister {
			e.logger.Warnf("utxo-persister lastProcessed marker is present but unparseable; --reset-utxo-persister given, Phase 3 will delete it so the persister rebuilds from genesis")
		} else {
			e.logger.Warnf("utxo-persister lastProcessed marker is present but unparseable and will block the persister's next start; re-run with --reset-utxo-persister to delete it (safe only with full delta history to genesis)")
		}
		return true, nil
	case !known:
		e.logger.Infof("no readable utxo-persister lastProcessed marker; nothing to reset")
		return false, nil
	case height <= uint64(target):
		e.logger.Infof("utxo-persister lastProcessed marker is %d, at/below target %d; it will be left untouched", height, target)
		return false, nil
	}

	// Marker above target: resolve its block on the main chain for cleanup.
	hash := e.resolveMainChainHashAtHeight(ctx, uint32(height))

	if e.opts.ResetUTXOPersister {
		e.logger.Infof("utxo-persister lastProcessed marker is %d, above target %d; --reset-utxo-persister given, Phase 3 will delete it (and its stale utxo-set files) and the persister will rebuild from genesis on next start", height, target)
	} else {
		e.logger.Warnf("utxo-persister lastProcessed marker is %d, above target %d; it will be LEFT as-is. The persister will stall on a marker above the rewound chain. Deleting it forces a rebuild from genesis, safe ONLY with full per-block delta history to genesis (a seeded node has none below its seed height and will stall). Re-run with --reset-utxo-persister if you have full history; otherwise reset the persister by hand.", height, target)
	}
	return true, hash
}

// resolveMainChainHashAtHeight returns the hash of the main-chain block at the
// given height, or nil if it can't be resolved (logged, never fatal — this
// feeds a best-effort cleanup). Uses GetBlockHeadersByHeight(h, h), the same
// call the UTXO persister uses to walk the main chain, so a fork block at the
// same height is never returned.
func (e *env) resolveMainChainHashAtHeight(ctx context.Context, height uint32) *chainhash.Hash {
	headers, _, err := e.blockchainStore.GetBlockHeadersByHeight(ctx, height, height)
	if err != nil {
		e.logger.Warnf("could not resolve the main-chain block at height %d to clean up its utxo-set files; if <blockstore>/<hash>.utxo-set exists for that block, remove it by hand: %v", height, err)
		return nil
	}
	if len(headers) == 0 {
		e.logger.Warnf("no main-chain block at height %d (marker above the tip?); its utxo-set files, if any, must be removed by hand", height)
		return nil
	}

	return headers[0].Hash()
}

// resolveTarget picks a target height from --target-height, or failing that,
// decodes state["BlockAssembler"].
func (e *env) resolveTarget(ctx context.Context) (uint32, *chainhash.Hash, error) {
	if e.opts.TargetHeight >= 0 {
		height := uint32(e.opts.TargetHeight)
		block, err := e.blockchainStore.GetBlockByHeight(ctx, height)
		if err != nil {
			return 0, nil, errors.NewStorageError("failed to look up block at target height %d", height, err)
		}
		return height, block.Hash(), nil
	}

	stateBytes, err := e.blockchainStore.GetState(ctx, blockassembly.StateKey)
	if err != nil {
		return 0, nil, errors.NewStorageError(`failed to read state["BlockAssembler"] (pass --target-height to override)`, err)
	}

	header, height, err := blockassembly.DecodeState(stateBytes)
	if err != nil {
		return 0, nil, errors.NewProcessingError(`failed to decode state["BlockAssembler"] (pass --target-height to override)`, err)
	}

	return height, header.Hash(), nil
}

// enumerateBlocksAboveTarget delegates to the blockchain store's
// ListBlockRefsAboveHeight helper (which already returns the rows ordered
// by (height DESC, id DESC) across all branches).
func (e *env) enumerateBlocksAboveTarget(ctx context.Context, target uint32) ([]blockToDelete, error) {
	refs, err := e.blockchainStore.ListBlockRefsAboveHeight(ctx, target)
	if err != nil {
		return nil, err
	}

	out := make([]blockToDelete, 0, len(refs))
	for _, r := range refs {
		out = append(out, blockToDelete{id: r.ID, hash: r.Hash, height: r.Height})
	}
	return out, nil
}

// confirmPrompt shows the summary and waits for y/N from Options.Stdin.
func (e *env) confirmPrompt(r *preflightResult) error {
	if e.opts.Stdout != nil {
		fmt.Fprintf(e.opts.Stdout, "\nAbout to rewind the blockchain:\n")
		fmt.Fprintf(e.opts.Stdout, "  current tip: %d\n", r.tip)
		fmt.Fprintf(e.opts.Stdout, "  target:      %d\n", r.target)
		fmt.Fprintf(e.opts.Stdout, "  blocks to delete (main + fork): %d\n", len(r.deleteList))
		// Keyed on utxoMarkerStale (marker above target OR unparseable), not on
		// utxoMarkerHash: under --reset-utxo-persister an unparseable or
		// above-the-tip marker is reset too (forcing a genesis rebuild) yet has no
		// resolved hash, and the operator must see that at confirmation time.
		if r.utxoMarkerStale {
			if e.opts.ResetUTXOPersister {
				fmt.Fprintf(e.opts.Stdout, "  utxo-persister marker: stale (above target or unparseable) — WILL BE RESET (persister rebuilds from genesis)\n")
			} else {
				fmt.Fprintf(e.opts.Stdout, "  utxo-persister marker: stale (above target or unparseable) — left as-is (pass --reset-utxo-persister to reset; see warning above)\n")
			}
		}
		fmt.Fprintf(e.opts.Stdout, "  This operation is DESTRUCTIVE and CANNOT be undone.\n")
		fmt.Fprintf(e.opts.Stdout, "\nProceed? [y/N] ")
	}

	if e.opts.Stdin == nil {
		return errors.NewProcessingError("no stdin available; pass --assume-yes")
	}

	reader := bufio.NewReader(e.opts.Stdin)

	// ReadString returns the data read *and* io.EOF when the input has no
	// trailing newline, so `printf y | teranode-cli ...` must not be treated
	// as a read failure.
	line, err := reader.ReadString('\n')
	if err != nil && !(errors.Is(err, io.EOF) && line != "") {
		if errors.Is(err, io.EOF) {
			return errors.NewProcessingError("no input on stdin (not a TTY?); re-run with an interactive shell (kubectl exec -it, or docker compose run for a stopped stack) or pass --assume-yes")
		}

		return errors.NewProcessingError("failed to read confirmation", err)
	}

	line = strings.ToLower(strings.TrimSpace(line))
	if line != "y" && line != "yes" {
		return errors.NewProcessingError("aborted by user")
	}

	return nil
}
