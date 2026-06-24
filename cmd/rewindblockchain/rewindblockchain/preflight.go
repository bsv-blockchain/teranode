package rewindblockchain

import (
	"bufio"
	"context"
	"encoding/binary"
	"fmt"
	"strings"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
)

// preflightResult holds everything the phases need to know up-front.
type preflightResult struct {
	target      uint32
	tip         uint32
	targetHash  *chainhash.Hash
	deleteList  []blockToDelete
	deleteByID  map[uint32]struct{}
	deleteByHsh map[chainhash.Hash]struct{}
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
		return nil, errors.NewStorageError("failed to read FSM state: %w", err)
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
		return nil, errors.NewStorageError("failed to read best block: %w", err)
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

	// 5. Enumerate blocks above target.
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

	res := &preflightResult{
		target:      target,
		tip:         tip,
		targetHash:  targetHash,
		deleteList:  deleteList,
		deleteByID:  byID,
		deleteByHsh: byHash,
	}

	// 6. Interactive confirmation.
	if !e.opts.AssumeYes && !e.opts.DryRun {
		if err = e.confirmPrompt(res); err != nil {
			return nil, err
		}
	}

	return res, nil
}

// resolveTarget picks a target height from --target-height, or failing that,
// decodes state["BlockAssembler"].
func (e *env) resolveTarget(ctx context.Context) (uint32, *chainhash.Hash, error) {
	if e.opts.TargetHeight >= 0 {
		height := uint32(e.opts.TargetHeight)
		block, err := e.blockchainStore.GetBlockByHeight(ctx, height)
		if err != nil {
			return 0, nil, errors.NewStorageError("failed to look up block at target height %d: %w", height, err)
		}
		return height, block.Hash(), nil
	}

	stateBytes, err := e.blockchainStore.GetState(ctx, "BlockAssembler")
	if err != nil {
		return 0, nil, errors.NewStorageError(`failed to read state["BlockAssembler"] (pass --target-height to override): %w`, err)
	}
	if len(stateBytes) < 4+80 {
		return 0, nil, errors.NewProcessingError(`state["BlockAssembler"] too short: %d bytes (pass --target-height)`, len(stateBytes))
	}

	height := binary.LittleEndian.Uint32(stateBytes[:4])
	header, err := model.NewBlockHeaderFromBytes(stateBytes[4:])
	if err != nil {
		return 0, nil, errors.NewProcessingError("failed to decode block header in state[BlockAssembler]: %w", err)
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
		fmt.Fprintf(e.opts.Stdout, "  This operation is DESTRUCTIVE and CANNOT be undone.\n")
		fmt.Fprintf(e.opts.Stdout, "\nProceed? [y/N] ")
	}

	if e.opts.Stdin == nil {
		return errors.NewProcessingError("no stdin available; pass --assume-yes")
	}

	reader := bufio.NewReader(e.opts.Stdin)
	line, err := reader.ReadString('\n')
	if err != nil {
		return errors.NewProcessingError("failed to read confirmation: %w", err)
	}

	line = strings.ToLower(strings.TrimSpace(line))
	if line != "y" && line != "yes" {
		return errors.NewProcessingError("aborted by user")
	}

	return nil
}
