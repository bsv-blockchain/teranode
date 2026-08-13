package subtreeprocessor

import (
	"context"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
)

// reconcileCoinbasesMsg is the request sent over reconcileCoinbasesCh to have
// the processor goroutine create the canonical coinbase UTXOs for a set of
// gap blocks. It mirrors resetBlocks: a small envelope carrying the request
// parameters plus a buffered response channel for the reply.
type reconcileCoinbasesMsg struct {
	// ctx is the caller-supplied context, forwarded to processCoinbaseUtxos
	// for cancellation; it is independent of the processor's own lifecycle
	// context.
	ctx context.Context

	// gapBlocks are the canonical blocks (ascending height) whose coinbase
	// UTXOs must exist.
	gapBlocks []*model.Block

	// responseCh receives the single error result of the reconcile.
	responseCh chan error
}

// ReconcileCoinbases creates the canonical coinbase UTXOs for the given gap
// blocks (ascending height). It is coinbase-only and idempotent -
// processCoinbaseUtxos tolerates ErrTxExists - and never touches any other
// in-memory processor state (no reset, no unmined-tx reload), which is what
// keeps its cost O(len(gapBlocks)) rather than O(mempool size).
//
// The work runs on the SubtreeProcessor's own goroutine (like Reset) via
// reconcileCoinbasesCh, so it never races the processor's in-memory state.
//
// Parameters:
//   - ctx: Context for cancellation of the underlying UTXO store calls
//   - gapBlocks: Canonical gap blocks, ascending height, needing coinbase UTXOs
//
// Returns:
//   - error: The first error encountered creating a gap block's coinbase, if any
func (stp *SubtreeProcessor) ReconcileCoinbases(ctx context.Context, gapBlocks []*model.Block) error {
	if len(gapBlocks) == 0 {
		return nil
	}

	processorCtx := stp.processorContext()

	responseCh := make(chan error, 1)
	req := reconcileCoinbasesMsg{
		ctx:        ctx,
		gapBlocks:  gapBlocks,
		responseCh: responseCh,
	}

	select {
	case stp.reconcileCoinbasesCh <- req:
	case <-processorCtx.Done():
		return errors.NewProcessingError("[SubtreeProcessor] ReconcileCoinbases aborted: processor stopped before request delivered", processorCtx.Err())
	}

	select {
	case err := <-responseCh:
		return err
	case <-processorCtx.Done():
		return errors.NewProcessingError("[SubtreeProcessor] ReconcileCoinbases aborted: processor stopped while waiting for response", processorCtx.Err())
	}
}

// reconcileCoinbases is the unexported worker invoked on the processor
// goroutine. It only ever calls processCoinbaseUtxos - one coinbase per
// block - and never touches per-transaction state; that is the scale
// invariant that keeps recovery cheap regardless of mempool size.
func (stp *SubtreeProcessor) reconcileCoinbases(ctx context.Context, gapBlocks []*model.Block) error {
	for i, block := range gapBlocks {
		if block == nil || block.Header == nil {
			// Reported before processCoinbaseUtxos is reached, because the error
			// formatting below describes the block with block.String(), which
			// dereferences both the block and its header.
			return errors.NewProcessingError("[SubtreeProcessor][reconcileCoinbases] gap block at index %d is nil or has no header", i)
		}

		if err := stp.processCoinbaseUtxos(ctx, block); err != nil {
			return errors.NewProcessingError("[SubtreeProcessor][reconcileCoinbases] error creating coinbase for block %s", block.String(), err)
		}
	}

	return nil
}
