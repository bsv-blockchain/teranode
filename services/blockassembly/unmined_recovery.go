package blockassembly

import (
	"context"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const unminedRecoveryRetryDelay = time.Minute

func (b *BlockAssembler) unminedRecoveryInterval() time.Duration {
	interval := b.settings.BlockAssembly.UnminedRecoveryInterval
	if interval <= 0 {
		return 0
	}
	return interval
}

// Retry deferred or failed recovery without spinning or waiting another full
// recovery interval after an operator resume or temporary loss of authority.
func (b *BlockAssembler) nextUnminedRecoveryDelay(recovered bool) time.Duration {
	interval := b.unminedRecoveryInterval()
	if interval == 0 {
		// Keep retrying if the processor reports an incomplete repair.
		if b.subtreeProcessor.RecoveryPending() {
			return unminedRecoveryRetryDelay
		}
		return 0
	}
	if !recovered && interval > unminedRecoveryRetryDelay {
		return unminedRecoveryRetryDelay
	}
	return interval
}

// recoverUnminedTransactions queues eligible missing entries at the current tip.
// It runs off the channel listener; the processor rechecks its anchor on admission.
func (b *BlockAssembler) recoverUnminedTransactions(ctx context.Context) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	if b.unminedRecoveryInterval() == 0 && !b.subtreeProcessor.RecoveryPending() {
		return false, nil
	}
	// Bound index scan, selection and queue admission. No live template is
	// replaced, so cancellation leaves already admitted batches with the queue.
	timeout := b.settings.BlockAssembly.UnminedRecoveryTimeout
	if timeout <= 0 {
		timeout = 5 * time.Minute
	}
	ctx, stopRecovery := context.WithTimeout(ctx, timeout)
	defer stopRecovery()
	// State and tip reads must not hold the assembly listener indefinitely.
	readCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	state, err := b.blockchainClient.ReadFSMState(readCtx)
	if err != nil {
		cancel()
		return false, err
	}
	if state != blockchain.FSMStateRUNNING {
		cancel()
		return false, nil
	}
	// Periodic recovery defers quietly while mined flags are in flight. Unlike
	// startup/reset, it must not wait here and stall the assembly listener.
	pending, err := b.blockchainClient.GetBlocksMinedNotSet(readCtx)
	if err != nil {
		cancel()
		return false, err
	}
	if len(pending) != 0 {
		cancel()
		return false, nil
	}
	bestHeader, bestMeta, err := b.blockchainClient.GetBestBlockHeader(readCtx)
	cancel()
	if err != nil {
		return false, err
	}
	header, height := b.CurrentBlock()
	if header == nil || bestHeader == nil || bestMeta == nil {
		return false, errors.NewProcessingError("unmined recovery requires a known chain tip")
	}
	repairPending := b.subtreeProcessor.RecoveryPending()
	if !repairPending && (height != bestMeta.Height || !header.Hash().IsEqual(bestHeader.Hash())) {
		// Let normal block/reorg processing catch up first. Recovery must not
		// introduce additional tip-moving resets or bypass their conflict rules.
		b.triggerReconcile()
		return false, errors.NewProcessingError("unmined recovery deferred until assembly reaches the chain tip")
	}

	started := time.Now()
	b.logger.Infof("[BlockAssembler] Recovering unmined transactions at height %d", height)
	hashes, err := b.scanUnminedRecoveryHashes(ctx)
	if err != nil {
		return false, err
	}
	err = b.subtreeProcessor.RecoverUnmined(ctx, header, hashes,
		func(ctx context.Context, candidates []chainhash.Hash, accepted func(chainhash.Hash) bool) ([]*utxo.UnminedTransaction, error) {
			// Recheck authority and tip before reading metadata. The processor
			// independently checks its anchor again before each queue batch.
			checkCtx, stopCheck := context.WithTimeout(ctx, 5*time.Second)
			defer stopCheck()
			state, err := b.blockchainClient.ReadFSMState(checkCtx)
			if err != nil {
				return nil, err
			}
			if state != blockchain.FSMStateRUNNING {
				return nil, errors.NewProcessingError("unmined recovery deferred because the FSM is no longer RUNNING")
			}
			latest, _, err := b.blockchainClient.GetBestBlockHeader(checkCtx)
			if err != nil {
				return nil, err
			}
			if latest == nil || (!repairPending && !latest.Hash().IsEqual(header.Hash())) {
				b.triggerReconcile()
				return nil, errors.NewProcessingError("unmined recovery deferred because the chain tip changed")
			}
			selected, err := b.prepareUnminedRecovery(ctx, candidates, accepted)
			if err != nil {
				return nil, err
			}
			// Selection runs outside the dispatcher and can outlive an FSM
			// transition. Admit only while authority still permits assembly.
			finalCtx, stopFinal := context.WithTimeout(ctx, 5*time.Second)
			defer stopFinal()
			state, err = b.blockchainClient.ReadFSMState(finalCtx)
			if err != nil {
				return nil, err
			}
			if state != blockchain.FSMStateRUNNING {
				return nil, errors.NewProcessingError("unmined recovery deferred because the FSM is no longer RUNNING")
			}
			latest, _, err = b.blockchainClient.GetBestBlockHeader(finalCtx)
			if err != nil {
				return nil, err
			}
			if latest == nil || !latest.Hash().IsEqual(header.Hash()) {
				b.triggerReconcile()
				return nil, errors.NewProcessingError("unmined recovery deferred because the chain tip changed")
			}
			return selected, nil
		})
	if err != nil {
		return false, err
	}
	// The chain may advance while the queue drains. Trigger normal reconciliation
	// if this pass no longer describes the current chain tip.
	checkCtx, stopCheck := context.WithTimeout(ctx, 5*time.Second)
	latest, _, checkErr := b.blockchainClient.GetBestBlockHeader(checkCtx)
	stopCheck()
	if checkErr != nil || latest == nil || !latest.Hash().IsEqual(header.Hash()) {
		b.triggerReconcile()
	}
	b.logger.Infof("[BlockAssembler] Unmined transaction recovery completed in %s", time.Since(started))
	return true, nil
}

// The index only discovers candidates. Eligibility is checked from fresh
// metadata off the dispatcher, with queued and assembled admission proof.
// In particular, startup's unlock/cleanup behavior is never used live.
func (b *BlockAssembler) scanUnminedRecoveryHashes(ctx context.Context) (hashes []chainhash.Hash, resultErr error) {
	var iterator utxo.UnminedTxIterator
	var err error
	if store, ok := b.utxoStore.(interface {
		GetUnminedTxIteratorContext(context.Context) (utxo.UnminedTxIterator, error)
	}); ok {
		// SQL must bind the initial query and Rows.Next to this deadline too.
		iterator, err = store.GetUnminedTxIteratorContext(ctx)
	} else {
		// Aerospike observes ctx in Next; its context-free configuration lookup
		// retains the client info timeout. Other legacy/custom constructors
		// must also provide their own bound until they support this API.
		iterator, err = b.utxoStore.GetUnminedTxIterator()
	}
	if err != nil {
		return nil, err
	}
	defer func() {
		if err := iterator.Close(); resultErr == nil {
			resultErr = err
		}
	}()
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		batch, err := iterator.Next(ctx)
		if err != nil {
			return nil, err
		}
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if len(batch) == 0 {
			return hashes, iterator.Err()
		}
		for _, tx := range batch {
			if tx != nil && tx.Skip {
				continue
			}
			if tx == nil || tx.Node == nil {
				return nil, errors.NewProcessingError("unmined recovery encountered missing transaction metadata")
			}
			hashes = append(hashes, tx.Hash)
		}
	}
}

// An older blockchain service is an expected rollout condition. Preserve warnings
// for readiness, storage and selection failures that require attention.
func (b *BlockAssembler) logUnminedRecoveryError(err error) {
	if status.Code(err) == codes.Unimplemented {
		b.logger.Infof("[BlockAssembler] Unmined recovery requires a blockchain service upgrade: %v", err)
		return
	}
	b.logger.Warnf("[BlockAssembler] Unmined transaction recovery deferred: %v", err)
}
