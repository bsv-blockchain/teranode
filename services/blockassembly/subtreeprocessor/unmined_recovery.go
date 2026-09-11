package subtreeprocessor

import (
	"context"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	utxostore "github.com/bsv-blockchain/teranode/stores/utxo"
)

type unminedRecoveryRequest struct {
	run    func() error
	result chan error
}

// RecoveryPending reports whether a failed memory rebuild needs a read-only retry.
func (stp *SubtreeProcessor) RecoveryPending() bool {
	return stp.recoveryPending.Load()
}

// RecoverUnmined selects transactions without modifying the live template, then
// rebuilds only assembly memory. The prepare callback must read fresh metadata,
// exclude invalid/mined transactions and return unique rows in parent-first order.
// Its accepted predicate identifies prior assembly/queue admission, including
// evidence retained after a failed rebuild. It must not mutate UTXO state.
//
// Once dispatched, cancellation waits for the callback to return so its caller
// cannot reuse selection state concurrently. Processor shutdown is the exception.
func (stp *SubtreeProcessor) RecoverUnmined(ctx context.Context, header *model.BlockHeader, scanHashes []chainhash.Hash,
	prepare func(context.Context, []chainhash.Hash, func(chainhash.Hash) bool) ([]*utxostore.UnminedTransaction, error)) error {
	if prepare == nil {
		return errors.NewProcessingError("[SubtreeProcessor] unmined recovery requires a selection callback")
	}
	ctx, cancel := context.WithCancel(ctx)
	stop := context.AfterFunc(stp.processorContext(), cancel)
	defer func() { stop(); cancel() }()
	request := unminedRecoveryRequest{
		run:    func() error { return stp.recoverUnmined(ctx, header, scanHashes, prepare) },
		result: make(chan error, 1),
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case stp.recoverUnminedCh <- request:
	}
	select {
	case err := <-request.result:
		return err
	case <-stp.processorContext().Done():
		return stp.processorContext().Err()
	}
}

func (stp *SubtreeProcessor) recoverUnmined(ctx context.Context, header *model.BlockHeader, scanHashes []chainhash.Hash,
	prepare func(context.Context, []chainhash.Hash, func(chainhash.Hash) bool) ([]*utxostore.UnminedTransaction, error)) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	current := stp.currentBlockHeader.Load()
	if header == nil || current == nil || !header.Hash().IsEqual(current.Hash()) {
		return errors.NewProcessingError("[SubtreeProcessor] unmined recovery aborted: block assembly tip changed")
	}
	// Stored subtree metadata reads the old transaction map asynchronously.
	// Wait while the old state is intact; the dispatcher cannot announce new
	// subtrees until this recovery finishes.
	if err := stp.waitForSubtreeStorage(ctx); err != nil {
		return err
	}
	accepted := make(map[chainhash.Hash]struct{}, len(stp.recoveryAccepted)+stp.currentTxMap.Length())
	for hash := range stp.recoveryAccepted {
		accepted[hash] = struct{}{}
	}
	for _, tree := range stp.chainedSubtrees {
		for _, node := range tree.Nodes {
			if node.Hash != *subtreepkg.CoinbasePlaceholderHash {
				accepted[node.Hash] = struct{}{}
			}
		}
	}
	if tree := stp.currentSubtree.Load(); tree != nil {
		for _, node := range tree.Nodes {
			if node.Hash != *subtreepkg.CoinbasePlaceholderHash {
				accepted[node.Hash] = struct{}{}
			}
		}
	}
	boundary, err := stp.queue.snapshotPublished(ctx, func(hash chainhash.Hash) { accepted[hash] = struct{}{} })
	if err != nil {
		return err
	}
	seen := make(map[chainhash.Hash]struct{}, len(scanHashes)+len(accepted))
	hashes := make([]chainhash.Hash, 0, len(scanHashes)+len(accepted))
	for _, hash := range scanHashes {
		if _, exists := seen[hash]; !exists {
			seen[hash] = struct{}{}
			hashes = append(hashes, hash)
		}
		if stp.currentTxMap.Exists(hash) {
			accepted[hash] = struct{}{}
		}
	}
	for hash := range accepted {
		if _, exists := seen[hash]; !exists {
			seen[hash] = struct{}{}
			hashes = append(hashes, hash)
		}
	}
	selected, err := prepare(ctx, hashes, func(hash chainhash.Hash) bool {
		_, exists := accepted[hash]
		return exists
	})
	if err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	// Validate the callback shape before touching live state. Metadata validity
	// and ancestry are the prepare callback's responsibility.
	clear(seen)
	for _, tx := range selected {
		if tx == nil || tx.Node == nil || tx.TxInpoints == nil || tx.Hash == *subtreepkg.CoinbasePlaceholderHash {
			return errors.NewProcessingError("[SubtreeProcessor] invalid unmined recovery selection")
		}
		if _, exists := seen[tx.Hash]; exists {
			return errors.NewProcessingError("[SubtreeProcessor] duplicate transaction in unmined recovery selection")
		}
		seen[tx.Hash] = struct{}{}
	}
	selected = stp.excludeRemovedRecoveryTransactions(selected)
	newTree, err := stp.newSubtree(int(stp.currentItemsPerFile.Load()))
	if err != nil {
		return err
	}
	if err := newTree.AddCoinbaseNode(); err != nil {
		newTree.Close()
		return err
	}
	// Publication and its admission flag share one lock. Previously leased jobs
	// retain their immutable subtrees; new jobs cannot see a partial rebuild.
	stp.miningSnapshots.mu.Lock()
	if stp.recoveryPendingSince.Load() == nil {
		since := stp.clock.Now()
		stp.recoveryPendingSince.Store(&since)
	}
	prometheusRecoveryPendingSince.Store(stp.recoveryPendingSince.Load())
	stp.recoveryPending.Store(true)
	stp.miningSnapshots.mu.Unlock()
	stp.recoveryAccepted = accepted
	stp.closeChainedSubtrees()
	if tree := stp.currentSubtree.Load(); tree != nil {
		stp.closeMiningSubtree(tree)
	}
	stp.currentSubtree.Store(newTree)
	stp.currentTxMap.Clear()
	stp.setTxCountFromSubtrees()
	for _, tx := range selected {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := stp.addDirectly(ctx, tx.Node, tx.TxInpoints, true, true); err != nil {
			return err
		}
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	// Nothing before this point removes queued feeds. A failed rebuild can retry
	// using the same prefix and accepted evidence, even after memory was cleared.
	stp.queue.discardThrough(boundary)
	stp.recoveryAccepted = nil
	stp.publishRecoveredMiningData()
	return nil
}

func (stp *SubtreeProcessor) publishRecoveredMiningData() {
	stp.miningSnapshots.mu.Lock()
	defer stp.miningSnapshots.mu.Unlock()
	stp.updatePrecomputedMiningDataLocked()
	stp.recoveryPending.Store(false)
	stp.clearRecoveryPendingMetric()
	stp.recoveryPendingSince.Store(nil)
}

// Shutdown removes only this processor's sample without opening its safety gate.
// The first-failure timestamp is reset only after successful publication.
func (stp *SubtreeProcessor) clearRecoveryPendingMetric() {
	prometheusRecoveryPendingSince.CompareAndSwap(stp.recoveryPendingSince.Load(), nil)
}

// Normal queue admission honours removeMap. Direct reconstruction must honour
// those outstanding evictions too, including descendants whose selected parent
// is excluded. Selection is parent-first, so one pass preserves that closure.
// Leave removeMap intact for delayed feeds; rebuilding is not a dequeue.
func (stp *SubtreeProcessor) excludeRemovedRecoveryTransactions(selected []*utxostore.UnminedTransaction) []*utxostore.UnminedTransaction {
	if stp.removeMap.Length() == 0 {
		return selected
	}
	rejected := make(map[chainhash.Hash]struct{})
	kept := selected[:0]
	for _, tx := range selected {
		exclude := stp.removeMap.Exists(tx.Hash)
		for _, parent := range tx.TxInpoints.ParentTxHashes {
			if _, removed := rejected[parent]; removed || stp.removeMap.Exists(parent) {
				exclude = true
				break
			}
		}
		if exclude {
			rejected[tx.Hash] = struct{}{}
			continue
		}
		kept = append(kept, tx)
	}
	return kept
}
