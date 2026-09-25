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

// Cap repair-owned backlog even when ordinary ingest intentionally has no cap.
const maxRecoveryQueuedItems int64 = 1 << 16

// RecoveryPending remains a safety gate for an incomplete recovery created by
// an older in-process path. Periodic repair never enters this state.
func (stp *SubtreeProcessor) RecoveryPending() bool { return stp.recoveryPending.Load() }

func (stp *SubtreeProcessor) publishRecoveredMiningData() {
	stp.miningSnapshots.mu.Lock()
	defer stp.miningSnapshots.mu.Unlock()
	stp.updatePrecomputedMiningDataLocked()
	stp.recoveryPending.Store(false)
	stp.clearRecoveryPendingMetric()
	stp.recoveryPendingSince.Store(nil)
}

func (stp *SubtreeProcessor) clearRecoveryPendingMetric() {
	prometheusRecoveryPendingSince.CompareAndSwap(stp.recoveryPendingSince.Load(), nil)
}

// RecoverUnmined captures an admission snapshot on the dispatcher, performs
// storage-heavy selection on its caller, then admits missing rows in bounded
// queue batches. The ordinary dequeue path stores and announces new subtrees.
func (stp *SubtreeProcessor) RecoverUnmined(ctx context.Context, header *model.BlockHeader, scanHashes []chainhash.Hash,
	prepare func(context.Context, []chainhash.Hash, func(chainhash.Hash) bool) ([]*utxostore.UnminedTransaction, error)) error {
	if prepare == nil {
		return errors.NewProcessingError("[SubtreeProcessor] unmined recovery requires a selection callback")
	}
	ctx, cancel := context.WithCancel(ctx)
	stop := context.AfterFunc(stp.processorContext(), cancel)
	defer func() { stop(); cancel() }()
	var snapshot *unminedRecoverySnapshot
	if err := stp.runRecoveryOnDispatcher(ctx, func() error {
		var err error
		snapshot, err = stp.captureUnminedRecovery(ctx, header, scanHashes)
		return err
	}); err != nil {
		return err
	}
	selected, err := stp.selectUnminedRecovery(ctx, snapshot, prepare, true)
	if err != nil {
		return err
	}
	if err := validateUnminedRecoverySelection(selected); err != nil {
		return err
	}
	var commitCursor *unminedRecoverySnapshot
	if err := stp.runRecoveryOnDispatcher(ctx, func() error {
		var err error
		commitCursor, err = stp.captureUnminedRecovery(ctx, header, nil)
		return err
	}); err != nil {
		return err
	}
	queued, err := commitCursor.queueHashes(ctx)
	if err != nil {
		return err
	}
	rejected := make(map[chainhash.Hash]struct{})
	const admissionBatchSize = 128
	for start := 0; start < len(selected); start += admissionBatchSize {
		end := min(start+admissionBatchSize, len(selected))
		if err := stp.runRecoveryOnDispatcher(ctx, func() error {
			return stp.commitUnminedRecovery(ctx, header, snapshot.epoch, selected[start:end], queued, rejected)
		}); err != nil {
			return err
		}
	}
	if len(selected) == 0 {
		return stp.runRecoveryOnDispatcher(ctx, func() error {
			return stp.commitUnminedRecovery(ctx, header, snapshot.epoch, nil, queued, rejected)
		})
	}
	return nil
}

func (stp *SubtreeProcessor) runRecoveryOnDispatcher(ctx context.Context, run func() error) error {
	request := unminedRecoveryRequest{run: run, result: make(chan error, 1)}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case stp.recoverUnminedCh <- request:
	}
	select {
	case err := <-request.result:
		return err
	case <-ctx.Done():
		return ctx.Err()
	case <-stp.processorContext().Done():
		return stp.processorContext().Err()
	}
}

type unminedRecoverySnapshot struct {
	hashes   []chainhash.Hash
	head     *TxBatch
	boundary *TxBatch
	epoch    uint64
}

func (stp *SubtreeProcessor) captureUnminedRecovery(ctx context.Context, header *model.BlockHeader, scanHashes []chainhash.Hash) (*unminedRecoverySnapshot, error) {
	if err := stp.checkRecoveryAnchor(ctx, header); err != nil {
		return nil, err
	}
	head, boundary := stp.queue.publishedCursor()
	return &unminedRecoverySnapshot{hashes: scanHashes, head: head, boundary: boundary, epoch: stp.recoveryEpoch.Load()}, nil
}

func (snapshot *unminedRecoverySnapshot) queueHashes(ctx context.Context) (map[chainhash.Hash]struct{}, error) {
	queued := make(map[chainhash.Hash]struct{})
	_, err := visitPublishedCursor(ctx, snapshot.head, snapshot.boundary, func(hash chainhash.Hash) {
		queued[hash] = struct{}{}
	})
	return queued, err
}

func (stp *SubtreeProcessor) selectUnminedRecovery(ctx context.Context, snapshot *unminedRecoverySnapshot,
	prepare func(context.Context, []chainhash.Hash, func(chainhash.Hash) bool) ([]*utxostore.UnminedTransaction, error), dispatched bool) ([]*utxostore.UnminedTransaction, error) {
	queued, err := snapshot.queueHashes(ctx)
	if err != nil {
		return nil, err
	}
	seen := make(map[chainhash.Hash]struct{}, len(snapshot.hashes))
	hashes := make([]chainhash.Hash, 0, len(snapshot.hashes)+len(queued))
	for _, hash := range snapshot.hashes {
		if _, exists := seen[hash]; !exists {
			seen[hash] = struct{}{}
			hashes = append(hashes, hash)
		}
	}
	for hash := range queued {
		if _, exists := seen[hash]; !exists {
			seen[hash] = struct{}{}
			hashes = append(hashes, hash)
		}
	}
	acceptedCache := make(map[chainhash.Hash]bool)
	var readErr error
	accepted := func(hash chainhash.Hash) bool {
		if _, exists := queued[hash]; exists {
			return true
		}
		if value, exists := acceptedCache[hash]; exists {
			return value
		}
		if readErr != nil {
			return false
		}
		var exists bool
		if dispatched {
			readErr = stp.runRecoveryOnDispatcher(ctx, func() error {
				if err := stp.checkRecoveryEpoch(ctx, snapshot.epoch); err != nil {
					return err
				}
				exists = stp.currentTxMap.Exists(hash)
				return nil
			})
		} else {
			exists = stp.currentTxMap.Exists(hash)
		}
		acceptedCache[hash] = exists
		return exists
	}
	selected, err := prepare(ctx, hashes, accepted)
	if readErr != nil {
		return nil, readErr
	}
	return selected, err
}

func (stp *SubtreeProcessor) checkRecoveryEpoch(ctx context.Context, epoch uint64) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if stp.recoveryEpoch.Load() != epoch {
		return errors.NewProcessingError("[SubtreeProcessor] unmined recovery aborted: assembly state or queued responsibility changed during selection")
	}
	return nil
}

func (stp *SubtreeProcessor) checkRecoveryAnchor(ctx context.Context, header *model.BlockHeader) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	current := stp.currentBlockHeader.Load()
	if header == nil || current == nil || !header.Hash().IsEqual(current.Hash()) {
		return errors.NewProcessingError("[SubtreeProcessor] unmined recovery aborted: block assembly tip changed")
	}
	if stp.recoveryPending.Load() {
		return errors.NewProcessingError("[SubtreeProcessor] incomplete legacy unmined recovery requires restart")
	}
	return nil
}

func validateUnminedRecoverySelection(selected []*utxostore.UnminedTransaction) error {
	seen := make(map[chainhash.Hash]struct{}, len(selected))
	for _, tx := range selected {
		if tx == nil || tx.Node == nil || tx.TxInpoints == nil || tx.Hash == *subtreepkg.CoinbasePlaceholderHash {
			return errors.NewProcessingError("[SubtreeProcessor] invalid unmined recovery selection")
		}
		if _, exists := seen[tx.Hash]; exists {
			return errors.NewProcessingError("[SubtreeProcessor] duplicate transaction in unmined recovery selection")
		}
		seen[tx.Hash] = struct{}{}
	}
	return nil
}

func (stp *SubtreeProcessor) commitUnminedRecovery(ctx context.Context, header *model.BlockHeader, epoch uint64, selected []*utxostore.UnminedTransaction, queued map[chainhash.Hash]struct{}, rejected map[chainhash.Hash]struct{}) error {
	if err := stp.checkRecoveryAnchor(ctx, header); err != nil {
		return err
	}
	if err := stp.checkRecoveryEpoch(ctx, epoch); err != nil {
		return err
	}
	selected = stp.excludeRemovedRecoveryTransactions(selected, rejected)
	batchSize := 128
	if stp.queue.maxItems > 0 && stp.queue.maxItems < int64(batchSize) {
		batchSize = int(stp.queue.maxItems)
	}
	nodes := make([]subtreepkg.Node, 0, batchSize)
	inpoints := make([]*subtreepkg.TxInpoints, 0, batchSize)
	for _, tx := range selected {
		if err := ctx.Err(); err != nil {
			return err
		}
		assembled := stp.currentTxMap.Exists(tx.Hash)
		_, inQueue := queued[tx.Hash]
		if tx.Locked && !assembled && !inQueue {
			// A queued/assembled lock observed during selection may have been
			// unwound before admission. Never revive it from stale evidence.
			return errors.NewProcessingError("[SubtreeProcessor] unmined recovery lost accepted proof for locked transaction %s", tx.Hash.String())
		}
		if assembled {
			continue
		}
		if inQueue {
			continue
		}
		nodes = append(nodes, *tx.Node)
		inpoints = append(inpoints, tx.TxInpoints)
		if len(nodes) == batchSize {
			if !stp.queue.enqueueRecoveryBatch(nodes, inpoints) {
				return errors.NewProcessingError("[SubtreeProcessor] unmined recovery queue full; retry remaining transactions")
			}
			nodes = make([]subtreepkg.Node, 0, batchSize)
			inpoints = make([]*subtreepkg.TxInpoints, 0, batchSize)
		}
	}
	if len(nodes) != 0 && !stp.queue.enqueueRecoveryBatch(nodes, inpoints) {
		return errors.NewProcessingError("[SubtreeProcessor] unmined recovery queue full; retry remaining transactions")
	}
	return nil
}

// Direct tests use the same capture/select/commit path without a running dispatcher.
func (stp *SubtreeProcessor) recoverUnmined(ctx context.Context, header *model.BlockHeader, scanHashes []chainhash.Hash,
	prepare func(context.Context, []chainhash.Hash, func(chainhash.Hash) bool) ([]*utxostore.UnminedTransaction, error)) error {
	snapshot, err := stp.captureUnminedRecovery(ctx, header, scanHashes)
	if err != nil {
		return err
	}
	selected, err := stp.selectUnminedRecovery(ctx, snapshot, prepare, false)
	if err != nil {
		return err
	}
	if err := validateUnminedRecoverySelection(selected); err != nil {
		return err
	}
	commitCursor, err := stp.captureUnminedRecovery(ctx, header, nil)
	if err != nil {
		return err
	}
	queued, err := commitCursor.queueHashes(ctx)
	if err != nil {
		return err
	}
	return stp.commitUnminedRecovery(ctx, header, snapshot.epoch, selected, queued, make(map[chainhash.Hash]struct{}))
}

// Normal queue admission honours removeMap. Repair must retain the same
// exclusion, including descendants whose selected parent was removed.
func (stp *SubtreeProcessor) excludeRemovedRecoveryTransactions(selected []*utxostore.UnminedTransaction, rejected map[chainhash.Hash]struct{}) []*utxostore.UnminedTransaction {
	if stp.removeMap.Length() == 0 && len(rejected) == 0 {
		return selected
	}
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
