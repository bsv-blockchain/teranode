package blockassembly

import (
	"context"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
)

type recoveryEligibility uint8

const (
	recoveryUnvisited recoveryEligibility = iota
	recoveryVisiting
	recoveryIncluded
	recoveryMined
	recoveryExcluded
)

type recoverySelectionEntry struct {
	tx    *utxo.UnminedTransaction
	state recoveryEligibility
}

type recoverySelectionFrame struct {
	hash       chainhash.Hash
	nextParent int
}

// prepareUnminedRecovery selects a parent-first snapshot without changing UTXO
// state. The processor dispatcher must own the assembly/queue snapshot while
// this runs. accepted identifies successful assembly handoffs, including locked
// transactions whose validator has not yet acknowledged its two-phase commit.
// An unqueued locked record may still be unwound after a rejected handoff and
// must not be admitted or unlocked by online recovery.
func (b *BlockAssembler) prepareUnminedRecovery(ctx context.Context, hashes []chainhash.Hash, accepted func(chainhash.Hash) bool) ([]*utxo.UnminedTransaction, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	header, height := b.CurrentBlock()
	if header == nil {
		return nil, errors.NewProcessingError("unmined recovery requires an assembly chain tip")
	}
	ids, err := b.blockchainClient.GetBlockHeaderIDs(ctx, header.Hash(), uint64(height)+1)
	if err != nil {
		return nil, errors.NewProcessingError("unmined recovery failed to read anchored chain", err)
	}
	chainIDs := make(map[uint32]struct{}, len(ids))
	for _, id := range ids {
		chainIDs[id] = struct{}{}
	}
	cache, err := b.readRecoveryGraph(ctx, hashes, chainIDs, accepted)
	if err != nil {
		return nil, err
	}
	return orderRecoveryTransactions(ctx, hashes, cache)
}

// readRecoveryGraph hydrates candidates and ancestor frontiers with bounded
// metadata batches, caching each transaction once for this selection.
func (b *BlockAssembler) readRecoveryGraph(ctx context.Context, hashes []chainhash.Hash, chainIDs map[uint32]struct{}, accepted func(chainhash.Hash) bool) (map[chainhash.Hash]*recoverySelectionEntry, error) {
	cache := make(map[chainhash.Hash]*recoverySelectionEntry, len(hashes))
	frontier := make([]chainhash.Hash, 0, len(hashes))
	schedule := func(hash chainhash.Hash) {
		if _, exists := cache[hash]; !exists {
			cache[hash] = &recoverySelectionEntry{state: recoveryExcluded}
			frontier = append(frontier, hash)
		}
	}
	for _, hash := range hashes {
		schedule(hash)
	}
	batchSize := b.settings.BlockAssembly.ParentValidationBatchSize
	if batchSize <= 0 {
		batchSize = 1000
	}
	// Hydrate initial candidates and newly discovered ancestor frontiers in
	// batches. Missing records are exclusions; backend failures abort the
	// entire selection before any template/queue mutation.
	for len(frontier) > 0 {
		pending := frontier
		frontier = nil
		for start := 0; start < len(pending); start += batchSize {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			end := min(start+batchSize, len(pending))
			batch := make([]*utxo.UnresolvedMetaData, 0, end-start)
			for i, hash := range pending[start:end] {
				batch = append(batch, &utxo.UnresolvedMetaData{Hash: hash, Idx: i})
			}
			// The index iterator can omit inpoints according to a storage
			// setting; online recovery must always request them explicitly.
			err := b.utxoStore.BatchDecorate(ctx, batch, fields.Creating, fields.Locked,
				fields.Conflicting, fields.BlockIDs, fields.UnminedSince, fields.IsCoinbase,
				fields.TxInpoints, fields.Fee, fields.SizeInBytes)
			if err != nil {
				return nil, errors.NewProcessingError("unmined recovery failed to read transaction metadata", err)
			}
			for _, item := range batch {
				entry := cache[item.Hash]
				if err := classifyRecoveryMetadata(item, entry, chainIDs, accepted); err != nil {
					return nil, err
				}
				if entry.state == recoveryUnvisited {
					for _, parent := range entry.tx.TxInpoints.ParentTxHashes {
						schedule(parent)
					}
				}
			}
		}
	}

	return cache, nil
}

// classifyRecoveryMetadata separates permanent exclusions from read failures.
// Chain membership permits mined parents without replaying them; only accepted
// handoffs permit a still-locked unmined record.
func classifyRecoveryMetadata(item *utxo.UnresolvedMetaData, entry *recoverySelectionEntry, chainIDs map[uint32]struct{}, accepted func(chainhash.Hash) bool) error {
	if item.Err != nil {
		if errors.Is(item.Err, errors.ErrTxNotFound) {
			return nil
		}
		return errors.NewProcessingError("unmined recovery failed to read transaction %s", item.Hash.String(), item.Err)
	}
	data := item.Data
	if data == nil {
		return errors.NewProcessingError("unmined recovery received no metadata for %s", item.Hash.String())
	}
	if data.Creating || data.Conflicting {
		return nil
	}
	for _, id := range data.BlockIDs {
		if _, mined := chainIDs[id]; mined {
			entry.state = recoveryMined
			return nil
		}
	}
	wasAccepted := accepted != nil && accepted(item.Hash)
	if data.IsCoinbase || (data.Locked && !wasAccepted) || (data.UnminedSince == 0 && !wasAccepted) {
		return nil
	}
	entry.tx = &utxo.UnminedTransaction{
		Node:       &subtree.Node{Hash: item.Hash, Fee: data.Fee, SizeInBytes: data.SizeInBytes},
		TxInpoints: &data.TxInpoints,
		BlockIDs:   data.BlockIDs,
		Locked:     data.Locked,
	}
	entry.state = recoveryUnvisited
	return nil
}

func orderRecoveryTransactions(ctx context.Context, hashes []chainhash.Hash, cache map[chainhash.Hash]*recoverySelectionEntry) ([]*utxo.UnminedTransaction, error) {
	selected := make([]*utxo.UnminedTransaction, 0, len(hashes))
	// Explicit DFS stack avoids recursion limits on long transaction chains.
	// A parent is appended only after all of its own ancestors are eligible;
	// excluded parents and cycles exclude descendants without store mutations.
	stack := make([]recoverySelectionFrame, 0, 64)
	for _, hash := range hashes {
		stack = append(stack[:0], recoverySelectionFrame{hash: hash})
		for len(stack) > 0 {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			frame := &stack[len(stack)-1]
			entry := cache[frame.hash]
			if entry.state != recoveryUnvisited && entry.state != recoveryVisiting {
				stack = stack[:len(stack)-1]
				continue
			}
			entry.state = recoveryVisiting
			parents := entry.tx.TxInpoints.ParentTxHashes
			if frame.nextParent == len(parents) {
				entry.state = recoveryIncluded
				selected = append(selected, entry.tx)
				stack = stack[:len(stack)-1]
				continue
			}
			parentHash := parents[frame.nextParent]
			parent := cache[parentHash]
			switch parent.state {
			case recoveryUnvisited:
				stack = append(stack, recoverySelectionFrame{hash: parentHash})
			case recoveryIncluded, recoveryMined:
				frame.nextParent++
			case recoveryVisiting, recoveryExcluded:
				entry.state = recoveryExcluded
				stack = stack[:len(stack)-1]
			}
		}
	}
	return selected, nil
}
