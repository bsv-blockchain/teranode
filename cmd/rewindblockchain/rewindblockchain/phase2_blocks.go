package rewindblockchain

import (
	"context"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
)

// phase2Blocks walks deleteList tip-first, removing every block + its
// subtree data + its transactions from the UTXO store.
func (e *env) phase2Blocks(ctx context.Context, pf *preflightResult) error {
	for _, b := range pf.deleteList {
		if err := e.rewindOneBlock(ctx, pf, b); err != nil {
			return errors.NewProcessingError("rewinding block %s (height %d): %w", b.hash.String(), b.height, err)
		}
	}
	return nil
}

func (e *env) rewindOneBlock(ctx context.Context, pf *preflightResult, bl blockToDelete) error {
	block, _, err := e.blockchainStore.GetBlock(ctx, bl.hash)
	if err != nil {
		return errors.NewStorageError("GetBlock: %w", err)
	}

	subtrees, err := block.GetSubtrees(ctx, e.logger, e.subtreeStore, e.concurrency)
	if err != nil {
		return errors.NewStorageError("GetSubtrees: %w", err)
	}

	collector := newRemovalCollector()

	// Iterate subtrees in REVERSE, nodes in REVERSE. Skip the coinbase
	// placeholder at subtree 0 index 0 (handled separately via block.CoinbaseTx).
	for si := len(subtrees) - 1; si >= 0; si-- {
		st := subtrees[si]
		if st == nil {
			continue
		}

		for ni := len(st.Nodes) - 1; ni >= 0; ni-- {
			node := st.Nodes[ni]

			if si == 0 && ni == 0 {
				continue
			}
			if node.Hash.Equal(subtree.CoinbasePlaceholderHashValue) {
				continue
			}

			hash := node.Hash
			if _, err = e.deleteTxWithParents(ctx, &hash, pf, collector); err != nil {
				return err
			}
		}
	}

	if block.CoinbaseTx != nil {
		if err = e.deleteCoinbase(ctx, pf, block.CoinbaseTx.TxIDChainHash()); err != nil {
			return err
		}
	}

	if err = e.flushCollector(ctx, collector); err != nil {
		return err
	}

	if err = e.deleteSubtreeBlobs(ctx, pf, block.Subtrees); err != nil {
		return err
	}

	if err = e.blockchainStore.DeleteBlock(ctx, bl.hash); err != nil {
		return errors.NewStorageError("DeleteBlock: %w", err)
	}
	e.stats.BlocksDeleted++

	return nil
}

// deleteCoinbase handles the coinbase for the current block. When the
// coinbase is also referenced by a surviving block (extremely rare), only
// trim BlockIDs. Otherwise Delete outright.
func (e *env) deleteCoinbase(ctx context.Context, pf *preflightResult, coinbaseHash *chainhash.Hash) error {
	meta, err := e.utxoStore.Get(ctx, coinbaseHash, fields.BlockIDs)
	if err != nil {
		if isNotFound(err) {
			return nil
		}
		return errors.NewStorageError("Get coinbase %s: %w", coinbaseHash.String(), err)
	}

	var deletedHits, survivors []uint32
	for _, id := range meta.BlockIDs {
		if _, ok := pf.deleteByID[id]; ok {
			deletedHits = append(deletedHits, id)
		} else {
			survivors = append(survivors, id)
		}
	}
	if len(survivors) > 0 {
		if len(deletedHits) > 0 {
			removals := []utxo.BlockIDsRemoval{{TxHash: coinbaseHash, BlockIDs: deletedHits}}
			if err = e.utxoStore.RemoveBlockIDs(ctx, removals); err != nil {
				return errors.NewStorageError("RemoveBlockIDs coinbase: %w", err)
			}
			e.stats.TxsBlockIDsTrimmed++
		}
		return nil
	}

	if err = e.utxoStore.Delete(ctx, coinbaseHash); err != nil {
		if !isNotFound(err) {
			return errors.NewStorageError("Delete coinbase: %w", err)
		}
	}
	e.stats.TxsDeleted++
	return nil
}

func (e *env) deleteSubtreeBlobs(ctx context.Context, pf *preflightResult, subtreeHashes []*chainhash.Hash) error {
	for _, sh := range subtreeHashes {
		if sh == nil {
			continue
		}

		shared, err := e.blockchainStore.HasBlockBelowHeightContainingSubtree(ctx, sh, pf.target)
		if err != nil {
			return errors.NewStorageError("HasBlockBelowHeightContainingSubtree: %w", err)
		}
		if shared {
			e.stats.SubtreesSkippedShared++
			continue
		}

		for _, ft := range []fileformat.FileType{
			fileformat.FileTypeSubtree,
			fileformat.FileTypeSubtreeData,
			fileformat.FileTypeSubtreeMeta,
			fileformat.FileTypeSubtreeToCheck,
		} {
			if delErr := e.subtreeStore.Del(ctx, sh[:], ft); delErr != nil {
				// Best effort — tolerate NotFound per file type.
				e.logger.Debugf("subtree blob delete %s %s: %v", sh.String(), ft, delErr)
			}
		}
		e.stats.SubtreesDeleted++
	}
	return nil
}
