package model

import (
	"context"
	"fmt"
	"slices"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util"
	"golang.org/x/sync/errgroup"
)

// filterNotOnLongestChainBatchSize is how many records the filters here read per
// BatchDecorate call.
const filterNotOnLongestChainBatchSize = 1024

// txMetaBatchReader is the slice of the UTXO store FilterTxsNotOnLongestChain reads from.
type txMetaBatchReader interface {
	BatchDecorate(ctx context.Context, unresolvedMetaDataSlice []*utxo.UnresolvedMetaData, fields ...fields.FieldName) error
}

// longestChainClient is the slice of the blockchain client FilterTxsNotOnLongestChain needs.
type longestChainClient interface {
	CheckBlockIsInCurrentChain(ctx context.Context, blockIDs []uint32) (bool, error)
}

// recentChainHeadersClient is the slice of the blockchain client RecentChainBlockIDs needs.
type recentChainHeadersClient interface {
	GetBlockHeaders(ctx context.Context, blockHash *chainhash.Hash, numberOfHeaders uint64) ([]*BlockHeader, []*BlockHeaderMeta, error)
}

// RecentChainBlockIDs returns the ids of the n blocks ending at blockHash, for
// FilterTxsNotOnLongestChain's fast path when blockHash is on the longest chain.
func RecentChainBlockIDs(ctx context.Context, blockchainClient recentChainHeadersClient, blockHash *chainhash.Hash, n uint64) (map[uint32]bool, error) {
	_, metas, err := blockchainClient.GetBlockHeaders(ctx, blockHash, n)
	if err != nil {
		return nil, errors.NewProcessingError("[RecentChainBlockIDs] error getting last block headers", err)
	}

	ids := make(map[uint32]bool, len(metas))
	for _, meta := range metas {
		ids[meta.ID] = true
	}

	return ids, nil
}

// FilterTxsNotOnLongestChain returns the subset of hashes that still look mined
// (unmined_since absent) but are not mined in any block on the current longest chain:
// exactly the txs whose MarkTransactionsOnLongestChain(false) would change something.
//
// It exists for blocks that left the longest chain after their txs were written as mined.
// A tx the block shares with the longest chain is mined there and must keep its cleared
// unmined_since; a tx that already has unmined_since needs no write at all.
//
//   - offChainBlockID is a block the caller has established is off the longest chain. A tx
//     whose only block it is gets returned without a chain lookup. The caller owns that
//     claim: if the chain can move back onto the block, re-check before marking.
//   - recentMainChainIDs is a set of block ids on the longest chain, typically the most
//     recent ones (RecentChainBlockIDs). A tx in any of them is on the longest chain without
//     a lookup. Other block id sets are resolved with CheckBlockIsInCurrentChain, once per
//     distinct set: the txs of one block share a handful of them.
//
// A hash missing from the UTXO store is an error, unless skipMissing is set: a caller
// re-marking an old block's txs passes it, since a tx pruned since then has nothing to mark.
func FilterTxsNotOnLongestChain(ctx context.Context, logger ulogger.Logger, tSettings *settings.Settings, utxoStore txMetaBatchReader,
	blockchainClient longestChainClient, recentMainChainIDs map[uint32]bool, offChainBlockID uint32, hashes []chainhash.Hash, skipMissing bool) ([]chainhash.Hash, error) {
	notOnLongestChain := make([]chainhash.Hash, 0, len(hashes))
	inCurrentChain := make(map[string]bool) // CheckBlockIsInCurrentChain answers, by block id set

	items, err := batchReadTxMeta(ctx, logger, tSettings, utxoStore, hashes, fields.BlockIDs, fields.UnminedSince)
	if err != nil {
		return nil, errors.NewProcessingError("[FilterTxsNotOnLongestChain] error getting transactions from utxo store", err)
	}

	for _, item := range items {
		if item.Err != nil {
			if skipMissing && errors.Is(item.Err, errors.ErrTxNotFound) {
				continue
			}

			return nil, errors.NewProcessingError("[FilterTxsNotOnLongestChain] error getting transaction %s from utxo store", item.Hash.String(), item.Err)
		}

		txMeta := item.Data
		if txMeta == nil {
			if skipMissing {
				continue
			}

			// transaction not found, not OK
			return nil, errors.NewProcessingError("[FilterTxsNotOnLongestChain] transaction %s not found on longest chain", item.Hash.String())
		}

		if txMeta.UnminedSince != 0 {
			// already unmined: marking it would change nothing (issue 1768 keeps an existing value)
			continue
		}

		if len(txMeta.BlockIDs) == 1 && txMeta.BlockIDs[0] == offChainBlockID {
			// the transaction is only in the off-chain block, so it is definitely not on the longest chain
			notOnLongestChain = append(notOnLongestChain, item.Hash)
			continue
		}

		foundInRecentBlock := false
		for _, blockID := range txMeta.BlockIDs {
			if recentMainChainIDs[blockID] {
				// the transaction is still in one of the last 1000 blocks, so it is still on the longest chain
				foundInRecentBlock = true
				break
			}
		}

		if foundInRecentBlock {
			continue
		}

		// check BlockIDs to see if the transaction is still on the longest chain
		key := fmt.Sprint(txMeta.BlockIDs)

		onLongestChain, known := inCurrentChain[key]
		if !known {
			var err error
			if onLongestChain, err = blockchainClient.CheckBlockIsInCurrentChain(ctx, txMeta.BlockIDs); err != nil {
				return nil, errors.NewProcessingError("[FilterTxsNotOnLongestChain] error checking if transaction is on longest chain", err)
			}

			inCurrentChain[key] = onLongestChain
		}

		if !onLongestChain {
			notOnLongestChain = append(notOnLongestChain, item.Hash)
		}
	}

	return notOnLongestChain, nil
}

// FilterTxsUnmined returns the subset of hashes whose unmined_since is set and that are
// mined in blockID: the txs of that block a MarkTransactionsOnLongestChain(true) would
// change. Hashes missing from the UTXO store are skipped.
func FilterTxsUnmined(ctx context.Context, logger ulogger.Logger, tSettings *settings.Settings, utxoStore txMetaBatchReader, blockID uint32, hashes []chainhash.Hash) ([]chainhash.Hash, error) {
	items, err := batchReadTxMeta(ctx, logger, tSettings, utxoStore, hashes, fields.UnminedSince, fields.BlockIDs)
	if err != nil {
		return nil, errors.NewProcessingError("[FilterTxsUnmined] error getting transactions from utxo store", err)
	}

	unmined := make([]chainhash.Hash, 0, len(hashes))

	for _, item := range items {
		if item.Err != nil {
			if errors.Is(item.Err, errors.ErrTxNotFound) {
				continue
			}

			return nil, errors.NewProcessingError("[FilterTxsUnmined] error getting transaction %s from utxo store", item.Hash.String(), item.Err)
		}

		if item.Data != nil && item.Data.UnminedSince != 0 && slices.Contains(item.Data.BlockIDs, blockID) {
			unmined = append(unmined, item.Hash)
		}
	}

	return unmined, nil
}

// batchReadTxMeta reads the given fields of hashes in concurrent BatchDecorate batches. The
// per-item result, including a per-item error, is on each returned item.
func batchReadTxMeta(ctx context.Context, logger ulogger.Logger, tSettings *settings.Settings, utxoStore txMetaBatchReader,
	hashes []chainhash.Hash, readFields ...fields.FieldName) ([]*utxo.UnresolvedMetaData, error) {
	items := make([]*utxo.UnresolvedMetaData, len(hashes))
	for idx, hash := range hashes {
		items[idx] = &utxo.UnresolvedMetaData{Hash: hash, Idx: idx}
	}

	g, gCtx := errgroup.WithContext(ctx)
	// SafeSetLimit panics loudly on a zero limit (MaxMinedRoutines and GetBatcherSize both 0)
	// rather than letting g.Go deadlock on a zero-capacity semaphore.
	util.SafeSetLimit(logger, g, max(tSettings.UtxoStore.MaxMinedRoutines, tSettings.UtxoStore.GetBatcherSize*2))

	for start := 0; start < len(items); start += filterNotOnLongestChainBatchSize {
		batch := items[start:min(start+filterNotOnLongestChainBatchSize, len(items))]

		g.Go(func() error {
			return utxoStore.BatchDecorate(gCtx, batch, readFields...)
		})
	}

	if err := g.Wait(); err != nil {
		return nil, err
	}

	return items, nil
}
