package teraslab

import (
	"context"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	teraslab "github.com/icellan/teraslab/client/go"
)

// SetMinedMulti updates the block ID for multiple transactions that have been mined.
func (s *Store) SetMinedMulti(ctx context.Context, hashes []*chainhash.Hash, minedBlockInfo utxo.MinedBlockInfo) (map[chainhash.Hash][]uint32, error) {
	if len(hashes) == 0 {
		return nil, nil
	}

	txids := make([]teraslab.TxID, len(hashes))
	for i, h := range hashes {
		txids[i] = hashToTxID(h)
	}

	params := teraslab.SetMinedBatchParams{
		BlockID:              minedBlockInfo.BlockID,
		BlockHeight:          minedBlockInfo.BlockHeight,
		SubtreeIdx:           uint32(minedBlockInfo.SubtreeIdx), //nolint:gosec
		OnLongestChain:       minedBlockInfo.OnLongestChain,
		UnsetMined:           minedBlockInfo.UnsetMined,
		CurrentBlockHeight:   s.blockHeight.Load(),
		BlockHeightRetention: s.settings.GetUtxoStoreBlockHeightRetention(),
	}

	resp, err := s.client.SetMinedBatch(ctx, params, txids)
	if err != nil {
		if pe, ok := err.(*teraslab.PartialError); ok {
			// Build result map from successes even if there are partial errors
			result := make(map[chainhash.Hash][]uint32)
			if resp != nil {
				for _, success := range resp.Successes {
					if int(success.ItemIndex) < len(hashes) {
						result[*hashes[success.ItemIndex]] = success.BlockIDs
					}
				}
			}
			// Return the successfully-mined block IDs AND surface the per-item
			// failures — they must not be silently dropped on the mined path.
			return result, partialErrorToError("SetMinedMulti", pe)
		}
		return nil, err
	}

	// Build result map from successes
	result := make(map[chainhash.Hash][]uint32)
	if resp != nil {
		for _, success := range resp.Successes {
			if int(success.ItemIndex) < len(hashes) {
				result[*hashes[success.ItemIndex]] = success.BlockIDs
			}
		}
	}

	// If no successes reported, do a Get to retrieve block IDs.
	//
	// The interface postcondition (when UnsetMined is false and a nil error is
	// returned) is that every input hash appears in the result map. A fallback Get
	// that fails must therefore surface the error, not be swallowed with a Warn +
	// continue, which would return a silently-incomplete map and skip downstream
	// double-spend detection for the missing tx.
	if len(result) == 0 {
		for _, h := range hashes {
			data, err := s.Get(ctx, h)
			if err != nil {
				return result, errors.NewStorageError("teraslab: SetMinedMulti fallback Get failed for %s", h.String(), err)
			}
			if data != nil {
				result[*h] = data.BlockIDs
			}
		}
	}

	return result, nil
}

// MarkTransactionsOnLongestChain marks transactions as being on the longest chain or not.
func (s *Store) MarkTransactionsOnLongestChain(ctx context.Context, txHashes []chainhash.Hash, onLongestChain bool) error {
	if len(txHashes) == 0 {
		return nil
	}

	txids := make([]teraslab.TxID, len(txHashes))
	for i, h := range txHashes {
		txids[i] = hashToTxID(&h)
	}

	params := teraslab.MarkLongestChainParams{
		OnLongestChain:       onLongestChain,
		CurrentBlockHeight:   s.blockHeight.Load(),
		BlockHeightRetention: s.settings.GetUtxoStoreBlockHeightRetention(),
	}

	_, err := s.client.MarkLongestChainBatch(ctx, params, txids)
	if err != nil {
		if pe, ok := err.(*teraslab.PartialError); ok {
			// Reorg path: a per-item failure here means a tx the chain expects
			// could not be (un)marked — surface it rather than swallow it.
			return partialErrorToError("MarkTransactionsOnLongestChain", pe)
		}
		return err
	}

	return nil
}
