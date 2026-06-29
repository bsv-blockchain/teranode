package teraslab

import (
	"context"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	teraslab "github.com/icellan/teraslab/client/go"
)

// QueryOldUnminedTransactions returns transaction hashes for unmined transactions older than the cutoff height.
func (s *Store) QueryOldUnminedTransactions(ctx context.Context, cutoffBlockHeight uint32) ([]chainhash.Hash, error) {
	txids, err := s.client.QueryOldUnmined(ctx, cutoffBlockHeight)
	if err != nil {
		return nil, err
	}

	hashes := make([]chainhash.Hash, len(txids))
	for i, txid := range txids {
		hashes[i] = chainhash.Hash(txid)
	}

	return hashes, nil
}

// PreserveTransactions marks transactions to be preserved from deletion until a specific block height.
func (s *Store) PreserveTransactions(ctx context.Context, txIDs []chainhash.Hash, preserveUntilHeight uint32) error {
	if len(txIDs) == 0 {
		return nil
	}

	txids := make([]teraslab.TxID, len(txIDs))
	for i, h := range txIDs {
		txids[i] = hashToTxID(&h)
	}

	_, err := s.client.PreserveTransactions(ctx, preserveUntilHeight, txids)
	if err != nil {
		if pe, ok := err.(*teraslab.PartialError); ok {
			// Map per-item failures to Teranode errors so the caller can branch on
			// errors.Is, matching every other mutation path in this package
			// (Unspend, SetLocked, SetMinedMulti). Returning the raw client type
			// would break the errors.Is chain.
			s.logger.Warnf("[TeraSlab] partial error during PreserveTransactions: %v", err)
			return partialErrorToError("PreserveTransactions", pe)
		}
		return err
	}

	return nil
}

// ProcessExpiredPreservations handles transactions whose preservation period has expired.
func (s *Store) ProcessExpiredPreservations(ctx context.Context, currentHeight uint32) error {
	_, err := s.client.ProcessExpiredPreservations(ctx, currentHeight, s.settings.GetUtxoStoreBlockHeightRetention())
	return err
}
