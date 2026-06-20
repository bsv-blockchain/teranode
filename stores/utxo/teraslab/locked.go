package teraslab

import (
	"context"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	teraslab "github.com/icellan/teraslab/client/go"
)

// SetLocked marks transactions as locked for spending.
func (s *Store) SetLocked(ctx context.Context, txHashes []chainhash.Hash, value bool) error {
	if len(txHashes) == 0 {
		return nil
	}

	txids := make([]teraslab.TxID, len(txHashes))
	for i, h := range txHashes {
		txids[i] = hashToTxID(&h)
	}

	_, err := s.client.SetLockedBatch(ctx, value, txids)
	if err != nil {
		if pe, ok := err.(*teraslab.PartialError); ok {
			return partialErrorToError("SetLocked", pe)
		}
		return err
	}

	return nil
}
