package blockchain

import (
	"context"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo"
)

// CommittedBlockHashLookup adapts a client's GetBlockByID to
// utxo.CommittedBlockHashFunc: the hash of the block committed under an id, or
// nil when no committed block holds it. It only reads; it never reserves an id.
// A nil client gives a nil func, which utxo.ConfirmLeftovers reports as an
// error rather than guessing.
func CommittedBlockHashLookup(client ClientI) utxo.CommittedBlockHashFunc {
	if client == nil {
		return nil
	}

	return func(ctx context.Context, id uint64) (*chainhash.Hash, error) {
		block, err := client.GetBlockByID(ctx, id)
		if err != nil {
			if errors.Is(err, errors.ErrBlockNotFound) {
				return nil, nil
			}

			return nil, err
		}

		if block == nil || block.Header == nil {
			return nil, nil
		}

		return block.Hash(), nil
	}
}
