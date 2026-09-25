package utxo

import (
	"context"
	"sync/atomic"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"golang.org/x/sync/errgroup"
)

// SetMinedIfPresent records info against each of hashes the store holds, and ignores those it
// has never seen. It returns how many it marked.
//
// It is for transactions a block path chose not to store, those with no spendable outputs below
// the checkpoint. They are normally absent, but an earlier attempt at the same block that failed
// in subtree validation can have stored them as unmined, and nothing else would ever mark them
// mined: on 2026-09-24 that left 352 mined data transactions unmined for good on mainnet. Each
// hash is asked about on its own, because a store answers a batch naming an unknown transaction
// with not found for the whole batch. A store failure other than not found is returned.
func SetMinedIfPresent(ctx context.Context, store Store, hashes []*chainhash.Hash, info MinedBlockInfo, maxWorkers int) (int, error) {
	if len(hashes) == 0 {
		return 0, nil
	}

	var marked atomic.Int64

	g, gCtx := errgroup.WithContext(ctx)
	g.SetLimit(max(1, maxWorkers))

	for _, h := range hashes {
		if h == nil {
			continue
		}

		g.Go(func() error {
			if _, err := store.SetMinedMulti(gCtx, []*chainhash.Hash{h}, info); err != nil {
				if errors.Is(err, errors.ErrTxNotFound) || errors.Is(err, errors.ErrNotFound) {
					return nil
				}

				return err
			}

			marked.Add(1)

			return nil
		})
	}

	if err := g.Wait(); err != nil {
		return 0, err
	}

	return int(marked.Load()), nil
}
