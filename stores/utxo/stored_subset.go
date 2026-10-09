package utxo

import (
	"context"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
)

// StoredTxProber is an optional store capability: a cheap answer to "which of these
// transactions did a create with no block information leave in the store".
//
// StoredSubset asks it on every block below the checkpoint, about transactions that are almost
// always absent, so a store whose general read walks several tables for a miss can answer this
// narrower question from the one place such a create writes. A store without it is asked
// through BatchDecorate. A wrapper store forwards it to the store it wraps.
type StoredTxProber interface {
	// StoredTxs returns the members of hashes the store holds a record of, in any order. A
	// hash it has never seen is left out, not reported as an error. hashes can be every skipped
	// transaction in a block, so an implementation splits it into reads its backend can take.
	StoredTxs(ctx context.Context, hashes []*chainhash.Hash) ([]chainhash.Hash, error)
}

// StoredSubset returns the members of hashes the store holds, in their original order, and
// drops those it has never seen.
//
// It is for transactions a block path chose not to store, those with no spendable outputs below
// the checkpoint. They are normally absent, but an earlier attempt at the same block that failed
// in subtree validation can have stored them as unmined, and nothing else would ever mark them
// mined: on 2026-09-24 that left 352 mined data transactions unmined for good on mainnet. The
// caller marks the returned ones mined with the rest of the block.
//
// Whether any were stored cannot be read off the rest of the block: an earlier attempt may have
// got only as far as a skipped transaction, so every skipped hash is asked about, on every
// attempt. A store that implements StoredTxProber answers that itself. Any other store is asked
// through BatchDecorate, in batches of storedSubsetDecorateBatchSize, and every store answers a
// miss on the entry itself rather than by failing the call. Either way a first attempt pays
// batched reads and no write. A store failure, whole-call or on one entry, is returned.
func StoredSubset(ctx context.Context, store Store, hashes []*chainhash.Hash) ([]*chainhash.Hash, error) {
	if prober, ok := store.(StoredTxProber); ok {
		return storedSubsetByProbe(ctx, prober, hashes)
	}

	return StoredSubsetByDecorate(ctx, store, hashes)
}

func storedSubsetByProbe(ctx context.Context, prober StoredTxProber, hashes []*chainhash.Hash) ([]*chainhash.Hash, error) {
	asked := make([]*chainhash.Hash, 0, len(hashes))

	for _, h := range hashes {
		if h != nil {
			asked = append(asked, h)
		}
	}

	if len(asked) == 0 {
		return nil, nil
	}

	held, err := prober.StoredTxs(ctx, asked)
	if err != nil {
		return nil, err
	}

	if len(held) == 0 {
		return nil, nil
	}

	set := make(map[chainhash.Hash]struct{}, len(held))
	for _, h := range held {
		set[h] = struct{}{}
	}

	var stored []*chainhash.Hash

	for _, h := range asked {
		if _, ok := set[*h]; ok {
			stored = append(stored, h)
		}
	}

	return stored, nil
}

// storedSubsetDecorateBatchSize bounds one BatchDecorate call. A legacy block can skip hundreds
// of thousands of transactions, and every other block-sized BatchDecorate caller splits its
// reads the same way, at the default of blockvalidation_processTxMetaUsingStore_BatchSize, so
// that one call never becomes a single aerospike batch the size of the block.
const storedSubsetDecorateBatchSize = 1024

// StoredSubsetByDecorate is StoredSubset through BatchDecorate alone. It is what a store
// without StoredTxProber gets, and what a wrapper store uses to forward StoredTxs to a store it
// wraps that has no prober of its own.
//
// Only a not-found answer on an entry means the store does not hold that transaction. Any other
// error on an entry, such as an aerospike record timeout, is returned rather than read as
// absent: reading it as absent would leave a stored transaction unmined for good, which is the
// failure this lookup exists to prevent.
func StoredSubsetByDecorate(ctx context.Context, store Store, hashes []*chainhash.Hash) ([]*chainhash.Hash, error) {
	items := make([]*UnresolvedMetaData, 0, len(hashes))

	for i, h := range hashes {
		if h == nil {
			continue
		}

		items = append(items, &UnresolvedMetaData{Hash: *h, Idx: i, Fields: []fields.FieldName{fields.BlockIDs}})
	}

	if len(items) == 0 {
		return nil, nil
	}

	for start := 0; start < len(items); start += storedSubsetDecorateBatchSize {
		end := min(start+storedSubsetDecorateBatchSize, len(items))

		if err := store.BatchDecorate(ctx, items[start:end], fields.BlockIDs); err != nil {
			return nil, err
		}
	}

	var stored []*chainhash.Hash

	for _, it := range items {
		if it.Err != nil {
			if errors.Is(it.Err, errors.ErrTxNotFound) {
				continue
			}

			return nil, errors.NewProcessingError("failed to look up transaction %s", it.Hash.String(), it.Err)
		}

		// A store that answers nothing at all, with no error either, has not found it.
		if it.Data != nil {
			stored = append(stored, hashes[it.Idx])
		}
	}

	return stored, nil
}

// ForwardStoredTxs is StoredTxs for a wrapper store: it asks the wrapped store's own prober
// when it has one and falls back to BatchDecorate on the wrapped store otherwise, so the
// wrapper's own BatchDecorate, and any cache behind it, is never involved.
func ForwardStoredTxs(ctx context.Context, wrapped Store, hashes []*chainhash.Hash) ([]chainhash.Hash, error) {
	if prober, ok := wrapped.(StoredTxProber); ok {
		return prober.StoredTxs(ctx, hashes)
	}

	stored, err := StoredSubsetByDecorate(ctx, wrapped, hashes)
	if err != nil {
		return nil, err
	}

	out := make([]chainhash.Hash, 0, len(stored))
	for _, h := range stored {
		out = append(out, *h)
	}

	return out, nil
}
