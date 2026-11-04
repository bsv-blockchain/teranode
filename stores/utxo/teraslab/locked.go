package teraslab

import (
	"context"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
)

// SetLocked marks transactions as locked (value=true) or unlocked (value=false)
// for spending.
//
// Concurrent SetLocked() calls sharing the same value are coalesced into one
// wire SetLockedBatch RPC via setLockedBatcher (grouped by value in
// sendSetLockedBatch), then per-item errors are mapped back to this call by its
// index span — so a single sequential caller still works (a batch of one) and
// the returned error contract is identical to the unbatched path.
func (s *Store) SetLocked(ctx context.Context, txHashes []chainhash.Hash, value bool) error {
	if len(txHashes) == 0 {
		return nil
	}

	done := make(chan error, 1)
	s.setLockedBatcher.Put(&batchSetLockedCall{ctx: ctx, value: value, hashes: txHashes, done: done})

	select {
	case err := <-done:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}
