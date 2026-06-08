package sql

import (
	"context"

	"github.com/bsv-blockchain/teranode/util/tracing"
)

// OffChainBlockIDs returns the complete set of block IDs known NOT to be on the
// current main chain — the in-memory off-chain (forked) set that backs
// CheckBlockIsInCurrentChain's O(1) negative lookup — together with the highest
// known block ID (maxBlockID).
//
// It is the batch, prefetch-friendly counterpart of CheckBlockIsInCurrentChain:
// instead of answering one candidate set per call, it hands the caller the whole
// negative set once so membership can be resolved locally. To stay
// consensus-equivalent the caller MUST apply the same two rules
// CheckBlockIsInCurrentChain applies: an ID is on the main chain iff
// (id <= maxBlockID) AND (id is not in the off-chain set). IDs above maxBlockID
// cannot exist yet and must NOT be treated as on-chain.
//
// rebuilding is true when the set must not be trusted:
//   - the in-memory chain check is disabled (useInMemoryChainCheck == false), or
//   - a main-chain rebuild is in progress (startup or reorg), during which the
//     set may be empty or stale.
//
// In both cases callers must fall back to per-block CheckBlockIsInCurrentChain,
// which has its own authoritative SQL path. A returned (nil, maxID, false, nil)
// means "the off-chain set is genuinely empty" — i.e. every known block at or
// below maxID is on the main chain — which is the common case on a healthy chain.
func (s *SQL) OffChainBlockIDs(ctx context.Context) ([]uint32, uint32, bool, error) {
	_, _, deferFn := tracing.Tracer("SyncManager").Start(ctx, "sql:OffChainBlockIDs")
	defer deferFn()

	if !s.useInMemoryChainCheck || s.mainChainRebuilding.Load() > 0 {
		return nil, 0, true, nil
	}

	// Read maxBlockID first so it is never newer than the off-chain snapshot:
	// a slightly-stale (lower) maxID only makes the caller's id > maxBlockID guard
	// more conservative (it falls through to the authoritative RPC), never less.
	maxID := uint32(s.maxBlockID.Load())

	// Grab the map pointer under the read lock and release immediately. The set is
	// published copy-on-write (rebuildOffChainSet swaps the whole map), so the
	// snapshot we hold is immutable and safe to range over after unlocking —
	// matching CheckBlockIsInCurrentChain and avoiding holding the lock across the
	// full copy.
	s.offChainBlockIDsMu.RLock()
	offChain := s.offChainBlockIDs
	s.offChainBlockIDsMu.RUnlock()

	ids := make([]uint32, 0, len(offChain))
	for id := range offChain {
		ids = append(ids, id)
	}

	return ids, maxID, false, nil
}
