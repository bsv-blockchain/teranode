package utxo

import (
	"context"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// prometheusUtxoConflictingDemotionRefused counts transactions that conflict
// resolution declined to demote because they are mined in a block on the
// applying block's own ancestry. A non-zero value is never routine: it means a
// block was accepted whose conflict resolution would have reversed a confirmed
// spend, so it should alert.
var prometheusUtxoConflictingDemotionRefused = promauto.NewCounter(
	prometheus.CounterOpts{
		Namespace: "teranode",
		Subsystem: "utxo",
		Name:      "conflicting_demotion_refused_total",
		Help:      "Demotions refused because the losing transaction is mined on the applying block's ancestry",
	},
)

// AncestryCheck reports whether any of blockIDs is an ancestor of (or is) the
// block identified by blockHash.
//
// It is a plain func rather than a blockchain client so that this package keeps
// no dependency on services/blockchain. Callers pass
// blockchain.ClientI.CheckBlockIsAncestorOfBlock.
type AncestryCheck func(ctx context.Context, blockIDs []uint32, blockHash *chainhash.Hash) (bool, error)

// AncestryGuard carries the chain authorisation for conflict resolution. It is
// a struct rather than a bare func so that "no guard" is a value a caller has to
// name (NoAncestryGuard) instead of something obtained by omission — this guard
// is the security property of ProcessConflicting.
type AncestryGuard struct {
	check AncestryCheck
}

// NewAncestryGuard wraps a chain-ancestry check for ProcessConflicting.
func NewAncestryGuard(check AncestryCheck) AncestryGuard {
	return AncestryGuard{check: check}
}

// NoAncestryGuard disables chain authorisation, skipping the check and the
// store read that feeds it.
//
// There is no production use for this. The conformance-test harness in
// stores/utxo/tests uses it because it drives the store directly with no
// blockchain service behind it.
var NoAncestryGuard = AncestryGuard{}

// enabled reports whether this guard will actually consult a chain.
func (g AncestryGuard) enabled() bool { return g.check != nil }

// ProcessConflictingOption configures optional ProcessConflicting behaviour.
// The ancestry guard is NOT optional; it is a required parameter.
type ProcessConflictingOption func(*processConflictingOptions)

type processConflictingOptions struct {
	onRefused func(loser chainhash.Hash)
}

// WithRefusalHandler registers a callback invoked once per losing transaction
// whose demotion was refused, so the caller can act on the offending block out
// of band (see the note on failure handling in ProcessConflicting). It must not
// block or re-enter the store.
func WithRefusalHandler(onRefused func(loser chainhash.Hash)) ProcessConflictingOption {
	return func(o *processConflictingOptions) {
		o.onRefused = onRefused
	}
}

// filterAncestryConfirmedLosers removes from losers every transaction that is
// mined in a block on blockHash's own ancestry, and reports which ones it
// removed.
//
// # WHY THIS EXISTS
//
// Demoting a transaction is how Teranode resolves a double spend: the losing
// spend is marked conflicting, unspent, and replaced by the winner. That is
// correct when the loser was mined on a branch the node is abandoning — the
// ordinary reorg case — because the loser's block is then a sibling of, not an
// ancestor of, the block being applied.
//
// It is never correct when the loser is confirmed in the applying block's own
// ancestry. That would reverse a confirmed spend without a reorg: the loser
// stays in a block on the chain while the UTXO set is rewritten to say someone
// else spent the outpoint. A block whose conflict resolution demands that is an
// ancestor double spend and should have been rejected during validation.
//
// Scope and cost. The set tested here is whatever GetCounterConflicting
// returned, which is NOT just the immediate counter-spenders: it also contains
// their descendant cones, and it is walked with maxNodes = 0 (deliberately
// unbounded, see the demotion-path comments on the store implementations). It is
// therefore not safe to assume this list is small. The further expansion done by
// MarkConflictingRecursively is not tested — a descendant spends the loser's
// outputs, so it cannot be chain-confirmed while the loser is being demoted.
//
// Because of that, the ancestry question is asked ONCE for the union of every
// candidate block ID rather than once per loser. The query already accepts a
// list, and the common answer is "none of these are ancestors", which settles
// the whole set in a single round trip. Only when the union does contain an
// ancestor — which means the block is invalid and is about to be refused — is
// the per-loser attribution worth paying for.
//
// Direction of failure: a stale block ID for a block that has since been
// orphaned makes the guard return false, so the demotion proceeds. That is the
// safe direction — the guard can miss, but it cannot invent a refusal and stall
// a legitimate reorg.
func filterAncestryConfirmedLosers(ctx context.Context, s Store, losers []chainhash.Hash, winners map[chainhash.Hash]struct{},
	blockHash chainhash.Hash, guard AncestryGuard) (kept []chainhash.Hash, refused []chainhash.Hash, err error) {
	if len(losers) == 0 {
		return losers, nil, nil
	}

	// The winners must be excluded before anything is asked of the chain.
	// GetCounterConflictingTxHashes seeds its result with the transaction it was
	// asked about and never removes it, so every winner appears in its own
	// counter-conflicting set. A winner IS mined in the block being applied —
	// that is the whole point of the block — and the ancestry query counts the
	// target block itself as a match, so testing winners here would refuse every
	// legitimate conflict resolution.
	toCheck := make([]chainhash.Hash, 0, len(losers))
	kept = make([]chainhash.Hash, 0, len(losers))

	for _, loser := range losers {
		if _, isWinner := winners[loser]; isWinner {
			kept = append(kept, loser)
			continue
		}

		toCheck = append(toCheck, loser)
	}

	if len(toCheck) == 0 {
		return kept, nil, nil
	}

	// Loser block IDs are not read anywhere else in this function, so this is
	// the one additional round trip the guard costs. It is paid only when a
	// block actually carries conflicting transactions, which is rare.
	unresolved := make([]*UnresolvedMetaData, len(toCheck))
	for i := range toCheck {
		unresolved[i] = &UnresolvedMetaData{Hash: toCheck[i], Idx: i}
	}

	if err = s.BatchDecorate(ctx, unresolved, fields.BlockIDs); err != nil {
		return nil, nil, errors.NewProcessingError("[filterAncestryConfirmedLosers] failed to read losing tx block ids", err)
	}

	resolved := make([]*UnresolvedMetaData, 0, len(unresolved))
	seenBlockID := make(map[uint32]struct{}, len(unresolved))
	unionBlockIDs := make([]uint32, 0, len(unresolved))

	for _, u := range unresolved {
		// A transient backend failure must not become a bypass of this check.
		// BatchDecorate reports both "no such record" and "the store errored" in
		// the same field, and everywhere else in ProcessConflicting a store error
		// aborts so the operation is retried. Keep that property here: only a
		// genuinely absent record is treated as no-evidence.
		if u.Err != nil && !errors.Is(u.Err, errors.ErrTxNotFound) {
			return nil, nil, errors.NewProcessingError("[filterAncestryConfirmedLosers][%s] failed to read losing tx block ids", u.Hash.String(), u.Err)
		}

		// A loser whose record is genuinely gone is not evidence of an ancestor
		// double spend. The trailer outlives the records it names, so refusing
		// here would make a node reject a block its peers accept purely because
		// it pruned earlier.
		if u.Err != nil || u.Data == nil || len(u.Data.BlockIDs) == 0 {
			kept = append(kept, u.Hash)
			continue
		}

		resolved = append(resolved, u)

		for _, blockID := range u.Data.BlockIDs {
			if _, seen := seenBlockID[blockID]; seen {
				continue
			}

			seenBlockID[blockID] = struct{}{}

			unionBlockIDs = append(unionBlockIDs, blockID)
		}
	}

	if len(unionBlockIDs) == 0 {
		return kept, nil, nil
	}

	// The single question that settles the common case.
	anyOnAncestry, err := guard.check(ctx, unionBlockIDs, &blockHash)
	if err != nil {
		return nil, nil, errors.NewProcessingError("[filterAncestryConfirmedLosers] failed to check ancestry of losing txs", err)
	}

	if !anyOnAncestry {
		for _, u := range resolved {
			kept = append(kept, u.Hash)
		}

		return kept, nil, nil
	}

	// The block is invalid. Identify which transactions are responsible so the
	// refusal names them; this runs at most once per offending block.
	for _, u := range resolved {
		onAncestry, ancestryErr := guard.check(ctx, u.Data.BlockIDs, &blockHash)
		if ancestryErr != nil {
			return nil, nil, errors.NewProcessingError("[filterAncestryConfirmedLosers][%s] failed to check ancestry of losing tx", u.Hash.String(), ancestryErr)
		}

		if onAncestry {
			refused = append(refused, u.Hash)
			continue
		}

		kept = append(kept, u.Hash)
	}

	return kept, refused, nil
}
