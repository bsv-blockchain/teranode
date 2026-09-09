package utxoset

import (
	"context"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/stores/utxo"
)

// GetCounterConflicting names the transactions that actually took the coins a losing
// transaction wanted, plus everything descended from them.
//
// It delegates to the shared walk rather than answering with a statement of its own, and both
// reference stores do exactly the same. That is the right call here for the reason this store
// argues everywhere else: the predicates that decide which transactions a conflict demotes are
// consensus rules, and two copies of them is a defect waiting for one to be edited alone. A
// single statement would be faster, and a faster second opinion about consensus is not a trade
// worth making.
//
// What made this possible was not the delegation but the two reads underneath it. The walk asks
// each transaction what it spends and asks each parent who took its outputs, and until recently
// this store could answer neither: it read inputs from a body that ages out, and it never
// populated the per-output spend state at all. Both now come from sources that survive, the
// stored inpoints and the journal.
//
// Unbounded, matching both reference stores. This is the demotion path, and it must walk the
// full descendant set to completion, because a budget failure here would wedge block assembly
// on that block forever.
func (s *Store) GetCounterConflicting(ctx context.Context, txHash chainhash.Hash) ([]chainhash.Hash, error) {
	return utxo.GetCounterConflictingTxHashes(ctx, s, txHash, 0)
}

// GetConflictingChildren returns the transactions recorded as contesting this one's coins, and
// everything descended from them.
//
// Delegated for the same reason as above. Bounded by the configured node budget, matching both
// reference stores: this one is a read for reporting rather than the demotion path, so refusing
// an unbounded cone is better than walking one.
func (s *Store) GetConflictingChildren(ctx context.Context, hash chainhash.Hash) ([]chainhash.Hash, error) {
	return utxo.GetConflictingChildren(ctx, s, hash, s.settings.UtxoStore.ConflictingChildrenMaxNodes)
}
