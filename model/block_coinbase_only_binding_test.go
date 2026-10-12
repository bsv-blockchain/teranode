package model

import (
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/stretchr/testify/require"
)

// TestCheckCoinbaseOnlyBodyBound covers the single definition of the coinbase-only binding
// (bitcoin-sv/teranode#4692), which Valid and both quick-validation entry points now share. Each
// clause is exercised directly, because the merkle comparison returns first: any fixture whose
// header root does not equal the coinbase txid never reaches the transaction-count check, so the
// count clause is unreachable through the merkle-mismatch fixtures the callers' own tests use.
//
// Mutation proof: dropping the TransactionCount clause reddens the third case; dropping the merkle
// comparison reddens the second; returning an error unconditionally reddens the first and fourth.
func TestCheckCoinbaseOnlyBodyBound(t *testing.T) {
	coinbase, err := bt.NewTxFromString(CoinbaseHex)
	require.NoError(t, err)

	// newBody builds the minimal shape the rule reads: a header merkle root, a coinbase, a subtree
	// list and a claimed transaction count. Nothing else is touched, because nothing else is read.
	newBody := func(merkleRoot *chainhash.Hash, subtrees []*chainhash.Hash, txCount uint64) *Block {
		return &Block{
			Header:           &BlockHeader{HashPrevBlock: &chainhash.Hash{}, HashMerkleRoot: merkleRoot},
			CoinbaseTx:       coinbase,
			Subtrees:         subtrees,
			TransactionCount: txCount,
		}
	}

	otherRoot := chainhash.Hash{0xAB}

	t.Run("subtrees present: no-op, whatever the root says", func(t *testing.T) {
		// Those bodies are bound by CheckMerkleRoot instead, so this rule must not judge them.
		require.NoError(t, newBody(&otherRoot, []*chainhash.Hash{{0x01}}, 99).CheckCoinbaseOnlyBodyBound())
	})

	t.Run("no subtrees, root is not the coinbase txid: corrupt", func(t *testing.T) {
		err := newBody(&otherRoot, nil, 1).CheckCoinbaseOnlyBodyBound()
		require.Error(t, err)
		require.True(t, errors.IsBlockCorrupt(err), "an emptied subtree list is a corrupt body, got: %v", err)
		require.False(t, errors.Is(err, errors.ErrBlockInvalid), "an unbound body must never condemn the hash")
		require.Contains(t, err.Error(), "the header merkle root is not the coinbase txid")
	})

	t.Run("no subtrees, root matches, but the body claims more than one transaction: corrupt", func(t *testing.T) {
		// The only fixture that reaches the consistency clause: the merkle comparison must PASS for
		// the count to be looked at, which is why it is built here rather than in the quick-path test.
		err := newBody(coinbase.TxIDChainHash(), nil, 2).CheckCoinbaseOnlyBodyBound()
		require.Error(t, err)
		require.True(t, errors.IsBlockCorrupt(err), "a self-contradictory body shape is corrupt, got: %v", err)
		require.Contains(t, err.Error(), "claims 2 transactions")
	})

	t.Run("genuine coinbase-only body: bound, no error", func(t *testing.T) {
		require.NoError(t, newBody(coinbase.TxIDChainHash(), nil, 1).CheckCoinbaseOnlyBodyBound())
		// A count of 0 is what an in-memory fixture that never set it carries; it must not be rejected.
		require.NoError(t, newBody(coinbase.TxIDChainHash(), nil, 0).CheckCoinbaseOnlyBodyBound())
	})
}
