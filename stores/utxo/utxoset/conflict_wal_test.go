package utxoset

import (
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/stretchr/testify/require"
)

// TestConflictIntentIDIsIndependentOfHashOrder is the property the whole log rests on.
//
// Begin is idempotent because the primary key is a hash over the operation's inputs rather
// than a generated id, and the hash sorts the transaction list before digesting it. That
// matters on the crash-retry path: the caller assembles its conflicting set from a map walk,
// so the same operation genuinely does arrive with the hashes in a different order each time.
// If the id moved with the order, every retry would insert a second row and startup replay
// would run the same resolution twice.
//
// The list itself must come back in the order it went in, unsorted, because it is the
// operation's own argument rather than a set the store owns.
func TestConflictIntentIDIsIndependentOfHashOrder(t *testing.T) {
	s, ctx := newTestStore(t)

	h1 := chainhash.HashH([]byte("intent-order-1"))
	h2 := chainhash.HashH([]byte("intent-order-2"))
	h3 := chainhash.HashH([]byte("intent-order-3"))

	block := chainhash.HashH([]byte("intent-order-block"))

	forward := utxo.ConflictIntent{
		Kind:        utxo.ConflictIntentForward,
		BlockHeight: 850_001,
		BlockHash:   block,
		TxHashes:    []chainhash.Hash{h3, h1, h2},
		StartedAt:   1_700_000_000_000_000_000,
	}

	require.NoError(t, s.BeginConflictIntent(ctx, forward))

	// The same operation, the same three hashes, a different order: one row, not two.
	reordered := forward
	reordered.TxHashes = []chainhash.Hash{h2, h3, h1}
	require.NoError(t, s.BeginConflictIntent(ctx, reordered))

	pending, err := s.PendingConflictIntents(ctx)
	require.NoError(t, err)
	require.Len(t, pending, 1, "a re-begin with the hashes reordered must hit the same primary key")

	got := pending[0]
	require.Equal(t, forward.IntentID(), got.IntentID())
	require.Equal(t, forward.BlockHash, got.BlockHash, "the block hash gates replay and must round-trip")
	require.Equal(t, []chainhash.Hash{h3, h1, h2}, got.TxHashes,
		"the hash list is the caller's argument and comes back in the order it was given")

	// A different block hash is a different operation, even with everything else equal.
	elsewhere := forward
	elsewhere.BlockHash = chainhash.HashH([]byte("intent-order-other-block"))
	require.NoError(t, s.BeginConflictIntent(ctx, elsewhere))

	pending, err = s.PendingConflictIntents(ctx)
	require.NoError(t, err)
	require.Len(t, pending, 2)

	require.NoError(t, s.CompleteConflictIntent(ctx, forward.IntentID()))
	require.NoError(t, s.CompleteConflictIntent(ctx, elsewhere.IntentID()))

	pending, err = s.PendingConflictIntents(ctx)
	require.NoError(t, err)
	require.Empty(t, pending)
}

// TestPendingConflictIntentsRejectsARaggedHashBlob pins the refusal rather than the
// truncation. A blob whose length is not a multiple of 32 cannot be split into the set the
// caller is about to act on, and serving a prefix of it would hand block assembly a subset
// of the transactions to resolve and let it call the resolution done.
func TestPendingConflictIntentsRejectsARaggedHashBlob(t *testing.T) {
	s, ctx := newTestStore(t)

	id := chainhash.HashH([]byte("ragged-intent"))
	block := chainhash.HashH([]byte("ragged-block"))

	_, err := s.pool.Exec(ctx, `
		INSERT INTO conflict_intents (intent_id, kind, block_height, block_hash, tx_hashes, started_at)
		VALUES ($1, 'forward', 10, $2, $3, 1)`, id[:], block[:], []byte{1, 2, 3})
	require.NoError(t, err)

	_, err = s.PendingConflictIntents(ctx)
	require.Error(t, err)
	require.Contains(t, err.Error(), "not a multiple of")
}
