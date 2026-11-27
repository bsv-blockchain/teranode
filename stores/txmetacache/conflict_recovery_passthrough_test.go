package txmetacache

import (
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/stretchr/testify/require"
)

type creatingConflictPublisherStore struct {
	utxo.Store
	evicted []chainhash.Hash
}

func (s *creatingConflictPublisherStore) CreatingConflictEvictor() func([]chainhash.Hash) {
	return func(hashes []chainhash.Hash) { s.evicted = append(s.evicted, hashes...) }
}

// The cache forwards the conflict recovery methods to its store without touching the cache.
func TestTxMetaCache_ForwardsConflictRecovery(t *testing.T) {
	c := newPerHashCache(t, Unallocated)
	ctx := t.Context()
	id := chainhash.Hash{1}

	require.NoError(t, c.SaveConflictRecovery(ctx, id, &utxo.ConflictRecovery{Version: 1, Phase: utxo.ConflictRecoveryPlanning}))

	r, err := c.LoadConflictRecovery(ctx, id)
	require.NoError(t, err)
	require.Nil(t, r)

	require.NoError(t, c.RestoreConflictSpends(ctx, nil, 1))
	require.NoError(t, c.SetConflictLocks(ctx, id, []chainhash.Hash{{2}}, true))
	require.NoError(t, c.ReleaseConflictLocks(ctx, id, []chainhash.Hash{{2}}))

	inner := &creatingConflictPublisherStore{Store: c.utxoStore}
	c.utxoStore = inner
	c.CreatingConflictEvictor()([]chainhash.Hash{id})
	require.Equal(t, []chainhash.Hash{id}, inner.evicted)
}
