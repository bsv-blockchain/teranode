package txmetacache

import (
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/stretchr/testify/require"
)

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
}
