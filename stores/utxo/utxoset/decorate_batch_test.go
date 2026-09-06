package utxoset

import (
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/stretchr/testify/require"
)

// TestBatchDecorateFillsEveryEntryFromOneQuery is the read path that matters at volume.
//
// Subtree validation resolves thousands of transactions at a time, so asking one at a time
// would be one round trip each. Both other implementations of this interface funnel single
// reads into this call for exactly that reason.
func TestBatchDecorateFillsEveryEntryFromOneQuery(t *testing.T) {
	s, ctx := newTestStore(t)

	const n = 5

	unresolved := make([]*utxo.UnresolvedMetaData, 0, n)
	sizes := make([]uint64, 0, n)

	for i := 0; i < n; i++ {
		tx := mkTx(t, 1, uint64(1_000+i))
		_, err := s.Create(ctx, tx, 700_000)
		require.NoError(t, err)

		unresolved = append(unresolved, &utxo.UnresolvedMetaData{Hash: *tx.TxIDChainHash(), Idx: i})
		sizes = append(sizes, uint64(tx.Size()))
	}

	require.NoError(t, s.BatchDecorate(ctx, unresolved))

	for i, u := range unresolved {
		require.NoError(t, u.Err, "entry %d", i)
		require.NotNil(t, u.Data, "entry %d must be filled", i)
		require.Equal(t, sizes[i], u.Data.SizeInBytes, "entry %d", i)
		require.Equal(t, i, u.Idx, "the caller's index must survive, it is how results are matched back")
	}
}

// TestBatchDecorateReportsMissingEntriesIndividually.
//
// One absent transaction must not fail the whole batch. The validator turns a missing parent
// into a rejection for that transaction alone, and failing the batch would reject every
// transaction that happened to be resolved alongside it.
func TestBatchDecorateReportsMissingEntriesIndividually(t *testing.T) {
	s, ctx := newTestStore(t)

	known := mkTx(t, 1, 1_000)
	_, err := s.Create(ctx, known, 700_000)
	require.NoError(t, err)

	missing := mkTx(t, 1, 9_999)

	unresolved := []*utxo.UnresolvedMetaData{
		{Hash: *known.TxIDChainHash(), Idx: 0},
		{Hash: *missing.TxIDChainHash(), Idx: 1},
	}

	require.NoError(t, s.BatchDecorate(ctx, unresolved),
		"a missing entry is reported on that entry, not by failing the batch")

	require.NoError(t, unresolved[0].Err)
	require.NotNil(t, unresolved[0].Data)

	require.True(t, errors.Is(unresolved[1].Err, errors.ErrTxNotFound),
		"want ErrTxNotFound on the entry, got %v", unresolved[1].Err)
	require.Nil(t, unresolved[1].Data)
}

// TestSetLockedMarksAndClears covers the two-phase commit release.
//
// A transaction created for the mempool is locked, and unlocked once it is committed. That
// unlock is a single-hash call per transaction on the hot path, which is why the sql store
// batches it.
func TestSetLockedMarksAndClears(t *testing.T) {
	s, ctx := newTestStore(t)

	tx := mkTx(t, 1, 1_000)
	_, err := s.Create(ctx, tx, 700_000, utxo.WithLocked(true))
	require.NoError(t, err)

	h := *tx.TxIDChainHash()

	got, err := s.Get(ctx, &h)
	require.NoError(t, err)
	require.True(t, got.Locked, "created locked")

	require.NoError(t, s.SetLocked(ctx, []chainhash.Hash{h}, false))

	got, err = s.Get(ctx, &h)
	require.NoError(t, err)
	require.False(t, got.Locked, "released at commit")

	require.NoError(t, s.SetLocked(ctx, []chainhash.Hash{h}, true))

	got, err = s.Get(ctx, &h)
	require.NoError(t, err)
	require.True(t, got.Locked, "and it can be set again")
}

// TestSetLockedReachesTheCoinRowsToo.
//
// The flag lives on the transaction row AND on every coin the transaction created, because
// the spend path reads the coin row and never the transaction row. Setting only one of them
// would leave a transaction that reports itself locked while its coins are spendable.
func TestSetLockedReachesTheCoinRowsToo(t *testing.T) {
	s, ctx := newTestStore(t)

	tx := mkTx(t, 3, 1_000)
	_, err := s.Create(ctx, tx, 700_000)
	require.NoError(t, err)

	h := *tx.TxIDChainHash()
	require.NoError(t, s.SetLocked(ctx, []chainhash.Hash{h}, true))

	var lockedCoins int
	require.NoError(t, s.pool.QueryRow(ctx,
		`SELECT count(*) FROM utxo WHERE txid = $1 AND (flags & $2::smallint) <> 0`,
		h[:], FlagLocked).Scan(&lockedCoins))

	require.Equal(t, 3, lockedCoins,
		"every coin must carry it: the spend path reads the coin row, never the transaction row")
}
