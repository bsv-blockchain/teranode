package recoverreplayedtransactions

import (
	"context"
	"net/url"
	"testing"

	"github.com/bsv-blockchain/teranode/model"
	blockchainsql "github.com/bsv-blockchain/teranode/stores/blockchain/sql"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/require"
)

func TestIdleGuardUsesPersistedState(t *testing.T) {
	ctx := context.Background()
	u, err := url.Parse("sqlitememory:///")
	require.NoError(t, err)
	chain, err := blockchainsql.New(ulogger.TestLogger{}, u, test.CreateBaseTestSettings(t))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, chain.Close(ctx)) })
	_, err = newIdleGuard(ctx, chain)
	require.Error(t, err)
	for _, state := range []string{"", "RUNNING", "CATCHINGBLOCKS", "idle", "IDLE\n", `"IDLE"`} {
		require.NoError(t, chain.SetState(ctx, "fsm_state", []byte(state)))
		_, err = newIdleGuard(ctx, chain)
		require.Error(t, err, state)
	}
	require.NoError(t, chain.SetState(ctx, "fsm_state", []byte("IDLE")))
	guard, err := newIdleGuard(ctx, chain)
	require.NoError(t, err)
	require.NoError(t, guard.Check(ctx))
	require.NoError(t, chain.SetState(ctx, "fsm_state", []byte("RUNNING")))
	require.ErrorContains(t, guard.Check(ctx), "persisted IDLE")
	require.NoError(t, chain.SetState(ctx, "fsm_state", []byte("IDLE")))
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	require.Error(t, guard.Check(canceled))
}

func TestIdleGuardStopsOnTipChangeAndUnavailableStore(t *testing.T) {
	ctx := context.Background()
	u, err := url.Parse("sqlitememory:///")
	require.NoError(t, err)
	chain, err := blockchainsql.New(ulogger.TestLogger{}, u, test.CreateBaseTestSettings(t))
	require.NoError(t, err)
	require.NoError(t, chain.SetState(ctx, "fsm_state", []byte("IDLE")))
	guard, err := newIdleGuard(ctx, chain)
	require.NoError(t, err)
	genesis, err := chain.GetBlockByID(ctx, 0)
	require.NoError(t, err)
	cb := genesis.CoinbaseTx.Clone()
	cb.LockTime++
	block := &model.Block{Header: &model.BlockHeader{Version: 1, HashPrevBlock: genesis.Hash(), HashMerkleRoot: cb.TxIDChainHash(), Timestamp: genesis.Header.Timestamp + 600, Bits: genesis.Header.Bits}, CoinbaseTx: cb, TransactionCount: 1, Height: 1}
	_, _, err = chain.StoreBlock(ctx, block, "test")
	require.NoError(t, err)
	require.ErrorContains(t, guard.Check(ctx), "tip changed")
	require.NoError(t, chain.Close(ctx))
	require.Error(t, guard.Check(ctx))
}
