package sql

import (
	"context"
	"net/url"
	"path/filepath"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/bsv-blockchain/teranode/stores/utxo/tests"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/require"
)

func TestConflictRecoveryDescendants(t *testing.T) {
	store, _ := newTestStore(t)
	tests.ConflictRecoveryDescendants(t, store)
}

func TestConflictRecoveryForwardOwnBlock(t *testing.T) {
	store, _ := newTestStore(t)
	tests.ConflictRecoveryForwardOwnBlock(t, store)
}

func TestConflictRecoveryForeignLock(t *testing.T) {
	store, _ := newTestStore(t)
	tests.ConflictRecoveryForeignLock(t, store)
}

func TestConflictRecoveryFailures(t *testing.T) {
	store, _ := newTestStore(t)
	tests.ConflictRecoveryFailures(t, store)
}

func TestConflictRecoveryLockOwnership(t *testing.T) {
	store, _ := newTestStore(t)
	ctx := t.Context()
	parent := newExtendedTxWithOutputs(t, 1)
	_, _, err := store.SpendAndCreate(ctx, parent, 1, utxo.WithCreateOnly())
	require.NoError(t, err)
	parents := []chainhash.Hash{*parent.TxIDChainHash()}
	owner := chainhash.HashH([]byte("recovery-operation"))
	other := chainhash.HashH([]byte("other-operation"))
	require.NoError(t, store.SetConflictLocks(ctx, owner, parents, true))
	require.Error(t, store.SetConflictLocks(ctx, other, parents, false))
	require.NoError(t, store.SetConflictLocks(ctx, owner, parents, true))
	// A normal lock writer replaces the operation's ownership atomically.
	require.NoError(t, store.SetLocked(ctx, parents, true))
	require.Error(t, store.SetConflictLocks(ctx, owner, parents, false))
	m, err := store.Get(ctx, parent.TxIDChainHash(), fields.Locked)
	require.NoError(t, err)
	require.True(t, m.Locked)
}

func TestConflictRecoveryPersistence(t *testing.T) {
	store, _ := newTestStore(t)
	tests.ConflictRecoveryPersistence(t, store)
}

func TestConflictRecoveryReopen(t *testing.T) {
	ctx := t.Context()
	settings := test.CreateBaseTestSettings(t)
	settings.BatcherDrainMode = true
	location := &url.URL{Scheme: "sqlite", Path: filepath.Join(t.TempDir(), "recovery.db")}
	store, err := New(ctx, ulogger.TestLogger{}, settings, location)
	require.NoError(t, err)
	intent := utxo.ConflictIntent{Kind: utxo.ConflictIntentForward, BlockHash: chainhash.HashH([]byte("reopen")), Recovery: &utxo.ConflictRecovery{Version: 1, Phase: utxo.ConflictRecoveryPlanning}}
	require.NoError(t, store.BeginConflictIntent(ctx, intent))
	recovery := &utxo.ConflictRecovery{Version: 1, Phase: utxo.ConflictRecoveryMutating, Transactions: []utxo.ConflictTransaction{{Hash: intent.BlockHash, Conflicting: true}}}
	require.NoError(t, store.SaveConflictRecovery(ctx, intent.IntentID(), recovery))
	require.NoError(t, store.Close(ctx))
	reopened, err := New(ctx, ulogger.TestLogger{}, settings, location)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, reopened.Close(context.Background())) })
	loaded, err := reopened.LoadConflictRecovery(ctx, intent.IntentID())
	require.NoError(t, err)
	require.Equal(t, recovery, loaded)
	require.NoError(t, reopened.BeginConflictIntent(ctx, intent))
	loaded, err = reopened.LoadConflictRecovery(ctx, intent.IntentID())
	require.NoError(t, err)
	require.Equal(t, recovery, loaded)
}

func TestConflictRecoveryUnexpectedOwner(t *testing.T) {
	store, _ := newTestStore(t)
	tests.ConflictRecoveryUnexpectedOwner(t, store)
}

func TestConflictRecoveryRelatedWinners(t *testing.T) {
	store, _ := newTestStore(t)
	tests.ConflictRecoveryRelatedWinners(t, store)
}

func TestConflictRecoveryConfirmedWinner(t *testing.T) {
	store, _ := newTestStore(t)
	tests.ConflictRecoveryConfirmedWinner(t, store)
}
