package smoke

import (
	"context"
	"encoding/hex"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/teranode/daemon"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/meta"
	"github.com/bsv-blockchain/teranode/test"
	"github.com/stretchr/testify/require"
)

// recordingUtxoStore counts the transaction writes and closes that reach the
// real UTXO store.
type recordingUtxoStore struct {
	utxo.Store
	spendAndCreates atomic.Int32
	closes          atomic.Int32
}

func (s *recordingUtxoStore) SpendAndCreate(ctx context.Context, tx *bt.Tx, blockHeight uint32, opts ...utxo.CreateOption) (*meta.Data, []*utxo.Spend, error) {
	s.spendAndCreates.Add(1)
	return s.Store.SpendAndCreate(ctx, tx, blockHeight, opts...)
}

func (s *recordingUtxoStore) Close(ctx context.Context) error {
	s.closes.Add(1)
	return s.Store.Close(ctx)
}

func crashTestOptions(store *recordingUtxoStore, restart bool) daemon.TestOptions {
	return daemon.TestOptions{
		EnableRPC:         true,
		EnableP2P:         true,
		EnableValidator:   true,
		SkipRemoveDataDir: restart,
		SettingsOverrideFunc: test.ComposeSettings(
			test.SystemTestSettings(),
		),
		StoreWrappers: daemon.StoreWrappers{
			UtxoStore: func(s utxo.Store) utxo.Store {
				store.Store = s
				return store
			},
		},
	}
}

// TestTestDaemonStoreWrappers checks that a StoreWrappers wrapper sits in front of
// the UTXO store every service uses, and that Stop closes the store through it.
func TestTestDaemonStoreWrappers(t *testing.T) {
	store := &recordingUtxoStore{}

	td := daemon.NewTestDaemon(t, crashTestOptions(store, false))

	require.Same(t, store, td.UtxoStore)

	coinbaseTx := td.MineToMaturityAndGetSpendableCoinbaseTx(t, td.Ctx)
	tx := td.CreateTransaction(t, coinbaseTx)

	_, err := td.CallRPC(td.Ctx, "sendrawtransaction", []any{hex.EncodeToString(tx.ExtendedBytes())})
	require.NoError(t, err)

	require.Positive(t, store.spendAndCreates.Load(), "the validator must write the transaction through the wrapper")

	td.Stop(t)

	require.EqualValues(t, 1, store.closes.Load(), "Stop must close the UTXO store through the wrapper")
}

// TestTestDaemonCrashRestart crashes a node, checks that no graceful store close
// ran and the store's database is gone, then restarts on the same data directory
// and checks that the committed chain survived and the node still mines.
func TestTestDaemonCrashRestart(t *testing.T) {
	store := &recordingUtxoStore{}

	td := daemon.NewTestDaemon(t, crashTestOptions(store, false))

	block3 := td.MineAndWait(t, 3)

	td.Crash(t)

	require.Zero(t, store.closes.Load(), "Crash must not close the UTXO store gracefully")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	_, err := store.Get(ctx, block3.CoinbaseTx.TxIDChainHash())
	require.Error(t, err, "the crashed UTXO store's database must be closed")

	td.Stop(t) // a no-op after Crash

	restarted := &recordingUtxoStore{}

	td = daemon.NewTestDaemon(t, crashTestOptions(restarted, true))
	defer td.Stop(t)

	td.WaitForBlockHeight(t, block3, blockWait, true)

	_, err = td.UtxoStore.Get(td.Ctx, block3.CoinbaseTx.TxIDChainHash())
	require.NoError(t, err, "a coinbase committed before the crash must survive it")

	block4 := td.MineAndWait(t, 1)
	require.EqualValues(t, 4, block4.Height)
}
