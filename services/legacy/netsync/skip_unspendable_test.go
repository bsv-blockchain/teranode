package netsync

import (
	"context"
	"net/url"
	"sync"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	txmap "github.com/bsv-blockchain/go-tx-map"
	"github.com/bsv-blockchain/go-wire"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/services/legacy/bsvutil"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/bsv-blockchain/teranode/stores/utxo/meta"
	utxosql "github.com/bsv-blockchain/teranode/stores/utxo/sql"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/stretchr/testify/require"
)

// newSQLiteMemoryStore is a real sqlitememory UTXO store, a separate database per name.
func newSQLiteMemoryStore(t *testing.T, tSettings *settings.Settings, name string) utxo.Store {
	t.Helper()

	storeURL, err := url.Parse("sqlitememory:///" + name)
	require.NoError(t, err)

	store, err := utxosql.New(context.Background(), ulogger.TestLogger{}, tSettings, storeURL)
	require.NoError(t, err)

	return store
}

// createSpyStore is a real store that records which transactions createUtxos asked it to create.
type createSpyStore struct {
	utxo.Store
	mu      sync.Mutex
	created map[chainhash.Hash]bool
}

func (s *createSpyStore) SpendAndCreate(ctx context.Context, tx *bt.Tx, blockHeight uint32, opts ...utxo.CreateOption) (*meta.Data, []*utxo.Spend, error) {
	s.mu.Lock()
	s.created[*tx.TxIDChainHash()] = true
	s.mu.Unlock()

	return s.Store.SpendAndCreate(ctx, tx, blockHeight, opts...)
}

func (s *createSpyStore) was(tx *bt.Tx) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.created[*tx.TxIDChainHash()]
}

// twoTxMap holds one ordinary transaction and one whose only output is OP_FALSE OP_RETURN
// data, which is provably unspendable in every era. Each input carries an unlocking script,
// because the SQL store will not take an input without one.
func twoTxMap(t *testing.T) (normal, data *bt.Tx, m *txmap.SyncedMap[chainhash.Hash, *TxMapWrapper]) {
	t.Helper()

	in := &bt.Input{PreviousTxOutIndex: 0, SequenceNumber: 0xffffffff, PreviousTxSatoshis: 5_000,
		UnlockingScript: bscript.NewFromBytes([]byte{0x00})}
	require.NoError(t, in.PreviousTxIDAdd(&chainhash.Hash{1}))

	normal = &bt.Tx{Version: 1, Inputs: []*bt.Input{in}, Outputs: []*bt.Output{
		{Satoshis: 1_000, LockingScript: &bscript.Script{bscript.OpDUP, bscript.OpHASH160}},
	}}

	in2 := &bt.Input{PreviousTxOutIndex: 1, SequenceNumber: 0xffffffff, PreviousTxSatoshis: 5_000,
		UnlockingScript: bscript.NewFromBytes([]byte{0x00})}
	require.NoError(t, in2.PreviousTxIDAdd(&chainhash.Hash{2}))

	data = &bt.Tx{Version: 1, Inputs: []*bt.Input{in2}}
	require.NoError(t, data.AddOpReturnOutput([]byte("only data")))

	m = txmap.NewSyncedMap[chainhash.Hash, *TxMapWrapper](2)
	m.Set(*normal.TxIDChainHash(), &TxMapWrapper{Tx: normal})
	m.Set(*data.TxIDChainHash(), &TxMapWrapper{Tx: data})

	return normal, data, m
}

// TestCreateUtxos_SkipsUnspendableTransactionsBelowTheCheckpointWhenAsked.
//
// A transaction with no spendable outputs can never be spent, so below the checkpoint on a
// node with no block persister there is nothing to store: SV Node keeps no record of it at
// all. The quick-validation path already honours blockvalidation_skipUnspendableTxStorage-
// DuringCatchup; the legacy block path, which is how mainnet receives its blocks, did not, so
// the setting changed nothing there. Its inputs are still spent in the next phase, so the
// UTXO set is unaffected.
func TestCreateUtxos_SkipsUnspendableTransactionsBelowTheCheckpointWhenAsked(t *testing.T) {
	const checkpointHeight = int32(1000)

	block := bsvutil.NewBlock(&wire.MsgBlock{Header: wire.BlockHeader{Version: 1}})
	block.SetHeight(500)

	run := func(t *testing.T, name string, skipSetting, outpointOnly bool) (*bt.Tx, *bt.Tx, *createSpyStore) {
		t.Helper()

		tSettings, params := newOutpointOnlySettings(t, true, true, checkpointHeight)
		tSettings.BlockValidation.SkipUnspendableTxStorageDuringCatchup = skipSetting

		spy := &createSpyStore{Store: newSQLiteMemoryStore(t, tSettings, name), created: map[chainhash.Hash]bool{}}
		sm := &SyncManager{settings: tSettings, chainParams: params, logger: ulogger.TestLogger{}, utxoStore: spy}

		normal, data, m := twoTxMap(t)
		require.NoError(t, sm.createUtxos(context.Background(), m, testBlockIdent(block), 7, outpointOnly))

		return normal, data, spy
	}

	stored := func(t *testing.T, store utxo.Store, tx *bt.Tx) bool {
		t.Helper()

		md, err := store.Get(context.Background(), tx.TxIDChainHash(), fields.BlockIDs)
		if errors.Is(err, errors.ErrTxNotFound) {
			return false
		}

		require.NoError(t, err)
		require.Contains(t, md.BlockIDs, uint32(7), "a stored transaction is stored mined in the block")

		return true
	}

	t.Run("setting on, below checkpoint: data transaction is not stored", func(t *testing.T) {
		normal, data, spy := run(t, "skip_on", true, true)
		require.True(t, spy.was(normal), "a transaction with a spendable output is always stored")
		require.False(t, spy.was(data), "nothing can ever spend it and no persister needs it")
		require.True(t, stored(t, spy, normal))
		require.False(t, stored(t, spy, data))
	})

	t.Run("setting off: both are stored", func(t *testing.T) {
		normal, data, spy := run(t, "skip_off", false, true)
		require.True(t, spy.was(normal))
		require.True(t, spy.was(data))
		require.True(t, stored(t, spy, normal))
		require.True(t, stored(t, spy, data))
	})

	t.Run("above the checkpoint the setting does not apply", func(t *testing.T) {
		normal, data, spy := run(t, "above_checkpoint", true, false)
		require.True(t, spy.was(normal))
		require.True(t, spy.was(data), "at the tip the mempool, the stamp and the persister may all need the row")
		require.True(t, stored(t, spy, normal))
		require.True(t, stored(t, spy, data))
	})
}
