package pebble

import (
	"context"
	"net/url"
	"path/filepath"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/tests"
	"github.com/stretchr/testify/require"
)

// txWithDataOutput builds [P2PKH, OP_FALSE OP_RETURN, P2PKH]: outputCount 3,
// page0Count 2, because ShouldStoreOutputAsUTXO rejects the middle output.
func txWithDataOutput(t testing.TB, seed uint64) *bt.Tx {
	t.Helper()

	tx := bt.NewTx()
	require.NoError(t, tx.FromUTXOs(&bt.UTXO{
		TxIDHash:      tests.Tx.TxIDChainHash(),
		Vout:          0,
		LockingScript: tests.Tx.Outputs[0].LockingScript,
		Satoshis:      tests.Tx.Outputs[0].Satoshis,
	}))
	tx.Inputs[0].UnlockingScript = bscript.NewFromBytes([]byte{0x00, 0x48, 0x30, 0x45})

	require.NoError(t, tx.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", seed))

	data := &bscript.Script{}
	require.NoError(t, data.AppendOpcodes(bscript.OpFALSE, bscript.OpRETURN))
	require.NoError(t, data.AppendPushDataString("teranode"))
	tx.AddOutput(&bt.Output{Satoshis: 0, LockingScript: data})

	require.NoError(t, tx.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 1000))

	return tx
}

// TestOutpointOnlySpendRejectsUnspendableOutput pins icellan finding 2. An
// outpoint-only spend of a provably unspendable output must be refused, and must
// not advance spentCount, because reaching page0Count stamps the whole record
// for deletion while a real output is still unspent.
func TestOutpointOnlySpendRejectsUnspendableOutput(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	require.NoError(t, store.SetBlockHeight(100))

	tx := txWithDataOutput(t, 41_000)

	_, err := store.Create(ctx, tx, 100, utxo.WithMinedBlockInfo(utxo.MinedBlockInfo{
		BlockID: 7, BlockHeight: 100, SubtreeIdx: 0, OnLongestChain: true,
	}))
	require.NoError(t, err)

	m, err := store.getMaster(tx.TxIDChainHash())
	require.NoError(t, err)
	require.Equal(t, uint32(3), m.outputCount)
	require.Equal(t, uint32(2), m.page0Count, "the data output must not be counted as spendable")

	// Spend the data output alone, outpoint-only.
	_, err = store.Spend(ctx, newSpendingTx(t, tx, 1), 101, utxo.IgnoreFlags{SkipUTXOHashCheck: true})
	require.Error(t, err, "spending a provably unspendable output must be refused")
	require.True(t, errors.Is(err, errors.ErrTxNotFound), "expected ErrTxNotFound, got %v", err)

	m, err = store.getMaster(tx.TxIDChainHash())
	require.NoError(t, err)
	require.Zero(t, m.spentCount, "a refused spend must not advance spentCount")
	require.Zero(t, m.deleteAtHeight, "a refused spend must not stamp the record for deletion")

	// The real outputs must still be spendable.
	_, err = store.Spend(ctx, newSpendingTx(t, tx, 0), 101, utxo.IgnoreFlags{SkipUTXOHashCheck: true})
	require.NoError(t, err)

	// Spending both real outputs is what legitimately completes the record.
	_, err = store.Spend(ctx, newSpendingTx(t, tx, 2), 101, utxo.IgnoreFlags{SkipUTXOHashCheck: true})
	require.NoError(t, err)

	m, err = store.getMaster(tx.TxIDChainHash())
	require.NoError(t, err)
	require.Equal(t, m.page0Count, m.spentCount)
	require.NotZero(t, m.deleteAtHeight, "the record is stamped only once every real utxo is spent")
}

// TestUnspendableSlotDoesNotCompletePage pins the second route in finding 2: a
// phantom write into a mixed page must not donate a completion credit that
// pushes pagesSpent to pagesTotal one real spend early.
func TestUnspendableSlotDoesNotCompletePage(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	require.NoError(t, store.SetBlockHeight(100))

	// Build a tx whose overflow page holds a data output alongside real ones.
	tx := bt.NewTx()
	require.NoError(t, tx.FromUTXOs(&bt.UTXO{
		TxIDHash:      tests.Tx.TxIDChainHash(),
		Vout:          0,
		LockingScript: tests.Tx.Outputs[0].LockingScript,
		Satoshis:      tests.Tx.Outputs[0].Satoshis,
	}))
	tx.Inputs[0].UnlockingScript = bscript.NewFromBytes([]byte{0x00, 0x48, 0x30, 0x45})

	total := int(store.pageSize) + 3
	for i := 0; i < total; i++ {
		if i == int(store.pageSize)+1 {
			data := &bscript.Script{}
			require.NoError(t, data.AppendOpcodes(bscript.OpFALSE, bscript.OpRETURN))
			require.NoError(t, data.AppendPushDataString("pad"))
			tx.AddOutput(&bt.Output{Satoshis: 0, LockingScript: data})

			continue
		}

		require.NoError(t, tx.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", uint64(52_000+i)))
	}

	mustCreate(t, store, tx, 100)

	dataVout := store.pageSize + 1

	_, err := store.Spend(ctx, newSpendingTx(t, tx, dataVout), 101, utxo.IgnoreFlags{SkipUTXOHashCheck: true})
	require.Error(t, err)
	require.True(t, errors.Is(err, errors.ErrTxNotFound), "got %v", err)

	m, err := store.getMaster(tx.TxIDChainHash())
	require.NoError(t, err)
	require.Zero(t, m.pagesSpent, "a refused spend must not credit page completion")
}

// TestStoreDirResolvesUnderDataFolder pins icellan finding 7. url.Host carries
// the first path component of pebble://data/utxostore, so resolving from Path
// alone dropped "data" and opened the store at the filesystem root.
func TestStoreDirResolvesUnderDataFolder(t *testing.T) {
	tSettings := settings.NewSettings()
	tSettings.DataFolder = t.TempDir()

	for _, tc := range []struct {
		name string
		raw  string
		want string
	}{
		{name: "host carries the first component", raw: "pebble://data/utxostore", want: "data/utxostore"},
		{name: "sqlite-style triple slash", raw: "pebble:///utxostore", want: "utxostore"},
		{name: "nested path", raw: "pebble:///teranode1/utxostore", want: "teranode1/utxostore"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			storeURL, err := url.Parse(tc.raw)
			require.NoError(t, err)

			dir, err := storeDir(tSettings, storeURL)
			require.NoError(t, err)

			require.Equal(t, filepath.Join(tSettings.DataFolder, tc.want), dir)
			require.DirExists(t, dir)

			// Never the filesystem root.
			require.NotEqual(t, "/"+tc.want, dir)
		})
	}
}
