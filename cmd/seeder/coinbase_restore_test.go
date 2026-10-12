package seeder

import (
	"context"
	"net/url"
	"os"
	"path/filepath"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/services/utxopersister"
	utxosql "github.com/bsv-blockchain/teranode/stores/utxo/sql"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/require"
)

// newTestUtxoStore builds a fresh in-memory sqlite UTXO store.
func newTestUtxoStore(ctx context.Context, t *testing.T) *utxosql.Store {
	t.Helper()

	tSettings := test.CreateBaseTestSettings(t)

	storeURL, err := url.Parse("sqlitememory:///coinbase_restore")
	require.NoError(t, err)

	store, err := utxosql.New(ctx, ulogger.TestLogger{}, tSettings, storeURL)
	require.NoError(t, err)

	return store
}

// wrapperFromCoinbase builds a UTXOWrapper carrying only the given output
// indices of a coinbase tx, mimicking what the utxo-set file records (i.e. the
// unspent outputs only).
func wrapperFromCoinbase(cb *bt.Tx, txid *chainhash.Hash, height uint32, indices []int) *utxopersister.UTXOWrapper {
	w := &utxopersister.UTXOWrapper{
		TxID:     *txid,
		Height:   height,
		Coinbase: true,
	}

	for _, i := range indices {
		o := cb.Outputs[i]
		w.UTXOs = append(w.UTXOs, &utxopersister.UTXO{
			Index:  uint32(i), //nolint:gosec // small test indices
			Value:  o.Satoshis,
			Script: []byte(*o.LockingScript),
		})
	}

	return w
}

// A fully-unspent coinbase must be stored with its real coinbase input restored
// from the V2 utxo-headers, so the stored transaction is byte-faithful and
// hashes back to the coinbase txid the block commits to.
func TestProcessUTXO_RestoresCoinbaseInputWhenFullyUnspent(t *testing.T) {
	ctx := context.Background()
	store := newTestUtxoStore(ctx, t)

	cb := makeCoinbaseTx(t, 9, 9) // 3-output coinbase
	require.True(t, cb.IsCoinbase())

	txid := cb.TxIDChainHash()

	w := wrapperFromCoinbase(cb, txid, 5, []int{0, 1, 2})
	coinbaseTxs := map[chainhash.Hash]*bt.Tx{*txid: cb}

	require.NoError(t, processUTXO(ctx, store, w, coinbaseTxs, false))

	got, err := store.Get(ctx, txid)
	require.NoError(t, err)
	require.NotNil(t, got.Tx)

	require.Equal(t, uint32(1), got.Tx.Version, "coinbase version must be restored")
	require.Len(t, got.Tx.Inputs, 1, "coinbase input must be restored")
	require.True(t, got.Tx.IsCoinbase(), "stored tx must be a valid coinbase")
	require.Equal(t, txid.String(), got.Tx.TxIDChainHash().String(),
		"stored coinbase must hash to the real coinbase txid")
}

// A coinbase with a spent output (so the reconstruction is not byte-faithful)
// must fall back to the output-only record: no input is invented, the store
// keys off the override txid, and nothing panics.
func TestProcessUTXO_CoinbaseFallbackWhenPartiallySpent(t *testing.T) {
	ctx := context.Background()
	store := newTestUtxoStore(ctx, t)

	cb := makeCoinbaseTx(t, 11, 11)
	txid := cb.TxIDChainHash()

	// Output index 2 has been spent, so only 0 and 1 remain in the set. The
	// reconstruction has fewer outputs than the real coinbase and cannot hash
	// back to txid, so the input must not be restored.
	w := wrapperFromCoinbase(cb, txid, 5, []int{0, 1})
	coinbaseTxs := map[chainhash.Hash]*bt.Tx{*txid: cb}

	require.NoError(t, processUTXO(ctx, store, w, coinbaseTxs, false))

	got, err := store.Get(ctx, txid)
	require.NoError(t, err)
	require.NotNil(t, got.Tx)
	require.Empty(t, got.Tx.Inputs, "must not invent an input for a non-faithful reconstruction")
}

// loadCoinbaseTxs must recover real coinbase transactions from a committed V2
// utxo-headers seed file, keyed by their true txid. Guards against the bug where
// seeded coinbases were stored without their input.
func TestLoadCoinbaseTxs_RealSeedFile(t *testing.T) {
	headersFile := filepath.Join("..", "..", "seeds", "tstn",
		"000000007816d70ab70ef0c70cfedec376acbe9a633927bdd3ad283224bf1a88.utxo-headers")

	if _, err := os.Stat(headersFile); os.IsNotExist(err) {
		t.Skipf("seed file %s not present", headersFile)
	}

	coinbaseTxs, err := loadCoinbaseTxs(ulogger.TestLogger{}, headersFile)
	require.NoError(t, err)
	require.NotEmpty(t, coinbaseTxs, "V2 seed file must yield coinbase transactions")

	// Every recovered coinbase must be a valid coinbase and hash to its map key.
	for key, cb := range coinbaseTxs {
		require.True(t, cb.IsCoinbase(), "recovered tx %s is not a coinbase", key)
		require.Equal(t, key.String(), cb.TxIDChainHash().String(),
			"recovered coinbase does not hash to its key")
	}

	// The block-300 coinbase that rendered as a blank screen must be present with
	// its input intact.
	txid, err := chainhash.NewHashFromStr(
		"829a24ee083b972fa7288d219d4231d2bc20ad1b701d6d1583f3724386da3e2a")
	require.NoError(t, err)

	cb, ok := coinbaseTxs[*txid]
	require.True(t, ok, "block-300 coinbase must be recovered from the seed file")
	require.Len(t, cb.Inputs, 1, "recovered coinbase must carry its input")
	require.Equal(t,
		"0000000000000000000000000000000000000000000000000000000000000000",
		cb.Inputs[0].PreviousTxIDStr(), "coinbase input must reference the null prevout")
}
