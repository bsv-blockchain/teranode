package repository_test

import (
	"context"
	"net/url"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/services/asset/repository"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/stores/blob"
	blockchain_store "github.com/bsv-blockchain/teranode/stores/blockchain"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/meta"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// newRepoWithMockedUtxoStore wires a Repository whose UTXO store is a testify
// mock, so a test can hand back the exact degenerate *bt.Tx that the aerospike
// store produces for a transaction stored from a UTXO-set snapshot. The
// sqlitememory store cannot produce that shape, so a mock is the only way to
// reach the asset boundary with it.
func newRepoWithMockedUtxoStore(t *testing.T) (*repository.Repository, *utxo.MockUtxostore, blob.Store) {
	t.Helper()

	logger := ulogger.NewErrorTestLogger(t)
	tSettings := test.CreateBaseTestSettings(t)

	memoryURL, err := url.Parse("memory://")
	require.NoError(t, err)

	txStore, err := blob.NewStore(logger, memoryURL)
	require.NoError(t, err)

	subtreeStore, err := blob.NewStore(logger, memoryURL)
	require.NoError(t, err)

	blockStore, err := blob.NewStore(logger, memoryURL)
	require.NoError(t, err)

	blockChainStore, err := blockchain_store.NewStore(logger, &url.URL{Scheme: "sqlitememory"}, tSettings)
	require.NoError(t, err)

	blockchainClient, err := blockchain.NewLocalClient(logger, tSettings, blockChainStore, nil, nil)
	require.NoError(t, err)

	mockUtxoStore := &utxo.MockUtxostore{}

	repo, err := repository.NewRepository(logger, tSettings, mockUtxoStore, txStore, blockchainClient, nil, subtreeStore, blockStore, nil, nil)
	require.NoError(t, err)

	return repo, mockUtxoStore, txStore
}

// snapshotReconstruction builds the *bt.Tx that
// stores/utxo/aerospike/get.go getExternalTransaction returns when the .tx
// external blob is absent and only the .outputs (UTXO-set) blob remains: no
// inputs, zero Version, and nil *bt.Output at every index that was not a live
// UTXO at snapshot time.
func snapshotReconstruction() *bt.Tx {
	return &bt.Tx{
		Outputs: []*bt.Output{
			nil, // output 0 was already spent when the snapshot was taken
			{Satoshis: 5000, LockingScript: bscript.NewFromBytes([]byte{0x51})},
		},
	}
}

// TestGetTransaction_SnapshotReconstructionDoesNotPanic is the direct regression
// test for issue #1306. Before the fix, GetTransaction called
// txMeta.Tx.ExtendedBytes() on this shape and go-bt dereferenced the nil
// *bt.Output, which the HTTP layer recovered into a 500 with an empty body — and
// on the POST /subtree/:hash/txs path took the process down entirely.
func TestGetTransaction_SnapshotReconstructionDoesNotPanic(t *testing.T) {
	repo, mockUtxoStore, _ := newRepoWithMockedUtxoStore(t)

	hash, err := chainhash.NewHashFromStr("1111111111111111111111111111111111111111111111111111111111111111")
	require.NoError(t, err)

	mockUtxoStore.On("Get", mock.Anything, hash, mock.Anything).
		Return(&meta.Data{Tx: snapshotReconstruction()}, nil)

	var (
		txBytes []byte
		getErr  error
	)

	require.NotPanics(t, func() {
		txBytes, getErr = repo.GetTransaction(context.Background(), hash)
	}, "reconstruction from a UTXO-set snapshot must not reach go-bt serialization")

	// The full transaction is genuinely not retained by this node, and the
	// default txstore is null:///, so the honest answer is not-found rather than
	// a fabricated body.
	require.Error(t, getErr)
	require.Nil(t, txBytes)
	require.True(t, errors.Is(getErr, errors.ErrNotFound) || errors.Is(getErr, errors.ErrTxNotFound),
		"expected a not-found error the HTTP layer maps to 404, got %v", getErr)
}

// TestGetTransaction_WrongTxidIsRejected pins the second half of the gate. Every
// snapshot shape known today is input-less and so is already rejected by
// TxIsSerializable — including the one with no nil hole at all, a seeded coinbase
// whose trailing output was spent (cmd/seeder's restoreCoinbaseInput declines to
// restore the input, and PadUTXOsWithNil pads only to maxIndex+1). The txid
// comparison is what covers everything else: a record that serializes cleanly but
// is not the transaction that was asked for must not be served as HTTP 200,
// which would be strictly worse than the panic.
func TestGetTransaction_WrongTxidIsRejected(t *testing.T) {
	repo, mockUtxoStore, _ := newRepoWithMockedUtxoStore(t)

	requested, err := chainhash.NewHashFromStr("2222222222222222222222222222222222222222222222222222222222222222")
	require.NoError(t, err)

	// A tx that is structurally fine but is not the one that was asked for.
	notTheRequestedTx := &bt.Tx{
		Version:  1,
		LockTime: 0,
		Inputs:   []*bt.Input{{PreviousTxOutIndex: 0}},
		Outputs: []*bt.Output{
			{Satoshis: 1000, LockingScript: bscript.NewFromBytes([]byte{0x51})},
		},
	}
	require.False(t, notTheRequestedTx.TxIDChainHash().IsEqual(requested))

	mockUtxoStore.On("Get", mock.Anything, requested, mock.Anything).
		Return(&meta.Data{Tx: notTheRequestedTx, IsCoinbase: true}, nil)

	txBytes, getErr := repo.GetTransaction(context.Background(), requested)

	require.Error(t, getErr, "a tx whose txid does not match the request must not be served")
	require.Nil(t, txBytes)
}

// TestGetTransaction_ServesAMatchingTx guards against over-rejection: a complete
// transaction whose txid matches the request must still be served from the UTXO
// store, without falling through to the TxStore.
func TestGetTransaction_ServesAMatchingTx(t *testing.T) {
	repo, mockUtxoStore, _ := newRepoWithMockedUtxoStore(t)

	tx, err := bt.NewTxFromString("01000000010000000000000000000000000000000000000000000000000000000000000000ffffffff17030200002f6d312d65752f605f77009f74384816a31807ffffffff0100000000000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac00000000")
	require.NoError(t, err)

	hash := tx.TxIDChainHash()

	mockUtxoStore.On("Get", mock.Anything, hash, mock.Anything).
		Return(&meta.Data{Tx: tx, IsCoinbase: true}, nil)

	txBytes, err := repo.GetTransaction(context.Background(), hash)
	require.NoError(t, err)
	require.Equal(t, tx.ExtendedBytes(), txBytes)
}

// TestGetTransaction_NilTxFallsThroughToTxStore covers the latent nil deref at
// the same call site: the store can legitimately return a non-nil *meta.Data
// with a nil Tx, and before the fix that skipped the TxStore fallback entirely
// because the fallback sat inside the `if err == nil && txMeta != nil` branch.
func TestGetTransaction_NilTxFallsThroughToTxStore(t *testing.T) {
	repo, mockUtxoStore, _ := newRepoWithMockedUtxoStore(t)

	hash, err := chainhash.NewHashFromStr("3333333333333333333333333333333333333333333333333333333333333333")
	require.NoError(t, err)

	mockUtxoStore.On("Get", mock.Anything, hash, mock.Anything).
		Return(&meta.Data{Tx: nil}, nil)

	require.NotPanics(t, func() {
		_, _ = repo.GetTransaction(context.Background(), hash)
	}, "a nil Tx must fall through to the TxStore, not be dereferenced")
}

// TestGetTransactionMeta_KeepsUsableMetadata pins the deliberate asymmetry with
// GetTransaction: the metadata endpoint returns twelve fields and only the
// transaction body is unrecoverable, so it must keep serving the rest rather
// than 404 the whole response. It must not hand back an unserializable Tx,
// because the JSON handler marshals it and MarshalJSON reaches the same panic.
func TestGetTransactionMeta_KeepsUsableMetadata(t *testing.T) {
	repo, mockUtxoStore, _ := newRepoWithMockedUtxoStore(t)

	hash, err := chainhash.NewHashFromStr("4444444444444444444444444444444444444444444444444444444444444444")
	require.NoError(t, err)

	mockUtxoStore.On("Get", mock.Anything, hash, mock.Anything).
		Return(&meta.Data{
			Tx:           snapshotReconstruction(),
			Fee:          1234,
			SizeInBytes:  250,
			BlockIDs:     []uint32{7},
			BlockHeights: []uint32{900000},
		}, nil)

	var txMeta *meta.Data

	require.NotPanics(t, func() {
		txMeta, err = repo.GetTransactionMeta(context.Background(), hash)
	})
	require.NoError(t, err, "metadata is still useful when only the body is missing")
	require.NotNil(t, txMeta)

	require.Nil(t, txMeta.Tx, "an unserializable reconstruction must not be handed to a JSON marshaller")
	require.Equal(t, uint64(1234), txMeta.Fee)
	require.Equal(t, []uint32{900000}, txMeta.BlockHeights)
}
