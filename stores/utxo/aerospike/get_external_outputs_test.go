package aerospike

import (
	"context"
	"io"
	"sync"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/go-chaincfg"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/services/utxopersister"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/stores/blob"
	"github.com/bsv-blockchain/teranode/stores/blob/memory"
	"github.com/bsv-blockchain/teranode/stores/blob/options"
	"github.com/bsv-blockchain/teranode/stores/utxo/meta"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/stretchr/testify/require"
)

// failingReadStore wraps a blob store and replaces the GetIoReader result for one
// file type, so a test can distinguish "the object is not there" from "the store
// failed".
type failingReadStore struct {
	blob.Store

	failFileType fileformat.FileType
	failWith     error
}

func (f *failingReadStore) GetIoReader(ctx context.Context, key []byte, fileType fileformat.FileType, opts ...options.FileOption) (io.ReadCloser, error) {
	if fileType == f.failFileType {
		return nil, f.failWith
	}

	return f.Store.GetIoReader(ctx, key, fileType, opts...)
}

// recordingReadStore counts GetIoReader calls per file type, and can fail one of
// them, so a test can assert which representations were consulted rather than only
// what came back. Proving a blob was never read is the point: "it hard-fails" and
// "it hard-fails without quietly reaching for the other representation" are
// different guarantees, and only the second is the one being made here.
type recordingReadStore struct {
	blob.Store

	mu     sync.Mutex
	counts map[fileformat.FileType]int

	failFileType fileformat.FileType
	failWith     error
}

func (r *recordingReadStore) GetIoReader(ctx context.Context, key []byte, fileType fileformat.FileType, opts ...options.FileOption) (io.ReadCloser, error) {
	r.mu.Lock()
	if r.counts == nil {
		r.counts = map[fileformat.FileType]int{}
	}
	r.counts[fileType]++
	r.mu.Unlock()

	if r.failWith != nil && fileType == r.failFileType {
		return nil, r.failWith
	}

	return r.Store.GetIoReader(ctx, key, fileType, opts...)
}

func (r *recordingReadStore) reads(fileType fileformat.FileType) int {
	r.mu.Lock()
	defer r.mu.Unlock()

	return r.counts[fileType]
}

// newOutputsOnlyStore builds a store whose external blob store holds only the
// .outputs (UTXO-set) representation of a transaction: the shape written by
// create.go for an input-less transaction, which is what cmd/seeder produces when
// bootstrapping from a snapshot. liveIndexes are the output indexes that were
// still unspent when the snapshot was taken.
func newOutputsOnlyStore(t *testing.T, liveIndexes ...uint32) (*Store, blob.Store, *bt.Tx) {
	t.Helper()

	chainParams := chaincfg.RegressionNetParams
	tSettings := &settings.Settings{}
	tSettings.ChainCfgParams = &chainParams

	mem := memory.New()
	t.Cleanup(func() { require.NoError(t, mem.Close(context.Background())) })

	s := &Store{
		ctx:           context.Background(),
		externalStore: mem,
		settings:      tSettings,
		logger:        ulogger.TestLogger{},
	}

	// A transaction with three outputs, of which only liveIndexes survive.
	tx := &bt.Tx{
		Version: 1,
		Inputs:  []*bt.Input{{PreviousTxOutIndex: 0}},
		Outputs: []*bt.Output{
			{Satoshis: 1000, LockingScript: bscript.NewFromBytes([]byte{0x51})},
			{Satoshis: 2000, LockingScript: bscript.NewFromBytes([]byte{0x52})},
			{Satoshis: 3000, LockingScript: bscript.NewFromBytes([]byte{0x53})},
		},
	}

	txHash := *tx.TxIDChainHash()

	wrapper := utxopersister.UTXOWrapper{
		TxID:   txHash,
		Height: 1,
	}

	for _, idx := range liveIndexes {
		wrapper.UTXOs = append(wrapper.UTXOs, &utxopersister.UTXO{
			Index:  idx,
			Value:  tx.Outputs[idx].Satoshis,
			Script: *tx.Outputs[idx].LockingScript,
		})
	}

	require.NoError(t, mem.Set(context.Background(), txHash[:], fileformat.FileTypeOutputs, wrapper.Bytes()))

	return s, mem, tx
}

// TestGetExternalTransaction_OutputsFallbackShape documents the shape at the root
// of issue 1306. The .outputs blob retains only the outputs that were live at
// snapshot time — no inputs, no version, no locktime, and no record that a spent
// output ever existed — so the reconstruction cannot be the transaction. It is
// still exactly what the outpoint-decoration and validator-extension callers need,
// which is why the fallback exists.
func TestGetExternalTransaction_OutputsFallbackShape(t *testing.T) {
	// Output 0 was already spent at snapshot time; 1 and 2 survived, so the hole
	// at index 0 stays in bounds.
	s, _, tx := newOutputsOnlyStore(t, 1, 2)

	got, err := s.getExternalTransaction(context.Background(), *tx.TxIDChainHash())
	require.NoError(t, err)

	require.Len(t, got.Outputs, 3)
	require.Nil(t, got.Outputs[0], "a spent index must stay nil — that is the store's encoding for 'not a live UTXO'")
	require.NotNil(t, got.Outputs[1])
	require.NotNil(t, got.Outputs[2])

	// The reconstruction is not the transaction and must never be mistaken for it.
	require.Empty(t, got.Inputs, "the .outputs blob retains no inputs")
	require.Zero(t, got.Version, "the .outputs blob retains no version")

	// It cannot even be identified: computing its txid would mean serializing it,
	// and serialization dereferences the nil hole — go-bt's Output.Size() and
	// appendTo() both fault, and TxIDChainHash() reaches them. That crash is the
	// reason meta.Data.TxIsSerializable exists, so assert the predicate the callers
	// actually gate on rather than the panic itself: a future go-bt that returned an
	// error instead of faulting would break a require.Panics here for a good reason,
	// while this assertion would keep meaning what it says.
	require.False(t, (&meta.Data{Tx: got}).TxIsSerializable(),
		"the reconstruction must fail the gate that every serializing caller checks")

	// Sanity: the source transaction is serializable, so the predicate is
	// discriminating rather than always-false.
	require.True(t, (&meta.Data{Tx: tx}).TxIsSerializable())
}

// TestGetExternalTransaction_CorruptTxDoesNotDegradeToOutputs pins that a .tx blob
// which is present but does not parse fails without consulting .outputs. Nothing
// else proves a parse failure never reaches the other representation, which is why
// it asserts read counts rather than only the returned error.
//
// This is not a guard on the absence-only gate. The fallback is decided on the
// error from opening the .tx blob; this body opens fine and fails later, in
// tx.ReadFrom, a branch that has never fallen back. The gate itself is pinned by
// TestGetExternalTransactionFallbackOnlyOnAbsence.
//
// An unparseable body is classified as a storage fault, never a transaction fault:
// the blob was written from this node's own bytes, so bytes that no longer parse
// mean this node's stored copy is torn, which is never evidence about the block or
// the peer that served it.
func TestGetExternalTransaction_CorruptTxDoesNotDegradeToOutputs(t *testing.T) {
	t.Run("an unparseable .tx body is a storage fault, not a miss", func(t *testing.T) {
		s, mem, tx := newOutputsOnlyStore(t, 1, 2)
		txHash := *tx.TxIDChainHash()

		// A .tx blob that exists and carries a valid store header, but whose contents
		// are not a transaction — a truncated or garbled write.
		require.NoError(t, mem.Set(context.Background(), txHash[:], fileformat.FileTypeTx,
			[]byte{0xff, 0xff, 0xff, 0xff}))

		rec := &recordingReadStore{Store: mem}
		s.externalStore = rec

		got, err := s.getExternalTransaction(context.Background(), txHash)

		require.Error(t, err)
		require.Nil(t, got)
		require.True(t, errors.Is(err, errors.ErrStorageError),
			"a .tx blob that will not parse is this node's stored copy being wrong, got %v", err)
		require.False(t, errors.Is(err, errors.ErrTxNotFound),
			"a corrupt blob is present, not absent — calling it not-found would invite the fallback")
		require.Equal(t, 1, rec.reads(fileformat.FileTypeTx))
		require.Zero(t, rec.reads(fileformat.FileTypeOutputs),
			"the reconstruction must not be substituted for a transaction the node does hold")
	})
}

// TestGetExternalTransaction_FallsBackOnlyOnNotFound covers the corners of the
// fallback contract that TestGetExternalTransactionFallbackOnlyOnAbsence does not:
// the public entry point, a failure of the fallback read itself, and the case
// where neither representation is stored. The plain cases — absent .tx falls back,
// a failed .tx read does not — are pinned there and are not repeated here.
func TestGetExternalTransaction_FallsBackOnlyOnNotFound(t *testing.T) {
	t.Run("the public reader falls back too", func(t *testing.T) {
		// A snapshot-seeded parent has only .outputs, and the validator reads such a
		// parent through this entry point to extend a child — so denying it the
		// fallback rejects valid spends as missing-parent. Covered end to end against
		// a real store by TestExternalOutputsOnlyParentIsReadableForExtension; this
		// keeps it pinned without a container.
		s, _, tx := newOutputsOnlyStore(t, 1, 2)

		got, err := s.GetTxFromExternalStore(context.Background(), *tx.TxIDChainHash())
		require.NoError(t, err, "a seeded parent must stay readable through the public entry point")
		require.Len(t, got.Outputs, 3)
		require.NotNil(t, got.Outputs[1], "the live indexes are what the extension path reads")
	})

	t.Run("transient .outputs read failure is surfaced, not reported as not-found", func(t *testing.T) {
		s, mem, tx := newOutputsOnlyStore(t, 1, 2)

		// .tx is genuinely absent, so the fallback is taken; the fallback read
		// itself then fails. That is a store failure, not "this node does not have
		// the transaction", and the caller must be able to tell them apart.
		s.externalStore = &failingReadStore{
			Store:        mem,
			failFileType: fileformat.FileTypeOutputs,
			failWith:     errors.NewServiceUnavailableError("read permits exhausted"),
		}

		got, err := s.getExternalTransaction(context.Background(), *tx.TxIDChainHash())

		require.Error(t, err)
		require.Nil(t, got)
		require.False(t, errors.Is(err, errors.ErrTxNotFound),
			"a failure to read the outputs blob must not masquerade as the tx being absent")
		require.True(t, errors.Is(err, errors.ErrServiceUnavailable),
			"the original failure must be preserved, got %v", err)
	})

	t.Run("neither representation stored is our fault, not the sending peer's", func(t *testing.T) {
		// An external record with neither blob is this node's storage contradicting
		// itself: create.go writes the blob before the Aerospike record, and the
		// pruner deletes the record before the blob, so no step in the normal
		// lifecycle leaves an external record without one.
		//
		// It must therefore stay classified as a storage fault, because
		// errors.IsTransientLocalError keys on ErrStorageError, and that predicate is
		// what decides whose fault a failed block is: legacy/netsync's
		// block_park_policy parks on it instead of rejecting, and blockvalidation's
		// peer metrics withhold blame for it. A weaker class here blames the peer
		// that merely sent a block spending such a parent, and then each replacement
		// peer in turn.
		s, _, tx := newOutputsOnlyStore(t)

		require.NoError(t, s.externalStore.Del(context.Background(), tx.TxIDChainHash()[:], fileformat.FileTypeOutputs))

		got, err := s.getExternalTransaction(context.Background(), *tx.TxIDChainHash())
		require.Error(t, err)
		require.Nil(t, got)

		require.True(t, errors.Is(err, errors.ErrStorageError),
			"whose fault it is must stay visible: this is a local storage fault, got %v", err)
		require.True(t, errors.IsTransientLocalError(err),
			"a missing blob of ours must not be attributed to the peer that sent the block, got %v", err)

		// The error also matches ErrNotFound: the memory store's not-found error is
		// wrapped as its cause, and errors.Is matches a code anywhere in the chain.
		// parkReadFailure and parkCommitFailure in legacy/netsync's
		// block_park_policy.go classify it correctly only because they test
		// IsTransientLocalError before ErrNotFound, which maps to blob-unusable and
		// files-gone respectively. Any consumer must keep that order, or a fault of
		// ours stops being retried.
		require.True(t, errors.Is(err, errors.ErrNotFound),
			"the chain carries the store's not-found cause, so consumers must test transient first, got %v", err)
	})
}
