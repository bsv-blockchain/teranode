package utxo_test

import (
	"context"
	"net/url"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/bsv-blockchain/teranode/stores/utxo/nullstore"
	utxosql "github.com/bsv-blockchain/teranode/stores/utxo/sql"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/require"
)

// countedStore is a real sqlitememory store that counts BatchDecorate calls and the entries in
// each, and can fail a call or answer one entry with an error the way aerospike reports a
// record timeout inside a batch that otherwise succeeded.
type countedStore struct {
	utxo.Store
	calls    int
	sizes    []int
	fail     error
	entryErr map[chainhash.Hash]error
}

func (s *countedStore) BatchDecorate(ctx context.Context, items []*utxo.UnresolvedMetaData, f ...fields.FieldName) error {
	s.calls++
	s.sizes = append(s.sizes, len(items))

	if s.fail != nil {
		return s.fail
	}

	if err := s.Store.BatchDecorate(ctx, items, f...); err != nil {
		return err
	}

	for _, it := range items {
		if err, ok := s.entryErr[it.Hash]; ok {
			it.Data, it.Err = nil, err
		}
	}

	return nil
}

func newCountedStore(t *testing.T, name string) *countedStore {
	t.Helper()

	storeURL, err := url.Parse("sqlitememory:///" + name)
	require.NoError(t, err)

	store, err := utxosql.New(context.Background(), ulogger.TestLogger{}, test.CreateBaseTestSettings(t), storeURL)
	require.NoError(t, err)

	return &countedStore{Store: store}
}

// storeTx stores a distinct transaction, unmined, as a failed earlier attempt at a block can,
// and returns its hash.
func storeTx(t *testing.T, store utxo.Store, n byte) chainhash.Hash {
	t.Helper()

	in := &bt.Input{PreviousTxOutIndex: 0, SequenceNumber: 0xffffffff, PreviousTxSatoshis: 5_000,
		UnlockingScript: bscript.NewFromBytes([]byte{0x00})}
	require.NoError(t, in.PreviousTxIDAdd(&chainhash.Hash{0xee, n}))

	tx := &bt.Tx{Version: 1, Inputs: []*bt.Input{in}}
	require.NoError(t, tx.AddOpReturnOutput([]byte{n}))

	_, _, err := store.SpendAndCreate(context.Background(), tx, 1, utxo.WithCreateOnly())
	require.NoError(t, err)

	return *tx.TxIDChainHash()
}

// A block path that chose not to store some transactions learns which of them were stored
// anyway, by an earlier failed attempt at the same block, in one batched read.
func TestStoredSubsetKeepsOnlyWhatTheStoreHolds(t *testing.T) {
	store := newCountedStore(t, "stored_subset_keeps")
	held := storeTx(t, store, 1)
	never := chainhash.Hash{2}

	got, err := utxo.StoredSubset(context.Background(), store, []*chainhash.Hash{&never, nil, &held})
	require.NoError(t, err, "a transaction the store has never seen is the normal case")
	require.Equal(t, []*chainhash.Hash{&held}, got)
	require.Equal(t, 1, store.calls, "one read for the whole set")
}

func TestStoredSubsetAsksNothingOfAnEmptySet(t *testing.T) {
	store := newCountedStore(t, "stored_subset_empty")

	got, err := utxo.StoredSubset(context.Background(), store, nil)
	require.NoError(t, err)
	require.Empty(t, got)
	require.Zero(t, store.calls)
}

func TestStoredSubsetReturnsAStoreFailure(t *testing.T) {
	store := newCountedStore(t, "stored_subset_fail")
	store.fail = errors.NewStorageError("down")
	h := chainhash.Hash{3}

	_, err := utxo.StoredSubset(context.Background(), store, []*chainhash.Hash{&h})
	require.Error(t, err)
}

// An error on one entry that is not a miss means the store did not answer for that
// transaction. Read as absent, a stored transaction would stay unmined for good.
func TestStoredSubsetReturnsAnEntryErrorThatIsNotAMiss(t *testing.T) {
	store := newCountedStore(t, "stored_subset_entry_err")
	held := storeTx(t, store, 1)
	never := chainhash.Hash{2}
	store.entryErr = map[chainhash.Hash]error{held: errors.NewStorageError("record timeout")}

	_, err := utxo.StoredSubset(context.Background(), store, []*chainhash.Hash{&never, &held})
	require.Error(t, err)
	require.True(t, errors.Is(err, errors.ErrStorageError), "the entry's own error is carried, got %v", err)
}

// A block can skip hundreds of thousands of transactions; the reads are split so no single
// BatchDecorate call carries them all.
func TestStoredSubsetSplitsALargeSetIntoBoundedReads(t *testing.T) {
	store := newCountedStore(t, "stored_subset_split")
	held := storeTx(t, store, 1)

	hashes := make([]*chainhash.Hash, 0, 2500)
	for i := 0; i < 2499; i++ {
		h := chainhash.Hash{0xaa, byte(i), byte(i >> 8)}
		hashes = append(hashes, &h)
	}

	hashes = append(hashes, &held)

	got, err := utxo.StoredSubset(context.Background(), store, hashes)
	require.NoError(t, err)
	require.Equal(t, []*chainhash.Hash{&held}, got, "the one stored transaction, found in the last read")
	require.Equal(t, []int{1024, 1024, 452}, store.sizes)
}

// A store that answers nothing at all, as the null store does, holds nothing.
func TestStoredSubsetTreatsNoAnswerAsAbsent(t *testing.T) {
	h := chainhash.Hash{4}

	got, err := utxo.StoredSubset(context.Background(), &nullstore.NullStore{}, []*chainhash.Hash{&h})
	require.NoError(t, err)
	require.Empty(t, got)
}

// proberStore gives the real store a StoredTxs probe. No store on main implements one yet, so
// the probe is the test's, answered from the real store by Get.
type proberStore struct {
	*countedStore
	probes int
}

func (s *proberStore) StoredTxs(ctx context.Context, hashes []*chainhash.Hash) ([]chainhash.Hash, error) {
	s.probes++

	var out []chainhash.Hash

	// Answered in reverse, to show StoredSubset restores the caller's order.
	for i := len(hashes) - 1; i >= 0; i-- {
		_, err := s.countedStore.Store.Get(ctx, hashes[i], fields.BlockIDs)
		if errors.Is(err, errors.ErrTxNotFound) {
			continue
		}

		if err != nil {
			return nil, err
		}

		out = append(out, *hashes[i])
	}

	return out, nil
}

// A store with its own cheap probe is asked through it, once, and never through BatchDecorate.
func TestStoredSubsetUsesTheStoresProbe(t *testing.T) {
	store := &proberStore{countedStore: newCountedStore(t, "stored_subset_probe")}
	a, b := storeTx(t, store, 1), storeTx(t, store, 2)
	never := chainhash.Hash{3}

	got, err := utxo.StoredSubset(context.Background(), store, []*chainhash.Hash{&a, &never, nil, &b})
	require.NoError(t, err)
	require.Equal(t, []*chainhash.Hash{&a, &b}, got, "the caller's order")
	require.Equal(t, 1, store.probes)
	require.Zero(t, store.calls, "BatchDecorate is not used when the store can probe")
}

// A wrapper forwards to the wrapped store's probe when it has one and to the wrapped store's
// BatchDecorate when it does not.
func TestForwardStoredTxs(t *testing.T) {
	never := chainhash.Hash{2}

	prober := &proberStore{countedStore: newCountedStore(t, "forward_probe")}
	a := storeTx(t, prober, 1)
	got, err := utxo.ForwardStoredTxs(context.Background(), prober, []*chainhash.Hash{&a, &never})
	require.NoError(t, err)
	require.Equal(t, []chainhash.Hash{a}, got)
	require.Equal(t, 1, prober.probes)
	require.Zero(t, prober.calls)

	plain := newCountedStore(t, "forward_plain")
	b := storeTx(t, plain, 1)
	got, err = utxo.ForwardStoredTxs(context.Background(), plain, []*chainhash.Hash{&b, &never})
	require.NoError(t, err)
	require.Equal(t, []chainhash.Hash{b}, got)
	require.Equal(t, 1, plain.calls)
}
