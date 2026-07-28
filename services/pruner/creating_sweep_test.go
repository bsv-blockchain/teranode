package pruner

import (
	"context"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/bsv-blockchain/teranode/stores/utxo/meta"
	"github.com/bsv-blockchain/teranode/stores/utxo/nullstore"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/require"
)

// fakeSweepStore scripts the store methods the creating-sweep uses; everything
// else falls through to the null store's no-ops.
type fakeSweepStore struct {
	*nullstore.NullStore

	stale           []chainhash.Hash
	queryErr        error
	getResp         map[chainhash.Hash]*meta.Data
	spendErr        error
	spendIncomplete bool

	spends         []chainhash.Hash
	finalized      []chainhash.Hash
	setConflicting []chainhash.Hash
	unlocked       []chainhash.Hash
	lastLimit      int
}

func (f *fakeSweepStore) SetLocked(_ context.Context, txHashes []chainhash.Hash, value bool) error {
	if !value {
		f.unlocked = append(f.unlocked, txHashes...)
	}

	return nil
}

func (f *fakeSweepStore) QueryStaleCreatingTxs(_ context.Context, _ uint32, limit int) ([]chainhash.Hash, error) {
	f.lastLimit = limit
	return f.stale, f.queryErr
}

func (f *fakeSweepStore) Get(_ context.Context, hash *chainhash.Hash, _ ...fields.FieldName) (*meta.Data, error) {
	if md, ok := f.getResp[*hash]; ok {
		return md, nil
	}

	return nil, errors.NewTxNotFoundError("not found")
}

// SpendAndCreate is how the sweeper re-spends (WithSpendOnly) after #1326 removed
// Store.Spend; record the call and return the injected spend error. On success it
// returns one no-error spend per input so the sweep's completeness guard is satisfied.
func (f *fakeSweepStore) SpendAndCreate(_ context.Context, tx *bt.Tx, _ uint32, _ ...utxo.CreateOption) (*meta.Data, []*utxo.Spend, error) {
	f.spends = append(f.spends, *tx.TxIDChainHash())

	if f.spendErr != nil {
		return nil, nil, f.spendErr
	}

	if f.spendIncomplete {
		// A nil top-level error but no per-input spends — models a swallowed per-input
		// error (e.g. the "already blessed" fallback) that must NOT be treated as success.
		return nil, nil, nil
	}

	spent := make([]*utxo.Spend, len(tx.Inputs))
	for i := range spent {
		spent[i] = &utxo.Spend{}
	}

	return nil, spent, nil
}

func (f *fakeSweepStore) FinalizeTransaction(_ context.Context, tx *bt.Tx) error {
	f.finalized = append(f.finalized, *tx.TxIDChainHash())
	return nil
}

func (f *fakeSweepStore) SetConflicting(_ context.Context, txHashes []chainhash.Hash, _ bool) ([]*utxo.Spend, []chainhash.Hash, error) {
	f.setConflicting = append(f.setConflicting, txHashes...)
	return nil, nil, nil
}

func newSweepServer(t *testing.T, store utxo.Store) *Server {
	t.Helper()

	tSettings := test.CreateBaseTestSettings(t)
	tSettings.Pruner.CreatingTxSweepMinAgeBlocks = 2

	return &Server{
		logger:    ulogger.NewErrorTestLogger(t),
		settings:  tSettings,
		utxoStore: store,
	}
}

// oneOutputTx returns a minimal tx (distinct txid via the vout index) usable as md.Tx.
func oneOutputTx(vout uint32) *bt.Tx {
	tx := bt.NewTx()
	in := &bt.Input{PreviousTxOutIndex: vout, UnlockingScript: bscript.NewFromBytes([]byte{})}
	_ = in.PreviousTxIDAdd(&chainhash.Hash{})
	tx.Inputs = append(tx.Inputs, in)
	tx.Outputs = append(tx.Outputs, &bt.Output{Satoshis: 1000, LockingScript: bscript.NewFromBytes([]byte{0x51})})

	return tx
}

func TestSweepCreatingTxsRollsForward(t *testing.T) {
	ns, err := nullstore.NewNullStore()
	require.NoError(t, err)

	txA := oneOutputTx(1)
	hashA := *txA.TxIDChainHash()

	store := &fakeSweepStore{
		NullStore: ns,
		stale:     []chainhash.Hash{hashA},
		getResp:   map[chainhash.Hash]*meta.Data{hashA: {Creating: true, Tx: txA}},
	}

	newSweepServer(t, store).sweepCreatingTxs(context.Background(), 100)

	require.Equal(t, []chainhash.Hash{hashA}, store.spends, "must attempt to spend the stale creating tx")
	require.Equal(t, []chainhash.Hash{hashA}, store.finalized, "a successful re-spend must be finalized")
	require.Equal(t, []chainhash.Hash{hashA}, store.unlocked, "a rolled-forward record must be unlocked so its outputs are spendable")
	require.Empty(t, store.setConflicting, "no double-spend, so nothing marked conflicting")
}

// TestSweepCreatingTxsDoesNotFinalizeIncompleteSpend is the ChiR1 sweep-side guard:
// a nil top-level spend error is not proof every input was spent (a per-input error
// could have been swallowed, e.g. by the "already blessed" fallback). The sweep must
// NOT finalize — finalizing would make the outputs spendable off an unspent input.
func TestSweepCreatingTxsDoesNotFinalizeIncompleteSpend(t *testing.T) {
	ns, err := nullstore.NewNullStore()
	require.NoError(t, err)

	txA := oneOutputTx(1)
	hashA := *txA.TxIDChainHash()

	store := &fakeSweepStore{
		NullStore:       ns,
		stale:           []chainhash.Hash{hashA},
		getResp:         map[chainhash.Hash]*meta.Data{hashA: {Creating: true, Tx: txA}},
		spendIncomplete: true, // nil error, but zero per-input spends returned
	}

	newSweepServer(t, store).sweepCreatingTxs(context.Background(), 100)

	require.Equal(t, []chainhash.Hash{hashA}, store.spends, "the sweep must attempt the spend")
	require.Empty(t, store.finalized, "an incomplete spend (nil error, missing per-input spends) must not be finalized")
	require.Empty(t, store.setConflicting, "an incomplete spend is not a double-spend")
}

// TestSweepCreatingTxsPassesLimit is the ChiR9 bound: the sweep must ask the store for
// a bounded page, not an unbounded result set.
func TestSweepCreatingTxsPassesLimit(t *testing.T) {
	ns, err := nullstore.NewNullStore()
	require.NoError(t, err)

	store := &fakeSweepStore{NullStore: ns} // no stale txs; we only care about the limit passed

	newSweepServer(t, store).sweepCreatingTxs(context.Background(), 100)

	require.Equal(t, creatingSweepMaxPerPass, store.lastLimit, "the sweep must bound the query to the per-pass cap")
}

// TestSweepCreatingTxsHonorsCancellation is the ChiR9 cancellation guard: a cancelled
// context must abort the roll-forward loop before doing per-tx work.
func TestSweepCreatingTxsHonorsCancellation(t *testing.T) {
	ns, err := nullstore.NewNullStore()
	require.NoError(t, err)

	txA := oneOutputTx(1)
	hashA := *txA.TxIDChainHash()

	store := &fakeSweepStore{
		NullStore: ns,
		stale:     []chainhash.Hash{hashA},
		getResp:   map[chainhash.Hash]*meta.Data{hashA: {Creating: true, Tx: txA}},
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	newSweepServer(t, store).sweepCreatingTxs(ctx, 100)

	require.Empty(t, store.spends, "a cancelled context must abort before any roll-forward re-spend")
	require.Empty(t, store.finalized)
}

func TestSweepCreatingTxsConflictingTerminal(t *testing.T) {
	ns, err := nullstore.NewNullStore()
	require.NoError(t, err)

	txA := oneOutputTx(1)
	hashA := *txA.TxIDChainHash()

	store := &fakeSweepStore{
		NullStore: ns,
		stale:     []chainhash.Hash{hashA},
		getResp:   map[chainhash.Hash]*meta.Data{hashA: {Creating: true, Tx: txA}},
		spendErr:  errors.ErrSpent,
	}

	newSweepServer(t, store).sweepCreatingTxs(context.Background(), 100)

	require.Equal(t, []chainhash.Hash{hashA}, store.setConflicting, "a double-spent creating tx must be marked conflicting")
	require.Equal(t, []chainhash.Hash{hashA}, store.finalized, "the conflicting tx must be finalized to its terminal state")
}

func TestSweepCreatingTxsSkipsAlreadyFinalized(t *testing.T) {
	ns, err := nullstore.NewNullStore()
	require.NoError(t, err)

	txA := oneOutputTx(1)
	hashA := *txA.TxIDChainHash()

	store := &fakeSweepStore{
		NullStore: ns,
		stale:     []chainhash.Hash{hashA},
		getResp:   map[chainhash.Hash]*meta.Data{hashA: {Creating: false, Tx: txA}}, // finalized between query and get
	}

	newSweepServer(t, store).sweepCreatingTxs(context.Background(), 100)

	require.Empty(t, store.spends, "a finalized record must not be re-spent")
	require.Empty(t, store.finalized)
}

func TestSweepCreatingTxsLeavesUnrecoverable(t *testing.T) {
	ns, err := nullstore.NewNullStore()
	require.NoError(t, err)

	txA := oneOutputTx(1)
	hashA := *txA.TxIDChainHash()

	store := &fakeSweepStore{
		NullStore: ns,
		stale:     []chainhash.Hash{hashA},
		getResp:   map[chainhash.Hash]*meta.Data{hashA: {Creating: true, Tx: txA}},
		spendErr:  errors.ErrTxNotFound, // e.g. a parent was evicted
	}

	newSweepServer(t, store).sweepCreatingTxs(context.Background(), 100)

	require.Empty(t, store.finalized, "an unrecoverable spend must not finalize the record")
	require.Empty(t, store.setConflicting, "an unrecoverable spend is not a double-spend")
}

func TestSweepCreatingTxsDisabledWhenAgeZero(t *testing.T) {
	ns, err := nullstore.NewNullStore()
	require.NoError(t, err)

	store := &fakeSweepStore{NullStore: ns, stale: []chainhash.Hash{chainhash.HashH([]byte("x"))}}

	s := newSweepServer(t, store)
	s.settings.Pruner.CreatingTxSweepMinAgeBlocks = 0

	s.sweepCreatingTxs(context.Background(), 100)

	require.Empty(t, store.spends, "the sweep must be a no-op when disabled (age 0)")
}

func TestSweepCreatingTxsQueryError(t *testing.T) {
	ns, err := nullstore.NewNullStore()
	require.NoError(t, err)

	store := &fakeSweepStore{NullStore: ns, queryErr: errors.NewStorageError("query boom")}

	newSweepServer(t, store).sweepCreatingTxs(context.Background(), 100)

	require.Empty(t, store.spends, "a query error must abort the sweep")
	require.Empty(t, store.finalized)
}

func TestSweepCreatingTxsGetError(t *testing.T) {
	ns, err := nullstore.NewNullStore()
	require.NoError(t, err)

	// Stale hash present, but Get has no entry for it → returns NOT_FOUND.
	store := &fakeSweepStore{NullStore: ns, stale: []chainhash.Hash{chainhash.HashH([]byte("gone"))}, getResp: map[chainhash.Hash]*meta.Data{}}

	newSweepServer(t, store).sweepCreatingTxs(context.Background(), 100)

	require.Empty(t, store.spends, "a get error must skip that tx")
	require.Empty(t, store.finalized)
}

func TestSweepCreatingTxsSkippedBelowMinAge(t *testing.T) {
	ns, err := nullstore.NewNullStore()
	require.NoError(t, err)

	store := &fakeSweepStore{NullStore: ns, stale: []chainhash.Hash{chainhash.HashH([]byte("x"))}}

	// blockHeight (2) <= minAge (2) → the sweep is skipped before querying.
	newSweepServer(t, store).sweepCreatingTxs(context.Background(), 2)

	require.Empty(t, store.spends, "the sweep must not run when blockHeight <= minAge")
}
