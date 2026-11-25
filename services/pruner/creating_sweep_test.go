package pruner

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/services/blockassembly"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/bsv-blockchain/teranode/stores/utxo/meta"
	"github.com/bsv-blockchain/teranode/stores/utxo/nullstore"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/lane"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// fakeSweepStore scripts the store methods the creating-sweep uses; everything
// else falls through to the null store's no-ops.
// sweepToken is the attempt token of the creating records in these tests. A creating record with
// no token was not written by a create-first attempt, and the sweep only clears it.
var sweepToken = []byte{0x5e}

type fakeSweepStore struct {
	*nullstore.NullStore

	stale           []chainhash.Hash
	queryErr        error
	queryBefore     []uint32
	getResp         map[chainhash.Hash]*meta.Data
	getErrs         map[chainhash.Hash]error
	getReads        []chainhash.Hash
	spendErr        error
	spendIncomplete bool
	spendSpends     []*utxo.Spend
	finalizeErr     error
	beginDeclined   bool
	// beginErr is what BeginCreateFirstRollback fails with, and spendWait what SpendWaitTimeout
	// answers, which is how long a rollback keeps trying a claim that a live lease refuses.
	beginErr  error
	spendWait time.Duration

	takeDeclined bool
	clearResult  bool
	clearErr     error

	spendOpts []utxo.CreateOption
	events    []string
	began     []chainhash.Hash
	unspent   [][]*utxo.Spend
	// unspendGuards holds, per Unspend call, the reversal guard its context carried.
	unspendGuards []utxo.ReversalGuard
	unspendFn     func(spends []*utxo.Spend) error
	ended         []chainhash.Hash
	taken         [][]byte
	cleared       []chainhash.Hash

	spends         []chainhash.Hash
	finalized      []chainhash.Hash
	setConflicting []chainhash.Hash
	unlocked       []chainhash.Hash
	lastLimit      int
	onUnlock       func()
	// unlockErrs is popped once per SetLocked(false) call; a nil entry or an empty list succeeds.
	unlockErrs   []error
	unlockCalls  int
	onGet        func()
	txUnreadable map[chainhash.Hash]bool
	endErr       error
	releaseErr   error

	// leaseDeclined makes TakeSpendLease refuse a record it would otherwise grant, and leaseErr
	// makes it fail. Without them the fake grants a lease as the store does: only for a record that
	// is creating and not rolling back.
	leaseDeclined bool
	leaseErr      error
	// leasesLive is popped once per SpendLeasesLive call; false when exhausted. leasesLiveErr fails
	// every call.
	leasesLive    []bool
	leasesLiveErr error

	leaseTokens  [][]byte
	leasesTaken  [][]byte
	leasesFreed  [][]byte
	liveReads    int
	releaseCalls int
	// trace records every call of the roll-forward and rollback protocol in order, the spend and
	// the finalize included. events keeps the calls that write the rollback state.
	trace []string
}

// fakeLease is the lease the fake grants.
var fakeLease = []byte{0xaa, 0xbb}

func (f *fakeSweepStore) record(event string) {
	f.events = append(f.events, event)
	f.trace = append(f.trace, event)
}

func (f *fakeSweepStore) TakeSpendLease(_ context.Context, hash *chainhash.Hash, attempt []byte) ([]byte, bool, error) {
	f.trace = append(f.trace, "lease")
	f.leaseTokens = append(f.leaseTokens, attempt)

	if f.leaseErr != nil {
		return nil, false, f.leaseErr
	}

	md, ok := f.getResp[*hash]
	if !ok || !md.Creating || md.RollingBack || f.leaseDeclined {
		return nil, false, nil
	}

	f.leasesTaken = append(f.leasesTaken, fakeLease)

	return fakeLease, true, nil
}

func (f *fakeSweepStore) ReleaseSpendLease(_ context.Context, _ *chainhash.Hash, _, lease []byte) error {
	f.trace = append(f.trace, "unlease")
	f.leasesFreed = append(f.leasesFreed, lease)

	return nil
}

func (f *fakeSweepStore) SpendLeasesLive(_ context.Context, _ *chainhash.Hash, _ []byte) (bool, error) {
	f.trace = append(f.trace, "leases")
	f.liveReads++

	if f.leasesLiveErr != nil {
		return false, f.leasesLiveErr
	}

	if len(f.leasesLive) > 0 {
		live := f.leasesLive[0]
		f.leasesLive = f.leasesLive[1:]

		return live, nil
	}

	return false, nil
}

func (f *fakeSweepStore) SetLocked(_ context.Context, txHashes []chainhash.Hash, value bool) error {
	if !value {
		f.unlockCalls++

		if len(f.unlockErrs) > 0 {
			err := f.unlockErrs[0]
			f.unlockErrs = f.unlockErrs[1:]

			if err != nil {
				return err
			}
		}

		if f.onUnlock != nil {
			f.onUnlock()
		}

		f.unlocked = append(f.unlocked, txHashes...)
	}

	return nil
}

func (f *fakeSweepStore) QueryStaleCreatingTxs(_ context.Context, before uint32, limit int) ([]chainhash.Hash, error) {
	f.lastLimit = limit
	f.queryBefore = append(f.queryBefore, before)

	return f.stale, f.queryErr
}

func (f *fakeSweepStore) Get(_ context.Context, hash *chainhash.Hash, requested ...fields.FieldName) (*meta.Data, error) {
	f.getReads = append(f.getReads, *hash)

	if err, ok := f.getErrs[*hash]; ok {
		return nil, err
	}

	if f.txUnreadable[*hash] && slices.Contains(requested, fields.Tx) {
		return nil, errors.NewStorageError("the external transaction blob of %s is gone", hash.String())
	}

	if md, ok := f.getResp[*hash]; ok {
		snapshot := *md

		if f.onGet != nil {
			f.onGet()
		}

		return &snapshot, nil
	}

	return nil, errors.NewTxNotFoundError("not found")
}

// SpendAndCreate is how the sweeper re-spends (WithSpendOnly) after #1326 removed
// Store.Spend; record the call and return the injected spend error. On success it
// returns one no-error spend per input so the sweep's completeness guard is satisfied.
func (f *fakeSweepStore) SpendAndCreate(_ context.Context, tx *bt.Tx, _ uint32, opts ...utxo.CreateOption) (*meta.Data, []*utxo.Spend, error) {
	f.spends = append(f.spends, *tx.TxIDChainHash())
	f.spendOpts = opts
	f.trace = append(f.trace, "spend")

	if f.spendErr != nil {
		return nil, f.spendSpends, f.spendErr
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

func (f *fakeSweepStore) FinalizeTransaction(_ context.Context, tx *bt.Tx, _, _ []byte) error {
	f.finalized = append(f.finalized, *tx.TxIDChainHash())
	f.trace = append(f.trace, "finalize")

	return f.finalizeErr
}

func (f *fakeSweepStore) GetMeta(_ context.Context, hash *chainhash.Hash, data *meta.Data) error {
	md, ok := f.getResp[*hash]
	if !ok {
		return errors.NewTxNotFoundError("not found")
	}

	*data = *md

	return nil
}

func (f *fakeSweepStore) BeginCreateFirstRollback(_ context.Context, hash *chainhash.Hash, _ []byte) (bool, error) {
	f.record("begin")
	f.began = append(f.began, *hash)

	if f.beginErr != nil {
		return false, f.beginErr
	}

	return !f.beginDeclined, nil
}

func (f *fakeSweepStore) SpendWaitTimeout() time.Duration { return f.spendWait }

func (f *fakeSweepStore) Unspend(ctx context.Context, spends []*utxo.Spend, _ ...bool) error {
	f.record("unspend")

	guard, _ := utxo.ReversalGuardFrom(ctx)
	f.unspendGuards = append(f.unspendGuards, guard)
	f.unspent = append(f.unspent, spends)

	if f.unspendFn != nil {
		return f.unspendFn(spends)
	}

	return nil
}

func (f *fakeSweepStore) EndCreateFirstRollback(_ context.Context, hash *chainhash.Hash, _ []byte) error {
	f.record("end")
	f.ended = append(f.ended, *hash)

	return f.endErr
}

func (f *fakeSweepStore) TakeRollbackHold(_ context.Context, _ *chainhash.Hash, attempt []byte) (bool, error) {
	f.record("take")
	f.taken = append(f.taken, attempt)

	return !f.takeDeclined, nil
}

func (f *fakeSweepStore) ReleaseRollbackHold(_ context.Context, _ *chainhash.Hash, _ []byte) error {
	f.record("release")
	f.releaseCalls++

	return f.releaseErr
}

func (f *fakeSweepStore) ClearCreateFirstHold(_ context.Context, hash *chainhash.Hash, adopt bool) (bool, error) {
	f.record("clear")
	f.cleared = append(f.cleared, *hash)

	if adopt {
		return false, errors.NewProcessingError("the sweep never adopts a hold")
	}

	return f.clearResult, f.clearErr
}

func (f *fakeSweepStore) SetConflicting(_ context.Context, txHashes []chainhash.Hash, _ bool) ([]*utxo.Spend, []chainhash.Hash, error) {
	f.setConflicting = append(f.setConflicting, txHashes...)
	return nil, nil, nil
}

func newSweepServer(t *testing.T, store utxo.Store) *Server {
	t.Helper()

	initPrometheusMetrics()

	tSettings := test.CreateBaseTestSettings(t)
	tSettings.Pruner.CreatingTxSweepMinAgeBlocks = 2

	return &Server{
		logger:       ulogger.NewErrorTestLogger(t),
		settings:     tSettings,
		utxoStore:    store,
		handOffDelay: time.Millisecond,
	}
}

// oneOutputTx returns a minimal tx (distinct txid via the vout index) usable as md.Tx.
func oneOutputTx(vout uint32) *bt.Tx {
	tx := bt.NewTx()
	in := &bt.Input{PreviousTxOutIndex: vout, PreviousTxSatoshis: 1000, PreviousTxScript: bscript.NewFromBytes([]byte{0x51}), UnlockingScript: bscript.NewFromBytes([]byte{})}
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
		getResp:   map[chainhash.Hash]*meta.Data{hashA: {Creating: true, Attempt: sweepToken, Tx: txA}},
	}

	ba := blockassembly.NewMock()
	ba.On("Store", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(true, nil)

	s := newSweepServer(t, store)
	s.blockAssemblyClient = ba
	s.sweepCreatingTxs(context.Background(), 100)

	require.Equal(t, []chainhash.Hash{hashA}, store.spends, "must attempt to spend the stale creating tx")
	require.Equal(t, []string{"lease", "spend", "finalize"}, store.trace, "the lease comes before the spend, and a finalized record needs no release")
	require.Empty(t, store.leasesFreed)
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
		getResp:         map[chainhash.Hash]*meta.Data{hashA: {Creating: true, Attempt: sweepToken, Tx: txA}},
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

	require.Equal(t, defaultCreatingSweepMaxPerPass, store.lastLimit, "a zero setting must fall back to the default per-pass cap")
}

func TestSweepCreatingTxsHonorsConfiguredLimit(t *testing.T) {
	ns, err := nullstore.NewNullStore()
	require.NoError(t, err)

	store := &fakeSweepStore{NullStore: ns}

	s := newSweepServer(t, store)
	s.settings.Pruner.CreatingTxSweepMaxPerPass = 25

	s.sweepCreatingTxs(context.Background(), 100)

	require.Equal(t, 25, store.lastLimit, "the sweep must bound the query to the configured per-pass cap")
}

func TestSweepCreatingTxsRunsWithCreateFirstDisabled(t *testing.T) {
	ns, err := nullstore.NewNullStore()
	require.NoError(t, err)

	txA := oneOutputTx(1)
	hashA := *txA.TxIDChainHash()

	store := &fakeSweepStore{
		NullStore: ns,
		stale:     []chainhash.Hash{hashA},
		getResp:   map[chainhash.Hash]*meta.Data{hashA: {Creating: true, Attempt: sweepToken, Tx: txA}},
	}

	ba := blockassembly.NewMock()
	ba.On("Store", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(true, nil)

	s := newSweepServer(t, store)
	s.settings.UtxoStore.UseCreateFirstOrder = false
	s.blockAssemblyClient = ba
	s.sweepCreatingTxs(context.Background(), 100)

	require.Equal(t, []chainhash.Hash{hashA}, store.finalized, "the sweep must heal stranded records whatever the feature flag says")
}

func TestSweepCreatingTxsAnnouncesBeforeUnlock(t *testing.T) {
	ns, err := nullstore.NewNullStore()
	require.NoError(t, err)

	txA := oneOutputTx(1)
	hashA := *txA.TxIDChainHash()
	inpoints := subtree.TxInpoints{}

	store := &fakeSweepStore{
		NullStore: ns,
		stale:     []chainhash.Hash{hashA},
		getResp:   map[chainhash.Hash]*meta.Data{hashA: {Creating: true, Attempt: sweepToken, Tx: txA, Fee: 7, SizeInBytes: 99, TxInpoints: inpoints}},
	}

	var events []string

	store.onUnlock = func() { events = append(events, "unlock") }

	ba := blockassembly.NewMock()
	ba.On("Store", mock.Anything, mock.Anything, uint64(7), uint64(99), inpoints, mock.Anything).
		Run(func(mock.Arguments) { events = append(events, "store") }).
		Return(true, nil)

	s := newSweepServer(t, store)
	s.blockAssemblyClient = ba
	s.sweepCreatingTxs(context.Background(), 100)

	require.Equal(t, []string{"store", "unlock"}, events, "block assembly must be told before the record is unlocked")
	ba.AssertNumberOfCalls(t, "Store", 1)
}

func TestSweepCreatingTxsStoreFailureLeavesLocked(t *testing.T) {
	ns, err := nullstore.NewNullStore()
	require.NoError(t, err)

	txA := oneOutputTx(1)
	hashA := *txA.TxIDChainHash()

	store := &fakeSweepStore{
		NullStore: ns,
		stale:     []chainhash.Hash{hashA},
		getResp:   map[chainhash.Hash]*meta.Data{hashA: {Creating: true, Attempt: sweepToken, Tx: txA}},
	}

	ba := blockassembly.NewMock()
	ba.On("Store", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(false, errors.NewServiceError("ba down"))

	s := newSweepServer(t, store)
	s.blockAssemblyClient = ba
	s.sweepCreatingTxs(context.Background(), 100)

	require.Equal(t, []chainhash.Hash{hashA}, store.finalized, "the record is finalized before the announce")
	require.Empty(t, store.unlocked, "a failed announce must leave the record locked")
}

func TestSweepCreatingTxsNoBlockAssemblyLeavesLocked(t *testing.T) {
	ns, err := nullstore.NewNullStore()
	require.NoError(t, err)

	txA := oneOutputTx(1)
	hashA := *txA.TxIDChainHash()

	store := &fakeSweepStore{
		NullStore: ns,
		stale:     []chainhash.Hash{hashA},
		getResp:   map[chainhash.Hash]*meta.Data{hashA: {Creating: true, Attempt: sweepToken, Tx: txA}},
	}

	newSweepServer(t, store).sweepCreatingTxs(context.Background(), 100)

	require.Equal(t, []chainhash.Hash{hashA}, store.finalized)
	require.Empty(t, store.unlocked, "without a block assembly client the tx cannot be announced, so it stays locked")
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
		getResp:   map[chainhash.Hash]*meta.Data{hashA: {Creating: true, Attempt: sweepToken, Tx: txA}},
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	newSweepServer(t, store).sweepCreatingTxs(ctx, 100)

	require.Empty(t, store.spends, "a cancelled context must abort before any roll-forward re-spend")
	require.Empty(t, store.finalized)
}

func TestSweepCreatingTxsDoubleSpendRollsBack(t *testing.T) {
	ns, err := nullstore.NewNullStore()
	require.NoError(t, err)

	txA := oneOutputTx(1)
	hashA := *txA.TxIDChainHash()
	failed := &utxo.Spend{Vout: 8, Err: errors.ErrSpent}

	store := &fakeSweepStore{
		NullStore:   ns,
		stale:       []chainhash.Hash{hashA},
		getResp:     map[chainhash.Hash]*meta.Data{hashA: {Creating: true, Attempt: sweepToken, Tx: txA}},
		spendErr:    errors.ErrSpent,
		spendSpends: []*utxo.Spend{failed},
	}

	newSweepServer(t, store).sweepCreatingTxs(context.Background(), 100)

	require.Equal(t, []string{"begin", "unspend", "release", "end"}, store.events, "a loser is marked rolling back, unspent, then its rollback ends")
	require.Equal(t, []string{"lease", "spend", "unlease", "leases", "begin", "unspend", "release", "end"}, store.trace,
		"the pass releases its own lease and reads the leases before it claims the rollback; the store decides inside the claim, so the rollback reads none")
	require.Equal(t, [][]byte{fakeLease}, store.leasesFreed, "the lease the pass took is the one it releases")
	require.Equal(t, []chainhash.Hash{hashA}, store.began)
	require.Len(t, store.unspent, 1)
	require.Len(t, store.unspent[0], len(txA.Inputs), "every input of the tx is unspent, not only the ones this pass took")
	require.Equal(t, txA.Inputs[0].PreviousTxOutIndex, store.unspent[0][0].Vout)
	require.Equal(t, hashA.String(), store.unspent[0][0].SpendingData.TxID.String())
	require.Equal(t, []chainhash.Hash{hashA}, store.ended)
	require.Empty(t, store.setConflicting, "a loser is deleted, not marked conflicting")
	require.Empty(t, store.finalized, "a loser must never be finalized")
}

// A rollback that did not begin, because the record is final, gone or owned by another caller,
// reversed nothing. The pass must not count it as progress, or a page that cannot converge reads
// as one that did.
func TestSweepCreatingTxsRollbackThatDidNotBeginIsNoProgress(t *testing.T) {
	ns, err := nullstore.NewNullStore()
	require.NoError(t, err)

	txA := oneOutputTx(1)
	hashA := *txA.TxIDChainHash()

	store := &fakeSweepStore{
		NullStore:     ns,
		stale:         []chainhash.Hash{hashA},
		getResp:       map[chainhash.Hash]*meta.Data{hashA: {Creating: true, Attempt: sweepToken, Tx: txA}},
		spendErr:      errors.ErrSpent,
		spendSpends:   []*utxo.Spend{{Vout: 8, Err: errors.ErrSpent}},
		beginDeclined: true,
	}

	logger := newCapturingLogger()
	srv := newSweepServer(t, store)
	srv.logger = logger

	srv.sweepCreatingTxs(context.Background(), 100)

	require.Empty(t, store.unspent, "a rollback that did not begin reverses nothing")

	var stuck bool

	for _, line := range logger.captured() {
		if strings.Contains(line, "rolled forward 0 of 1 stale creating txs") {
			stuck = true
		}
	}

	require.True(t, stuck, "a pass whose only record was not rolled back made no progress and says so")
}

func TestSweepCreatingTxsEveryUnwinnableErrorRollsBack(t *testing.T) {
	for _, spendErr := range []error{errors.ErrSpent, errors.ErrTxConflicting, errors.ErrFrozen, errors.ErrUtxoHashMismatch, errors.ErrTxNotFound, errors.ErrUtxoSpendingTxPruned} {
		ns, err := nullstore.NewNullStore()
		require.NoError(t, err)

		txA := oneOutputTx(1)
		hashA := *txA.TxIDChainHash()

		store := &fakeSweepStore{
			NullStore: ns,
			stale:     []chainhash.Hash{hashA},
			getResp:   map[chainhash.Hash]*meta.Data{hashA: {Creating: true, Attempt: sweepToken, Tx: txA}},
			spendErr:  spendErr,
		}

		newSweepServer(t, store).sweepCreatingTxs(context.Background(), 100)

		require.Equal(t, []chainhash.Hash{hashA}, store.ended, "%v must roll the record back", spendErr)
		require.Empty(t, store.finalized)
	}
}

func TestSweepCreatingTxsRollsBackCreatingRecordWithMissingVout(t *testing.T) {
	ns, err := nullstore.NewNullStore()
	require.NoError(t, err)

	txA := oneOutputTx(1)
	missingVout := &bt.Input{PreviousTxOutIndex: 9, PreviousTxSatoshis: 1, PreviousTxScript: bscript.NewFromBytes([]byte{0x51}), UnlockingScript: bscript.NewFromBytes([]byte{})}
	require.NoError(t, missingVout.PreviousTxIDAdd(&chainhash.Hash{1}))
	txA.Inputs = append(txA.Inputs, missingVout)
	hashA := *txA.TxIDChainHash()

	store := &fakeSweepStore{
		NullStore: ns,
		stale:     []chainhash.Hash{hashA},
		getResp:   map[chainhash.Hash]*meta.Data{hashA: {Creating: true, Attempt: sweepToken, Tx: txA}},
		spendErr:  errors.NewTxNotFoundError("[SPEND_BATCH_LUA][%s] UTXO not found for vout 9", hashA.String()),
	}

	newSweepServer(t, store).sweepCreatingTxs(context.Background(), 100)

	require.Equal(t, []chainhash.Hash{hashA}, store.ended, "a record whose input names a missing vout must be rolled back and deleted")
	require.Empty(t, store.finalized)
	require.Equal(t, []string{"begin", "unspend", "release", "end"}, store.events)
}

func TestSweepCreatingTxsLockedParentRetriesNextBlock(t *testing.T) {
	ns, err := nullstore.NewNullStore()
	require.NoError(t, err)

	txA := oneOutputTx(1)
	hashA := *txA.TxIDChainHash()

	store := &fakeSweepStore{
		NullStore: ns,
		stale:     []chainhash.Hash{hashA},
		getResp:   map[chainhash.Hash]*meta.Data{hashA: {Creating: true, Attempt: sweepToken, Tx: txA}},
		spendErr:  errors.ErrTxLocked,
		// A per-input result makes the outcome of the spend known, so its lease is released.
		spendSpends: []*utxo.Spend{{Err: errors.ErrTxLocked}},
	}

	newSweepServer(t, store).sweepCreatingTxs(context.Background(), 100)

	require.Equal(t, []chainhash.Hash{hashA}, store.spends, "the spend is attempted")
	require.Empty(t, store.finalized, "a locked parent must not finalize")
	require.Empty(t, store.events, "a locked parent is transient: no rollback")
	require.Equal(t, []string{"lease", "spend", "unlease"}, store.trace, "the lease is taken before the spend and released once the spend has failed")
	require.Equal(t, [][]byte{fakeLease}, store.leasesFreed)
	require.Empty(t, store.unlocked)
}

// txWithParents returns a tx that spends one output of each given parent. The vout of the first
// input makes the txid distinct.
func txWithParents(vout uint32, parents ...chainhash.Hash) *bt.Tx {
	tx := oneOutputTx(vout)
	tx.Inputs = nil

	for i := range parents {
		in := &bt.Input{PreviousTxOutIndex: vout + uint32(i), PreviousTxSatoshis: 1000, PreviousTxScript: bscript.NewFromBytes([]byte{0x51}), UnlockingScript: bscript.NewFromBytes([]byte{})} //nolint:gosec // small test index
		_ = in.PreviousTxIDAdd(&parents[i])
		tx.Inputs = append(tx.Inputs, in)
	}

	return tx
}

// blockedParent is one parent that fails the re-spend of a creating record. state is what the
// store holds for the parent, and a nil state is a parent that is not in the store. spendErr is
// the error of its input (a locked parent when nil), and noTxID leaves the parent out of the
// result of the spend.
type blockedParent struct {
	state    *meta.Data
	readErr  error
	spendErr error
	noTxID   bool
}

// A record that only a parent blocks is retried on every pass. With pruner_creatingTxSweepBlockedAgeBlocks
// above 0 it is rolled back when it is older than that age and every parent that blocked it in this pass
// has itself been blocked for at least that age. The age of the parent is read from its own record.
func TestSweepCreatingTxsRollsBackARecordOnlyWhenItsParentsHaveBlockedItForLong(t *testing.T) {
	ns, err := nullstore.NewNullStore()
	require.NoError(t, err)

	const (
		height = uint32(100)
		age    = uint32(20)
	)

	parentA := chainhash.HashH([]byte("parent a"))
	parentB := chainhash.HashH([]byte("parent b"))

	locked := func(since uint32) *meta.Data { return &meta.Data{Locked: true, UnminedSince: since} }
	creating := func(since uint32) *meta.Data { return &meta.Data{Creating: true, UnminedSince: since} }

	cases := []struct {
		name         string
		setting      uint32
		childSince   uint32
		parents      []blockedParent
		wantRollback bool
		noParentRead bool
	}{
		{name: "the rule is off", setting: 0, childSince: 10, parents: []blockedParent{{state: locked(10)}}, noParentRead: true},
		{name: "a locked parent has blocked it for long", setting: age, childSince: 70, parents: []blockedParent{{state: locked(60)}}, wantRollback: true},
		{name: "a creating parent has blocked it for long", setting: age, childSince: 70, parents: []blockedParent{{state: creating(60), spendErr: errors.ErrTxCreating}}, wantRollback: true},
		{name: "a parent has blocked it for exactly the age", setting: age, childSince: 70, parents: []blockedParent{{state: locked(height - age)}}, wantRollback: true},
		{name: "a parent has blocked it for one block less than the age", setting: age, childSince: 70, parents: []blockedParent{{state: locked(height - age + 1)}}},
		{name: "a parent became blocked recently", setting: age, childSince: 70, parents: []blockedParent{{state: locked(95)}}},
		{name: "the record is one block older than the age", setting: age, childSince: height - age - 1, parents: []blockedParent{{state: locked(10)}}, wantRollback: true},
		{name: "the record is exactly the age", setting: age, childSince: height - age, parents: []blockedParent{{state: locked(10)}}},
		{name: "the record is younger than the age", setting: age, childSince: 85, parents: []blockedParent{{state: locked(10)}}},
		{name: "the record has no age", setting: age, childSince: 0, parents: []blockedParent{{state: locked(10)}}},
		{name: "the parent is not in the store", setting: age, childSince: 70, parents: []blockedParent{{}}},
		{name: "the parent cannot be read", setting: age, childSince: 70, parents: []blockedParent{{readErr: errors.NewStorageError("read boom")}}},
		{name: "the parent no longer blocks", setting: age, childSince: 70, parents: []blockedParent{{state: &meta.Data{UnminedSince: 10}}}},
		{name: "the parent has no unminedSince", setting: age, childSince: 70, parents: []blockedParent{{state: locked(0)}}},
		{name: "the parent has the floor of a backdated unminedSince", setting: age, childSince: 70, parents: []blockedParent{{state: locked(1)}}},
		{name: "the spend does not name the parent", setting: age, childSince: 70, parents: []blockedParent{{state: locked(10), noTxID: true}}},
		{name: "two parents have blocked it for long", setting: age, childSince: 70, parents: []blockedParent{{state: locked(10)}, {state: creating(20), spendErr: errors.ErrTxCreating}}, wantRollback: true},
		{name: "one of two parents became blocked recently", setting: age, childSince: 70, parents: []blockedParent{{state: locked(10)}, {state: locked(95)}}},
		{name: "one of two parents cannot be read", setting: age, childSince: 70, parents: []blockedParent{{state: locked(10)}, {}}},
		{name: "another input failed for a reason that is not a parent in flight", setting: age, childSince: 70, parents: []blockedParent{{state: locked(10)}, {state: locked(10), spendErr: errors.NewStorageError("write timed out")}}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			hashes := []chainhash.Hash{parentA, parentB}[:len(tc.parents)]

			txA := txWithParents(1, hashes...)
			hashA := *txA.TxIDChainHash()

			getResp := map[chainhash.Hash]*meta.Data{hashA: {Creating: true, Attempt: sweepToken, Tx: txA, UnminedSince: tc.childSince}}
			getErrs := map[chainhash.Hash]error{}
			spends := make([]*utxo.Spend, len(tc.parents))

			for i, p := range tc.parents {
				if p.state != nil {
					getResp[hashes[i]] = p.state
				}

				if p.readErr != nil {
					getErrs[hashes[i]] = p.readErr
				}

				spendErr := p.spendErr
				if spendErr == nil {
					spendErr = errors.ErrTxLocked
				}

				spends[i] = &utxo.Spend{Err: spendErr}
				if !p.noTxID {
					spends[i].TxID = &hashes[i]
				}
			}

			store := &fakeSweepStore{
				NullStore:   ns,
				stale:       []chainhash.Hash{hashA},
				getResp:     getResp,
				getErrs:     getErrs,
				spendErr:    errors.ErrTxLocked,
				spendSpends: spends,
			}

			s := newSweepServer(t, store)
			s.settings.Pruner.CreatingTxSweepBlockedAgeBlocks = tc.setting
			s.sweepCreatingTxs(context.Background(), height)

			if tc.wantRollback {
				require.Equal(t, []chainhash.Hash{hashA}, store.began, "the record is rolled back")
			} else {
				require.Empty(t, store.began, "the record is left creating for the next pass")
				require.Empty(t, store.events)
			}

			if tc.noParentRead {
				for _, parent := range hashes {
					require.NotContains(t, store.getReads, parent, "the rule is off, so no parent is read")
				}
			}

			require.Len(t, store.queryBefore, 1, "the child's age comes from its own record, not from a second query")
		})
	}
}

// A record that lost a double-spend is rolled back whatever the setting is, and without reading any parent.
func TestSweepCreatingTxsRollsBackAnUnwinnableRecordWithTheBlockedAgeOff(t *testing.T) {
	ns, err := nullstore.NewNullStore()
	require.NoError(t, err)

	parent := chainhash.HashH([]byte("parent"))
	txA := txWithParents(1, parent)
	hashA := *txA.TxIDChainHash()

	store := &fakeSweepStore{
		NullStore:   ns,
		stale:       []chainhash.Hash{hashA},
		getResp:     map[chainhash.Hash]*meta.Data{hashA: {Creating: true, Attempt: sweepToken, Tx: txA, UnminedSince: 99}},
		spendErr:    errors.ErrSpent,
		spendSpends: []*utxo.Spend{{TxID: &parent, Err: errors.ErrSpent}},
	}

	s := newSweepServer(t, store)
	require.Equal(t, uint32(0), s.settings.Pruner.CreatingTxSweepBlockedAgeBlocks, "the rule is off unless it is set")
	s.sweepCreatingTxs(context.Background(), 100)

	require.Equal(t, []chainhash.Hash{hashA}, store.began)
	require.NotContains(t, store.getReads, parent)
}

func TestSweepCreatingTxsRollForwardDoesNotIgnoreLocks(t *testing.T) {
	ns, err := nullstore.NewNullStore()
	require.NoError(t, err)

	txA := oneOutputTx(1)
	hashA := *txA.TxIDChainHash()

	store := &fakeSweepStore{
		NullStore: ns,
		stale:     []chainhash.Hash{hashA},
		getResp:   map[chainhash.Hash]*meta.Data{hashA: {Creating: true, Attempt: sweepToken, Tx: txA}},
	}

	newSweepServer(t, store).sweepCreatingTxs(context.Background(), 100)

	var o utxo.CreateOptions

	for _, opt := range store.spendOpts {
		opt(&o)
	}

	require.True(t, o.SpendOnly)
	require.False(t, o.IgnoreFlags.IgnoreLocked, "the sweep must not bypass the locked-parent hand-off barrier")
}

func TestSweepCreatingTxsFinishesAbandonedRollback(t *testing.T) {
	ns, err := nullstore.NewNullStore()
	require.NoError(t, err)

	txA := oneOutputTx(1)
	txA.Inputs[0].PreviousTxScript = bscript.NewFromBytes([]byte{0x51})
	txA.Inputs[0].PreviousTxSatoshis = 2000
	hashA := *txA.TxIDChainHash()

	store := &fakeSweepStore{
		NullStore: ns,
		stale:     []chainhash.Hash{hashA},
		getResp:   map[chainhash.Hash]*meta.Data{hashA: {Creating: true, Attempt: sweepToken, RollingBack: true, Tx: txA}},
	}

	newSweepServer(t, store).sweepCreatingTxs(context.Background(), 100)

	require.Equal(t, []string{"take", "unspend", "release", "end"}, store.events, "an abandoned rollback is finished by taking the hold, unspending, releasing it and ending the rollback")
	require.Equal(t, []string{"take", "leases", "unspend", "release", "end"}, store.trace, "the leases are read under the hold and before any unspend")
	require.Len(t, store.unspent, 1)
	require.Len(t, store.unspent[0], len(txA.Inputs), "every input is unspent")
	require.Equal(t, []chainhash.Hash{hashA}, store.ended)
	require.Empty(t, store.spends, "a rolling-back record must never be re-spent")
	require.Empty(t, store.finalized)
}

// A rollback the sweep runs reverses the spends of a record the pruner does not own. The store
// keeps the historical spends that predate the record, and counts history from the larger of its
// own height and the height of the call. The sweep names both: the record it rolls back, and the
// pruner height it runs at. A store that is not told keeps no spend, or counts history from a
// tip that lags behind the pruner.
func TestSweepCreatingTxsReversesUnderTheRecordAndThePrunerHeight(t *testing.T) {
	const prunerHeight = uint32(137)

	for name, tc := range map[string]struct {
		meta      func(tx *bt.Tx) *meta.Data
		configure func(*fakeSweepStore)
	}{
		"a loser rolled back": {
			meta: func(tx *bt.Tx) *meta.Data { return &meta.Data{Creating: true, Attempt: sweepToken, Tx: tx} },
			configure: func(f *fakeSweepStore) {
				f.spendErr = errors.ErrSpent
				f.spendSpends = []*utxo.Spend{{Vout: 8, Err: errors.ErrSpent}}
			},
		},
		"an abandoned rollback finished": {
			meta: func(tx *bt.Tx) *meta.Data {
				return &meta.Data{Creating: true, Attempt: sweepToken, RollingBack: true, Tx: tx}
			},
			configure: func(*fakeSweepStore) {},
		},
	} {
		t.Run(name, func(t *testing.T) {
			ns, err := nullstore.NewNullStore()
			require.NoError(t, err)

			tx := oneOutputTx(1)
			tx.Inputs[0].PreviousTxScript = bscript.NewFromBytes([]byte{0x51})
			tx.Inputs[0].PreviousTxSatoshis = 2000
			hash := *tx.TxIDChainHash()

			store := &fakeSweepStore{
				NullStore: ns,
				stale:     []chainhash.Hash{hash},
				getResp:   map[chainhash.Hash]*meta.Data{hash: tc.meta(tx)},
			}
			tc.configure(store)

			newSweepServer(t, store).sweepCreatingTxs(context.Background(), prunerHeight)

			require.Len(t, store.unspendGuards, 1, "the rollback reversed the spends once")
			require.NotNil(t, store.unspendGuards[0].Record, "the reversal names the record whose spends it undoes")
			require.Equal(t, hash, *store.unspendGuards[0].Record)
			require.Equal(t, prunerHeight, store.unspendGuards[0].CallHeight, "the reversal counts history from the pruner height")
		})
	}
}

func TestSweepCreatingTxsFinishesRollbackPastUnspendableInput(t *testing.T) {
	ns, err := nullstore.NewNullStore()
	require.NoError(t, err)

	txA := oneOutputTx(1)
	badInput := &bt.Input{PreviousTxOutIndex: 9, PreviousTxSatoshis: 1, PreviousTxScript: bscript.NewFromBytes([]byte{0x51}), UnlockingScript: bscript.NewFromBytes([]byte{})}
	require.NoError(t, badInput.PreviousTxIDAdd(&chainhash.Hash{1}))
	txA.Inputs = append(txA.Inputs, badInput)
	hashA := *txA.TxIDChainHash()

	store := &fakeSweepStore{
		NullStore: ns,
		stale:     []chainhash.Hash{hashA},
		getResp:   map[chainhash.Hash]*meta.Data{hashA: {Creating: true, Attempt: sweepToken, RollingBack: true, Tx: txA}},
		unspendFn: func(spends []*utxo.Spend) error {
			for _, sp := range spends {
				if sp.Vout == 9 {
					return errors.NewUtxoHashMismatchError("mismatch")
				}
			}

			return nil
		},
	}

	newSweepServer(t, store).sweepCreatingTxs(context.Background(), 100)

	require.Equal(t, []chainhash.Hash{hashA}, store.ended, "a mismatched input must not keep the record rolling back")
	require.Len(t, store.unspent, 3, "one batch call, then one call per input")
	require.Len(t, store.unspent[1], 1)
	require.Equal(t, uint32(1), store.unspent[1][0].Vout, "the valid input is unspent on its own")
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

// A creating record with no attempt token is the record of a create whose clear of the creating
// bin failed. Its inputs were spent before it was written, or it never had any to spend. The sweep
// clears the bin: it spends nothing, rolls nothing back and announces nothing.
func TestSweepCreatingTxsClearsARecordWithNoAttemptToken(t *testing.T) {
	ns, err := nullstore.NewNullStore()
	require.NoError(t, err)

	txA := oneOutputTx(1)
	hashA := *txA.TxIDChainHash()

	for name, finalizeErr := range map[string]error{
		"cleared":       nil,
		"already clear": utxo.MarkAlreadyFinal(errors.NewTxExistsError("already final")),
		// A spend error would send a record with a token to the rollback. This one must not go.
		"clear fails": errors.NewStorageError("clear boom"),
	} {
		store := &fakeSweepStore{
			NullStore:   ns,
			stale:       []chainhash.Hash{hashA},
			getResp:     map[chainhash.Hash]*meta.Data{hashA: {Creating: true, Tx: txA}},
			finalizeErr: finalizeErr,
			spendErr:    errors.ErrTxNotFound,
		}

		newSweepServer(t, store).sweepCreatingTxs(context.Background(), 100)

		require.Equal(t, []chainhash.Hash{hashA}, store.finalized, name)
		require.Empty(t, store.spends, "%s: there is no spend to repeat", name)
		require.Empty(t, store.began, "%s: a record that no create-first attempt wrote is never rolled back", name)
		require.Empty(t, store.unspent, name)
	}
}

func TestSweepCreatingTxsLeavesUnrecoverable(t *testing.T) {
	ns, err := nullstore.NewNullStore()
	require.NoError(t, err)

	txA := oneOutputTx(1)
	hashA := *txA.TxIDChainHash()

	store := &fakeSweepStore{
		NullStore: ns,
		stale:     []chainhash.Hash{hashA},
		getResp:   map[chainhash.Hash]*meta.Data{hashA: {Creating: true, Attempt: sweepToken, Tx: txA}},
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

// TestSweepCreatingTxsFinalizeOfGoneRecordReleasesItsLeaseAndReversesNothing pins that a
// roll-forward whose finalize is refused, because a rollback began after its lease was granted,
// releases the lease and unspends nothing itself: the owner of that rollback waits for the lease
// and then reverses every input. The pass does not claim a rollback another caller owns.
func TestSweepCreatingTxsFinalizeOfGoneRecordReleasesItsLeaseAndReversesNothing(t *testing.T) {
	ns, err := nullstore.NewNullStore()
	require.NoError(t, err)

	txA := oneOutputTx(1)
	hashA := *txA.TxIDChainHash()

	store := &fakeSweepStore{
		NullStore:   ns,
		stale:       []chainhash.Hash{hashA},
		getResp:     map[chainhash.Hash]*meta.Data{hashA: {Creating: true, Attempt: sweepToken, Tx: txA}},
		finalizeErr: errors.NewTxNotFoundError("tx is rolling back"),

		beginDeclined: true,
	}

	newSweepServer(t, store).sweepCreatingTxs(context.Background(), 100)

	require.Equal(t, []chainhash.Hash{hashA}, store.finalized)
	require.Equal(t, []string{"lease", "spend", "finalize", "unlease", "leases", "begin"}, store.trace,
		"the lease is released after the refused finalize, and the declined claim ends the pass for this record")
	require.Equal(t, [][]byte{fakeLease}, store.leasesFreed)
	require.Empty(t, store.unspent, "the roll-forward reverses nothing: the owner of the rollback does")
	require.Empty(t, store.taken, "the roll-forward never takes the rollback hold")
	require.Empty(t, store.ended)
	require.Empty(t, store.unlocked, "a record that was not finalized is never unlocked")
}

func TestSweepCreatingTxsDoesNotSpendForRecordThatBeganRollingBack(t *testing.T) {
	ns, err := nullstore.NewNullStore()
	require.NoError(t, err)

	txA := oneOutputTx(1)
	hashA := *txA.TxIDChainHash()

	store := &fakeSweepStore{
		NullStore: ns,
		stale:     []chainhash.Hash{hashA},
		getResp:   map[chainhash.Hash]*meta.Data{hashA: {Creating: true, Attempt: sweepToken, Tx: txA}},
	}

	srv := newSweepServer(t, store)
	gets := 0
	store.onGet = func() {
		gets++

		if gets == 2 {
			store.getResp[hashA] = &meta.Data{Creating: true, Attempt: sweepToken, RollingBack: true, Tx: txA}
		}
	}

	srv.sweepCreatingTxs(context.Background(), 100)

	require.Empty(t, store.spends, "the roll-forward must not spend the parents of a record whose rollback began after the sweep read it")
	require.Equal(t, []string{"lease"}, store.trace, "the refused lease is the only call the roll-forward makes")
	require.Empty(t, store.leasesTaken)
	require.Empty(t, store.leasesFreed, "a lease that was not granted is not released")
	require.Empty(t, store.finalized)
	require.Empty(t, store.unspent)
}

// TestSweepCreatingTxsSecondSweepOfMarkedRecordDoesNothing pins the exclusive ownership of an
// abandoned rollback: one sweep takes the hold of the attempt and finishes the rollback, and a
// second sweep that read the same marked record fails the take and reverses nothing, so its
// delayed unspend cannot clear the spends of an attempt that re-created the txid.
func TestSweepCreatingTxsSecondSweepOfMarkedRecordDoesNothing(t *testing.T) {
	ns, err := nullstore.NewNullStore()
	require.NoError(t, err)

	txA := oneOutputTx(1)
	hashA := *txA.TxIDChainHash()
	token := []byte{9, 9, 9}

	store := &fakeSweepStore{
		NullStore: ns,
		stale:     []chainhash.Hash{hashA},
		getResp:   map[chainhash.Hash]*meta.Data{hashA: {Creating: true, RollingBack: true, Attempt: token, Tx: txA}},
	}

	srv := newSweepServer(t, store)

	srv.sweepCreatingTxs(context.Background(), 100)

	require.Equal(t, []string{"take", "unspend", "release", "end"}, store.events, "the first sweep takes the hold, unspends, releases and ends the rollback")
	require.Equal(t, [][]byte{token}, store.taken, "the take names the attempt the sweep read from the record")

	store.events = nil
	store.takeDeclined = true

	srv.sweepCreatingTxs(context.Background(), 100)

	require.Equal(t, []string{"take"}, store.events, "a sweep that cannot take the hold must reverse nothing and end nothing")
	require.Len(t, store.unspent, 1, "only the first sweep unspent")
	require.Len(t, store.ended, 1, "only the first sweep ended the rollback")
}

// TestSweepCreatingTxsEndsReversedRollbackWithoutTheTransaction pins that a rollback whose spends
// are all reversed is finished from the markers alone: its external blob may be gone, so reading
// the transaction fails, and the sweep must neither read it, take the hold nor unspend again.
func TestSweepCreatingTxsEndsReversedRollbackWithoutTheTransaction(t *testing.T) {
	ns, err := nullstore.NewNullStore()
	require.NoError(t, err)

	txA := oneOutputTx(1)
	hashA := *txA.TxIDChainHash()
	token := []byte{4, 2}

	store := &fakeSweepStore{
		NullStore:    ns,
		stale:        []chainhash.Hash{hashA},
		getResp:      map[chainhash.Hash]*meta.Data{hashA: {Creating: true, RollingBack: true, Reversed: true, Attempt: token, Tx: txA}},
		txUnreadable: map[chainhash.Hash]bool{hashA: true},
	}

	newSweepServer(t, store).sweepCreatingTxs(context.Background(), 100)

	require.Equal(t, []string{"end"}, store.events, "a reversed rollback is ended without a take, an unspend or a read of the transaction")
	require.Equal(t, []chainhash.Hash{hashA}, store.ended)
	require.Empty(t, store.unspent)
}

// TestSweepCreatingTxsLeavesReversedRollbackWhoseEndFails pins that a failed
// end leaves the record for the next pass, which again reads only the markers.
func TestSweepCreatingTxsLeavesReversedRollbackWhoseEndFails(t *testing.T) {
	ns, err := nullstore.NewNullStore()
	require.NoError(t, err)

	txA := oneOutputTx(1)
	hashA := *txA.TxIDChainHash()

	store := &fakeSweepStore{
		NullStore:    ns,
		stale:        []chainhash.Hash{hashA},
		getResp:      map[chainhash.Hash]*meta.Data{hashA: {Creating: true, Attempt: sweepToken, RollingBack: true, Reversed: true, Tx: txA}},
		txUnreadable: map[chainhash.Hash]bool{hashA: true},
		endErr:       errors.NewStorageError("end boom"),
	}

	newSweepServer(t, store).sweepCreatingTxs(context.Background(), 100)

	require.Equal(t, []string{"end"}, store.events)
	require.Empty(t, store.unspent)
}

// TestSweepCreatingTxsDoesNotEndARollbackWhoseHoldWasNotReleased pins that a release that fails after
// the reversal leaves the record rolling back: ending it would mark the spends reversed over a busy
// marker the conversion refuses, and nothing would then take the marker over.
func TestSweepCreatingTxsDoesNotEndARollbackWhoseHoldWasNotReleased(t *testing.T) {
	ns, err := nullstore.NewNullStore()
	require.NoError(t, err)

	txA := oneOutputTx(1)
	hashA := *txA.TxIDChainHash()

	store := &fakeSweepStore{
		NullStore:  ns,
		stale:      []chainhash.Hash{hashA},
		getResp:    map[chainhash.Hash]*meta.Data{hashA: {Creating: true, Attempt: sweepToken, RollingBack: true, Tx: txA}},
		releaseErr: errors.NewStorageError("release boom"),
	}

	newSweepServer(t, store).sweepCreatingTxs(context.Background(), 100)

	require.Equal(t, []string{"take", "unspend", "release"}, store.events)
	require.Empty(t, store.ended)
}

// TestSweepCreatingTxsDeletesExpiredHold pins that the sweep reaches a hold, which carries no
// transaction bytes, asks the store to clear it without adopting, and reverses nothing.
func TestSweepCreatingTxsDeletesExpiredHold(t *testing.T) {
	ns, err := nullstore.NewNullStore()
	require.NoError(t, err)

	hash := chainhash.HashH([]byte("held"))

	store := &fakeSweepStore{
		NullStore:   ns,
		stale:       []chainhash.Hash{hash},
		getResp:     map[chainhash.Hash]*meta.Data{hash: {Creating: true, Attempt: sweepToken, RollingBack: true, Hold: true}},
		clearResult: true,
	}

	newSweepServer(t, store).sweepCreatingTxs(context.Background(), 100)

	require.Equal(t, []string{"clear"}, store.events, "a hold is cleared and nothing else is done to it")
	require.Equal(t, []chainhash.Hash{hash}, store.cleared)
	require.Empty(t, store.spends)
	require.Empty(t, store.unspent)
}

// TestSweepCreatingTxsLeavesLiveHold pins that a hold the store does not clear, because it has
// not expired or its holder is busy, is left alone and is not counted as a failure.
func TestSweepCreatingTxsLeavesLiveHold(t *testing.T) {
	ns, err := nullstore.NewNullStore()
	require.NoError(t, err)

	hash := chainhash.HashH([]byte("live hold"))

	store := &fakeSweepStore{
		NullStore: ns,
		stale:     []chainhash.Hash{hash},
		getResp:   map[chainhash.Hash]*meta.Data{hash: {Creating: true, Attempt: sweepToken, RollingBack: true, Hold: true}},
	}

	newSweepServer(t, store).sweepCreatingTxs(context.Background(), 100)

	require.Equal(t, []string{"clear"}, store.events)
	require.Empty(t, store.unspent)
	require.Empty(t, store.finalized)
}

// sweepFailures reads the sweep's failure counter.
func sweepFailures() float64 {
	return testutil.ToFloat64(prunerCreatingSweepFailures)
}

// TestSweepCreatingTxsLeavesUnwinnableRecordWhoseLeaseIsLive pins that the sweep does not claim the
// rollback of a record while a spender of its attempt may still be spending: the rollback would
// have to wait for that spender, so the record is left creating and counted as a failure.
func TestSweepCreatingTxsLeavesUnwinnableRecordWhoseLeaseIsLive(t *testing.T) {
	ns, err := nullstore.NewNullStore()
	require.NoError(t, err)

	txA := oneOutputTx(1)
	hashA := *txA.TxIDChainHash()

	store := &fakeSweepStore{
		NullStore:   ns,
		stale:       []chainhash.Hash{hashA},
		getResp:     map[chainhash.Hash]*meta.Data{hashA: {Creating: true, Attempt: sweepToken, Tx: txA}},
		spendErr:    errors.ErrSpent,
		spendSpends: []*utxo.Spend{{Err: errors.ErrSpent}},
		leasesLive:  []bool{true},
	}

	srv := newSweepServer(t, store)
	before := sweepFailures()

	srv.sweepCreatingTxs(context.Background(), 100)

	require.Equal(t, []string{"lease", "spend", "unlease", "leases"}, store.trace)
	require.Empty(t, store.began, "the rollback must not be claimed while a lease is live")
	require.Empty(t, store.unspent)
	require.Empty(t, store.ended)
	require.Empty(t, store.finalized)
	require.Equal(t, before+1, sweepFailures(), "the record is counted as not healed in this pass")
}

// TestSweepCreatingTxsLeavesUnwinnableRecordWhoseLeasesCannotBeRead pins that a failed read of the
// leases is treated as a live lease: nothing is rolled back.
func TestSweepCreatingTxsLeavesUnwinnableRecordWhoseLeasesCannotBeRead(t *testing.T) {
	ns, err := nullstore.NewNullStore()
	require.NoError(t, err)

	txA := oneOutputTx(1)
	hashA := *txA.TxIDChainHash()

	store := &fakeSweepStore{
		NullStore:     ns,
		stale:         []chainhash.Hash{hashA},
		getResp:       map[chainhash.Hash]*meta.Data{hashA: {Creating: true, Attempt: sweepToken, Tx: txA}},
		spendErr:      errors.ErrSpent,
		leasesLiveErr: errors.NewStorageError("lease read boom"),
	}

	srv := newSweepServer(t, store)
	before := sweepFailures()

	srv.sweepCreatingTxs(context.Background(), 100)

	require.Equal(t, 1, store.liveReads)
	require.Empty(t, store.events, "no rollback call is made when the leases cannot be read")
	require.Empty(t, store.unspent)
	require.Equal(t, before+1, sweepFailures())
}

// TestSweepCreatingTxsKeepsLeaseOfSpendWithUnknownOutcome pins that a re-spend that may still
// commit keeps its lease, so that a rollback of the record waits until the store can no longer
// apply it.
func TestSweepCreatingTxsKeepsLeaseOfSpendWithUnknownOutcome(t *testing.T) {
	ns, err := nullstore.NewNullStore()
	require.NoError(t, err)

	txA := oneOutputTx(1)
	hashA := *txA.TxIDChainHash()

	store := &fakeSweepStore{
		NullStore: ns,
		stale:     []chainhash.Hash{hashA},
		getResp:   map[chainhash.Hash]*meta.Data{hashA: {Creating: true, Attempt: sweepToken, Tx: txA}},
		spendErr:  errors.NewServiceUnavailableError("spend batch did not complete"),
		// Per-input results do not make a spend known when the store stopped waiting for its batch.
		spendSpends: []*utxo.Spend{{}},
	}

	newSweepServer(t, store).sweepCreatingTxs(context.Background(), 100)

	require.Equal(t, []string{"lease", "spend"}, store.trace, "the lease of a spend that may still commit is not released")
	require.Empty(t, store.leasesFreed)
	require.Empty(t, store.events)
}

// TestSweepCreatingTxsFinishRollbackWaitsForALiveLease pins that the sweep reverses nothing for an
// abandoned rollback while a spender of the attempt may still be spending: it takes the hold,
// finds the lease live, releases the hold and leaves the record rolling back for a later pass.
func TestSweepCreatingTxsFinishRollbackWaitsForALiveLease(t *testing.T) {
	ns, err := nullstore.NewNullStore()
	require.NoError(t, err)

	txA := oneOutputTx(1)
	hashA := *txA.TxIDChainHash()

	store := &fakeSweepStore{
		NullStore:  ns,
		stale:      []chainhash.Hash{hashA},
		getResp:    map[chainhash.Hash]*meta.Data{hashA: {Creating: true, Attempt: sweepToken, RollingBack: true, Tx: txA}},
		leasesLive: []bool{true},
	}

	srv := newSweepServer(t, store)

	srv.sweepCreatingTxs(context.Background(), 100)

	require.Equal(t, []string{"take", "leases", "release"}, store.trace, "the hold is taken, the lease is found live and the hold is released")
	require.Empty(t, store.unspent, "no spend is reversed while a spend of the attempt may still land")
	require.Empty(t, store.ended, "the rollback is not ended")

	store.trace = nil

	srv.sweepCreatingTxs(context.Background(), 100)

	require.Equal(t, []string{"take", "leases", "unspend", "release", "end"}, store.trace, "a later pass, with the lease stale, finishes the rollback")
	require.Equal(t, []chainhash.Hash{hashA}, store.ended)
}

// TestSweepCreatingTxsFinishRollbackReleasesTheHoldWhenLeasesCannotBeRead pins that a failed read
// of the leases under the hold releases the hold, reverses nothing and is a sweep failure.
func TestSweepCreatingTxsFinishRollbackReleasesTheHoldWhenLeasesCannotBeRead(t *testing.T) {
	ns, err := nullstore.NewNullStore()
	require.NoError(t, err)

	txA := oneOutputTx(1)
	hashA := *txA.TxIDChainHash()

	store := &fakeSweepStore{
		NullStore:     ns,
		stale:         []chainhash.Hash{hashA},
		getResp:       map[chainhash.Hash]*meta.Data{hashA: {Creating: true, Attempt: sweepToken, RollingBack: true, Tx: txA}},
		leasesLiveErr: errors.NewStorageError("lease read boom"),
	}

	srv := newSweepServer(t, store)
	before := sweepFailures()

	srv.sweepCreatingTxs(context.Background(), 100)

	require.Equal(t, []string{"take", "leases", "release"}, store.trace)
	require.Equal(t, 1, store.releaseCalls)
	require.Empty(t, store.unspent)
	require.Empty(t, store.ended)
	require.Equal(t, before+1, sweepFailures(), "the read error surfaces as a failure of the pass")
}

// TestSweepCreatingTxsRollForwardNamesTheAttemptOnItsLease pins that the lease is asked for the
// attempt the sweep read from the record, so the store can refuse it for a later attempt.
func TestSweepCreatingTxsRollForwardNamesTheAttemptOnItsLease(t *testing.T) {
	ns, err := nullstore.NewNullStore()
	require.NoError(t, err)

	txA := oneOutputTx(1)
	hashA := *txA.TxIDChainHash()
	token := []byte{7, 7}

	store := &fakeSweepStore{
		NullStore: ns,
		stale:     []chainhash.Hash{hashA},
		getResp:   map[chainhash.Hash]*meta.Data{hashA: {Creating: true, Attempt: token, Tx: txA}},
	}

	newSweepServer(t, store).sweepCreatingTxs(context.Background(), 100)

	require.Equal(t, [][]byte{token}, store.leaseTokens)
	require.Equal(t, []chainhash.Hash{hashA}, store.finalized)
}

// TestSweepCreatingTxsLeaseErrorSpendsNothing pins that a store fault on the lease stops the
// roll-forward before any input is spent.
func TestSweepCreatingTxsLeaseErrorSpendsNothing(t *testing.T) {
	ns, err := nullstore.NewNullStore()
	require.NoError(t, err)

	txA := oneOutputTx(1)
	hashA := *txA.TxIDChainHash()

	store := &fakeSweepStore{
		NullStore: ns,
		stale:     []chainhash.Hash{hashA},
		getResp:   map[chainhash.Hash]*meta.Data{hashA: {Creating: true, Attempt: sweepToken, Tx: txA}},
		leaseErr:  errors.NewStorageError("lease boom"),
	}

	newSweepServer(t, store).sweepCreatingTxs(context.Background(), 100)

	require.Equal(t, []string{"lease"}, store.trace)
	require.Empty(t, store.spends)
	require.Empty(t, store.finalized)
}

// TestSweepCreatingTxsKeepsLeaseOfSpendWithNoPerInputResult pins the other unknown outcome: a spend
// that failed without a result per input may still commit, so its lease is kept.
func TestSweepCreatingTxsKeepsLeaseOfSpendWithNoPerInputResult(t *testing.T) {
	ns, err := nullstore.NewNullStore()
	require.NoError(t, err)

	txA := oneOutputTx(1)
	hashA := *txA.TxIDChainHash()

	store := &fakeSweepStore{
		NullStore: ns,
		stale:     []chainhash.Hash{hashA},
		getResp:   map[chainhash.Hash]*meta.Data{hashA: {Creating: true, Attempt: sweepToken, Tx: txA}},
		spendErr:  errors.ErrTxLocked,
	}

	newSweepServer(t, store).sweepCreatingTxs(context.Background(), 100)

	require.Equal(t, []string{"lease", "spend"}, store.trace)
	require.Empty(t, store.leasesFreed)
}

// sweepLogRecorder records the info and warning lines of a sweep pass.
type sweepLogRecorder struct {
	ulogger.TestLogger

	lines []string
}

func (l *sweepLogRecorder) Infof(format string, args ...interface{}) {
	l.lines = append(l.lines, fmt.Sprintf(format, args...))
}

func (l *sweepLogRecorder) Warnf(format string, args ...interface{}) {
	l.lines = append(l.lines, fmt.Sprintf(format, args...))
}

func (l *sweepLogRecorder) contains(part string) bool {
	for _, line := range l.lines {
		if strings.Contains(line, part) {
			return true
		}
	}

	return false
}

// TestSweepCreatingTxsCountsARollbackAsFinishedOnlyWhenItEnded pins that a pass which could not
// finish a marked rollback, because a spend lease is live or the hold is not the sweep's to take,
// does not report it as finished and does not hide a page that made no progress.
func TestSweepCreatingTxsCountsARollbackAsFinishedOnlyWhenItEnded(t *testing.T) {
	ns, err := nullstore.NewNullStore()
	require.NoError(t, err)

	txA := oneOutputTx(1)
	hashA := *txA.TxIDChainHash()

	store := &fakeSweepStore{
		NullStore:  ns,
		stale:      []chainhash.Hash{hashA},
		getResp:    map[chainhash.Hash]*meta.Data{hashA: {Creating: true, Attempt: sweepToken, RollingBack: true, Tx: txA}},
		leasesLive: []bool{true},
	}

	srv := newSweepServer(t, store)
	recorder := &sweepLogRecorder{}
	srv.logger = recorder
	before := sweepFailures()

	srv.sweepCreatingTxs(context.Background(), 100)

	require.Empty(t, store.ended, "precondition: the live lease stopped the finish")
	require.False(t, recorder.contains("finished abandoned rollback"), "a rollback that was not ended is not reported as finished")
	require.True(t, recorder.contains("rolled forward 0 of 1"), "a page with no progress is reported")
	require.Equal(t, before, sweepFailures(), "a live lease is a wait, not a failure of the pass")

	recorder.lines = nil

	srv.sweepCreatingTxs(context.Background(), 100)

	require.Equal(t, []chainhash.Hash{hashA}, store.ended, "precondition: the second pass ended the rollback")
	require.True(t, recorder.contains("finished abandoned rollback"))
	require.False(t, recorder.contains("rolled forward 0 of 1"))
}

// TestSweepCreatingTxsLeavesTheHandOffToTheActorThatFinalized pins that the sweep hands a record to
// block assembly, and unlocks it, only when its own finalize cleared the creating state. A finalize
// that finds the record already final lost to another actor's finalize of the same attempt. That
// actor owns the hand-off and any unwind of it, so a second hand-off here could leave block assembly
// holding a transaction that actor then deletes.
func TestSweepCreatingTxsLeavesTheHandOffToTheActorThatFinalized(t *testing.T) {
	ns, err := nullstore.NewNullStore()
	require.NoError(t, err)

	txA := oneOutputTx(1)
	hashA := *txA.TxIDChainHash()

	store := &fakeSweepStore{
		NullStore:   ns,
		stale:       []chainhash.Hash{hashA},
		getResp:     map[chainhash.Hash]*meta.Data{hashA: {Creating: true, Attempt: sweepToken, Tx: txA, Fee: 7, SizeInBytes: 99}},
		finalizeErr: utxo.MarkAlreadyFinal(errors.NewTxExistsError("already final")),
	}

	ba := blockassembly.NewMock()

	srv := newSweepServer(t, store)
	srv.blockAssemblyClient = ba

	before := sweepFailures()

	srv.sweepCreatingTxs(context.Background(), 100)

	require.Equal(t, []string{"lease", "spend", "finalize", "unlease"}, store.trace, "a finalize removes only the lease of its caller, so the sweep releases its own, and nothing is rolled back")
	ba.AssertNotCalled(t, "Store", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything)
	require.Empty(t, store.unlocked, "the lock belongs to the actor that hands the transaction off")
	require.Empty(t, store.began)
	require.Equal(t, before, sweepFailures(), "a record another actor completed is healed, not a failure")
}

// TestSweepCreatingTxsRollbackRefusedForALiveLeaseLeavesTheRecordCreating pins what the sweep does
// when a spender takes a lease between the sweep's read of the leases and its claim of the rollback:
// the store refuses the claim, the sweep reverses nothing and the record stays creating.
func TestSweepCreatingTxsRollbackRefusedForALiveLeaseLeavesTheRecordCreating(t *testing.T) {
	ns, err := nullstore.NewNullStore()
	require.NoError(t, err)

	txA := oneOutputTx(1)
	hashA := *txA.TxIDChainHash()

	store := &fakeSweepStore{
		NullStore:   ns,
		stale:       []chainhash.Hash{hashA},
		getResp:     map[chainhash.Hash]*meta.Data{hashA: {Creating: true, Attempt: sweepToken, Tx: txA}},
		spendErr:    errors.ErrSpent,
		spendSpends: []*utxo.Spend{{Err: errors.ErrSpent}},
		beginErr:    utxo.MarkSpendLeaseLive(errors.NewProcessingError("a spender of the attempt holds a live lease")),
		spendWait:   30 * time.Millisecond,
	}

	srv := newSweepServer(t, store)
	before := sweepFailures()

	srv.sweepCreatingTxs(context.Background(), 100)

	require.NotEmpty(t, store.began, "the claim was tried")
	require.Empty(t, store.unspent, "a spend of the attempt may still land, so nothing is reversed")
	require.Empty(t, store.ended)
	require.Zero(t, store.releaseCalls, "no claim was made, so there is no hold to release")
	require.Equal(t, before+1, sweepFailures())

	for _, event := range store.events {
		require.Equal(t, "begin", event)
	}
}

// TestSweepCreatingTxsKeepsLeaseOfSpendWithAnInputInDoubt pins that a re-spend whose per-input error
// is not a verdict of the store keeps its lease: the write of that input may still land.
func TestSweepCreatingTxsKeepsLeaseOfSpendWithAnInputInDoubt(t *testing.T) {
	ns, err := nullstore.NewNullStore()
	require.NoError(t, err)

	txA := oneOutputTx(1)
	hashA := *txA.TxIDChainHash()

	store := &fakeSweepStore{
		NullStore:   ns,
		stale:       []chainhash.Hash{hashA},
		getResp:     map[chainhash.Hash]*meta.Data{hashA: {Creating: true, Attempt: sweepToken, Tx: txA}},
		spendErr:    errors.NewUtxoError("spend failed"),
		spendSpends: []*utxo.Spend{{Err: errors.NewStorageError("write timed out")}},
	}

	newSweepServer(t, store).sweepCreatingTxs(context.Background(), 100)

	require.Equal(t, []string{"lease", "spend"}, store.trace, "the lease stays live and no rollback is claimed")
	require.Empty(t, store.leasesFreed)
	require.Empty(t, store.began)
}

// heightRecordingBlockAssembly records the height each Store call names as the one the transaction
// was validated at, and the lane position it carries.
type heightRecordingBlockAssembly struct {
	*blockassembly.Mock

	heights       []uint32
	lanePositions []uint64
}

func (h *heightRecordingBlockAssembly) Store(ctx context.Context, hash *chainhash.Hash, fee, size uint64, txInpoints subtree.TxInpoints, validatedHeight uint32, lanePosition uint64) (bool, error) {
	h.heights = append(h.heights, validatedHeight)
	h.lanePositions = append(h.lanePositions, lanePosition)

	return h.Mock.Store(ctx, hash, fee, size, txInpoints, validatedHeight, lanePosition)
}

// The sweep tells block assembly the height the transaction was validated at, by the rule that block
// assembly applies to an unmined transaction on reload.
func TestSweepCreatingTxsAnnouncesTheHeightTheTransactionWasValidatedAt(t *testing.T) {
	ns, err := nullstore.NewNullStore()
	require.NoError(t, err)

	const (
		sweepHeight = uint32(500)
		retention   = uint32(144)
	)

	cases := map[string]struct {
		unminedSince uint32
		want         uint32
	}{
		"inside the retention":      {unminedSince: 490, want: 490},
		"the oldest inside":         {unminedSince: sweepHeight - retention + 1, want: sweepHeight - retention + 1},
		"backdated":                 {unminedSince: utxo.BackdatedUnminedSince(sweepHeight, retention), want: 0},
		"outside the retention":     {unminedSince: 100, want: 0},
		"the floor of a backdating": {unminedSince: 1, want: 0},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			txA := oneOutputTx(1)
			hashA := *txA.TxIDChainHash()

			store := &fakeSweepStore{
				NullStore: ns,
				stale:     []chainhash.Hash{hashA},
				getResp:   map[chainhash.Hash]*meta.Data{hashA: {Creating: true, Attempt: sweepToken, Tx: txA, UnminedSince: tc.unminedSince}},
			}

			ba := &heightRecordingBlockAssembly{Mock: blockassembly.NewMock()}
			ba.On("Store", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(true, nil)

			s := newSweepServer(t, store)
			s.settings.UtxoStore.UnminedTxRetention = retention
			s.blockAssemblyClient = ba
			s.sweepCreatingTxs(context.Background(), sweepHeight)

			require.Equal(t, []uint32{tc.want}, ba.heights)
		})
	}
}

// The sweep tells block assembly the lane position the record carries, as the validator does for a
// record it creates, and a retry of the announce carries the same value. A record with none says 0.
func TestSweepCreatingTxsAnnouncesTheLanePositionOfTheRecord(t *testing.T) {
	ns, err := nullstore.NewNullStore()
	require.NoError(t, err)

	for name, position := range map[string]uint64{
		"a record that carries a position": lane.Pack(210, 3),
		"a record that carries none":       0,
	} {
		t.Run(name, func(t *testing.T) {
			txA := oneOutputTx(1)
			hashA := *txA.TxIDChainHash()

			store := &fakeSweepStore{
				NullStore: ns,
				stale:     []chainhash.Hash{hashA},
				getResp:   map[chainhash.Hash]*meta.Data{hashA: {Creating: true, Attempt: sweepToken, Tx: txA, UnminedSince: 490, LanePosition: position}},
			}

			ba := &flakyBlockAssembly{Mock: blockassembly.NewMock(), failures: 1}

			s := newSweepServer(t, store)
			s.blockAssemblyClient = ba
			s.sweepCreatingTxs(context.Background(), 500)

			require.NotEmpty(t, ba.lanes, "the sweep announced the record")

			for _, got := range ba.lanes {
				require.Equal(t, position, got, "every attempt, the retry too, carries the record's position")
			}
		})
	}
}

// flakyBlockAssembly fails the first failures Store calls, or all of them when failures is negative,
// and records each call with the validated height it named.
type flakyBlockAssembly struct {
	*blockassembly.Mock

	failures int
	calls    int
	heights  []uint32
	lanes    []uint64
	onStore  func()
}

func (f *flakyBlockAssembly) Store(_ context.Context, _ *chainhash.Hash, _, _ uint64, _ subtree.TxInpoints, validatedHeight uint32, lanePosition uint64) (bool, error) {
	f.calls++
	f.heights = append(f.heights, validatedHeight)
	f.lanes = append(f.lanes, lanePosition)

	if f.onStore != nil {
		f.onStore()
	}

	if f.failures != 0 {
		if f.failures > 0 {
			f.failures--
		}

		return false, errors.NewServiceError("ba down")
	}

	return true, nil
}

// handOffSweep returns a server and store with one creating record that rolls forward, and the
// block assembly client it announces to.
func handOffSweep(t *testing.T, failures int) (*Server, *fakeSweepStore, *flakyBlockAssembly, chainhash.Hash) {
	t.Helper()

	ns, err := nullstore.NewNullStore()
	require.NoError(t, err)

	txA := oneOutputTx(1)
	hashA := *txA.TxIDChainHash()

	store := &fakeSweepStore{
		NullStore: ns,
		stale:     []chainhash.Hash{hashA},
		getResp:   map[chainhash.Hash]*meta.Data{hashA: {Creating: true, Attempt: sweepToken, Tx: txA, UnminedSince: 99, Fee: 7, SizeInBytes: 99}},
	}

	ba := &flakyBlockAssembly{Mock: blockassembly.NewMock(), failures: failures}

	s := newSweepServer(t, store)
	s.blockAssemblyClient = ba

	return s, store, ba, hashA
}

// afterRollForward is what the store holds for the record once the sweep has finalized it: final and
// still locked, so it no longer comes back from the query for creating records.
func afterRollForward(store *fakeSweepStore, hash chainhash.Hash, md *meta.Data) {
	store.stale = nil
	store.getResp[hash] = md
}

func TestSweepCreatingTxsRetriesAFailedAnnounceInThePass(t *testing.T) {
	s, store, ba, hashA := handOffSweep(t, 1)

	s.sweepCreatingTxs(context.Background(), 100)

	require.Equal(t, 2, ba.calls, "the announce that failed is tried again")
	require.Equal(t, []chainhash.Hash{hashA}, store.unlocked)
	require.Empty(t, s.handOffs, "nothing is owed")
}

func TestSweepCreatingTxsKeepsAFailingAnnounceOnTheRetryListAndFinishesItNextPass(t *testing.T) {
	s, store, ba, hashA := handOffSweep(t, -1)

	s.sweepCreatingTxs(context.Background(), 100)

	require.Equal(t, 3, ba.calls, "a bounded number of attempts in the pass")
	require.Empty(t, store.unlocked, "a record that is not announced stays locked")
	require.Len(t, s.handOffs, 1)
	require.Equal(t, hashA, s.handOffs[0].hash)
	require.Equal(t, stageAnnounce, s.handOffs[0].stage)
	require.Equal(t, []chainhash.Hash{hashA}, store.spends)

	ba.failures = 0
	ba.calls = 0
	ba.heights = nil

	afterRollForward(store, hashA, &meta.Data{Locked: true})
	store.queryErr = errors.NewStorageError("query boom")

	// 300 is far past the retention of the validated height, so a value worked out again would be 0.
	s.sweepCreatingTxs(context.Background(), 300)

	require.Equal(t, 1, ba.calls)
	require.Equal(t, []uint32{99}, ba.heights, "the retry names the height of the first attempt")
	require.Equal(t, []chainhash.Hash{hashA}, store.unlocked, "the list is drained before the query, which failed")
	require.Empty(t, s.handOffs)
	require.Equal(t, []chainhash.Hash{hashA}, store.spends, "the record is not rolled forward a second time")
	require.Equal(t, []chainhash.Hash{hashA}, store.finalized)
}

func TestSweepCreatingTxsRetriesAFailedUnlockWithoutAnnouncingAgain(t *testing.T) {
	t.Run("in the pass", func(t *testing.T) {
		s, store, ba, hashA := handOffSweep(t, 0)
		store.unlockErrs = []error{errors.NewStorageError("unlock boom")}

		s.sweepCreatingTxs(context.Background(), 100)

		require.Equal(t, 1, ba.calls)
		require.Equal(t, 2, store.unlockCalls)
		require.Equal(t, []chainhash.Hash{hashA}, store.unlocked)
		require.Empty(t, s.handOffs)
	})

	t.Run("on the next pass", func(t *testing.T) {
		s, store, ba, hashA := handOffSweep(t, 0)
		boom := errors.NewStorageError("unlock boom")
		store.unlockErrs = []error{boom, boom, boom}

		s.sweepCreatingTxs(context.Background(), 100)

		require.Equal(t, 1, ba.calls)
		require.Equal(t, 3, store.unlockCalls, "a bounded number of attempts in the pass")
		require.Empty(t, store.unlocked)
		require.Len(t, s.handOffs, 1)
		require.Equal(t, stageUnlock, s.handOffs[0].stage)

		afterRollForward(store, hashA, &meta.Data{Locked: true})
		s.sweepCreatingTxs(context.Background(), 101)

		require.Equal(t, 1, ba.calls, "the announce already succeeded and is not repeated")
		require.Equal(t, []chainhash.Hash{hashA}, store.unlocked)
		require.Empty(t, s.handOffs)
	})
}

// An entry is acted on only after its record is read. A record that is gone, mined or no longer
// locked was finished by another component and is dropped. A record that shows anything else is
// left on the list and logged.
func TestSweepCreatingTxsDrainReadsTheRecordFirst(t *testing.T) {
	cases := []struct {
		name     string
		state    *meta.Data
		readErr  error
		wantKept bool
	}{
		{name: "the record is gone"},
		{name: "the record is mined", state: &meta.Data{Locked: true, BlockIDs: []uint32{3}}},
		{name: "the record is no longer locked", state: &meta.Data{}},
		{name: "the record is creating again", state: &meta.Data{Locked: true, Creating: true}, wantKept: true},
		{name: "the record is conflicting", state: &meta.Data{Locked: true, Conflicting: true}, wantKept: true},
		{name: "the record cannot be read", readErr: errors.NewStorageError("read boom"), wantKept: true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s, store, ba, hashA := handOffSweep(t, 0)

			store.stale = nil
			delete(store.getResp, hashA)

			if tc.state != nil {
				store.getResp[hashA] = tc.state
			}

			if tc.readErr != nil {
				store.getErrs = map[chainhash.Hash]error{hashA: tc.readErr}
			}

			s.handOffs = []handOff{{hash: hashA, stage: stageAnnounce, validatedHeight: 99}}

			recorder := &sweepLogRecorder{}
			s.logger = recorder

			s.sweepCreatingTxs(context.Background(), 100)

			require.Zero(t, ba.calls, "nothing is announced for a record that this entry no longer describes")
			require.Empty(t, store.unlocked)

			if tc.wantKept {
				require.Len(t, s.handOffs, 1)
				require.True(t, recorder.contains(hashA.String()), "the unexpected state is logged")
			} else {
				require.Empty(t, s.handOffs)
			}
		})
	}
}

func TestSweepCreatingTxsRetryListIsBounded(t *testing.T) {
	ns, err := nullstore.NewNullStore()
	require.NoError(t, err)

	txs := []*bt.Tx{oneOutputTx(1), oneOutputTx(2), oneOutputTx(3)}
	store := &fakeSweepStore{NullStore: ns, getResp: map[chainhash.Hash]*meta.Data{}}

	for _, tx := range txs {
		hash := *tx.TxIDChainHash()
		store.stale = append(store.stale, hash)
		store.getResp[hash] = &meta.Data{Creating: true, Attempt: sweepToken, Tx: tx}
	}

	ba := &flakyBlockAssembly{Mock: blockassembly.NewMock(), failures: -1}

	s := newSweepServer(t, store)
	s.blockAssemblyClient = ba
	s.handOffLimit = 2

	recorder := &sweepLogRecorder{}
	s.logger = recorder
	before := testutil.ToFloat64(prunerCreatingSweepHandOffDropped)

	s.sweepCreatingTxs(context.Background(), 100)

	require.Len(t, s.handOffs, 2)
	require.Equal(t, store.stale[1], s.handOffs[0].hash, "the oldest entry is the one that goes")
	require.Equal(t, store.stale[2], s.handOffs[1].hash)
	require.Equal(t, before+1, testutil.ToFloat64(prunerCreatingSweepHandOffDropped))
	require.True(t, recorder.contains(store.stale[0].String()), "the dropped record is logged")
	require.Equal(t, 3, ba.calls, "once the announce has failed for good the rest of the pass does not try it again")
	require.Equal(t, float64(2), testutil.ToFloat64(prunerCreatingSweepHandOffPending))
}

// A missing block assembly client is a setting of this node and does not change while it runs, so
// a list would only hold records that nothing can complete. They stay locked for the reload.
func TestSweepCreatingTxsNoBlockAssemblyClientQueuesNothing(t *testing.T) {
	s, store, _, hashA := handOffSweep(t, 0)
	s.blockAssemblyClient = nil

	s.sweepCreatingTxs(context.Background(), 100)

	require.Equal(t, []chainhash.Hash{hashA}, store.finalized)
	require.Empty(t, s.handOffs)
	require.Empty(t, store.unlocked)
}

func TestSweepCreatingTxsRetryHonoursTheContext(t *testing.T) {
	s, store, ba, hashA := handOffSweep(t, -1)
	s.handOffDelay = time.Hour

	ctx, cancel := context.WithCancel(context.Background())
	ba.onStore = cancel

	done := make(chan struct{})

	go func() {
		s.sweepCreatingTxs(ctx, 100)
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(10 * time.Second):
		require.FailNow(t, "the retry slept through a cancelled context")
	}

	require.Equal(t, 1, ba.calls)
	require.Empty(t, store.unlocked)
	require.Len(t, s.handOffs, 1)
	require.Equal(t, hashA, s.handOffs[0].hash)
}
