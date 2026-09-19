package netsync

import (
	"context"
	"net/url"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	txmap "github.com/bsv-blockchain/go-tx-map"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/services/validator"
	utxostore "github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/bsv-blockchain/teranode/stores/utxo/meta"
	"github.com/bsv-blockchain/teranode/stores/utxo/sql"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/stretchr/testify/require"
)

// bareNotFoundStore is the real SQLite store with one difference on the spend
// path: when a spend fails because the spending transaction's parent record is
// gone, it hands the caller a bare ErrTxNotFound instead of the aggregate
// ErrUtxoError the SQL store wraps it in. Every other call, including the
// GetMeta the validator then makes and the DeleteComplete the legacy path
// compensates with, reaches the real store untouched.
//
// That is the store shape Validator.validateInternal's own ErrTxNotFound branch
// exists for: the comment above its SpenderCreatedByCaller gate says the SQL and
// Aerospike stores shadow the branch with the aggregate ErrUtxoError, and the
// gate is kept "so a store that returns a bare ErrTxNotFound gets the same
// answer". This wrapper is that store.
type bareNotFoundStore struct {
	utxostore.Store
}

func (s *bareNotFoundStore) SpendAndCreate(ctx context.Context, tx *bt.Tx, blockHeight uint32, opts ...utxostore.CreateOption) (*meta.Data, []*utxostore.Spend, error) {
	md, spends, err := s.Store.SpendAndCreate(ctx, tx, blockHeight, opts...)
	if err != nil && errors.Is(err, errors.ErrTxNotFound) {
		return md, spends, errors.NewTxNotFoundError("[bareNotFoundStore] parent of %s not found", tx.TxID())
	}

	return md, spends, err
}

// TestLegacyBlockRejectsReplayWhenStoreSurfacesBareTxNotFound drives a replay of
// a fully pruned chain through the production legacy block path
// (ValidateTransactionsLegacyMode: create every transaction, then validate each
// through the real validator with WithSpenderCreatedByCaller) over a store that
// answers a missing parent with a bare ErrTxNotFound.
//
// History: P (mined 100) -> C (mined 101, spends P:0) -> G (mined 102, spends
// C:0). The pruner removed P and C end to end, so no replay marker survives
// anywhere. A block at height 101 then carries C again. The create phase writes
// C's record (mined, unlocked, C:0 unspent even though G consumed it), and the
// spend phase cannot find P. The validator's ErrTxNotFound branch reads C's own
// metadata, finds it mined, unlocked and not conflicting, and would bless it on
// the strength of the record the create phase just wrote; the
// SpenderCreatedByCaller gate at Validator.go:1225 is the only thing that stops
// it, because the aggregate-error branch the SQL store normally takes is not
// entered for a bare ErrTxNotFound.
//
// The assertions are on the store: the ghost record must be gone and C:0 must
// not be spendable again. With the gate removed the validator blesses C, the
// block validates, the ghost stays, and C:0 can be spent a second time.
func TestLegacyBlockRejectsReplayWhenStoreSurfacesBareTxNotFound(t *testing.T) {
	initPrometheusMetrics()

	ctx := context.Background()
	logger := ulogger.TestLogger{}

	// Regtest params (CSVHeight 576, so height 101 takes the pre-CSV finality
	// branch that needs no blockchain client) with a checkpoint at 1000 so the
	// below-checkpoint outpoint-only path engages, as it does in production.
	tSettings, params := newOutpointOnlySettings(t, true, true, 1000)

	storeURL, err := url.Parse("sqlitememory:///legacy_bare_not_found_bless_gate")
	require.NoError(t, err)

	store, err := sql.New(ctx, logger, tSettings, storeURL)
	require.NoError(t, err)
	require.NoError(t, store.SetBlockHeight(102))
	require.NoError(t, store.SetMedianBlockTime(1700000000))

	script, err := bscript.NewP2PKHFromAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa")
	require.NoError(t, err)

	// An ordinary (non-coinbase) parent, so its output is spendable at once:
	// the funding outpoint it names never has to exist for a create that skips
	// extended inputs.
	parent := bt.NewTx()
	fundingHash := chainhash.HashH([]byte("funding for the pruned parent"))
	fundingInput := &bt.Input{PreviousTxOutIndex: 0, SequenceNumber: 0xffffffff, UnlockingScript: bscript.NewFromBytes([]byte{0x00})}
	require.NoError(t, fundingInput.PreviousTxIDAdd(&fundingHash))
	parent.Inputs = append(parent.Inputs, fundingInput)
	parent.Outputs = append(parent.Outputs, &bt.Output{Satoshis: 500, LockingScript: script})
	_, err = store.Create(ctx, parent, 100, utxostore.WithSkipExtendedInputs(true),
		utxostore.WithMinedBlockInfo(utxostore.MinedBlockInfo{BlockID: 100, BlockHeight: 100}))
	require.NoError(t, err)

	child := spendOutput(t, parent, 0, 400, script)
	mineLikeLegacy(t, ctx, store, child, 101)

	grandchild := spendOutput(t, child, 0, 300, script)
	mineLikeLegacy(t, ctx, store, grandchild, 102)

	// The pruner removed the chain end to end: P and C are gone, with them the
	// only place a replay marker for C could live.
	require.NoError(t, store.DeleteComplete(ctx, parent.TxIDChainHash()))
	require.NoError(t, store.DeleteComplete(ctx, child.TxIDChainHash()))

	wrapped := &bareNotFoundStore{Store: store}

	v, err := validator.New(ctx, logger, tSettings, wrapped, nil, nil, nil, nil, nil)
	require.NoError(t, err)

	sm := &SyncManager{
		ctx:              ctx,
		settings:         tSettings,
		chainParams:      params,
		logger:           logger,
		utxoStore:        wrapped,
		validationClient: v,
	}

	// A block at height 101 carrying C again. createTxMap never puts the
	// coinbase in the map, so the map holds exactly the replayed child.
	txMap := txmap.NewSyncedMap[chainhash.Hash, *TxMapWrapper]()
	txMap.Set(*child.TxIDChainHash(), &TxMapWrapper{Tx: child})

	bi := blockIdent{
		hash:      chainhash.HashH([]byte("replayed block 101")),
		prevBlock: chainhash.HashH([]byte("block 100")),
		height:    101,
		timestamp: time.Unix(1700000000, 0),
		// The quick-validation path runs only for a block whose ancestry a
		// header run proved, which is the path under test.
		origin: blockRequestOrigin{headerProven: true},
	}

	blockErr := sm.ValidateTransactionsLegacyMode(ctx, txMap, bi, 101)

	// End state in the store, first: the record the create phase wrote for C is
	// a ghost (mined, unlocked, C:0 unspent while G already consumed it) and
	// must have been removed by the compensating delete.
	_, getErr := store.Get(ctx, child.TxIDChainHash())
	require.ErrorIs(t, getErr, errors.ErrTxNotFound,
		"the replayed child must not remain in the store after the block is rejected: it is a caller-created record blessed by nothing but itself")

	// C:0 was consumed by G in history; a fresh spender must be refused.
	respend := spendOutput(t, child, 0, 200, script)
	_, _, spendErr := store.SpendAndCreate(ctx, respend, 103, utxostore.WithSpendOnly(),
		utxostore.WithSkipUTXOHashCheck(true), utxostore.WithIgnoreLocked(true))
	require.Error(t, spendErr, "C:0 was consumed by a mined grandchild and must not be spendable again")
	require.ErrorIs(t, spendErr, errors.ErrTxNotFound)

	// And the block itself was rejected on the missing parent.
	require.Error(t, blockErr, "a replay of a fully pruned chain must not validate")
	require.ErrorIs(t, blockErr, errors.ErrTxNotFound)
}

// spendOutput builds a transaction with one input spending parent:vout and one
// output, un-extended, the way the outpoint-only legacy path sees transactions.
func spendOutput(t *testing.T, parent *bt.Tx, vout uint32, satoshis uint64, script *bscript.Script) *bt.Tx {
	t.Helper()

	tx := bt.NewTx()
	input := &bt.Input{PreviousTxOutIndex: vout, SequenceNumber: 0xffffffff, UnlockingScript: bscript.NewFromBytes([]byte{0x00})}
	require.NoError(t, input.PreviousTxIDAdd(parent.TxIDChainHash()))
	tx.Inputs = append(tx.Inputs, input)
	tx.Outputs = append(tx.Outputs, &bt.Output{Satoshis: satoshis, LockingScript: script})

	return tx
}

// mineLikeLegacy records tx as the legacy path does: created with mined info
// and no lock, then its inputs spent by outpoint.
func mineLikeLegacy(t *testing.T, ctx context.Context, store utxostore.Store, tx *bt.Tx, height uint32) {
	t.Helper()

	_, _, err := store.SpendAndCreate(ctx, tx, height, utxostore.WithCreateOnly(),
		utxostore.WithMinedBlockInfo(utxostore.MinedBlockInfo{BlockID: height, BlockHeight: height}),
		utxostore.WithSkipExtendedInputs(true))
	require.NoError(t, err)

	_, _, err = store.SpendAndCreate(ctx, tx, height, utxostore.WithSpendOnly(),
		utxostore.WithSkipUTXOHashCheck(true), utxostore.WithIgnoreLocked(true))
	require.NoError(t, err)
}

// TestLegacyLeftoverOfFullyPrunedChainIsNotBlessedOnRetry: attempt 1 at a block
// replaying a fully pruned chain ran createUtxos (which now creates locked) and
// died before pre-validation. Attempt 2 finds the record, answers ErrTxExists,
// and must treat it as its own earlier write, not as a pre-existing child of a
// pruned parent: rejected on the missing parent and removed.
func TestLegacyLeftoverOfFullyPrunedChainIsNotBlessedOnRetry(t *testing.T) {
	initPrometheusMetrics()

	ctx := context.Background()
	logger := ulogger.TestLogger{}
	tSettings, params := newOutpointOnlySettings(t, true, true, 1000)

	storeURL, err := url.Parse("sqlitememory:///legacy_leftover_not_blessed")
	require.NoError(t, err)

	store, err := sql.New(ctx, logger, tSettings, storeURL)
	require.NoError(t, err)
	require.NoError(t, store.SetBlockHeight(102))
	require.NoError(t, store.SetMedianBlockTime(1700000000))

	script, err := bscript.NewP2PKHFromAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa")
	require.NoError(t, err)

	parent := bt.NewTx()
	fundingHash := chainhash.HashH([]byte("funding for the pruned parent, leftover case"))
	fundingInput := &bt.Input{PreviousTxOutIndex: 0, SequenceNumber: 0xffffffff, UnlockingScript: bscript.NewFromBytes([]byte{0x00})}
	require.NoError(t, fundingInput.PreviousTxIDAdd(&fundingHash))
	parent.Inputs = append(parent.Inputs, fundingInput)
	parent.Outputs = append(parent.Outputs, &bt.Output{Satoshis: 500, LockingScript: script})
	_, err = store.Create(ctx, parent, 100, utxostore.WithSkipExtendedInputs(true),
		utxostore.WithMinedBlockInfo(utxostore.MinedBlockInfo{BlockID: 100, BlockHeight: 100}))
	require.NoError(t, err)

	child := spendOutput(t, parent, 0, 400, script)
	mineLikeLegacy(t, ctx, store, child, 101)

	grandchild := spendOutput(t, child, 0, 300, script)
	mineLikeLegacy(t, ctx, store, grandchild, 102)

	require.NoError(t, store.DeleteComplete(ctx, parent.TxIDChainHash()))
	require.NoError(t, store.DeleteComplete(ctx, child.TxIDChainHash()))

	v, err := validator.New(ctx, logger, tSettings, store, nil, nil, nil, nil, nil)
	require.NoError(t, err)

	sm := &SyncManager{ctx: ctx, settings: tSettings, chainParams: params, logger: logger, utxoStore: store, validationClient: v}

	txMap := txmap.NewSyncedMap[chainhash.Hash, *TxMapWrapper]()
	txMap.Set(*child.TxIDChainHash(), &TxMapWrapper{Tx: child})

	bi := blockIdent{hash: chainhash.HashH([]byte("replayed block 101, leftover")), prevBlock: chainhash.HashH([]byte("block 100")), height: 101, timestamp: time.Unix(1700000000, 0), origin: blockRequestOrigin{headerProven: true}}

	// Attempt 1: the create phase only, then the process dies.
	created, err := sm.createUtxos(ctx, txMap, bi, 101, true)
	require.NoError(t, err)
	require.Len(t, created, 1)

	leftover, err := store.Get(ctx, child.TxIDChainHash(), fields.Locked)
	require.NoError(t, err)
	require.True(t, leftover.Locked, "precondition: the legacy create phase leaves the record locked until the block commits")

	// Attempt 2.
	blockErr := sm.ValidateTransactionsLegacyMode(ctx, txMap, bi, 101)
	require.Error(t, blockErr, "the retry must not validate a replay of a fully pruned chain")
	require.ErrorIs(t, blockErr, errors.ErrTxNotFound)

	_, getErr := store.Get(ctx, child.TxIDChainHash())
	require.ErrorIs(t, getErr, errors.ErrTxNotFound, "the leftover must be removed, not blessed")

	respend := spendOutput(t, child, 0, 200, script)
	_, _, spendErr := store.SpendAndCreate(ctx, respend, 103, utxostore.WithSpendOnly(), utxostore.WithSkipUTXOHashCheck(true), utxostore.WithIgnoreLocked(true))
	require.Error(t, spendErr, "C:0 was consumed by a mined grandchild and must not be spendable again")
}

// TestLegacyRemovesRecreatedDescendantsOfPrunedReplay is the legacy twin of the
// quick path's TestQuickValidateRemovesRecreatedDescendantsOfPrunedReplay. A
// reviewer found the descendant defect on the quick path only; both paths
// create every transaction of a block first and spend afterwards, and both hand
// the same list to utxo.PrunedReplayGhosts, so the scenario has to be pinned
// here too.
//
// History: P (two outputs, only the first ever spent, so P survives pruning and
// keeps the marker) -> C -> D -> E. The pruner removes C and D. A block then
// replays C and D. C is rejected on P's marker. D is not rejected at all,
// because it spends the C this very attempt recreated, so only the dependency
// walk can catch it, and E already consumed D:0.
func TestLegacyRemovesRecreatedDescendantsOfPrunedReplay(t *testing.T) {
	initPrometheusMetrics()

	ctx := context.Background()
	logger := ulogger.TestLogger{}
	tSettings, params := newOutpointOnlySettings(t, true, true, 1000)

	storeURL, err := url.Parse("sqlitememory:///legacy_pruned_descendant")
	require.NoError(t, err)

	store, err := sql.New(ctx, logger, tSettings, storeURL)
	require.NoError(t, err)
	require.NoError(t, store.SetBlockHeight(103))
	require.NoError(t, store.SetMedianBlockTime(1700000000))

	sql.ResetPrunerServiceForTests()
	t.Cleanup(sql.ResetPrunerServiceForTests)

	script, err := bscript.NewP2PKHFromAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa")
	require.NoError(t, err)

	onLongestChain := func(height uint32, txs ...*bt.Tx) {
		t.Helper()

		hashes := make([]*chainhash.Hash, 0, len(txs))
		for _, tx := range txs {
			hashes = append(hashes, tx.TxIDChainHash())
		}

		_, minedErr := store.SetMinedMulti(ctx, hashes, utxostore.MinedBlockInfo{BlockID: height, BlockHeight: height, OnLongestChain: true})
		require.NoError(t, minedErr)
	}

	parent := bt.NewTx()
	fundingHash := chainhash.HashH([]byte("funding for the pruned descendant case"))
	fundingInput := &bt.Input{PreviousTxOutIndex: 0, SequenceNumber: 0xffffffff, UnlockingScript: bscript.NewFromBytes([]byte{0x00})}
	require.NoError(t, fundingInput.PreviousTxIDAdd(&fundingHash))
	parent.Inputs = append(parent.Inputs, fundingInput)
	parent.Outputs = append(parent.Outputs,
		&bt.Output{Satoshis: 500, LockingScript: script},
		&bt.Output{Satoshis: 500, LockingScript: script})
	_, err = store.Create(ctx, parent, 100, utxostore.WithSkipExtendedInputs(true),
		utxostore.WithMinedBlockInfo(utxostore.MinedBlockInfo{BlockID: 100, BlockHeight: 100}))
	require.NoError(t, err)

	child := spendOutput(t, parent, 0, 400, script)
	mineLikeLegacy(t, ctx, store, child, 101)

	dependent := spendOutput(t, child, 0, 300, script)
	mineLikeLegacy(t, ctx, store, dependent, 102)

	last := spendOutput(t, dependent, 0, 200, script)
	mineLikeLegacy(t, ctx, store, last, 103)

	onLongestChain(100, parent)
	onLongestChain(101, child)
	onLongestChain(102, dependent)
	onLongestChain(103, last)

	prunerService, err := store.GetPrunerService()
	require.NoError(t, err)

	pruned, err := prunerService.Prune(ctx, 1300, "legacy-pruned-descendant")
	require.NoError(t, err)
	require.Equal(t, int64(2), pruned, "fixture: the pruner removes C and D and leaves P holding C's marker")

	v, err := validator.New(ctx, logger, tSettings, store, nil, nil, nil, nil, nil)
	require.NoError(t, err)

	sm := &SyncManager{ctx: ctx, settings: tSettings, chainParams: params, logger: logger, utxoStore: store, validationClient: v}

	txMap := txmap.NewSyncedMap[chainhash.Hash, *TxMapWrapper]()
	txMap.Set(*child.TxIDChainHash(), &TxMapWrapper{Tx: child})
	txMap.Set(*dependent.TxIDChainHash(), &TxMapWrapper{Tx: dependent})

	bi := blockIdent{hash: chainhash.HashH([]byte("replayed block 101, descendant")), prevBlock: chainhash.HashH([]byte("block 100")), height: 101, timestamp: time.Unix(1700000000, 0), origin: blockRequestOrigin{headerProven: true}}

	blockErr := sm.ValidateTransactionsLegacyMode(ctx, txMap, bi, 101)
	require.Error(t, blockErr, "a block replaying a pruned transaction must not validate")

	for name, hash := range map[string]*chainhash.Hash{
		"the rejected replay":     child.TxIDChainHash(),
		"its recreated dependent": dependent.TxIDChainHash(),
	} {
		_, getErr := store.Get(ctx, hash)
		require.ErrorIsf(t, getErr, errors.ErrTxNotFound, "%s must not survive the rejected block", name)
	}

	respend := spendOutput(t, dependent, 0, 100, script)
	_, _, spendErr := store.SpendAndCreate(ctx, respend, 104, utxostore.WithSpendOnly(),
		utxostore.WithSkipUTXOHashCheck(true), utxostore.WithIgnoreLocked(true))
	require.Error(t, spendErr, "D:0 was consumed by a mined transaction and must not be spendable again")
}

// retryableReplayStore is the real store with one difference: a spend it
// rejects as a pruned replay comes back wrapped in a transient storage error,
// the shape the aggregate takes when a sibling input of the same transaction
// hit DEVICE_OVERLOAD.
type retryableReplayStore struct {
	utxostore.Store
}

func (s *retryableReplayStore) SpendAndCreate(ctx context.Context, tx *bt.Tx, blockHeight uint32, opts ...utxostore.CreateOption) (*meta.Data, []*utxostore.Spend, error) {
	md, spends, err := s.Store.SpendAndCreate(ctx, tx, blockHeight, opts...)
	if err != nil && errors.Is(err, errors.ErrUtxoSpendingTxPruned) {
		return md, spends, errors.NewStorageError("[retryableReplayStore] injected overload on a sibling input", err)
	}

	return md, spends, err
}

// TestLegacyRecordsPrunedReplayDespiteRetryableSibling: PreValidateTransactions
// tested IsRetryableError before the pruned-replay classification, and the
// store's aggregate is retryable as soon as any input carries a storage error.
// A replay whose sibling input kept failing transiently was never recorded as a
// replay, and its recreated record stayed once the retries gave up. A pruned
// replay is deterministic, so it is classified first. Reproduced by review.
func TestLegacyRecordsPrunedReplayDespiteRetryableSibling(t *testing.T) {
	initPrometheusMetrics()

	ctx := context.Background()
	logger := ulogger.TestLogger{}
	tSettings, params := newOutpointOnlySettings(t, true, true, 1000)
	tSettings.Pruner.UTXODefensiveEnabled = false

	storeURL, err := url.Parse("sqlitememory:///legacy_retryable_pruned_replay")
	require.NoError(t, err)

	store, err := sql.New(ctx, logger, tSettings, storeURL)
	require.NoError(t, err)

	sql.ResetPrunerServiceForTests()
	t.Cleanup(sql.ResetPrunerServiceForTests)

	require.NoError(t, store.SetBlockHeight(102))
	require.NoError(t, store.SetMedianBlockTime(1700000000))

	script, err := bscript.NewP2PKHFromAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa")
	require.NoError(t, err)

	// P keeps output 1 unspent, so it survives the prune and carries C's marker.
	parent := bt.NewTx()
	fundingHash := chainhash.HashH([]byte("funding for the marked parent"))
	fundingInput := &bt.Input{PreviousTxOutIndex: 0, SequenceNumber: 0xffffffff, UnlockingScript: bscript.NewFromBytes([]byte{0x00})}
	require.NoError(t, fundingInput.PreviousTxIDAdd(&fundingHash))
	parent.Inputs = append(parent.Inputs, fundingInput)
	parent.Outputs = append(parent.Outputs, &bt.Output{Satoshis: 500, LockingScript: script}, &bt.Output{Satoshis: 500, LockingScript: script})
	_, err = store.Create(ctx, parent, 100, utxostore.WithSkipExtendedInputs(true),
		utxostore.WithMinedBlockInfo(utxostore.MinedBlockInfo{BlockID: 100, BlockHeight: 100}))
	require.NoError(t, err)

	child := spendOutput(t, parent, 0, 400, script)
	mineLikeLegacy(t, ctx, store, child, 101)

	grandchild := spendOutput(t, child, 0, 300, script)
	mineLikeLegacy(t, ctx, store, grandchild, 102)

	svc, err := store.GetPrunerService()
	require.NoError(t, err)
	_, err = svc.Prune(ctx, 102+tSettings.GetUtxoStoreBlockHeightRetention()+10, "prune-child")
	require.NoError(t, err)

	_, err = store.Get(ctx, child.TxIDChainHash())
	require.ErrorIs(t, err, errors.ErrTxNotFound, "fixture: C is pruned and P carries its marker")

	wrapped := &retryableReplayStore{Store: store}

	v, err := validator.New(ctx, logger, tSettings, wrapped, nil, nil, nil, nil, nil)
	require.NoError(t, err)

	sm := &SyncManager{ctx: ctx, settings: tSettings, chainParams: params, logger: logger, utxoStore: wrapped, validationClient: v}

	txMap := txmap.NewSyncedMap[chainhash.Hash, *TxMapWrapper]()
	txMap.Set(*child.TxIDChainHash(), &TxMapWrapper{Tx: child})

	bi := blockIdent{hash: chainhash.HashH([]byte("replayed block 101, retryable sibling")), prevBlock: chainhash.HashH([]byte("block 100")), height: 101, timestamp: time.Unix(1700000000, 0), origin: blockRequestOrigin{headerProven: true}}

	blockErr := sm.ValidateTransactionsLegacyMode(ctx, txMap, bi, 101)
	require.Error(t, blockErr, "a block replaying a pruned transaction must not validate")
	require.ErrorIs(t, blockErr, errors.ErrUtxoSpendingTxPruned)

	_, getErr := store.Get(ctx, child.TxIDChainHash())
	require.ErrorIs(t, getErr, errors.ErrTxNotFound, "the recreated replay must be removed even when its error also reads as retryable")
}

// TestUnlockBlockTransactionsReleasesTheCreatePhaseLock pins the post-commit
// half of the legacy lock: records created locked by createUtxos are unlocked
// once the block is committed, in chunks, with the coinbase skipped.
func TestUnlockBlockTransactionsReleasesTheCreatePhaseLock(t *testing.T) {
	ctx := context.Background()
	logger := ulogger.TestLogger{}
	tSettings, params := newOutpointOnlySettings(t, true, true, 1000)
	tSettings.UtxoStore.MaxMinedBatchSize = 2

	storeURL, err := url.Parse("sqlitememory:///legacy_unlock_pass")
	require.NoError(t, err)

	store, err := sql.New(ctx, logger, tSettings, storeURL)
	require.NoError(t, err)
	require.NoError(t, store.SetBlockHeight(100))

	sm := &SyncManager{ctx: ctx, settings: tSettings, chainParams: params, logger: logger, utxoStore: store}

	txMap := txmap.NewSyncedMap[chainhash.Hash, *TxMapWrapper]()
	hashes := []chainhash.Hash{chainhash.HashH([]byte("coinbase placeholder"))}

	for i := 0; i < 5; i++ {
		tx := bt.NewTx()
		tx.Version = 1
		require.NoError(t, tx.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", uint64(1000+i)))
		txMap.Set(*tx.TxIDChainHash(), &TxMapWrapper{Tx: tx})
		hashes = append(hashes, *tx.TxIDChainHash())
	}

	bi := blockIdent{hash: chainhash.HashH([]byte("block 100")), height: 100, origin: blockRequestOrigin{headerProven: true}}
	created, err := sm.createUtxos(ctx, txMap, bi, 100, true)
	require.NoError(t, err)
	require.Len(t, created, 5)

	for _, hash := range hashes[1:] {
		hash := hash
		meta, getErr := store.Get(ctx, &hash, fields.Locked)
		require.NoError(t, getErr)
		require.True(t, meta.Locked, "precondition: created locked")
	}

	require.NoError(t, sm.unlockBlockTransactions(ctx, blockRequestOrigin{headerProven: true}, 100, hashes))

	for _, hash := range hashes[1:] {
		hash := hash
		meta, getErr := store.Get(ctx, &hash, fields.Locked)
		require.NoError(t, getErr)
		require.False(t, meta.Locked, "end state: unlocked once the block is committed")
	}
}
