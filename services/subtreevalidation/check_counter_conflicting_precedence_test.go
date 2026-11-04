package subtreevalidation

import (
	"context"
	"fmt"
	"slices"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/services/validator"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/bsv-blockchain/teranode/stores/utxo/meta"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/require"
)

// faultyGetStore fails the batch's reads of one hash with a storage error: its
// phase-1 read, the one asking for its inpoints, and its read as a member of the
// walk. The cached path reads the tx's body first (checkStoredTxAtHeight), and
// that read is left alone, so the fault reaches the batch.
type faultyGetStore struct {
	utxo.Store
	fail chainhash.Hash
}

func (f *faultyGetStore) Get(ctx context.Context, hash *chainhash.Hash, fs ...fields.FieldName) (*meta.Data, error) {
	if hash.Equal(f.fail) && !slices.Contains(fs, fields.Tx) {
		return nil, errors.NewStorageError("store down")
	}

	return f.Store.Get(ctx, hash, fs...)
}

// buildCone stores a chain of n spenders below root, each spending output 0 of
// the one before, and returns it top first. The first is created with firstOpts.
func buildCone(t *testing.T, ctx context.Context, store utxo.Store, root *bt.Tx, n int, firstOpts ...utxo.CreateOption) []chainhash.Hash {
	t.Helper()

	cone := make([]chainhash.Hash, 0, n)

	prev := root
	for i := 0; i < n; i++ {
		child := spendingChild(t, prev, 0)

		if i == 0 {
			spendAndCreate(t, ctx, store, child, firstOpts...)
		} else {
			spendAndCreate(t, ctx, store, child)
		}

		cone = append(cone, *child.TxIDChainHash())
		prev = child
	}

	return cone
}

// A walk that stops on its budget still has the counter cone it reached
// mined-checked: every member of it descends from a counter-spender, so a mined
// one is proof, whichever of the walk's two passes ran out. The walk runs the
// counter-spenders' cone first and the conflicting txs' own cone second, on one
// budget.
func Test_checkCounterConflictingOnCurrentChainBatch_overBudgetWalkChecksTheCounterConeItReached(t *testing.T) {
	ctx := context.Background()
	minedInfo := utxo.MinedBlockInfo{BlockID: 77, BlockHeight: 100}
	blockIds := map[uint32]bool{77: true}

	// requireProven checks the walk's premise, then that the batch names the
	// mined counter-cone member.
	requireProven := func(t *testing.T, store utxo.Store, tSettings *settings.Settings, counter *bt.Tx, txs []chainhash.Hash, cone []chainhash.Hash, minedMember chainhash.Hash, counterConeComplete bool) {
		t.Helper()

		cones, _, walkErr := utxo.GetConflictingChildrenFromRoots(ctx, store, [][]chainhash.Hash{{*counter.TxIDChainHash()}, txs}, tSettings.UtxoStore.ConflictingChildrenMaxNodes)
		require.ErrorIs(t, walkErr, errors.ErrUtxoWalkLimitExceeded)
		require.Contains(t, cones[0], minedMember, "the walk reached the mined member")

		if counterConeComplete {
			require.ElementsMatch(t, cone, cones[0], "the counter cone was walked in full")
		} else {
			require.Less(t, len(cones[0]), len(cone), "the counter cone was cut short")
		}

		s := &Server{logger: ulogger.NewErrorTestLogger(t), settings: tSettings, utxoStore: store}

		_, err := s.checkCounterConflictingOnCurrentChainBatch(ctx, txs, blockIds, failOnMissingRecords)
		require.ErrorIs(t, err, errors.ErrTxInvalid)
		require.ErrorContains(t, err, minedMember.String(), "the mined counter-cone member is named")
	}

	t.Run("own cone overflows after a complete counter cone", func(t *testing.T) {
		tSettings := test.CreateBaseTestSettings(t)
		tSettings.UtxoStore.SpendBatcherDurationMillis = 1
		// two roots (tx1 and its counter-spender): a budget of 8. The counter
		// cone, the counter-spender and 3 below it, fits; tx1's own cone of 12
		// does not.
		tSettings.UtxoStore.ConflictingChildrenMaxNodes = 4

		store := newSQLUtxoStore(t, ctx, tSettings)
		counter, chain := buildConflictingChain(t, ctx, store, 12, false)
		cone := buildCone(t, ctx, store, counter, 3)

		// the third member: neither a root nor a direct counter-spender
		_, err := store.SetMinedMulti(ctx, []*chainhash.Hash{&cone[2]}, minedInfo)
		require.NoError(t, err)

		requireProven(t, store, tSettings, counter, chain[:1], cone, cone[2], true)
	})

	t.Run("counter cone overflows after reaching a mined member", func(t *testing.T) {
		tSettings := test.CreateBaseTestSettings(t)
		tSettings.UtxoStore.SpendBatcherDurationMillis = 1
		// two roots: a budget of 4, against a counter cone of 10
		tSettings.UtxoStore.ConflictingChildrenMaxNodes = 2

		store := newSQLUtxoStore(t, ctx, tSettings)
		counter, chain := buildConflictingChain(t, ctx, store, 1, false)
		cone := buildCone(t, ctx, store, counter, 10)

		_, err := store.SetMinedMulti(ctx, []*chainhash.Hash{&cone[1]}, minedInfo)
		require.NoError(t, err)

		requireProven(t, store, tSettings, counter, chain, cone, cone[1], false)
	})
}

// A tx-invalid verdict proven from records that were read is final: the block
// is rejected whatever else in the batch could not be checked. A processing
// error is returned only when nothing is proven. Otherwise one tx whose parent
// was pruned for good, or one runaway cone, makes a block that also carries a
// provably invalid tx retry forever instead of being rejected.
func Test_checkCounterConflictingOnCurrentChainBatch_provenTxInvalidWins(t *testing.T) {
	ctx := context.Background()
	mined := utxo.WithMinedBlockInfo(utxo.MinedBlockInfo{BlockID: 77, BlockHeight: 100})
	blockIds := map[uint32]bool{77: true}

	// requireRejected checks the cached path end to end: the verdict is
	// tx-invalid through its wrapping.
	requireRejected := func(t *testing.T, store utxo.Store, tSettings *settings.Settings, conflictingTxs []chainhash.Hash) {
		t.Helper()

		s, block, cachedSubtrees := newCachedCheckServer(t, store, tSettings, conflictingTxs, []uint32{77})

		err := s.checkCachedSubtreesAgainstCandidateChain(ctx, block, cachedSubtrees)
		require.ErrorIs(t, err, errors.ErrTxInvalid)
	}

	t.Run("fresh path: a pruned parent does not hide a mined counter-spender", func(t *testing.T) {
		tSettings := test.CreateBaseTestSettings(t)

		store := newSQLUtxoStore(t, ctx, tSettings)
		_, chain := buildConflictingChain(t, ctx, store, 1, false, mined)

		// a conflicting tx whose parent the store no longer holds: on its own,
		// the fresh path's verdict is a retryable TX_NOT_FOUND
		orphanParent := buildTx(t, []outpoint{{tx: parentTx1, vout: 0}}, 1, 9001)
		_, _, err := store.SpendAndCreate(ctx, orphanParent, 122, utxo.WithCreateOnly())
		require.NoError(t, err)

		orphan := spendingChild(t, orphanParent, 0)
		createConflicting(t, ctx, store, orphan)
		require.NoError(t, store.Delete(ctx, orphanParent.TxIDChainHash()))

		s := &Server{logger: ulogger.NewErrorTestLogger(t), settings: tSettings, utxoStore: store}

		alone, err := s.checkCounterConflictingOnCurrentChainBatch(ctx, []chainhash.Hash{*orphan.TxIDChainHash()}, blockIds, failOnMissingRecords)
		require.Nil(t, alone)
		require.ErrorIs(t, err, errors.ErrTxNotFound, "alone, the orphan is a retryable failure")

		txHashes := []chainhash.Hash{*orphan.TxIDChainHash(), chain[0]}

		_, err = s.processTxMetaUsingStore(ctx, txHashes, make([]metaSliceItem, len(txHashes)), blockIds, true, false, true)
		require.ErrorIs(t, err, errors.ErrTxInvalid)
	})

	t.Run("cached path: a store fault on one tx does not hide a mined counter-spender", func(t *testing.T) {
		tSettings := test.CreateBaseTestSettings(t)
		tSettings.UtxoStore.SpendBatcherDurationMillis = 1

		store := newSQLUtxoStore(t, ctx, tSettings)
		faultedTx := buildIndependentConflicts(t, ctx, store, 1)[0]
		_, chain := buildConflictingChain(t, ctx, store, 1, false, mined)

		faulty := &faultyGetStore{Store: store, fail: faultedTx}

		s, block, cachedSubtrees := newCachedCheckServer(t, faulty, tSettings, []chainhash.Hash{faultedTx}, []uint32{77})
		err := s.checkCachedSubtreesAgainstCandidateChain(ctx, block, cachedSubtrees)
		require.ErrorIs(t, err, errors.ErrStorageError, "alone, the faulted tx is a retryable failure")
		require.NotErrorIs(t, err, errors.ErrTxInvalid)

		requireRejected(t, faulty, tSettings, []chainhash.Hash{faultedTx, chain[0]})
	})

	t.Run("a walk over its budget does not hide a mined direct counter-spender", func(t *testing.T) {
		const coneLength = 10

		tSettings := test.CreateBaseTestSettings(t)
		tSettings.UtxoStore.SpendBatcherDurationMillis = 1
		// four roots (two conflicting txs, two counter-spenders): a budget of 8,
		// against one counter-spender cone of 10
		tSettings.UtxoStore.ConflictingChildrenMaxNodes = 2

		store := newSQLUtxoStore(t, ctx, tSettings)
		counter, chain := buildConflictingChain(t, ctx, store, 1, false)

		prev := counter
		for i := 0; i < coneLength; i++ {
			child := spendingChild(t, prev, 0)
			spendAndCreate(t, ctx, store, child)
			prev = child
		}

		minedConflict := buildIndependentConflicts(t, ctx, store, 1, mined)[0]

		s, block, cachedSubtrees := newCachedCheckServer(t, store, tSettings, chain, []uint32{77})
		require.ErrorIs(t, s.checkCachedSubtreesAgainstCandidateChain(ctx, block, cachedSubtrees), errors.ErrUtxoWalkLimitExceeded,
			"alone, the oversized cone fails closed")

		requireRejected(t, store, tSettings, []chainhash.Hash{chain[0], minedConflict})
	})

	t.Run("a mined member the walk reached before a store fault stopped it is proof", func(t *testing.T) {
		const coneLength = 10

		tSettings := test.CreateBaseTestSettings(t)
		tSettings.UtxoStore.SpendBatcherDurationMillis = 1

		store := newSQLUtxoStore(t, ctx, tSettings)
		counter, chain := buildConflictingChain(t, ctx, store, 1, false)

		// a counter-spender cone whose first member is mined, and whose fourth
		// cannot be read: the walk stops there
		cone := buildCone(t, ctx, store, counter, coneLength, mined)
		faulty := &faultyGetStore{Store: store, fail: cone[3]}

		requireRejected(t, faulty, tSettings, chain)
	})

	t.Run("a frozen counter-spender does not hide a mined counter-spender", func(t *testing.T) {
		tSettings := test.CreateBaseTestSettings(t)
		tSettings.UtxoStore.SpendBatcherDurationMillis = 1

		store := newSQLUtxoStore(t, ctx, tSettings)

		_, _, err := store.SpendAndCreate(ctx, parentTx1, 122, utxo.WithCreateOnly())
		require.NoError(t, err)

		freezeOutput(t, ctx, store, parentTx1, 2, tSettings)
		createConflicting(t, ctx, store, tx1)

		minedConflict := buildIndependentConflicts(t, ctx, store, 1, mined)[0]

		requireRejected(t, store, tSettings, []chainhash.Hash{*tx1.TxIDChainHash(), minedConflict})
	})

	t.Run("a frozen own cone does not hide a mined counter-spender", func(t *testing.T) {
		tSettings := test.CreateBaseTestSettings(t)
		tSettings.UtxoStore.SpendBatcherDurationMillis = 1

		store := newSQLUtxoStore(t, ctx, tSettings)
		_, chain := buildConflictingChain(t, ctx, store, 1, false)

		tip, err := store.Get(ctx, &chain[0], fields.Tx)
		require.NoError(t, err)
		freezeOutput(t, ctx, store, tip.Tx, 0, tSettings)

		minedConflict := buildIndependentConflicts(t, ctx, store, 1, mined)[0]

		requireRejected(t, store, tSettings, []chainhash.Hash{chain[0], minedConflict})
	})
}

// On the fresh path an entry left set is never decorated again, so it must have
// passed the conflict check. When the check ran and passed, the collected
// entries have, and clearing them would only make every fail-fast retry re-check
// the whole batch: they stay set, and the batches' error is still returned.
func Test_processTxMetaUsingStore_passedConflictCheckKeepsEntries(t *testing.T) {
	ctx := context.Background()

	tSettings := test.CreateBaseTestSettings(t)
	// one tx per batch, run in order: the conflicting tx is collected first,
	// then the second missing tx trips the threshold
	tSettings.BlockValidation.ProcessTxMetaUsingStoreBatchSize = 1
	tSettings.BlockValidation.ProcessTxMetaUsingStoreConcurrency = 1
	tSettings.BlockValidation.ProcessTxMetaUsingStoreMissingTxThreshold = 1

	store := newSQLUtxoStore(t, ctx, tSettings)
	_, chain := buildConflictingChain(t, ctx, store, 1, false)

	s := &Server{logger: ulogger.NewErrorTestLogger(t), settings: tSettings, utxoStore: store}

	txHashes := []chainhash.Hash{chain[0], chainhash.HashH([]byte("missing-1")), chainhash.HashH([]byte("missing-2"))}
	txMetaSlice := make([]metaSliceItem, len(txHashes))

	_, err := s.processTxMetaUsingStore(ctx, txHashes, txMetaSlice, map[uint32]bool{77: true}, true, true, true)
	require.ErrorIs(t, err, errors.ErrThresholdExceeded)
	require.NotErrorIs(t, err, errors.ErrContextCanceled, "ValidateSubtreeInternal retries the threshold by its own code")
	require.NotErrorIs(t, err, errors.ErrTxInvalid)

	require.True(t, txMetaSlice[0].isSet, "the conflicting tx passed the check, so the retry need not decorate and check it again")
	require.True(t, txMetaSlice[0].conflicting)
}

// One input that cannot be checked does not hide the spender of another input
// of the same tx: every readable input's counter-spender is collected, the
// failure is recorded, and a mined counter-spender wins. Otherwise a block whose
// one conflicting tx has a frozen or out-of-range input 0 and a mined
// counter-spender on input 1 retries forever.
func Test_checkCounterConflictingOnCurrentChainBatch_perInputCounterSpenders(t *testing.T) {
	ctx := context.Background()
	mined := utxo.WithMinedBlockInfo(utxo.MinedBlockInfo{BlockID: 77, BlockHeight: 100})
	blockIds := map[uint32]bool{77: true}

	// requireTxInvalid checks the batch under both missing-record policies: the
	// failed input is a processing error under either, never a missing record.
	requireTxInvalid := func(t *testing.T, s *Server, conflicting chainhash.Hash, counter chainhash.Hash) {
		t.Helper()

		for _, policy := range []missingRecordPolicy{skipMissingRecords, failOnMissingRecords} {
			notFound, err := s.checkCounterConflictingOnCurrentChainBatch(ctx, []chainhash.Hash{conflicting}, blockIds, policy)
			require.Nil(t, notFound)
			require.ErrorIs(t, err, errors.ErrTxInvalid, "policy %d", policy)
			require.ErrorContains(t, err, counter.String(), "the mined counter-spender on input 1 is named")
		}
	}

	t.Run("a frozen input 0 does not hide a mined counter-spender on input 1", func(t *testing.T) {
		tSettings := test.CreateBaseTestSettings(t)
		tSettings.UtxoStore.SpendBatcherDurationMillis = 1

		store := newSQLUtxoStore(t, ctx, tSettings)

		parent := buildTx(t, []outpoint{{tx: parentTx1, vout: 0}}, 2, 9100)
		_, _, err := store.SpendAndCreate(ctx, parent, 122, utxo.WithCreateOnly())
		require.NoError(t, err)

		freezeOutput(t, ctx, store, parent, 0, tSettings)

		counter := buildTx(t, []outpoint{{tx: parent, vout: 1}}, 1, 9101)
		spendAndCreate(t, ctx, store, counter, mined)

		conflicting := buildTx(t, []outpoint{{tx: parent, vout: 0}, {tx: parent, vout: 1}}, 1, 9102)
		createConflicting(t, ctx, store, conflicting)

		s := &Server{logger: ulogger.NewErrorTestLogger(t), settings: tSettings, utxoStore: store}

		// alone, input 0 is a processing error
		_, err = s.checkCounterConflictingOnCurrentChainBatch(ctx, []chainhash.Hash{*conflicting.TxIDChainHash()}, map[uint32]bool{}, failOnMissingRecords)
		require.ErrorContains(t, err, "counter conflicting tx is frozen")

		requireTxInvalid(t, s, *conflicting.TxIDChainHash(), *counter.TxIDChainHash())
	})

	t.Run("a pruned parent of input 0 does not hide a mined counter-spender on input 1", func(t *testing.T) {
		tSettings := test.CreateBaseTestSettings(t)
		tSettings.UtxoStore.SpendBatcherDurationMillis = 1

		store := newSQLUtxoStore(t, ctx, tSettings)

		pruned := buildTx(t, []outpoint{{tx: parentTx1, vout: 0}}, 1, 9150)
		_, _, err := store.SpendAndCreate(ctx, pruned, 122, utxo.WithCreateOnly())
		require.NoError(t, err)

		parent := buildTx(t, []outpoint{{tx: parentTx1, vout: 1}}, 1, 9151)
		_, _, err = store.SpendAndCreate(ctx, parent, 122, utxo.WithCreateOnly())
		require.NoError(t, err)

		counter := buildTx(t, []outpoint{{tx: parent, vout: 0}}, 1, 9152)
		spendAndCreate(t, ctx, store, counter, mined)

		conflicting := buildTx(t, []outpoint{{tx: pruned, vout: 0}, {tx: parent, vout: 0}}, 1, 9153)
		createConflicting(t, ctx, store, conflicting)
		require.NoError(t, store.Delete(ctx, pruned.TxIDChainHash()))

		s := &Server{logger: ulogger.NewErrorTestLogger(t), settings: tSettings, utxoStore: store}

		// alone, the pruned parent is a missing record: skipped on the cached
		// path, a retryable failure on the fresh path
		notFound, err := s.checkCounterConflictingOnCurrentChainBatch(ctx, []chainhash.Hash{*conflicting.TxIDChainHash()}, map[uint32]bool{}, skipMissingRecords)
		require.NoError(t, err)
		require.Equal(t, []chainhash.Hash{*conflicting.TxIDChainHash()}, notFound)

		_, err = s.checkCounterConflictingOnCurrentChainBatch(ctx, []chainhash.Hash{*conflicting.TxIDChainHash()}, map[uint32]bool{}, failOnMissingRecords)
		require.ErrorIs(t, err, errors.ErrTxNotFound)

		requireTxInvalid(t, s, *conflicting.TxIDChainHash(), *counter.TxIDChainHash())
	})

	t.Run("an out-of-range input 0 does not hide a mined counter-spender on input 1", func(t *testing.T) {
		tSettings := test.CreateBaseTestSettings(t)
		tSettings.UtxoStore.SpendBatcherDurationMillis = 1

		store := newSQLUtxoStore(t, ctx, tSettings)

		// short has one output, and input 0 spends its output 5
		short := buildTx(t, []outpoint{{tx: parentTx1, vout: 0}}, 1, 9200)
		_, _, err := store.SpendAndCreate(ctx, short, 122, utxo.WithCreateOnly())
		require.NoError(t, err)

		parent := buildTx(t, []outpoint{{tx: parentTx1, vout: 1}}, 1, 9201)
		_, _, err = store.SpendAndCreate(ctx, parent, 122, utxo.WithCreateOnly())
		require.NoError(t, err)

		counter := buildTx(t, []outpoint{{tx: parent, vout: 0}}, 1, 9202)
		spendAndCreate(t, ctx, store, counter, mined)

		conflicting := bt.NewTx()
		conflicting.LockTime = 9203
		require.NoError(t, conflicting.From(short.TxIDChainHash().String(), 5, short.Outputs[0].LockingScript.String(), short.Outputs[0].Satoshis))
		require.NoError(t, conflicting.From(parent.TxIDChainHash().String(), 0, parent.Outputs[0].LockingScript.String(), parent.Outputs[0].Satoshis))

		for _, in := range conflicting.Inputs {
			in.UnlockingScript = bscript.NewFromBytes([]byte{})
		}

		require.NoError(t, conflicting.AddP2PKHOutputFromScript(parent.Outputs[0].LockingScript, 1))
		createConflicting(t, ctx, store, conflicting)

		s := &Server{logger: ulogger.NewErrorTestLogger(t), settings: tSettings, utxoStore: store}

		// alone, input 0 is a processing error
		_, err = s.checkCounterConflictingOnCurrentChainBatch(ctx, []chainhash.Hash{*conflicting.TxIDChainHash()}, map[uint32]bool{}, failOnMissingRecords)
		require.ErrorContains(t, err, "out of range")

		requireTxInvalid(t, s, *conflicting.TxIDChainHash(), *counter.TxIDChainHash())
	})
}

// conflictingValidator accepts any tx and reports it stored as conflicting, as
// the validator does for a block's tx that lost a double spend.
type conflictingValidator struct {
	validator.Interface
}

func (conflictingValidator) ValidateWithOptions(context.Context, *bt.Tx, uint32, *validator.Options) (*meta.Data, error) {
	return &meta.Data{Conflicting: true}, nil
}

// blessMissingTransaction asks the batched check, so a tx it blesses gets the
// verdict a block's cached or fresh subtree gets for it: a member of the
// counter-conflicting set mined on the candidate chain rejects it even when
// another member of the set was pruned. The per-transaction check it used to run
// returned the pruned member's processing error before its mined check, so the
// same block was rejected or retried depending on whether the tx was already in
// the store.
func Test_blessMissingTransaction_provenTxInvalidWins(t *testing.T) {
	ctx := context.Background()
	blockIds := map[uint32]bool{77: true}

	tSettings := test.CreateBaseTestSettings(t)
	tSettings.UtxoStore.SpendBatcherDurationMillis = 1

	store := newSQLUtxoStore(t, ctx, tSettings)
	counter, _ := buildConflictingChain(t, ctx, store, 1, false)

	// the counter-spender's child is pruned: the walk reaches it and cannot read it
	pruned := spendingChild(t, counter, 0)
	spendAndCreate(t, ctx, store, pruned)
	require.NoError(t, store.Delete(ctx, pruned.TxIDChainHash()))

	s := &Server{
		logger:          ulogger.NewErrorTestLogger(t),
		settings:        tSettings,
		utxoStore:       store,
		validatorClient: conflictingValidator{},
	}

	bless := func() error {
		_, err := s.blessMissingTransaction(ctx, chainhash.Hash{}, chainhash.Hash{}, tx1, 123, blockIds, validator.ProcessOptions())
		return err
	}

	// alone, the pruned member is a retryable failure
	err := bless()
	require.ErrorIs(t, err, errors.ErrTxNotFound)
	require.NotErrorIs(t, err, errors.ErrTxInvalid)

	_, err = store.SetMinedMulti(ctx, []*chainhash.Hash{counter.TxIDChainHash()}, utxo.MinedBlockInfo{BlockID: 77, BlockHeight: 100})
	require.NoError(t, err)

	err = bless()
	require.ErrorIs(t, err, errors.ErrTxInvalid)
	require.ErrorContains(t, err, "[blessMissingTransaction]", "bless keeps its own wrapping")
	require.ErrorContains(t, err, counter.TxIDChainHash().String(), "the mined counter-spender is named")
}

// A proven conflict is the block's verdict: processTxMetaUsingStore returns it
// with TX_INVALID on top and no CONTEXT_CANCELED in its chain, from both the
// batched and the unbatched branch. A missing record keeps the CONTEXT_CANCELED
// wrap block validation needs to retry it as this node's own failure (see
// keepsCancellation). TestValidateBlock_ProvenConflictIsNotACancellation in
// blockvalidation checks how the callers classify every failure.
func Test_processTxMetaUsingStore_provenConflictIsNotACancellation(t *testing.T) {
	ctx := context.Background()
	mined := utxo.WithMinedBlockInfo(utxo.MinedBlockInfo{BlockID: 77, BlockHeight: 100})
	blockIds := map[uint32]bool{77: true}

	for _, batched := range []bool{true, false} {
		t.Run(fmt.Sprintf("batched=%t", batched), func(t *testing.T) {
			tSettings := test.CreateBaseTestSettings(t)

			store := newSQLUtxoStore(t, ctx, tSettings)
			_, chain := buildConflictingChain(t, ctx, store, 1, false, mined)

			orphanParent := buildTx(t, []outpoint{{tx: parentTx1, vout: 0}}, 1, 9301)
			_, _, err := store.SpendAndCreate(ctx, orphanParent, 122, utxo.WithCreateOnly())
			require.NoError(t, err)

			orphan := spendingChild(t, orphanParent, 0)
			createConflicting(t, ctx, store, orphan)
			require.NoError(t, store.Delete(ctx, orphanParent.TxIDChainHash()))

			s := &Server{logger: ulogger.NewErrorTestLogger(t), settings: tSettings, utxoStore: store}

			process := func(txHashes ...chainhash.Hash) error {
				_, err := s.processTxMetaUsingStore(ctx, txHashes, make([]metaSliceItem, len(txHashes)), blockIds, batched, false, true)
				return err
			}

			err = process(chain[0])
			require.True(t, provenConflict(err), "TX_INVALID on top: %v", err)
			require.NotErrorIs(t, err, errors.ErrContextCanceled)
			require.False(t, errors.IsContextError(err))

			err = process(*orphan.TxIDChainHash(), chain[0])
			require.True(t, provenConflict(err), "a failure elsewhere does not bring the wrap back: %v", err)
			require.NotErrorIs(t, err, errors.ErrContextCanceled)

			err = process(*orphan.TxIDChainHash())
			require.ErrorIs(t, err, errors.ErrTxNotFound)
			require.ErrorIs(t, err, errors.ErrContextCanceled, "a missing record stays retryable")
			require.False(t, provenConflict(err))
		})
	}

	t.Run("a TX_INVALID under another code is not a proven conflict", func(t *testing.T) {
		err := settledError(ctx, errProcessTxMetaSettle, errors.NewProcessingError("not proven", errors.NewTxInvalidError("deeper in the chain")))
		require.ErrorIs(t, err, errors.ErrContextCanceled, "only TX_INVALID on top is a proven conflict")
	})

	t.Run("a store fault whose text names a deadline is not a cancellation while ctx is live", func(t *testing.T) {
		err := settledError(ctx, errProcessTxMetaSettle, errors.NewStorageError("store read", context.DeadlineExceeded))
		require.NotErrorIs(t, err, errors.ErrContextCanceled)
		require.ErrorIs(t, err, errors.ErrStorageError)
	})

	// ValidateSubtreeInternal returns the error as it is, so a failure that is not
	// wrapped as a cancellation still carries the prefix, under its own code.
	t.Run("a failure that is not wrapped keeps the prefix and its code", func(t *testing.T) {
		for _, failure := range []*errors.Error{
			errors.NewTxInvalidError("proven"),
			errors.NewStorageError("store down"),
			errors.NewProcessingError("walk", errors.NewUtxoWalkLimitExceededError("over budget")),
			errors.NewThresholdExceededError("threshold"),
		} {
			err := settledError(ctx, errProcessTxMetaSettle, failure)

			var top *errors.Error
			require.True(t, errors.As(err, &top))
			require.Equal(t, failure.Code(), top.Code(), "%v", err)
			require.Equal(t, errProcessTxMetaSettle, top.Message())
			require.ErrorIs(t, err, failure)
			require.NotErrorIs(t, err, errors.ErrContextCanceled)
		}
	})
}

// cancelOnCheckStore cancels the caller's ctx when the conflict check makes its
// first read (phase 1 asks for the tx's inpoints), and serves that read and every
// later one anyway: a store whose reads had already completed when the
// cancellation arrived.
type cancelOnCheckStore struct {
	utxo.Store
	cancel context.CancelFunc
}

func (c *cancelOnCheckStore) Get(_ context.Context, hash *chainhash.Hash, fs ...fields.FieldName) (*meta.Data, error) {
	if slices.Contains(fs, fields.TxInpoints) {
		c.cancel()
	}

	return c.Store.Get(context.Background(), hash, fs...)
}

func (c *cancelOnCheckStore) GetMeta(_ context.Context, hash *chainhash.Hash, data *meta.Data) error {
	return c.Store.GetMeta(context.Background(), hash, data)
}

// A proven conflict is proven by records that were read, so it is TX_INVALID on
// every path even when ctx is cancelled while the check runs. The fresh path
// used to return it wrapped as a cancellation in that case, while the cached path
// and blessMissingTransaction rejected: the same block was retried or rejected
// depending on which path reached its tx.
func Test_provenConflictWithCancelledCtxIsTxInvalidOnEveryPath(t *testing.T) {
	mined := utxo.WithMinedBlockInfo(utxo.MinedBlockInfo{BlockID: 77, BlockHeight: 100})
	blockIds := map[uint32]bool{77: true}

	// newStore returns a store holding tx1, conflicting, whose counter-spender is
	// mined on the candidate chain, and a ctx the store cancels once the check
	// starts reading.
	newStore := func(t *testing.T) (context.Context, *settings.Settings, *cancelOnCheckStore) {
		t.Helper()

		tSettings := test.CreateBaseTestSettings(t)
		tSettings.UtxoStore.SpendBatcherDurationMillis = 1

		store := newSQLUtxoStore(t, context.Background(), tSettings)
		buildConflictingChain(t, context.Background(), store, 1, false, mined)

		ctx, cancel := context.WithCancel(context.Background())
		t.Cleanup(cancel)

		return ctx, tSettings, &cancelOnCheckStore{Store: store, cancel: cancel}
	}

	requireProvenAfterCancel := func(t *testing.T, ctx context.Context, err error) {
		t.Helper()

		require.Error(t, ctx.Err(), "ctx was cancelled during the check")
		require.ErrorIs(t, err, errors.ErrTxInvalid)
		require.NotErrorIs(t, err, errors.ErrContextCanceled)
	}

	for _, batched := range []bool{true, false} {
		t.Run(fmt.Sprintf("fresh path, batched=%t", batched), func(t *testing.T) {
			ctx, tSettings, store := newStore(t)
			s := &Server{logger: ulogger.NewErrorTestLogger(t), settings: tSettings, utxoStore: store}

			txHashes := []chainhash.Hash{*tx1.TxIDChainHash()}
			_, err := s.processTxMetaUsingStore(ctx, txHashes, make([]metaSliceItem, len(txHashes)), blockIds, batched, false, true)

			requireProvenAfterCancel(t, ctx, err)
			require.True(t, provenConflict(err), "TX_INVALID on top: %v", err)
		})
	}

	t.Run("cached path", func(t *testing.T) {
		ctx, tSettings, store := newStore(t)
		s, block, cachedSubtrees := newCachedCheckServer(t, store, tSettings, []chainhash.Hash{*tx1.TxIDChainHash()}, []uint32{77})

		requireProvenAfterCancel(t, ctx, s.checkCachedSubtreesAgainstCandidateChain(ctx, block, cachedSubtrees))
	})

	t.Run("bless", func(t *testing.T) {
		ctx, tSettings, store := newStore(t)
		s := &Server{
			logger:          ulogger.NewErrorTestLogger(t),
			settings:        tSettings,
			utxoStore:       store,
			validatorClient: conflictingValidator{},
		}

		_, err := s.blessMissingTransaction(ctx, chainhash.Hash{}, chainhash.Hash{}, tx1, 123, blockIds, validator.ProcessOptions())
		requireProvenAfterCancel(t, ctx, err)
	})
}
