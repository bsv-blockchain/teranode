package aerospike_test

import (
	"testing"
	"time"

	"github.com/bsv-blockchain/aerospike-client-go/v8"
	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	astore "github.com/bsv-blockchain/teranode/stores/utxo/aerospike"
	apruner "github.com/bsv-blockchain/teranode/stores/utxo/aerospike/pruner"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	spendpkg "github.com/bsv-blockchain/teranode/stores/utxo/spend"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/bsv-blockchain/teranode/util/uaerospike"
	"github.com/stretchr/testify/require"
)

// TestPrunerReplayProtection exercises real pruning and the same-spender replay path.
func TestPrunerReplayProtection(t *testing.T) {
	for _, tc := range []struct {
		name                                                                            string
		paginated, markerFailure, ttl, unspend, expressions, defensive, replace, freeze bool
	}{
		{name: "normal"},
		{name: "ttl", ttl: true},
		{name: "ttl_marker_failure", ttl: true, markerFailure: true},
		{name: "paginated_parent", paginated: true},
		{name: "marker_failure", markerFailure: true},
		{name: "paginated_marker_failure", paginated: true, markerFailure: true},
		// Unspend resets the utxo to its bare hash and leaves the marker alone,
		// so a check nested under "still spent by exactly this child" would stop
		// firing at the moment a compensating rollback depends on it.
		{name: "unspent_parent", unspend: true},
		{name: "paginated_unspent_parent", paginated: true, unspend: true},
		// After the rollback a replacement transaction takes the output. The
		// marker must still win over the conflicting-spender answer, or the block
		// paths cannot identify the replay and its recreated record survives.
		{name: "replaced_parent", unspend: true, replace: true},
		// After the rollback the output is frozen. The marker must still win
		// over the frozen answer, for the same reason.
		{name: "frozen_parent", unspend: true, freeze: true},
		// The expression spend path (utxoBatchSize == 1) writes without Lua when
		// its filter passes. After an unspend the element IS the bare hash, so
		// the first-seen clause passes; the filter has to consult the marker
		// too, or the replay goes straight through.
		{name: "expressions", expressions: true},
		{name: "expressions_unspent_parent", expressions: true, unspend: true},
		{name: "expressions_paginated_unspent_parent", expressions: true, paginated: true, unspend: true},
		{name: "expressions_replaced_parent", expressions: true, unspend: true, replace: true},
		// Defensive mode reads the marker off the scanned record and verifies
		// each spending child before deleting; the marker set must be the same
		// page-only set in both modes.
		{name: "defensive", defensive: true},
		{name: "defensive_paginated_parent", defensive: true, paginated: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			testPrunerReplayProtection(t, tc.paginated, tc.markerFailure, tc.ttl, tc.unspend, tc.expressions, tc.defensive, tc.replace, tc.freeze)
		})
	}
}

func testPrunerReplayProtection(t *testing.T, paginated, markerFailure, ttl, unspendParent, expressions, defensive, replaceParent, freezeParent bool) {
	t.Helper()
	logger := ulogger.New("pruner-replay-test")
	s := test.CreateBaseTestSettings(t)
	s.UtxoStore.DisableDAHCleaner = false
	s.Pruner.UTXODefensiveEnabled = defensive
	s.Pruner.UTXOSetTTL = ttl
	s.Aerospike.EnableSpendFilterExpressions = true
	var outputIndex uint32
	if paginated {
		s.UtxoStore.UtxoBatchSize = 2
		outputIndex = 3
	}
	if expressions {
		// useExpressionSpend requires one utxo per record.
		s.UtxoStore.UtxoBatchSize = 1
	}
	client, store, ctx, cleanup := initAerospike(t, s, logger)
	t.Cleanup(cleanup)
	require.NoError(t, store.SetBlockHeight(1000))
	parent := bt.NewTx()
	require.NoError(t, parent.From("1111111111111111111111111111111111111111111111111111111111111111", 0, "51", 30000))
	require.NoError(t, parent.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 4000))
	require.NoError(t, parent.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 4000))
	if paginated {
		for i := 0; i < 3; i++ {
			require.NoError(t, parent.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 4000))
		}
	}
	_, err := store.Create(ctx, parent, 1000)
	require.NoError(t, err)
	child := bt.NewTx()
	require.NoError(t, child.From(parent.TxID(), outputIndex, parent.Outputs[outputIndex].LockingScript.String(), parent.Outputs[outputIndex].Satoshis))
	require.NoError(t, child.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 3000))
	// Retry after a successful spend but before creation is still legitimate.
	_, err = store.Spend(ctx, child, 1000)
	require.NoError(t, err)
	_, _, err = store.SpendAndCreate(ctx, child, 1000)
	require.NoError(t, err)
	_, err = store.SetMinedMulti(ctx, []*chainhash.Hash{parent.TxIDChainHash(), child.TxIDChainHash()}, utxo.MinedBlockInfo{BlockID: 1000, BlockHeight: 1000, OnLongestChain: true})
	require.NoError(t, err)
	grandchild := bt.NewTx()
	require.NoError(t, grandchild.From(child.TxID(), 0, child.Outputs[0].LockingScript.String(), child.Outputs[0].Satoshis))
	require.NoError(t, grandchild.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 2000))
	_, _, err = store.SpendAndCreate(ctx, grandchild, 1001)
	require.NoError(t, err)
	_, err = store.SetMinedMulti(ctx, []*chainhash.Hash{grandchild.TxIDChainHash()}, utxo.MinedBlockInfo{BlockID: 1001, BlockHeight: 1001, OnLongestChain: true})
	require.NoError(t, err)
	childKey, err := aerospike.NewKey(store.GetNamespace(), store.GetName(), child.TxIDChainHash().CloneBytes())
	require.NoError(t, err)
	before, err := client.Get(nil, childKey)
	require.NoError(t, err)
	require.NotEmpty(t, before.Bins[fields.BlockIDs.String()])
	require.Equal(t, 1, before.Bins[fields.SpentUtxos.String()])
	_, _, err = store.SpendAndCreate(ctx, child, 1200)
	require.ErrorIs(t, err, errors.ErrTxExists, "control: retained mined record prevents recreation")
	{
		astore.ResetPrunerServiceForTests()
		t.Cleanup(astore.ResetPrunerServiceForTests)
		require.NoError(t, store.CreateIndexIfNotExists(ctx, apruner.IndexName, fields.DeleteAtHeight.String(), aerospike.NUMERIC))
		require.NoError(t, store.WaitForIndexReady(ctx, apruner.IndexName))
		svc, err := store.GetPrunerService()
		require.NoError(t, err)
		require.NotNil(t, svc)
		parentKey, err := aerospike.NewKey(store.GetNamespace(), store.GetName(), uaerospike.CalculateKeySource(parent.TxIDChainHash(), outputIndex, s.UtxoStore.UtxoBatchSize))
		require.NoError(t, err)
		if markerFailure {
			// A malformed marker bin makes the real parent update fail on the
			// server. The cycle must SUCCEED and hold this one child back:
			// returning an error here would unwind into PruneWithPartitions,
			// which never retries a non-timeout error, so one poison record
			// would block pruning node-wide forever.
			require.NoError(t, client.Put(nil, parentKey, aerospike.BinMap{fields.DeletedChildren.String(): "invalid-map"}))
			pruned, err := svc.(*apruner.Service).PruneWithPartitions(ctx, 1300, "failed-parent-update", 1)
			require.NoError(t, err, "a per-record marker failure must not fail the cycle")
			require.Equal(t, int64(0), pruned, "the held-back child must not be counted as pruned")
			retained, getErr := client.Get(nil, childKey)
			require.NoError(t, getErr, "failed marker write must retain the child")
			require.Equal(t, before.Expiration, retained.Expiration, "failed marker write must not schedule child expiry")
			require.Equal(t, before.Bins[fields.BlockIDs.String()], retained.Bins[fields.BlockIDs.String()])
			require.Equal(t, before.Bins[fields.SpentUtxos.String()], retained.Bins[fields.SpentUtxos.String()])
			require.NoError(t, client.Put(nil, parentKey, aerospike.BinMap{fields.DeletedChildren.String(): nil}))
		}
		n, err := svc.(*apruner.Service).PruneWithPartitions(ctx, 1300, "isolated-replay-investigation", 1)
		require.NoError(t, err)
		require.Equal(t, int64(1), n, "normal pruning must delete the fully spent child only")
		require.Eventually(t, func() bool { exists, err := client.Exists(nil, childKey); return err == nil && !exists }, 5*time.Second, 20*time.Millisecond)

		// The record the spend path reads is the page holding the spent output,
		// so that is the one that must carry the marker. The master gets no copy
		// in either mode when it differs: nothing reads it there, and it grew
		// without bound on a high fan-out parent until RECORD_TOO_BIG.
		pageRecord, err := client.Get(nil, parentKey)
		require.NoError(t, err)
		require.Contains(t, pageRecord.Bins[fields.DeletedChildren.String()], child.TxID(), "the spending page must carry replay protection")

		if paginated {
			masterKey, err := aerospike.NewKey(store.GetNamespace(), store.GetName(), parent.TxIDChainHash().CloneBytes())
			require.NoError(t, err)
			masterRecord, err := client.Get(nil, masterKey)
			require.NoError(t, err)

			if markers := masterRecord.Bins[fields.DeletedChildren.String()]; markers != nil {
				require.NotContains(t, markers, child.TxID(),
					"the master must not accumulate markers for outputs it does not hold")
			}
		}
	}
	if unspendParent {
		utxoHash, hashErr := util.UTXOHashFromOutput(parent.TxIDChainHash(), parent.Outputs[outputIndex], outputIndex)
		require.NoError(t, hashErr)
		require.NoError(t, store.Unspend(ctx, []*utxo.Spend{{
			TxID:         parent.TxIDChainHash(),
			Vout:         outputIndex,
			UTXOHash:     utxoHash,
			SpendingData: spendpkg.NewSpendingData(child.TxIDChainHash(), 0),
		}}))

		record, getErr := client.Get(nil, parentKeyForOutput(t, store, parent.TxIDChainHash(), outputIndex, s.UtxoStore.UtxoBatchSize))
		require.NoError(t, getErr)
		require.Contains(t, record.Bins[fields.DeletedChildren.String()], child.TxID(), "unspend must leave the replay marker in place")
	}
	if freezeParent {
		utxoHash, hashErr := util.UTXOHashFromOutput(parent.TxIDChainHash(), parent.Outputs[outputIndex], outputIndex)
		require.NoError(t, hashErr)
		require.NoError(t, store.FreezeUTXOs(ctx, []*utxo.Spend{{TxID: parent.TxIDChainHash(), Vout: outputIndex, UTXOHash: utxoHash}}, s))
	}
	if replaceParent {
		replacement := bt.NewTx()
		require.NoError(t, replacement.From(parent.TxID(), outputIndex, parent.Outputs[outputIndex].LockingScript.String(), parent.Outputs[outputIndex].Satoshis))
		require.NoError(t, replacement.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 2999))
		_, _, err = store.SpendAndCreate(ctx, replacement, 1200)
		require.NoError(t, err, "fixture: the replacement takes the output")
	}

	_, _, err = store.SpendAndCreate(ctx, child, 1200)
	require.ErrorIs(t, err, errors.ErrUtxoSpendingTxPruned, "pruned confirmed child must not be recreated")
	require.NotErrorIs(t, err, errors.ErrSpent, "the marker must win over the conflicting-spender answer")
	require.NotErrorIs(t, err, errors.ErrFrozen, "the marker must win over the frozen answer")
	require.Contains(t, err.Error(), "spending transaction was pruned")
	exists, err := client.Exists(nil, childKey)
	require.NoError(t, err)
	require.False(t, exists)
}

// TestPrunerUnresolvableRecordDoesNotBlockCycle proves that a record whose
// external blob has vanished is retained on its own while the rest of the cycle
// still prunes. Before the fix, getTxInputsFromBins returned a ProcessingError
// that unwound the whole chunk into PruneWithPartitions, where a non-timeout
// error is never retried, so one such record blocked all pruning permanently.
func TestPrunerUnresolvableRecordDoesNotBlockCycle(t *testing.T) {
	logger := ulogger.New("pruner-unresolvable-test")
	s := test.CreateBaseTestSettings(t)
	s.UtxoStore.DisableDAHCleaner = false
	s.Pruner.UTXODefensiveEnabled = false
	s.Aerospike.EnableSpendFilterExpressions = true

	client, store, ctx, cleanup := initAerospike(t, s, logger)
	t.Cleanup(cleanup)
	require.NoError(t, store.SetBlockHeight(1000))

	// Parent keeps output 2 unspent so it survives this prune cycle and can carry
	// the replay markers we assert on.
	parent := bt.NewTx()
	require.NoError(t, parent.From("1111111111111111111111111111111111111111111111111111111111111111", 0, "51", 30000))

	for i := 0; i < 3; i++ {
		require.NoError(t, parent.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 4000))
	}

	_, err := store.Create(ctx, parent, 1000)
	require.NoError(t, err)

	// Two independent children of the same parent, both fully spent and mined,
	// so both are eligible for pruning in the same cycle.
	children := make([]*bt.Tx, 2)

	for i := range children {
		child := bt.NewTx()
		require.NoError(t, child.From(parent.TxID(), uint32(i), parent.Outputs[i].LockingScript.String(), parent.Outputs[i].Satoshis))
		require.NoError(t, child.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 3000))
		_, _, err = store.SpendAndCreate(ctx, child, 1000)
		require.NoError(t, err)
		_, err = store.SetMinedMulti(ctx, []*chainhash.Hash{child.TxIDChainHash()}, utxo.MinedBlockInfo{BlockID: 1000, BlockHeight: 1000, OnLongestChain: true})
		require.NoError(t, err)

		grandchild := bt.NewTx()
		require.NoError(t, grandchild.From(child.TxID(), 0, child.Outputs[0].LockingScript.String(), child.Outputs[0].Satoshis))
		require.NoError(t, grandchild.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 2000))
		_, _, err = store.SpendAndCreate(ctx, grandchild, 1001)
		require.NoError(t, err)
		_, err = store.SetMinedMulti(ctx, []*chainhash.Hash{grandchild.TxIDChainHash()}, utxo.MinedBlockInfo{BlockID: 1001, BlockHeight: 1001, OnLongestChain: true})
		require.NoError(t, err)

		children[i] = child
	}

	_, err = store.SetMinedMulti(ctx, []*chainhash.Hash{parent.TxIDChainHash()}, utxo.MinedBlockInfo{BlockID: 1000, BlockHeight: 1000, OnLongestChain: true})
	require.NoError(t, err)

	broken, healthy := children[0], children[1]

	brokenKey, err := aerospike.NewKey(store.GetNamespace(), store.GetName(), broken.TxIDChainHash().CloneBytes())
	require.NoError(t, err)

	healthyKey, err := aerospike.NewKey(store.GetNamespace(), store.GetName(), healthy.TxIDChainHash().CloneBytes())
	require.NoError(t, err)

	// Mark one record external without writing any blob. Its input references are
	// now unrecoverable, which is exactly the anomaly the old code swallowed by
	// deleting the record and losing the parent's replay protection.
	require.NoError(t, client.Put(nil, brokenKey, aerospike.BinMap{fields.External.String(): true}))

	astore.ResetPrunerServiceForTests()
	t.Cleanup(astore.ResetPrunerServiceForTests)
	require.NoError(t, store.CreateIndexIfNotExists(ctx, apruner.IndexName, fields.DeleteAtHeight.String(), aerospike.NUMERIC))
	require.NoError(t, store.WaitForIndexReady(ctx, apruner.IndexName))

	svc, err := store.GetPrunerService()
	require.NoError(t, err)

	n, err := svc.(*apruner.Service).PruneWithPartitions(ctx, 1300, "unresolvable-record", 1)
	require.NoError(t, err, "one unresolvable record must not fail the prune cycle")
	require.Equal(t, int64(1), n, "the healthy child must still be pruned")

	require.Eventually(t, func() bool {
		exists, err := client.Exists(nil, healthyKey)
		return err == nil && !exists
	}, 5*time.Second, 20*time.Millisecond)

	exists, err := client.Exists(nil, brokenKey)
	require.NoError(t, err)
	require.True(t, exists, "the unresolvable record must be retained, not deleted")

	parentKey, err := aerospike.NewKey(store.GetNamespace(), store.GetName(), parent.TxIDChainHash().CloneBytes())
	require.NoError(t, err)

	parentRecord, err := client.Get(nil, parentKey)
	require.NoError(t, err)
	require.Contains(t, parentRecord.Bins[fields.DeletedChildren.String()], healthy.TxID(), "the pruned child must leave replay protection on its parent")

	// A second cycle still makes progress rather than aborting, so a retained
	// record cannot wedge pruning.
	_, err = svc.(*apruner.Service).PruneWithPartitions(ctx, 1300, "unresolvable-record-retry", 1)
	require.NoError(t, err, "the retained record must not wedge later cycles")

	exists, err = client.Exists(nil, brokenKey)
	require.NoError(t, err)
	require.True(t, exists)
}

// parentKeyForOutput returns the Aerospike key of the record that holds a given
// output of a transaction: the master record for the first page, a pagination
// record beyond it.
func parentKeyForOutput(t *testing.T, store *astore.Store, txID *chainhash.Hash, vout uint32, batchSize int) *aerospike.Key {
	t.Helper()

	key, err := aerospike.NewKey(store.GetNamespace(), store.GetName(), uaerospike.CalculateKeySource(txID, vout, batchSize))
	require.NoError(t, err)

	return key
}

// TestPrunedReplayRollbackKeepsSiblingHistoricalSpend pins the store state
// after a pruned child's replay is rejected on one parent's marker while a
// sibling parent carries no marker for it. The state is produced with this
// PR's own hold-back path: Q's marker write fails, P's lands in the same batch,
// and C is retained. Q:0 still records C as its spender, the confirmed spend.
// A rebroadcast of C is rejected on P's marker, and Q:0 must STILL be spent by
// C afterwards: that spend was never made by the replay, and releasing it hands
// a confirmed output to any new transaction. Reproduced by review.
func TestPrunedReplayRollbackKeepsSiblingHistoricalSpend(t *testing.T) {
	logger := ulogger.New("pruner-replay-sibling-test")
	s := test.CreateBaseTestSettings(t)
	s.UtxoStore.DisableDAHCleaner = false
	s.Pruner.UTXODefensiveEnabled = false
	s.Aerospike.EnableSpendFilterExpressions = true

	client, store, ctx, cleanup := initAerospike(t, s, logger)
	t.Cleanup(cleanup)
	require.NoError(t, store.SetBlockHeight(1000))

	mined := func(blockHeight uint32, txs ...*bt.Tx) {
		t.Helper()

		hashes := make([]*chainhash.Hash, 0, len(txs))
		for _, tx := range txs {
			hashes = append(hashes, tx.TxIDChainHash())
		}

		_, err := store.SetMinedMulti(ctx, hashes, utxo.MinedBlockInfo{BlockID: blockHeight, BlockHeight: blockHeight, OnLongestChain: true})
		require.NoError(t, err)
	}

	newParent := func(seed string) *bt.Tx {
		t.Helper()

		parent := bt.NewTx()
		require.NoError(t, parent.From(seed, 0, "51", 30000))
		// Output 1 stays unspent so the parent survives the prune cycle.
		require.NoError(t, parent.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 4000))
		require.NoError(t, parent.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 4000))

		_, err := store.Create(ctx, parent, 1000)
		require.NoError(t, err)

		return parent
	}

	p := newParent("1111111111111111111111111111111111111111111111111111111111111111")
	q := newParent("2222222222222222222222222222222222222222222222222222222222222222")

	c := bt.NewTx()
	require.NoError(t, c.From(p.TxID(), 0, p.Outputs[0].LockingScript.String(), p.Outputs[0].Satoshis))
	require.NoError(t, c.From(q.TxID(), 0, q.Outputs[0].LockingScript.String(), q.Outputs[0].Satoshis))
	require.NoError(t, c.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 7000))
	_, _, err := store.SpendAndCreate(ctx, c, 1000)
	require.NoError(t, err)
	mined(1000, p, q, c)

	d := bt.NewTx()
	require.NoError(t, d.From(c.TxID(), 0, c.Outputs[0].LockingScript.String(), c.Outputs[0].Satoshis))
	require.NoError(t, d.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 6000))
	_, _, err = store.SpendAndCreate(ctx, d, 1001)
	require.NoError(t, err)
	mined(1001, d)

	cKey, err := aerospike.NewKey(store.GetNamespace(), store.GetName(), c.TxIDChainHash().CloneBytes())
	require.NoError(t, err)
	pKey, err := aerospike.NewKey(store.GetNamespace(), store.GetName(), uaerospike.CalculateKeySource(p.TxIDChainHash(), 0, s.UtxoStore.UtxoBatchSize))
	require.NoError(t, err)
	qKey, err := aerospike.NewKey(store.GetNamespace(), store.GetName(), uaerospike.CalculateKeySource(q.TxIDChainHash(), 0, s.UtxoStore.UtxoBatchSize))
	require.NoError(t, err)

	astore.ResetPrunerServiceForTests()
	t.Cleanup(astore.ResetPrunerServiceForTests)
	require.NoError(t, store.CreateIndexIfNotExists(ctx, apruner.IndexName, fields.DeleteAtHeight.String(), aerospike.NUMERIC))
	require.NoError(t, store.WaitForIndexReady(ctx, apruner.IndexName))

	svc, err := store.GetPrunerService()
	require.NoError(t, err)
	require.NotNil(t, svc)

	// Q's marker write fails, P's lands, C is held back.
	require.NoError(t, client.Put(nil, qKey, aerospike.BinMap{fields.DeletedChildren.String(): "invalid-map"}))
	pruned, err := svc.(*apruner.Service).PruneWithPartitions(ctx, 1300, "sibling-marker-failure", 1)
	require.NoError(t, err, "fixture: a per-record marker failure must not fail the cycle")
	require.Equal(t, int64(0), pruned, "fixture: C must be held back")
	require.NoError(t, client.Put(nil, qKey, aerospike.BinMap{fields.DeletedChildren.String(): nil}))

	pRecord, err := client.Get(nil, pKey)
	require.NoError(t, err)
	require.Contains(t, pRecord.Bins[fields.DeletedChildren.String()], c.TxID(), "fixture: P must carry C's marker")

	qRecord, err := client.Get(nil, qKey)
	require.NoError(t, err)
	require.Nil(t, qRecord.Bins[fields.DeletedChildren.String()], "fixture: Q must carry no marker")

	cExists, err := client.Exists(nil, cKey)
	require.NoError(t, err)
	require.True(t, cExists, "fixture: the held-back child is retained")

	q0Hash, err := util.UTXOHashFromOutput(q.TxIDChainHash(), q.Outputs[0], 0)
	require.NoError(t, err)
	q0 := &utxo.Spend{TxID: q.TxIDChainHash(), Vout: 0, UTXOHash: q0Hash}

	before, err := store.GetSpend(ctx, q0)
	require.NoError(t, err)
	require.NotNil(t, before.SpendingData, "fixture: Q:0 is spent")
	require.Equal(t, *c.TxIDChainHash(), *before.SpendingData.TxID, "fixture: Q:0 is spent by C")

	_, _, err = store.SpendAndCreate(ctx, c, 1200)
	require.ErrorIs(t, err, errors.ErrUtxoSpendingTxPruned, "the replay must be rejected on P's marker")

	after, err := store.GetSpend(ctx, q0)
	require.NoError(t, err)
	require.NotNil(t, after.SpendingData, "Q:0 must still record its confirmed spend by C after the rejected replay; a nil spender means the rollback released a confirmed output")
	require.Equal(t, *c.TxIDChainHash(), *after.SpendingData.TxID, "Q:0 must still be spent by C")
	require.Equal(t, int(utxo.Status_SPENT), after.Status, "Q:0 must still be SPENT")

	thief := bt.NewTx()
	require.NoError(t, thief.From(q.TxID(), 0, q.Outputs[0].LockingScript.String(), q.Outputs[0].Satoshis))
	require.NoError(t, thief.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 3999))
	_, _, err = store.SpendAndCreate(ctx, thief, 1201)
	require.ErrorIs(t, err, errors.ErrSpent, "a new transaction must not be able to take the confirmed output Q:0")

	thiefKey, err := aerospike.NewKey(store.GetNamespace(), store.GetName(), thief.TxIDChainHash().CloneBytes())
	require.NoError(t, err)
	thiefExists, err := client.Exists(nil, thiefKey)
	require.NoError(t, err)
	require.False(t, thiefExists, "the thief must not have been created")
}

// TestSpendDoesNotBlessSpenderCreatedByCaller: "parent not found, but the
// spending transaction exists" is treated as a transaction validated before its
// parent was pruned, and the error is cleared. Not when the caller wrote that
// record itself in this pass (the create-first block paths), or a replay of a
// transaction whose parent was pruned too is blessed by its own copy.
func TestSpendDoesNotBlessSpenderCreatedByCaller(t *testing.T) {
	logger := ulogger.New("spend-bless-test")
	s := test.CreateBaseTestSettings(t)
	s.Aerospike.EnableSpendFilterExpressions = true

	_, store, ctx, cleanup := initAerospike(t, s, logger)
	t.Cleanup(cleanup)
	require.NoError(t, store.SetBlockHeight(1000))

	parent := bt.NewTx()
	require.NoError(t, parent.From("1111111111111111111111111111111111111111111111111111111111111111", 0, "51", 30000))
	require.NoError(t, parent.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 4000))
	_, err := store.Create(ctx, parent, 1000)
	require.NoError(t, err)

	child := bt.NewTx()
	require.NoError(t, child.From(parent.TxID(), 0, parent.Outputs[0].LockingScript.String(), parent.Outputs[0].Satoshis))
	require.NoError(t, child.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 3000))
	_, _, err = store.SpendAndCreate(ctx, child, 1000)
	require.NoError(t, err)
	_, err = store.SetMinedMulti(ctx, []*chainhash.Hash{parent.TxIDChainHash(), child.TxIDChainHash()}, utxo.MinedBlockInfo{BlockID: 1000, BlockHeight: 1000, OnLongestChain: true})
	require.NoError(t, err)

	require.NoError(t, store.DeleteComplete(ctx, parent.TxIDChainHash()))

	_, err = store.Spend(ctx, child, 1200)
	require.NoError(t, err, "control: a pre-existing child with a pruned parent is blessed")

	_, err = store.Spend(ctx, child, 1200, utxo.IgnoreFlags{SpenderCreatedByCaller: true})
	require.ErrorIs(t, err, errors.ErrTxNotFound, "a record the caller wrote itself is not proof of prior validation")
}
