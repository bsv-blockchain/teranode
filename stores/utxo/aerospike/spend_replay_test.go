package aerospike_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/bsv-blockchain/aerospike-client-go/v8"
	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	astore "github.com/bsv-blockchain/teranode/stores/utxo/aerospike"
	apruner "github.com/bsv-blockchain/teranode/stores/utxo/aerospike/pruner"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/bsv-blockchain/teranode/util/uaerospike"
	"github.com/stretchr/testify/require"
)

// The replay tests run at the default retention of 288, so a spend recorded at 1000 is history
// from block 1144 on (utxo.HistoricalSpendAge) and the pruner may delete its child from 1289.
const (
	replayRetention  = 288
	replaySpendBlock = 1000
	replayPruneBlock = 1400
	// replayBlock is when a pruned transaction is resubmitted: 1,001 blocks after its spend.
	replayBlock = 2001
)

type replayConfig struct {
	paginated, ttl, expressions, defensive bool
	// parents is how many parents the child spends one output of; default 1.
	parents int
}

// replayFixture is a stored child C that spends output outputIndex of each of its parents, was
// mined with them in block 1000 and then had its only output spent by a grandchild, so the pruner
// may delete it. Every parent keeps an unspent output, so every parent survives the prune.
type replayFixture struct {
	t           *testing.T
	client      *uaerospike.Client
	store       *astore.Store
	ctx         context.Context
	settings    *settings.Settings
	parents     []*bt.Tx
	child       *bt.Tx
	childKey    *aerospike.Key
	outputIndex uint32
}

func newReplayFixture(t *testing.T, cfg replayConfig) *replayFixture {
	t.Helper()

	s := test.CreateBaseTestSettings(t)
	s.GlobalBlockHeightRetention = replayRetention
	s.UtxoStore.DisableDAHCleaner = false
	s.Pruner.UTXODefensiveEnabled = cfg.defensive
	s.Pruner.UTXOSetTTL = cfg.ttl
	s.Aerospike.EnableSpendFilterExpressions = true

	var outputIndex uint32

	outputs := 2

	if cfg.paginated {
		s.UtxoStore.UtxoBatchSize = 2
		outputIndex = 3
		outputs = 5
	}

	if cfg.expressions {
		// the expression path needs one utxo per record
		s.UtxoStore.UtxoBatchSize = 1
	}

	// Most replay tests prune, and the pruner sweeps the whole set.
	client, store, ctx, cleanup := initAerospikeOwnServer(t, s, ulogger.New("spend-replay-test"))
	t.Cleanup(cleanup)

	require.NoError(t, store.SetBlockHeight(replaySpendBlock))

	if cfg.parents == 0 {
		cfg.parents = 1
	}

	f := &replayFixture{t: t, client: client, store: store, ctx: ctx, settings: s, outputIndex: outputIndex}
	child := bt.NewTx()

	for i := 0; i < cfg.parents; i++ {
		parent := bt.NewTx()
		require.NoError(t, parent.From(fmt.Sprintf("%064x", 100+i), 0, "51", 30000))

		for o := 0; o < outputs; o++ {
			require.NoError(t, parent.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 4000))
		}

		_, err := store.Create(ctx, parent, replaySpendBlock)
		require.NoError(t, err)

		require.NoError(t, child.From(parent.TxID(), outputIndex, parent.Outputs[outputIndex].LockingScript.String(), parent.Outputs[outputIndex].Satoshis))

		f.parents = append(f.parents, parent)
	}

	require.NoError(t, child.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 3000))

	_, _, err := store.SpendAndCreate(ctx, child, replaySpendBlock)
	require.NoError(t, err)

	mined := make([]*chainhash.Hash, 0, 1+len(f.parents))
	mined = append(mined, child.TxIDChainHash())

	for _, parent := range f.parents {
		mined = append(mined, parent.TxIDChainHash())
	}

	_, err = store.SetMinedMulti(ctx, mined, utxo.MinedBlockInfo{BlockID: replaySpendBlock, BlockHeight: replaySpendBlock, OnLongestChain: true})
	require.NoError(t, err)

	grandchild := bt.NewTx()
	require.NoError(t, grandchild.From(child.TxID(), 0, child.Outputs[0].LockingScript.String(), child.Outputs[0].Satoshis))
	require.NoError(t, grandchild.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 2000))

	_, _, err = store.SpendAndCreate(ctx, grandchild, replaySpendBlock+1)
	require.NoError(t, err)

	_, err = store.SetMinedMulti(ctx, []*chainhash.Hash{grandchild.TxIDChainHash()}, utxo.MinedBlockInfo{BlockID: replaySpendBlock + 1, BlockHeight: replaySpendBlock + 1, OnLongestChain: true})
	require.NoError(t, err)

	f.child = child
	f.childKey = f.keyOf(child.TxIDChainHash())

	return f
}

func (f *replayFixture) keyOf(hash *chainhash.Hash) *aerospike.Key {
	f.t.Helper()

	key, err := aerospike.NewKey(f.store.GetNamespace(), f.store.GetName(), hash.CloneBytes())
	require.NoError(f.t, err)

	return key
}

// prune runs the real pruner once and waits for the child's record to be gone.
func (f *replayFixture) prune(height uint32) {
	f.t.Helper()

	astore.ResetPrunerServiceForTests()
	f.t.Cleanup(astore.ResetPrunerServiceForTests)

	require.NoError(f.t, f.store.CreateIndexIfNotExists(f.ctx, apruner.IndexName, fields.DeleteAtHeight.String(), aerospike.NUMERIC))
	require.NoError(f.t, f.store.WaitForIndexReady(f.ctx, apruner.IndexName))

	svc, err := f.store.GetPrunerService()
	require.NoError(f.t, err)

	n, err := svc.(*apruner.Service).PruneWithPartitions(f.ctx, height, "spend-replay", 1)
	require.NoError(f.t, err)
	require.Equal(f.t, int64(1), n, "the pruner deletes the fully spent child only")

	require.Eventually(f.t, func() bool {
		exists, existsErr := f.client.Exists(nil, f.childKey)

		return existsErr == nil && !exists
	}, 5*time.Second, 20*time.Millisecond, "the pruned child's record is gone")
}

func (f *replayFixture) childExists() bool {
	f.t.Helper()

	exists, err := f.client.Exists(nil, f.childKey)
	require.NoError(f.t, err)

	return exists
}

// spendOf is the spend of output outputIndex of parent i by the child, with the child's spending data.
func (f *replayFixture) spendOf(i int) *utxo.Spend {
	f.t.Helper()

	spends, err := utxo.GetSpends(f.child)
	require.NoError(f.t, err)

	return spends[i]
}

// requireStillSpentByChild asserts the parent's output still records the child as its spender.
func (f *replayFixture) requireStillSpentByChild(i int) {
	f.t.Helper()

	resp, err := f.store.GetSpend(f.ctx, f.spendOf(i))
	require.NoError(f.t, err)
	require.NotNil(f.t, resp.SpendingData, "parent %d output must still record its spender", i)
	require.Equal(f.t, *f.child.TxIDChainHash(), *resp.SpendingData.TxID)
	require.Equal(f.t, int(utxo.Status_SPENT), resp.Status)
}

func (f *replayFixture) requireNoMarker(i int) {
	f.t.Helper()

	// the Lua marker refusal would refuse the replay even with the height rule broken, so pin
	// that the pruner wrote none: the refusal under test comes from the height
	record, err := f.client.Get(nil, parentKeyForOutput(f.t, f.store, f.parents[i].TxIDChainHash(), f.outputIndex, f.settings.UtxoStore.UtxoBatchSize))
	require.NoError(f.t, err)
	require.NotContains(f.t, record.Bins, fields.DeletedChildren.String(), "the pruner must write no replay marker on parent %d", i)
}

// TestPrunedReplayIsRefusedAtSpend_Aerospike: child C spent parent P's output at 1000, was mined
// and then pruned while P survives. Resubmitted a thousand blocks later it finds P's output
// already spent by C, an idempotent match, recorded 1,001 blocks ago. C has no record, so the
// spend is refused as a pruned replay, writes nothing and leaves P's output spent by C.
func TestPrunedReplayIsRefusedAtSpend_Aerospike(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name string
		cfg  replayConfig
	}{
		{name: "normal"},
		{name: "ttl", cfg: replayConfig{ttl: true}},
		{name: "paginated_parent", cfg: replayConfig{paginated: true}},
		{name: "defensive", cfg: replayConfig{defensive: true}},
		{name: "expressions", cfg: replayConfig{expressions: true}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newReplayFixture(t, tc.cfg)

			_, _, err := f.store.SpendAndCreate(f.ctx, f.child, replayBlock)
			require.ErrorIs(t, err, errors.ErrTxExists, "control: while C is stored its resubmission is refused at the create, as before")

			f.prune(replayPruneBlock)
			f.requireNoMarker(0)

			_, _, err = f.store.SpendAndCreate(f.ctx, f.child, replayBlock)
			require.ErrorIs(t, err, errors.ErrUtxoSpendingTxPruned, "a pruned confirmed child must not be recreated")
			require.Contains(t, err.Error(), "spending transaction was pruned")
			require.False(t, f.childExists(), "the replay must not create C")

			f.requireStillSpentByChild(0)

			// the refusal wrote nothing, so a second replay is refused the same way
			_, _, err = f.store.SpendAndCreate(f.ctx, f.child, replayBlock+1)
			require.ErrorIs(t, err, errors.ErrUtxoSpendingTxPruned)
			f.requireStillSpentByChild(0)
		})
	}
}

// TestHonestRetryIsNotARejectedReplay_Aerospike: D spends P's output 1 at 2001 and the node
// crashes before D's record is created. Retried two blocks later it meets its own spend, an
// idempotent match 2 blocks old, which is accepted as before so the create can run. On the
// expression path (one utxo per record) the first spend is written without Lua and must record its
// height as well: a spend without one never counts as historical, so a replay of it after
// pruning would be accepted.
func TestHonestRetryIsNotARejectedReplay_Aerospike(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name string
		cfg  replayConfig
	}{
		{name: "lua"},
		{name: "expressions", cfg: replayConfig{expressions: true}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newReplayFixture(t, tc.cfg)

			retry := bt.NewTx()
			require.NoError(t, retry.From(f.parents[0].TxID(), 1, f.parents[0].Outputs[1].LockingScript.String(), f.parents[0].Outputs[1].Satoshis))
			require.NoError(t, retry.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 3000))

			_, err := f.store.Spend(f.ctx, retry, replayBlock)
			require.NoError(t, err)

			// the spend height, whichever path wrote it
			entryKey := f.parents[0].TxIDChainHash()
			heightKey, heightOffset := f.keyOf(entryKey), 1
			if tc.cfg.expressions {
				// one utxo per record: output 1 lives on its own record, at offset 0
				heightKey, heightOffset = parentKeyForOutput(t, f.store, entryKey, 1, 1), 0
			}

			height, ok := spendHeightAt(t, f.client, heightKey, heightOffset)
			require.True(t, ok, "the spend records its height")
			require.Equal(t, uint32(replayBlock), height)

			_, _, err = f.store.SpendAndCreate(f.ctx, retry, replayBlock+2)
			require.NoError(t, err, "a retry within a few blocks of its own spend must not be refused")

			exists, err := f.client.Exists(nil, f.keyOf(retry.TxIDChainHash()))
			require.NoError(t, err)
			require.True(t, exists, "the retry created its record")
		})
	}
}

// TestStoredOldChildIsNotARejectedReplay_Aerospike: E waited unmined for 200 blocks and is
// resubmitted. Its record exists, so the match is ordinary: no replay error, and the create
// answers TX_EXISTS as it did before heights were stored.
func TestStoredOldChildIsNotARejectedReplay_Aerospike(t *testing.T) {
	t.Parallel()

	f := newReplayFixture(t, replayConfig{})

	_, err := f.store.Spend(f.ctx, f.child, replaySpendBlock+200)
	require.NoError(t, err, "C's record exists: a match 200 blocks old is not a replay")

	_, _, err = f.store.SpendAndCreate(f.ctx, f.child, replayBlock)
	require.ErrorIs(t, err, errors.ErrTxExists)
	require.NotErrorIs(t, err, errors.ErrUtxoSpendingTxPruned)

	f.requireStillSpentByChild(0)
}

// TestReappliedSpendAtItsOwnHeightIsAccepted_Aerospike: a block that applies a transaction the
// store already spent for (quick validation, a re-sync after a reset) spends at the height the
// spend was recorded at, so the match is young and passes, with or without the spender's record.
func TestReappliedSpendAtItsOwnHeightIsAccepted_Aerospike(t *testing.T) {
	t.Parallel()

	f := newReplayFixture(t, replayConfig{})
	f.prune(replayPruneBlock)

	_, err := f.store.Spend(f.ctx, f.child, replaySpendBlock)
	require.NoError(t, err, "incoming height equals the recorded height: the match is young")

	_, err = f.store.Spend(f.ctx, f.child, replaySpendBlock-50)
	require.NoError(t, err, "a block below the recorded height is not history either")

	f.requireStillSpentByChild(0)
}

// TestFailedReplayKeepsItsHistoricalMatch_Aerospike: C spent two parents' outputs. P:0 is still
// spent by C, and Q:0 was released since (a rolled-back reorg). The replay of the pruned C matches
// P:0, which is history, and spends Q:0 fresh. The call fails on the replay refusal and reverses
// the spend it wrote, Q:0, but not P:0: that spend was made by the confirmed C, and releasing it
// hands a confirmed output to anyone.
func TestFailedReplayKeepsItsHistoricalMatch_Aerospike(t *testing.T) {
	t.Parallel()

	f := newReplayFixture(t, replayConfig{parents: 2})
	f.prune(replayPruneBlock)

	// release Q:0 as a reorg would
	require.NoError(t, f.store.Unspend(f.ctx, []*utxo.Spend{f.spendOf(1)}))

	released, err := f.store.GetSpend(f.ctx, f.spendOf(1))
	require.NoError(t, err)
	require.Nil(t, released.SpendingData, "fixture: Q:0 is free")

	_, _, err = f.store.SpendAndCreate(f.ctx, f.child, replayBlock)
	require.ErrorIs(t, err, errors.ErrUtxoSpendingTxPruned)

	f.requireStillSpentByChild(0)

	after, err := f.store.GetSpend(f.ctx, f.spendOf(1))
	require.NoError(t, err)
	require.Nil(t, after.SpendingData, "the fresh spend the failed call wrote is reversed")
	require.False(t, f.childExists())

	// a new spender must not be able to take the confirmed output
	thief := bt.NewTx()
	require.NoError(t, thief.From(f.parents[0].TxID(), f.outputIndex, f.parents[0].Outputs[f.outputIndex].LockingScript.String(), f.parents[0].Outputs[f.outputIndex].Satoshis))
	require.NoError(t, thief.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 3999))

	_, _, err = f.store.SpendAndCreate(f.ctx, thief, replayBlock+1)
	require.ErrorIs(t, err, errors.ErrSpent)
}

// TestPreUpgradeEntryWithoutSpenderIsAccepted_Aerospike: an output spent before heights were
// stored has no height in the spendHeights bin. It never counts as history, so a spend left behind
// before the upgrade cannot stall a block. The cost is that a spender pruned after the upgrade, by
// a pruner that writes no marker, is accepted again as on a store without the check.
func TestPreUpgradeEntryWithoutSpenderIsAccepted_Aerospike(t *testing.T) {
	t.Parallel()

	f := newReplayFixture(t, replayConfig{})

	key := f.keyOf(f.parents[0].TxIDChainHash())
	spendHeightClear(t, f.client, key, int(f.outputIndex))
	_, ok := spendHeightAt(t, f.client, key, int(f.outputIndex))
	require.False(t, ok)

	f.prune(replayPruneBlock)

	_, _, err := f.store.SpendAndCreate(f.ctx, f.child, replayPruneBlock+1000)
	require.NoError(t, err, "no height recorded: not history, however late the spend comes")
	require.True(t, f.childExists())
	f.requireStillSpentByChild(0)
}

// TestUnspendAfterAHistoricalMatchReleasesTheOutput_Aerospike: a reorg that unmines C unspends its
// inputs. That must still work on an entry that carries a height, whether or not the replay was
// refused first.
func TestUnspendAfterAHistoricalMatchReleasesTheOutput_Aerospike(t *testing.T) {
	t.Parallel()

	f := newReplayFixture(t, replayConfig{})
	f.prune(replayPruneBlock)

	_, _, err := f.store.SpendAndCreate(f.ctx, f.child, replayBlock)
	require.ErrorIs(t, err, errors.ErrUtxoSpendingTxPruned)

	require.NoError(t, f.store.Unspend(f.ctx, []*utxo.Spend{f.spendOf(0)}))

	released, err := f.store.GetSpend(f.ctx, f.spendOf(0))
	require.NoError(t, err)
	require.Nil(t, released.SpendingData, "the output is released")
	require.Len(t, spendHeightEntry(t, f.store, f.parents[0].TxIDChainHash(), int(f.outputIndex)), 32)

	other := bt.NewTx()
	require.NoError(t, other.From(f.parents[0].TxID(), f.outputIndex, f.parents[0].Outputs[f.outputIndex].LockingScript.String(), f.parents[0].Outputs[f.outputIndex].Satoshis))
	require.NoError(t, other.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 3999))

	_, _, err = f.store.SpendAndCreate(f.ctx, other, replayBlock+1)
	require.NoError(t, err, "a different spender can take the released output")
}

// TestHistoricalMatchPassesWhenTheSpenderRecordExists_Aerospike pins an open decision. The existence
// read asks whether the call's own transaction has a record. A create-first caller (the second wave
// of SpendAndCreateMulti, RequireParentOutputs, which creates every record before spending) has
// written that record already, so a replay of a pruned transaction arriving that way finds it and
// its historical match passes. Such a call carries a transaction of a block being applied, not a
// mempool submission.
func TestHistoricalMatchPassesWhenTheSpenderRecordExists_Aerospike(t *testing.T) {
	t.Parallel()

	f := newReplayFixture(t, replayConfig{})
	f.prune(replayPruneBlock)

	_, err := f.store.Create(f.ctx, f.child, replayBlock)
	require.NoError(t, err)

	_, err = f.store.Spend(f.ctx, f.child, replayBlock, utxo.IgnoreFlags{RequireParentOutputs: true})
	require.NoError(t, err, "the record exists, so the historical match is an ordinary one")
}
