package aerospike_test

import (
	"strings"
	"testing"
	"time"

	"github.com/bsv-blockchain/aerospike-client-go/v8"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	teranode_aerospike "github.com/bsv-blockchain/teranode/stores/utxo/aerospike"
	"github.com/bsv-blockchain/teranode/stores/utxo/tests"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/bsv-blockchain/teranode/util/uaerospike"
	"github.com/stretchr/testify/require"
)

// The conflict recovery cases share one Aerospike container. Every case starts from empty UTXO,
// WAL and checklist sets, so the order of the cases does not matter.
func TestAerospikeConflictRecovery(t *testing.T) {
	client, store, _, deferFn := initAerospike(t, test.CreateBaseTestSettings(t), ulogger.NewErrorTestLogger(t))
	t.Cleanup(deferFn)

	cases := []struct {
		name string
		run  func(t *testing.T)
	}{
		{"WALPendingScan", func(t *testing.T) { conflictWALPendingScan(t, client, store) }},
		{"WALIndex", func(t *testing.T) { conflictWALIndex(t, client, store) }},
		{"WideCascade", func(t *testing.T) { tests.ConflictRecoveryWideCascade(t, store) }},
		{"LateLoserChild", func(t *testing.T) { tests.ConflictRecoveryLateLoserChild(t, store) }},
		{"LateFreeInputSpender", func(t *testing.T) { tests.ConflictRecoveryLateFreeInputSpender(t, store) }},
		{"OutsiderOwnedInput", func(t *testing.T) { tests.ConflictRecoveryOutsiderOwnedInput(t, store) }},
		{"EarlyFailure", func(t *testing.T) { tests.ConflictRecoveryEarlyFailure(t, store) }},
		{"RefusedAfterRollback", func(t *testing.T) { tests.ConflictRecoveryRefusedAfterRollback(t, store) }},
		{"HealStaleForward", func(t *testing.T) { tests.ConflictRecoveryHealStaleForward(t, store) }},
		{"SetConflictingChildren", func(t *testing.T) { tests.ConflictRecoverySetConflictingChildren(t, store, true) }},
		{"ReverseCompletionFailure", func(t *testing.T) { tests.ConflictRecoveryReverseCompletionFailure(t, store) }},
		{"WinnerWithConflictingChild", func(t *testing.T) { tests.ConflictRecoveryWinnerWithConflictingChild(t, store) }},
		{"LateWinnerChild", func(t *testing.T) { tests.ConflictRecoveryLateWinnerChild(t, store) }},
		{"ReverseThenReadd", func(t *testing.T) { tests.ConflictRecoveryReverseThenReadd(t, store) }},
		{"LegacyIntent", func(t *testing.T) { tests.ConflictRecoveryLegacyIntent(t, store) }},
		{"CancelledVerify", func(t *testing.T) { tests.ConflictRecoveryCancelledVerify(t, store) }},
		{"PartialReverseReadd", func(t *testing.T) { tests.ConflictRecoveryPartialReverseReadd(t, store) }},
		{"FreedInputTaken", func(t *testing.T) { tests.ConflictRecoveryFreedInputTaken(t, store) }},
		{"BlockKeyedIntent", func(t *testing.T) { tests.ConflictRecoveryBlockKeyedIntent(t, store) }},
		{"SupersetWinners", func(t *testing.T) { tests.ConflictRecoverySupersetWinners(t, store) }},
		{"SupersetRefusedPhases", func(t *testing.T) { tests.ConflictRecoverySupersetRefusedPhases(t, store) }},
		{"FreedWinningInputTaken", func(t *testing.T) { tests.ConflictRecoveryFreedWinningInputTaken(t, store) }},
		{"AppliedWinnerPrunedParent", func(t *testing.T) { tests.ConflictRecoveryAppliedWinnerPrunedParent(t, store) }},
		{"LegacyKey", func(t *testing.T) { tests.ConflictRecoveryLegacyKey(t, store) }},
		{"RollbackStepFailures", func(t *testing.T) { tests.ConflictRecoveryRollbackStepFailures(t, store) }},
		{"RollbackRetryTwoWinners", func(t *testing.T) { tests.ConflictRecoveryRollbackRetryTwoWinners(t, store) }},
		{"ReplanThenHeal", func(t *testing.T) { tests.ConflictRecoveryReplanThenHeal(t, store) }},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cleanConflictSets(t, client)
			tc.run(t)
		})
	}
}

// cleanConflictSets empties the UTXO set, the WAL root set and the WAL checklist chunk set.
func cleanConflictSets(t *testing.T, client *uaerospike.Client) {
	cleanDB(t, client)
	cleanSet(t, client, aerospikeSet+"_conflictwal")
	cleanSet(t, client, aerospikeSet+"_conflictwal_data")
}

// The pending scan reads only the WAL root. It must not load a checklist, and it must finish the
// deletion of an intent whose chunk cleanup was interrupted instead of returning it.
func conflictWALPendingScan(t *testing.T, client *uaerospike.Client, store *teranode_aerospike.Store) {
	ctx := t.Context()

	intent := utxo.ConflictIntent{
		Recovery:    &utxo.ConflictRecovery{Version: 1, Phase: utxo.ConflictRecoveryPlanning},
		Kind:        utxo.ConflictIntentForward,
		BlockHeight: 7,
		BlockHash:   chainhash.HashH([]byte("pending-scan-block")),
		TxHashes:    []chainhash.Hash{chainhash.HashH([]byte("pending-scan-winner"))},
		StartedAt:   1,
	}
	id := intent.IntentID()

	require.NoError(t, store.BeginConflictIntent(ctx, intent))
	require.NoError(t, store.SaveConflictRecovery(ctx, id, &utxo.ConflictRecovery{
		Version:      1,
		Phase:        utxo.ConflictRecoveryFinished,
		Transactions: []utxo.ConflictTransaction{{Hash: intent.TxHashes[0]}},
		Winners:      intent.TxHashes,
	}))

	pending, err := store.PendingConflictIntents(ctx)
	require.NoError(t, err)
	require.Len(t, pending, 1)
	require.Equal(t, id, pending[0].IntentID())
	require.Nil(t, pending[0].Recovery, "the pending scan must not load checklists")

	recovery, err := store.LoadConflictRecovery(ctx, id)
	require.NoError(t, err)
	require.Equal(t, utxo.ConflictRecoveryFinished, recovery.Phase)

	rootKey, err := aerospike.NewKey(store.GetNamespace(), store.GetName()+"_conflictwal", id[:])
	require.NoError(t, err)

	root, err := client.Get(nil, rootKey, "recoveryRefs")
	require.NoError(t, err)

	refs, ok := root.Bins["recoveryRefs"].([]interface{})
	require.True(t, ok)
	require.NotEmpty(t, refs)

	ref, ok := refs[0].(string)
	require.True(t, ok)

	digest, _, found := strings.Cut(ref, ":")
	require.True(t, found)

	chunkKey, err := aerospike.NewKey(store.GetNamespace(), store.GetName()+"_conflictwal_data", id.String()+":"+digest+":0")
	require.NoError(t, err)

	exists, err := client.Exists(nil, chunkKey)
	require.NoError(t, err)
	require.True(t, exists, "the checklist chunk must exist before cleanup")

	// Model a CompleteConflictIntent that stopped after it marked the root done.
	require.NoError(t, client.PutBins(nil, rootKey, aerospike.NewBin("recoveryDone", true)))

	pending, err = store.PendingConflictIntents(ctx)
	require.NoError(t, err)
	require.Empty(t, pending, "a completed intent must not be returned as pending")

	exists, err = client.Exists(nil, rootKey)
	require.NoError(t, err)
	require.False(t, exists, "the scan must finish deleting the WAL root")

	exists, err = client.Exists(nil, chunkKey)
	require.NoError(t, err)
	require.False(t, exists, "the scan must finish deleting the checklist chunks")
}

// Startup finds pending intents through the WAL index, without a scan of the namespace. The index
// is built by the server over every WAL root, so a root written before the index existed, by a
// build that predates it, is found through it too. While the index is missing the read falls back
// to a scan of the set.
func conflictWALIndex(t *testing.T, client *uaerospike.Client, store *teranode_aerospike.Store) {
	ctx := t.Context()
	walSet := store.GetName() + "_conflictwal"
	indexName := store.ConflictWALIndexName()

	// Model a store from before the index: drop it, and recreate it whatever the outcome, for the
	// cases that follow.
	require.NoError(t, client.DropIndex(nil, store.GetNamespace(), walSet, indexName))
	t.Cleanup(store.EnsureConflictWALIndex)

	require.Eventually(t, func() bool {
		_, readable, err := store.QueryConflictWALIndex(ctx)
		return err == nil && !readable
	}, 30*time.Second, 100*time.Millisecond, "the dropped index must stop being readable")

	// A root as an older build wrote it: no keyFormula and no recovery bins, keyed by the legacy
	// formula.
	legacy := utxo.ConflictIntent{
		Kind:        utxo.ConflictIntentReverse,
		BlockHeight: 11,
		BlockHash:   chainhash.HashH([]byte("wal-index-legacy-block")),
		TxHashes:    []chainhash.Hash{chainhash.HashH([]byte("wal-index-legacy-tx"))},
		StartedAt:   1,
	}
	legacyID := utxo.LegacyConflictIntentID(legacy.Kind, legacy.BlockHash, legacy.BlockHeight, legacy.TxHashes)

	legacyKey, keyErr := aerospike.NewKey(store.GetNamespace(), walSet, legacyID[:])
	require.NoError(t, keyErr)
	require.NoError(t, client.PutBins(nil, legacyKey,
		aerospike.NewBin("kind", string(legacy.Kind)),
		aerospike.NewBin("blockHeight", int(legacy.BlockHeight)),
		aerospike.NewBin("blockHash", legacy.BlockHash[:]),
		aerospike.NewBin("txHashes", legacy.TxHashes[0][:]),
		aerospike.NewBin("startedAt", int(legacy.StartedAt)),
	))

	pending, err := store.PendingConflictIntents(ctx)
	require.NoError(t, err, "without the index the read falls back to a scan of the set")
	require.Len(t, pending, 1)
	require.Equal(t, legacyID, pending[0].IntentID())

	// The upgrade: the store creates the index, and the server builds it over the existing root.
	store.EnsureConflictWALIndex()

	require.Eventually(t, func() bool {
		_, readable, err := store.QueryConflictWALIndex(ctx)
		return err == nil && readable
	}, 60*time.Second, 100*time.Millisecond, "the recreated index must become readable")

	forward := utxo.ConflictIntent{
		Recovery:    &utxo.ConflictRecovery{Version: 1, Phase: utxo.ConflictRecoveryPlanning},
		Kind:        utxo.ConflictIntentForward,
		BlockHeight: 12,
		BlockHash:   chainhash.HashH([]byte("wal-index-forward-block")),
		TxHashes:    []chainhash.Hash{chainhash.HashH([]byte("wal-index-forward-winner"))},
		StartedAt:   2,
	}
	require.NoError(t, store.BeginConflictIntent(ctx, forward))

	indexed, readable, queryErr := store.QueryConflictWALIndex(ctx)
	require.NoError(t, queryErr)
	require.True(t, readable)
	require.Equal(t, 2, indexed, "the index must hold the root written before it and the one written after")

	pending, err = store.PendingConflictIntents(ctx)
	require.NoError(t, err)
	require.Len(t, pending, 2)

	ids := map[chainhash.Hash]bool{}
	for _, intent := range pending {
		ids[intent.IntentID()] = true
	}

	require.True(t, ids[legacyID], "the legacy root keeps its own key")
	require.True(t, ids[utxo.ForwardConflictIntentID(forward.BlockHash)])

	require.NoError(t, store.CompleteConflictIntent(ctx, legacyID))
	require.NoError(t, store.CompleteConflictIntent(ctx, forward.IntentID()))

	indexed, readable, queryErr = store.QueryConflictWALIndex(ctx)
	require.NoError(t, queryErr)
	require.True(t, readable)
	require.Zero(t, indexed, "a completed intent leaves the index")
}
