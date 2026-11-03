package aerospike_test

import (
	"strings"
	"testing"

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
