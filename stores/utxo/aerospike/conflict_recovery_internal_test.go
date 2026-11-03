package aerospike

import (
	"context"
	"crypto/sha256"
	"fmt"
	"testing"
	"time"

	"github.com/bsv-blockchain/aerospike-client-go/v8"
	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/util"
	"github.com/stretchr/testify/require"
)

// restoreTestTxs stores a parent with outputs outputs and a child that spends all of them.
func restoreTestTxs(t *testing.T, s *Store, outputs int) (parent, child *bt.Tx) {
	t.Helper()

	ctx := t.Context()

	parent = bt.NewTx()
	in := &bt.Input{PreviousTxOutIndex: 0, PreviousTxSatoshis: 1_000_000, PreviousTxScript: bscript.NewFromBytes([]byte{0x51}),
		SequenceNumber: 0xffffffff, UnlockingScript: bscript.NewFromBytes([]byte{0x00})}
	require.NoError(t, in.PreviousTxIDAdd(&chainhash.Hash{0xd5, 0xd5}))
	parent.Inputs = []*bt.Input{in}

	for i := 0; i < outputs; i++ {
		parent.Outputs = append(parent.Outputs, &bt.Output{Satoshis: 10_000, LockingScript: bscript.NewFromBytes([]byte{0x52})})
	}

	_, _, err := s.SpendAndCreate(ctx, parent, 1, utxo.WithCreateOnly())
	require.NoError(t, err)

	child = bt.NewTx()
	for i := 0; i < outputs; i++ {
		require.NoError(t, child.From(parent.TxIDChainHash().String(), uint32(i), parent.Outputs[i].LockingScript.String(), parent.Outputs[i].Satoshis)) //nolint:gosec // small test index
		child.Inputs[i].UnlockingScript = bscript.NewFromBytes([]byte{0x00})
	}

	child.Outputs = []*bt.Output{{Satoshis: 5_000, LockingScript: bscript.NewFromBytes([]byte{0x52})}}

	_, _, err = s.SpendAndCreate(ctx, child, 2)
	require.NoError(t, err)

	return parent, child
}

// RestoreConflictSpends writes the recorded spends with one Lua batch, not one round trip per
// outpoint: a wide cascade restores thousands of outpoints.
func TestRestoreConflictSpends_OneRoundTripPerBatch(t *testing.T) {
	s, _ := setupStoreForLockTest(t)
	ctx := t.Context()

	_, child := restoreTestTxs(t, s, 3)

	spends, err := utxo.GetSpends(child)
	require.NoError(t, err)
	require.NoError(t, s.Unspend(ctx, spends))

	calls := 0
	s.batchOperateFn = func(policy *aerospike.BatchPolicy, records []aerospike.BatchRecordIfc) aerospike.Error {
		calls++
		require.Positive(t, policy.TotalTimeout)
		require.LessOrEqual(t, policy.TotalTimeout, time.Second)
		require.LessOrEqual(t, policy.SocketTimeout, time.Second)
		return s.client.BatchOperate(policy, records)
	}

	bounded, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	require.NoError(t, s.RestoreConflictSpends(bounded, spends, 2))
	require.Equal(t, 1, calls, "the restore must send its spends as one batch")

	s.batchOperateFn = nil

	for _, sp := range spends {
		res, err := s.GetSpend(ctx, sp)
		require.NoError(t, err)
		require.NotNil(t, res.SpendingData)
		require.Equal(t, child.TxIDChainHash(), res.SpendingData.TxID)
	}
}

// Only the phase of a published checklist changes, so a phase save must not rewrite its chunks.
func TestSaveConflictRecovery_PhaseSaveKeepsChunks(t *testing.T) {
	s, client := setupStoreForLockTest(t)
	ctx := t.Context()

	_, child := restoreTestTxs(t, s, 1)

	spends, err := utxo.GetSpends(child)
	require.NoError(t, err)

	intent := utxo.ConflictIntent{
		Recovery:  &utxo.ConflictRecovery{Version: 1, Phase: utxo.ConflictRecoveryPlanning},
		Kind:      utxo.ConflictIntentForward,
		BlockHash: chainhash.HashH([]byte("phase-save-block")),
		TxHashes:  []chainhash.Hash{*child.TxIDChainHash()},
	}
	id := intent.IntentID()
	require.NoError(t, s.BeginConflictIntent(ctx, intent))

	recovery := &utxo.ConflictRecovery{
		Version:        1,
		Phase:          utxo.ConflictRecoveryPrepared,
		Transactions:   []utxo.ConflictTransaction{{Hash: *child.TxIDChainHash(), Conflicting: true}},
		OriginalSpends: spends,
		Winners:        intent.TxHashes,
	}
	require.NoError(t, s.SaveConflictRecovery(ctx, id, recovery))

	rootKey, err := aerospike.NewKey(s.namespace, s.conflictWALSet(), id[:])
	require.NoError(t, err)

	root, err := client.Get(nil, rootKey, "recovery")
	require.NoError(t, err)

	digest, ok := root.Bins["recovery"].(string)
	require.True(t, ok)

	chunkKey, err := s.recoveryChunkKey(id, digest, 0)
	require.NoError(t, err)

	before, err := client.GetHeader(nil, chunkKey)
	require.NoError(t, err)

	for _, phase := range []string{utxo.ConflictRecoveryMutating, utxo.ConflictRecoveryApplied, utxo.ConflictRecoveryFinished} {
		recovery.Phase = phase
		require.NoError(t, s.SaveConflictRecovery(ctx, id, recovery))

		loaded, err := s.LoadConflictRecovery(ctx, id)
		require.NoError(t, err)
		require.Equal(t, phase, loaded.Phase)
		require.Equal(t, recovery.Transactions, loaded.Transactions)
	}

	after, err := client.GetHeader(nil, chunkKey)
	require.NoError(t, err)
	require.Equal(t, before.Generation, after.Generation, "a phase save must not rewrite the checklist chunks")

	require.NoError(t, s.CompleteConflictIntent(ctx, id))
}

func TestConflictBatchPolicyDeadline(t *testing.T) {
	policy := aerospike.NewBatchPolicy()
	policy.TotalTimeout = time.Minute
	policy.SocketTimeout = time.Minute
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	require.NoError(t, boundConflictBatchPolicy(ctx, policy))
	require.Positive(t, policy.TotalTimeout)
	require.LessOrEqual(t, policy.TotalTimeout, time.Second)
	require.LessOrEqual(t, policy.SocketTimeout, time.Second)
	cancel()
	require.ErrorIs(t, boundConflictBatchPolicy(ctx, policy), context.Canceled)
	expired, stop := context.WithDeadline(t.Context(), time.Now().Add(-time.Second))
	defer stop()
	require.ErrorIs(t, boundConflictBatchPolicy(expired, policy), context.DeadlineExceeded)
}

// LoadConflictRecovery refuses a root record whose checklist reference or chunks are damaged, and
// SaveConflictRecovery refuses a checklist for an intent that does not exist.
func TestConflictRecovery_DamagedRecords(t *testing.T) {
	s, _ := setupStoreForLockTest(t)
	ctx := t.Context()

	missing := chainhash.Hash{0xaa}
	require.Error(t, s.SaveConflictRecovery(ctx, missing, &utxo.ConflictRecovery{Version: 1, Phase: utxo.ConflictRecoveryPrepared}), "no root record")
	require.ErrorContains(t, s.SaveConflictRecovery(ctx, missing, &utxo.ConflictRecovery{Version: 2}), "unsupported")
	require.ErrorContains(t, s.RestoreConflictSpends(ctx, []*utxo.Spend{{}}, 1), "invalid conflict restoration spend")

	intent := utxo.ConflictIntent{Kind: utxo.ConflictIntentForward, BlockHeight: 1, BlockHash: chainhash.HashH([]byte("damaged")), StartedAt: 1}
	id := intent.IntentID()
	require.NoError(t, s.BeginConflictIntent(ctx, intent))

	key, err := aerospike.NewKey(s.namespace, s.conflictWALSet(), id[:])
	require.NoError(t, err)
	policy := util.GetAerospikeWritePolicy(s.settings, 0)

	put := func(bins ...*aerospike.Bin) {
		t.Helper()
		require.NoError(t, s.client.PutBins(policy, key, bins...))
	}
	load := func(want string) {
		t.Helper()
		_, err := s.LoadConflictRecovery(ctx, id)
		require.ErrorContains(t, err, want)
	}

	put(aerospike.NewBin("recovery", "abcd"))
	load("invalid conflict recovery reference")

	put(aerospike.NewBin("recovery", "abcd"), aerospike.NewBin("recoveryN", 1))
	_, lerr := s.LoadConflictRecovery(ctx, id)
	require.Error(t, lerr, "a chunk that does not exist")

	chunkKey, kerr := s.recoveryChunkKey(id, "abcd", 0)
	require.NoError(t, kerr)
	require.NoError(t, s.client.PutBins(policy, chunkKey, aerospike.NewBin("data", "not bytes")))
	load("invalid conflict recovery chunk")

	require.NoError(t, s.client.PutBins(policy, chunkKey, aerospike.NewBin("data", []byte("{}"))))
	load("conflict recovery checksum mismatch")

	data := []byte(`{"Version":1,"Phase":"bogus"}`)
	digest := fmt.Sprintf("%x", sha256.Sum256(data))
	chunkKey, kerr = s.recoveryChunkKey(id, digest, 0)
	require.NoError(t, kerr)
	require.NoError(t, s.client.PutBins(policy, chunkKey, aerospike.NewBin("data", data)))
	put(aerospike.NewBin("recovery", digest), aerospike.NewBin("recoveryN", 1))
	load("unknown conflict recovery phase")

	data = []byte(`{"Version":1,"Phase":"prepared"}`)
	digest = fmt.Sprintf("%x", sha256.Sum256(data))
	chunkKey, kerr = s.recoveryChunkKey(id, digest, 0)
	require.NoError(t, kerr)
	require.NoError(t, s.client.PutBins(policy, chunkKey, aerospike.NewBin("data", data)))
	put(aerospike.NewBin("recovery", digest), aerospike.NewBin("recoveryN", 1), aerospike.NewBin("recoveryPhase", nil))
	load("missing conflict recovery phase")

	// Damaged cleanup references stop the deletion and keep the root.
	put(aerospike.NewBin("recoveryRefs", []interface{}{7}))
	require.ErrorContains(t, s.CompleteConflictIntent(ctx, id), "invalid conflict recovery cleanup reference")
	put(aerospike.NewBin("recoveryRefs", []interface{}{"a:b:c"}))
	require.ErrorContains(t, s.CompleteConflictIntent(ctx, id), "invalid conflict recovery cleanup reference")
	put(aerospike.NewBin("recoveryRefs", []interface{}{"a:zero"}))
	require.ErrorContains(t, s.CompleteConflictIntent(ctx, id), "invalid conflict recovery cleanup count")

	// A root marked done is finished by the next load and by the next scan.
	put(aerospike.NewBin("recoveryRefs", []interface{}{digest + ":1"}), aerospike.NewBin("recoveryDone", true))
	r, lerr := s.LoadConflictRecovery(ctx, id)
	require.NoError(t, lerr)
	require.Nil(t, r)
	r, lerr = s.LoadConflictRecovery(ctx, id)
	require.NoError(t, lerr)
	require.Nil(t, r, "the record is gone")
}
