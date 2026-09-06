package aerospike_test

import (
	"context"
	"testing"
	"time"

	"github.com/bsv-blockchain/aerospike-client-go/v8"
	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/stores/blob"
	"github.com/bsv-blockchain/teranode/stores/blob/options"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	teranode_aerospike "github.com/bsv-blockchain/teranode/stores/utxo/aerospike"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/bsv-blockchain/teranode/stores/utxo/meta"
	"github.com/bsv-blockchain/teranode/stores/utxo/tests"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/bsv-blockchain/teranode/util/uaerospike"
	"github.com/stretchr/testify/require"
)

// These three cases pin the master-first cascade of DeleteComplete on the only
// backend that paginates. They are deliberately NOT behind the `aerospike` build
// tag: nothing in CI compiles a tagged file in this package (make test runs
// -tags "testtxmetacache", and the aerospike tag is applied only by
// test/scripts/run_tests_sequentially.sh, which collects ./test/sequentialtest).
// The gate that already covers this package is the TestContainers skip in
// initAerospike, which is what aerospike_server_test.go relies on too.

// deleteCompleteUnlockingScript is a minimal unlocking script for the test
// transactions below. The store spends by UTXO hash and never verifies a script,
// so this is enough. Local copy of the tests package's unexported equivalent.
var deleteCompleteUnlockingScript = bscript.NewFromBytes([]byte{0x00, 0x48, 0x30, 0x45})

// deleteCompleteAddress is the throwaway payout address the helpers below use.
const deleteCompleteAddress = "1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa"

// paginatedSpendAndCreateTx builds an extended transaction spending output vout
// of tests.Tx and paying numOutputs addresses, so the record paginates on the
// Aerospike backend. Local copy of the tests-package helper, which is unexported;
// satoshis makes the txid unique per case, and the remaining outputs carry one
// satoshi each.
func paginatedSpendAndCreateTx(t *testing.T, vout uint32, satoshis uint64, numOutputs int) *bt.Tx {
	t.Helper()

	tx := bt.NewTx()
	require.NoError(t, tx.FromUTXOs(&bt.UTXO{
		TxIDHash:      tests.Tx.TxIDChainHash(),
		Vout:          vout,
		LockingScript: tests.Tx.Outputs[vout].LockingScript,
		Satoshis:      tests.Tx.Outputs[vout].Satoshis,
	}))
	tx.Inputs[0].UnlockingScript = deleteCompleteUnlockingScript

	require.NoError(t, tx.PayToAddress(deleteCompleteAddress, satoshis))

	for i := 1; i < numOutputs; i++ {
		require.NoError(t, tx.PayToAddress(deleteCompleteAddress, 1))
	}

	return tx
}

// spendChildOutputStatus spends output vout of child and reports how the store
// answered: refused as TX_LOCKED, refused because the parent record is missing,
// or accepted. Backends surface a refusal either on the aggregate error or on the
// per-spend Err, so both are checked. It leaves no residue either way.
//
// All three outcomes are returned because the residue case has to distinguish
// them: "rejected" is not enough — WHICH rejection it is decides whether the
// residue is safe (TX_LOCKED) or merely absent (TX_NOT_FOUND).
func spendChildOutputStatus(t *testing.T, ctx context.Context, db utxo.Store, child *bt.Tx, vout uint32) (locked, missingParent, accepted bool) {
	t.Helper()

	gc := bt.NewTx()
	require.NoError(t, gc.FromUTXOs(&bt.UTXO{
		TxIDHash:      child.TxIDChainHash(),
		Vout:          vout,
		LockingScript: child.Outputs[vout].LockingScript,
		Satoshis:      child.Outputs[vout].Satoshis,
	}))
	gc.Inputs[0].UnlockingScript = deleteCompleteUnlockingScript
	require.NoError(t, gc.PayToAddress(deleteCompleteAddress, child.Outputs[vout].Satoshis))

	_ = db.DeleteComplete(ctx, gc.TxIDChainHash())

	_, spends, err := db.SpendAndCreate(ctx, gc, db.GetBlockHeight()+1)

	locked = errors.Is(err, errors.ErrTxLocked)
	missingParent = errors.Is(err, errors.ErrTxNotFound)
	spendFailed := err != nil

	for _, s := range spends {
		if s == nil || s.Err == nil {
			continue
		}

		spendFailed = true

		if errors.Is(s.Err, errors.ErrTxLocked) {
			locked = true
		}

		if errors.Is(s.Err, errors.ErrTxNotFound) {
			missingParent = true
		}
	}

	accepted = !spendFailed

	_ = db.DeleteComplete(ctx, gc.TxIDChainHash())

	return locked, missingParent, accepted
}

// masterChildCount reads TotalExtraRecs straight off the master record with the
// raw client, which is the only way to see the pagination fan-out from outside
// the store.
func masterChildCount(t *testing.T, client *uaerospike.Client, store *teranode_aerospike.Store, hash *chainhash.Hash) int {
	t.Helper()

	tSettings := test.CreateBaseTestSettings(t)

	key, err := aerospike.NewKey(store.GetNamespace(), store.GetName(), hash[:])
	require.NoError(t, err)

	rec, aErr := client.Get(util.GetAerospikeReadPolicy(tSettings), key, fields.TotalExtraRecs.String())
	require.Nil(t, aErr)
	require.NotNil(t, rec)

	count, ok := rec.Bins[fields.TotalExtraRecs.String()].(int)
	require.True(t, ok, "the master record must carry TotalExtraRecs")

	return count
}

// childRecordExists reports whether pagination record `index` of hash is present.
func childRecordExists(t *testing.T, client *uaerospike.Client, store *teranode_aerospike.Store, hash *chainhash.Hash, index uint32) bool {
	t.Helper()

	tSettings := test.CreateBaseTestSettings(t)

	key, err := aerospike.NewKey(store.GetNamespace(), store.GetName(), uaerospike.CalculateKeySourceInternal(hash, index))
	require.NoError(t, err)

	_, aErr := client.Get(util.GetAerospikeReadPolicy(tSettings), key)
	if aErr != nil {
		require.True(t, errors.Is(aErr, aerospike.ErrKeyNotFound), "unexpected error reading pagination record %d: %v", index, aErr)
		return false
	}

	return true
}

// deleteChildRecordsByHand removes pagination records 1..count with the raw
// client, which is how the state a half-completed cascade used to leave is
// reproduced without having to make the store fail on cue.
func deleteChildRecordsByHand(t *testing.T, client *uaerospike.Client, store *teranode_aerospike.Store, hash *chainhash.Hash, from, to int) {
	t.Helper()

	tSettings := test.CreateBaseTestSettings(t)

	for i := from; i <= to; i++ {
		key, err := aerospike.NewKey(store.GetNamespace(), store.GetName(), uaerospike.CalculateKeySourceInternal(hash, uint32(i))) // nolint:gosec
		require.NoError(t, err)

		_, aErr := client.Delete(util.GetAerospikeWritePolicy(tSettings, 0), key)
		require.Nil(t, aErr)
	}
}

// deleteMasterRecordByHand removes ONLY the master record, leaving every
// pagination child in place — the residue a cascade that failed at its child step
// leaves behind.
func deleteMasterRecordByHand(t *testing.T, client *uaerospike.Client, store *teranode_aerospike.Store, hash *chainhash.Hash) {
	t.Helper()

	tSettings := test.CreateBaseTestSettings(t)

	key, err := aerospike.NewKey(store.GetNamespace(), store.GetName(), hash[:])
	require.NoError(t, err)

	_, aErr := client.Delete(util.GetAerospikeWritePolicy(tSettings, 0), key)
	require.Nil(t, aErr)
}

// unminedIteratorHas walks GetUnminedTxIterator once and reports whether hash is
// enumerated. This is the "would block assembly mine it" question, asked the way
// the unmined reload asks it.
func unminedIteratorHas(t *testing.T, ctx context.Context, store *teranode_aerospike.Store, hash *chainhash.Hash) bool {
	t.Helper()

	it, err := store.GetUnminedTxIterator()
	require.NoError(t, err)

	defer func() {
		require.NoError(t, it.Close())
	}()

	for {
		batch, err := it.Next(ctx)
		require.NoError(t, err)

		if len(batch) == 0 {
			return false
		}

		for _, unmined := range batch {
			if unmined != nil && unmined.Node != nil && unmined.Hash.IsEqual(hash) {
				return true
			}
		}
	}
}

// waitUnminedIteratorHas polls unminedIteratorHas until it reports true or the
// budget runs out. Aerospike builds the unmined_since secondary index the
// iterator ranges over asynchronously after the store is created, so a single
// walk immediately after the first write can legitimately come back empty. It
// exists so the NEGATIVE assertions below mean "it left the iterator", not "the
// iterator never worked".
func waitUnminedIteratorHas(t *testing.T, ctx context.Context, store *teranode_aerospike.Store, hash *chainhash.Hash, within time.Duration) bool {
	t.Helper()

	deadline := time.Now().Add(within)

	for {
		if unminedIteratorHas(t, ctx, store, hash) {
			return true
		}

		if time.Now().After(deadline) {
			return false
		}

		time.Sleep(200 * time.Millisecond)
	}
}

// delFailingBlobStore fails every Del and delegates everything else, so the LAST
// step of the cascade can be made to fail against an otherwise real store. Del is
// the only step of the three the store exposes an injection point for.
type delFailingBlobStore struct {
	blob.Store

	err error
}

func (b *delFailingBlobStore) Del(context.Context, []byte, fileformat.FileType, ...options.FileOption) error {
	return b.err
}

// TestDeleteComplete_HalfCompletedCascadeLeavesNoMineableRecord is the regression
// test for the finding this round is about: on the old children-first order, ANY
// failure in the cascade left the master record alive with dead or partly-dead
// pagination children — a transaction Get returns, the unmined iterator
// enumerates, the reload unlocks and block assembly will mine, while a spend of
// any output above utxostore_utxoBatchSize answers TX_NOT_FOUND for good, and
// nothing in the node recreates a pagination child.
//
// What it proves and what it does not: the failure is injected at step 3 (the
// external blob), because that is the only one of the three steps the store
// exposes an injection point for. It is a REAL failure of the shipped cascade,
// not a hand-built state. A failure at step 2 (the child batch) leaves orphan
// children instead; that state is covered by
// TestDeleteComplete_OrphanChildResidueIsUnreachableNotMineable below.
func TestDeleteComplete_HalfCompletedCascadeLeavesNoMineableRecord(t *testing.T) {
	logger := ulogger.NewErrorTestLogger(t)
	tSettings := test.CreateBaseTestSettings(t)

	client, store, ctx, deferFn := initAerospike(t, tSettings, logger)
	t.Cleanup(deferFn)

	utxoBatchSize := tSettings.UtxoStore.UtxoBatchSize
	require.Positive(t, utxoBatchSize)

	// The parent whose output the paginated child spends.
	_, _, err := store.SpendAndCreate(ctx, tests.Tx, 1000, utxo.WithCreateOnly())
	require.NoError(t, err)

	child := paginatedSpendAndCreateTx(t, 4, 6001, utxoBatchSize+72)
	hash := child.TxIDChainHash()

	_ = store.DeleteComplete(ctx, hash)

	// The block-assembly shape: spend the input, create the paginated record Locked.
	_, _, err = store.SpendAndCreate(ctx, child, store.GetBlockHeight()+1, utxo.WithLocked(true))
	require.NoError(t, err)

	require.Positive(t, masterChildCount(t, client, store, hash), "precondition: the record really paginates")
	require.True(t, waitUnminedIteratorHas(t, ctx, store, hash, 30*time.Second),
		"precondition: before the delete the record IS enumerated by the unmined iterator, so the negative assertion below means something")

	// Make the last step of the cascade fail, wrapping the store's own external
	// store so reads still see the blob that was written.
	healthy := store.GetExternalStore()
	store.SetExternalStore(&delFailingBlobStore{Store: healthy, err: errors.NewStorageError("blob store unavailable")})

	require.Error(t, store.DeleteComplete(ctx, hash), "the cascade fails at its last step")

	// The gate assertions: whatever the cascade left behind, it is not mineable.
	require.ErrorIs(t, store.GetMeta(ctx, hash, &meta.Data{}), errors.ErrTxNotFound,
		"no readable master record survives a failed cascade")
	require.False(t, childRecordExists(t, client, store, hash, 1),
		"the pagination children went with the master, so no unspendable high-numbered outputs are left addressable")
	require.False(t, unminedIteratorHas(t, ctx, store, hash),
		"the record must be out of the unmined iterator, which is what stops block assembly lifting it into a template")

	// A retry after the failure does not error — but it does NOT finish the job
	// either: the master is already gone, so it returns nil at the master read
	// without reaching the orphan blob. That is the documented cost of master-first,
	// not something the retry papers over.
	store.SetExternalStore(healthy)
	require.NoError(t, store.DeleteComplete(ctx, hash),
		"a retry after a failed cascade must not error on the work the first attempt already did")
}

// TestDeleteComplete_AbsentChildrenAreSuccess exercises the cascade's documented
// tolerance of missing pagination records, which nothing covered before. It is
// also the shape a cascade that died between steps used to leave behind, so it is
// the state a retry has to converge on rather than error on.
func TestDeleteComplete_AbsentChildrenAreSuccess(t *testing.T) {
	logger := ulogger.NewErrorTestLogger(t)
	tSettings := test.CreateBaseTestSettings(t)

	client, store, ctx, deferFn := initAerospike(t, tSettings, logger)
	t.Cleanup(deferFn)

	utxoBatchSize := tSettings.UtxoStore.UtxoBatchSize
	require.Positive(t, utxoBatchSize)

	_, _, err := store.SpendAndCreate(ctx, tests.Tx, 1000, utxo.WithCreateOnly())
	require.NoError(t, err)

	cases := []struct {
		name       string
		vout       uint32
		satoshis   uint64
		numOutputs int
		// removeAll deletes every child by hand; otherwise only child 1 goes.
		removeAll bool
	}{
		{name: "all children already gone", vout: 4, satoshis: 6001, numOutputs: utxoBatchSize + 72, removeAll: true},
		{name: "one child already gone", vout: 3, satoshis: 6002, numOutputs: utxoBatchSize + 72, removeAll: false},
		{name: "single child transaction", vout: 2, satoshis: 6003, numOutputs: utxoBatchSize + 1, removeAll: true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			child := paginatedSpendAndCreateTx(t, tc.vout, tc.satoshis, tc.numOutputs)
			hash := child.TxIDChainHash()

			_ = store.DeleteComplete(ctx, hash)

			_, _, err := store.SpendAndCreate(ctx, child, store.GetBlockHeight()+1, utxo.WithLocked(true))
			require.NoError(t, err)

			childCount := masterChildCount(t, client, store, hash)
			require.Positive(t, childCount, "precondition: the record paginates")

			if tc.removeAll {
				deleteChildRecordsByHand(t, client, store, hash, 1, childCount)
			} else {
				deleteChildRecordsByHand(t, client, store, hash, 1, 1)
			}

			require.NoError(t, store.DeleteComplete(ctx, hash),
				"an absent pagination child is success, not an error: the cascade has to converge on the state a half-completed one left")

			require.ErrorIs(t, store.GetMeta(ctx, hash, &meta.Data{}), errors.ErrTxNotFound)
			require.False(t, childRecordExists(t, client, store, hash, uint32(childCount)), // nolint:gosec
				"the children that were still there were removed too")
		})
	}
}

// TestDeleteComplete_OrphanChildResidueIsUnreachableNotMineable pins the residue
// the master-first order deliberately trades INTO, so the design is measured on
// what it leaves as well as on what it removes.
//
// This state is not a defect: when the cascade fails after the master is gone,
// the pagination children survive as locked orphans. The trade it encodes is
// unreachable residue over wrongly reachable residue — the old order's failure
// left a record the node would MINE and then refuse to serve the outputs of,
// which is strictly worse than a record the node no longer has and refuses
// spends against.
//
// The lock on the orphans is the safety property, not the cost: an UNLOCKED
// orphan child would hold an unspent, non-creating UTXO, so a descendant spending
// a high-numbered output of a transaction that no longer exists would be
// ACCEPTED. Assertion 3 is what makes that a fact rather than an argument.
func TestDeleteComplete_OrphanChildResidueIsUnreachableNotMineable(t *testing.T) {
	logger := ulogger.NewErrorTestLogger(t)
	tSettings := test.CreateBaseTestSettings(t)

	client, store, ctx, deferFn := initAerospike(t, tSettings, logger)
	t.Cleanup(deferFn)

	utxoBatchSize := tSettings.UtxoStore.UtxoBatchSize
	require.Positive(t, utxoBatchSize)

	highVout := uint32(utxoBatchSize + 20) // guaranteed to land on a pagination child

	_, _, err := store.SpendAndCreate(ctx, tests.Tx, 1000, utxo.WithCreateOnly())
	require.NoError(t, err)

	child := paginatedSpendAndCreateTx(t, 4, 6001, utxoBatchSize+72)
	hash := child.TxIDChainHash()

	_ = store.DeleteComplete(ctx, hash)

	_, spends, err := store.SpendAndCreate(ctx, child, store.GetBlockHeight()+1, utxo.WithLocked(true))
	require.NoError(t, err)
	require.Len(t, spends, 1)

	childCount := masterChildCount(t, client, store, hash)
	require.Positive(t, childCount)

	utxoHash4, err := util.UTXOHashFromOutput(tests.Tx.TxIDChainHash(), tests.Tx.Outputs[4], 4)
	require.NoError(t, err)

	parentOutpoint := &utxo.Spend{TxID: tests.Tx.TxIDChainHash(), Vout: 4, UTXOHash: utxoHash4}

	require.True(t, waitUnminedIteratorHas(t, ctx, store, hash, 30*time.Second),
		"precondition: before the delete the record IS enumerated by the unmined iterator")

	// Build the residue in the order unwindShed produces it.
	//
	// 1. the master goes first and the child pass fails, so the children survive;
	deleteMasterRecordByHand(t, client, store, hash)

	// 2. the verify-after-delete read is what gates the next step, and it now says
	//    "provably gone";
	require.ErrorIs(t, store.GetMeta(ctx, hash, &meta.Data{}), errors.ErrTxNotFound)

	// 3. so the unwind unspends. Without this the test would model a state the code
	//    never produces: the master being provably gone is exactly what licenses the
	//    unspend, so a residue whose parent input is still spent is hand-made.
	require.NoError(t, store.Unspend(ctx, spends))

	resp, err := store.GetSpend(ctx, parentOutpoint)
	require.NoError(t, err)
	require.NotEqual(t, int(utxo.Status_SPENT), resp.Status,
		"the inputs really are returned — the half of the fix the old order could not deliver")

	// Assertion 1 — nothing readable.
	require.ErrorIs(t, store.GetMeta(ctx, hash, &meta.Data{}), errors.ErrTxNotFound)

	// Assertion 2 — not mineable. This is the property the old order loses.
	require.False(t, unminedIteratorHas(t, ctx, store, hash),
		"orphan pagination children carry no unmined_since, so nothing can lift this into a mining template")

	// Assertion 3 — a descendant of a high-numbered output is REJECTED, never
	// accepted. If the orphan were left unlocked this spend would succeed, and the
	// node would accept a transaction spending an output of a transaction it does
	// not have.
	locked, _, accepted := spendChildOutputStatus(t, ctx, store, child, highVout)
	require.False(t, accepted, "a spend against orphan residue must never be accepted")
	require.True(t, locked, "the orphan's lock is what refuses it: TX_LOCKED is the residue's safety property, not just its cost")

	// Assertion 4 — a low-numbered output lived on the master, which really is
	// gone, so that spend gets the missing-parent answer instead.
	locked, missingParent, accepted := spendChildOutputStatus(t, ctx, store, child, 1)
	require.False(t, accepted)
	require.False(t, locked)
	require.True(t, missingParent, "the master is gone, so an output that lived on it answers missing-parent")

	// Assertion 5 — the documented limitation, asserted rather than hoped for: once
	// the master is gone a later DeleteComplete cannot enumerate the orphans (the
	// child count lived on the master), so it returns nil without touching them.
	// That is precisely why the child pass is retried INSIDE the cascade and why
	// the residue log line carries the child count.
	require.NoError(t, store.DeleteComplete(ctx, hash))

	locked, _, accepted = spendChildOutputStatus(t, ctx, store, child, highVout)
	require.False(t, accepted)
	require.True(t, locked, "a later complete delete does not reach the orphans; they are still there and still locked")

	// Assertion 6 — adoption. A re-create of the same txid (the same call the
	// validator makes) writes every record CREATE_ONLY: the master is recreated and
	// the surviving children are adopted. The unmined reload's SetLocked then clears
	// their stale locked bin, and the record is coherent again.
	_, _, err = store.SpendAndCreate(ctx, child, store.GetBlockHeight()+1, utxo.WithLocked(true))
	require.NoError(t, err, "a re-create of the same transaction adopts the orphan children")

	require.NoError(t, store.GetMeta(ctx, hash, &meta.Data{}), "the master record is readable again")
	require.Equal(t, childCount, masterChildCount(t, client, store, hash),
		"the recreated master reports the same pagination fan-out, so the adopted children are addressable")

	require.NoError(t, store.SetLocked(ctx, []chainhash.Hash{*hash}, false),
		"the unmined reload's unlock is what clears the adopted children's stale lock")

	_, _, accepted = spendChildOutputStatus(t, ctx, store, child, highVout)
	require.True(t, accepted, "after adoption and unlock a high-numbered output is spendable again")
}
