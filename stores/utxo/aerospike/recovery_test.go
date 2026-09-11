package aerospike_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	as "github.com/bsv-blockchain/aerospike-client-go/v8"
	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	rr "github.com/bsv-blockchain/teranode/pkg/replayrecovery"
	recovery "github.com/bsv-blockchain/teranode/stores/utxo/aerospike"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/bsv-blockchain/teranode/util/uaerospike"
	"github.com/stretchr/testify/require"
)

func TestRecoveryConditionalMarkersAndReplay(t *testing.T) {
	if testing.Short() {
		t.Skip("requires Aerospike")
	}
	settings := test.CreateBaseTestSettings(t)
	client, store, ctx, cleanup := initAerospike(t, settings, ulogger.NewErrorTestLogger(t))
	t.Cleanup(cleanup)
	backend, err := recovery.NewRecoveryBackend(client.Client, store.GetNamespace(), store.GetName(), settings.UtxoStore.UtxoBatchSize)
	require.NoError(t, err)
	parent := bt.NewTx()
	require.NoError(t, parent.From("1111111111111111111111111111111111111111111111111111111111111111", 0, "51", 10000))
	require.NoError(t, parent.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 4000))
	require.NoError(t, parent.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 4000))
	_, err = store.Create(ctx, parent, 1000)
	require.NoError(t, err)
	child := newChildSpendingOutput(t, parent, 0, 1)
	_, _, err = store.SpendAndCreate(ctx, child, 1000)
	require.NoError(t, err)
	// Simulate lost-marker deletion followed by recreation, preserving parent spend.
	ck, err := as.NewKey(store.GetNamespace(), store.GetName(), child.TxIDChainHash().CloneBytes())
	require.NoError(t, err)
	_, err = client.Delete(nil, ck)
	require.NoError(t, err)
	_, _, err = store.SpendAndCreate(ctx, child, 1001)
	require.NoError(t, err)
	pk, err := as.NewKey(store.GetNamespace(), store.GetName(), parent.TxIDChainHash().CloneBytes())
	require.NoError(t, err)
	wp := as.NewWritePolicy(0, 3600)
	require.NoError(t, client.Put(wp, pk, as.BinMap{"audit": map[interface{}]interface{}{int(7): []interface{}{[]byte{0, 255}, int64(9007199254740993), true}}}))
	local, err := backend.Transaction(ctx, child.TxID())
	require.NoError(t, err)
	require.Equal(t, child.Bytes(), local.Bytes())
	parents, err := backend.Inputs(ctx, child.TxID())
	require.NoError(t, err)
	require.Equal(t, []string{parent.TxID()}, parents)
	before, err := backend.Snapshot(ctx, child)
	require.NoError(t, err)
	require.Len(t, before.Parents, 1)
	require.Greater(t, before.Parents[0].Record.ExpiresAt, time.Now().Unix())
	require.NoError(t, client.Put(as.NewWritePolicy(0, as.TTLDontUpdate), pk, as.BinMap{"changed": true}))
	_, err = backend.Mark(ctx, before.Parents[0].Record, child.TxID())
	require.Error(t, err)
	rec, err := backend.Read(ctx, before.Records[0].Key)
	require.NoError(t, err)
	require.NotNil(t, rec)
	before, err = backend.Snapshot(ctx, child)
	require.NoError(t, err)
	marked, err := backend.Mark(ctx, before.Parents[0].Record, child.TxID())
	require.NoError(t, err)
	require.NoError(t, client.Put(as.NewWritePolicy(0, as.TTLDontUpdate), pk, as.BinMap{"extraMutation": true}))
	diverged, err := backend.Read(ctx, marked.Key)
	require.NoError(t, err)
	require.False(t, backend.MatchesMarked(before.Parents[0].Record, *diverged, child.TxID()))
	require.True(t, backend.HasMarker(marked, child.TxID()))
	require.True(t, backend.MatchesMarked(before.Parents[0].Record, marked, child.TxID()))
	require.Equal(t, before.Parents[0].Record.ExpiresAt, marked.ExpiresAt)
	require.NoError(t, backend.Delete(ctx, before.Records[0]))
	rec, err = backend.Read(ctx, before.Records[0].Key)
	require.NoError(t, err)
	require.Nil(t, rec)
	_, _, err = store.SpendAndCreate(ctx, child, 1002)
	require.ErrorContains(t, err, "invalid spend")
	rec, err = backend.Read(ctx, before.Records[0].Key)
	require.NoError(t, err)
	require.Nil(t, rec)
	parentAfter, err := client.Get(nil, pk)
	require.NoError(t, err)
	require.Equal(t, true, parentAfter.Bins["changed"])
	require.NotNil(t, parentAfter.Bins[fields.Utxos.String()])
	require.NoError(t, backend.VerifyParent(ctx, before.Parents[0]))
	outputs := parentAfter.Bins[fields.Utxos.String()].([]interface{})
	original := append([]byte(nil), outputs[0].([]byte)...)
	changed := append([]byte(nil), original...)
	changed[32] ^= 1
	outputs[0] = changed
	require.NoError(t, client.Put(as.NewWritePolicy(0, as.TTLDontUpdate), pk, as.BinMap{fields.Utxos.String(): outputs}))
	require.Error(t, backend.VerifyParent(ctx, before.Parents[0]), "marker alone must not hide changed confirmed spend owner")
	outputs[0] = original
	require.NoError(t, client.Put(as.NewWritePolicy(0, as.TTLDontUpdate), pk, as.BinMap{fields.Utxos.String(): outputs}))
	require.NoError(t, backend.VerifyParent(ctx, before.Parents[0]))

}

func TestRecoveryPagesAndFailures(t *testing.T) {
	if testing.Short() {
		t.Skip("requires Aerospike")
	}
	settings := test.CreateBaseTestSettings(t)
	settings.UtxoStore.UtxoBatchSize = 2
	client, store, ctx, cleanup := initAerospike(t, settings, ulogger.NewErrorTestLogger(t))
	t.Cleanup(cleanup)
	backend, err := recovery.NewRecoveryBackend(client.Client, store.GetNamespace(), store.GetName(), 2)
	require.NoError(t, err)
	parent := bt.NewTx()
	require.NoError(t, parent.From("2222222222222222222222222222222222222222222222222222222222222222", 0, "51", 50000))
	for i := 0; i < 4; i++ {
		require.NoError(t, parent.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 10000))
	}
	_, err = store.Create(ctx, parent, 1000)
	require.NoError(t, err)
	otherParent := bt.NewTx()
	require.NoError(t, otherParent.From("3333333333333333333333333333333333333333333333333333333333333333", 0, "51", 20000))
	require.NoError(t, otherParent.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 10000))
	_, err = store.Create(ctx, otherParent, 1000)
	require.NoError(t, err)
	child := bt.NewTx()
	for _, vout := range []uint32{0, 2} {
		require.NoError(t, child.From(parent.TxID(), vout, parent.Outputs[vout].LockingScript.String(), parent.Outputs[vout].Satoshis))
	}
	require.NoError(t, child.From(otherParent.TxID(), 0, otherParent.Outputs[0].LockingScript.String(), otherParent.Outputs[0].Satoshis))
	for i := 0; i < 3; i++ {
		require.NoError(t, child.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 1000))
	}
	_, _, err = store.SpendAndCreate(ctx, child, 1000)
	require.NoError(t, err)
	snapshot, err := backend.Snapshot(ctx, child)
	require.NoError(t, err)
	require.Len(t, snapshot.Records, 2)
	require.Len(t, snapshot.Parents, 3)
	// External inputs cannot silently produce an incomplete dependency graph.
	_, err = backend.Inputs(ctx, child.TxID())
	require.ErrorContains(t, err, "raw inputs unavailable")
	visited := map[string]int{}
	require.NoError(t, backend.Scan(ctx, func(id string) error { visited[id]++; return nil }))
	require.Equal(t, 1, visited[parent.TxID()])
	require.Equal(t, 1, visited[child.TxID()])
	masterKey, err := as.NewKey(store.GetNamespace(), store.GetName(), snapshot.Records[0].Key)
	require.NoError(t, err)
	master, err := client.Get(nil, masterKey)
	require.NoError(t, err)
	for _, tc := range []struct {
		name  string
		field fields.FieldName
		value interface{}
	}{
		{"wrong ID", fields.TxID, make([]byte, 32)},
		{"wrong page count", fields.TotalExtraRecs, 2},
		{"mined", fields.BlockIDs, []interface{}{1}},
		{"malformed spend", fields.Utxos, []interface{}{[]byte{1}, []byte{2}}},
		{"creating", fields.Creating, true},
		{"coinbase mismatch", fields.IsCoinbase, true},
		{"size mismatch", fields.SizeInBytes, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.NoError(t, client.Put(nil, masterKey, as.BinMap{tc.field.String(): tc.value}))
			_, err := backend.Snapshot(ctx, child)
			require.Error(t, err)
			require.NoError(t, client.Put(nil, masterKey, as.BinMap{tc.field.String(): master.Bins[tc.field.String()]}))
		})
	}
	snapshot, err = backend.Snapshot(ctx, child)
	require.NoError(t, err)
	parentPageKey, err := as.NewKey(store.GetNamespace(), store.GetName(), snapshot.Parents[1].Record.Key)
	require.NoError(t, err)
	parentPage, err := client.Get(nil, parentPageKey)
	require.NoError(t, err)
	_, err = client.Delete(nil, parentPageKey)
	require.NoError(t, err)
	_, err = backend.Snapshot(ctx, child)
	require.ErrorContains(t, err, "required recovery record missing")
	_, err = backend.Mark(ctx, snapshot.Parents[1].Record, child.TxID())
	require.Error(t, err)
	absent, err := backend.Read(ctx, snapshot.Parents[1].Record.Key)
	require.NoError(t, err)
	require.Nil(t, absent, "marker must not recreate missing parent")
	require.NoError(t, client.Put(nil, parentPageKey, parentPage.Bins))
	snapshot, err = backend.Snapshot(ctx, child)
	require.NoError(t, err)
	for _, p := range snapshot.Parents {
		_, err := backend.Mark(ctx, p.Record, child.TxID())
		require.NoError(t, err)
	}
	// A changed child is never deleted using a stale generation.
	require.NoError(t, client.Put(nil, masterKey, as.BinMap{"changed": true}))
	require.Error(t, backend.Delete(ctx, snapshot.Records[0]))
	current, err := backend.Read(ctx, snapshot.Records[0].Key)
	require.NoError(t, err)
	require.NotNil(t, current)
	require.NoError(t, backend.Delete(ctx, snapshot.Records[1]))
	require.NoError(t, backend.Delete(ctx, *current))
	// Inventory remains usable after master deletion; each addressed parent blocks replay.
	for _, r := range snapshot.Records {
		current, err := backend.Read(ctx, r.Key)
		require.NoError(t, err)
		require.Nil(t, current)
	}
	_, _, err = store.SpendAndCreate(ctx, child, 1001)
	require.ErrorContains(t, err, "invalid spend")
}

func TestRecoveryScanReadOnlyAndComplete(t *testing.T) {
	if testing.Short() {
		t.Skip("requires Aerospike")
	}
	settings := test.CreateBaseTestSettings(t)
	client, store, ctx, cleanup := initAerospike(t, settings, ulogger.NewErrorTestLogger(t))
	t.Cleanup(cleanup)
	backend, err := recovery.NewRecoveryBackend(client.Client, store.GetNamespace(), store.GetName(), 128, "recovery-test:3000")
	require.NoError(t, err)
	for i := 0; i < 259; i++ {
		hash := chainhash.HashH([]byte(fmt.Sprintf("scan-%d", i)))
		key, err := as.NewKey(store.GetNamespace(), store.GetName(), hash.CloneBytes())
		require.NoError(t, err)
		require.NoError(t, client.Put(as.NewWritePolicy(0, 3600), key, as.BinMap{fields.TxID.String(): hash.CloneBytes(), fields.TotalExtraRecs.String(): 0, fields.UnminedSince.String(): 1, fields.BlockIDs.String(): []interface{}{}}))
	}
	hash := chainhash.HashH([]byte("scan-0"))
	before, err := backend.Read(ctx, hash.CloneBytes())
	require.NoError(t, err)
	count := 0
	require.NoError(t, backend.Scan(ctx, func(string) error { count++; return nil }))
	require.Equal(t, 259, count)
	after, err := backend.Read(ctx, hash.CloneBytes())
	require.NoError(t, err)
	require.True(t, backend.Equal(*before, *after), "discovery must preserve bins, generation, expiration")
	stopped := errors.NewError("stop scan")
	require.ErrorIs(t, backend.Scan(ctx, func(string) error { return stopped }), stopped)
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	require.ErrorIs(t, backend.Scan(canceled, func(string) error { return nil }), context.Canceled)
	// A master with missing mined/unmined metadata remains visible to audit.
	key, err := as.NewKey(store.GetNamespace(), store.GetName(), hash.CloneBytes())
	require.NoError(t, err)
	require.NoError(t, client.Put(nil, key, as.BinMap{fields.TotalExtraRecs.String(): nil, fields.UnminedSince.String(): nil, fields.BlockIDs.String(): nil}))
	count = 0
	require.NoError(t, backend.Scan(ctx, func(string) error { count++; return nil }))
	require.Equal(t, 259, count)
}

func TestRecoveryCensusAndAbsentSnapshot(t *testing.T) {
	if testing.Short() {
		t.Skip("requires Aerospike")
	}
	settings := test.CreateBaseTestSettings(t)
	settings.UtxoStore.UtxoBatchSize = 2
	client, store, ctx, cleanup := initAerospike(t, settings, ulogger.NewErrorTestLogger(t))
	t.Cleanup(cleanup)
	backend, err := recovery.NewRecoveryBackend(client.Client, store.GetNamespace(), store.GetName(), 2)
	require.NoError(t, err)
	parent := bt.NewTx()
	require.NoError(t, parent.From("5555555555555555555555555555555555555555555555555555555555555555", 0, "51", 50000))
	for range 4 {
		require.NoError(t, parent.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 10000))
	}
	_, err = store.Create(ctx, parent, 1000)
	require.NoError(t, err)
	child := newChildSpendingOutput(t, parent, 2, 3)
	for range 2 {
		require.NoError(t, child.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 1000))
	}
	_, _, err = store.SpendAndCreate(ctx, child, 1000)
	require.NoError(t, err)
	pk, err := as.NewKey(store.GetNamespace(), store.GetName(), parent.TxIDChainHash().CloneBytes())
	require.NoError(t, err)
	require.NoError(t, client.Put(nil, pk, as.BinMap{fields.UnminedSince.String(): nil, fields.BlockIDs.String(): []interface{}{1}, fields.BlockHeights.String(): []interface{}{1}, fields.SubtreeIdxs.String(): []interface{}{0}}))
	var inventory []rr.InventoryRecord
	require.NoError(t, backend.Inventory(ctx, func(r rr.InventoryRecord) error { inventory = append(inventory, r); return nil }))
	require.Len(t, inventory, 4)
	for _, r := range inventory {
		require.Empty(t, r.Reason)
		require.NotEmpty(t, r.Record.Data)
		if r.TxID == parent.TxID() && r.Master {
			require.False(t, r.Candidate)
		}
	}
	var refs []rr.SpendReference
	require.NoError(t, backend.SpendReferences(ctx, func(r rr.SpendReference) error { refs = append(refs, r); return nil }))
	require.Len(t, refs, 1)
	require.Equal(t, parent.TxID(), refs[0].ParentTxID)
	require.Equal(t, child.TxID(), refs[0].ChildTxID)
	require.Equal(t, uint32(2), refs[0].Vout)
	require.Equal(t, uint32(0), refs[0].Vin)
	require.False(t, refs[0].Marked)
	require.Equal(t, uaerospike.CalculateKeySourceInternal(parent.TxIDChainHash(), 1), refs[0].ParentKey)
	_, err = backend.SnapshotAbsent(ctx, child)
	require.Error(t, err)
	snapshot, err := backend.Snapshot(ctx, child)
	require.NoError(t, err)
	// A surviving page is not an absent child.
	require.NoError(t, backend.Delete(ctx, snapshot.Records[0]))
	_, err = backend.SnapshotAbsent(ctx, child)
	require.Error(t, err)
	require.NoError(t, backend.Delete(ctx, snapshot.Records[1]))
	absent, err := backend.SnapshotAbsent(ctx, child)
	require.NoError(t, err)
	require.Empty(t, absent.Records)
	require.Len(t, absent.Parents, 2)
	for _, p := range absent.Parents {
		if len(p.Record.Key) == 36 {
			_, err = backend.Mark(ctx, p.Record, child.TxID())
			require.NoError(t, err)
		}
	}
	require.NoError(t, backend.SpendReferences(ctx, func(r rr.SpendReference) error {
		require.False(t, r.Marked, "master marker is also required")
		return nil
	}))
	for _, p := range absent.Parents {
		if len(p.Record.Key) == 32 {
			_, err = backend.Mark(ctx, p.Record, child.TxID())
			require.NoError(t, err)
		}
	}
	refs = nil
	require.NoError(t, backend.SpendReferences(ctx, func(r rr.SpendReference) error { refs = append(refs, r); return nil }))
	require.Len(t, refs, 1)
	require.True(t, refs[0].Marked)
	// Malformed replay markers must be visible without losing spend edges.
	markerKey, err := as.NewKey(store.GetNamespace(), store.GetName(), uaerospike.CalculateKeySourceInternal(parent.TxIDChainHash(), 1))
	require.NoError(t, err)
	require.NoError(t, client.Put(nil, markerKey, as.BinMap{fields.DeletedChildren.String(): "corrupt"}))
	require.NoError(t, backend.Inventory(ctx, func(r rr.InventoryRecord) error {
		if r.TxID == parent.TxID() && r.Page == 1 {
			require.Contains(t, r.Reason, "deletedChildren")
		}
		return nil
	}))
	refs = nil
	require.NoError(t, backend.SpendReferences(ctx, func(r rr.SpendReference) error { refs = append(refs, r); return nil }))
	require.Len(t, refs, 1)
	require.False(t, refs[0].Marked)
	require.NoError(t, client.Put(nil, markerKey, as.BinMap{fields.DeletedChildren.String(): map[interface{}]interface{}{child.TxID(): true}}))
	// Locked records remain census members and explicitly block repair.
	require.NoError(t, client.Put(nil, pk, as.BinMap{fields.Locked.String(): true}))
	require.NoError(t, backend.Inventory(ctx, func(r rr.InventoryRecord) error {
		if r.Master {
			require.True(t, r.Candidate)
			require.Contains(t, r.Reason, "locked")
		}
		return nil
	}))
	_, err = backend.SnapshotAbsent(ctx, child)
	require.ErrorContains(t, err, "locked")
	require.NoError(t, client.Put(nil, pk, as.BinMap{fields.Locked.String(): false}))
	for _, flag := range []fields.FieldName{fields.Conflicting, fields.Creating} {
		require.NoError(t, client.Put(nil, pk, as.BinMap{flag.String(): true}))
		require.NoError(t, backend.Inventory(ctx, func(r rr.InventoryRecord) error {
			if r.Master {
				require.True(t, r.Candidate)
				require.Contains(t, r.Reason, flag.String())
			}
			return nil
		}))
		_, err = backend.SnapshotAbsent(ctx, child)
		require.Error(t, err)
		require.NoError(t, client.Put(nil, pk, as.BinMap{flag.String(): false}))
	}
	require.NoError(t, client.Put(as.NewWritePolicy(0, 3600), pk, as.BinMap{"finite": true}))
	_, err = backend.SnapshotAbsent(ctx, child)
	require.ErrorContains(t, err, "finite")
	require.NoError(t, client.Put(as.NewWritePolicy(0, as.TTLDontExpire), pk, as.BinMap{"finite": nil}))
	unknown, err := as.NewKey(store.GetNamespace(), store.GetName(), make([]byte, 32))
	require.NoError(t, err)
	require.NoError(t, client.Put(nil, unknown, as.BinMap{fields.TxID.String(): parent.TxIDChainHash().CloneBytes()}))
	findings := 0
	require.NoError(t, backend.Inventory(ctx, func(r rr.InventoryRecord) error {
		if r.TxID == "" {
			findings++
			require.Contains(t, r.Reason, "ownership")
			require.Equal(t, unknown.Digest(), r.Record.Key)
		}
		return nil
	}))
	require.Equal(t, 1, findings)
	stop := errors.NewProcessingError("stop census")
	require.ErrorIs(t, backend.Inventory(ctx, func(rr.InventoryRecord) error { return stop }), stop)
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	require.ErrorIs(t, backend.Inventory(cancelled, func(rr.InventoryRecord) error { t.Fatal("unexpected callback"); return nil }), context.Canceled)
	// Ownerless pages remain visible after their master disappears.
	_, err = client.Delete(nil, pk)
	require.NoError(t, err)
	ownerless := 0
	require.NoError(t, backend.Inventory(ctx, func(r rr.InventoryRecord) error {
		require.Contains(t, r.Reason, "master is absent")
		ownerless++
		return nil
	}))
	require.Equal(t, 2, ownerless)
}
