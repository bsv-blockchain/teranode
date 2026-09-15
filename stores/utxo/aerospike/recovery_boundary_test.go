package aerospike_test

import (
	"context"
	"strings"
	"testing"
	"time"

	as "github.com/bsv-blockchain/aerospike-client-go/v8"
	"github.com/bsv-blockchain/go-bt/v2"
	rr "github.com/bsv-blockchain/teranode/pkg/replayrecovery"
	recovery "github.com/bsv-blockchain/teranode/stores/utxo/aerospike"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/require"
)

// Corrupted persisted metadata must never become authority for a recovery write.
func TestRecoveryBoundaryCorruptRecords(t *testing.T) {
	if testing.Short() {
		t.Skip("requires Aerospike")
	}
	settings := test.CreateBaseTestSettings(t)
	client, store, ctx, cleanup := initAerospike(t, settings, ulogger.NewErrorTestLogger(t))
	t.Cleanup(cleanup)
	backend, err := recovery.NewRecoveryBackend(client.Client, store.GetNamespace(), store.GetName(), settings.UtxoStore.UtxoBatchSize)
	require.NoError(t, err)
	parent := bt.NewTx()
	require.NoError(t, parent.From(strings.Repeat("44", 32), 0, "51", 10000))
	for range 2 {
		require.NoError(t, parent.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 4000))
	}
	_, err = store.Create(ctx, parent, 1000)
	require.NoError(t, err)
	child := newChildSpendingOutput(t, parent, 0, 1)
	_, _, err = store.SpendAndCreate(ctx, child, 1000)
	require.NoError(t, err)
	snapshot, err := backend.Snapshot(ctx, child)
	require.NoError(t, err)
	childKey, err := as.NewKey(store.GetNamespace(), store.GetName(), snapshot.Records[0].Key)
	require.NoError(t, err)
	childRecord, err := client.Get(nil, childKey)
	require.NoError(t, err)
	parentKey, err := as.NewKey(store.GetNamespace(), store.GetName(), snapshot.Parents[0].Record.Key)
	require.NoError(t, err)
	parentRecord, err := client.Get(nil, parentKey)
	require.NoError(t, err)
	input := childRecord.Bins[fields.Inputs.String()].([]interface{})[0].([]byte)
	output := childRecord.Bins[fields.Outputs.String()].([]interface{})[0].([]byte)
	wrongInput := append([]byte(nil), input...)
	wrongInput[0] ^= 1
	wrongOutput := append([]byte(nil), output...)
	wrongOutput[0] ^= 1
	wrongUTXO := append([]byte(nil), childRecord.Bins[fields.Utxos.String()].([]interface{})[0].([]byte)...)
	wrongUTXO[0] ^= 1

	for _, tc := range []struct {
		name, operation, want string
		field                 fields.FieldName
		value                 interface{}
	}{
		{"input particle", "inputs", "malformed raw input", fields.Inputs, []interface{}{1}},
		{"truncated input", "inputs", "", fields.Inputs, []interface{}{[]byte{1}}},
		{"trailing input", "inputs", "trailing raw input bytes", fields.Inputs, []interface{}{append(append([]byte(nil), input...), 0)}},
		{"negative version", "transaction", "invalid transaction version", fields.Version, -1},
		{"overflow version", "transaction", "invalid transaction version", fields.Version, int64(1) << 32},
		{"negative locktime", "transaction", "invalid transaction locktime", fields.LockTime, -1},
		{"overflow locktime", "transaction", "invalid transaction locktime", fields.LockTime, int64(1) << 32},
		{"missing outputs", "transaction", "raw outputs unavailable", fields.Outputs, nil},
		{"output particle", "transaction", "malformed raw output", fields.Outputs, []interface{}{1}},
		{"truncated output", "transaction", "", fields.Outputs, []interface{}{[]byte{1}}},
		{"trailing output", "transaction", "trailing raw output bytes", fields.Outputs, []interface{}{append(append([]byte(nil), output...), 0)}},
		{"different transaction", "transaction", "raw transaction identity mismatch", fields.Outputs, []interface{}{wrongOutput}},
		{"unmined height", "snapshot", "unminedSince", fields.UnminedSince, -1},
		{"output count", "snapshot", "child output count mismatch", fields.TotalUtxos, 0},
		{"version mismatch", "snapshot", "child version mismatch", fields.Version, 7},
		{"locktime mismatch", "snapshot", "child locktime mismatch", fields.LockTime, 7},
		{"external flag", "snapshot", "malformed external flag", fields.External, false},
		{"input count", "snapshot", "child input count mismatch", fields.Inputs, []interface{}{}},
		{"input identity", "snapshot", "child input identity mismatch", fields.Inputs, []interface{}{wrongInput}},
		{"raw output count", "snapshot", "child raw outputs mismatch", fields.Outputs, []interface{}{}},
		{"raw output identity", "snapshot", "child raw output mismatch", fields.Outputs, []interface{}{wrongOutput}},
		{"UTXO count", "snapshot", "UTXO count mismatch", fields.RecordUtxos, 0},
		{"spent count", "snapshot", "spent UTXO count mismatch", fields.SpentUtxos, 1},
		{"UTXO page length", "snapshot", "UTXO page length mismatch", fields.Utxos, []interface{}{}},
		{"UTXO hash", "snapshot", "child UTXO hash mismatch", fields.Utxos, []interface{}{wrongUTXO}},
		{"marker particle", "mark", "malformed deletedChildren", fields.DeletedChildren, true},
		{"marker value", "mark", "malformed deletedChildren entry", fields.DeletedChildren, map[interface{}]interface{}{child.TxID(): false}},
		{"marker hash", "mark", "", fields.DeletedChildren, map[interface{}]interface{}{strings.Repeat("z", 64): true}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.NoError(t, client.Put(nil, childKey, as.BinMap{tc.field.String(): tc.value}))
			t.Cleanup(func() {
				require.NoError(t, client.Put(nil, childKey, as.BinMap{tc.field.String(): childRecord.Bins[tc.field.String()]}))
			})
			before, err := backend.Read(ctx, snapshot.Records[0].Key)
			require.NoError(t, err)
			switch tc.operation {
			case "inputs":
				_, err = backend.Inputs(ctx, child.TxID())
			case "transaction":
				_, err = backend.Transaction(ctx, child.TxID())
			case "snapshot":
				_, err = backend.Snapshot(ctx, child)
			case "mark":
				_, err = backend.Mark(ctx, *before, child.TxID())
			}
			require.Error(t, err)
			if tc.want != "" {
				require.ErrorContains(t, err, tc.want)
			}
			after, err := backend.Read(ctx, before.Key)
			require.NoError(t, err)
			require.Equal(t, before, after, "failed recovery must preserve data, generation, and expiry")
		})
	}

	for _, tc := range []struct {
		name, want string
		field      fields.FieldName
		value      interface{}
	}{
		{"parent output index", "parent output index mismatch", fields.TotalUtxos, 0},
		{"parent inventory", "parent page inventory mismatch", fields.TotalExtraRecs, 1},
		{"parent locked", "unsafe locked state", fields.Locked, true},
		{"parent page length", "UTXO page length mismatch", fields.Utxos, []interface{}{}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.NoError(t, client.Put(nil, parentKey, as.BinMap{tc.field.String(): tc.value}))
			t.Cleanup(func() {
				require.NoError(t, client.Put(nil, parentKey, as.BinMap{tc.field.String(): parentRecord.Bins[tc.field.String()]}))
			})
			_, err := backend.Snapshot(ctx, child)
			require.ErrorContains(t, err, tc.want)
		})
	}

	snapshot, err = backend.Snapshot(ctx, child)
	require.NoError(t, err)
	require.ErrorContains(t, backend.VerifyParent(ctx, snapshot.Parents[0]), "required parent replay marker missing")
	marked, err := backend.Mark(ctx, snapshot.Parents[0].Record, child.TxID())
	require.NoError(t, err)
	deadline, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	again, err := backend.Mark(deadline, marked, child.TxID())
	require.NoError(t, err)
	require.Equal(t, marked, again, "retry of a durable marker must not refresh TTL or increment generation")
	require.False(t, backend.MatchesMarked(marked, rr.Record{Key: marked.Key, Data: marked.Data, Generation: marked.Generation + 1, ExpiresAt: marked.ExpiresAt}, child.TxID()))
	require.NoError(t, backend.VerifyParent(ctx, snapshot.Parents[0]))
}

func TestRecoveryBoundaryInvalidSnapshots(t *testing.T) {
	backend := new(recovery.RecoveryBackend)
	child := strings.Repeat("a", 64)
	for _, data := range []string{
		`null`, `{"x":{"unknown":true}}`, `{"x":{"type":"unknown"}}`,
		`{"x":{"type":"list","list":[{"type":"unknown"}]}}`,
		`{"x":{"type":"map","map":[{"key":{"type":"unknown"},"value":{"type":"nil"}}]}}`,
		`{"x":{"type":"map","map":[{"key":{"type":"bytes","bytes":"AA=="},"value":{"type":"nil"}}]}}`,
		`{"x":{"type":"map","map":[{"key":{"type":"string","value":"a"},"value":{"type":"nil"}},{"key":{"type":"string","value":"a"},"value":{"type":"nil"}}]}}`,
		`{"x":{"type":"map","map":[{"key":{"type":"string","value":"a"},"value":{"type":"unknown"}}]}}`,
	} {
		t.Run(data, func(t *testing.T) {
			before := rr.Record{Data: []byte(data), Generation: 1}
			after := rr.Record{Data: []byte(data), Generation: 2}
			require.False(t, backend.HasMarker(before, child))
			require.False(t, backend.MatchesMarked(before, after, child))
		})
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := backend.Read(ctx, make([]byte, 32))
	require.ErrorIs(t, err, context.Canceled)
	_, err = backend.Mark(ctx, rr.Record{}, child)
	require.ErrorIs(t, err, context.Canceled)
	require.ErrorIs(t, backend.Delete(ctx, rr.Record{}), context.Canceled)
	_, err = backend.Read(context.Background(), []byte{1})
	require.ErrorContains(t, err, "invalid recovery key length")
	for _, id := range []string{"short", strings.Repeat("z", 64)} {
		_, err = backend.Inputs(context.Background(), id)
		require.Error(t, err)
		_, err = backend.Transaction(context.Background(), id)
		require.Error(t, err)
		_, err = backend.Mark(context.Background(), rr.Record{}, id)
		require.Error(t, err)
		require.Error(t, backend.VerifyParent(context.Background(), rr.Parent{Child: id}))
	}
	_, err = backend.Snapshot(context.Background(), nil)
	require.Error(t, err)
	require.ErrorContains(t, backend.VerifyParent(context.Background(), rr.Parent{Child: child}), "invalid protected parent key")
	require.Error(t, backend.VerifyParent(context.Background(), rr.Parent{Child: child, Record: rr.Record{Key: make([]byte, 32), Data: []byte("null")}}))
	_, err = recovery.NewRecoveryBackend(nil, "test", "utxo", 1)
	require.Error(t, err)
	_, err = recovery.NewRecoveryBackend(new(as.Client), "test", "utxo", 1, "one", "two")
	require.Error(t, err)
	for _, target := range []string{"", "user@host", "host/path", "host\n"} {
		_, err = recovery.NewRecoveryBackend(new(as.Client), "test", "utxo", 1, target)
		require.Error(t, err)
	}
}
