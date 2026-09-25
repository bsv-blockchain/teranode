package pruner

import (
	"context"
	"testing"

	"github.com/bsv-blockchain/aerospike-client-go/v8"
	"github.com/bsv-blockchain/aerospike-client-go/v8/types"
	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/stores/blob/memory"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/bsv-blockchain/teranode/util/uaerospike"
	aeroTest "github.com/bsv-blockchain/testcontainers-aerospike-go"
	"github.com/stretchr/testify/require"
)

// newFlushTestService starts an Aerospike container and returns a pruner
// service on it. No Lua package is configured, so markers go through the plain
// MapPutItems path; the marker rules under test sit above that choice.
func newFlushTestService(t *testing.T) (*Service, *uaerospike.Client) {
	t.Helper()

	ctx := context.Background()

	container, err := aeroTest.RunContainer(ctx)
	test.SkipIfContainerUnavailable(t, err)

	t.Cleanup(func() {
		require.NoError(t, container.Terminate(ctx))
	})

	host, err := container.Host(ctx)
	require.NoError(t, err)

	port, err := container.ServicePort(ctx)
	require.NoError(t, err)

	client, err := uaerospike.NewClient(host, port)
	require.NoError(t, err)

	service, err := NewService(createTestSettings(), Options{
		Logger:        ulogger.TestLogger{},
		Client:        client,
		Namespace:     "test",
		Set:           "test",
		IndexWaiter:   &MockIndexWaiter{Client: client, Namespace: "test", Set: "test"},
		ExternalStore: memory.New(),
	})
	require.NoError(t, err)

	return service, client
}

// TestFlushCleanupBatches_MarkersAreAllOrNothingPerChild drives
// flushCleanupBatches in the package it lives in, against real records.
//
// Child C spends output 0 of two parents, P1 and P2. P1 always names C as the
// spender of that output. Each row changes what P2 says, and pins three things
// for the end state: whether C is deleted, whether P1 carries C's marker, and
// whether P2 does.
//
// The rules: a child is deleted only when every present parent that it claims
// either carries its marker or names a different, well-formed spender (a
// conflicting loser). Anything else holds the child back, and a held-back child
// carries no marker anywhere, because the spend path answers a marker ahead of
// everything else and would refuse the live child's own re-spend.
func TestFlushCleanupBatches_MarkersAreAllOrNothingPerChild(t *testing.T) {
	s, client := newFlushTestService(t)

	var other chainhash.Hash
	other[0] = 0xEE

	for i, tc := range []struct {
		name string
		// p2 is the P2 record's bins; nil means P2 does not exist.
		p2 func(child *chainhash.Hash) aerospike.BinMap
		// heldBack is whether C must survive the cycle.
		heldBack bool
		// p1Marked / p2Marked are the markers expected afterwards.
		p1Marked, p2Marked bool
	}{
		{
			name:     "both parents name the child",
			p2:       func(c *chainhash.Hash) aerospike.BinMap { return spentBy(c) },
			p1Marked: true, p2Marked: true,
		},
		{
			// R5: one parent's marker write fails. The marker that landed on P1
			// must be withdrawn, or the live child is poisoned for good.
			name: "a failed marker write on one parent is withdrawn from the other",
			p2: func(c *chainhash.Hash) aerospike.BinMap {
				bins := spentBy(c)
				bins[fields.DeletedChildren.String()] = "invalid-map"

				return bins
			},
			heldBack: true,
		},
		{
			// R4 P0: a rolled-back spend leaves the bare 32-byte utxo hash.
			// Deleting C with no marker there let a replay recreate it and spend
			// the output cleanly.
			name: "an unspent output holds the child back and marks nothing",
			p2: func(*chainhash.Hash) aerospike.BinMap {
				return aerospike.BinMap{fields.Utxos.String(): []interface{}{make([]byte, 32)}}
			},
			heldBack: true,
		},
		{
			name:     "a parent with no utxos bin holds the child back",
			p2:       func(*chainhash.Hash) aerospike.BinMap { return aerospike.BinMap{fields.TxID.String(): []byte{1}} },
			heldBack: true,
		},
		{
			name: "an output past the end of the page holds the child back",
			p2: func(*chainhash.Hash) aerospike.BinMap {
				return aerospike.BinMap{fields.Utxos.String(): []interface{}{}}
			},
			heldBack: true,
		},
		{
			// A conflicting loser: the output names a different spender. No
			// marker there, and the child is not held back for it.
			name:     "a different well-formed spender is left unmarked and does not hold the child",
			p2:       func(*chainhash.Hash) aerospike.BinMap { return spentBy(&other) },
			p1Marked: true,
		},
		{
			name:     "a parent that is gone needs no marker",
			p2:       nil,
			p1Marked: true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p1 := chainhash.HashH([]byte{byte(i), 1})
			p2 := chainhash.HashH([]byte{byte(i), 2})
			child := chainhash.HashH([]byte{byte(i), 3})

			p1Key := putRecord(t, client, &p1, spentBy(&child))

			p2Key, err := aerospike.NewKey("test", "test", uaerospike.CalculateKeySource(&p2, 0, 128))
			require.NoError(t, err)

			if tc.p2 != nil {
				p2Key = putRecord(t, client, &p2, tc.p2(&child))
			}

			childKey := putRecord(t, client, &child, aerospike.BinMap{fields.TxID.String(): child.CloneBytes()})

			updates := make(map[string]*parentUpdateInfo)
			require.NoError(t, s.addParentUpdatesForInput(updates, &p1, 0, &child))
			require.NoError(t, s.addParentUpdatesForInput(updates, &p2, 0, &child))

			heldBack, flushErr := s.flushCleanupBatches(context.Background(), updates,
				[]*pendingDeletion{{txHash: &child, keys: []*aerospike.Key{childKey}}}, nil)
			require.NoError(t, flushErr, "no row fails the cycle: every per-record problem is isolated")

			exists, err := client.Exists(nil, childKey)
			require.NoError(t, err)
			require.Equal(t, tc.heldBack, exists, "whether the child survives")

			if tc.heldBack {
				require.Equal(t, 1, heldBack)
			}

			require.Equal(t, tc.p1Marked, hasMarker(t, client, p1Key, &child), "P1 marker")

			if tc.p2 != nil {
				require.Equal(t, tc.p2Marked, hasMarker(t, client, p2Key, &child), "P2 marker")
			}
		})
	}
}

// TestFlushCleanupBatches_FailedDeleteWithdrawsMarkers pins the phase after
// the marker write: the markers landed on both parents, and then the delete of
// the child's own record was refused. That child is still present, so its
// markers must be withdrawn, or the spend path refuses the live child's own
// re-spend as a pruned replay. A refused pagination-record delete does not keep
// the child: its master record is gone, so the markers stay.
//
// A key in a namespace the server does not have is the refusal: the server
// answers that one record with INVALID_NAMESPACE, not in doubt, and the rest of
// the batch goes through.
func TestFlushCleanupBatches_FailedDeleteWithdrawsMarkers(t *testing.T) {
	s, client := newFlushTestService(t)

	for i, tc := range []struct {
		name string
		// refuseMaster puts the refused key first, where the master record is.
		refuseMaster bool
		wantMarked   bool
	}{
		{name: "a refused master delete withdraws the markers", refuseMaster: true},
		{name: "a refused pagination delete keeps the markers", wantMarked: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p1 := chainhash.HashH([]byte{byte(i), 5, 1})
			p2 := chainhash.HashH([]byte{byte(i), 5, 2})
			child := chainhash.HashH([]byte{byte(i), 5, 3})

			p1Key := putRecord(t, client, &p1, spentBy(&child))
			p2Key := putRecord(t, client, &p2, spentBy(&child))
			childKey := putRecord(t, client, &child, aerospike.BinMap{fields.TxID.String(): child.CloneBytes()})

			refused, err := aerospike.NewKey("nosuchnamespace", "test", child.String())
			require.NoError(t, err)

			// The refused key stands in for the child's own master record, which
			// then stays present; as a pagination key it follows a master that is
			// deleted.
			keys := []*aerospike.Key{childKey, refused}
			if tc.refuseMaster {
				keys = []*aerospike.Key{refused}
			}

			updates := make(map[string]*parentUpdateInfo)
			require.NoError(t, s.addParentUpdatesForInput(updates, &p1, 0, &child))
			require.NoError(t, s.addParentUpdatesForInput(updates, &p2, 0, &child))

			_, flushErr := s.flushCleanupBatches(context.Background(), updates,
				[]*pendingDeletion{{txHash: &child, keys: keys}}, nil)
			require.Error(t, flushErr, "a refused delete is still reported")

			require.Equal(t, tc.wantMarked, hasMarker(t, client, p1Key, &child), "P1 marker")
			require.Equal(t, tc.wantMarked, hasMarker(t, client, p2Key, &child), "P2 marker")
		})
	}
}

// TestDeleteRefused pins which per-record answers count as a delete that
// definitely did not happen. Only those withdraw markers: a delete that may
// have landed keeps them, because withdrawing after a landed delete is the
// unmarked deletion this package exists to prevent.
func TestDeleteRefused(t *testing.T) {
	refusal := aerospike.ErrInvalidUser

	for _, tc := range []struct {
		name string
		rec  *aerospike.BatchRecord
		want bool
	}{
		{name: "deleted", rec: &aerospike.BatchRecord{ResultCode: types.OK}},
		{name: "already gone", rec: &aerospike.BatchRecord{ResultCode: types.KEY_NOT_FOUND_ERROR, Err: aerospike.ErrKeyNotFound}},
		{name: "never answered", rec: &aerospike.BatchRecord{ResultCode: types.NO_RESPONSE}},
		{name: "timed out in doubt", rec: &aerospike.BatchRecord{ResultCode: types.TIMEOUT, Err: aerospike.ErrTimeout, InDoubt: true}},
		{name: "refused", rec: &aerospike.BatchRecord{ResultCode: types.INVALID_USER, Err: refusal}, want: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, deleteRefused(tc.rec))
		})
	}
}

// TestProcessSingleRecord_ReportsWhenNothingWasMarked: the manual entry point
// must not return success when a parent is present and nothing was written.
func TestProcessSingleRecord_ReportsWhenNothingWasMarked(t *testing.T) {
	s, client := newFlushTestService(t)

	var other chainhash.Hash
	other[0] = 0xEE

	inputsFrom := func(parent *chainhash.Hash) []*bt.Input {
		input := &bt.Input{PreviousTxOutIndex: 0}
		require.NoError(t, input.PreviousTxIDAdd(parent))

		return []*bt.Input{input}
	}

	for i, tc := range []struct {
		name    string
		bins    func(child *chainhash.Hash) aerospike.BinMap
		wantErr bool
	}{
		{name: "the parent names the child", bins: func(c *chainhash.Hash) aerospike.BinMap { return spentBy(c) }},
		{name: "the parent names another spender", bins: func(*chainhash.Hash) aerospike.BinMap { return spentBy(&other) }, wantErr: true},
		{name: "the parent output is unspent", bins: func(*chainhash.Hash) aerospike.BinMap {
			return aerospike.BinMap{fields.Utxos.String(): []interface{}{make([]byte, 32)}}
		}, wantErr: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			parent := chainhash.HashH([]byte{byte(i), 9, 1})
			child := chainhash.HashH([]byte{byte(i), 9, 2})
			parentKey := putRecord(t, client, &parent, tc.bins(&child))

			err := s.ProcessSingleRecord(&child, inputsFrom(&parent))
			if tc.wantErr {
				require.Error(t, err)
				require.False(t, hasMarker(t, client, parentKey, &child))

				return
			}

			require.NoError(t, err)
			require.True(t, hasMarker(t, client, parentKey, &child))
		})
	}
}

// spentBy returns bins whose utxos bin holds one output spent by spender.
func spentBy(spender *chainhash.Hash) aerospike.BinMap {
	return aerospike.BinMap{fields.Utxos.String(): []interface{}{spentUtxoElement(spender)}}
}

func putRecord(t *testing.T, client *uaerospike.Client, txID *chainhash.Hash, bins aerospike.BinMap) *aerospike.Key {
	t.Helper()

	key, err := aerospike.NewKey("test", "test", uaerospike.CalculateKeySource(txID, 0, 128))
	require.NoError(t, err)
	require.NoError(t, client.Put(nil, key, bins))

	return key
}

func hasMarker(t *testing.T, client *uaerospike.Client, key *aerospike.Key, child *chainhash.Hash) bool {
	t.Helper()

	record, err := client.Get(nil, key, fields.DeletedChildren.String())
	require.NoError(t, err)

	markers, ok := record.Bins[fields.DeletedChildren.String()].(map[interface{}]interface{})
	if !ok {
		return false
	}

	_, marked := markers[child.String()]

	return marked
}
