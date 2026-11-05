package pruner

import (
	"context"
	"testing"

	"github.com/bsv-blockchain/aerospike-client-go/v8"
	"github.com/bsv-blockchain/aerospike-client-go/v8/types"
	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/stores/blob/memory"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/bsv-blockchain/teranode/util/uaerospike"
	aeroTest "github.com/bsv-blockchain/testcontainers-aerospike-go"
	"github.com/stretchr/testify/require"
)

// flushCleanupBatches runs one chunk through flushChunk and drains the carry at
// once, so it sends the marker batch, then the delete batch, then a page batch
// if any master had pages. The partition worker instead keeps one carry across
// its chunks, which folds each delete batch into the next chunk's marker batch.
func (s *Service) flushCleanupBatches(ctx context.Context, parentUpdates map[string]*parentUpdateInfo, deletions []*pendingDeletion, externalFiles []*externalFileInfo) (heldBack int, err error) {
	carry := &deleteCarry{}

	_, heldBack, err = s.flushChunk(ctx, carry, parentUpdates, deletions, externalFiles)
	if err != nil {
		return heldBack, err
	}

	_, drainedHeldBack, err := s.drainCarry(ctx, carry)

	return heldBack + drainedHeldBack, err
}

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
		// p2Offsets are the outputs of P2 the child claims; default output 0.
		p2Offsets []uint32
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
			// A frozen output carries the freeze sentinel where the spending data
			// goes, which names no transaction.
			name: "a frozen output holds the child back",
			p2: func(*chainhash.Hash) aerospike.BinMap {
				frozen := make([]byte, 68)
				copy(frozen[32:], subtree.FrozenBytes[:])

				return aerospike.BinMap{fields.Utxos.String(): []interface{}{frozen}}
			},
			heldBack: true,
		},
		{
			// Two outputs of P2 are claimed by the child, and only the second
			// names it: one holder is enough, as in spendHolder.
			name: "one of several claimed outputs naming the child is enough",
			p2: func(c *chainhash.Hash) aerospike.BinMap {
				return aerospike.BinMap{fields.Utxos.String(): []interface{}{make([]byte, 32), spentUtxoElement(c)}}
			},
			p1Marked: true, p2Marked: true, p2Offsets: []uint32{0, 1},
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
			p2Offsets := tc.p2Offsets
			if p2Offsets == nil {
				p2Offsets = []uint32{0}
			}

			for _, vout := range p2Offsets {
				require.NoError(t, s.addParentUpdatesForInput(updates, &p2, vout, &child))
			}

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

// TestClassifyDeletionResults pins how a deletion batch's per-record answers
// are read. The case that matters is a record the server never answered: the
// client leaves it with ResultCode NO_RESPONSE and a nil Err, and reading the
// nil Err as success let the caller delete the external blob of a child whose
// record and markers were still in place. Such a master is unanswered (markers
// stay, blob stays), and every record not definitely deleted is a failure.
func TestClassifyDeletionResults(t *testing.T) {
	deleted := chainhash.HashH([]byte("deleted"))
	gone := chainhash.HashH([]byte("gone"))
	silent := chainhash.HashH([]byte("silent"))
	inDoubt := chainhash.HashH([]byte("in doubt"))
	refused := chainhash.HashH([]byte("refused"))
	paged := chainhash.HashH([]byte("paged"))

	records := []*aerospike.BatchRecord{
		{ResultCode: types.OK}, // 0: deleted master
		{ResultCode: types.KEY_NOT_FOUND_ERROR, Err: aerospike.ErrKeyNotFound}, // 1: already-gone master
		{ResultCode: types.NO_RESPONSE},                                        // 2: never-answered master, nil Err
		{ResultCode: types.TIMEOUT, Err: aerospike.ErrTimeout, InDoubt: true},  // 3: in-doubt master
		{ResultCode: types.INVALID_USER, Err: aerospike.ErrInvalidUser},        // 4: refused master
		{ResultCode: types.OK},          // 5: paged child's master
		{ResultCode: types.NO_RESPONSE}, // 6: paged child's pagination record, never answered
	}

	batch := make([]aerospike.BatchRecordIfc, len(records))
	for i, rec := range records {
		batch[i] = rec
	}

	masters := map[int]*chainhash.Hash{0: &deleted, 1: &gone, 2: &silent, 3: &inDoubt, 4: &refused, 5: &paged}

	results := classifyDeletionResults(batch, masters)

	require.Equal(t, map[chainhash.Hash]struct{}{refused: {}}, results.survivors,
		"only a definite refusal withdraws the markers")
	require.Equal(t, map[chainhash.Hash]struct{}{silent: {}, inDoubt: {}}, results.unanswered,
		"a master the server never answered, or answered in doubt, keeps its markers and its blob")
	require.Equal(t, []*aerospike.BatchRecord{records[2], records[3], records[4], records[6]}, results.failedRecords,
		"every record not definitely deleted fails the cycle, a pagination record included")
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

// TestFlushCleanupBatches_PageScannedWithItsMasterGoesWithTheMaster covers a
// chunk that holds both the master record M of a paged child C and its
// pagination record M1, which carries its own deleteAtHeight. processRecordChunk
// queues two deletions for C: {C, [M, M1]} for the master, whose inputs give
// the marker on P, and {C, [M1]} for the page, which has no inputs bin and
// queues no marker. M is present when the flush starts, so the page entry is
// dropped and M1 goes with M. P keeps C's marker: otherwise a replay of C finds
// no record and no marker, and is recreated as a ghost.
func TestFlushCleanupBatches_PageScannedWithItsMasterGoesWithTheMaster(t *testing.T) {
	s, client := newFlushTestService(t)

	parent := chainhash.HashH([]byte("standalone-page-parent"))
	child := chainhash.HashH([]byte("standalone-page-child"))

	parentKey := putRecord(t, client, &parent, spentBy(&child))
	masterKey := putRecord(t, client, &child, aerospike.BinMap{
		fields.TxID.String():           child.CloneBytes(),
		fields.TotalExtraRecs.String(): 1,
	})

	pageKey, err := aerospike.NewKey("test", "test", uaerospike.CalculateKeySourceInternal(&child, 1))
	require.NoError(t, err)
	require.NoError(t, client.Put(nil, pageKey, aerospike.BinMap{fields.TxID.String(): child.CloneBytes()}))

	updates := make(map[string]*parentUpdateInfo)
	require.NoError(t, s.addParentUpdatesForInput(updates, &parent, 0, &child))

	deletions := []*pendingDeletion{
		{txHash: &child, keys: []*aerospike.Key{masterKey, pageKey}},  // the scanned master
		{txHash: &child, keys: []*aerospike.Key{pageKey}, page: true}, // the scanned page
	}

	heldBack, flushErr := s.flushCleanupBatches(context.Background(), updates, deletions, nil)
	require.NoError(t, flushErr)
	require.Equal(t, 0, heldBack, "the page goes with its master, so nothing is held back")

	for name, key := range map[string]*aerospike.Key{"master": masterKey, "page": pageKey} {
		exists, err := client.Exists(nil, key)
		require.NoError(t, err)
		require.False(t, exists, "the %s record is deleted by the master's deletion", name)
	}

	require.True(t, hasMarker(t, client, parentKey, &child),
		"the master is gone, so the parent must keep the child's replay marker")
}

// TestFlushCleanupBatches_PageWaitsForItsMaster covers a pagination record M1
// scanned in a different chunk from its master M. The hold-back set is filled
// per chunk from M's inputs, so a chunk holding only M1 finds nothing blocked
// for C. While M is present, kept back for a failed marker write or a refused
// delete, M1 must stay too: a replay of C would otherwise rewrite M1 with
// unspent outputs while M answered ErrTxExists. Once M is gone, M1 is the
// leftover of an earlier cycle and is deleted on its own.
func TestFlushCleanupBatches_PageWaitsForItsMaster(t *testing.T) {
	s, client := newFlushTestService(t)

	for _, tc := range []struct {
		name          string
		masterPresent bool
	}{
		{"master present: the page stays", true},
		{"master gone: the page is deleted", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			child := chainhash.HashH([]byte("page-waits-" + tc.name))

			if tc.masterPresent {
				putRecord(t, client, &child, aerospike.BinMap{
					fields.TxID.String():           child.CloneBytes(),
					fields.TotalExtraRecs.String(): 1,
				})
			}

			pageKey, err := aerospike.NewKey("test", "test", uaerospike.CalculateKeySourceInternal(&child, 1))
			require.NoError(t, err)
			require.NoError(t, client.Put(nil, pageKey, aerospike.BinMap{fields.TxID.String(): child.CloneBytes()}))

			deletions := []*pendingDeletion{{txHash: &child, keys: []*aerospike.Key{pageKey}, page: true}}

			heldBack, flushErr := s.flushCleanupBatches(context.Background(), map[string]*parentUpdateInfo{}, deletions, nil)
			require.NoError(t, flushErr)

			exists, err := client.Exists(nil, pageKey)
			require.NoError(t, err)
			require.Equal(t, tc.masterPresent, exists)

			require.Equal(t, 0, heldBack, "a page kept for a present master is accounted for by that master, not counted here")
		})
	}
}

// TestFlushCleanupBatches_RefusedMasterKeepsItsPages: a master M whose delete
// the server refuses must keep its pages too. One batch is not atomic across
// records, so a page sent alongside a refused M was deleted while M stayed and
// its markers were withdrawn; a replay then rewrote the page with unspent
// outputs while M answered ErrTxExists. Pages now go in a second batch, only
// for masters the first one removed.
//
// As in TestFlushCleanupBatches_FailedDeleteWithdrawsMarkers, a key in a
// namespace the server does not have stands in for the refused master.
func TestFlushCleanupBatches_RefusedMasterKeepsItsPages(t *testing.T) {
	s, client := newFlushTestService(t)

	child := chainhash.HashH([]byte("refused-master-child"))

	pageKey, err := aerospike.NewKey("test", "test", uaerospike.CalculateKeySourceInternal(&child, 1))
	require.NoError(t, err)
	require.NoError(t, client.Put(nil, pageKey, aerospike.BinMap{fields.TxID.String(): child.CloneBytes()}))

	refusedMaster, err := aerospike.NewKey("nosuchnamespace", "test", child.String())
	require.NoError(t, err)

	deletions := []*pendingDeletion{{txHash: &child, keys: []*aerospike.Key{refusedMaster, pageKey}}}

	_, flushErr := s.flushCleanupBatches(context.Background(), map[string]*parentUpdateInfo{}, deletions, nil)
	require.Error(t, flushErr, "a refused delete is still reported")

	exists, err := client.Exists(nil, pageKey)
	require.NoError(t, err)
	require.True(t, exists, "the page of a master the server refused to delete must stay")
}

// TestFlushCleanupBatches_FailedDeleteStillRemovesConfirmedBlobs: one batch
// deletes child A and is refused child B's master. A's master is confirmed
// gone, so no later scan finds A again, and its external blob carries no DAH.
// The failed record must not stop A's blob from being deleted, and B's blob,
// whose master stays, must stay.
func TestFlushCleanupBatches_FailedDeleteStillRemovesConfirmedBlobs(t *testing.T) {
	s, client := newFlushTestService(t)

	ctx := context.Background()

	deletedChild := chainhash.HashH([]byte("confirmed-blob-child"))
	refusedChild := chainhash.HashH([]byte("refused-blob-child"))

	deletedKey := putRecord(t, client, &deletedChild, aerospike.BinMap{fields.TxID.String(): deletedChild.CloneBytes()})

	refusedKey, err := aerospike.NewKey("nosuchnamespace", "test", refusedChild.String())
	require.NoError(t, err)

	for _, hash := range []chainhash.Hash{deletedChild, refusedChild} {
		require.NoError(t, s.external.Set(ctx, hash.CloneBytes(), fileformat.FileTypeTx, []byte("blob")))
	}

	deletions := []*pendingDeletion{
		{txHash: &deletedChild, keys: []*aerospike.Key{deletedKey}},
		{txHash: &refusedChild, keys: []*aerospike.Key{refusedKey}},
	}
	files := []*externalFileInfo{
		{txHash: &deletedChild, fileType: fileformat.FileTypeTx},
		{txHash: &refusedChild, fileType: fileformat.FileTypeTx},
	}

	_, flushErr := s.flushCleanupBatches(ctx, map[string]*parentUpdateInfo{}, deletions, files)
	require.Error(t, flushErr, "the refused delete is still reported")

	exists, err := client.Exists(nil, deletedKey)
	require.NoError(t, err)
	require.False(t, exists, "precondition: the first child's master is deleted")

	blobExists, blobErr := s.external.Exists(ctx, deletedChild.CloneBytes(), fileformat.FileTypeTx)
	require.NoError(t, blobErr)
	require.False(t, blobExists, "the blob of a child whose master is confirmed gone must be deleted even though the batch had a failure")

	blobExists, blobErr = s.external.Exists(ctx, refusedChild.CloneBytes(), fileformat.FileTypeTx)
	require.NoError(t, blobErr)
	require.True(t, blobExists, "the blob of a child whose master delete was refused must stay")
}

// TestIsPaginationRecord: processRecordChunk sets pendingDeletion.page from
// this, so a scanned page never stands in for its master. A master carries
// totalExtraRecs, or, from a writer that does not set it, sits under the bare
// txid; a pagination record is neither.
func TestIsPaginationRecord(t *testing.T) {
	s, client := newFlushTestService(t)

	paged := chainhash.HashH([]byte("is-pagination-paged"))
	legacy := chainhash.HashH([]byte("is-pagination-legacy"))

	masterKey := putRecord(t, client, &paged, aerospike.BinMap{
		fields.TxID.String():           paged.CloneBytes(),
		fields.TotalExtraRecs.String(): 1,
	})
	legacyKey := putRecord(t, client, &legacy, aerospike.BinMap{fields.TxID.String(): legacy.CloneBytes()})

	pageKey, err := aerospike.NewKey("test", "test", uaerospike.CalculateKeySourceInternal(&paged, 1))
	require.NoError(t, err)
	require.NoError(t, client.Put(nil, pageKey, aerospike.BinMap{fields.TxID.String(): paged.CloneBytes()}))

	for _, tc := range []struct {
		name string
		key  *aerospike.Key
		hash *chainhash.Hash
		page bool
	}{
		{"master with totalExtraRecs", masterKey, &paged, false},
		{"master without totalExtraRecs", legacyKey, &legacy, false},
		{"pagination record", pageKey, &paged, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			record, err := client.Get(nil, tc.key)
			require.NoError(t, err)
			require.Equal(t, tc.page, s.isPaginationRecord(record, tc.hash))
		})
	}
}

// countBatches makes every pruner batch call go to the client through a
// counter, and returns the counter.
func countBatches(s *Service, client *uaerospike.Client) *int {
	calls := 0

	s.batchOperateFn = func(policy *aerospike.BatchPolicy, records []aerospike.BatchRecordIfc) error {
		calls++

		return client.BatchOperate(policy, records)
	}

	return &calls
}

// TestFlushChunk_DeletesRideTheNextChunksBatch pins the cost and the order of
// the carried deletes. Chunk 1 queues child C1 on parent P1, chunk 2 queues C2
// on P2. Chunk 1's batch writes C1's marker and deletes nothing. Chunk 2's
// batch writes C2's marker and deletes C1, whose marker the previous batch
// confirmed. The drain deletes C2. Three batches for two chunks, one per chunk
// plus the drain, and no child is deleted in the batch that marks it.
func TestFlushChunk_DeletesRideTheNextChunksBatch(t *testing.T) {
	s, client := newFlushTestService(t)
	calls := countBatches(s, client)
	ctx := context.Background()

	queue := func(n byte) (*chainhash.Hash, *aerospike.Key, *aerospike.Key, map[string]*parentUpdateInfo) {
		parent := chainhash.HashH([]byte{0xC0, n, 1})
		child := chainhash.HashH([]byte{0xC0, n, 2})

		parentKey := putRecord(t, client, &parent, spentBy(&child))
		childKey := putRecord(t, client, &child, aerospike.BinMap{fields.TxID.String(): child.CloneBytes()})

		updates := make(map[string]*parentUpdateInfo)
		require.NoError(t, s.addParentUpdatesForInput(updates, &parent, 0, &child))

		return &child, parentKey, childKey, updates
	}

	exists := func(key *aerospike.Key) bool {
		found, err := client.Exists(nil, key)
		require.NoError(t, err)

		return found
	}

	c1, p1Key, c1Key, updates1 := queue(1)
	c2, p2Key, c2Key, updates2 := queue(2)

	carry := &deleteCarry{}

	deleted, heldBack, err := s.flushChunk(ctx, carry, updates1, []*pendingDeletion{{txHash: c1, keys: []*aerospike.Key{c1Key}}}, nil)
	require.NoError(t, err)
	require.Equal(t, 1, *calls)
	require.Zero(t, deleted)
	require.Zero(t, heldBack)
	require.True(t, hasMarker(t, client, p1Key, c1))
	require.True(t, exists(c1Key), "a child is not deleted in the batch that marks it")

	deleted, _, err = s.flushChunk(ctx, carry, updates2, []*pendingDeletion{{txHash: c2, keys: []*aerospike.Key{c2Key}}}, nil)
	require.NoError(t, err)
	require.Equal(t, 2, *calls, "chunk 2's markers and chunk 1's delete share one batch")
	require.Equal(t, 1, deleted)
	require.False(t, exists(c1Key))
	require.True(t, hasMarker(t, client, p2Key, c2))
	require.True(t, exists(c2Key))

	deleted, _, err = s.drainCarry(ctx, carry)
	require.NoError(t, err)
	require.Equal(t, 3, *calls)
	require.Equal(t, 1, deleted)
	require.False(t, exists(c2Key))
	require.True(t, carry.empty())
}

// TestFlushChunk_MarkerOnARecordTheSameBatchDeletes covers a parent P that is
// itself a pruned child, cleared by chunk 1, while chunk 2 queues P's own child
// C on it. Chunk 2's batch deletes P, so C's marker write on P is not sent: the
// rows of one batch run in no fixed order, and P's delete answers whether P is
// gone. When it is, C is cleared. When the delete is refused, P may still be
// there unmarked, so C is held back.
func TestFlushChunk_MarkerOnARecordTheSameBatchDeletes(t *testing.T) {
	s, client := newFlushTestService(t)
	ctx := context.Background()

	for _, tc := range []struct {
		name    string
		refused bool
		ttl     bool
	}{
		{name: "the parent's delete lands, so the child is cleared"},
		// In TTL mode the parent's "delete" is a one-second expiry that leaves
		// it readable, so the child's marker on it is written anyway.
		{name: "in TTL mode the marker on the expiring parent is still written", ttl: true},
		// A key in a namespace the server does not have is refused with
		// INVALID_NAMESPACE, which is not in doubt.
		{name: "the parent's delete is refused, so the child is held back", refused: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			calls := countBatches(s, client)

			s.utxoSetTTL = tc.ttl
			t.Cleanup(func() { s.utxoSetTTL = false })

			n := byte(0)
			if tc.refused {
				n = 1
			}

			if tc.ttl {
				n = 2
			}

			parent := chainhash.HashH([]byte{0xD0, n, 1})
			child := chainhash.HashH([]byte{0xD0, n, 2})

			parentKey := putRecord(t, client, &parent, spentBy(&child))
			if tc.refused {
				var err error
				parentKey, err = aerospike.NewKey("nosuchnamespace", "test", uaerospike.CalculateKeySource(&parent, 0, 128))
				require.NoError(t, err)
			}

			childKey := putRecord(t, client, &child, aerospike.BinMap{fields.TxID.String(): child.CloneBytes()})

			carry := &deleteCarry{deletions: []*pendingDeletion{{txHash: &parent, keys: []*aerospike.Key{parentKey}}}, updates: []map[string]*parentUpdateInfo{{}}}

			info := &parentUpdateInfo{key: parentKey, seen: make(map[chainhash.Hash]struct{})}
			info.add(&child, 0)
			updates := map[string]*parentUpdateInfo{string(parentKey.Digest()): info}

			_, heldBack, err := s.flushChunk(ctx, carry, updates, []*pendingDeletion{{txHash: &child, keys: []*aerospike.Key{childKey}}}, nil)

			if tc.refused {
				require.Equal(t, 2, *calls, "the batch, then the withdrawal of the held-back child's markers")
				require.Error(t, err, "a refused delete fails the batch, as before")
				require.Equal(t, 2, heldBack, "the surviving parent and its child are both held back")
				require.True(t, carry.empty(), "the held-back child is not carried")

				return
			}

			require.NoError(t, err)
			require.Equal(t, 1, *calls)
			require.Zero(t, heldBack)

			if tc.ttl {
				require.True(t, hasMarker(t, client, parentKey, &child), "the expiring parent carries the marker")
			}

			_, _, err = s.drainCarry(ctx, carry)
			require.NoError(t, err)

			if tc.ttl {
				// The child's "delete" is a one-second expiry too.
				return
			}

			found, err := client.Exists(nil, childKey)
			require.NoError(t, err)
			require.False(t, found)
		})
	}
}

// TestFlushChunk_UnsentBatchKeepsTheCarry: a batch that never goes out, because
// the context is done, must leave the carried deletes owed, so the drain still
// sends them.
func TestFlushChunk_UnsentBatchKeepsTheCarry(t *testing.T) {
	s, client := newFlushTestService(t)

	child := chainhash.HashH([]byte{0xE0, 2})
	childKey := putRecord(t, client, &child, aerospike.BinMap{fields.TxID.String(): child.CloneBytes()})

	carry := &deleteCarry{deletions: []*pendingDeletion{{txHash: &child, keys: []*aerospike.Key{childKey}}}, updates: []map[string]*parentUpdateInfo{{}}}

	done, cancel := context.WithCancel(context.Background())
	cancel()

	_, _, err := s.flushChunk(done, carry, nil, nil, nil)
	require.ErrorIs(t, err, context.Canceled)
	require.Len(t, carry.deletions, 1)

	deleted, _, err := s.drainCarryDetached(done, carry)
	require.NoError(t, err)
	require.Equal(t, 1, deleted)

	found, err := client.Exists(nil, childKey)
	require.NoError(t, err)
	require.False(t, found)
}

// TestMarkParents_InDoubtRefusalIsAFailedWrite: a refused marker write that is
// in doubt may follow a retried attempt that did write the markers, so it is a
// failed write. Its children are held back and the withdrawal goes to that
// record as well, rather than skipping it as a record that was never written.
func TestMarkParents_InDoubtRefusalIsAFailedWrite(t *testing.T) {
	s, client := newFlushTestService(t)

	parent := chainhash.HashH([]byte{0xF0, 1})
	child := chainhash.HashH([]byte{0xF0, 2})
	parentKey := putRecord(t, client, &parent, spentBy(&child))

	updates := make(map[string]*parentUpdateInfo)
	require.NoError(t, s.addParentUpdatesForInput(updates, &parent, 0, &child))

	var withdrawnFrom [][]byte

	call := 0
	s.batchOperateFn = func(policy *aerospike.BatchPolicy, records []aerospike.BatchRecordIfc) error {
		call++

		if call == 1 {
			for _, rec := range records {
				br := rec.BatchRec()
				br.ResultCode = types.FILTERED_OUT
				br.InDoubt = true
				br.Err = &aerospike.AerospikeError{ResultCode: types.FILTERED_OUT, InDoubt: true}
			}

			return nil
		}

		for _, rec := range records {
			withdrawnFrom = append(withdrawnFrom, rec.BatchRec().Key.Digest())
		}

		return client.BatchOperate(policy, records)
	}

	plan, err := s.markParents(context.Background(), updates)
	require.NoError(t, err)
	require.Contains(t, plan.blocked, child)
	require.Equal(t, [][]byte{parentKey.Digest()}, withdrawnFrom, "the in-doubt record is withdrawn from, not read and stripped")
}

// TestMarkerWritesKeepTheRecordsLifetime: in TTL mode a parent can already be
// expiring when its marker write, or a withdrawal, reaches it. Neither write
// may give it its lifetime back, or a parent the pruner counted gone, with its
// pages and blob removed, would live on. A record touched to expire in one
// second must still expire after both writes.
func TestMarkerWritesKeepTheRecordsLifetime(t *testing.T) {
	s, client := newFlushTestService(t)

	parent := chainhash.HashH([]byte{0xF1, 1})
	child := chainhash.HashH([]byte{0xF1, 2})
	parentKey := putRecord(t, client, &parent, spentBy(&child))

	require.NoError(t, client.Touch(aerospike.NewWritePolicy(0, 60), parentKey))

	expiresSoon := func(what string) {
		header, err := client.GetHeader(nil, parentKey)
		require.NoError(t, err)
		require.LessOrEqual(t, header.Expiration, uint32(60), what)
	}

	expiresSoon("the touch set a short lifetime")

	updates := make(map[string]*parentUpdateInfo)
	require.NoError(t, s.addParentUpdatesForInput(updates, &parent, 0, &child))

	_, err := s.markParents(context.Background(), updates)
	require.NoError(t, err)
	require.True(t, hasMarker(t, client, parentKey, &child))
	expiresSoon("the marker write kept the lifetime")

	s.withdrawMarkers(updates, map[chainhash.Hash]struct{}{child: {}})
	require.False(t, hasMarker(t, client, parentKey, &child))
	expiresSoon("the withdrawal kept the lifetime")
}

// TestFlushChunk_CarriedDeleteSparesAChildThatLeftTheChain covers a child C
// cleared by one batch whose record changes before the next batch carries its
// delete. A block invalidation in between strips C's block ids and stamps
// unmined_since, and a reorg's move-back can lock C while it saves C for
// restoration. Either way C is no longer the mined, fully spent record the
// pruner selected, so the carried delete is refused on the server, C stays,
// and the marker naming C on its parent P is withdrawn. The refusal is the
// designed outcome, so it is counted as held back and does not fail the flush.
// An unmined conflicting child is still deleted, as dropUnminedRecords lets it
// through at selection. The last row is the control: a child that did not
// change is deleted.
func TestFlushChunk_CarriedDeleteSparesAChildThatLeftTheChain(t *testing.T) {
	s, client := newFlushTestService(t)
	ctx := context.Background()

	for i, tc := range []struct {
		name      string
		ttl       bool
		change    aerospike.BinMap
		wantSpare bool
	}{
		{name: "locked and unmined", change: aerospike.BinMap{fields.Locked.String(): true, fields.UnminedSince.String(): 100}, wantSpare: true},
		{name: "locked only", change: aerospike.BinMap{fields.Locked.String(): true}, wantSpare: true},
		{name: "unmined only", change: aerospike.BinMap{fields.UnminedSince.String(): 100}, wantSpare: true},
		{name: "locked and unmined, TTL mode", ttl: true, change: aerospike.BinMap{fields.Locked.String(): true, fields.UnminedSince.String(): 100}, wantSpare: true},
		{name: "unmined and conflicting is deleted", change: aerospike.BinMap{fields.UnminedSince.String(): 100, fields.Conflicting.String(): true}},
		{name: "unchanged child is deleted", change: aerospike.BinMap{fields.Locked.String(): false}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s.utxoSetTTL = tc.ttl
			t.Cleanup(func() { s.utxoSetTTL = false })

			parent := chainhash.HashH([]byte{0xD7, byte(i), 1})
			child := chainhash.HashH([]byte{0xD7, byte(i), 2})

			parentKey := putRecord(t, client, &parent, spentBy(&child))
			childKey := putRecord(t, client, &child, aerospike.BinMap{fields.TxID.String(): child.CloneBytes()})

			updates := make(map[string]*parentUpdateInfo)
			require.NoError(t, s.addParentUpdatesForInput(updates, &parent, 0, &child))

			carry := &deleteCarry{}

			_, heldBack, err := s.flushChunk(ctx, carry, updates, []*pendingDeletion{{txHash: &child, keys: []*aerospike.Key{childKey}}}, nil)
			require.NoError(t, err)
			require.Zero(t, heldBack)
			require.True(t, hasMarker(t, client, parentKey, &child), "the first batch marks the parent")

			require.NoError(t, client.Put(nil, childKey, tc.change))

			deleted, heldBack, err := s.drainCarry(ctx, carry)
			require.NoError(t, err, "a refused delete of a changed child is not a failure")

			record, err := client.Get(nil, childKey)
			if !tc.wantSpare {
				require.Equal(t, 1, deleted)
				require.Zero(t, heldBack)
				var aeroErr *aerospike.AerospikeError
				require.ErrorAs(t, err, &aeroErr)
				require.True(t, aeroErr.Matches(types.KEY_NOT_FOUND_ERROR), "the unchanged child is deleted")
				require.True(t, hasMarker(t, client, parentKey, &child), "a deleted child keeps its marker")

				return
			}

			require.NoError(t, err, "the changed child is still present")
			require.Zero(t, deleted)
			require.Equal(t, 1, heldBack)
			require.False(t, hasMarker(t, client, parentKey, &child), "the marker of a spared child is withdrawn")

			if tc.ttl {
				require.NotEqual(t, uint32(1), record.Expiration, "the TTL touch is refused too")
			}
		})
	}
}
