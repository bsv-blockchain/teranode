package pruner

import (
	"context"
	"os"
	"sync"
	"testing"
	"time"

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
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/vmihailenco/msgpack/v5"
)

// flushCleanupBatches runs one chunk through flushChunk and drains the carry at
// once, so it sends the marker batch, then the delete batch, then a page batch
// if any master had pages. The partition worker instead keeps one carry across
// its chunks, which folds each delete batch into the next chunk's marker batch.
func (s *Service) flushCleanupBatches(ctx context.Context, parentUpdates map[string]*parentUpdateInfo, deletions []*pendingDeletion, externalFiles []*externalFileInfo) (heldBack int, err error) {
	carry := &deleteCarry{blockHeight: testPruneHeight}

	_, heldBack, err = s.flushChunk(ctx, carry, parentUpdates, deletions, externalFiles)
	if err != nil {
		return heldBack, err
	}

	_, drainedHeldBack, err := s.drainCarry(ctx, carry)

	return heldBack + drainedHeldBack, err
}

// newFlushTestService starts an Aerospike container and returns a pruner
// service on it. AEROSPIKE_CONTAINER_IMAGE picks the server image. Markers go
// through the filtered MapPutItems write, or native sub-op 15 when
// AEROSPIKE_EXPECT_NATIVE_OPS=true; the marker rules under test sit above that
// choice.
func newFlushTestService(t *testing.T) (*Service, *uaerospike.Client) {
	t.Helper()

	ctx := context.Background()

	var containerOpts []testcontainers.ContainerCustomizer

	if image := os.Getenv("AEROSPIKE_CONTAINER_IMAGE"); image != "" {
		platform := os.Getenv("AEROSPIKE_CONTAINER_PLATFORM")
		if platform == "" {
			platform = "linux/amd64"
		}

		containerOpts = append(containerOpts, aeroTest.WithImage(image), testcontainers.WithImagePlatform(platform))
	}

	container, err := aeroTest.RunContainer(ctx, containerOpts...)
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

	// Against the fork image (AEROSPIKE_EXPECT_NATIVE_OPS=true) every marker
	// write goes out as native sub-op 15, so the marker rules are proven on
	// that path too.
	if os.Getenv("AEROSPIKE_EXPECT_NATIVE_OPS") == "true" {
		service.buildVerifiedMarkerRecord = testNativeMarkerRecord
	}

	return service, client
}

// testNativeMarkerRecord builds a native sub-op 15 marker write as the store's
// prunerVerifiedMarkerRecord does: payload [15, [[key, txid, offsets]...]],
// UPDATE_ONLY, TTLDontUpdate. The store package cannot be imported here.
func testNativeMarkerRecord(key *aerospike.Key, children []MarkerChild) (aerospike.BatchRecordIfc, bool) {
	entries := make([]any, len(children))

	for i, child := range children {
		offsets := make([]any, len(child.Offsets))
		for j, offset := range child.Offsets {
			offsets[j] = int64(offset)
		}

		entries[i] = []any{child.Hash.String(), child.Hash.CloneBytes(), offsets}
	}

	payload, err := msgpack.Marshal([]any{uint8(15), []any{entries}})
	if err != nil {
		panic(err)
	}

	policy := aerospike.NewBatchWritePolicy()
	policy.RecordExistsAction = aerospike.UPDATE_ONLY
	policy.Expiration = aerospike.TTLDontUpdate

	return aerospike.NewBatchWrite(policy, key, aerospike.TeranodeModifyOp(nativeResultBin, payload)), true
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
// carries no marker anywhere, because a marker on an output that names the
// live child would refuse that child's own re-spend.
func TestFlushCleanupBatches_MarkersAreAllOrNothingPerChild(t *testing.T) {
	s, client := newFlushTestService(t)

	// On the fork image the rows must have gone out as native writes, or a
	// green run would only prove the filtered path again.
	if build := s.buildVerifiedMarkerRecord; build != nil {
		nativeWrites := 0
		s.buildVerifiedMarkerRecord = func(key *aerospike.Key, children []MarkerChild) (aerospike.BatchRecordIfc, bool) {
			nativeWrites++
			return build(key, children)
		}

		defer func() {
			require.Positive(t, nativeWrites, "the marker writes must take the native path")
		}()
	}

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

// testPruneHeight is the block height the flush tests prune at. putRecord
// gives a record a deleteAtHeight below it unless bins sets one, so a carried
// delete passes carriedMasterGuard's height leg as a scanned record would.
const testPruneHeight = 1000

func putRecord(t *testing.T, client *uaerospike.Client, txID *chainhash.Hash, bins aerospike.BinMap) *aerospike.Key {
	t.Helper()

	key, err := aerospike.NewKey("test", "test", uaerospike.CalculateKeySource(txID, 0, 128))
	require.NoError(t, err)

	withDAH := make(aerospike.BinMap, len(bins)+1)
	withDAH[fields.DeleteAtHeight.String()] = testPruneHeight - 1

	for name, value := range bins {
		withDAH[name] = value
	}

	require.NoError(t, client.Put(nil, key, withDAH))

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

// TestFlushChunk_BlobsOfConfirmedDeletesOutliveTheContext: the batch call does
// not watch the context, so a delete batch can land after the drain's deadline
// has passed. The child's master is then gone, so no later scan finds it, and
// its blob carries no DAH. The blob must still be deleted.
func TestFlushChunk_BlobsOfConfirmedDeletesOutliveTheContext(t *testing.T) {
	s, client := newFlushTestService(t)

	child := chainhash.HashH([]byte("late-batch-blob-child"))
	childKey := putRecord(t, client, &child, aerospike.BinMap{fields.TxID.String(): child.CloneBytes()})
	require.NoError(t, s.external.Set(context.Background(), child.CloneBytes(), fileformat.FileTypeTx, []byte("blob")))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// The batch answers after the context has ended.
	s.batchOperateFn = func(policy *aerospike.BatchPolicy, records []aerospike.BatchRecordIfc) error {
		err := client.BatchOperate(policy, records)
		cancel()

		return err
	}

	carry := &deleteCarry{
		blockHeight: testPruneHeight,
		deletions:   []*pendingDeletion{{txHash: &child, keys: []*aerospike.Key{childKey}}},
		files:       []*externalFileInfo{{txHash: &child, fileType: fileformat.FileTypeTx}},
		updates:     []map[string]*parentUpdateInfo{{}},
	}

	deleted, _, err := s.flushChunk(ctx, carry, nil, nil, nil)
	require.NoError(t, err)
	require.Equal(t, 1, deleted, "fixture: the master delete landed")

	exists, err := client.Exists(nil, childKey)
	require.NoError(t, err)
	require.False(t, exists, "fixture: the master is gone")

	blobExists, err := s.external.Exists(context.Background(), child.CloneBytes(), fileformat.FileTypeTx)
	require.NoError(t, err)
	require.False(t, blobExists, "the blob of a child whose master delete landed must go even though the context ended")
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

	carry := &deleteCarry{blockHeight: testPruneHeight}

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

// TestFlushChunk_ChunksOfOneWorkerOverlapTheirBatches: with a chunk group limit
// above 1, two chunks of one worker share its carry, and their batches must be
// in flight at the same time, not queued behind the carry's lock. It also runs
// the overlap that lock used to rule out: chunk A's batch deletes X, a child
// cleared earlier, while chunk B's batch writes the marker for X's own child on
// X. Once both batches are in flight, each row forces one order on the server:
// the marker first, so it goes with X, or the delete first, so the UPDATE_ONLY
// marker answers KEY_NOT_FOUND and counts as a parent that is gone. Either way
// X ends up gone and B's child is cleared and deleted. In TTL mode the delete
// is a one-second touch: the marker still lands on X, and with TTLDontUpdate it
// leaves X expiring.
func TestFlushChunk_ChunksOfOneWorkerOverlapTheirBatches(t *testing.T) {
	s, client := newFlushTestService(t)
	s.chunkGroupLimit = 2
	ctx := context.Background()

	for i, tc := range []struct {
		name        string
		deleteFirst bool
		ttl         bool
	}{
		{name: "marker lands first"},
		{name: "delete lands first", deleteFirst: true},
		{name: "marker lands first, TTL mode", ttl: true},
		{name: "delete lands first, TTL mode", deleteFirst: true, ttl: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s.utxoSetTTL = tc.ttl
			t.Cleanup(func() { s.utxoSetTTL = false })

			// gone reports whether a record's delete took effect: removed, or in
			// TTL mode still expiring within the one-second touch.
			gone := func(key *aerospike.Key) bool {
				record, err := client.GetHeader(nil, key)
				if err != nil {
					var aeroErr *aerospike.AerospikeError
					require.ErrorAs(t, err, &aeroErr)
					require.True(t, aeroErr.Matches(types.KEY_NOT_FOUND_ERROR))

					return true
				}

				// A header read of a missing key answers no record and no error.
				if record == nil {
					return true
				}

				return tc.ttl && record.Expiration <= 1
			}

			// X was cleared by an earlier chunk and waits in the carry. B's child
			// cB spends X:0.
			x := chainhash.HashH([]byte{0xC5, byte(i), 1})
			cB := chainhash.HashH([]byte{0xC5, byte(i), 2})
			xKey := putRecord(t, client, &x, spentBy(&cB))
			cBKey := putRecord(t, client, &cB, aerospike.BinMap{fields.TxID.String(): cB.CloneBytes()})

			updatesB := make(map[string]*parentUpdateInfo)
			require.NoError(t, s.addParentUpdatesForInput(updatesB, &x, 0, &cB))

			// A's own chunk: cA on parent pA.
			pA := chainhash.HashH([]byte{0xC5, byte(i), 3})
			cA := chainhash.HashH([]byte{0xC5, byte(i), 4})
			putRecord(t, client, &pA, spentBy(&cA))
			cAKey := putRecord(t, client, &cA, aerospike.BinMap{fields.TxID.String(): cA.CloneBytes()})

			updatesA := make(map[string]*parentUpdateInfo)
			require.NoError(t, s.addParentUpdatesForInput(updatesA, &pA, 0, &cA))

			carry := &deleteCarry{blockHeight: testPruneHeight, deletions: []*pendingDeletion{{txHash: &x, keys: []*aerospike.Key{xKey}}}, updates: []map[string]*parentUpdateInfo{{}}}

			// The first two batches wait until both are in flight, or give up.
			// Then the one chosen to land first runs, and the other waits for it.
			var (
				mu       sync.Mutex
				entered  int
				bothIn   = make(chan struct{})
				overlap  bool
				firstIn  = make(chan struct{})
				released bool
				landed   = [2]chan struct{}{make(chan struct{}), make(chan struct{})}
			)

			// Batch 1 is A's: it deletes X. Batch 2 is B's: it marks X.
			first, second := 1, 0
			if tc.deleteFirst {
				first, second = 0, 1
			}

			s.batchOperateFn = func(policy *aerospike.BatchPolicy, records []aerospike.BatchRecordIfc) error {
				mu.Lock()
				entered++
				n := entered

				// Overlap only while the first batch is still waiting.
				if n == 2 && !released {
					overlap = true
					released = true
					close(bothIn)
				}
				mu.Unlock()

				if n > 2 {
					return client.BatchOperate(policy, records)
				}

				if n == 1 {
					close(firstIn)
				}

				select {
				case <-bothIn:
				case <-time.After(5 * time.Second):
					mu.Lock()
					if !released {
						released = true
						close(bothIn)
					}
					mu.Unlock()
				}

				if n-1 == second {
					select {
					case <-landed[first]:
					case <-time.After(5 * time.Second):
					}
				}

				err := client.BatchOperate(policy, records)

				close(landed[n-1])

				return err
			}

			var wg sync.WaitGroup

			errs := make([]error, 2)

			wg.Add(1)

			go func() {
				defer wg.Done()

				_, _, errs[0] = s.flushChunk(ctx, carry, updatesA, []*pendingDeletion{{txHash: &cA, keys: []*aerospike.Key{cAKey}}}, nil)
			}()

			<-firstIn

			wg.Add(1)

			go func() {
				defer wg.Done()

				_, _, errs[1] = s.flushChunk(ctx, carry, updatesB, []*pendingDeletion{{txHash: &cB, keys: []*aerospike.Key{cBKey}}}, nil)
			}()

			wg.Wait()
			require.NoError(t, errs[0])
			require.NoError(t, errs[1])

			mu.Lock()
			require.True(t, overlap, "the second chunk's batch must go out while the first one's is still in flight")
			mu.Unlock()

			require.True(t, gone(xKey), "chunk A's batch deleted X, and B's marker did not give it a new lifetime")

			deleted, heldBack, err := s.drainCarry(ctx, carry)
			require.NoError(t, err)
			require.True(t, carry.empty())
			require.Equal(t, 2, deleted, "the drain deletes both chunks' children")
			require.Zero(t, heldBack)
			require.True(t, gone(cAKey), "A's child is deleted by the drain")
			require.True(t, gone(cBKey), "B's child is cleared whichever of the two writes on X landed first, and the drain deletes it")
		})
	}
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

			// Count the marker writes (as opposed to the delete) that reach the parent's key.
			// The parent's end state cannot tell a write that was held back from one that ran
			// beside its own delete, so the batch is read.
			markerWrites := 0
			counted := s.batchOperateFn
			s.batchOperateFn = func(policy *aerospike.BatchPolicy, records []aerospike.BatchRecordIfc) error {
				for _, rec := range records {
					if _, isWrite := rec.(*aerospike.BatchWrite); isWrite && string(rec.BatchRec().Key.Digest()) == string(parentKey.Digest()) {
						markerWrites++
					}
				}

				return counted(policy, records)
			}

			childKey := putRecord(t, client, &child, aerospike.BinMap{fields.TxID.String(): child.CloneBytes()})

			carry := &deleteCarry{blockHeight: testPruneHeight, deletions: []*pendingDeletion{{txHash: &parent, keys: []*aerospike.Key{parentKey}}}, updates: []map[string]*parentUpdateInfo{{}}}

			info := &parentUpdateInfo{key: parentKey, seen: make(map[chainhash.Hash]struct{})}
			info.add(&child, 0)
			updates := map[string]*parentUpdateInfo{string(parentKey.Digest()): info}

			_, heldBack, err := s.flushChunk(ctx, carry, updates, []*pendingDeletion{{txHash: &child, keys: []*aerospike.Key{childKey}}}, nil)

			if tc.ttl {
				// The parent's expiry touch is a write too, so there are two.
				require.Positive(t, markerWrites, "the marker on the expiring parent is sent")
			} else if !tc.refused {
				require.Zero(t, markerWrites, "the marker on a record the same batch deletes is not sent")
			}

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

	carry := &deleteCarry{blockHeight: testPruneHeight, deletions: []*pendingDeletion{{txHash: &child, keys: []*aerospike.Key{childKey}}}, updates: []map[string]*parentUpdateInfo{{}}}

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
// restoration, and a conflict resolution that releases one of C's outputs
// clears its deleteAtHeight. Each way C is no longer the mined, fully spent record the
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
		// A conflict resolution that releases one of the child's outputs
		// clears its deleteAtHeight (Lua setDeleteAtHeight), so the output it
		// released is live again and must not go with the child.
		{name: "deleteAtHeight cleared", change: aerospike.BinMap{fields.DeleteAtHeight.String(): nil}, wantSpare: true},
		{name: "deleteAtHeight cleared, TTL mode", ttl: true, change: aerospike.BinMap{fields.DeleteAtHeight.String(): nil}, wantSpare: true},
		{name: "deleteAtHeight raised past the prune height", change: aerospike.BinMap{fields.DeleteAtHeight.String(): testPruneHeight + 1}, wantSpare: true},
		{name: "deleteAtHeight at the prune height is deleted", change: aerospike.BinMap{fields.DeleteAtHeight.String(): testPruneHeight}},
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

			carry := &deleteCarry{blockHeight: testPruneHeight}

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

// TestWithdrawMarkers_ResendsAFailedWithdrawal: a child kept after a refused
// delete may never be scanned again (a reorg cleared its deletion height), so a
// withdrawal that fails once is sent again in the same call. A marker left
// behind would refuse the child's idempotent re-spend once it is mined again.
func TestWithdrawMarkers_ResendsAFailedWithdrawal(t *testing.T) {
	s, client := newFlushTestService(t)

	parent := chainhash.HashH([]byte{0xF2, 1})
	child := chainhash.HashH([]byte{0xF2, 2})
	parentKey := putRecord(t, client, &parent, spentBy(&child))

	updates := make(map[string]*parentUpdateInfo)
	require.NoError(t, s.addParentUpdatesForInput(updates, &parent, 0, &child))

	_, err := s.markParents(context.Background(), updates)
	require.NoError(t, err)
	require.True(t, hasMarker(t, client, parentKey, &child), "fixture: the marker landed")

	calls := 0
	s.batchOperateFn = func(policy *aerospike.BatchPolicy, records []aerospike.BatchRecordIfc) error {
		calls++

		if calls == 1 {
			for _, rec := range records {
				br := rec.BatchRec()
				br.ResultCode = types.TIMEOUT
				br.Err = &aerospike.AerospikeError{ResultCode: types.TIMEOUT}
			}

			return nil
		}

		return client.BatchOperate(policy, records)
	}

	s.withdrawMarkers(updates, map[chainhash.Hash]struct{}{child: {}})
	require.Equal(t, 2, calls, "the failed withdrawal is sent once more")
	require.False(t, hasMarker(t, client, parentKey, &child), "the resend removed the marker")
}

// TestWithdrawMarkers_ResendsAfterABatchThatStoppedEarly: a batch-level error
// that is not a transport failure (MAX_ERROR_RATE) can return before the server
// answers any record. Those records keep the client's initial NO_RESPONSE
// result with no per-record error, and must be sent again, not counted refused.
func TestWithdrawMarkers_ResendsAfterABatchThatStoppedEarly(t *testing.T) {
	s, client := newFlushTestService(t)

	parent := chainhash.HashH([]byte{0xF4, 1})
	child := chainhash.HashH([]byte{0xF4, 2})
	parentKey := putRecord(t, client, &parent, spentBy(&child))

	updates := make(map[string]*parentUpdateInfo)
	require.NoError(t, s.addParentUpdatesForInput(updates, &parent, 0, &child))

	_, err := s.markParents(context.Background(), updates)
	require.NoError(t, err)
	require.True(t, hasMarker(t, client, parentKey, &child), "fixture: the marker landed")

	calls := 0
	s.batchOperateFn = func(policy *aerospike.BatchPolicy, records []aerospike.BatchRecordIfc) error {
		calls++

		if calls == 1 {
			for _, rec := range records {
				br := rec.BatchRec()
				require.Equal(t, types.NO_RESPONSE, br.ResultCode, "fixture: an unanswered record")
				require.Nil(t, br.Err, "fixture: an unanswered record carries no error")
			}

			return &aerospike.AerospikeError{ResultCode: types.MAX_ERROR_RATE}
		}

		return client.BatchOperate(policy, records)
	}

	s.withdrawMarkers(updates, map[chainhash.Hash]struct{}{child: {}})
	require.Equal(t, 2, calls, "the unanswered withdrawal is sent once more")
	require.False(t, hasMarker(t, client, parentKey, &child), "the resend removed the marker")
}

// TestIsTransientWriteFailure_UnansweredRecord: a batch write the server never
// answered keeps the client's initial result, NO_RESPONSE with no error, and is
// resent. An answered refusal is not.
func TestIsTransientWriteFailure_UnansweredRecord(t *testing.T) {
	key, err := aerospike.NewKey("test", "set", "k")
	require.NoError(t, err)

	unanswered := aerospike.NewBatchWrite(nil, key).BatchRec()
	require.Equal(t, types.NO_RESPONSE, unanswered.ResultCode, "fixture: the client's initial result")
	require.Nil(t, unanswered.Err)
	require.True(t, isTransientWriteFailure(unanswered))

	refused := aerospike.NewBatchWrite(nil, key).BatchRec()
	refused.ResultCode = types.BIN_TYPE_ERROR
	refused.Err = &aerospike.AerospikeError{ResultCode: types.BIN_TYPE_ERROR}
	require.False(t, isTransientWriteFailure(refused))

	// Every lasting refusal, named one by one so dropping any from
	// lastingWriteRefusals fails here.
	for _, code := range []types.ResultCode{
		types.TYPE_NOT_SUPPORTED, types.PARAMETER_ERROR, types.ALWAYS_FORBIDDEN, types.BIN_TYPE_ERROR,
		types.RECORD_TOO_BIG, types.UNSUPPORTED_FEATURE, types.INVALID_NAMESPACE, types.BIN_NAME_TOO_LONG,
		types.ENTERPRISE_ONLY, types.OP_NOT_APPLICABLE, types.FILTERED_OUT, types.NOT_AUTHENTICATED,
		types.ROLE_VIOLATION, types.NOT_WHITELISTED,
	} {
		rec := aerospike.NewBatchWrite(nil, key).BatchRec()
		rec.ResultCode = code
		rec.Err = &aerospike.AerospikeError{ResultCode: code}
		require.False(t, isTransientWriteFailure(rec), "code %v is a lasting refusal", code)

		rec.Err = nil
		require.False(t, isTransientWriteFailure(rec), "code %v with no error is a lasting refusal", code)
	}

	require.False(t, isBatchTransportError(&aerospike.AerospikeError{ResultCode: types.MAX_ERROR_RATE}),
		"MAX_ERROR_RATE leaves per-record results to read, so it is not resent wholesale")

	// A partition with no live node: the client stamps INVALID_NODE_ERROR per
	// record, with a fresh error and no cause chain, and the server answers
	// PARTITION_UNAVAILABLE. Those, and any code not known to be a lasting
	// refusal, are resent.
	for _, code := range []types.ResultCode{types.INVALID_NODE_ERROR, types.PARTITION_UNAVAILABLE, types.FAIL_FORBIDDEN, types.QUERY_ABORTED} {
		rec := aerospike.NewBatchWrite(nil, key).BatchRec()
		rec.ResultCode = code
		rec.Err = &aerospike.AerospikeError{ResultCode: code}
		require.True(t, isTransientWriteFailure(rec), "code %v", code)

		rec.Err = nil
		require.True(t, isTransientWriteFailure(rec), "code %v with no error", code)
	}
}

// TestWithdrawMarkers_ResendsToAParentWhoseMarkerOutcomeWasUnread: a parent
// whose marker write was counted failed may still hold the marker, when the
// failure was an outcome the pruner could not read. Its withdrawal is resent
// like any other, not sent once.
func TestWithdrawMarkers_ResendsToAParentWhoseMarkerOutcomeWasUnread(t *testing.T) {
	s, client := newFlushTestService(t)

	parent := chainhash.HashH([]byte{0xF5, 1})
	child := chainhash.HashH([]byte{0xF5, 2})
	parentKey := putRecord(t, client, &parent, spentBy(&child))

	updates := make(map[string]*parentUpdateInfo)
	require.NoError(t, s.addParentUpdatesForInput(updates, &parent, 0, &child))

	_, err := s.markParents(context.Background(), updates)
	require.NoError(t, err)
	require.True(t, hasMarker(t, client, parentKey, &child), "fixture: the marker landed")

	for _, info := range updates {
		info.markerFailed = true // the write landed, but its answer was lost
	}

	calls := 0
	s.batchOperateFn = func(policy *aerospike.BatchPolicy, records []aerospike.BatchRecordIfc) error {
		calls++

		if calls == 1 {
			return &aerospike.AerospikeError{ResultCode: types.TIMEOUT}
		}

		return client.BatchOperate(policy, records)
	}

	s.withdrawMarkers(updates, map[chainhash.Hash]struct{}{child: {}})
	require.Equal(t, 2, calls, "the failed withdrawal is sent once more")
	require.False(t, hasMarker(t, client, parentKey, &child), "the resend removed the marker")
}

// TestWithdrawMarkers_UnroutableKeyDoesNotBlockTheOthers: the client stamps a
// key it cannot route to a node INVALID_NODE_ERROR, and under the default
// RespondAllKeys it then returns before sending anything (client.go
// BatchOperate). The stub models exactly that. The unroutable key is resent,
// so the other parent's withdrawal must still be sent past it.
func TestWithdrawMarkers_UnroutableKeyDoesNotBlockTheOthers(t *testing.T) {
	s, client := newFlushTestService(t)

	down := chainhash.HashH([]byte{0xF6, 1})
	up := chainhash.HashH([]byte{0xF6, 2})
	downChild := chainhash.HashH([]byte{0xF6, 3})
	upChild := chainhash.HashH([]byte{0xF6, 4})
	downKey := putRecord(t, client, &down, spentBy(&downChild))
	upKey := putRecord(t, client, &up, spentBy(&upChild))

	updates := make(map[string]*parentUpdateInfo)
	require.NoError(t, s.addParentUpdatesForInput(updates, &down, 0, &downChild))
	require.NoError(t, s.addParentUpdatesForInput(updates, &up, 0, &upChild))

	_, err := s.markParents(context.Background(), updates)
	require.NoError(t, err)
	require.True(t, hasMarker(t, client, upKey, &upChild), "fixture: the marker landed")

	downSends := 0
	s.batchOperateFn = func(policy *aerospike.BatchPolicy, records []aerospike.BatchRecordIfc) error {
		require.True(t, policy == nil || policy.RespondAllKeys, "the withdrawal keeps the default RespondAllKeys")

		routable := make([]aerospike.BatchRecordIfc, 0, len(records))

		for _, rec := range records {
			br := rec.BatchRec()
			if br.Key.Equals(downKey) {
				downSends++
				br.ResultCode = types.INVALID_NODE_ERROR
				br.Err = &aerospike.AerospikeError{ResultCode: types.INVALID_NODE_ERROR}

				continue
			}

			routable = append(routable, rec)
		}

		if len(routable) < len(records) {
			return &aerospike.AerospikeError{ResultCode: types.INVALID_NODE_ERROR}
		}

		return client.BatchOperate(policy, routable)
	}

	s.withdrawMarkers(updates, map[chainhash.Hash]struct{}{downChild: {}, upChild: {}})
	require.Equal(t, withdrawMarkerAttempts, downSends, "the unroutable key is resent every attempt")
	require.False(t, hasMarker(t, client, upKey, &upChild), "the routable parent's marker was withdrawn")
}

// TestWithdrawMarkers_KeepsEveryParentWhenTheRoutableResendAlsoFails: an
// unsent batch lists its unroutable key first in the retry indexes, then the
// routable keys whose own resend failed, so the indexes are not ascending.
// Every parent must still be sent on the next attempt. The unroutable key is
// the last one in the first batch, so the indexes are out of order every run.
func TestWithdrawMarkers_KeepsEveryParentWhenTheRoutableResendAlsoFails(t *testing.T) {
	s, client := newFlushTestService(t)

	parents := make([]chainhash.Hash, 3)
	children := make([]chainhash.Hash, 3)
	keys := make([]*aerospike.Key, 3)
	updates := make(map[string]*parentUpdateInfo)
	withdraw := make(map[chainhash.Hash]struct{})

	for i := range parents {
		parents[i] = chainhash.HashH([]byte{0xF7, byte(i)})
		children[i] = chainhash.HashH([]byte{0xF7, byte(i), 1})
		keys[i] = putRecord(t, client, &parents[i], spentBy(&children[i]))
		require.NoError(t, s.addParentUpdatesForInput(updates, &parents[i], 0, &children[i]))
		withdraw[children[i]] = struct{}{}
	}

	_, err := s.markParents(context.Background(), updates)
	require.NoError(t, err)

	for i := range keys {
		require.True(t, hasMarker(t, client, keys[i], &children[i]), "fixture: marker %d landed", i)
	}

	var downKey *aerospike.Key

	downSends, routableSends := 0, 0
	s.batchOperateFn = func(policy *aerospike.BatchPolicy, records []aerospike.BatchRecordIfc) error {
		if downKey == nil {
			downKey = records[len(records)-1].BatchRec().Key
		}

		for _, rec := range records {
			br := rec.BatchRec()
			if br.Key.Equals(downKey) {
				downSends++
				br.ResultCode = types.INVALID_NODE_ERROR
				br.Err = &aerospike.AerospikeError{ResultCode: types.INVALID_NODE_ERROR}

				return &aerospike.AerospikeError{ResultCode: types.INVALID_NODE_ERROR}
			}
		}

		routableSends++
		if routableSends == 1 {
			return &aerospike.AerospikeError{ResultCode: types.TIMEOUT}
		}

		return client.BatchOperate(policy, records)
	}

	s.withdrawMarkers(updates, withdraw)
	require.Equal(t, withdrawMarkerAttempts, downSends, "the unroutable parent is resent every attempt")

	for i := range keys {
		if keys[i].Equals(downKey) {
			require.True(t, hasMarker(t, client, keys[i], &children[i]), "the unroutable parent keeps its marker")

			continue
		}

		require.False(t, hasMarker(t, client, keys[i], &children[i]), "routable parent %d was resent and withdrawn", i)
	}
}

// withdrawFailures installs a fresh withdrawal-failure counter for one test
// and returns a reader for it.
func withdrawFailures(t *testing.T) func() float64 {
	t.Helper()

	prev := prometheusUtxoMarkerWithdrawFailures
	counter := prometheus.NewCounter(prometheus.CounterOpts{Name: "test_marker_withdraw_failures_total"})
	prometheusUtxoMarkerWithdrawFailures = counter

	t.Cleanup(func() { prometheusUtxoMarkerWithdrawFailures = prev })

	return func() float64 { return testutil.ToFloat64(counter) }
}

// answerEveryRecord makes every pruner batch call answer each record with code,
// without reaching the server, and returns the call counter.
func answerEveryRecord(s *Service, code types.ResultCode) *int {
	calls := 0

	s.batchOperateFn = func(_ *aerospike.BatchPolicy, records []aerospike.BatchRecordIfc) error {
		calls++

		for _, rec := range records {
			batchRec := rec.BatchRec()
			batchRec.ResultCode = code
			batchRec.Err = &aerospike.AerospikeError{ResultCode: code}
		}

		return nil
	}

	return &calls
}

// TestWithdrawMarkers_LastingRefusal: a withdrawal refused with a code that
// answers the same way every time is sent once, not resent. It is counted when
// the parent's marker write landed, since that marker then stays and refuses
// the kept child's re-spend. It is not counted when the marker write itself
// failed: a record that cannot take this write did not take the marker either.
func TestWithdrawMarkers_LastingRefusal(t *testing.T) {
	for _, tc := range []struct {
		name         string
		markerFailed bool
		counted      float64
	}{
		{name: "marker landed", counted: 1},
		{name: "marker write failed", markerFailed: true, counted: 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, client := newFlushTestService(t)
			failures := withdrawFailures(t)

			parent := chainhash.HashH([]byte{0xF4, 1})
			child := chainhash.HashH([]byte{0xF4, 2})
			putRecord(t, client, &parent, spentBy(&child))

			updates := make(map[string]*parentUpdateInfo)
			require.NoError(t, s.addParentUpdatesForInput(updates, &parent, 0, &child))

			for _, info := range updates {
				info.markerFailed = tc.markerFailed
			}

			calls := answerEveryRecord(s, types.BIN_TYPE_ERROR)

			s.withdrawMarkers(updates, map[chainhash.Hash]struct{}{child: {}})
			require.Equal(t, 1, *calls, "a lasting refusal is not resent")
			require.Equal(t, tc.counted, failures())
		})
	}
}

// TestWithdrawMarkers_NoMarkerBinIsNothingToWithdraw: a parent with no
// deletedChildren bin has no marker to remove. It is not resent and not
// counted as a failure.
func TestWithdrawMarkers_NoMarkerBinIsNothingToWithdraw(t *testing.T) {
	s, client := newFlushTestService(t)
	failures := withdrawFailures(t)

	parent := chainhash.HashH([]byte{0xF5, 1})
	child := chainhash.HashH([]byte{0xF5, 2})
	putRecord(t, client, &parent, spentBy(&child))

	updates := make(map[string]*parentUpdateInfo)
	require.NoError(t, s.addParentUpdatesForInput(updates, &parent, 0, &child))

	calls := answerEveryRecord(s, types.BIN_NOT_FOUND)

	s.withdrawMarkers(updates, map[chainhash.Hash]struct{}{child: {}})
	require.Equal(t, 1, *calls, "nothing to withdraw is not resent")
	require.Zero(t, failures(), "nothing to withdraw is not a failure")
}

// TestWithdrawMarkers_StopsAfterTheLastAttempt: a withdrawal that keeps failing
// is sent withdrawMarkerAttempts times and then left, counted and logged.
func TestWithdrawMarkers_StopsAfterTheLastAttempt(t *testing.T) {
	s, client := newFlushTestService(t)
	failures := withdrawFailures(t)

	parent := chainhash.HashH([]byte{0xF3, 1})
	child := chainhash.HashH([]byte{0xF3, 2})
	parentKey := putRecord(t, client, &parent, spentBy(&child))

	updates := make(map[string]*parentUpdateInfo)
	require.NoError(t, s.addParentUpdatesForInput(updates, &parent, 0, &child))

	_, err := s.markParents(context.Background(), updates)
	require.NoError(t, err)

	calls := 0
	s.batchOperateFn = func(_ *aerospike.BatchPolicy, _ []aerospike.BatchRecordIfc) error {
		calls++

		return &aerospike.AerospikeError{ResultCode: types.TIMEOUT}
	}

	s.withdrawMarkers(updates, map[chainhash.Hash]struct{}{child: {}})
	require.Equal(t, withdrawMarkerAttempts, calls)
	require.True(t, hasMarker(t, client, parentKey, &child), "a withdrawal that never lands leaves the marker")
	require.Equal(t, float64(1), failures(), "the parent left with its marker is counted")
}

// TestDrainCarryDetached_EachBatchGetsItsOwnBudget: a worker's drain sends the
// carried master deletes, then the pages of the masters they removed. The
// delete batch may spend the client's whole overload retry budget, so the
// pages batch must still be allowed to start after it. With one deadline for
// the whole drain, the pages were never sent and the worker failed a cycle
// whose every chunk had succeeded.
func TestDrainCarryDetached_EachBatchGetsItsOwnBudget(t *testing.T) {
	s := newTestService(t)
	s.settings.Pruner.SkipDeletions = false
	s.drainFlushTimeout = 100 * time.Millisecond

	child := chainhash.HashH([]byte("drained child"))

	masterKey, err := aerospike.NewKey(s.namespace, s.set, uaerospike.CalculateKeySourceInternal(&child, 0))
	require.NoError(t, err)

	pageKey, err := aerospike.NewKey(s.namespace, s.set, uaerospike.CalculateKeySourceInternal(&child, 1))
	require.NoError(t, err)

	var batches [][]string

	s.batchOperateFn = func(_ *aerospike.BatchPolicy, records []aerospike.BatchRecordIfc) error {
		sent := make([]string, 0, len(records))

		for _, rec := range records {
			sent = append(sent, string(rec.BatchRec().Key.Digest()))
			rec.BatchRec().ResultCode = types.OK
		}

		batches = append(batches, sent)

		if len(batches) == 1 {
			// The delete batch outlasts the per-batch budget, as an overload
			// retry can.
			time.Sleep(3 * s.drainFlushTimeout)
		}

		return nil
	}

	carry := &deleteCarry{
		blockHeight: testPruneHeight,
		deletions:   []*pendingDeletion{{txHash: &child, keys: []*aerospike.Key{masterKey, pageKey}}},
		updates:     []map[string]*parentUpdateInfo{{}},
	}

	deleted, heldBack, drainErr := s.drainCarryDetached(context.Background(), carry)
	require.NoError(t, drainErr, "the pages batch starts on a budget of its own")
	require.Equal(t, 1, deleted)
	require.Zero(t, heldBack)
	require.Equal(t, [][]string{{string(masterKey.Digest())}, {string(pageKey.Digest())}}, batches, "the master goes first, then its page")
	require.True(t, carry.empty())
}

func TestDrainFlushTimeout(t *testing.T) {
	require.Equal(t, 2*time.Minute+drainFlushMargin, drainFlushTimeout(2*time.Minute), "the default overload budget plus the margin")
	require.Equal(t, drainFlushMargin, drainFlushTimeout(0), "no overload retry leaves the margin")
}

// TestMarkParents_NativeNotSpenderIsWithdrawnFrom: a native sub-op 15 write
// answers NOT_SPENDER with no error, so the client never marks it in doubt.
// A batch retry's first attempt may still have written C's marker on P before
// P's output stopped naming C. The re-read then finds the output unspent and
// holds C back. C's marker must come off P too, although the re-read stripped
// C from P's entry: a marker left on P would refuse C's idempotent re-spend.
func TestMarkParents_NativeNotSpenderIsWithdrawnFrom(t *testing.T) {
	s, client := newFlushTestService(t)
	s.buildVerifiedMarkerRecord = testNativeMarkerRecord

	parent := chainhash.HashH([]byte{0xF1, 1})
	child := chainhash.HashH([]byte{0xF1, 2})

	// The output is unspent now, but the retried first attempt left C's marker.
	parentKey := putRecord(t, client, &parent, aerospike.BinMap{
		fields.Utxos.String():           []interface{}{make([]byte, 32)},
		fields.DeletedChildren.String(): map[interface{}]interface{}{child.String(): true},
	})

	updates := make(map[string]*parentUpdateInfo)
	require.NoError(t, s.addParentUpdatesForInput(updates, &parent, 0, &child))

	notSpender := map[interface{}]interface{}{"status": "ERROR", "errorCode": "NOT_SPENDER", "message": "not the spender"}

	call := 0
	s.batchOperateFn = func(policy *aerospike.BatchPolicy, records []aerospike.BatchRecordIfc) error {
		call++

		if call == 1 {
			for _, rec := range records {
				nativeAnswer(rec.BatchRec(), notSpender, nil)
			}

			return nil
		}

		return client.BatchOperate(policy, records)
	}

	plan, err := s.markParents(context.Background(), updates)
	require.NoError(t, err)
	require.Contains(t, plan.blocked, child, "the re-read finds the output unspent and holds the child back")
	require.False(t, hasMarker(t, client, parentKey, &child), "the marker a retried first attempt may have left is withdrawn")
}

// TestMarkParents_NativeNotSpenderTwiceKeepsEveryChild: children A and B claim
// outputs 0 and 1 of P, and the native write to P answers NOT_SPENDER on both
// sends. A retried first attempt may have written both markers. The re-read
// between the sends finds output 0 unspent, strips A, and holds it back, so the
// second send carries only B. The copy noteNativeRefusal keeps must still name
// A, or A's marker stays on P and refuses A's idempotent re-spend.
func TestMarkParents_NativeNotSpenderTwiceKeepsEveryChild(t *testing.T) {
	s, client := newFlushTestService(t)
	s.buildVerifiedMarkerRecord = testNativeMarkerRecord

	parent := chainhash.HashH([]byte{0xF2, 1})
	childA := chainhash.HashH([]byte{0xF2, 2})
	childB := chainhash.HashH([]byte{0xF2, 3})

	parentKey := putRecord(t, client, &parent, aerospike.BinMap{
		fields.Utxos.String():           []interface{}{make([]byte, 32), spentUtxoElement(&childB)},
		fields.DeletedChildren.String(): map[interface{}]interface{}{childA.String(): true, childB.String(): true},
	})

	updates := make(map[string]*parentUpdateInfo)
	require.NoError(t, s.addParentUpdatesForInput(updates, &parent, 0, &childA))
	require.NoError(t, s.addParentUpdatesForInput(updates, &parent, 1, &childB))

	notSpender := map[interface{}]interface{}{"status": "ERROR", "errorCode": "NOT_SPENDER", "message": "not the spender"}

	// Batch 1 is the first marker write, 2 the re-read, 3 the second marker
	// write, and the rest are withdrawals.
	call := 0
	s.batchOperateFn = func(policy *aerospike.BatchPolicy, records []aerospike.BatchRecordIfc) error {
		call++

		if call == 1 || call == 3 {
			for _, rec := range records {
				nativeAnswer(rec.BatchRec(), notSpender, nil)
			}

			return nil
		}

		return client.BatchOperate(policy, records)
	}

	plan, err := s.markParents(context.Background(), updates)
	require.NoError(t, err)
	require.GreaterOrEqual(t, call, 3, "fixture: the record was written twice")
	require.Contains(t, plan.blocked, childA, "the re-read holds A back")
	require.Contains(t, plan.blocked, childB, "the second refusal holds B back")
	require.False(t, hasMarker(t, client, parentKey, &childA), "A's marker comes off P although the second send no longer carried A")
	require.False(t, hasMarker(t, client, parentKey, &childB))
}

// refuseNativeMarkerWritesTo answers the sub-op 15 marker write to each key in refused as
// the server does for a record whose output names another spender (NOT_SPENDER), the
// ones in written as OK, and sends every other record of the first batch through the
// real client. Later batches all go to the client.
func refuseNativeMarkerWritesTo(s *Service, client *uaerospike.Client, refused, written []*aerospike.Key) {
	notSpender := map[interface{}]interface{}{"status": "ERROR", "errorCode": "NOT_SPENDER", "message": "not the spender"}
	ok := map[interface{}]interface{}{"status": "OK"}

	in := func(keys []*aerospike.Key, key *aerospike.Key) bool {
		for _, k := range keys {
			if string(k.Digest()) == string(key.Digest()) {
				return true
			}
		}

		return false
	}

	first := true

	s.batchOperateFn = func(policy *aerospike.BatchPolicy, records []aerospike.BatchRecordIfc) error {
		if !first {
			return client.BatchOperate(policy, records)
		}

		first = false

		var real []aerospike.BatchRecordIfc

		for _, rec := range records {
			switch key := rec.BatchRec().Key; {
			case in(refused, key):
				nativeAnswer(rec.BatchRec(), notSpender, nil)
			case in(written, key):
				nativeAnswer(rec.BatchRec(), ok, nil)
			default:
				real = append(real, rec)
			}
		}

		if len(real) == 0 {
			return nil
		}

		return client.BatchOperate(policy, real)
	}
}

// TestFlushChunk_RefusedCarriedDeleteWithdrawsANativeRefusedMarker: child A is queued on
// parents P and Q. The native marker write to P answers NOT_SPENDER, after a retried
// first attempt has already left A's marker on P, because P's output now names another
// spender. The re-read strips A from P's entry, and A is not held back, so it is cleared
// and carried. When its delete is then refused (a reorg cleared its deleteAtHeight), the
// withdrawal must still reach P: the carry has to keep the copy of P's entry the plan
// made before the strip, or A's marker stays on P for good. A is present and unmined by
// then and no later scan finds it, and the marker would refuse A's idempotent re-spend.
func TestFlushChunk_RefusedCarriedDeleteWithdrawsANativeRefusedMarker(t *testing.T) {
	s, client := newFlushTestService(t)
	s.buildVerifiedMarkerRecord = testNativeMarkerRecord

	ctx := context.Background()

	p := chainhash.HashH([]byte{0xF3, 1})
	q := chainhash.HashH([]byte{0xF3, 2})
	a := chainhash.HashH([]byte{0xF3, 3})
	other := chainhash.HashH([]byte{0xF3, 4})

	// P's output names another spender, with A's marker left by the retried attempt.
	pKey := putRecord(t, client, &p, aerospike.BinMap{
		fields.Utxos.String():           []interface{}{spentUtxoElement(&other)},
		fields.DeletedChildren.String(): map[interface{}]interface{}{a.String(): true},
	})
	qKey := putRecord(t, client, &q, spentBy(&a))
	aKey := putRecord(t, client, &a, aerospike.BinMap{fields.TxID.String(): a.CloneBytes()})

	updates := make(map[string]*parentUpdateInfo)
	require.NoError(t, s.addParentUpdatesForInput(updates, &p, 0, &a))
	require.NoError(t, s.addParentUpdatesForInput(updates, &q, 0, &a))

	refuseNativeMarkerWritesTo(s, client, []*aerospike.Key{pKey}, []*aerospike.Key{qKey})

	carry := &deleteCarry{blockHeight: testPruneHeight}

	_, heldBack, err := s.flushChunk(ctx, carry, updates, []*pendingDeletion{{txHash: &a, keys: []*aerospike.Key{aKey}}}, nil)
	require.NoError(t, err)
	require.Zero(t, heldBack, "the re-read does not hold A back: P's output names another spender")
	require.True(t, hasMarker(t, client, pKey, &a), "fixture: the retried attempt's marker is still on P")

	// A reorg clears A's deleteAtHeight before its carried delete goes out.
	require.NoError(t, client.Put(nil, aKey, aerospike.BinMap{fields.DeleteAtHeight.String(): nil}))

	deleted, heldBack, err := s.drainCarry(ctx, carry)
	require.NoError(t, err, "a refused delete of a changed child is not a failure")
	require.Zero(t, deleted)
	require.Equal(t, 1, heldBack, "A is spared")

	_, err = client.Get(nil, aKey)
	require.NoError(t, err, "A is still present")
	require.False(t, hasMarker(t, client, pKey, &a), "A's marker comes off P with the refused delete, although the re-read stripped A from P's entry")
}

// TestFlushChunk_TiedBlockedChildWithdrawsANativeRefusedMarker: child A is queued on R and
// on P, and child B on R as well. The batch also deletes P, so A's marker on P waits for
// that delete. The native marker write to R answers NOT_SPENDER with A's marker already
// left there by a retried attempt: R's output 0 names another spender and its output 1
// names B. The re-read strips A from R's entry, which keeps B, and the second write to R
// carries B only. P's delete is refused, so A is held back and its marker must come off R,
// which only the plan's copy of R's entry still lists.
func TestFlushChunk_TiedBlockedChildWithdrawsANativeRefusedMarker(t *testing.T) {
	s, client := newFlushTestService(t)
	s.buildVerifiedMarkerRecord = testNativeMarkerRecord

	ctx := context.Background()

	r := chainhash.HashH([]byte{0xF4, 1})
	p := chainhash.HashH([]byte{0xF4, 2})
	a := chainhash.HashH([]byte{0xF4, 3})
	b := chainhash.HashH([]byte{0xF4, 5})
	other := chainhash.HashH([]byte{0xF4, 4})

	rKey := putRecord(t, client, &r, aerospike.BinMap{
		fields.Utxos.String():           []interface{}{spentUtxoElement(&other), spentUtxoElement(&b)},
		fields.DeletedChildren.String(): map[interface{}]interface{}{a.String(): true},
	})
	pKey := putRecord(t, client, &p, spentBy(&a))
	aKey := putRecord(t, client, &a, aerospike.BinMap{fields.TxID.String(): a.CloneBytes()})
	bKey := putRecord(t, client, &b, aerospike.BinMap{fields.TxID.String(): b.CloneBytes()})

	updates := make(map[string]*parentUpdateInfo)
	require.NoError(t, s.addParentUpdatesForInput(updates, &r, 0, &a))
	require.NoError(t, s.addParentUpdatesForInput(updates, &r, 1, &b))
	require.NoError(t, s.addParentUpdatesForInput(updates, &p, 0, &a))

	// P's carried delete is refused: a reorg cleared its deleteAtHeight.
	require.NoError(t, client.Put(nil, pKey, aerospike.BinMap{fields.DeleteAtHeight.String(): nil}))

	notSpender := map[interface{}]interface{}{"status": "ERROR", "errorCode": "NOT_SPENDER", "message": "not the spender"}
	ok := map[interface{}]interface{}{"status": "OK"}

	// Batch 1 carries R's marker write and P's delete, 2 is the re-read, 3 is the second
	// write to R. R's writes are answered as a native server would, the rest is real.
	call := 0
	s.batchOperateFn = func(policy *aerospike.BatchPolicy, records []aerospike.BatchRecordIfc) error {
		call++

		if call != 1 && call != 3 {
			return client.BatchOperate(policy, records)
		}

		var real []aerospike.BatchRecordIfc

		for _, rec := range records {
			switch {
			case call == 3:
				nativeAnswer(rec.BatchRec(), ok, nil)
			case string(rec.BatchRec().Key.Digest()) == string(rKey.Digest()):
				nativeAnswer(rec.BatchRec(), notSpender, nil)
			default:
				real = append(real, rec)
			}
		}

		if len(real) == 0 {
			return nil
		}

		return client.BatchOperate(policy, real)
	}

	carry := &deleteCarry{
		blockHeight: testPruneHeight,
		deletions:   []*pendingDeletion{{txHash: &p, keys: []*aerospike.Key{pKey}}},
		updates:     []map[string]*parentUpdateInfo{{}},
	}

	_, heldBack, err := s.flushChunk(ctx, carry, updates, []*pendingDeletion{
		{txHash: &a, keys: []*aerospike.Key{aKey}},
		{txHash: &b, keys: []*aerospike.Key{bKey}},
	}, nil)
	require.NoError(t, err)
	require.GreaterOrEqual(t, call, 3, "fixture: R was written twice")
	require.Equal(t, 2, heldBack, "P's refused delete and A, which waits on it")

	_, getErr := client.Get(nil, pKey)
	require.NoError(t, getErr, "fixture: P is still present")
	require.False(t, hasMarker(t, client, rKey, &a), "A's marker comes off R, which the re-read stripped A from")
}

// TestFlushChunk_BlobOfAnUnansweredDeleteStays: a master delete the server never answered
// (NO_RESPONSE, or in doubt) may or may not have landed, so the child's blob must stay.
// The next cycle re-finds the record if it is still there; a blob deleted under a record
// that survived would be an orphaned pointer.
func TestFlushChunk_BlobOfAnUnansweredDeleteStays(t *testing.T) {
	s, client := newFlushTestService(t)

	ctx := context.Background()

	child := chainhash.HashH([]byte("unanswered-blob-child"))
	childKey := putRecord(t, client, &child, aerospike.BinMap{fields.TxID.String(): child.CloneBytes()})

	require.NoError(t, s.external.Set(ctx, child.CloneBytes(), fileformat.FileTypeTx, []byte("blob")))

	s.batchOperateFn = func(_ *aerospike.BatchPolicy, records []aerospike.BatchRecordIfc) error {
		for _, rec := range records {
			br := rec.BatchRec()
			br.ResultCode = types.NO_RESPONSE
			br.InDoubt = true
			br.Err = &aerospike.AerospikeError{ResultCode: types.NO_RESPONSE, InDoubt: true}
		}

		return nil
	}

	_, flushErr := s.flushCleanupBatches(ctx,
		map[string]*parentUpdateInfo{},
		[]*pendingDeletion{{txHash: &child, keys: []*aerospike.Key{childKey}}},
		[]*externalFileInfo{{txHash: &child, fileType: fileformat.FileTypeTx}})
	require.Error(t, flushErr, "an unanswered delete fails the batch")

	blobExists, err := s.external.Exists(ctx, child.CloneBytes(), fileformat.FileTypeTx)
	require.NoError(t, err)
	require.True(t, blobExists, "the blob of a child whose master delete went unanswered must stay")
}

// TestFlushChunk_PageWhoseMasterReadFailedIsKept: a pagination record is deleted only once
// its master is known to be gone. A master read that failed with anything but
// KEY_NOT_FOUND (a timeout, an overloaded node) says nothing about the master, so the page
// must not be deleted and not carried: the next cycle finds it again.
func TestFlushChunk_PageWhoseMasterReadFailedIsKept(t *testing.T) {
	s, _ := newFlushTestService(t)

	ctx := context.Background()

	child := chainhash.HashH([]byte("page-master-read-failed"))

	pageKey, keyErr := aerospike.NewKey(s.namespace, s.set, uaerospike.CalculateKeySourceInternal(&child, 1))
	require.NoError(t, keyErr)

	s.batchOperateFn = func(policy *aerospike.BatchPolicy, records []aerospike.BatchRecordIfc) error {
		for _, rec := range records {
			br := rec.BatchRec()
			br.ResultCode = types.TIMEOUT
			br.Err = &aerospike.AerospikeError{ResultCode: types.TIMEOUT}
		}

		return nil
	}

	carry := &deleteCarry{blockHeight: testPruneHeight}

	deleted, heldBack, err := s.flushChunk(ctx, carry, map[string]*parentUpdateInfo{},
		[]*pendingDeletion{{txHash: &child, keys: []*aerospike.Key{pageKey}, page: true}}, nil)
	require.NoError(t, err)
	require.Zero(t, deleted)
	require.Equal(t, 1, heldBack, "the page whose master could not be read is counted as held back")
	require.True(t, carry.empty(), "and is not carried to be deleted")
}

// TestMarkParents_TransportFailureWithdrawsEveryMarker: a marker batch that went out and
// then failed in transport may have written any of its markers, so every child's marker is
// withdrawn from every record. A marker left on a child that stays would refuse the child's
// idempotent re-spend.
func TestMarkParents_TransportFailureWithdrawsEveryMarker(t *testing.T) {
	s, client := newFlushTestService(t)

	parent := chainhash.HashH([]byte{0xF5, 1})
	child := chainhash.HashH([]byte{0xF5, 2})
	parentKey := putRecord(t, client, &parent, spentBy(&child))

	updates := make(map[string]*parentUpdateInfo)
	require.NoError(t, s.addParentUpdatesForInput(updates, &parent, 0, &child))

	call := 0
	s.batchOperateFn = func(policy *aerospike.BatchPolicy, records []aerospike.BatchRecordIfc) error {
		call++

		if err := client.BatchOperate(policy, records); err != nil {
			return err
		}

		if call == 1 {
			// The batch landed, and the answer was lost on the way back.
			return &aerospike.AerospikeError{ResultCode: types.TIMEOUT}
		}

		return nil
	}

	_, err := s.markParents(context.Background(), updates)
	require.Error(t, err, "the transport failure is returned")
	require.False(t, hasMarker(t, client, parentKey, &child), "a marker the lost batch may have written is withdrawn")
}
