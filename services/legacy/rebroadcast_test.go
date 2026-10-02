package legacy

import (
	"context"
	"net/url"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-wire"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/bsv-blockchain/teranode/stores/utxo/meta"
	utxosql "github.com/bsv-blockchain/teranode/stores/utxo/sql"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func testTxInv(b byte) wire.InvVect {
	return wire.InvVect{Type: wire.InvTypeTx, Hash: chainhash.Hash{b}}
}

// mustAdd adds iv to q and fails unless it was added without evicting.
func mustAdd(t *testing.T, q *rebroadcastQueue, iv wire.InvVect, data interface{}) {
	t.Helper()

	added, evicted := q.add(iv, data)
	require.True(t, added, "add of %v must be accepted", iv.Hash)
	require.False(t, evicted, "add of %v must not evict", iv.Hash)
}

// retryBatch runs one retry and returns the batch handed to relay.
func retryBatch(q *rebroadcastQueue, maxTips int) (batch []relayMsg, agedOut int) {
	_, agedOut = q.retry(maxTips, func(msgs []relayMsg) { batch = msgs })
	return batch, agedOut
}

// TestRebroadcastQueue_ReaddKeepsPositionAndBudget locks in that re-adding an
// iv already in the queue does not reset its retry budget or move it to the
// back. Otherwise a Kafka replay could refresh the budget of a tx that should
// have aged out, or put a parent behind its children.
func TestRebroadcastQueue_ReaddKeepsPositionAndBudget(t *testing.T) {
	q := newRebroadcastQueue(10)

	mustAdd(t, q, testTxInv(1), "first")
	mustAdd(t, q, testTxInv(2), "other")

	_, _ = retryBatch(q, 10)
	_, _ = retryBatch(q, 10)

	mustAdd(t, q, testTxInv(1), "second")
	require.Equal(t, 2, q.len())

	entries := q.entries()
	require.Equal(t, testTxInv(1), entries[0].iv, "re-add must keep the entry's position")
	require.Equal(t, 2, entries[0].tips, "re-add must keep the entry's budget")
	require.Equal(t, "second", entries[0].data, "re-add must refresh the data payload")
}

// TestRebroadcastQueue_EvictsFreshEntriesAtCap covers the cap policy from
// review: fresh txs alone fill the queue between blocks at modest tx rates,
// so at the cap a new add evicts the oldest entry not retried yet. Entries
// that have been retried, the likely stuck ones, keep their place, and a new
// add is only dropped when every entry has been retried.
func TestRebroadcastQueue_EvictsFreshEntriesAtCap(t *testing.T) {
	const capacity = 4

	q := newRebroadcastQueue(capacity)

	// Two entries survive a retry, two fresh ones follow.
	mustAdd(t, q, testTxInv(1), nil)
	mustAdd(t, q, testTxInv(2), nil)
	_, _ = retryBatch(q, 10)
	mustAdd(t, q, testTxInv(3), nil)
	mustAdd(t, q, testTxInv(4), nil)

	added, evicted := q.add(testTxInv(5), nil)
	require.True(t, added)
	require.True(t, evicted, "a full queue with fresh entries must evict one")
	require.Equal(t, []wire.InvVect{testTxInv(1), testTxInv(2), testTxInv(4), testTxInv(5)}, q.ivs(),
		"the oldest fresh entry must go, retried entries must stay")

	added, evicted = q.add(testTxInv(6), nil)
	require.True(t, added)
	require.True(t, evicted)
	require.Equal(t, []wire.InvVect{testTxInv(1), testTxInv(2), testTxInv(5), testTxInv(6)}, q.ivs())

	// An update of an existing entry never evicts.
	added, evicted = q.add(testTxInv(1), "updated")
	require.True(t, added)
	require.False(t, evicted)
	require.Equal(t, "updated", q.entries()[0].data)

	// Once every entry has been retried, a new add is dropped.
	_, _ = retryBatch(q, 10)

	added, evicted = q.add(testTxInv(7), nil)
	require.False(t, added, "a full queue of retried entries must drop the new add")
	require.False(t, evicted)
	require.Equal(t, capacity, q.len())

	q.remove(testTxInv(2))
	mustAdd(t, q, testTxInv(7), nil)
}

// TestRebroadcastQueue_FreshTailSurvivesRemovals checks the fresh-tail
// pointer when the first fresh entry is removed or pruned.
func TestRebroadcastQueue_FreshTailSurvivesRemovals(t *testing.T) {
	q := newRebroadcastQueue(3)

	mustAdd(t, q, testTxInv(1), nil)
	_, _ = retryBatch(q, 10)
	mustAdd(t, q, testTxInv(2), nil)
	mustAdd(t, q, testTxInv(3), nil)

	// Removing the first fresh entry moves the tail start to the next one.
	q.remove(testTxInv(2))
	mustAdd(t, q, testTxInv(4), nil)

	added, evicted := q.add(testTxInv(5), nil)
	require.True(t, added)
	require.True(t, evicted)
	require.Equal(t, []wire.InvVect{testTxInv(1), testTxInv(4), testTxInv(5)}, q.ivs())

	// Removing every fresh entry leaves nothing to evict.
	q.remove(testTxInv(4))
	q.remove(testTxInv(5))
	require.Nil(t, q.firstFresh)

	mustAdd(t, q, testTxInv(6), nil)
	require.Equal(t, testTxInv(6), q.firstFresh.Value.(*rebroadcastEntry).iv)
}

// TestRebroadcastQueue_RetryKeepsInsertionOrder checks that entries with no
// known dependency between them go out in queue order, not map order.
func TestRebroadcastQueue_RetryKeepsInsertionOrder(t *testing.T) {
	const n = 200

	q := newRebroadcastQueue(n)

	// Insert in a hash order that differs from insertion order, so a
	// map-ordered implementation would not pass by chance.
	want := make([]wire.InvVect, 0, n)
	for i := 0; i < n; i++ {
		iv := wire.InvVect{Type: wire.InvTypeTx, Hash: chainhash.Hash{byte(n - i), byte(i * 7)}}
		mustAdd(t, q, iv, i)

		want = append(want, iv)
	}

	q.remove(want[10])
	want = append(want[:10], want[11:]...)

	for retry := 0; retry < 2; retry++ {
		batch, _ := retryBatch(q, 10)
		require.Len(t, batch, len(want))

		for i, msg := range batch {
			require.Equal(t, want[i], *msg.invVect, "retry %d position %d out of order", retry, i)
			require.True(t, msg.requeue, "rebroadcasts must bypass the peers' known inventory")
		}
	}
}

// TestRebroadcastQueue_RetrySortsParentsFirst covers the parent-first
// requirement from issue 1826: sending the parent first avoids relying on the
// peer's bounded orphan pool, and txmeta Kafka reads can queue a child before
// its parent.
func TestRebroadcastQueue_RetrySortsParentsFirst(t *testing.T) {
	q := newRebroadcastQueue(10)

	root, child, grandchild, unrelated := testTxInv(1), testTxInv(2), testTxInv(3), testTxInv(4)

	// Queued in the worst order a partitioned read could give.
	for _, iv := range []wire.InvVect{grandchild, unrelated, child, root} {
		mustAdd(t, q, iv, nil)
	}

	parents := map[wire.InvVect][]chainhash.Hash{
		grandchild: {child.Hash, {0xaa}}, // second parent is not queued
		child:      {root.Hash},
	}
	for _, entry := range q.entries() {
		entry.parents = parents[entry.iv]
	}

	batch, _ := retryBatch(q, 10)

	got := make([]wire.InvVect, len(batch))
	for i, msg := range batch {
		got[i] = *msg.invVect
	}

	require.Equal(t, []wire.InvVect{root, child, grandchild, unrelated}, got)
}

// TestRebroadcastQueue_AgesOutAfterMaxTips asserts the per-entry budget: an
// entry is offered on exactly maxTips retries and then removed.
func TestRebroadcastQueue_AgesOutAfterMaxTips(t *testing.T) {
	const maxTips = 3

	q := newRebroadcastQueue(10)
	mustAdd(t, q, testTxInv(1), "data")

	for i := 1; i < maxTips; i++ {
		batch, agedOut := retryBatch(q, maxTips)
		require.Len(t, batch, 1)
		require.Zero(t, agedOut)
		require.Equal(t, 1, q.len(), "entry must remain after retry %d", i)
	}

	batch, agedOut := retryBatch(q, maxTips)
	require.Len(t, batch, 1, "the last retry must still offer the entry")
	require.Equal(t, 1, agedOut)
	require.Zero(t, q.len(), "entry must age out after maxTips retries")

	relayed, agedOut := q.retry(maxTips, func([]relayMsg) { t.Fatal("empty queue must not relay") })
	require.Zero(t, relayed)
	require.Zero(t, agedOut)
}

// TestPruneRebroadcastQueue checks that mined, conflicting and unknown txs are
// dropped before a retry, while unmined txs stay queued in order.
func TestPruneRebroadcastQueue(t *testing.T) {
	ctx := context.Background()
	tSettings := test.CreateBaseTestSettings(t)

	storeURL, err := url.Parse("sqlitememory:///")
	require.NoError(t, err)

	store, err := utxosql.New(ctx, ulogger.TestLogger{}, tSettings, storeURL)
	require.NoError(t, err)

	newTx := func(satoshis uint64, opts ...utxo.CreateOption) wire.InvVect {
		tx := bt.NewTx()
		require.NoError(t, tx.AddP2PKHOutputFromAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", satoshis))

		_, err := store.Create(ctx, tx, 100, opts...)
		require.NoError(t, err)

		return wire.InvVect{Type: wire.InvTypeTx, Hash: *tx.TxIDChainHash()}
	}

	unminedA := newTx(1000)

	parentTx := bt.NewTx()
	require.NoError(t, parentTx.AddP2PKHOutputFromAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 5000))

	childTx := bt.NewTx()
	require.NoError(t, childTx.From(parentTx.TxID(), 0, parentTx.Outputs[0].LockingScript.String(), 5000))
	require.NoError(t, childTx.AddP2PKHOutputFromAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 4000))
	childTx.Inputs[0].UnlockingScript = bscript.NewFromBytes([]byte{0x51})

	_, err = store.Create(ctx, childTx, 100)
	require.NoError(t, err)

	withParent := wire.InvVect{Type: wire.InvTypeTx, Hash: *childTx.TxIDChainHash()}
	mined := newTx(2000, utxo.WithMinedBlockInfo(utxo.MinedBlockInfo{BlockID: 1, BlockHeight: 100}))
	conflicting := newTx(3000, utxo.WithConflicting(true))
	unminedB := newTx(4000)
	notFound := testTxInv(0xee)

	q := newRebroadcastQueue(10)
	for _, iv := range []wire.InvVect{unminedA, mined, conflicting, notFound, unminedB, withParent} {
		mustAdd(t, q, iv, nil)
	}

	lookup, err := lookupRebroadcasts(ctx, store, q.ivs())
	require.NoError(t, err)
	require.Equal(t, 6, q.len(), "the lookup must not touch the queue")

	// An entry re-added while the lookup ran moves to the back and still gets its result.
	q.remove(unminedB)
	mustAdd(t, q, unminedB, nil)

	result := q.applyLookup(lookup)
	require.Equal(t, rebroadcastPruneResult{mined: 1, conflicting: 1, notFound: 1}, result)

	entries := q.entries()
	require.Len(t, entries, 3)
	require.Equal(t, unminedA, entries[0].iv)
	require.Equal(t, withParent, entries[1].iv)
	require.Equal(t, unminedB, entries[2].iv)

	require.Empty(t, entries[0].parents)
	require.Equal(t, []chainhash.Hash{*parentTx.TxIDChainHash()}, entries[1].parents,
		"the lookup must record the parents retry orders by")

	t.Run("nil store keeps every entry", func(t *testing.T) {
		lookup, err := lookupRebroadcasts(ctx, nil, q.ivs())
		require.NoError(t, err)
		require.Zero(t, q.applyLookup(lookup))
		require.Equal(t, 3, q.len())
	})

	t.Run("an entry removed during the lookup is not touched", func(t *testing.T) {
		gone := newRebroadcastQueue(10)
		require.Zero(t, gone.applyLookup(lookup))
		require.Zero(t, gone.len())
	})
}

// TestRebroadcastHandler_RetriesOncePerBlock drives the handler loop: nothing
// is retried until a block arrives, blocks arriving during the post-block
// delay share one retry, and each later block triggers another.
func TestRebroadcastHandler_RetriesOncePerBlock(t *testing.T) {
	s := &server{
		ctx:                  context.Background(),
		logger:               ulogger.TestLogger{},
		modifyRebroadcastInv: make(chan interface{}, modifyRebroadcastInvBuffer),
		rebroadcastTip:       make(chan struct{}, 1),
		rebroadcastTipDelay:  50 * time.Millisecond,
		relayInv:             make(chan relayMsg, 16),
		quit:                 make(chan struct{}),
	}

	s.wg.Add(1)

	go s.rebroadcastHandler()

	t.Cleanup(func() {
		close(s.quit)
		s.wg.Wait()
	})

	iv1, iv2 := testTxInv(1), testTxInv(2)
	s.AddRebroadcastInventory(&iv1, "one")
	s.AddRebroadcastInventory(&iv2, "two")

	expectRetry := func(msg string) {
		t.Helper()

		for _, want := range []wire.InvVect{iv1, iv2} {
			select {
			case got := <-s.relayInv:
				require.Equal(t, want, *got.invVect, msg)
				require.True(t, got.requeue, msg)
			case <-time.After(2 * time.Second):
				t.Fatal(msg)
			}
		}
	}

	expectNoRetry := func(msg string) {
		t.Helper()

		select {
		case got := <-s.relayInv:
			t.Fatalf("%s: unexpected relay of %v", msg, got.invVect)
		case <-time.After(200 * time.Millisecond):
		}
	}

	expectNoRetry("retried before any block")

	s.BlockConnected()
	s.BlockConnected()
	s.BlockConnected()
	expectRetry("no retry after the first blocks")
	expectNoRetry("blocks inside one delay must share one retry")

	s.BlockConnected()
	expectRetry("no retry after a later block")
}

// TestRebroadcastHandler_TakesAddsDuringSlowLookup covers the stall review
// found: the UTXO lookup before a retry used to run on the handler
// goroutine, and on Aerospike it can outlast its context. Meanwhile nothing
// drained modifyRebroadcastInv, so adds were dropped right after each block.
func TestRebroadcastHandler_TakesAddsDuringSlowLookup(t *testing.T) {
	store := &utxo.MockUtxostore{}
	lookupStarted := make(chan struct{})
	releaseLookup := make(chan struct{})

	var startOnce, releaseOnce sync.Once

	release := func() { releaseOnce.Do(func() { close(releaseLookup) }) }

	store.On("BatchDecorate", mock.Anything, mock.Anything, mock.Anything).Run(func(mock.Arguments) {
		startOnce.Do(func() { close(lookupStarted) })
		<-releaseLookup
	}).Return(nil)

	s := &server{
		ctx:    context.Background(),
		logger: ulogger.TestLogger{},
		// One slot: an add is only accepted once the handler took the last.
		modifyRebroadcastInv: make(chan interface{}, 1),
		rebroadcastTip:       make(chan struct{}, 1),
		rebroadcastTipDelay:  10 * time.Millisecond,
		relayInv:             make(chan relayMsg, 64),
		quit:                 make(chan struct{}),
		utxoStore:            store,
	}

	s.wg.Add(1)

	go s.rebroadcastHandler()

	t.Cleanup(func() {
		release()
		close(s.quit)
		s.wg.Wait()
	})

	first := testTxInv(1)
	s.AddRebroadcastInventory(&first, "first")
	s.BlockConnected()

	select {
	case <-lookupStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("lookup did not start")
	}

	const adds = 20

	for i := 0; i < adds; i++ {
		iv := testTxInv(byte(0x10 + i))
		s.AddRebroadcastInventory(&iv, i)

		require.Eventually(t, func() bool { return len(s.modifyRebroadcastInv) == 0 }, time.Second, time.Millisecond,
			"handler stopped taking adds while the lookup ran (add %d)", i)
	}

	require.Zero(t, s.droppedRebroadcastAdds.Load())

	release()

	// The retry covers the entry the lookup saw and the ones added meanwhile.
	for i := 0; i < adds+1; i++ {
		select {
		case <-s.relayInv:
		case <-time.After(2 * time.Second):
			t.Fatalf("retry relayed only %d of %d entries", i, adds+1)
		}
	}
}

// TestLookupRebroadcasts_ParentsReadFailureKeepsMinedCheck covers review:
// reading TxInpoints can need a blob read for a large external tx, and a
// failure there used to fail the whole item and hide that the tx was mined.
func TestLookupRebroadcasts_ParentsReadFailureKeepsMinedCheck(t *testing.T) {
	mined, unmined := testTxInv(1), testTxInv(2)

	store := &utxo.MockUtxostore{}
	store.On("BatchDecorate", mock.Anything, mock.Anything, mock.Anything).Run(func(args mock.Arguments) {
		items := args.Get(1).([]*utxo.UnresolvedMetaData)
		requested := args.Get(2).([]fields.FieldName)

		for _, item := range items {
			if slices.Contains(requested, fields.TxInpoints) {
				item.Err = errors.NewStorageError("external tx blob read failed")
				continue
			}

			item.Data = &meta.Data{}
			if item.Hash == mined.Hash {
				item.Data.BlockIDs = []uint32{7}
			}
		}
	}).Return(nil)

	results, err := lookupRebroadcasts(context.Background(), store, []wire.InvVect{mined, unmined})
	require.NoError(t, err)
	require.Len(t, results, 2)

	require.Equal(t, rebroadcastMined, results[0].status, "a failed parents read must not hide a mined tx")
	require.Equal(t, rebroadcastKeep, results[1].status)
	require.False(t, results[1].parentsKnown, "a failed parents read leaves the parents unknown")

	// The parents read only asks for the tx that stays.
	store.AssertNumberOfCalls(t, "BatchDecorate", 2)

	parentsCall := store.Calls[1].Arguments.Get(1).([]*utxo.UnresolvedMetaData)
	require.Len(t, parentsCall, 1)
	require.Equal(t, unmined.Hash, parentsCall[0].Hash)
}

// newBlockingLookupServer returns a server whose UTXO lookups block until
// release is called, and a channel that receives each lookup as it starts.
func newBlockingLookupServer(t *testing.T) (s *server, lookups <-chan struct{}, release func()) {
	t.Helper()

	started := make(chan struct{}, 16)
	releaseCh := make(chan struct{})

	var releaseOnce sync.Once

	release = func() { releaseOnce.Do(func() { close(releaseCh) }) }

	store := &utxo.MockUtxostore{}
	store.On("BatchDecorate", mock.Anything, mock.Anything, mock.Anything).Run(func(args mock.Arguments) {
		// Only the first read of each lookup blocks and is reported.
		if !slices.Contains(args.Get(2).([]fields.FieldName), fields.TxInpoints) {
			started <- struct{}{}
			<-releaseCh
		}
	}).Return(nil)

	s = &server{
		ctx:                  context.Background(),
		logger:               ulogger.TestLogger{},
		modifyRebroadcastInv: make(chan interface{}, modifyRebroadcastInvBuffer),
		rebroadcastTip:       make(chan struct{}, 1),
		rebroadcastTipDelay:  10 * time.Millisecond,
		relayInv:             make(chan relayMsg, 64),
		quit:                 make(chan struct{}),
		utxoStore:            store,
	}

	return s, started, release
}

// TestRebroadcastHandler_BlockDuringLookupSchedulesAnotherRetry covers review:
// a block that arrived while a lookup ran used to be swallowed, so the retry
// could reach peers before they had processed that block, and the next try
// waited for the following block.
func TestRebroadcastHandler_BlockDuringLookupSchedulesAnotherRetry(t *testing.T) {
	s, lookups, release := newBlockingLookupServer(t)

	s.wg.Add(1)

	go s.rebroadcastHandler()

	t.Cleanup(func() {
		release()
		close(s.quit)
		s.wg.Wait()
	})

	iv := testTxInv(1)
	s.AddRebroadcastInventory(&iv, nil)
	s.BlockConnected()

	select {
	case <-lookups:
	case <-time.After(2 * time.Second):
		t.Fatal("first lookup did not start")
	}

	// A block arrives while the lookup runs.
	s.BlockConnected()
	release()

	select {
	case <-s.relayInv:
	case <-time.After(2 * time.Second):
		t.Fatal("first retry not relayed")
	}

	select {
	case <-lookups:
	case <-time.After(2 * time.Second):
		t.Fatal("the block that arrived during the lookup did not schedule another retry")
	}

	select {
	case <-s.relayInv:
	case <-time.After(2 * time.Second):
		t.Fatal("second retry not relayed")
	}
}

// TestRebroadcastHandler_ShutdownWaitsForLookup covers review: the lookup
// goroutine was not waited for, so it could still run against a store being
// closed after the handler returned.
func TestRebroadcastHandler_ShutdownWaitsForLookup(t *testing.T) {
	s, lookups, release := newBlockingLookupServer(t)
	t.Cleanup(release)

	s.wg.Add(1)

	go s.rebroadcastHandler()

	iv := testTxInv(1)
	s.AddRebroadcastInventory(&iv, nil)
	s.BlockConnected()

	select {
	case <-lookups:
	case <-time.After(2 * time.Second):
		t.Fatal("lookup did not start")
	}

	close(s.quit)

	stopped := make(chan struct{})

	go func() {
		s.wg.Wait()
		close(stopped)
	}()

	select {
	case <-stopped:
		t.Fatal("handler returned while its lookup was still running")
	case <-time.After(200 * time.Millisecond):
	}

	release()

	select {
	case <-stopped:
	case <-time.After(2 * time.Second):
		t.Fatal("handler did not return after its lookup finished")
	}
}

// TestRelayTxBatch_BatchesDoNotInterleave checks that batches reach the
// peerHandler whole and in the order they were handed over, so a parent in
// one batch is never overtaken by a child in the next.
func TestRelayTxBatch_BatchesDoNotInterleave(t *testing.T) {
	s := &server{
		ctx:      context.Background(),
		relayInv: make(chan relayMsg), // unbuffered, so both senders contend
		quit:     make(chan struct{}),
	}
	t.Cleanup(func() { close(s.quit) })

	const perBatch = 100

	var want []wire.InvVect

	for b := byte(0); b < 3; b++ {
		batch := make([]relayMsg, 0, perBatch)

		for i := 0; i < perBatch; i++ {
			iv := wire.InvVect{Type: wire.InvTypeTx, Hash: chainhash.Hash{b, byte(i)}}
			batch = append(batch, relayMsg{invVect: &iv})
			want = append(want, iv)
		}

		s.relayTxBatch(batch)
	}

	for i, iv := range want {
		select {
		case got := <-s.relayInv:
			require.Equal(t, iv, *got.invVect, "position %d", i)
		case <-time.After(2 * time.Second):
			t.Fatalf("timed out at position %d", i)
		}
	}
}
