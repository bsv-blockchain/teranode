package legacy

import (
	"context"
	"net/url"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-wire"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	utxosql "github.com/bsv-blockchain/teranode/stores/utxo/sql"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/require"
)

func testTxInv(b byte) wire.InvVect {
	return wire.InvVect{Type: wire.InvTypeTx, Hash: chainhash.Hash{b}}
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

	require.True(t, q.add(testTxInv(1), "first"))
	require.True(t, q.add(testTxInv(2), "other"))

	_, _ = retryBatch(q, 10)
	_, _ = retryBatch(q, 10)

	require.True(t, q.add(testTxInv(1), "second"), "re-add of an existing iv must be accepted")
	require.Equal(t, 2, q.len())

	entries := q.entries()
	require.Equal(t, testTxInv(1), entries[0].iv, "re-add must keep the entry's position")
	require.Equal(t, 2, entries[0].tips, "re-add must keep the entry's budget")
	require.Equal(t, "second", entries[0].data, "re-add must refresh the data payload")
}

// TestRebroadcastQueue_DropsAtCap covers the bounded-memory contract: once the
// queue holds `capacity` entries, new adds are rejected, while updates of
// existing entries still succeed.
func TestRebroadcastQueue_DropsAtCap(t *testing.T) {
	const capacity = 4

	q := newRebroadcastQueue(capacity)

	for i := 0; i < capacity; i++ {
		require.True(t, q.add(testTxInv(byte(i+1)), i), "add %d below cap must be accepted", i)
	}

	require.False(t, q.add(testTxInv(0xff), "overflow"), "add beyond cap must be rejected")
	require.Equal(t, capacity, q.len(), "rejected add must not mutate the queue")

	require.True(t, q.add(testTxInv(1), "updated"), "update of existing entry must succeed at cap")
	require.Equal(t, "updated", q.entries()[0].data)

	q.remove(testTxInv(2))
	require.True(t, q.add(testTxInv(0xff), "fits"), "add must succeed once an entry is removed")
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
		require.True(t, q.add(iv, i))

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
// requirement from issue 1826: SV Node only accepts a child once it has the
// parent, and txmeta Kafka reads can queue a child before its parent.
func TestRebroadcastQueue_RetrySortsParentsFirst(t *testing.T) {
	q := newRebroadcastQueue(10)

	root, child, grandchild, unrelated := testTxInv(1), testTxInv(2), testTxInv(3), testTxInv(4)

	// Queued in the worst order a partitioned read could give.
	for _, iv := range []wire.InvVect{grandchild, unrelated, child, root} {
		require.True(t, q.add(iv, nil))
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
	require.True(t, q.add(testTxInv(1), "data"))

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
		require.True(t, q.add(iv, nil))
	}

	result, err := pruneRebroadcastQueue(ctx, store, q)
	require.NoError(t, err)
	require.Equal(t, rebroadcastPruneResult{mined: 1, conflicting: 1, notFound: 1}, result)

	entries := q.entries()
	require.Len(t, entries, 3)
	require.Equal(t, unminedA, entries[0].iv)
	require.Equal(t, unminedB, entries[1].iv)
	require.Equal(t, withParent, entries[2].iv)

	require.Empty(t, entries[0].parents)
	require.Equal(t, []chainhash.Hash{*parentTx.TxIDChainHash()}, entries[2].parents,
		"prune must record the parents retry orders by")

	t.Run("nil store keeps every entry", func(t *testing.T) {
		result, err := pruneRebroadcastQueue(ctx, nil, q)
		require.NoError(t, err)
		require.Zero(t, result)
		require.Equal(t, 3, q.len())
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
