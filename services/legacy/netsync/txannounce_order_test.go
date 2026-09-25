package netsync

import (
	"sync"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-batcher/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	txmap "github.com/bsv-blockchain/go-tx-map"
	"github.com/bsv-blockchain/teranode/stores/txmetacache"
	"github.com/bsv-blockchain/teranode/stores/utxo/meta"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/stretchr/testify/require"
)

func announceHashes(batch []*TxHashAndFee) []chainhash.Hash {
	out := make([]chainhash.Hash, len(batch))
	for i, item := range batch {
		out[i] = item.TxHash
	}

	return out
}

// TestOrderAnnounceBatch checks that a batch is reordered parents-first from
// the parents recorded at announce time, and that the recorded entries are
// consumed.
func TestOrderAnnounceBatch(t *testing.T) {
	root, child, grandchild, unrelated := chainhash.Hash{1}, chainhash.Hash{2}, chainhash.Hash{3}, chainhash.Hash{4}

	batch := []*TxHashAndFee{{TxHash: grandchild}, {TxHash: unrelated}, {TxHash: child}, {TxHash: root}}

	t.Run("parents first", func(t *testing.T) {
		sm := &SyncManager{announceParents: txmap.NewSyncedMap[chainhash.Hash, []chainhash.Hash]()}
		sm.announceParents.Set(grandchild, []chainhash.Hash{child, {0xaa}})
		sm.announceParents.Set(child, []chainhash.Hash{root})
		sm.announceParents.Set(root, []chainhash.Hash{{0xbb}})

		got := sm.orderAnnounceBatch(batch)

		require.Equal(t, []chainhash.Hash{root, child, grandchild, unrelated}, announceHashes(got))
		require.Zero(t, sm.announceParents.Length(), "recorded parents must be consumed")
		require.Equal(t, grandchild, batch[0].TxHash, "the batcher's slice must not be reordered in place")
	})

	t.Run("no recorded parents keeps order", func(t *testing.T) {
		sm := &SyncManager{}

		require.Equal(t, announceHashes(batch), announceHashes(sm.orderAnnounceBatch(batch)))
	})
}

// TestProcessTXmetaBatchMessage_AnnouncesParentsFirst drives the txmeta path:
// a child read before its parent, as a partitioned topic can deliver them, is
// announced after the parent.
func TestProcessTXmetaBatchMessage_AnnouncesParentsFirst(t *testing.T) {
	parent, child := chainhash.Hash{0x10}, chainhash.Hash{0x20}
	external := chainhash.Hash{0x30}

	var (
		mu        sync.Mutex
		announced []*TxHashAndFee
	)

	sm := &SyncManager{
		logger:          ulogger.TestLogger{},
		announceParents: txmap.NewSyncedMap[chainhash.Hash, []chainhash.Hash](),
	}
	sm.txAnnounceBatcher = batcher.NewWithDeduplication[TxHashAndFee](100, 50*time.Millisecond, func(batch []*TxHashAndFee) {
		mu.Lock()
		announced = append(announced, sm.orderAnnounceBatch(batch)...)
		mu.Unlock()
	}, false)

	spends := func(p chainhash.Hash) subtreepkg.TxInpoints {
		return subtreepkg.NewTxInpointsFromPacked([]chainhash.Hash{p}, []uint32{1, 0})
	}

	msg := buildTXmetaBatchMessage(t, []txmetaTestEntry{
		{hash: child, action: txmetacache.WireActionADD, meta: meta.Data{Fee: 1, SizeInBytes: 100, TxInpoints: spends(parent)}},
		{hash: parent, action: txmetacache.WireActionADD, meta: meta.Data{Fee: 1, SizeInBytes: 100, TxInpoints: spends(external)}},
	})

	require.NoError(t, sm.processTXmetaBatchMessage(msg))

	require.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()

		return len(announced) == 2
	}, 2*time.Second, 10*time.Millisecond)

	mu.Lock()
	defer mu.Unlock()

	require.Equal(t, []chainhash.Hash{parent, child}, announceHashes(announced))
}
