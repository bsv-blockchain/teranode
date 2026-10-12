package netsync

import (
	"sync"
	"testing"
	"time"

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

// recordingNotifier records announced batches in order. If holdFor is set,
// the call for the batch containing that tx is held for hold, so a later
// flush that does not wait for it is recorded first. Keying the hold on
// content rather than on the first call keeps that deterministic when
// flushes race.
type recordingNotifier struct {
	*MockPeerNotifier

	holdFor *chainhash.Hash
	hold    time.Duration

	mu      sync.Mutex
	batches [][]chainhash.Hash
}

func (n *recordingNotifier) AnnounceNewTransactions(batch []*TxHashAndFee) {
	if n.holdFor != nil {
		for _, item := range batch {
			if item.TxHash == *n.holdFor {
				time.Sleep(n.hold)
				break
			}
		}
	}

	n.mu.Lock()
	defer n.mu.Unlock()

	n.batches = append(n.batches, announceHashes(batch))
}

func (n *recordingNotifier) announced() []chainhash.Hash {
	n.mu.Lock()
	defer n.mu.Unlock()

	var out []chainhash.Hash
	for _, batch := range n.batches {
		out = append(out, batch...)
	}

	return out
}

// newAnnounceTestSyncManager returns a SyncManager whose tx announce batcher
// is built by the production constructor, with a small batch size.
func newAnnounceTestSyncManager(t *testing.T, notifier PeerNotifier, size int) *SyncManager {
	t.Helper()

	sm := &SyncManager{
		logger:          ulogger.TestLogger{},
		peerNotifier:    notifier,
		announceParents: txmap.NewSyncedMap[chainhash.Hash, []chainhash.Hash](),
	}
	sm.txAnnounceBatcher = sm.newTxAnnounceBatcher(size, 50*time.Millisecond)

	t.Cleanup(sm.closeTxAnnounceBatcher)

	return sm
}

func spending(parent chainhash.Hash) meta.Data {
	return meta.Data{Fee: 1, SizeInBytes: 100, TxInpoints: subtreepkg.NewTxInpointsFromPacked([]chainhash.Hash{parent}, []uint32{1, 0})}
}

// TestProcessTXmetaBatchMessage_AnnouncesParentsFirst drives the txmeta path
// through the production batcher: a child read before its parent, as a
// partitioned topic can deliver them, is announced after the parent.
func TestProcessTXmetaBatchMessage_AnnouncesParentsFirst(t *testing.T) {
	parent, child := chainhash.Hash{0x10}, chainhash.Hash{0x20}

	notifier := &recordingNotifier{MockPeerNotifier: NewMockPeerNotifier()}
	sm := newAnnounceTestSyncManager(t, notifier, 100)

	msg := buildTXmetaBatchMessage(t, []txmetaTestEntry{
		{hash: child, action: txmetacache.WireActionADD, meta: spending(parent)},
		{hash: parent, action: txmetacache.WireActionADD, meta: spending(chainhash.Hash{0x30})},
	})

	require.NoError(t, sm.processTXmetaBatchMessage(msg))

	require.Eventually(t, func() bool { return len(notifier.announced()) == 2 }, 2*time.Second, 10*time.Millisecond)
	require.Equal(t, []chainhash.Hash{parent, child}, notifier.announced())
}

// TestTxAnnounceBatcher_FlushesInOrder covers review: with background=true
// go-batcher runs each flush on its own goroutine, so a child in one
// size-triggered batch could be announced before its parent in the batch
// before it. The parent's batch is held here, so out-of-order flushing would
// always record the child's batch first.
func TestTxAnnounceBatcher_FlushesInOrder(t *testing.T) {
	parent, child := chainhash.Hash{0x10}, chainhash.Hash{0x20}

	notifier := &recordingNotifier{MockPeerNotifier: NewMockPeerNotifier(), holdFor: &parent, hold: 200 * time.Millisecond}
	sm := newAnnounceTestSyncManager(t, notifier, 2)

	// Batch size 2: the parent fills the first batch, the child the second.
	msg := buildTXmetaBatchMessage(t, []txmetaTestEntry{
		{hash: parent, action: txmetacache.WireActionADD, meta: spending(chainhash.Hash{0x30})},
		{hash: chainhash.Hash{0x11}, action: txmetacache.WireActionADD, meta: spending(chainhash.Hash{0x31})},
		{hash: child, action: txmetacache.WireActionADD, meta: spending(parent)},
		{hash: chainhash.Hash{0x21}, action: txmetacache.WireActionADD, meta: spending(chainhash.Hash{0x32})},
	})

	require.NoError(t, sm.processTXmetaBatchMessage(msg))

	require.Eventually(t, func() bool { return len(notifier.announced()) == 4 }, 2*time.Second, 10*time.Millisecond)

	got := notifier.announced()
	require.Equal(t, parent, got[0], "batches were announced out of order: %v", got)
	require.Equal(t, child, got[2], "batches were announced out of order: %v", got)
}
