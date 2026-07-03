package subtreeprocessor

import (
	"context"
	"fmt"
	"net/url"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	blob_memory "github.com/bsv-blockchain/teranode/stores/blob/memory"
	"github.com/bsv-blockchain/teranode/stores/utxo/sql"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/require"
)

// TestSubtreeAnnouncementIntervalDefault pins the BA-SUBTREE-008 cadence default:
// SubtreeAnnouncementInterval defaults to 10 seconds.
func TestSubtreeAnnouncementIntervalDefault(t *testing.T) {
	tSettings := test.CreateBaseTestSettings(t)
	require.Equal(t, 10*time.Second, tSettings.BlockAssembly.SubtreeAnnouncementInterval,
		"BA-SUBTREE-008: the default subtree-announcement cadence must be 10 seconds")
}

// TestPeriodicAnnouncementOfIncompleteSubtree pins BA-SUBTREE-008: the service
// announces the current incomplete subtree on the SubtreeAnnouncementInterval
// timer so that mining candidates stay fresh during low-traffic periods, even
// when no subtree ever fills up.
//
// The processor is configured with a large subtree size (so a handful of txs
// never completes a subtree) and a short announcement interval. After adding a
// few transactions, the test asserts that the incomplete subtree is announced on
// newSubtreeChan and that the announcement repeats on the timer.
func TestPeriodicAnnouncementOfIncompleteSubtree(t *testing.T) {
	const (
		subtreeSize       = 16 // large enough that 3 txs neither completes nor "nearly fills" it
		txsToAdd          = 3
		announceEvery     = 75 * time.Millisecond
		announcedLength   = txsToAdd + 1 // coinbase placeholder at index 0 + the txs
		waitForFirst      = 5 * time.Second
		waitForPeriodicit = 5 * time.Second
	)

	utxoStoreURL, err := url.Parse("sqlitememory:///test")
	require.NoError(t, err)

	tSettings := test.CreateBaseTestSettings(t)
	utxoStore, err := sql.New(context.Background(), ulogger.TestLogger{}, tSettings, utxoStoreURL)
	require.NoError(t, err)

	tSettings.BlockAssembly.InitialMerkleItemsPerSubtree = subtreeSize
	tSettings.BlockAssembly.MinimumMerkleItemsPerSubtree = subtreeSize
	tSettings.BlockAssembly.SubtreeAnnouncementInterval = announceEvery
	// Dequeue transactions immediately so they reach the current subtree without
	// waiting on the double-spend window.
	tSettings.BlockAssembly.DoubleSpendWindow = 0

	// Observe announcements. The periodic-announcement path waits on the
	// request's ErrChan, so the drain must reply, mirroring production.
	announced := make(chan *subtreepkg.Subtree, 32)
	newSubtreeChan := make(chan NewSubtreeRequest, 32)
	go func() {
		for req := range newSubtreeChan {
			select {
			case announced <- req.Subtree:
			default:
			}
			if req.ErrChan != nil {
				req.ErrChan <- nil
			}
		}
	}()
	// Registered before Start's cleanup so it runs AFTER Stop (LIFO): the
	// processor is stopped before the channel is closed, avoiding send-on-closed.
	t.Cleanup(func() { close(newSubtreeChan) })

	stp, err := NewSubtreeProcessor(t.Context(), ulogger.TestLogger{}, tSettings, blob_memory.New(), nil, utxoStore, newSubtreeChan)
	require.NoError(t, err)

	stp.Start(t.Context())
	t.Cleanup(func() { stp.Stop(context.Background()) })

	// Low-traffic load: a few transactions that never fill the subtree.
	txHashes := make([]chainhash.Hash, txsToAdd)
	parentHash := chainhash.HashH([]byte("periodic-announce-parent"))
	for i := range txsToAdd {
		txHashes[i] = chainhash.HashH(fmt.Appendf(nil, "periodic-announce-tx-%d", i))
		stp.AddBatch(
			[]subtreepkg.Node{{Hash: txHashes[i], Fee: uint64(100 + i), SizeInBytes: 200}},
			[]*subtreepkg.TxInpoints{{ParentTxHashes: []chainhash.Hash{parentHash}}},
		)
	}

	// The first announcement must be the incomplete current subtree (placeholder
	// + the txs we added), proving the timer fires without a subtree completing.
	var firstAnnouncement *subtreepkg.Subtree
	select {
	case firstAnnouncement = <-announced:
	case <-time.After(waitForFirst):
		t.Fatal("BA-SUBTREE-008: incomplete subtree was never announced on the timer")
	}

	require.NotNil(t, firstAnnouncement)
	require.Equal(t, announcedLength, firstAnnouncement.Length(),
		"the announced subtree must be the incomplete current subtree (coinbase placeholder + added txs), not a completed one")
	require.Equal(t, subtreepkg.CoinbasePlaceholderHashValue, firstAnnouncement.Nodes[0].Hash,
		"index 0 of the announced subtree must be the coinbase placeholder")
	require.Less(t, firstAnnouncement.Length(), subtreeSize,
		"the announced subtree must be incomplete (fewer nodes than a full subtree)")

	// Every added tx must be present in the announced incomplete subtree.
	announcedHashes := make(map[chainhash.Hash]struct{}, firstAnnouncement.Length())
	for _, node := range firstAnnouncement.Nodes {
		announcedHashes[node.Hash] = struct{}{}
	}
	for _, h := range txHashes {
		require.Contains(t, announcedHashes, h, "added tx %s missing from the announced incomplete subtree", h)
	}

	// Periodicity: the timer must fire again, announcing the still-incomplete
	// subtree a second time without any new transactions arriving.
	select {
	case again := <-announced:
		require.NotNil(t, again)
		require.Equal(t, announcedLength, again.Length(),
			"the repeated announcement must again be the incomplete current subtree")
	case <-time.After(waitForPeriodicit):
		t.Fatal("BA-SUBTREE-008: the announcement timer did not fire repeatedly")
	}
}
