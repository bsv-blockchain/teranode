package netsync

import (
	"context"
	"net/url"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-wire"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	blockchain2 "github.com/bsv-blockchain/teranode/services/blockchain"
	blockchainstore "github.com/bsv-blockchain/teranode/stores/blockchain"
	chainoptions "github.com/bsv-blockchain/teranode/stores/blockchain/options"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/kafka"
	kafkamessage "github.com/bsv-blockchain/teranode/util/kafka/kafka_message"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

var announceReconcileStoreCounter atomic.Int64

// announceReconcileChain is the real LocalClient over a sqlitememory store, with
// one seam the real client cannot give: a GetBestBlockHeader fault.
type announceReconcileChain struct {
	blockchain2.ClientI

	bestErr atomic.Pointer[error]
}

func (c *announceReconcileChain) GetBestBlockHeader(ctx context.Context) (*model.BlockHeader, *model.BlockHeaderMeta, error) {
	if err := c.bestErr.Load(); err != nil {
		return nil, nil, *err
	}

	return c.ClientI.GetBestBlockHeader(ctx)
}

type announceReconcileHarness struct {
	sm       *SyncManager
	notifier *MockPeerNotifier
	chain    *announceReconcileChain
	blocks   []*model.Block
}

func newAnnounceReconcileHarness(t *testing.T) *announceReconcileHarness {
	t.Helper()

	tSettings := test.CreateBaseTestSettings(t)
	ctx := context.Background()

	chainURL, err := url.Parse("sqlitememory:///announce_reconcile_" + strconv.FormatInt(announceReconcileStoreCounter.Add(1), 10))
	require.NoError(t, err)

	bcStore, err := blockchainstore.NewStore(ulogger.TestLogger{}, chainURL, tSettings)
	require.NoError(t, err)
	t.Cleanup(func() { _ = bcStore.Close(ctx) })

	local, err := blockchain2.NewLocalClient(ulogger.TestLogger{}, tSettings, bcStore, nil, nil)
	require.NoError(t, err)

	chain := &announceReconcileChain{ClientI: local}
	notifier := NewMockPeerNotifier()

	mined := minedBlocks(t, 3)
	blocks := make([]*model.Block, 0, len(mined))

	for _, b := range mined {
		blk, err := model.NewBlockFromMsgBlock(b.MsgBlock(), tSettings)
		require.NoError(t, err)

		blocks = append(blocks, blk)
	}

	return &announceReconcileHarness{
		sm: &SyncManager{
			ctx:              ctx,
			logger:           ulogger.TestLogger{},
			settings:         tSettings,
			peerNotifier:     notifier,
			blockchainClient: chain,
		},
		notifier: notifier,
		chain:    chain,
		blocks:   blocks,
	}
}

// commit stores block i in the chain the way the Blockchain service does,
// without the blocks-final publish that should follow it.
func (h *announceReconcileHarness) commit(t *testing.T, i int) *chainhash.Hash {
	t.Helper()

	require.NoError(t, h.chain.AddBlock(h.sm.ctx, h.blocks[i], "", chainoptions.WithMinedSet(true), chainoptions.WithSubtreesSet(true)))

	return h.blocks[i].Header.Hash()
}

func (h *announceReconcileHarness) blocksFinal(t *testing.T, i int, height uint32) {
	t.Helper()

	value, err := proto.Marshal(&kafkamessage.KafkaBlocksFinalTopicMessage{Header: h.blocks[i].Header.Bytes(), Height: height})
	require.NoError(t, err)

	require.NoError(t, h.sm.processBlocksFinalMessage(&kafka.KafkaMessage{Key: []byte(h.blocks[i].Header.Hash().String()), Value: value}))
}

func (h *announceReconcileHarness) requireNoAnnouncement(t *testing.T) {
	t.Helper()

	require.Empty(t, h.notifier.relayInventoryChan, "no block may be announced")
}

func (h *announceReconcileHarness) requireAnnounced(t *testing.T, hash *chainhash.Hash, height int32) {
	t.Helper()

	require.Len(t, h.notifier.relayInventoryChan, 1, "exactly one block must be announced")

	call := <-h.notifier.relayInventoryChan
	require.Equal(t, wire.InvTypeBlock, call.invVect.Type)
	require.Equal(t, *hash, call.invVect.Hash)

	wireHeader, ok := call.data.(*wire.BlockHeader)
	require.True(t, ok, "relay data must be the wire block header, got %T", call.data)
	require.Equal(t, *hash, wireHeader.BlockHash())

	select {
	case heights := <-h.notifier.updatePeerHeightsChan:
		require.Equal(t, *hash, *heights.latestBlkHash)
		require.Equal(t, height, heights.latestHeight)
	default:
		t.Fatal("UpdatePeerHeights was not called for the announced block")
	}
}

// TestReconcileAnnouncedTip_AnnouncesATipWhoseBlocksFinalWasLost is the 2.1-TC-MS-008
// failure: the Blockchain service commits a block and the blocks-final publish never
// happens (a crash after the commit, or an async publish dropped while Kafka is down).
// Nothing else made Legacy announce that block to its peers. The reconciler must announce
// it once it has been the tip for a full interval.
func TestReconcileAnnouncedTip_AnnouncesATipWhoseBlocksFinalWasLost(t *testing.T) {
	h := newAnnounceReconcileHarness(t)
	ctx := context.Background()

	h.blocksFinal(t, 0, 1)
	<-h.notifier.relayInventoryChan
	<-h.notifier.updatePeerHeightsChan

	h.commit(t, 0)
	tip := h.commit(t, 1)

	pending := h.sm.reconcileAnnouncedTip(ctx, nil)
	require.Equal(t, tip, pending, "the first tick only notes the unannounced tip")
	h.requireNoAnnouncement(t)

	pending = h.sm.reconcileAnnouncedTip(ctx, pending)
	require.Nil(t, pending)
	h.requireAnnounced(t, tip, 2)
	require.Equal(t, int32(2), h.notifier.blockConnectedCalls.Load(), "BlockConnected once for blocks-final, once for the reconcile")

	pending = h.sm.reconcileAnnouncedTip(ctx, pending)
	require.Nil(t, pending, "an announced tip is not announced again")
	h.requireNoAnnouncement(t)
}

// A tip that blocks-final already announced is left alone, so the reconciler adds no
// traffic on a healthy node.
func TestReconcileAnnouncedTip_LeavesATipThatBlocksFinalAnnounced(t *testing.T) {
	h := newAnnounceReconcileHarness(t)
	ctx := context.Background()

	h.commit(t, 0)
	h.blocksFinal(t, 0, 1)
	<-h.notifier.relayInventoryChan
	<-h.notifier.updatePeerHeightsChan

	var pending *chainhash.Hash
	for i := 0; i < 3; i++ {
		pending = h.sm.reconcileAnnouncedTip(ctx, pending)
		require.Nil(t, pending)
	}

	h.requireNoAnnouncement(t)
}

// A fork block announced after the tip does not hide the tip's own announcement:
// blocks-final announces every valid block, so the last announcement need not be the tip.
func TestReconcileAnnouncedTip_RemembersMoreThanTheLastAnnouncement(t *testing.T) {
	h := newAnnounceReconcileHarness(t)
	ctx := context.Background()

	h.commit(t, 0)
	h.blocksFinal(t, 0, 1)
	<-h.notifier.relayInventoryChan
	<-h.notifier.updatePeerHeightsChan

	other := &model.BlockHeader{
		Version:        1,
		HashPrevBlock:  &chainhash.Hash{0x01},
		HashMerkleRoot: &chainhash.Hash{0x02},
		Timestamp:      1700000000,
		Bits:           model.NBit{0xff, 0xff, 0x00, 0x1d},
		Nonce:          7,
	}
	value, err := proto.Marshal(&kafkamessage.KafkaBlocksFinalTopicMessage{Header: other.Bytes(), Height: 1})
	require.NoError(t, err)
	require.NoError(t, h.sm.processBlocksFinalMessage(&kafka.KafkaMessage{Key: []byte(other.Hash().String()), Value: value}))
	<-h.notifier.relayInventoryChan
	<-h.notifier.updatePeerHeightsChan

	pending := h.sm.reconcileAnnouncedTip(ctx, nil)
	pending = h.sm.reconcileAnnouncedTip(ctx, pending)
	require.Nil(t, pending)
	h.requireNoAnnouncement(t)
}

// A tip that moves between two ticks is not announced: blocks-final is still delivering,
// as during catch-up, and the next block's announcement will follow.
func TestReconcileAnnouncedTip_WaitsWhileTheTipMoves(t *testing.T) {
	h := newAnnounceReconcileHarness(t)
	ctx := context.Background()

	first := h.commit(t, 0)

	pending := h.sm.reconcileAnnouncedTip(ctx, nil)
	require.Equal(t, first, pending)

	second := h.commit(t, 1)

	pending = h.sm.reconcileAnnouncedTip(ctx, pending)
	require.Equal(t, second, pending, "a new tip restarts the wait")
	h.requireNoAnnouncement(t)

	pending = h.sm.reconcileAnnouncedTip(ctx, pending)
	require.Nil(t, pending)
	h.requireAnnounced(t, second, 2)
}

// A failed tip read announces nothing and keeps the pending tip, so one blip neither
// announces early nor restarts the wait.
func TestReconcileAnnouncedTip_KeepsThePendingTipOnAReadFailure(t *testing.T) {
	h := newAnnounceReconcileHarness(t)
	ctx := context.Background()

	tip := h.commit(t, 0)

	pending := h.sm.reconcileAnnouncedTip(ctx, nil)
	require.Equal(t, tip, pending)

	var readErr error = errors.NewServiceError("blockchain unavailable")
	h.chain.bestErr.Store(&readErr)

	pending = h.sm.reconcileAnnouncedTip(ctx, pending)
	require.Equal(t, tip, pending)
	h.requireNoAnnouncement(t)

	h.chain.bestErr.Store(nil)

	pending = h.sm.reconcileAnnouncedTip(ctx, pending)
	require.Nil(t, pending)
	h.requireAnnounced(t, tip, 1)
}

// The running loop announces a lost tip on its own and stops with its context.
func TestRunBlockAnnounceReconciler_AnnouncesAndStops(t *testing.T) {
	h := newAnnounceReconcileHarness(t)

	tip := h.commit(t, 0)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})

	go func() {
		h.sm.runBlockAnnounceReconciler(ctx, 10*time.Millisecond)
		close(done)
	}()

	require.Eventually(t, func() bool { return len(h.notifier.relayInventoryChan) == 1 }, 5*time.Second, 5*time.Millisecond)
	h.requireAnnounced(t, tip, 1)

	cancel()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("the reconciler did not stop when its context was cancelled")
	}
}
