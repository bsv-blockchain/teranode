package p2p

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/services/blockchain/blockchain_api"
	"github.com/bsv-blockchain/teranode/util/expiringmap"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// publishedBlockMessages decodes every BlockMessage this test's mock P2P
// client was asked to Publish to topic, in call order, so tests can assert on
// what actually reached the network instead of on which internal function ran.
func publishedBlockMessages(p2pClient *MockServerP2PClient, topic string) []BlockMessage {
	var out []BlockMessage

	for _, call := range p2pClient.Calls {
		if call.Method != "Publish" || call.Arguments.String(1) != topic {
			continue
		}

		var msg BlockMessage
		if err := json.Unmarshal(call.Arguments.Get(2).([]byte), &msg); err == nil {
			out = append(out, msg)
		}
	}

	return out
}

// newAnnounceTestServer wires a Server against a blockchain.Mock for the
// announceBlock tests below: RUNNING FSM (outbound gossip allowed), full
// listen mode, and a connected peer so the sent-and-non-empty-mesh gate in
// announceBlock does not itself suppress publishes.
func newAnnounceTestServer(t *testing.T, mockBlockchain *blockchain.Mock) (*Server, *MockServerP2PClient) {
	t.Helper()

	fsmState := blockchain_api.FSMStateType_RUNNING
	mockBlockchain.On("GetFSMCurrentState", mock.Anything).Return(&fsmState, nil).Maybe()
	mockBlockchain.On("GetBestBlockHeader", mock.Anything).
		Return(model.GenesisBlockHeader, &model.BlockHeaderMeta{Height: 100}, nil).Maybe()
	mockBlockchain.On("GetState", mock.Anything, mock.Anything).
		Return([]byte(nil), nil).Maybe()

	s, p2pClient := newGateTestServer(t, mockBlockchain)
	p2pClient.peerID = mustNewPeerID(t)
	s.notificationCh = make(chan *notificationMsg, 64)
	s.AssetHTTPAddressURL = "http://asset.example"

	// newGateTestServer builds a Server literal directly rather than through
	// NewServer, so this pointer-shaped field is nil unless set here; without
	// it the preAnnouncedBlockHashes dedup in announceBlock is silently
	// disabled (see its nil guard), which is exactly what test (c) below
	// exercises.
	s.preAnnouncedBlockHashes = expiringmap.New[chainhash.Hash, struct{}](preAnnouncedBlockHashTTL)
	t.Cleanup(s.preAnnouncedBlockHashes.Stop)

	return s, p2pClient
}

// (a) A Block notification for a block whose subtrees are already validated
// must be announced immediately, exactly once.
func TestAnnounceBlock_BlockNotification_SubtreesSetTrue_AnnouncesOnce(t *testing.T) {
	mockBlockchain := &blockchain.Mock{}
	mockBlockchain.On("GetBlockHeader", mock.Anything, mock.Anything).
		Return(model.GenesisBlockHeader, &model.BlockHeaderMeta{Height: 100, SubtreesSet: true}, nil)

	s, p2pClient := newAnnounceTestServer(t, mockBlockchain)

	hash := chainhash.HashH([]byte("ready block"))
	require.NoError(t, s.handleBlockNotification(context.Background(), &hash))

	published := publishedBlockMessages(p2pClient, s.blockTopicName)
	require.Len(t, published, 1, "a block whose subtrees are already set must be announced")
	require.Equal(t, hash.String(), published[0].Hash)
}

// (b) A Block notification for a block whose subtrees_set flag is still false
// (validation has not finished, e.g. an optimistically-mined peer block whose
// background block.Valid check has not run yet) must not be announced. The
// BlockSubtreesSet notification that follows once validation completes
// announces it instead, exactly once.
func TestAnnounceBlock_BlockNotification_SubtreesSetFalse_AnnouncedLaterByBlockSubtreesSet(t *testing.T) {
	mockBlockchain := &blockchain.Mock{}
	mockBlockchain.On("GetBlockHeader", mock.Anything, mock.Anything).
		Return(model.GenesisBlockHeader, &model.BlockHeaderMeta{Height: 100, SubtreesSet: false}, nil).Once()
	mockBlockchain.On("GetBlockHeader", mock.Anything, mock.Anything).
		Return(model.GenesisBlockHeader, &model.BlockHeaderMeta{Height: 100, SubtreesSet: true}, nil)

	s, p2pClient := newAnnounceTestServer(t, mockBlockchain)

	hash := chainhash.HashH([]byte("optimistic peer block"))

	require.NoError(t, s.handleBlockNotification(context.Background(), &hash))
	require.Empty(t, publishedBlockMessages(p2pClient, s.blockTopicName),
		"a block whose subtrees_set flag is still false must not be announced")

	require.NoError(t, s.handleBlockSubtreesSetNotification(context.Background(), &hash))
	published := publishedBlockMessages(p2pClient, s.blockTopicName)
	require.Len(t, published, 1, "the BlockSubtreesSet notification must announce the block once validation completes")
	require.Equal(t, hash.String(), published[0].Hash)
}

// (c) A block whose subtrees are already set when it is added (the locally
// mined and quick-validate/catchup paths) is announced by the Block
// notification. The SetBlockSubtreesSet call both paths still make fires a
// second, BlockSubtreesSet, notification for the SAME hash — this must not
// double-announce it, even with another block's announcement landing between
// the two notifications.
func TestAnnounceBlock_BlockAndBlockSubtreesSetForSameHash_AnnouncedOnceEachDespiteInterleaving(t *testing.T) {
	mockBlockchain := &blockchain.Mock{}
	mockBlockchain.On("GetBlockHeader", mock.Anything, mock.Anything).
		Return(model.GenesisBlockHeader, &model.BlockHeaderMeta{Height: 100, SubtreesSet: true}, nil)

	s, p2pClient := newAnnounceTestServer(t, mockBlockchain)

	mined := chainhash.HashH([]byte("locally mined block"))
	other := chainhash.HashH([]byte("interleaved other block"))

	// AddBlock(WithSubtreesSet(true)) -> Block notification announces it.
	require.NoError(t, s.handleBlockNotification(context.Background(), &mined))

	// Another block is announced before this node gets to call
	// SetBlockSubtreesSet for "mined", so the lastAnnouncedBlockHash
	// consecutive-duplicate guard alone would not catch the coming repeat.
	require.NoError(t, s.handleBlockNotification(context.Background(), &other))

	// SetBlockSubtreesSet's own notification for "mined" must not re-announce it.
	require.NoError(t, s.handleBlockSubtreesSetNotification(context.Background(), &mined))

	published := publishedBlockMessages(p2pClient, s.blockTopicName)
	require.Len(t, published, 2, "each block must be announced exactly once")
	require.Equal(t, mined.String(), published[0].Hash)
	require.Equal(t, other.String(), published[1].Hash)
}

// (d) An invalid block must never be announced, on either notification type.
func TestAnnounceBlock_InvalidBlock_NeverAnnounced(t *testing.T) {
	mockBlockchain := &blockchain.Mock{}
	mockBlockchain.On("GetBlockHeader", mock.Anything, mock.Anything).
		Return(model.GenesisBlockHeader, &model.BlockHeaderMeta{Height: 100, SubtreesSet: true, Invalid: true}, nil)

	s, p2pClient := newAnnounceTestServer(t, mockBlockchain)

	hash := chainhash.HashH([]byte("invalid block"))

	require.NoError(t, s.handleBlockNotification(context.Background(), &hash))
	require.NoError(t, s.handleBlockSubtreesSetNotification(context.Background(), &hash))

	require.Empty(t, publishedBlockMessages(p2pClient, s.blockTopicName), "an invalid block must never be announced")
}

// The two notification types share one dispatch point in
// processBlockchainNotification; confirm BlockSubtreesSet is wired to the same
// announce path as Block, not silently dropped as an unhandled type.
func TestProcessBlockchainNotification_RoutesBlockSubtreesSetToAnnounce(t *testing.T) {
	mockBlockchain := &blockchain.Mock{}
	mockBlockchain.On("GetBlockHeader", mock.Anything, mock.Anything).
		Return(model.GenesisBlockHeader, &model.BlockHeaderMeta{Height: 100, SubtreesSet: true}, nil)

	s, p2pClient := newAnnounceTestServer(t, mockBlockchain)

	hash := chainhash.HashH([]byte("routed via BlockSubtreesSet"))

	err := s.processBlockchainNotification(context.Background(), &blockchain.Notification{
		Type: model.NotificationType_BlockSubtreesSet,
		Hash: hash.CloneBytes(),
	})
	require.NoError(t, err)

	published := publishedBlockMessages(p2pClient, s.blockTopicName)
	require.Len(t, published, 1)
	require.Equal(t, hash.String(), published[0].Hash)
}
