package netsync

import (
	"context"
	"sync/atomic"
	"testing"

	"github.com/bsv-blockchain/go-wire"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	teranodeblockchain "github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/stretchr/testify/require"
)

// tipFaultClient is the real blockchain client with GetBestBlockHeader made to
// fail for its next failures calls. It counts every GetBestBlockHeader call, and
// passes every call it does not fail to the client it wraps.
type tipFaultClient struct {
	teranodeblockchain.ClientI
	failures atomic.Int32
	calls    atomic.Int32
}

func (c *tipFaultClient) GetBestBlockHeader(ctx context.Context) (*model.BlockHeader, *model.BlockHeaderMeta, error) {
	c.calls.Add(1)

	if c.failures.Add(-1) >= 0 {
		return nil, nil, errors.NewServiceUnavailableError("injected best block header failure")
	}

	return c.ClientI.GetBestBlockHeader(ctx)
}

// wrapTipFaults puts a tipFaultClient around the manager's real blockchain
// client. The header rules keep their own client, so calls counts only what the
// manager itself asks.
func wrapTipFaults(sm *SyncManager) *tipFaultClient {
	c := &tipFaultClient{ClientI: sm.blockchainClient}
	sm.blockchainClient = c

	return c
}

// An outbound peer this node never asked sends a batch below the checkpoint
// while the chain's best block header cannot be read. The batch is dropped: a
// failed tip read must not skip the rule and let a later read admit the batch.
func TestHeaderRequestRule_AnUnaskedBatchIsDroppedWhenTheTipReadFails(t *testing.T) {
	sm := headerRuleManager(t)
	honest := mainnetFirstCheckpointHeaders(t)
	client := wrapTipFaults(sm)

	peer, _, _ := demotionPeer(t, sm, 50, 20000)
	require.False(t, peer.Inbound())

	client.failures.Store(1)
	sm.handleHeadersMsg(&headersMsg{headers: headersMsgOf(t, honest[:wire.MaxBlockHeadersPerMsg]), peer: peer})

	_, _, ok := sm.headerCache.PeerTop(peer)
	require.False(t, ok, "the unasked batch made no branch")
	require.Zero(t, sm.headerCache.heldHeaders())
	require.True(t, peer.Connected())
}

// A batch from an asked peer is dropped, not blamed, when the tip cannot be
// read, and the same batch is kept once the read works again.
func TestHeaderRequestRule_AnAskedBatchWaitsForAReadableTip(t *testing.T) {
	sm := headerRuleManager(t)
	honest := mainnetFirstCheckpointHeaders(t)

	peer, _, _ := demotionPeer(t, sm, 51, 20000)
	askForHeaders(t, sm, peer)

	client := wrapTipFaults(sm)
	client.failures.Store(1)

	batch := honest[:wire.MaxBlockHeadersPerMsg]
	sm.handleHeadersMsg(&headersMsg{headers: headersMsgOf(t, batch), peer: peer})

	require.Zero(t, sm.headerCache.heldHeaders(), "nothing is held while the tip is unreadable")
	require.True(t, peer.Connected())

	sm.handleHeadersMsg(&headersMsg{headers: headersMsgOf(t, batch), peer: peer})

	top, _, ok := sm.headerCache.PeerTop(peer)
	require.True(t, ok)
	require.Equal(t, int32(wire.MaxBlockHeadersPerMsg), top)
}

// The rule and the fill share one read of the chain's tip. A batch from an
// asked peer that the fill then refuses, here because it connects to nothing the
// node holds, so that no assignment pass follows, costs exactly one
// GetBestBlockHeader call.
func TestHeaderRequestRule_OneHeadersMessageReadsTheTipOnce(t *testing.T) {
	sm := headerRuleManager(t)
	honest := mainnetFirstCheckpointHeaders(t)

	peer, _, _ := demotionPeer(t, sm, 52, 20000)
	askForHeaders(t, sm, peer)

	client := wrapTipFaults(sm)

	sm.handleHeadersMsg(&headersMsg{headers: headersMsgOf(t, honest[3000:3000+wire.MaxBlockHeadersPerMsg]), peer: peer})

	require.Equal(t, int32(1), client.calls.Load(), "one headers message, one tip read")
	require.Zero(t, sm.headerCache.heldHeaders(), "a batch that connects to nothing is held nowhere")
	require.True(t, peer.Connected())
}

// A download pass whose read of the committed tip fails does nothing. It used to report height
// zero to the header cache as the tip, which the cache takes as a reorg to genesis: the floor
// moved down from 11,000 and the honest branch above it no longer connected to it.
func TestAssignWantedBlocks_AFailedTipReadLeavesTheHeaderCacheAlone(t *testing.T) {
	sm := windowManager(t)
	honest := mainnetFirstCheckpointHeaders(t)

	outbound, rec, _ := demotionPeer(t, sm, 102, 20000)
	askForHeaders(t, sm, outbound)
	sendHeadersInReplies(t, sm, outbound, honest[windowCheckpoint:])

	top, ok := sm.headerCache.Top()
	require.True(t, ok)
	require.Equal(t, int32(11111), top)

	client := wrapTipFaults(sm)
	client.failures.Store(1)

	sm.assignWantedBlocks()

	top, ok = sm.headerCache.Top()
	require.True(t, ok, "the branch still connects to the committed tip")
	require.Equal(t, int32(11111), top)

	sm.headerCache.mu.Lock()
	floor := sm.headerCache.floorHeight
	sm.headerCache.mu.Unlock()
	require.Equal(t, int32(windowCheckpoint), floor, "the floor is still the committed tip")
	require.Zero(t, rec.count(), "and nobody was asked for a block")
	require.Equal(t, int32(1), client.calls.Load(), "the pass read the tip once and stopped")
}
