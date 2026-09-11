package netsync

import (
	"context"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	txmap "github.com/bsv-blockchain/go-tx-map"
	"github.com/bsv-blockchain/go-wire"
	"github.com/bsv-blockchain/teranode/errors"
	blockchain2 "github.com/bsv-blockchain/teranode/services/blockchain"
	peerpkg "github.com/bsv-blockchain/teranode/services/legacy/peer"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func Test_invMsg(t *testing.T) {
	t.Run("IPv4", func(t *testing.T) {
		wireInvMsg := wire.NewMsgInv()
		_ = wireInvMsg.AddInvVect(&wire.InvVect{
			Type: wire.InvTypeBlock,
			Hash: chainhash.Hash{0x01, 0x02, 0x03, 0x04},
		})
		tSettings := test.CreateBaseTestSettings(t)

		peer, err := peerpkg.NewOutboundPeer(ulogger.TestLogger{}, tSettings, &peerpkg.Config{}, "localhost:8333")
		require.NoError(t, err)

		sm := &SyncManager{
			peerStates: txmap.NewSyncedMap[*peerpkg.Peer, *peerSyncState](),
		}

		sm.peerStates.Set(peer, &peerSyncState{})

		invMsg := &invMsg{
			inv:  wireInvMsg,
			peer: peer,
		}

		invBytes := invMsg.Bytes()
		assert.Len(t, invBytes, 67)

		newInvMsg, err := sm.newInvFromBytes(invBytes)
		require.NoError(t, err)

		assert.Equal(t, invMsg.inv, newInvMsg.inv)
		assert.Equal(t, invMsg.peer.Addr(), newInvMsg.peer.Addr())
		assert.Equal(t, invMsg.peer, newInvMsg.peer) // checks actual memory address of peer

		// test kafka message marshall/un-marshall
		kafkaMessage := sm.newKafkaMessageFromInv(invMsg.inv, invMsg.peer)

		newInvMsg, err = sm.newInvFromKafkaMessage(kafkaMessage)
		require.NoError(t, err)

		assert.Equal(t, invMsg.inv, newInvMsg.inv)
		assert.Equal(t, invMsg.peer.Addr(), newInvMsg.peer.Addr())
		assert.Equal(t, invMsg.peer, newInvMsg.peer) // checks actual memory address of peer
	})

	t.Run("IPv6 short", func(t *testing.T) {
		wireInvMsg := wire.NewMsgInv()
		_ = wireInvMsg.AddInvVect(&wire.InvVect{
			Type: wire.InvTypeBlock,
			Hash: chainhash.Hash{0x01, 0x02, 0x03, 0x04},
		})
		tSettings := test.CreateBaseTestSettings(t)

		peer, err := peerpkg.NewOutboundPeer(ulogger.TestLogger{}, tSettings, &peerpkg.Config{}, "[::1]:8333")
		require.NoError(t, err)

		sm := &SyncManager{
			peerStates: txmap.NewSyncedMap[*peerpkg.Peer, *peerSyncState](),
		}

		sm.peerStates.Set(peer, &peerSyncState{})

		invMsg := &invMsg{
			inv:  wireInvMsg,
			peer: peer,
		}

		invBytes := invMsg.Bytes()
		assert.Len(t, invBytes, 63)

		newInvMsg, err := sm.newInvFromBytes(invBytes)
		require.NoError(t, err)

		assert.Equal(t, invMsg.inv, newInvMsg.inv)
		assert.Equal(t, invMsg.peer.Addr(), newInvMsg.peer.Addr())
		assert.Equal(t, invMsg.peer, newInvMsg.peer) // checks actual memory address of peer

		// test kafka message marshall/un-marshall
		kafkaMessage := sm.newKafkaMessageFromInv(invMsg.inv, invMsg.peer)
		newInvMsg, err = sm.newInvFromKafkaMessage(kafkaMessage)
		require.NoError(t, err)

		assert.Equal(t, invMsg.inv, newInvMsg.inv)
		assert.Equal(t, invMsg.peer.Addr(), newInvMsg.peer.Addr())
		assert.Equal(t, invMsg.peer, newInvMsg.peer) // checks actual memory address of peer
	})

	t.Run("IPv6 long", func(t *testing.T) {
		wireInvMsg := wire.NewMsgInv()
		_ = wireInvMsg.AddInvVect(&wire.InvVect{
			Type: wire.InvTypeBlock,
			Hash: chainhash.Hash{0x01, 0x02, 0x03, 0x04},
		})
		tSettings := test.CreateBaseTestSettings(t)

		peer, err := peerpkg.NewOutboundPeer(ulogger.TestLogger{}, tSettings, &peerpkg.Config{}, "[2600:1f18:573a:32f:ba74:c04d:50a3:ca7d]:8333")
		require.NoError(t, err)

		sm := &SyncManager{
			peerStates: txmap.NewSyncedMap[*peerpkg.Peer, *peerSyncState](),
		}

		sm.peerStates.Set(peer, &peerSyncState{})

		invMsg := &invMsg{
			inv:  wireInvMsg,
			peer: peer,
		}

		invBytes := invMsg.Bytes()
		assert.Len(t, invBytes, 98)

		newInvMsg, err := sm.newInvFromBytes(invBytes)
		require.NoError(t, err)

		assert.Equal(t, invMsg.inv, newInvMsg.inv)
		assert.Equal(t, invMsg.peer.Addr(), newInvMsg.peer.Addr())
		assert.Equal(t, invMsg.peer, newInvMsg.peer) // checks actual memory address of peer

		// test kafka message marshall/un-marshall
		kafkaMessage := sm.newKafkaMessageFromInv(invMsg.inv, invMsg.peer)
		newInvMsg, err = sm.newInvFromKafkaMessage(kafkaMessage)
		require.NoError(t, err)

		assert.Equal(t, invMsg.inv, newInvMsg.inv)
		assert.Equal(t, invMsg.peer.Addr(), newInvMsg.peer.Addr())
		assert.Equal(t, invMsg.peer, newInvMsg.peer) // checks actual memory address of peer
	})
}

// TestProcessInvMsg_BlocksAreNotGatedOnTheRunningState pins the behaviour two
// readers have now reported as a bug, because the comment above the state read
// used to claim it covered blocks and the switch never did.
//
// It is not a bug. Past the last checkpoint headers-first mode is off, and an inv
// is then the only way this node hears that a block exists, so gating blocks on
// RUNNING would leave a node that is catching blocks with no block discovery.
// The Kafka listeners are wired the same way: block listener always enabled,
// transaction listener gated on RUNNING.
//
// Asserted on the request queue, which is what the decision is for: with the
// gate shut a block inv still queues a getdata and a transaction inv does not.
func TestProcessInvMsg_BlocksAreNotGatedOnTheRunningState(t *testing.T) {
	tSettings := test.CreateBaseTestSettings(t)

	newHarness := func(t *testing.T) (*SyncManager, *peerpkg.Peer, *peerSyncState) {
		t.Helper()

		p, err := peerpkg.NewOutboundPeer(ulogger.TestLogger{}, tSettings, &peerpkg.Config{}, "localhost:8333")
		require.NoError(t, err)

		client := &blockchain2.Mock{}
		// Nothing is stored, so every announcement is for a block this node does
		// not have, which is the case that decides whether to fetch.
		client.On("GetBlockHeader", mock.Anything, mock.Anything).
			Return(nil, nil, errors.NewBlockNotFoundError("no such block"))

		sm := &SyncManager{
			logger:           ulogger.TestLogger{},
			settings:         tSettings,
			ctx:              context.Background(),
			peerStates:       txmap.NewSyncedMap[*peerpkg.Peer, *peerSyncState](),
			blockchainClient: client,
			blockPark:        newBlockPark(ulogger.TestLogger{}, tSettings, nil),
			rejectedTxns:     txmap.NewSyncedMap[chainhash.Hash, struct{}](10),
		}
		// Headers-first off is the state this matters in: past the last
		// checkpoint an inv is the only block discovery this node has.
		sm.headersFirstMode.Store(false)

		state := &peerSyncState{
			requestQueue: txmap.NewSyncedSlice[wire.InvVect](maxRequestedBlocks),
		}
		sm.peerStates.Set(p, state)

		return sm, p, state
	}

	blockInv := wire.NewInvVect(wire.InvTypeBlock, &chainhash.Hash{0x0b})
	txInv := wire.NewInvVect(wire.InvTypeTx, &chainhash.Hash{0x7a})

	t.Run("a block inv is queued with the gate shut", func(t *testing.T) {
		sm, p, state := newHarness(t)

		sm.processInvMsg(0, blockInv, false, p, false, state, -1)

		require.Equal(t, 1, state.requestQueue.Length(),
			"a block announcement must be fetched outside RUNNING, or a node past its last checkpoint never hears about a block")
	})

	t.Run("a transaction inv is dropped with the gate shut", func(t *testing.T) {
		sm, p, state := newHarness(t)

		sm.processInvMsg(0, txInv, false, p, false, state, -1)

		require.Zero(t, state.requestQueue.Length(),
			"a transaction this node will not validate yet must not be fetched")
	})
}
