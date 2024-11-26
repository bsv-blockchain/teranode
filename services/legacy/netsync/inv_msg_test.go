package netsync

import (
	"testing"

	peerpkg "github.com/bitcoin-sv/ubsv/services/legacy/peer"
	"github.com/bitcoin-sv/ubsv/services/legacy/wire"
	"github.com/bitcoin-sv/ubsv/ulogger"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func Test_invMsg(t *testing.T) {
	t.Run("IPv4", func(t *testing.T) {
		wireInvMsg := wire.NewMsgInv()
		_ = wireInvMsg.AddInvVect(&wire.InvVect{
			Type: wire.InvTypeBlock,
			Hash: chainhash.Hash{0x01, 0x02, 0x03, 0x04},
		})

		peer, err := peerpkg.NewOutboundPeer(ulogger.TestLogger{}, &peerpkg.Config{}, "localhost:8333")
		require.NoError(t, err)

		sm := &SyncManager{
			peerStates: map[*peerpkg.Peer]*peerSyncState{},
		}

		sm.peerStates[peer] = &peerSyncState{}

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
	})

	t.Run("IPv6 short", func(t *testing.T) {
		wireInvMsg := wire.NewMsgInv()
		_ = wireInvMsg.AddInvVect(&wire.InvVect{
			Type: wire.InvTypeBlock,
			Hash: chainhash.Hash{0x01, 0x02, 0x03, 0x04},
		})

		peer, err := peerpkg.NewOutboundPeer(ulogger.TestLogger{}, &peerpkg.Config{}, "[::1]:8333")
		require.NoError(t, err)

		sm := &SyncManager{
			peerStates: map[*peerpkg.Peer]*peerSyncState{},
		}

		sm.peerStates[peer] = &peerSyncState{}

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
	})

	t.Run("IPv6 long", func(t *testing.T) {
		wireInvMsg := wire.NewMsgInv()
		_ = wireInvMsg.AddInvVect(&wire.InvVect{
			Type: wire.InvTypeBlock,
			Hash: chainhash.Hash{0x01, 0x02, 0x03, 0x04},
		})

		peer, err := peerpkg.NewOutboundPeer(ulogger.TestLogger{}, &peerpkg.Config{}, "[2600:1f18:573a:32f:ba74:c04d:50a3:ca7d]:8333")
		require.NoError(t, err)

		sm := &SyncManager{
			peerStates: map[*peerpkg.Peer]*peerSyncState{},
		}

		sm.peerStates[peer] = &peerSyncState{}

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
	})
}
