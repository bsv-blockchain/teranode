package p2p

import (
	"fmt"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/bitcoin-sv/ubsv/services/legacy/wire"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var (
	tx1        = "b042f298deabcebbf15355aa3a13c7d7cfe96c44ac4f492735f936f8e50d06f6"
	tx1Hash, _ = chainhash.NewHashFromStr(tx1)
)

func TestNewPeerManager(t *testing.T) {

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))

	t.Run("nil peers no error", func(t *testing.T) {
		pm := NewPeerManager(logger, wire.TestNet)
		require.NotNil(t, pm)
	})

	t.Run("1 peer", func(t *testing.T) {
		pm := NewPeerManager(logger, wire.TestNet)
		require.NotNil(t, pm)

		peerHandler := NewMockPeerHandler()

		peer, err := NewPeer(logger, "localhost:18333", peerHandler, wire.TestNet)
		require.NoError(t, err)

		err = pm.AddPeer(peer)
		require.NoError(t, err)
		assert.Len(t, pm.GetPeers(), 1)
	})

	t.Run("1 peer - de dup", func(t *testing.T) {
		peers := []string{
			"localhost:18333",
			"localhost:18333",
			"localhost:18333",
			"localhost:18333",
		}

		pm := NewPeerManager(logger, wire.TestNet)
		require.NotNil(t, pm)

		peerHandler := NewMockPeerHandler()

		for _, peerAddress := range peers {
			peer, _ := NewPeer(logger, peerAddress, peerHandler, wire.TestNet)
			_ = pm.AddPeer(peer)
		}

		assert.Len(t, pm.GetPeers(), 4)
	})
}

func TestAnnounceNewTransaction(t *testing.T) {
	t.Run("announce tx", func(t *testing.T) {
		logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
		pm := NewPeerManager(logger, wire.TestNet, WithBatchDuration(1*time.Millisecond))
		require.NotNil(t, pm)

		peerHandler := NewMockPeerHandler()

		peer, _ := NewPeerMock("localhost:18333", peerHandler, wire.TestNet)
		err := pm.AddPeer(peer)
		require.NoError(t, err)

		pm.AnnounceTransaction(tx1Hash, nil)

		// we need to wait for the batcher to send the inv
		time.Sleep(5 * time.Millisecond)

		announcements := peer.GetAnnouncements()
		require.Len(t, announcements, 1)
		assert.Equal(t, tx1Hash, announcements[0])
	})

	t.Run("announce tx - multiple peers", func(t *testing.T) {
		logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
		pm := NewPeerManager(logger, wire.TestNet, WithBatchDuration(1*time.Millisecond))
		require.NotNil(t, pm)

		peerHandler := NewMockPeerHandler()

		numberOfPeers := 5
		peers := make([]*PeerMock, numberOfPeers)
		for i := 0; i < numberOfPeers; i++ {
			peers[i], _ = NewPeerMock(fmt.Sprintf("localhost:1833%d", i), peerHandler, wire.TestNet)
			err := pm.AddPeer(peers[i])
			require.NoError(t, err)
		}

		pm.AnnounceTransaction(tx1Hash, nil)

		// we need to wait for the batcher to send the inv
		time.Sleep(5 * time.Millisecond)

		peersMessaged := 0
		for _, peer := range peers {
			announcements := peer.GetAnnouncements()
			if len(announcements) == 0 {
				continue
			}

			require.Len(t, announcements, 1)
			assert.Equal(t, tx1Hash, announcements[0])
			peersMessaged++
		}
		assert.GreaterOrEqual(t, peersMessaged, len(peers)/2)
	})
}

func TestPeerManager_addPeer(t *testing.T) {
}

func TestPeerManager_sendInvBatch(t *testing.T) {
}
