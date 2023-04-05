package propagation

import (
	"context"

	"github.com/libsv/go-p2p"
	"github.com/libsv/go-p2p/wire"
	"github.com/ordishs/go-utils"
)

// The plan.

// 1. Connect to reg test mode P2P interface
// 2. Listen for new transaction or block announcements (INV)
// 3. Request full transaction or block when new transaction or block notification is received (GET DATA)
// 4. Validate transaction or block
// 5. Store transaction or block
// 6. Announce transaction or block to other peers (INV)
// 7. Repeat

type Server struct {
	logger      utils.Logger
	peerHandler p2p.PeerHandlerI
}

func NewServer(logger utils.Logger) *Server {
	return &Server{
		logger:      logger,
		peerHandler: NewPeerHandler(),
	}
}

func (s *Server) Start(ctx context.Context) error {
	pm := p2p.NewPeerManager(s.logger, wire.TestNet)

	peer, err := p2p.NewPeer(s.logger, "localhost:18333", s.peerHandler, wire.TestNet)
	if err != nil {
		s.logger.Fatalf("error creating peer %s: %v", "localhost:18333", err)
	}

	if err = pm.AddPeer(peer); err != nil {
		s.logger.Fatalf("error adding peer %s: %v", "localhost:18333", err)
	}

	<-ctx.Done()

	return nil
}

func (s *Server) Stop(ctx context.Context) {
	_, cancel := context.WithCancel(ctx)
	defer cancel()
}
