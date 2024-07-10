package legacy

import (
	"context"
	"github.com/bitcoin-sv/ubsv/stores/blob"
	"github.com/bitcoin-sv/ubsv/stores/blockchain"
	"github.com/bitcoin-sv/ubsv/stores/utxo"
	"github.com/bitcoin-sv/ubsv/ulogger"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/ordishs/gocore"
)

type Server struct {
	logger      ulogger.Logger
	stats       *gocore.Stat
	peerManager *PeerManager
	//tb          *TeranodeBridge
	lastHash *chainhash.Hash
	height   uint32
}

// New will return a server instance with the logger stored within it
func New(logger ulogger.Logger, blockchainStore blockchain.Store, subtreeStore blob.Store, utxoStore utxo.Store) *Server {
	// initPrometheusMetrics()

	return &Server{
		logger:      logger,
		stats:       gocore.NewStat("legacy"),
		peerManager: NewPeerManager(logger, blockchainStore, subtreeStore, utxoStore),
	}
}

func (s *Server) Health(_ context.Context) (int, string, error) {
	return 0, "", nil
}

func (s *Server) Init(ctx context.Context) error {
	//var err error

	// Create a new Teranode bridge
	//if !gocore.Config().GetBool("legacy_direct", true) {
	//	s.tb, err = NewTeranodeBridge(ctx, s.logger)
	//	if err != nil {
	//		s.logger.Fatalf("Failed to create Teranode bridge: %v", err)
	//	}
	//}

	return nil
}

// Start function
func (s *Server) Start(ctx context.Context) error {
	return s.peerManager.Start(ctx)
}

func (s *Server) Stop(ctx context.Context) error {
	return s.peerManager.Stop(ctx)
}
