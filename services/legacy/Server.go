package legacy

import (
	"context"

	"github.com/bitcoin-sv/ubsv/services/blockchain"
	"github.com/bitcoin-sv/ubsv/services/legacy/chaincfg"
	"github.com/bitcoin-sv/ubsv/services/legacy/wire"
	"github.com/bitcoin-sv/ubsv/services/validator"
	"github.com/bitcoin-sv/ubsv/stores/blob"
	"github.com/bitcoin-sv/ubsv/stores/utxo"
	"github.com/bitcoin-sv/ubsv/ulogger"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/ordishs/gocore"
)

type Server struct {
	logger      ulogger.Logger
	stats       *gocore.Stat
	peerManager *PeerManager
	server      *server
	//tb        *TeranodeBridge
	lastHash *chainhash.Hash
	height   uint32

	// ubsv stores
	blockchainClient blockchain.ClientI
	validationClient validator.Interface
	subtreeStore     blob.Store
	utxoStore        utxo.Store
}

// New will return a server instance with the logger stored within it
func New(logger ulogger.Logger, blockchainClient blockchain.ClientI, validationClient validator.Interface,
	subtreeStore blob.Store, utxoStore utxo.Store) *Server {
	// initPrometheusMetrics()

	return &Server{
		logger:           logger,
		stats:            gocore.NewStat("legacy"),
		blockchainClient: blockchainClient,
		validationClient: validationClient,
		subtreeStore:     subtreeStore,
		utxoStore:        utxoStore,
		//peerManager: NewPeerManager(logger, blockchainStore, validationClient, subtreeStore, utxoStore),
	}
}

func (s *Server) Health(_ context.Context) (int, string, error) {
	return 0, "", nil
}

func (s *Server) Init(ctx context.Context) error {
	var err error

	// Create a new Teranode bridge
	//if !gocore.Config().GetBool("legacy_direct", true) {
	//	s.tb, err = NewTeranodeBridge(ctx, s.logger)
	//	if err != nil {
	//		s.logger.Fatalf("Failed to create Teranode bridge: %v", err)
	//	}
	//}
	wire.SetLimits(4000000000)

	connectAddresses, _ := gocore.Config().GetMulti("legacy_connect_peers", "|", []string{"54.169.45.196:8333"})
	listenAddresses, _ := gocore.Config().GetMulti("legacy_listen_addresses", "|", []string{"127.0.0.1:8333"})

	s.server, err = newServer(ctx, s.logger, s.blockchainClient, s.validationClient, s.utxoStore, listenAddresses, &chaincfg.MainNetParams)
	if err != nil {
		return err
	}

	for _, addr := range connectAddresses {
		_ = s.server.addrManager.AddAddressByIP(addr)
	}

	return nil
}

// Start function
func (s *Server) Start(_ context.Context) error {
	s.server.Start()
	return nil
}

func (s *Server) Stop(_ context.Context) error {
	return s.server.Stop()
}
