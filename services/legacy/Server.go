package legacy

import (
	"context"

	"github.com/bitcoin-sv/ubsv/services/blockvalidation"
	"github.com/bitcoin-sv/ubsv/services/subtreevalidation"

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
	logger ulogger.Logger
	stats  *gocore.Stat
	server *server
	//tb        *TeranodeBridge
	lastHash *chainhash.Hash
	height   uint32

	// ubsv stores
	blockchainClient  blockchain.ClientI
	validationClient  validator.Interface
	subtreeStore      blob.Store
	utxoStore         utxo.Store
	subtreeValidation subtreevalidation.Interface
	blockValidation   blockvalidation.Interface
}

// New will return a server instance with the logger stored within it
func New(logger ulogger.Logger,
	blockchainClient blockchain.ClientI,
	validationClient validator.Interface,
	subtreeStore blob.Store,
	utxoStore utxo.Store,
	subtreeValidation subtreevalidation.Interface,
	blockValidation blockvalidation.Interface,
) *Server {

	// initPrometheusMetrics()

	return &Server{
		logger:            logger,
		stats:             gocore.NewStat("legacy"),
		blockchainClient:  blockchainClient,
		validationClient:  validationClient,
		subtreeStore:      subtreeStore,
		utxoStore:         utxoStore,
		subtreeValidation: subtreeValidation,
		blockValidation:   blockValidation,
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

	// seed.bitcoinsv.io
	// TODO use testnet-seed.bitcoinsv.io for testnet, stn-seed.bitcoinsv.io for STN
	connectAddresses, _ := gocore.Config().GetMulti("legacy_connect_peers", "|", []string{"44.213.141.106:8333|13.213.100.250:8333|18.199.12.185:8333"})

	// get the public IP and listen on it
	ip := GetOutboundIP()
	defaultListenAddresses := []string{ip.String() + ":8333"}
	// TODO not setting any listen addresses triggers upnp, which does not seem to work yet
	listenAddresses, _ := gocore.Config().GetMulti("legacy_listen_addresses", "|", defaultListenAddresses)

	assetHttpAddress, ok := gocore.Config().Get("asset_httpAddress", "")
	if !ok {
		panic("missing setting: asset_httpAddress")
	}

	s.server, err = newServer(ctx, s.logger, gocore.Config(),
		s.blockchainClient,
		s.validationClient,
		s.utxoStore,
		s.subtreeStore,
		s.subtreeValidation,
		s.blockValidation,
		listenAddresses,
		&chaincfg.MainNetParams,
		assetHttpAddress,
	)
	if err != nil {
		return err
	}

	for _, addr := range connectAddresses {
		_ = s.server.addrManager.AddAddressByIP(addr)
	}

	return nil
}

// Start function
func (s *Server) Start(ctx context.Context) error {
	s.server.Start()

	go func() {
		<-ctx.Done()
		s.server.Stop()
	}()

	return nil
}

func (s *Server) Stop(_ context.Context) error {
	return s.server.Stop()
}
