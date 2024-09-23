package legacy

import (
	"context"
	"github.com/bitcoin-sv/ubsv/errors"
	"github.com/bitcoin-sv/ubsv/services/blockchain/blockchain_api"
	"github.com/bitcoin-sv/ubsv/services/blockvalidation"
	"github.com/bitcoin-sv/ubsv/services/subtreevalidation"
	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/bitcoin-sv/ubsv/chaincfg"
	"github.com/bitcoin-sv/ubsv/services/blockchain"
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

	network, _ := gocore.Config().Get("network", "mainnet")
	chainParams, err := chaincfg.GetChainParams(network)

	// get the public IP and listen on it
	ip, err := GetOutboundIP()
	if err != nil {
		return err
	}
	defaultListenAddresses := []string{ip.String() + ":8333"}
	// TODO not setting any listen addresses triggers upnp, which does not seem to work yet
	listenAddresses, _ := gocore.Config().GetMulti("legacy_listen_addresses", "|", defaultListenAddresses)

	assetHttpAddress, ok := gocore.Config().Get("asset_httpAddress", "")
	if !ok {
		return errors.NewConfigurationError("missing setting: asset_httpAddress")
	}

	s.server, err = newServer(ctx, s.logger, gocore.Config(),
		s.blockchainClient,
		s.validationClient,
		s.utxoStore,
		s.subtreeStore,
		s.subtreeValidation,
		s.blockValidation,
		listenAddresses,
		chainParams,
		assetHttpAddress,
	)
	if err != nil {
		return err
	}

	// TODO: is this still needed? Also defined in services/legacy/peer_server.go:2271
	connectAddresses, _ := gocore.Config().GetMulti("legacy_connect_peers", "|", []string{})
	for _, addr := range connectAddresses {
		_ = s.server.addrManager.AddAddressByIP(addr)
	}

	return nil
}

// Start function
func (s *Server) Start(ctx context.Context) error {

	// Check if we need to Restore. If so, move FSM to the Restore state
	// Restore will block and wait for RUN event to be manually sent
	// TODO: think if we can automate transition to RUN state after restore is complete.
	fsmStateRestore := gocore.Config().GetBool("fsm_state_restore", false)
	if fsmStateRestore {
		// Send Restore event to FSM
		_, err := s.blockchainClient.Restore(ctx, &emptypb.Empty{})
		if err != nil {
			s.logger.Errorf("[Legacy Server] failed to send Restore event [%v], this should not happen, FSM will continue without Restoring", err)
		}

		// Wait for node to finish Restoring.
		// this means FSM got a RUN event and transitioned to RUN state
		// this will block
		s.logger.Infof("[Legacy Server] Node is restoring, waiting for FSM to transition to Running state")
		_ = s.blockchainClient.WaitForFSMtoTransitionToGivenState(ctx, blockchain_api.FSMStateType_RUNNING)
		s.logger.Infof("[Legacy Server] Node finished restoring and has transitioned to Running state, continuing to start Legacy service")
	}

	// Tell FSM that we are in legacy sync, so it will transition to LegacySync state
	_, err := s.blockchainClient.LegacySync(ctx, &emptypb.Empty{})
	if err != nil {
		s.logger.Errorf("[Legacy Server] failed to send Legacy Sync event to the FSM [%v]", err)
	}

	go func() {
		<-ctx.Done()
		_ = s.server.Stop()
	}()

	s.server.Start()

	return nil
}

func (s *Server) Stop(_ context.Context) error {
	return s.server.Stop()
}
