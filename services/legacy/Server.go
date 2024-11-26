package legacy

import (
	"context"
	"net/http"

	"github.com/bitcoin-sv/ubsv/chaincfg"
	"github.com/bitcoin-sv/ubsv/errors"
	"github.com/bitcoin-sv/ubsv/services/blockassembly"
	"github.com/bitcoin-sv/ubsv/services/blockchain"
	"github.com/bitcoin-sv/ubsv/services/blockvalidation"
	"github.com/bitcoin-sv/ubsv/services/legacy/peer_api"
	"github.com/bitcoin-sv/ubsv/services/legacy/wire"
	"github.com/bitcoin-sv/ubsv/services/subtreevalidation"
	"github.com/bitcoin-sv/ubsv/services/validator"
	"github.com/bitcoin-sv/ubsv/stores/blob"
	"github.com/bitcoin-sv/ubsv/stores/utxo"
	"github.com/bitcoin-sv/ubsv/ulogger"
	"github.com/bitcoin-sv/ubsv/util"
	"github.com/bitcoin-sv/ubsv/util/health"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/ordishs/gocore"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/emptypb"
)

type Server struct {
	peer_api.UnimplementedPeerServiceServer
	logger ulogger.Logger
	stats  *gocore.Stat
	server *server
	// tb        *TeranodeBridge
	lastHash *chainhash.Hash
	height   uint32

	// ubsv stores
	blockchainClient  blockchain.ClientI
	validationClient  validator.Interface
	subtreeStore      blob.Store
	tempStore         blob.Store
	utxoStore         utxo.Store
	subtreeValidation subtreevalidation.Interface
	blockValidation   blockvalidation.Interface
	blockAssembly     *blockassembly.Client
}

// New will return a server instance with the logger stored within it
func New(logger ulogger.Logger,
	blockchainClient blockchain.ClientI,
	validationClient validator.Interface,
	subtreeStore blob.Store,
	tempStore blob.Store,
	utxoStore utxo.Store,
	subtreeValidation subtreevalidation.Interface,
	blockValidation blockvalidation.Interface,
	blockAssembly *blockassembly.Client,
) *Server {

	initPrometheusMetrics()

	return &Server{
		logger:            logger,
		stats:             gocore.NewStat("legacy"),
		blockchainClient:  blockchainClient,
		validationClient:  validationClient,
		subtreeStore:      subtreeStore,
		tempStore:         tempStore,
		utxoStore:         utxoStore,
		subtreeValidation: subtreeValidation,
		blockValidation:   blockValidation,
		blockAssembly:     blockAssembly,
	}
}

func (s *Server) Health(ctx context.Context, checkLiveness bool) (int, string, error) {
	if checkLiveness {
		// Add liveness checks here. Don't include dependency checks.
		// If the service is stuck return http.StatusServiceUnavailable
		// to indicate a restart is needed
		return http.StatusOK, "OK", nil
	}

	// Add readiness checks here. Include dependency checks.
	// If any dependency is not ready, return http.StatusServiceUnavailable
	// If all dependencies are ready, return http.StatusOK
	// A failed dependency check does not imply the service needs restarting
	checks := []health.Check{
		{Name: "BlockchainClient", Check: s.blockchainClient.Health},
		{Name: "ValidationClient", Check: s.validationClient.Health},
		{Name: "SubtreeStore", Check: s.subtreeStore.Health},
		{Name: "UTXOStore", Check: s.utxoStore.Health},
		{Name: "SubtreeValidation", Check: s.subtreeValidation.Health},
		{Name: "BlockValidation", Check: s.blockValidation.Health},
		{Name: "FSM", Check: blockchain.CheckFSM(s.blockchainClient)},
	}

	return health.CheckAll(ctx, checkLiveness, checks)
}

func (s *Server) Init(ctx context.Context) error {
	var err error

	// Create a new Teranode bridge
	// if !gocore.Config().GetBool("legacy_direct", true) {
	//	s.tb, err = NewTeranodeBridge(ctx, s.logger)
	//	if err != nil {
	//		s.logger.Fatalf("Failed to create Teranode bridge: %v", err)
	//	}
	// }
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
		s.tempStore,
		s.subtreeValidation,
		s.blockValidation,
		s.blockAssembly,
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

func (s *Server) GetPeers(ctx context.Context, _ *emptypb.Empty) (*peer_api.GetPeersResponse, error) {
	s.logger.Debugf("GetPeers called")

	if s.server == nil {
		return nil, errors.NewError("server is not initialized")
	}

	s.logger.Debugf("Creating reply channel")
	serverPeers := s.server.getPeers()

	resp := &peer_api.GetPeersResponse{}
	for _, sp := range serverPeers {
		resp.Peers = append(resp.Peers, &peer_api.Peer{
			Id:        sp.ID(),
			Addr:      sp.Addr(),
			AddrLocal: sp.LocalAddr().String(),
			Services:  sp.Services().String(),
			LastSend:  sp.LastSend().Unix(),
			LastRecv:  sp.LastRecv().Unix(),
			// ConnTime:       sp.ConnTime.Unix(),
			PingTime:   sp.LastPingMicros(),
			TimeOffset: sp.TimeOffset(),
			Version:    sp.ProtocolVersion(),
			// SubVer:         sp.SubVer(),
			StartingHeight: sp.StartingHeight(),
			CurrentHeight:  sp.LastBlock(),
			Banscore:       int32(sp.banScore.Int()),
			Whitelisted:    sp.isWhitelisted,
			FeeFilter:      sp.feeFilter,
			// SendSize:         sp.SendSize(),
			// RecvSize:         sp.RecvSize(),
			// SendMemory:       sp.SendMemory(),
			// PauseSend:        sp.PauseSend(),
			// UnpauseSend:      sp.UnpauseSend(),
			BytesSent:     sp.BytesSent(),
			BytesReceived: sp.BytesReceived(),
			// AvgRecvBandwidth: sp.AvgRecvBandwidth(),
			// AssocId:          sp.AssocId(),
			// StreamPolicy:     sp.StreamPolicy(),
			Inbound: sp.Inbound(),
		})
	}
	return resp, nil
}

// Start function
func (s *Server) Start(ctx context.Context) error {
	s.logger.Infof("[Legacy Server] Starting...")

	// Check if we need to Restore. If so, move FSM to the Restore state
	// Restore will block and wait for RUN event to be manually sent
	// TODO: think if we can automate transition to RUN state after restore is complete.
	fsmStateRestore := gocore.Config().GetBool("fsm_state_restore", false)
	if fsmStateRestore {
		// Send Restore event to FSM
		err := s.blockchainClient.Restore(ctx)
		if err != nil {
			s.logger.Errorf("[Legacy Server] failed to send Restore event [%v], this should not happen, FSM will continue without Restoring", err)
		}

		// Wait for node to finish Restoring.
		// this means FSM got a RUN event and transitioned to RUN state
		// this will block
		s.logger.Infof("[Legacy Server] Node is restoring, waiting for FSM to transition to Running state")
		_ = s.blockchainClient.WaitForFSMtoTransitionToGivenState(ctx, blockchain.FSMStateRUNNING)
		s.logger.Infof("[Legacy Server] Node finished restoring and has transitioned to Running state, continuing to start Legacy service")
	}

	// Tell FSM that we are in legacy sync, so it will transition to LegacySync state
	err := s.blockchainClient.LegacySync(ctx)
	if err != nil {
		s.logger.Errorf("[Legacy Server] failed to send Legacy Sync event to the FSM [%v]", err)
	}

	s.logger.Infof("[Legacy Server] Starting internal server...")
	s.server.Start()
	s.logger.Infof("[Legacy Server] Internal server started")

	// this will block
	if err = util.StartGRPCServer(ctx, s.logger, "legacy", func(server *grpc.Server) {
		peer_api.RegisterPeerServiceServer(server, s)
	}); err != nil {
		return errors.WrapGRPC(errors.NewServiceNotStartedError("[Legacy] can't start GRPC server", err))
	}

	return nil
}

func (s *Server) Stop(_ context.Context) error {
	return s.server.Stop()
}
