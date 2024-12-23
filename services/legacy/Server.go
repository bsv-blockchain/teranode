package legacy

import (
	"context"
	"net/http"

	"github.com/bitcoin-sv/teranode/errors"
	"github.com/bitcoin-sv/teranode/services/blockassembly"
	"github.com/bitcoin-sv/teranode/services/blockchain"
	"github.com/bitcoin-sv/teranode/services/blockvalidation"
	"github.com/bitcoin-sv/teranode/services/legacy/peer_api"
	"github.com/bitcoin-sv/teranode/services/legacy/wire"
	"github.com/bitcoin-sv/teranode/services/subtreevalidation"
	"github.com/bitcoin-sv/teranode/services/validator"
	"github.com/bitcoin-sv/teranode/settings"
	"github.com/bitcoin-sv/teranode/stores/blob"
	"github.com/bitcoin-sv/teranode/stores/utxo"
	"github.com/bitcoin-sv/teranode/ulogger"
	"github.com/bitcoin-sv/teranode/util"
	"github.com/bitcoin-sv/teranode/util/health"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/ordishs/gocore"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/emptypb"
)

type Server struct {
	peer_api.UnimplementedPeerServiceServer
	logger   ulogger.Logger
	settings *settings.Settings
	stats    *gocore.Stat
	server   *server
	lastHash *chainhash.Hash
	height   uint32

	// teranode stores
	blockchainClient    blockchain.ClientI
	validationClient    validator.Interface
	subtreeStore        blob.Store
	tempStore           blob.Store
	utxoStore           utxo.Store
	subtreeValidation   subtreevalidation.Interface
	blockValidation     blockvalidation.Interface
	blockAssemblyClient *blockassembly.Client
}

// New will return a server instance with the logger stored within it
func New(logger ulogger.Logger,
	tSettings *settings.Settings,
	blockchainClient blockchain.ClientI,
	validationClient validator.Interface,
	subtreeStore blob.Store,
	tempStore blob.Store,
	utxoStore utxo.Store,
	subtreeValidation subtreevalidation.Interface,
	blockValidation blockvalidation.Interface,
	blockAssemblyClient *blockassembly.Client,
) *Server {

	initPrometheusMetrics()

	return &Server{
		logger:              logger,
		settings:            tSettings,
		stats:               gocore.NewStat("legacy"),
		blockchainClient:    blockchainClient,
		validationClient:    validationClient,
		subtreeStore:        subtreeStore,
		tempStore:           tempStore,
		utxoStore:           utxoStore,
		subtreeValidation:   subtreeValidation,
		blockValidation:     blockValidation,
		blockAssemblyClient: blockAssemblyClient,
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
	checks := make([]health.Check, 0, 8)

	if s.blockchainClient != nil {
		checks = append(checks, health.Check{Name: "BlockchainClient", Check: s.blockchainClient.Health})
		checks = append(checks, health.Check{Name: "FSM", Check: blockchain.CheckFSM(s.blockchainClient)})
	}

	if s.validationClient != nil {
		checks = append(checks, health.Check{Name: "ValidationClient", Check: s.validationClient.Health})
	}

	if s.subtreeStore != nil {
		checks = append(checks, health.Check{Name: "SubtreeStore", Check: s.subtreeStore.Health})
	}

	if s.utxoStore != nil {
		checks = append(checks, health.Check{Name: "UTXOStore", Check: s.utxoStore.Health})
	}

	if s.subtreeValidation != nil {
		checks = append(checks, health.Check{Name: "SubtreeValidation", Check: s.subtreeValidation.Health})
	}

	if s.blockValidation != nil {
		checks = append(checks, health.Check{Name: "BlockValidation", Check: s.blockValidation.Health})
	}

	if s.blockAssemblyClient != nil {
		checks = append(checks, health.Check{Name: "BlockAssembly", Check: s.blockAssemblyClient.Health})
	}

	return health.CheckAll(ctx, checkLiveness, checks)
}

func (s *Server) Init(ctx context.Context) error {
	var err error

	wire.SetLimits(4000000000)

	// get the public IP and listen on it
	ip, err := GetOutboundIP()
	if err != nil {
		return err
	}
	defaultListenAddresses := []string{ip.String() + ":8333"}
	// TODO not setting any listen addresses triggers upnp, which does not seem to work yet
	listenAddresses := s.settings.Legacy.ListenAddresses
	if len(listenAddresses) == 0 {
		listenAddresses = defaultListenAddresses
	}

	assetHTTPAddress := s.settings.Asset.HTTPAddress
	if assetHTTPAddress == "" {
		return errors.NewConfigurationError("missing setting: asset_httpAddress")
	}

	s.server, err = newServer(ctx, s.logger, s.settings, gocore.Config(),
		s.blockchainClient,
		s.validationClient,
		s.utxoStore,
		s.subtreeStore,
		s.tempStore,
		s.subtreeValidation,
		s.blockValidation,
		s.blockAssemblyClient,
		listenAddresses,
		assetHTTPAddress,
	)
	if err != nil {
		return err
	}

	// TODO: is this still needed? Also defined in services/legacy/peer_server.go:2271
	connectAddresses := s.settings.Legacy.ConnectPeers

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
