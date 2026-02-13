// Package legacy implements a Bitcoin SV legacy protocol server that handles peer-to-peer communication
// and blockchain synchronization using the traditional Bitcoin network protocol.
//
// The legacy package provides a bridge between modern Bitcoin SV architecture and the traditional
// Bitcoin network protocol. It maintains compatibility with legacy Bitcoin nodes while integrating
// with Teranode's high-performance microservices architecture.
//
// Key features:
// - Full implementation of the Bitcoin P2P protocol
// - Peer discovery and connection management
// - Network address management with persistent storage
// - Ban list management for malicious peers
// - Block and transaction relay
// - Integration with blockchain, validation, and other Teranode services
//
// Architecture:
// The legacy service is designed as a standalone microservice that communicates with other
// Teranode components through well-defined gRPC interfaces. It maintains its own peer connections
// and translates between legacy protocol messages and modern service requests.
//
// Security:
// The package implements various security measures including peer banning, connection limits,
// and protocol validation to protect against DoS attacks and other network threats.
package legacy

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-wire"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/services/blockassembly"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/services/blockvalidation"
	"github.com/bsv-blockchain/teranode/services/legacy/peer_api"
	"github.com/bsv-blockchain/teranode/services/subtreevalidation"
	"github.com/bsv-blockchain/teranode/services/validator"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/stores/blob"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util"
	"github.com/bsv-blockchain/teranode/util/health"
	"github.com/ordishs/gocore"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/emptypb"
)

// Server represents the main legacy protocol server structure that implements the peer service interface.
// It serves as the primary integration point between the legacy Bitcoin protocol and Teranode services.
//
// Server manages peer connections, handles protocol messages, and coordinates with other Teranode
// microservices to provide blockchain synchronization and transaction relay services.
//
// Concurrency safety:
// - The Server maintains its own goroutines for handling peer connections and message processing
// - All method calls should be considered thread-safe unless explicitly noted otherwise
// - The underlying server implementation handles the complexity of concurrent peer connections
type Server struct {
	// UnimplementedPeerServiceServer is embedded to satisfy the gRPC interface
	peer_api.UnimplementedPeerServiceServer

	// logger provides logging functionality
	logger ulogger.Logger

	// settings contains the configuration settings for the server
	settings *settings.Settings

	// stats tracks server statistics
	stats *gocore.Stat

	// server is the internal server implementation
	server *server

	// lastHash stores the most recent block hash
	lastHash *chainhash.Hash

	// height represents the current blockchain height
	height uint32

	// blockchainClient handles blockchain operations
	// Used for querying blockchain state and submitting new blocks
	blockchainClient blockchain.ClientI

	// validationClient handles transaction validation
	// Used to validate incoming transactions before relay
	validationClient validator.Interface

	// subtreeStore provides storage for merkle subtrees
	// Used in block validation and merkle proof verification
	subtreeStore blob.Store

	// tempStore provides temporary storage
	// Used for ephemeral data storage during processing
	tempStore blob.Store

	// utxoStore manages the UTXO set
	// Used for transaction validation and UTXO queries
	utxoStore utxo.Store

	// subtreeValidation handles merkle subtree validation
	// Used to verify merkle proofs and validate block structure
	subtreeValidation subtreevalidation.Interface

	// blockValidation handles block validation
	// Used to validate incoming blocks before acceptance
	blockValidation blockvalidation.Interface

	// blockAssemblyClient handles block assembly operations
	// Used for mining and block template generation
	blockAssemblyClient *blockassembly.Client
}

// New creates and returns a new Server instance with the provided dependencies.
// It initializes Prometheus metrics and returns a configured server ready for use.
//
// New is the recommended constructor for the legacy server and sets up all necessary
// internal state required for proper operation. It does not start any network operations
// or begin listening for connections until the Start method is called.
//
// Parameters:
//   - logger: Provides structured logging capabilities for the server
//   - tSettings: Contains all configuration settings for the server and its components
//   - blockchainClient: Interface to the blockchain service for querying and submitting blocks
//   - validationClient: Interface to the transaction validation service
//   - subtreeStore: Blob storage interface for merkle subtree data
//   - tempStore: Temporary blob storage for ephemeral data
//   - utxoStore: Interface to the UTXO (Unspent Transaction Output) database
//   - subtreeValidation: Interface to the subtree validation service
//   - blockValidation: Interface to the block validation service
//   - blockAssemblyClient: Client for the block assembly service (used for mining)
//
// Returns a properly configured Server instance that is ready to be initialized and started.
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

// Health performs health checks on the server and its dependencies.
// It implements the health.Checker interface for integration with Teranode's health monitoring system.
//
// The Health method performs two types of checks depending on the checkLiveness parameter:
//  1. Liveness checks: Verify if the service is running and responsive but do not check dependencies.
//     These are used to determine if the service should be restarted.
//  2. Readiness checks: Verify if the service and all its dependencies are operational.
//     These are used to determine if the service can accept traffic.
//
// The method aggregates health status from all dependent services including the blockchain client,
// validation client, stores, and connected peers. It also performs custom checks like verifying
// peer connections and recent peer activity.
//
// Parameters:
//   - ctx: Context for cancellation and timeouts
//   - checkLiveness: If true, performs only liveness checks; if false, performs readiness checks
//
// Returns:
//   - HTTP status code (200 for healthy, 503 for unhealthy)
//   - A human-readable status message
//   - Error details if the check failed
func (s *Server) Health(ctx context.Context, checkLiveness bool) (int, string, error) {
	if checkLiveness {
		// Add liveness checks here. Don't include dependency checks.
		// If the service is stuck return http.StatusServiceUnavailable
		// to indicate a restart is needed
		return http.StatusOK, "OK", nil
	}

	// Add readiness checks here. Include dependency checks.
	// If all dependencies are ready, return http.StatusOK
	// A failed dependency check does not imply the service needs restarting
	checks := make([]health.Check, 0, 10)

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

	// Add custom check for peer connection
	checks = append(checks, health.Check{
		Name: "PeerConnection",
		Check: func(ctx context.Context, checkLiveness bool) (int, string, error) {
			peersResp, err := s.GetPeers(ctx, &emptypb.Empty{})
			if err != nil {
				return http.StatusServiceUnavailable, "Failed to get peers for health check", err
			}
			peers := peersResp.GetPeers()
			if len(peers) == 0 {
				return http.StatusServiceUnavailable, "No connected peers", nil
			}
			return http.StatusOK, fmt.Sprintf("Connected to %d peers", len(peers)), nil
		},
	})

	// Add custom check for recent peer activity
	checks = append(checks, health.Check{
		Name: "PeerActivity",
		Check: func(ctx context.Context, checkLiveness bool) (int, string, error) {
			peersResp, err := s.GetPeers(ctx, &emptypb.Empty{})
			if err != nil {
				return http.StatusServiceUnavailable, "Failed to get peers for activity check", err
			}
			peers := peersResp.GetPeers()
			currentTime := time.Now().Unix()
			recentActivityThreshold := currentTime - 120 // 2 minutes in seconds
			hasRecentActivity := false
			for _, p := range peers {
				if p.GetLastRecv() >= recentActivityThreshold {
					hasRecentActivity = true
					break
				}
			}
			if !hasRecentActivity {
				return http.StatusServiceUnavailable, "No peer activity in the last 2 minutes", nil
			}
			return http.StatusOK, "Recent peer activity detected", nil
		},
	})

	return health.CheckAll(ctx, checkLiveness, checks)
}

// Init initializes the server by setting up wire limits, network listeners,
// and other required components. It returns an error if initialization fails.
//
// This method must be called after creating a new Server with the New function and before
// calling Start. It performs the following key operations:
// - Sets up message size limits for the wire protocol
// - Determines the server's public IP address for listening
// - Creates and configures the internal server implementation
// - Sets up initial peer connections from configuration
//
// Init does not start accepting connections or begin network operations. That happens when
// Start is called.
//
// Parameters:
//   - ctx: Context for cancellation and timeout control
//
// Returns an error if any part of initialization fails, particularly if required
// configuration settings are missing or network setup fails.
func (s *Server) Init(ctx context.Context) error {
	var err error

	wire.SetLimits(4000000000)

	// get the public IP and listen on it
	ip, err := GetOutboundIP()
	if err != nil {
		return err
	}

	defaultListenAddresses := []string{ip.String() + ":" + activeNetParams.DefaultPort}
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

// GetPeerCount returns the total number of connected peers.
//
// This method is part of the peer_api.PeerServiceServer gRPC interface and provides
// a simple count of currently connected peers to the legacy server.
//
// Parameters:
//   - ctx: Context for cancellation and timeout control
//   - _: Empty message parameter (required by gRPC interface)
//
// Returns:
//   - GetPeerCountResponse containing the peer count
//   - Error if the operation fails (nil on success)
func (s *Server) GetPeerCount(ctx context.Context, _ *emptypb.Empty) (*peer_api.GetPeerCountResponse, error) {
	pc := s.server.ConnectedCount()

	return &peer_api.GetPeerCountResponse{Count: pc}, nil
}

// GetPeers returns detailed information about all connected peers.
// It provides a comprehensive snapshot of the current peer connections, including
// connection details, protocol versions, network statistics, and peer status.
//
// This method is part of the peer_api.PeerServiceServer gRPC interface and is used
// by monitoring and control systems to observe the state of the peer network.
//
// The returned information includes:
// - Peer identification and addressing information
// - Connection statistics (bytes sent/received, timing)
// - Protocol and version information
// - Blockchain synchronization status
// - Ban score and whitelisting status
//
// Parameters:
//   - ctx: Context for cancellation and timeout control
//   - _: Empty message parameter (required by gRPC interface)
//
// Returns:
//   - GetPeersResponse containing detailed information about all peers
//   - Error if the operation fails, particularly if the server is not initialized
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

// IsBanned checks if a specific IP address or subnet is currently banned.
//
// This method is part of the peer_api.PeerServiceServer gRPC interface and provides
// a way to query the ban status of a specific network address.
//
// Parameters:
//   - ctx: Context for cancellation and timeout control
//   - peer: Request containing the IP address or subnet to check
//
// Returns:
//   - IsBannedResponse containing a boolean indicating ban status
//   - Error if the operation fails (nil on success)
func (s *Server) IsBanned(ctx context.Context, peer *peer_api.IsBannedRequest) (*peer_api.IsBannedResponse, error) {
	return &peer_api.IsBannedResponse{IsBanned: s.server.banList.IsBanned(peer.IpOrSubnet)}, nil
}

// ListBanned returns a list of all currently banned IP addresses and subnets.
//
// This method is part of the peer_api.PeerServiceServer gRPC interface and provides
// a complete view of all banned network addresses.
//
// Parameters:
//   - ctx: Context for cancellation and timeout control
//   - _: Empty message parameter (required by gRPC interface)
//
// Returns:
//   - ListBannedResponse containing a list of all banned addresses
//   - Error if the operation fails (nil on success)
func (s *Server) ListBanned(ctx context.Context, _ *emptypb.Empty) (*peer_api.ListBannedResponse, error) {
	return &peer_api.ListBannedResponse{Banned: s.server.banList.ListBanned()}, nil
}

// ClearBanned removes all entries from the ban list, allowing previously banned
// addresses to reconnect.
//
// This method is part of the peer_api.PeerServiceServer gRPC interface and provides
// a way to reset the ban list, typically used for administrative purposes.
//
// Parameters:
//   - ctx: Context for cancellation and timeout control
//   - _: Empty message parameter (required by gRPC interface)
//
// Returns:
//   - ClearBannedResponse with Ok=true to indicate success
//   - Error if the operation fails (nil on success)
func (s *Server) ClearBanned(ctx context.Context, _ *emptypb.Empty) (*peer_api.ClearBannedResponse, error) {
	s.server.banList.Clear()
	return &peer_api.ClearBannedResponse{Ok: true}, nil
}

// BanPeer bans a peer from connecting for a specified duration.
//
// This method is part of the peer_api.PeerServiceServer gRPC interface and provides
// a way to temporarily ban a peer based on its network address. The peer will be
// disconnected if currently connected and prevented from reconnecting until the
// ban expires.
//
// Parameters:
//   - ctx: Context for cancellation and timeout control
//   - peer: Request containing the peer address and ban duration
//   - Addr: The peer's network address (IP:Port)
//   - Until: Unix timestamp when the ban should expire (seconds since epoch)
//
// Returns:
//   - BanPeerResponse with Ok=true if the ban was successful, Ok=false otherwise
//   - Error if the ban operation fails, particularly if the peer address is invalid
func (s *Server) BanPeer(ctx context.Context, peer *peer_api.BanPeerRequest) (*peer_api.BanPeerResponse, error) {
	err := s.banPeer(peer.Addr, peer.Until)
	if err != nil {
		return &peer_api.BanPeerResponse{Ok: false}, err
	}

	return &peer_api.BanPeerResponse{Ok: true}, nil
}

// UnbanPeer removes a ban on a specific peer, allowing it to reconnect immediately.
//
// This method is part of the peer_api.PeerServiceServer gRPC interface and provides
// a way to lift an existing ban on a peer. The request is processed asynchronously
// through a channel to the internal server component.
//
// Parameters:
//   - ctx: Context for cancellation and timeout control
//   - peer: Request containing the peer address to unban
//   - Addr: The peer's network address (IP:Port) or subnet
//
// Returns:
//   - UnbanPeerResponse with Ok=true to indicate the request was received
//   - Error if the operation fails (nil on success)
//
// Note: The Ok=true response indicates only that the request was received and processed,
// not necessarily that the unban operation was successful. The actual unban operation
// happens asynchronously.
func (s *Server) UnbanPeer(ctx context.Context, peer *peer_api.UnbanPeerRequest) (*peer_api.UnbanPeerResponse, error) {
	// put the unban request on the unbanPeer channel
	s.server.unbanPeer <- unbanPeerReq{addr: bannedPeerAddr(peer.Addr)}
	s.logger.Infof("Unban requested for peer %s", peer.Addr)

	return &peer_api.UnbanPeerResponse{Ok: true}, nil // ok means the request was received and processed, but no necessarily successfully
}

// banPeer is an internal helper method to ban a peer by its address for a specified duration.
//
// This method attempts to find a connected peer with the provided address, and if found,
// sends a ban request to the server's ban channel. It handles the actual work of finding
// the peer and initiating the ban process.
//
// Parameters:
//   - peerAddr: The network address of the peer to ban (IP:Port)
//   - until: Unix timestamp indicating when the ban should expire (seconds since epoch)
//
// Returns an error if the peer couldn't be found by the provided address
func (s *Server) banPeer(peerAddr string, until int64) error {
	var foundPeer *serverPeer

	peers := s.server.getPeers()

	s.logger.Infof("Peer length: %d", len(peers))

	for _, sp := range peers {
		if sp.Addr() == peerAddr {
			foundPeer = sp
			break
		}
	}

	if foundPeer == nil {
		s.logger.Warnf("Attempted to ban legacy peer %s but peer not found", peerAddr)
		return errors.New(errors.ERR_INVALID_IP, "tried to ban legacy peer but peer not found")
	}
	s.server.banPeerForDuration <- &banPeerForDurationMsg{peer: foundPeer, until: until}
	s.logger.Infof("Banned legacypeer %s for %d seconds", peerAddr, until)

	return nil
}

// logPeerStats periodically logs statistics about connected peers.
// It runs as a separate goroutine and logs information about peer connections
// once per minute until the provided context is canceled.
//
// This method collects and logs detailed information about each connected peer,
// including connection durations, bytes transferred, and protocol state. It also
// logs the current FSM state of the blockchain service if available.
//
// The statistics are useful for monitoring peer connectivity, network health,
// and debugging communication issues between nodes.
//
// Parameters:
//   - ctx: Context that controls when logging should stop
func (s *Server) logPeerStats(ctx context.Context) {
	ctxLogger := s.logger.WithTraceContext(ctx)
	for {
		select {
		case <-ctx.Done():
			ctxLogger.Infof("[Legacy Server] Stopping peer statistics logging")
			return
		case <-time.After(time.Minute):
			peersResp, err := s.GetPeers(ctx, &emptypb.Empty{})
			if err != nil {
				ctxLogger.Errorf("[Legacy Server] Failed to get peers for stats: %v", err)
				continue
			}

			peers := peersResp.GetPeers()

			for _, p := range peers {
				lastSendElapsed := time.Since(time.Unix(p.GetLastSend(), 0))
				lastRecvElapsed := time.Since(time.Unix(p.GetLastRecv(), 0))
				ctxLogger.Infof("[Legacy Server] Peer %s (ID: %d) - Services: %s, Inbound: %t, Bytes Sent: %d, Bytes Received: %d, Ping: %dµs, Last Send: %v ago, Last Recv: %v ago, Height: %d, BanScore: %d",
					p.GetAddr(), p.GetId(), p.GetServices(), p.GetInbound(), p.GetBytesSent(), p.GetBytesReceived(), p.GetPingTime(), lastSendElapsed, lastRecvElapsed, p.GetCurrentHeight(), p.GetBanscore())
			}

			state, err := s.blockchainClient.GetFSMCurrentState(context.Background())
			if err != nil {
				ctxLogger.Debugf("Peer stats - Connected: %d, Current FSM State: unknown, error: %v", len(peers), err)
			} else {
				ctxLogger.Debugf("Peer stats - Connected: %d, Current FSM State: %v", len(peers), state)
			}
		}
	}
}

// Start begins the server operation, including starting the internal server
// and gRPC service. It returns an error if the server fails to start.
//
// This method initializes and starts all components of the legacy service:
// - Waits for the blockchain FSM to transition from IDLE state
// - Starts the internal peer server to handle P2P connections
// - Begins periodic peer statistics logging
// - Launches the gRPC service to handle API requests
//
// The method blocks until the gRPC server completes (either successfully running
// or encountering an error). The readyCh parameter is used to signal when the
// server is fully operational and ready to accept connections.
//
// Concurrency notes:
// - Start launches multiple goroutines for different server components
// - Each component runs independently but coordinates via the server state
// - The gRPC service runs in the current goroutine and blocks until completion
//
// Parameters:
//   - ctx: Context for cancellation and timeout control
//   - readyCh: Channel to signal when the server is ready to accept connections
//
// Returns an error if any component fails to start, particularly if the blockchain
// service isn't ready or the gRPC server fails to initialize
func (s *Server) Start(ctx context.Context, readyCh chan<- struct{}) error {
	var closeOnce sync.Once
	defer closeOnce.Do(func() { close(readyCh) })

	// Blocks until the FSM transitions from the IDLE state
	err := s.blockchainClient.WaitUntilFSMTransitionFromIdleState(ctx)
	if err != nil {
		s.logger.Errorf("[Legacy Server] Failed to wait for FSM transition from IDLE state: %s", err)

		return err
	}

	s.logger.Infof("[Legacy Server] Starting internal server...")
	go s.server.Start()
	s.logger.Infof("[Legacy Server] Internal server started on port %s", s.settings.Legacy.GRPCListenAddress)

	// Start periodic peer statistics logging
	go s.logPeerStats(ctx)
	s.logger.Infof("[Legacy Server] Started peer statistics logging")

	apiKey := s.settings.GRPCAdminAPIKey
	if apiKey == "" {
		// Generate a random API key if not provided
		key := make([]byte, 32)
		if _, err := rand.Read(key); err != nil {
			return errors.WrapGRPC(errors.NewServiceNotStartedError("[P2P] failed to generate API key", err))
		}

		apiKey = hex.EncodeToString(key)
		s.logger.Infof("[Legacy] Generated admin API key: %s", apiKey)
	}

	// Define protected methods - use the full gRPC method path
	protectedMethods := map[string]bool{
		"/peer_api.PeerService/BanPeer":   true,
		"/peer_api.PeerService/UnbanPeer": true,
	}

	// Create auth options
	authOptions := &util.AuthOptions{
		APIKey:           apiKey,
		ProtectedMethods: protectedMethods,
	}

	// this will block
	if err = util.StartGRPCServer(ctx, s.logger, s.settings, "legacy", s.settings.Legacy.GRPCListenAddress, func(server *grpc.Server) {
		peer_api.RegisterPeerServiceServer(server, s)
		closeOnce.Do(func() { close(readyCh) })
	}, authOptions); err != nil {
		return errors.WrapGRPC(errors.NewServiceNotStartedError("[Legacy] can't start GRPC server", err))
	}

	return nil
}

// Stop gracefully shuts down the server and its components.
// It returns an error if the shutdown process fails.
//
// This method performs a clean shutdown of all server components:
// - Closes all peer connections
// - Stops all network listeners
// - Shuts down the internal server state
// - Releases any allocated resources
//
// The method attempts to ensure that all connections are properly terminated
// and resources are released, but it may not wait indefinitely for operations
// to complete if they're taking too long.
//
// Parameters:
//   - _: Context parameter (unused in the current implementation)
//
// Returns an error if the shutdown process encounters problems, or nil on successful shutdown
func (s *Server) Stop(_ context.Context) error {
	return s.server.Stop()
}
