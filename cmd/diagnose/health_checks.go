package diagnose

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/services/blockassembly"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/services/blockvalidation"
	"github.com/bsv-blockchain/teranode/services/p2p"
	"github.com/bsv-blockchain/teranode/services/subtreevalidation"
	"github.com/bsv-blockchain/teranode/services/validator"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util"
	"github.com/bsv-blockchain/teranode/util/kafka"
	"github.com/ordishs/gocore"
)

// Clients that are reused across multiple checks are stored here
// to avoid creating duplicate connections.
type serviceClients struct {
	blockchain      blockchain.ClientI
	blockValidation blockvalidation.Interface
	blockAssembly   blockassembly.ClientI
	p2p             p2p.ClientI
}

func runHealthChecks(ctx context.Context, logger ulogger.Logger, s *settings.Settings) []HealthResult {
	var results []HealthResult

	// Create reusable clients
	clients := createClients(ctx, logger, s)

	// gRPC services
	results = append(results, checkGRPCServices(ctx, logger, s, clients)...)

	// HTTP services
	results = append(results, checkHTTPServices(ctx, s)...)

	// Infrastructure
	results = append(results, checkKafka(ctx, s))
	results = append(results, checkAerospike(ctx, logger, s))
	results = append(results, checkPostgres(ctx, s))

	// Contextual checks (reuse clients)
	results = append(results, checkBlockchainState(ctx, s, clients)...)

	return results
}

func createClients(ctx context.Context, logger ulogger.Logger, s *settings.Settings) *serviceClients {
	c := &serviceClients{}

	if s.BlockChain.GRPCAddress != "" {
		c.blockchain, _ = blockchain.NewClient(ctx, logger, s, "diagnose")
	}

	if s.BlockValidation.GRPCAddress != "" {
		c.blockValidation, _ = blockvalidation.NewClient(ctx, logger, s, "diagnose")
	}

	if s.BlockAssembly.GRPCAddress != "" {
		c.blockAssembly, _ = blockassembly.NewClient(ctx, logger, s)
	}

	if s.P2P.GRPCAddress != "" {
		c.p2p, _ = p2p.NewClient(ctx, logger, s)
	}

	return c
}

func checkGRPCServices(ctx context.Context, logger ulogger.Logger, s *settings.Settings, clients *serviceClients) []HealthResult {
	type grpcService struct {
		name    string
		address string
		skipMsg string // reason for skipping (empty = use check)
		check   func() (int, string, error)
	}

	var services []grpcService

	// Blockchain
	if s.BlockChain.GRPCAddress != "" {
		if clients.blockchain != nil {
			client := clients.blockchain
			services = append(services, grpcService{
				name:    "Blockchain gRPC",
				address: s.BlockChain.GRPCAddress,
				check:   func() (int, string, error) { return client.Health(ctx, true) },
			})
		} else {
			services = append(services, grpcService{
				name:    "Blockchain gRPC",
				address: s.BlockChain.GRPCAddress,
				check: func() (int, string, error) {
					return http.StatusServiceUnavailable, "", errors.New(errors.ERR_SERVICE_UNAVAILABLE, "failed to create client")
				},
			})
		}
	} else {
		services = append(services, grpcService{name: "Blockchain gRPC", skipMsg: skipReason("Blockchain")})
	}

	// Validator
	if s.Validator.GRPCAddress != "" {
		client, err := validator.NewClient(ctx, logger, s)
		if err != nil {
			services = append(services, grpcService{
				name:    "Validator gRPC",
				address: s.Validator.GRPCAddress,
				check:   func() (int, string, error) { return http.StatusServiceUnavailable, "", err },
			})
		} else {
			services = append(services, grpcService{
				name:    "Validator gRPC",
				address: s.Validator.GRPCAddress,
				check:   func() (int, string, error) { return client.Health(ctx, true) },
			})
		}
	} else {
		services = append(services, grpcService{name: "Validator gRPC", skipMsg: skipReason("Validator")})
	}

	// Block Validation
	if s.BlockValidation.GRPCAddress != "" {
		if clients.blockValidation != nil {
			client := clients.blockValidation
			services = append(services, grpcService{
				name:    "Block Validation gRPC",
				address: s.BlockValidation.GRPCAddress,
				check:   func() (int, string, error) { return client.Health(ctx, true) },
			})
		} else {
			services = append(services, grpcService{
				name:    "Block Validation gRPC",
				address: s.BlockValidation.GRPCAddress,
				check: func() (int, string, error) {
					return http.StatusServiceUnavailable, "", errors.New(errors.ERR_SERVICE_UNAVAILABLE, "failed to create client")
				},
			})
		}
	} else {
		services = append(services, grpcService{name: "Block Validation gRPC", skipMsg: skipReason("BlockValidation")})
	}

	// Block Assembly
	if s.BlockAssembly.GRPCAddress != "" {
		if clients.blockAssembly != nil {
			client := clients.blockAssembly
			services = append(services, grpcService{
				name:    "Block Assembly gRPC",
				address: s.BlockAssembly.GRPCAddress,
				check:   func() (int, string, error) { return client.Health(ctx, true) },
			})
		} else {
			services = append(services, grpcService{
				name:    "Block Assembly gRPC",
				address: s.BlockAssembly.GRPCAddress,
				check: func() (int, string, error) {
					return http.StatusServiceUnavailable, "", errors.New(errors.ERR_SERVICE_UNAVAILABLE, "failed to create client")
				},
			})
		}
	} else {
		services = append(services, grpcService{name: "Block Assembly gRPC", skipMsg: skipReason("BlockAssembly")})
	}

	// Subtree Validation
	if s.SubtreeValidation.GRPCAddress != "" {
		client, err := subtreevalidation.NewClient(ctx, logger, s, "diagnose")
		if err != nil {
			services = append(services, grpcService{
				name:    "Subtree Validation gRPC",
				address: s.SubtreeValidation.GRPCAddress,
				check:   func() (int, string, error) { return http.StatusServiceUnavailable, "", err },
			})
		} else {
			services = append(services, grpcService{
				name:    "Subtree Validation gRPC",
				address: s.SubtreeValidation.GRPCAddress,
				check:   func() (int, string, error) { return client.Health(ctx, true) },
			})
		}
	} else {
		services = append(services, grpcService{name: "Subtree Validation gRPC", skipMsg: skipReason("SubtreeValidation")})
	}

	// P2P (uses GetPeers as health indicator - no Health method on ClientI)
	if s.P2P.GRPCAddress != "" {
		if clients.p2p != nil {
			client := clients.p2p
			services = append(services, grpcService{
				name:    "P2P gRPC",
				address: s.P2P.GRPCAddress,
				check: func() (int, string, error) {
					_, err := client.GetPeers(ctx)
					if err != nil {
						return http.StatusServiceUnavailable, "", err
					}
					return http.StatusOK, "", nil
				},
			})
		} else {
			services = append(services, grpcService{
				name:    "P2P gRPC",
				address: s.P2P.GRPCAddress,
				check: func() (int, string, error) {
					return http.StatusServiceUnavailable, "", errors.New(errors.ERR_SERVICE_UNAVAILABLE, "failed to create client")
				},
			})
		}
	} else {
		services = append(services, grpcService{name: "P2P gRPC", skipMsg: skipReason("P2P")})
	}

	var results []HealthResult

	for _, svc := range services {
		if svc.check == nil {
			msg := svc.skipMsg
			if msg == "" {
				msg = "not configured"
			}

			results = append(results, HealthResult{
				Service: svc.name,
				Address: "-",
				Status:  StatusSKIP,
				Message: msg,
			})

			continue
		}

		start := time.Now()
		statusCode, _, err := svc.check()
		latency := time.Since(start)

		r := HealthResult{
			Service: svc.name,
			Address: svc.address,
			Latency: formatLatency(latency),
		}

		if err != nil {
			r.Status = StatusFAIL
			r.Error = err.Error()
		} else if !isHealthy(statusCode) {
			r.Status = StatusFAIL
		} else {
			r.Status = StatusOK
		}

		results = append(results, r)
	}

	return results
}

func checkHTTPServices(ctx context.Context, s *settings.Settings) []HealthResult {
	var results []HealthResult

	// Asset Server
	if s.Asset.HTTPListenAddress != "" {
		addr := normalizeHTTPAddress(s.Asset.HTTPListenAddress)
		results = append(results, checkHTTPEndpoint(ctx, "Asset HTTP", addr, "/health"))
	} else {
		results = append(results, HealthResult{
			Service: "Asset HTTP",
			Address: "-",
			Status:  StatusSKIP,
			Message: skipReason("Asset"),
		})
	}

	// Propagation HTTP
	if s.Propagation.HTTPListenAddress != "" {
		addr := normalizeHTTPAddress(s.Propagation.HTTPListenAddress)
		results = append(results, checkHTTPEndpoint(ctx, "Propagation HTTP", addr, "/health"))
	}

	// Block Persister HTTP
	if s.BlockPersister.HTTPListenAddress != "" {
		addr := normalizeHTTPAddress(s.BlockPersister.HTTPListenAddress)
		results = append(results, checkHTTPEndpoint(ctx, "Block Persister HTTP", addr, "/health"))
	}

	// RPC
	if s.RPC.RPCListenerURL != nil {
		results = append(results, checkHTTPEndpoint(ctx, "RPC", s.RPC.RPCListenerURL.String(), "/"))
	}

	// Health Check endpoint
	if s.HealthCheckHTTPListenAddress != "" {
		addr := normalizeHTTPAddress(s.HealthCheckHTTPListenAddress)
		results = append(results, checkHTTPEndpoint(ctx, "Health Endpoint", addr, "/health"))
	}

	// Profiler (pprof)
	if s.ProfilerAddr != "" {
		addr := normalizeHTTPAddress(s.ProfilerAddr)
		results = append(results, checkHTTPEndpoint(ctx, "Profiler (pprof)", addr, "/debug/pprof/"))
	}

	return results
}

func checkHTTPEndpoint(ctx context.Context, name, address, path string) HealthResult {
	client := &http.Client{Timeout: 2 * time.Second}
	u := fmt.Sprintf("%s%s", address, path)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return HealthResult{
			Service: name,
			Address: address,
			Status:  StatusFAIL,
			Error:   err.Error(),
		}
	}

	start := time.Now()
	resp, err := client.Do(req)
	latency := time.Since(start)

	if err != nil {
		return HealthResult{
			Service: name,
			Address: address,
			Status:  StatusFAIL,
			Latency: formatLatency(latency),
			Error:   err.Error(),
		}
	}
	defer resp.Body.Close()

	r := HealthResult{
		Service: name,
		Address: address,
		Latency: formatLatency(latency),
	}

	if resp.StatusCode >= 200 && resp.StatusCode < 400 {
		r.Status = StatusOK
	} else if resp.StatusCode == http.StatusUnauthorized || resp.StatusCode == http.StatusForbidden {
		// Auth-protected endpoints are still reachable
		r.Status = StatusOK
		r.Message = fmt.Sprintf("HTTP %d (auth required)", resp.StatusCode)
	} else {
		r.Status = StatusFAIL
		r.Error = fmt.Sprintf("HTTP %d", resp.StatusCode)
	}

	return r
}

func checkKafka(ctx context.Context, s *settings.Settings) HealthResult {
	hosts := s.Kafka.Hosts
	if hosts == "" {
		return HealthResult{
			Service: "Kafka",
			Address: "-",
			Status:  StatusSKIP,
			Message: "not configured",
		}
	}

	brokers := strings.Split(hosts, ",")
	checker := kafka.HealthChecker(ctx, brokers)

	start := time.Now()
	statusCode, _, err := checker(ctx, true)
	latency := time.Since(start)

	r := HealthResult{
		Service: "Kafka",
		Address: hosts,
		Latency: formatLatency(latency),
	}

	if err != nil {
		r.Status = StatusFAIL
		r.Error = err.Error()
	} else if statusCode != http.StatusOK {
		r.Status = StatusFAIL
	} else {
		r.Status = StatusOK
	}

	return r
}

func checkAerospike(ctx context.Context, logger ulogger.Logger, s *settings.Settings) HealthResult {
	if s.Aerospike.Host == "" || s.Aerospike.Port == 0 {
		return HealthResult{
			Service: "Aerospike",
			Address: "-",
			Status:  StatusSKIP,
			Message: "not configured",
		}
	}

	address := fmt.Sprintf("%s:%d", s.Aerospike.Host, s.Aerospike.Port)

	aerospikeURL := &url.URL{
		Scheme: "aerospike",
		Host:   address,
		Path:   "/teranode",
	}

	start := time.Now()
	client, err := util.GetAerospikeClient(logger, aerospikeURL, s)
	latency := time.Since(start)

	if err != nil {
		return HealthResult{
			Service: "Aerospike",
			Address: address,
			Status:  StatusFAIL,
			Latency: formatLatency(latency),
			Error:   err.Error(),
		}
	}

	if !client.IsConnected() {
		return HealthResult{
			Service: "Aerospike",
			Address: address,
			Status:  StatusFAIL,
			Latency: formatLatency(latency),
			Error:   "not connected",
		}
	}

	nodes := client.GetNodes()

	return HealthResult{
		Service: "Aerospike",
		Address: address,
		Status:  StatusOK,
		Latency: formatLatency(latency),
		Message: fmt.Sprintf("%d node(s)", len(nodes)),
	}
}

func checkPostgres(ctx context.Context, s *settings.Settings) HealthResult {
	address := s.PostgresCheckAddress
	if address == "" {
		return HealthResult{
			Service: "PostgreSQL",
			Address: "-",
			Status:  StatusSKIP,
			Message: "not configured",
		}
	}

	start := time.Now()
	conn, err := net.DialTimeout("tcp", address, 2*time.Second)
	latency := time.Since(start)

	if err != nil {
		return HealthResult{
			Service: "PostgreSQL",
			Address: address,
			Status:  StatusFAIL,
			Latency: formatLatency(latency),
			Error:   err.Error(),
		}
	}

	_ = conn.Close()

	return HealthResult{
		Service: "PostgreSQL",
		Address: address,
		Status:  StatusOK,
		Latency: formatLatency(latency),
	}
}

func checkBlockchainState(ctx context.Context, s *settings.Settings, clients *serviceClients) []HealthResult {
	var results []HealthResult

	if clients.blockchain == nil {
		return results
	}

	// FSM State (captured for use in chain tip freshness check)
	var fsmState *blockchain.FSMStateType

	fsmResult, err := clients.blockchain.GetFSMCurrentState(ctx)
	if err != nil {
		results = append(results, HealthResult{
			Service: "FSM State",
			Address: "-",
			Status:  StatusFAIL,
			Error:   err.Error(),
		})
	} else {
		fsmState = fsmResult
		results = append(results, HealthResult{
			Service: "FSM State",
			Address: "-",
			Status:  StatusOK,
			Message: fsmState.String(),
		})
	}

	// Block height and chain tip freshness
	stats, err := clients.blockchain.GetBlockStats(ctx)
	if err == nil && stats != nil {
		msg := fmt.Sprintf("height=%d, blocks=%d", stats.MaxHeight, stats.BlockCount)
		status := StatusOK

		if stats.LastBlockTime > 0 {
			tipTime := time.Unix(int64(stats.LastBlockTime), 0)
			tipAge := time.Since(tipTime)

			// Only check freshness if timestamp is recent (not regtest genesis-era)
			if tipTime.Year() >= 2020 {
				msg += fmt.Sprintf(", tip_age=%s", formatDuration(tipAge))

				// Only flag as stale if FSM is RUNNING - during catchup,
				// legacy sync, or idle the tip is expected to be old
				if tipAge > 30*time.Minute && fsmState != nil && *fsmState == blockchain.FSMStateRUNNING {
					status = StatusFAIL
					msg += fmt.Sprintf(" (stale - fsm=%s, node may be stuck)", fsmState.String())
				}
			} else {
				msg += fmt.Sprintf(", last_block=%s", tipTime.Format("2006-01-02"))
			}
		}

		if fsmState != nil && *fsmState != blockchain.FSMStateRUNNING {
			msg += fmt.Sprintf(", fsm=%s", fsmState.String())
		}

		results = append(results, HealthResult{
			Service: "Chain Tip",
			Address: "-",
			Status:  status,
			Message: msg,
		})
	}

	// Catchup status
	if clients.blockValidation != nil {
		catchup, err := clients.blockValidation.GetCatchupStatus(ctx)
		if err == nil && catchup != nil {
			if catchup.IsCatchingUp {
				progress := ""
				if catchup.TotalBlocks > 0 {
					pct := float64(catchup.BlocksValidated) / float64(catchup.TotalBlocks) * 100
					progress = fmt.Sprintf("%.1f%% (%d/%d blocks)", pct, catchup.BlocksValidated, catchup.TotalBlocks)
				} else {
					progress = fmt.Sprintf("%d blocks validated", catchup.BlocksValidated)
				}

				results = append(results, HealthResult{
					Service: "Catchup",
					Address: "-",
					Status:  StatusOK,
					Message: fmt.Sprintf("syncing from %s, %s", truncate(catchup.PeerID, 16), progress),
				})
			} else {
				results = append(results, HealthResult{
					Service: "Catchup",
					Address: "-",
					Status:  StatusOK,
					Message: "not catching up",
				})
			}
		}
	}

	// Block Assembly state
	if clients.blockAssembly != nil {
		baState, err := clients.blockAssembly.GetBlockAssemblyState(ctx)
		if err != nil {
			results = append(results, HealthResult{
				Service: "Block Assembly State",
				Address: "-",
				Status:  StatusFAIL,
				Error:   err.Error(),
			})
		} else {
			msg := fmt.Sprintf("state=%s, txs=%d, queue=%d, subtrees=%d, height=%d",
				baState.BlockAssemblyState, baState.TxCount, baState.QueueCount,
				baState.SubtreeCount, baState.CurrentHeight)

			results = append(results, HealthResult{
				Service: "Block Assembly State",
				Address: "-",
				Status:  StatusOK,
				Message: msg,
			})
		}

		// Mining candidate
		candidate, err := clients.blockAssembly.GetMiningCandidate(ctx)
		if err != nil {
			results = append(results, HealthResult{
				Service: "Mining Candidate",
				Address: "-",
				Status:  StatusFAIL,
				Error:   err.Error(),
			})
		} else if candidate != nil {
			size := formatBytesUint64(candidate.SizeWithoutCoinbase)
			results = append(results, HealthResult{
				Service: "Mining Candidate",
				Address: "-",
				Status:  StatusOK,
				Message: fmt.Sprintf("height=%d, txs=%d, size=%s", candidate.Height, candidate.NumTxs, size),
			})
		}
	}

	// P2P Peers
	if clients.p2p != nil {
		peers, err := clients.p2p.GetPeers(ctx)
		if err != nil {
			results = append(results, HealthResult{
				Service: "P2P Peers",
				Address: "-",
				Status:  StatusFAIL,
				Error:   err.Error(),
			})
		} else {
			connected := 0
			var maxHeight uint32

			for _, peer := range peers {
				if peer.IsConnected {
					connected++
				}

				if peer.Height > maxHeight {
					maxHeight = peer.Height
				}
			}

			r := HealthResult{
				Service: "P2P Peers",
				Address: "-",
				Status:  StatusOK,
				Message: fmt.Sprintf("%d connected, %d total, max_peer_height=%d", connected, len(peers), maxHeight),
			}

			if connected == 0 {
				r.Status = StatusFAIL
				r.Message = "no peers connected"
				r.Error = "node may be isolated or still starting up"
			}

			results = append(results, r)
		}

		// Banned peers
		banned, err := clients.p2p.ListBanned(ctx)
		if err == nil && len(banned) > 0 {
			results = append(results, HealthResult{
				Service: "Banned Peers",
				Address: "-",
				Status:  StatusOK,
				Message: fmt.Sprintf("%d peer(s) banned", len(banned)),
			})
		}
	}

	// Cross-service consistency checks
	results = append(results, checkServiceConsistency(stats, fsmState, clients, ctx)...)

	return results
}

func checkServiceConsistency(stats *model.BlockStats, fsmState *blockchain.FSMStateType, clients *serviceClients, ctx context.Context) []HealthResult {
	var results []HealthResult

	if stats == nil {
		return results
	}

	isRunning := fsmState != nil && *fsmState == blockchain.FSMStateRUNNING
	chainHeight := stats.MaxHeight

	// Block Assembly height vs blockchain height
	// During catchup or legacy sync, BA is expected to lag behind.
	if clients.blockAssembly != nil {
		baState, err := clients.blockAssembly.GetBlockAssemblyState(ctx)
		if err == nil && baState != nil {
			drift := int64(chainHeight) - int64(baState.CurrentHeight)
			if drift < 0 {
				drift = -drift
			}

			msg := fmt.Sprintf("ba_height=%d, chain_height=%d", baState.CurrentHeight, chainHeight)
			status := StatusOK

			if drift > 2 && isRunning {
				status = StatusFAIL
				msg += fmt.Sprintf(" (drift=%d, out of sync)", drift)
			} else if drift > 2 {
				msg += fmt.Sprintf(" (drift=%d, expected during %s)", drift, fsmState.String())
			}

			results = append(results, HealthResult{
				Service: "BA/Chain Sync",
				Address: "-",
				Status:  status,
				Message: msg,
			})
		}
	}

	// Peer height vs our height - are we behind the network?
	// During catchup this is expected, only FAIL when running.
	if clients.p2p != nil {
		peers, err := clients.p2p.GetPeers(ctx)
		if err == nil {
			var maxPeerHeight uint32

			for _, peer := range peers {
				if peer.Height > maxPeerHeight {
					maxPeerHeight = peer.Height
				}
			}

			if maxPeerHeight > 0 {
				behind := int64(maxPeerHeight) - int64(chainHeight)
				msg := fmt.Sprintf("our_height=%d, best_peer=%d", chainHeight, maxPeerHeight)
				status := StatusOK

				if behind > 10 && isRunning {
					status = StatusFAIL
					msg += fmt.Sprintf(", behind=%d blocks", behind)
				} else if behind > 10 {
					msg += fmt.Sprintf(", behind=%d blocks (expected during %s)", behind, fsmState.String())
				} else if behind > 0 {
					msg += fmt.Sprintf(", behind=%d blocks", behind)
				} else {
					msg += ", in sync"
				}

				results = append(results, HealthResult{
					Service: "Chain vs Peers",
					Address: "-",
					Status:  status,
					Message: msg,
				})
			}
		}
	}

	return results
}

// isServiceEnabled checks the gocore config for the start flag of a service.
// The config key is "start" + formalName (e.g. "startBlockchain", "startP2P").
// This respects SETTINGS_CONTEXT for environment-specific overrides.
func isServiceEnabled(formalName string) bool {
	return gocore.Config().GetBool("start" + formalName)
}

// skipReason returns a SKIP message explaining why a service is not checked.
// If the service is disabled in settings, it says so. Otherwise "not configured".
func skipReason(formalName string) string {
	if !isServiceEnabled(formalName) {
		return fmt.Sprintf("disabled (start%s=false)", formalName)
	}

	return "not configured"
}

func normalizeHTTPAddress(addr string) string {
	if strings.HasPrefix(addr, "http://") || strings.HasPrefix(addr, "https://") {
		return addr
	}

	// ":8090" -> "http://localhost:8090"
	// "localhost:8090" -> "http://localhost:8090"
	// "0.0.0.0:8090" -> "http://localhost:8090" (can't connect to 0.0.0.0)
	if strings.HasPrefix(addr, ":") {
		return "http://localhost" + addr
	}

	if strings.HasPrefix(addr, "0.0.0.0:") {
		return "http://localhost:" + addr[len("0.0.0.0:"):]
	}

	return "http://" + addr
}

func truncate(s string, maxLen int) string {
	if len(s) <= maxLen {
		return s
	}

	return s[:maxLen] + "..."
}

func formatBytesUint64(n uint64) string {
	const (
		kb = 1024
		mb = 1024 * kb
		gb = 1024 * mb
	)

	switch {
	case n >= gb:
		return fmt.Sprintf("%.1f GB", float64(n)/float64(gb))
	case n >= mb:
		return fmt.Sprintf("%.1f MB", float64(n)/float64(mb))
	case n >= kb:
		return fmt.Sprintf("%.1f KB", float64(n)/float64(kb))
	default:
		return fmt.Sprintf("%d B", n)
	}
}

func formatDuration(d time.Duration) string {
	if d < time.Minute {
		return fmt.Sprintf("%ds", int(d.Seconds()))
	}

	if d < time.Hour {
		return fmt.Sprintf("%dm", int(d.Minutes()))
	}

	return fmt.Sprintf("%dh%dm", int(d.Hours()), int(d.Minutes())%60)
}
