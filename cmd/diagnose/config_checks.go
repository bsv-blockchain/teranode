package diagnose

import (
	"fmt"
	"os"
	"strings"

	"github.com/bsv-blockchain/teranode/settings"
)

func runConfigChecks(s *settings.Settings) []ConfigResult {
	var results []ConfigResult

	results = append(results, checkContext(s))
	results = append(results, checkNetwork(s))
	results = append(results, checkPortConflicts(s)...)
	results = append(results, checkSecurity(s)...)
	results = append(results, checkKafkaConfig(s)...)
	results = append(results, checkRPCConfig(s)...)
	results = append(results, checkP2PConfig(s)...)
	results = append(results, checkRetention(s)...)
	results = append(results, checkDatabase(s)...)
	results = append(results, checkServices(s)...)
	results = append(results, checkPolicy(s)...)
	results = append(results, checkObservability(s)...)
	results = append(results, checkDataFolder(s)...)

	return results
}

func checkContext(s *settings.Settings) ConfigResult {
	ctx := s.Context
	if ctx == "" {
		ctx = "(default)"
	}

	return ConfigResult{
		Severity: SeverityINFO,
		Check:    "Configuration context",
		Value:    ctx,
	}
}

func checkNetwork(s *settings.Settings) ConfigResult {
	network := "unknown"
	if s.ChainCfgParams != nil {
		network = s.ChainCfgParams.Name
	}

	return ConfigResult{
		Severity: SeverityINFO,
		Check:    "Network",
		Value:    network,
	}
}

func checkPortConflicts(s *settings.Settings) []ConfigResult {
	type portEntry struct {
		service string
		address string
	}

	addresses := []portEntry{
		{"Blockchain gRPC", s.BlockChain.GRPCListenAddress},
		{"Blockchain HTTP", s.BlockChain.HTTPListenAddress},
		{"Block Assembly gRPC", s.BlockAssembly.GRPCListenAddress},
		{"Block Validation gRPC", s.BlockValidation.GRPCListenAddress},
		{"Subtree Validation gRPC", s.SubtreeValidation.GRPCListenAddress},
		{"Validator gRPC", s.Validator.GRPCListenAddress},
		{"Validator HTTP", s.Validator.HTTPListenAddress},
		{"P2P gRPC", s.P2P.GRPCListenAddress},
		{"P2P HTTP", s.P2P.HTTPListenAddress},
		{"Propagation gRPC", s.Propagation.GRPCListenAddress},
		{"Propagation HTTP", s.Propagation.HTTPListenAddress},
		{"Asset HTTP", s.Asset.HTTPListenAddress},
		{"Asset Centrifuge", s.Asset.CentrifugeListenAddress},
		{"Block Persister HTTP", s.BlockPersister.HTTPListenAddress},
		{"Faucet HTTP", s.Faucet.HTTPListenAddress},
		{"Health Check HTTP", s.HealthCheckHTTPListenAddress},
	}

	// Add RPC listener URL if configured
	if s.RPC.RPCListenerURL != nil {
		addresses = append(addresses, portEntry{"RPC", ":" + s.RPC.RPCListenerURL.Port()})
	}

	// normalizedAddr groups by "ip:port" to detect true conflicts.
	// ":8080" and "0.0.0.0:8080" both bind all interfaces, so they conflict.
	// "127.0.0.1:8080" and "10.0.0.1:8080" bind different interfaces, so they don't.
	type bindEntry struct {
		ip      string // "" or "0.0.0.0" means all interfaces
		port    string
		service string
	}

	var binds []bindEntry

	for _, entry := range addresses {
		if entry.address == "" {
			continue
		}

		ip, port := parseBindAddress(entry.address)
		if port == "" {
			continue
		}

		binds = append(binds, bindEntry{ip: ip, port: port, service: entry.service})
	}

	var results []ConfigResult

	// Check each pair for conflicts
	seen := make(map[string]bool)

	for i := 0; i < len(binds); i++ {
		for j := i + 1; j < len(binds); j++ {
			if binds[i].port != binds[j].port {
				continue
			}

			// Same port - check if IPs conflict
			if !ipsConflict(binds[i].ip, binds[j].ip) {
				continue
			}

			key := binds[i].service + "/" + binds[j].service
			if seen[key] {
				continue
			}

			seen[key] = true

			results = append(results, ConfigResult{
				Severity:    SeverityERROR,
				Check:       "Port conflict",
				Value:       fmt.Sprintf("port %s: %s and %s", binds[i].port, binds[i].service, binds[j].service),
				Recommended: "Each service should use a unique port",
			})
		}
	}

	if len(results) == 0 {
		results = append(results, ConfigResult{
			Severity: SeverityOK,
			Check:    "Port conflicts",
			Value:    fmt.Sprintf("none (%d listen addresses checked)", len(binds)),
		})
	}

	return results
}

// parseBindAddress extracts the IP and port from a listen address.
// Handles formats: ":8080", "0.0.0.0:8080", "127.0.0.1:8080", "localhost:8080"
func parseBindAddress(address string) (ip string, port string) {
	// Strip any URL scheme
	if idx := strings.Index(address, "://"); idx >= 0 {
		address = address[idx+3:]
	}

	// Handle bare :port
	if strings.HasPrefix(address, ":") {
		return "", address[1:]
	}

	// Handle host:port
	idx := strings.LastIndex(address, ":")
	if idx < 0 {
		return address, ""
	}

	return address[:idx], address[idx+1:]
}

// ipsConflict returns true if two bind IPs would conflict on the same port.
// An empty IP or "0.0.0.0" means "all interfaces" and conflicts with everything.
func ipsConflict(a, b string) bool {
	aAll := isAllInterfaces(a)
	bAll := isAllInterfaces(b)

	// If either binds all interfaces, they conflict
	if aAll || bAll {
		return true
	}

	// Both bind specific interfaces - conflict only if same IP
	return strings.EqualFold(a, b)
}

func isAllInterfaces(ip string) bool {
	return ip == "" || ip == "0.0.0.0" || ip == "::"
}

func checkSecurity(s *settings.Settings) []ConfigResult {
	var results []ConfigResult
	severity := warnIfProd(s)

	if s.SecurityLevelGRPC == 0 {
		results = append(results, ConfigResult{
			Severity:    severity,
			Check:       "gRPC TLS",
			Value:       "disabled (level 0)",
			Recommended: "Set securityLevelGRPC >= 1 for production",
		})
	} else {
		results = append(results, ConfigResult{
			Severity: SeverityOK,
			Check:    "gRPC TLS",
			Value:    fmt.Sprintf("level %d", s.SecurityLevelGRPC),
		})
	}

	if s.SecurityLevelHTTP == 0 {
		results = append(results, ConfigResult{
			Severity:    severity,
			Check:       "HTTP TLS",
			Value:       "disabled (level 0)",
			Recommended: "Set securityLevelHTTP >= 1 for production",
		})
	} else {
		results = append(results, ConfigResult{
			Severity: SeverityOK,
			Check:    "HTTP TLS",
			Value:    fmt.Sprintf("level %d", s.SecurityLevelHTTP),
		})
	}

	// TLS cert/key files - if TLS enabled but no certs, service will crash
	if s.SecurityLevelHTTP > 0 && (s.ServerCertFile == "" || s.ServerKeyFile == "") {
		results = append(results, ConfigResult{
			Severity:    SeverityERROR,
			Check:       "TLS certificate files",
			Value:       fmt.Sprintf("cert=%q, key=%q", s.ServerCertFile, s.ServerKeyFile),
			Recommended: "Set server_certFile and server_keyFile when TLS is enabled",
		})
	}

	if s.GRPCAdminAPIKey == "" {
		results = append(results, ConfigResult{
			Severity:    severity,
			Check:       "gRPC admin API key",
			Value:       "(empty)",
			Recommended: "Set grpc_admin_api_key (32+ chars)",
		})
	} else if len(s.GRPCAdminAPIKey) < 16 {
		results = append(results, ConfigResult{
			Severity:    SeverityWARN,
			Check:       "gRPC admin API key",
			Value:       fmt.Sprintf("%d chars (weak)", len(s.GRPCAdminAPIKey)),
			Recommended: "Use at least 16 characters, ideally 32+",
		})
	} else {
		results = append(results, ConfigResult{
			Severity: SeverityOK,
			Check:    "gRPC admin API key",
			Value:    fmt.Sprintf("%d chars", len(s.GRPCAdminAPIKey)),
		})
	}

	return results
}

func checkKafkaConfig(s *settings.Settings) []ConfigResult {
	var results []ConfigResult

	if s.Kafka.Hosts == "" {
		results = append(results, ConfigResult{
			Severity:    SeverityERROR,
			Check:       "Kafka hosts",
			Value:       "(empty)",
			Recommended: "Set KAFKA_HOSTS (e.g. localhost:9092)",
		})
	} else {
		results = append(results, ConfigResult{
			Severity: SeverityOK,
			Check:    "Kafka hosts",
			Value:    s.Kafka.Hosts,
		})
	}

	severity := warnIfProd(s)

	if s.Kafka.ReplicationFactor <= 1 {
		results = append(results, ConfigResult{
			Severity:    severity,
			Check:       "Kafka replication factor",
			Value:       fmt.Sprintf("%d", s.Kafka.ReplicationFactor),
			Recommended: "3+ for production (data loss risk with single replica)",
		})
	} else {
		results = append(results, ConfigResult{
			Severity: SeverityOK,
			Check:    "Kafka replication factor",
			Value:    fmt.Sprintf("%d", s.Kafka.ReplicationFactor),
		})
	}

	if s.Kafka.EnableTLS {
		results = append(results, ConfigResult{
			Severity: SeverityOK,
			Check:    "Kafka TLS",
			Value:    "enabled",
		})

		if s.Kafka.TLSCAFile == "" && s.Kafka.TLSCertFile == "" {
			results = append(results, ConfigResult{
				Severity:    SeverityWARN,
				Check:       "Kafka TLS certificates",
				Value:       "(no cert files configured)",
				Recommended: "Set KAFKA_TLS_CA_FILE and KAFKA_TLS_CERT_FILE",
			})
		}

		if s.Kafka.TLSSkipVerify {
			results = append(results, ConfigResult{
				Severity:    SeverityWARN,
				Check:       "Kafka TLS skip verify",
				Value:       "true",
				Recommended: "Disable in production (MITM risk)",
			})
		}
	} else {
		results = append(results, ConfigResult{
			Severity:    severity,
			Check:       "Kafka TLS",
			Value:       "disabled",
			Recommended: "Enable KAFKA_ENABLE_TLS for production",
		})
	}

	return results
}

func checkRPCConfig(s *settings.Settings) []ConfigResult {
	var results []ConfigResult

	if s.RPC.RPCListenerURL == nil {
		results = append(results, ConfigResult{
			Severity: SeverityINFO,
			Check:    "RPC service",
			Value:    "not configured",
		})

		return results
	}

	results = append(results, ConfigResult{
		Severity: SeverityINFO,
		Check:    "RPC listener",
		Value:    s.RPC.RPCListenerURL.String(),
	})

	if s.RPC.RPCMaxClients <= 1 {
		results = append(results, ConfigResult{
			Severity:    SeverityWARN,
			Check:       "RPC max clients",
			Value:       fmt.Sprintf("%d", s.RPC.RPCMaxClients),
			Recommended: "10-100+ for production (default 1 is very restrictive)",
		})
	} else {
		results = append(results, ConfigResult{
			Severity: SeverityOK,
			Check:    "RPC max clients",
			Value:    fmt.Sprintf("%d", s.RPC.RPCMaxClients),
		})
	}

	if s.RPC.RPCUser == "" && s.RPC.RPCPass == "" {
		severity := warnIfProd(s)
		results = append(results, ConfigResult{
			Severity:    severity,
			Check:       "RPC authentication",
			Value:       "disabled (no user/pass)",
			Recommended: "Set rpc_user and rpc_pass",
		})
	} else {
		results = append(results, ConfigResult{
			Severity: SeverityOK,
			Check:    "RPC authentication",
			Value:    "enabled",
		})
	}

	return results
}

func checkP2PConfig(s *settings.Settings) []ConfigResult {
	var results []ConfigResult

	if len(s.P2P.ListenAddresses) == 0 {
		results = append(results, ConfigResult{
			Severity:    SeverityERROR,
			Check:       "P2P listen addresses",
			Value:       "(empty)",
			Recommended: "Set p2p_listen_addresses (e.g. /ip4/0.0.0.0/tcp/9905)",
		})
	} else {
		results = append(results, ConfigResult{
			Severity: SeverityOK,
			Check:    "P2P listen addresses",
			Value:    strings.Join(s.P2P.ListenAddresses, ", "),
		})
	}

	results = append(results, ConfigResult{
		Severity: SeverityINFO,
		Check:    "P2P listen mode",
		Value:    s.P2P.ListenMode,
	})

	results = append(results, ConfigResult{
		Severity: SeverityINFO,
		Check:    "P2P DHT mode",
		Value:    s.P2P.DHTMode,
	})

	if s.P2P.EnableNAT {
		results = append(results, ConfigResult{
			Severity:    SeverityWARN,
			Check:       "P2P NAT",
			Value:       "enabled",
			Recommended: "Disable on cloud/shared hosting (triggers abuse reports)",
		})
	}

	if s.P2P.EnableMDNS {
		results = append(results, ConfigResult{
			Severity:    SeverityWARN,
			Check:       "P2P mDNS",
			Value:       "enabled",
			Recommended: "Disable on cloud/shared hosting (triggers abuse reports)",
		})
	}

	if s.P2P.AllowPrivateIPs {
		severity := warnIfProd(s)
		results = append(results, ConfigResult{
			Severity:    severity,
			Check:       "P2P allow private IPs",
			Value:       "enabled",
			Recommended: "Disable on cloud/shared hosting",
		})
	}

	if len(s.P2P.StaticPeers) > 0 {
		results = append(results, ConfigResult{
			Severity: SeverityINFO,
			Check:    "P2P static peers",
			Value:    fmt.Sprintf("%d configured", len(s.P2P.StaticPeers)),
		})
	}

	if len(s.P2P.BootstrapPeers) > 0 {
		results = append(results, ConfigResult{
			Severity: SeverityINFO,
			Check:    "P2P bootstrap peers",
			Value:    fmt.Sprintf("%d configured", len(s.P2P.BootstrapPeers)),
		})
	}

	return results
}

func checkRetention(s *settings.Settings) []ConfigResult {
	var results []ConfigResult

	if s.GlobalBlockHeightRetention < 144 {
		results = append(results, ConfigResult{
			Severity:    SeverityWARN,
			Check:       "Block height retention",
			Value:       fmt.Sprintf("%d blocks", s.GlobalBlockHeightRetention),
			Recommended: ">= 144 (at least 1 day of blocks)",
		})
	} else {
		results = append(results, ConfigResult{
			Severity: SeverityOK,
			Check:    "Block height retention",
			Value:    fmt.Sprintf("%d blocks", s.GlobalBlockHeightRetention),
		})
	}

	return results
}

func checkDatabase(s *settings.Settings) []ConfigResult {
	var results []ConfigResult

	// Blockchain store type
	if s.BlockChain.StoreURL != nil {
		results = append(results, ConfigResult{
			Severity: SeverityINFO,
			Check:    "Blockchain store",
			Value:    s.BlockChain.StoreURL.Scheme + "://" + s.BlockChain.StoreURL.Host,
		})
	} else {
		results = append(results, ConfigResult{
			Severity:    SeverityERROR,
			Check:       "Blockchain store",
			Value:       "(not configured)",
			Recommended: "Set blockchain_store URL",
		})
	}

	// PostgreSQL connection pool
	pgPoolValue := fmt.Sprintf("max_open=%d, max_idle=%d", s.Postgres.MaxOpenConns, s.Postgres.MaxIdleConns)
	if s.Postgres.MaxIdleConns > s.Postgres.MaxOpenConns {
		results = append(results, ConfigResult{
			Severity:    SeverityWARN,
			Check:       "PostgreSQL pool",
			Value:       pgPoolValue,
			Recommended: "max_idle should be <= max_open (excess idle conns are wasted)",
		})
	} else {
		results = append(results, ConfigResult{
			Severity: SeverityINFO,
			Check:    "PostgreSQL pool",
			Value:    pgPoolValue,
		})
	}

	// Aerospike
	if s.Aerospike.Host != "" {
		results = append(results, ConfigResult{
			Severity: SeverityINFO,
			Check:    "Aerospike",
			Value:    fmt.Sprintf("%s:%d", s.Aerospike.Host, s.Aerospike.Port),
		})
	} else {
		results = append(results, ConfigResult{
			Severity: SeverityINFO,
			Check:    "Aerospike",
			Value:    "not configured",
		})
	}

	return results
}

func checkServices(s *settings.Settings) []ConfigResult {
	var results []ConfigResult

	type svcEntry struct {
		name    string
		address string
	}

	services := []svcEntry{
		{"Blockchain gRPC", s.BlockChain.GRPCAddress},
		{"Validator gRPC", s.Validator.GRPCAddress},
		{"Block Validation gRPC", s.BlockValidation.GRPCAddress},
		{"Block Assembly gRPC", s.BlockAssembly.GRPCAddress},
		{"Subtree Validation gRPC", s.SubtreeValidation.GRPCAddress},
		{"P2P gRPC", s.P2P.GRPCAddress},
		{"Asset HTTP", s.Asset.HTTPListenAddress},
	}

	var configured, unconfigured []string

	for _, svc := range services {
		if svc.address != "" {
			configured = append(configured, svc.name)
		} else {
			unconfigured = append(unconfigured, svc.name)
		}
	}

	results = append(results, ConfigResult{
		Severity: SeverityINFO,
		Check:    "Services configured",
		Value:    fmt.Sprintf("%d of %d", len(configured), len(services)),
	})

	if len(unconfigured) > 0 {
		results = append(results, ConfigResult{
			Severity: SeverityINFO,
			Check:    "Services not configured",
			Value:    strings.Join(unconfigured, ", "),
		})
	}

	// Check for localhost in non-dev context
	if !isDevContext(s) {
		var localhostServices []string

		for _, svc := range services {
			if svc.address == "" {
				continue
			}

			if strings.Contains(svc.address, "localhost") || strings.Contains(svc.address, "127.0.0.1") {
				localhostServices = append(localhostServices, svc.name)
			}
		}

		if len(localhostServices) > 0 {
			results = append(results, ConfigResult{
				Severity:    SeverityWARN,
				Check:       "Localhost in non-dev context",
				Value:       strings.Join(localhostServices, ", "),
				Recommended: "Use actual hostnames for distributed deployments",
			})
		}
	}

	return results
}

func checkPolicy(s *settings.Settings) []ConfigResult {
	var results []ConfigResult

	if s.Policy == nil {
		return results
	}

	// Excessive block size
	if s.Policy.ExcessiveBlockSize == 0 {
		results = append(results, ConfigResult{
			Severity:    SeverityINFO,
			Check:       "Excessive block size",
			Value:       "unlimited",
			Recommended: "Set a limit (e.g. 4GB-32GB) matching network consensus",
		})
	} else {
		results = append(results, ConfigResult{
			Severity: SeverityINFO,
			Check:    "Excessive block size",
			Value:    formatBytes(s.Policy.ExcessiveBlockSize),
		})
	}

	// Mining fee
	results = append(results, ConfigResult{
		Severity: SeverityINFO,
		Check:    "Min mining tx fee",
		Value:    fmt.Sprintf("%.8f", s.Policy.MinMiningTxFee),
	})

	return results
}

func checkDataFolder(s *settings.Settings) []ConfigResult {
	var results []ConfigResult

	dataFolder := s.DataFolder
	if dataFolder == "" {
		dataFolder = "data"
	}

	info, err := os.Stat(dataFolder)
	if err != nil {
		if os.IsNotExist(err) {
			results = append(results, ConfigResult{
				Severity:    SeverityWARN,
				Check:       "Data folder",
				Value:       dataFolder,
				Recommended: "Create the data folder before starting the node",
			})
		} else {
			results = append(results, ConfigResult{
				Severity:    SeverityERROR,
				Check:       "Data folder",
				Value:       fmt.Sprintf("%s (%v)", dataFolder, err),
				Recommended: "Check permissions on data folder",
			})
		}
	} else if !info.IsDir() {
		results = append(results, ConfigResult{
			Severity:    SeverityERROR,
			Check:       "Data folder",
			Value:       dataFolder,
			Recommended: "Path exists but is not a directory",
		})
	} else {
		// Check writable by attempting to create a temp file
		testFile := dataFolder + "/.diagnose_test"
		f, err := os.Create(testFile)
		if err != nil {
			results = append(results, ConfigResult{
				Severity:    SeverityERROR,
				Check:       "Data folder",
				Value:       fmt.Sprintf("%s (not writable)", dataFolder),
				Recommended: "Check write permissions on data folder",
			})
		} else {
			_ = f.Close()
			_ = os.Remove(testFile)

			results = append(results, ConfigResult{
				Severity: SeverityOK,
				Check:    "Data folder",
				Value:    fmt.Sprintf("%s (writable)", dataFolder),
			})
		}
	}

	return results
}

func checkObservability(s *settings.Settings) []ConfigResult {
	var results []ConfigResult

	// Tracing
	if s.TracingEnabled {
		collectorURL := ""
		if s.TracingCollectorURL != nil {
			collectorURL = s.TracingCollectorURL.String()
		}

		results = append(results, ConfigResult{
			Severity: SeverityINFO,
			Check:    "Tracing",
			Value:    fmt.Sprintf("enabled, sample_rate=%.2f, collector=%s", s.TracingSampleRate, collectorURL),
		})
	} else {
		results = append(results, ConfigResult{
			Severity: SeverityINFO,
			Check:    "Tracing",
			Value:    "disabled",
		})
	}

	// Profiler
	if s.ProfilerAddr != "" {
		results = append(results, ConfigResult{
			Severity:    SeverityINFO,
			Check:       "Profiler (pprof)",
			Value:       s.ProfilerAddr,
			Recommended: "Ensure not exposed publicly",
		})
	}

	// Prometheus
	if s.PrometheusEndpoint != "" {
		results = append(results, ConfigResult{
			Severity: SeverityINFO,
			Check:    "Prometheus endpoint",
			Value:    s.PrometheusEndpoint,
		})
	}

	// Log level
	results = append(results, ConfigResult{
		Severity: SeverityINFO,
		Check:    "Log level",
		Value:    s.LogLevel,
	})

	return results
}

func isDevContext(s *settings.Settings) bool {
	ctx := strings.ToLower(s.Context)
	return ctx == "" || ctx == "dev" || ctx == "test" || ctx == "docker"
}

// warnIfProd returns WARN for production contexts, INFO for dev contexts.
func warnIfProd(s *settings.Settings) Severity {
	if isDevContext(s) {
		return SeverityINFO
	}

	return SeverityWARN
}

func formatBytes(n int) string {
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
