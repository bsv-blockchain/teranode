package util

import (
	"net"
	"strings"

	"github.com/bsv-blockchain/teranode/ulogger"
)

// knownPlaceholderAPIKeys are well-known, non-secret values that must never be
// used to guard admin RPCs. bsv-blockchain/teranode is a public repository, so
// any value committed to settings.conf is world-readable; accepting one of
// these would make the admin auth interceptor a no-op. Compared case-insensitively.
var knownPlaceholderAPIKeys = map[string]struct{}{
	"testkey":   {},
	"test":      {},
	"changeme":  {},
	"change_me": {},
	"change-me": {},
	"password":  {},
	"secret":    {},
	"admin":     {},
	"apikey":    {},
	"api_key":   {},
	"default":   {},
}

// minAdminAPIKeyLength is the shortest admin key that does not draw a startup
// warning; it matches the cmd/diagnose weak-key threshold. 32+ is recommended.
const minAdminAPIKeyLength = 16

// IsPlaceholderAdminAPIKey reports whether key is a well-known, non-secret
// placeholder that must never guard admin RPCs. Leading/trailing whitespace is
// ignored and the comparison is case-insensitive.
func IsPlaceholderAdminAPIKey(key string) bool {
	_, ok := knownPlaceholderAPIKeys[strings.ToLower(strings.TrimSpace(key))]
	return ok
}

// WarnIfAdminAPIKeyExposed logs a warning if a configured (non-empty) admin API
// key is weak, or is served on a listener where it can be read in transit.
// Placeholder rejection is handled separately by ValidateAdminAPIKey (util/grpc.go),
// which fails startup outright rather than warning - a publicly-known
// credential is a hard configuration error, not just a hygiene issue.
//
// A short key draws a length warning: it matches the cmd/diagnose weak-key
// threshold (32+ recommended).
//
// When the key is served on a non-loopback listener without verified gRPC
// transport security, the key would travel where it can be harvested; this
// logs a warning, because internal deployments on trusted networks
// legitimately run without TLS. securityLevel 0 sends the key in cleartext
// and level 1 encrypts without verifying the server certificate
// (MITM-exploitable, see loadTLSCredentials), so both warn; level 2+
// (verified TLS) is treated as safe. Callers should only invoke this once the
// key has already passed ValidateAdminAPIKey and is known non-empty.
func WarnIfAdminAPIKeyExposed(logger ulogger.Logger, serviceName, apiKey, listenAddress string, securityLevel int) {
	trimmed := strings.TrimSpace(apiKey)
	if trimmed == "" {
		return
	}

	if len(trimmed) < minAdminAPIKeyLength {
		logger.Warnf("[%s] grpc_admin_api_key is only %d characters; use at least %d (32+ recommended) so the admin secret is not trivially guessable", serviceName, len(trimmed), minAdminAPIKeyLength)
	}

	if securityLevel <= 1 && !isLoopbackListenAddress(listenAddress) {
		logger.Warnf("[%s] grpc_admin_api_key is set but the gRPC listener %q is not loopback-bound and securityLevelGRPC=%d does not provide verified transport security, so the admin key can be harvested in transit; bind the listener to loopback or set securityLevelGRPC >= 2 with certificate verification", serviceName, listenAddress, securityLevel)
	}
}

// isLoopbackListenAddress reports whether a gRPC listen address is bound only to
// the loopback interface. An unspecified host (empty, "0.0.0.0" or "::") is
// treated as non-loopback because it accepts connections from any interface.
func isLoopbackListenAddress(listenAddress string) bool {
	addr := strings.TrimSpace(listenAddress)
	if addr == "" {
		return false
	}

	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		// No port present; treat the whole string as the host.
		host = addr
	}

	host = strings.TrimSpace(host)

	// A bare bracketed IPv6 literal ("[::1]") has no port for SplitHostPort to
	// strip, so remove the brackets before parsing.
	if strings.HasPrefix(host, "[") && strings.HasSuffix(host, "]") {
		host = host[1 : len(host)-1]
	}

	if host == "" {
		// e.g. ":9904" - binds all interfaces.
		return false
	}

	if ip := net.ParseIP(host); ip != nil {
		return ip.IsLoopback()
	}

	return strings.EqualFold(host, "localhost")
}
