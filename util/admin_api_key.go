package util

import (
	"net"
	"strings"

	"github.com/bsv-blockchain/teranode/errors"
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

// IsPlaceholderAdminAPIKey reports whether key is a well-known, non-secret
// placeholder that must never guard admin RPCs. Leading/trailing whitespace is
// ignored and the comparison is case-insensitive.
func IsPlaceholderAdminAPIKey(key string) bool {
	_, ok := knownPlaceholderAPIKeys[strings.ToLower(strings.TrimSpace(key))]
	return ok
}

// ValidateAdminAPIKey guards the configured gRPC admin API key before a server
// starts serving protected (state-mutating) admin RPCs.
//
// It hard-fails (returns an error) when the key is a well-known placeholder such
// as "testkey", so a world-readable committed value can never silently reduce
// admin authentication to a no-op. An empty key is intentionally allowed here:
// callers fall back to a randomly generated key that leaves admin RPCs
// unreachable (fail-closed), which is safe.
//
// When a real key is configured but served on a non-loopback listener without
// verified gRPC transport security, the key would travel where it can be
// harvested; this logs a warning rather than failing, because internal
// deployments on trusted networks legitimately run without TLS and must not be
// crashed at startup. securityLevel 0 sends the key in cleartext and level 1
// encrypts without verifying the server certificate (MITM-exploitable, see
// loadTLSCredentials), so both warn; level 2+ (verified TLS) is treated as safe.
func ValidateAdminAPIKey(logger ulogger.Logger, serviceName, apiKey, listenAddress string, securityLevel int) error {
	trimmed := strings.TrimSpace(apiKey)
	if trimmed == "" {
		return nil
	}

	if IsPlaceholderAdminAPIKey(trimmed) {
		return errors.NewConfigurationError("[%s] grpc_admin_api_key is set to the well-known placeholder %q; this repository is public so such a value protects nothing - supply a strong random secret (32+ chars) via an environment variable or secret store", serviceName, trimmed)
	}

	if securityLevel <= 1 && !isLoopbackListenAddress(listenAddress) {
		logger.Warnf("[%s] grpc_admin_api_key is set but the gRPC listener %q is not loopback-bound and securityLevelGRPC=%d does not provide verified transport security, so the admin key can be harvested in transit; bind the listener to loopback or set securityLevelGRPC >= 2 with certificate verification", serviceName, listenAddress, securityLevel)
	}

	return nil
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
	if host == "" {
		// e.g. ":9904" - binds all interfaces.
		return false
	}

	if ip := net.ParseIP(host); ip != nil {
		return ip.IsLoopback()
	}

	return strings.EqualFold(host, "localhost")
}
