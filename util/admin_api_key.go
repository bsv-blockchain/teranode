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

// IsWeakAdminAPIKey reports whether a configured key is a real value (neither
// empty nor a well-known placeholder) that is nonetheless short enough to be
// worth brute-forcing. Callers that expose the listener beyond loopback should
// treat this as fatal rather than advisory: unlike a placeholder, a short key is
// accepted as genuine, so nothing else stops an attacker who guesses it.
func IsWeakAdminAPIKey(key string) bool {
	trimmed := strings.TrimSpace(key)

	return trimmed != "" && !IsPlaceholderAdminAPIKey(trimmed) && len(trimmed) < minAdminAPIKeyLength
}

// MinAdminAPIKeyLength is the shortest admin key that is not considered weak.
func MinAdminAPIKeyLength() int { return minAdminAPIKeyLength }

// ValidateAdminAPIKey inspects the configured gRPC admin API key before a server
// installs the auth interceptor, and reports whether the caller should ignore the
// configured value and fall back to the random-key (fail-closed) path.
//
// A well-known placeholder such as "testkey" is ignored (logged at Error) rather
// than trusted: this repository is public, so a committed placeholder protects
// nothing. Ignoring it lands on the same fail-closed path as an empty key - a
// random key is generated and the protected admin RPCs stay unreachable until a
// real secret is configured - so the node keeps validating and relaying blocks
// instead of being taken offline by a setting that only guards admin RPCs. An
// empty key returns false (nothing to ignore), and the caller's own empty check
// generates the random key.
//
// When a real key is configured but served on a non-loopback listener without
// verified gRPC transport security, the key would travel where it can be
// harvested; this logs a warning, because internal deployments on trusted
// networks legitimately run without TLS. securityLevel 0 sends the key in
// cleartext and level 1 encrypts without verifying the server certificate
// (MITM-exploitable, see loadTLSCredentials), so both warn; level 2+ (verified
// TLS) is treated as safe.
func ValidateAdminAPIKey(logger ulogger.Logger, serviceName, apiKey, listenAddress string, securityLevel int) (ignoreKey bool) {
	trimmed := strings.TrimSpace(apiKey)
	if trimmed == "" {
		return false
	}

	if IsPlaceholderAdminAPIKey(trimmed) {
		logger.Errorf("[%s] grpc_admin_api_key is the well-known placeholder %q; this repository is public so it protects nothing. Ignoring it: a random key is used instead, so admin RPCs are unreachable until a strong secret is supplied via an environment variable or secret store", serviceName, trimmed)
		return true
	}

	if len(trimmed) < minAdminAPIKeyLength {
		logger.Warnf("[%s] grpc_admin_api_key is only %d characters; use at least %d (32+ recommended) so the admin secret is not trivially guessable", serviceName, len(trimmed), minAdminAPIKeyLength)
	}

	if securityLevel <= 1 && !isLoopbackListenAddress(listenAddress) {
		logger.Warnf("[%s] grpc_admin_api_key is set but the gRPC listener %q is not loopback-bound and securityLevelGRPC=%d does not provide verified transport security, so the admin key can be harvested in transit; bind the listener to loopback or set securityLevelGRPC >= 2 with certificate verification", serviceName, listenAddress, securityLevel)
	}

	return false
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
