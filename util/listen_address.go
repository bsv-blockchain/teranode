package util

import (
	"net"
	"strings"
)

// IsLoopbackListenAddress reports whether a listen address is bound only to
// the loopback interface. An unspecified host (empty, "0.0.0.0" or "::") is
// treated as non-loopback because it accepts connections from any interface.
func IsLoopbackListenAddress(listenAddress string) bool {
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
