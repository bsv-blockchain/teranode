package util

import (
	"fmt"
	"net"
	"net/http"
	"net/url"
	"strings"
)

// WebsocketOriginChecker returns a CheckOrigin function for a gorilla
// websocket upgrader. It replaces the "always allow any origin" default,
// which makes an endpoint trivially embeddable by any third-party page for
// cross-site WebSocket hijacking.
//
// Rules:
//   - No Origin header (non-browser clients, e.g. server-to-server tooling) -
//     always allowed, since CheckOrigin only exists to stop browsers from
//     silently including credentials on a cross-origin request.
//   - Origin host matches the request Host (same-host) - always allowed.
//   - Anything else - allowed only if present in allowedOrigins.
//
// Both websocket endpoints (services/p2p's /p2p-ws and the Asset service's
// /connection/websocket) share this one implementation deliberately: two
// independently maintained copies of a security predicate drift, and a fix to
// one (Origin: null handling, default-port normalisation) would silently not
// reach the other.
func WebsocketOriginChecker(allowedOrigins []string) func(r *http.Request) bool {
	allowed := make(map[string]struct{}, len(allowedOrigins))

	for _, o := range allowedOrigins {
		o = strings.ToLower(strings.TrimSpace(o))
		if o != "" {
			allowed[o] = struct{}{}
		}
	}

	return func(r *http.Request) bool {
		origin := r.Header.Get("Origin")
		if origin == "" {
			return true
		}

		if u, err := url.Parse(origin); err == nil && u.Host != "" && strings.EqualFold(u.Host, r.Host) {
			return true
		}

		_, ok := allowed[strings.ToLower(strings.TrimSpace(origin))]

		return ok
	}
}

// DevServerOrigins returns the http/https localhost Origin header values for
// the given Vite dev-server ports (settings.Dashboard.DevServerPorts).
//
// Callers must gate this on LoopbackListenAddress: dashboard_devServerPorts
// has a non-empty default in every settings context, so appending these
// unconditionally would permanently allowlist http(s)://localhost:5173 and
// :4173 on every node, dev or production - re-opening a slice of exactly the
// cross-site websocket hijacking the origin check exists to close.
func DevServerOrigins(ports []int) []string {
	origins := make([]string, 0, len(ports)*2)

	for _, port := range ports {
		origins = append(origins,
			fmt.Sprintf("http://localhost:%d", port),
			fmt.Sprintf("https://localhost:%d", port),
		)
	}

	return origins
}

// LoopbackListenAddress reports whether a "host:port" listen address binds
// only the loopback interface. A wildcard bind (":8090", "0.0.0.0:8090",
// "[::]:8090") is reachable from the network and therefore not loopback.
//
// It is the gate for the dev-server origin escape hatch: a node that is only
// reachable from its own host cannot be targeted by a browser elsewhere on
// the network, so allowlisting local dev origins there costs nothing, while a
// network-reachable node keeps a strict default-deny origin check.
func LoopbackListenAddress(listenAddress string) bool {
	listenAddress = strings.TrimSpace(listenAddress)
	if listenAddress == "" {
		return false
	}

	host, _, err := net.SplitHostPort(listenAddress)
	if err != nil {
		// No port component; treat the whole value as the host.
		host = listenAddress
	}

	host = strings.Trim(host, "[]")
	if host == "" {
		// ":8090" - wildcard bind on every interface.
		return false
	}

	if strings.EqualFold(host, "localhost") {
		return true
	}

	ip := net.ParseIP(host)
	if ip == nil {
		return false
	}

	return ip.IsLoopback()
}
