package settings

import (
	"fmt"
	"net"
	"strconv"
	"strings"

	"github.com/multiformats/go-multiaddr"
)

// ValidateP2PListenAddresses checks that p2p_listen_addresses describes the bind
// the P2P service will actually perform.
//
// go-p2p-message-bus has no listen-address option: it always binds libp2p to
// /ip4/0.0.0.0/tcp/<p2p_port> and /ip6/::/tcp/<p2p_port>. Any value that asks for
// something narrower (a specific interface, loopback, a different port) would be
// silently ignored, so it is rejected here instead. Accepted forms are the
// wildcard IPs "0.0.0.0" and "::", with or without the /ip4 or /ip6 multiaddr
// prefix, optionally followed by /tcp/<p2p_port>.
func ValidateP2PListenAddresses(addrs []string, port int) error {
	if len(addrs) == 0 {
		return fmt.Errorf("p2p_listen_addresses not set in config")
	}

	for _, raw := range addrs {
		addr := strings.TrimSpace(raw)
		if addr == "" {
			return fmt.Errorf("p2p_listen_addresses contains an empty entry")
		}

		if err := validateListenAddress(addr, port); err != nil {
			return fmt.Errorf("p2p_listen_addresses entry %q is not supported: %w (libp2p always binds 0.0.0.0 and :: on p2p_port %d; use p2p_advertise_addresses or a firewall to control reachability)", addr, err, port)
		}
	}

	return nil
}

func validateListenAddress(addr string, port int) error {
	if !strings.HasPrefix(addr, "/") {
		ip := net.ParseIP(addr)
		if ip == nil {
			return fmt.Errorf("not an IP address or multiaddr")
		}
		if !ip.IsUnspecified() {
			return fmt.Errorf("binding a specific interface is not supported")
		}
		return nil
	}

	ma, err := multiaddr.NewMultiaddr(addr)
	if err != nil {
		return fmt.Errorf("invalid multiaddr: %w", err)
	}

	var (
		sawIP   bool
		portStr string
	)
	for _, c := range ma.Protocols() {
		switch c.Code {
		case multiaddr.P_IP4, multiaddr.P_IP6:
			v, err := ma.ValueForProtocol(c.Code)
			if err != nil {
				return fmt.Errorf("invalid multiaddr: %w", err)
			}
			ip := net.ParseIP(v)
			if ip == nil || !ip.IsUnspecified() {
				return fmt.Errorf("binding a specific interface is not supported")
			}
			sawIP = true
		case multiaddr.P_TCP:
			portStr, err = ma.ValueForProtocol(c.Code)
			if err != nil {
				return fmt.Errorf("invalid multiaddr: %w", err)
			}
		default:
			return fmt.Errorf("unsupported multiaddr component /%s", c.Name)
		}
	}

	if !sawIP {
		return fmt.Errorf("multiaddr has no /ip4 or /ip6 component")
	}

	if portStr != "" {
		p, err := strconv.Atoi(portStr)
		if err != nil || p != port {
			return fmt.Errorf("port %s does not match p2p_port %d", portStr, port)
		}
	}

	return nil
}
