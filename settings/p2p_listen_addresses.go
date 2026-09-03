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

	// Walk the components rather than calling ValueForProtocol, which only
	// returns the first match and would let /ip4/0.0.0.0/tcp/9905/ip4/10.0.0.1
	// slip through on the strength of its leading wildcard.
	var sawIP, sawPort bool

	for _, c := range ma {
		switch c.Protocol().Code {
		case multiaddr.P_IP4, multiaddr.P_IP6:
			ip := net.ParseIP(c.Value())
			if ip == nil || !ip.IsUnspecified() {
				return fmt.Errorf("binding a specific interface is not supported")
			}
			if sawIP {
				return fmt.Errorf("multiaddr has more than one IP component")
			}
			sawIP = true
		case multiaddr.P_TCP:
			if c.Value() != strconv.Itoa(port) {
				return fmt.Errorf("port %s does not match p2p_port %d", c.Value(), port)
			}
			if sawPort {
				return fmt.Errorf("multiaddr has more than one /tcp component")
			}
			sawPort = true
		default:
			return fmt.Errorf("unsupported multiaddr component /%s", c.Protocol().Name)
		}
	}

	if !sawIP {
		return fmt.Errorf("multiaddr has no /ip4 or /ip6 component")
	}

	return nil
}
