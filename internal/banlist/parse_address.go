package banlist

import (
	"fmt"
	"net"
	"strings"

	"github.com/bsv-blockchain/teranode/errors"
)

func parseAddress(ipOrSubnet string) (subnet *net.IPNet, err error) {
	if strings.Contains(ipOrSubnet, "/") {
		_, subnet, err = net.ParseCIDR(ipOrSubnet)
		if err != nil {
			return nil, errors.New(errors.ERR_INVALID_SUBNET, fmt.Sprintf("can't parse subnet: %s", ipOrSubnet))
		}

		return subnet, nil
	}

	// Strip port from IP address (handles both IPv4 and IPv6)
	if host, _, err := net.SplitHostPort(ipOrSubnet); err == nil {
		ipOrSubnet = host
	}

	ip := net.ParseIP(ipOrSubnet)
	if ip == nil {
		return nil, errors.New(errors.ERR_INVALID_IP, fmt.Sprintf("can't parse IP: %s", ipOrSubnet))
	}

	// Build the host network from parsed bytes, not the original spelling, so
	// IPv4-mapped IPv6 hosts become IPv4 /32 hosts rather than IPv6 /32 prefixes.
	if ipv4 := ip.To4(); ipv4 != nil {
		return &net.IPNet{IP: ipv4, Mask: net.CIDRMask(32, 32)}, nil
	}

	return &net.IPNet{IP: ip, Mask: net.CIDRMask(128, 128)}, nil
}
