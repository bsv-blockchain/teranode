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

	var cidr string
	if ip.To4() != nil {
		cidr = fmt.Sprintf("%s/32", ipOrSubnet)
	} else {
		cidr = fmt.Sprintf("%s/128", ipOrSubnet)
	}

	_, subnet, err = net.ParseCIDR(cidr)
	if err != nil {
		return nil, errors.New(errors.ERR_INVALID_IP, fmt.Sprintf("can't create subnet from IP: %s", ipOrSubnet))
	}

	return subnet, nil
}
