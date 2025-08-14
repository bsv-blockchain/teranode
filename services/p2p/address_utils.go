package p2p

import (
	"strings"

	p2p "github.com/bsv-blockchain/go-p2p"
	ma "github.com/multiformats/go-multiaddr"
)

// Wrapper functions removed - use go-p2p public functions directly:
// - p2p.IsPrivateIPString() for IP string checks
// - p2p.GetIPFromMultiaddr() for multiaddr IP extraction
// - p2p.IsPrivateIP() for multiaddr IP checks

// FilterPublicAddresses returns only addresses with public IPs
func FilterPublicAddresses(addrs []ma.Multiaddr) []ma.Multiaddr {
	var publicAddrs []ma.Multiaddr

	for _, addr := range addrs {
		// Use the go-p2p library function to check if it's private
		if !p2p.IsPrivateIP(addr) {
			publicAddrs = append(publicAddrs, addr)
		}
	}

	return publicAddrs
}

// SelectBestAddress chooses the best address from a list
// Preference order: configured advertise address > public IP > private IP
func SelectBestAddress(addrs []ma.Multiaddr, configuredAddrs []string) ma.Multiaddr {
	if len(addrs) == 0 {
		return nil
	}

	// First, check if any address matches a configured advertise address
	if len(configuredAddrs) > 0 {
		for _, addr := range addrs {
			addrStr := addr.String()
			for _, configured := range configuredAddrs {
				if strings.Contains(addrStr, configured) || strings.Contains(configured, addrStr) {
					return addr
				}
			}
		}
	}

	// Second, prefer public addresses
	publicAddrs := FilterPublicAddresses(addrs)
	if len(publicAddrs) > 0 {
		return publicAddrs[0]
	}

	// Finally, fall back to first available address (even if private)
	return addrs[0]
}

// IsAddressAdvertised checks if an address is in the configured advertise list
func IsAddressAdvertised(addr ma.Multiaddr, configuredAddrs []string) bool {
	if len(configuredAddrs) == 0 {
		return false
	}

	addrStr := addr.String()
	for _, configured := range configuredAddrs {
		// Check if the configured address is contained in or contains the multiaddr
		if strings.Contains(addrStr, configured) || strings.Contains(configured, addrStr) {
			return true
		}
	}

	return false
}
