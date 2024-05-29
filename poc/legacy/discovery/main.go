package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"time"

	"github.com/bitcoin-sv/ubsv/services/legacy/chaincfg"
)

var params = &chaincfg.MainNetParams

func main() {
	// DNS seed nodes for peer discovery
	dnsSeeds := params.DNSSeeds

	// Discover peers using DNS seed nodes
	for _, seed := range dnsSeeds {
		go func(seed string) {
			peers, err := discoverPeers(seed)
			if err != nil {
				log.Printf("Failed to discover peers from %s: %v", seed, err)
				return
			}

			for _, peer := range peers {
				fmt.Printf("Discovered peer: %s\n", peer)
			}
		}(seed.Host)
	}

	// Keep the program running to allow for peer discovery
	select {}
}

func discoverPeers(seed string) ([]string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	ips, err := net.DefaultResolver.LookupIPAddr(ctx, seed)
	if err != nil {
		return nil, err
	}

	peers := make([]string, len(ips))
	for i, ip := range ips {
		peers[i] = net.JoinHostPort(ip.String(), params.DefaultPort)
	}
	return peers, nil
}
