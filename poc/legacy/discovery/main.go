package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"os"
	"sync"
	"time"

	"github.com/bitcoin-sv/ubsv/services/legacy/chaincfg"
)

var params = &chaincfg.MainNetParams
var knownPeers = make(map[string]bool)
var mu sync.Mutex
var persistentFile = "peers.json"

func main() {
	// Load persistent peers from file
	loadPersistentPeers()

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
				mu.Lock()
				knownPeers[peer] = true
				mu.Unlock()
			}
			savePersistentPeers()
			propagatePeers(peers)
		}(seed.Host)
	}

	// Periodically propagate known peers to other peers
	go func() {
		for {
			time.Sleep(1 * time.Minute)
			mu.Lock()
			var peers []string
			for peer := range knownPeers {
				peers = append(peers, peer)
			}
			mu.Unlock()
			propagatePeers(peers)
		}
	}()

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

func propagatePeers(peers []string) {
	// Implementation to propagate peers to connected nodes
	// This is a placeholder, as the exact method depends on the Bitcoin SV node's networking code
	for _, peer := range peers {
		fmt.Printf("Propagating peer: %s\n", peer)
	}
}

func loadPersistentPeers() {
	file, err := os.Open(persistentFile)
	if err != nil {
		log.Printf("Failed to load persistent peers: %v", err)
		return
	}
	defer file.Close()

	var peers []string
	err = json.NewDecoder(file).Decode(&peers)
	if err != nil {
		log.Printf("Failed to decode persistent peers: %v", err)
		return
	}

	mu.Lock()
	for _, peer := range peers {
		knownPeers[peer] = true
	}
	mu.Unlock()
}

func savePersistentPeers() {
	file, err := os.Create(persistentFile)
	if err != nil {
		log.Printf("Failed to save persistent peers: %v", err)
		return
	}
	defer file.Close()

	mu.Lock()
	var peers []string
	for peer := range knownPeers {
		peers = append(peers, peer)
	}
	mu.Unlock()

	err = json.NewEncoder(file).Encode(peers)
	if err != nil {
		log.Printf("Failed to encode persistent peers: %v", err)
	}
}
