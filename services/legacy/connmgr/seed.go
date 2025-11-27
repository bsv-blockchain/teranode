// Copyright (c) 2016 The btcsuite developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

package connmgr

import (
	"fmt"
	mrand "math/rand/v2"
	"net"
	"strconv"
	"time"

	"github.com/bsv-blockchain/go-chaincfg"
	"github.com/bsv-blockchain/go-wire"
)

const (
	// These constants are used by the DNS seed code to pick a random last
	// seen time.
	secondsIn3Days int32 = 24 * 60 * 60 * 3
	secondsIn4Days int32 = 24 * 60 * 60 * 4
)

// OnSeed is the signature of the callback function which is invoked when DNS
// seeding is successful. The callback receives a slice of network addresses
// discovered through DNS seeding.
type OnSeed func(addrs []*wire.NetAddress)

// LookupFunc is the signature of the DNS lookup function used to resolve
// DNS seed hostnames to IP addresses.
type LookupFunc func(string) ([]net.IP, error)

// SeedFromDNS uses DNS seeding to populate the address manager with peers.
func SeedFromDNS(chainParams *chaincfg.Params, reqServices wire.ServiceFlag,
	lookupFn LookupFunc, seedFn OnSeed) {
	for _, dnsseed := range chainParams.DNSSeeds {
		var host string
		if !dnsseed.HasFiltering || reqServices == wire.SFNodeNetwork {
			host = dnsseed.Host
		} else {
			host = fmt.Sprintf("x%x.%s", uint64(reqServices), dnsseed.Host)
		}

		go func(host string) {
			seedpeers, err := lookupFn(host)
			if err != nil {
				// log.Infof("DNS discovery failed on seed %s: %v", host, err)
				return
			}

			numPeers := len(seedpeers)

			// log.Infof("%d addresses found from DNS seed %s", numPeers, host)

			if numPeers == 0 {
				return
			}

			addresses := make([]*wire.NetAddress, len(seedpeers))
			// Parse port as uint16 directly to avoid integer conversion issues
			port, err := strconv.ParseUint(chainParams.DefaultPort, 10, 16)
			if err != nil {
				// Invalid port configuration, skip this seed
				return
			}

			for i, peer := range seedpeers {
				randSource := mrand.NewPCG(uint64(time.Now().UnixNano()), uint64(secondsIn4Days))
				// #nosec G404
				rand := mrand.New(randSource)
				addresses[i] = wire.NewNetAddressTimestamp(
					// bitcoind seeds with addresses from
					// a time randomly selected between 3
					// and 7 days ago.
					time.Now().Add(-1*time.Second*time.Duration(secondsIn3Days+rand.Int32N(secondsIn4Days))),
					0, peer, uint16(port))
			}

			seedFn(addresses)
		}(host)
	}
}
