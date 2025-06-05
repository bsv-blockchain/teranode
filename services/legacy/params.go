// Copyright (c) 2013-2016 The btcsuite developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

// Package legacy implements a Bitcoin SV legacy protocol server that handles peer-to-peer communication
// and blockchain synchronization using the traditional Bitcoin network protocol.
//
// The params.go file defines network parameters for different Bitcoin networks (mainnet, testnet,
// regtest) used by the legacy service. These parameters configure network-specific settings like RPC
// ports and are based on the chain configuration parameters defined in the chaincfg package.
package legacy

import (
	"github.com/bitcoin-sv/teranode/pkg/go-chaincfg"
)

// activeNetParams is a pointer to the parameters specific to the
// currently active bitcoin network.
//
// This variable serves as the global reference point for accessing the current network parameters
// throughout the legacy service. It defaults to mainnet parameters but can be modified during
// initialization to point to testnet or regnet parameters based on configuration.
var activeNetParams = &mainNetParams

// params is used to group parameters for various networks such as the main
// network and test networks.
//
// The params struct extends the base chaincfg.Params with additional RPC configuration
// specifically used by the legacy service. This allows for network-specific RPC port
// settings while inheriting all the standard chain configuration parameters.
type params struct {
	// Params contains the base chain configuration parameters
	*chaincfg.Params

	// rpcPort is the default port used for the legacy RPC service
	rpcPort string
}

// mainNetParams contains parameters specific to the main network (mainnet).
//
// NOTE: The RPC port is intentionally different than the reference implementation because
// bsvd does not handle wallet requests. The separate wallet process listens on the well-known
// port and forwards requests it does not handle on to bsvd. This approach allows the wallet
// process to emulate the full reference implementation RPC API.
//
// The main Bitcoin network (mainnet) is the primary production blockchain where real
// value transactions occur. It uses the standard port 8333 for P2P communication and
// defines the official genesis block and consensus rules.
var mainNetParams = params{
	Params:  &chaincfg.MainNetParams,
	rpcPort: "8334",
}

// regressionNetParams contains parameters specific to the regression test network (regtest).
//
// NOTE: The RPC port is intentionally different than the reference implementation - see
// the mainNetParams comment for details.
//
// The regression test network (regtest) is a local testing environment where developers
// can create blocks on demand with minimal difficulty, allowing for controlled testing
// scenarios. Unlike testnet, regtest is intended for local development and testing only,
// with a private blockchain that can be reset at any time.
var regressionNetParams = params{
	Params:  &chaincfg.RegressionNetParams,
	rpcPort: "18334",
}

// testNetParams contains parameters specific to the test network (version 3).
//
// NOTE: The RPC port is intentionally different than the reference implementation - see
// the mainNetParams comment for details.
//
// The test network (testnet) is a public alternative blockchain used for testing purposes.
// It mimics the main network but uses different ports (18333 for P2P) and allows for
// easier mining to facilitate testing. Testnet coins have no real-world value and the
// network is periodically reset. It serves as a staging environment before deploying
// to mainnet.
var testNetParams = params{
	Params:  &chaincfg.TestNetParams,
	rpcPort: "18334",
}
