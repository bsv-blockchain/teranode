// Copyright (c) 2013-2016 The btcsuite developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

package legacy

import (
	"github.com/bitcoin-sv/ubsv/chaincfg"
)

// activeNetParams is a pointer to the parameters specific to the
// currently active bitcoin network.
var activeNetParams = &mainNetParams

// params is used to group parameters for various networks such as the main
// network and test networks.
type params struct {
	*chaincfg.Params
	rpcPort string
}

// mainNetParams contains parameters specific to the main network
// (wire.MainNet).  NOTE: The RPC port is intentionally different than the
// reference implementation because bsvd does not handle wallet requests.  The
// separate wallet process listens on the well-known port and forwards requests
// it does not handle on to bsvd.  This approach allows the wallet process
// to emulate the full reference implementation RPC API.
var mainNetParams = params{
	Params:  &chaincfg.MainNetParams,
	rpcPort: "8334",
}

// regressionNetParams contains parameters specific to the regression test
// network (wire.TestNet).  NOTE: The RPC port is intentionally different
// than the reference implementation - see the mainNetParams comment for
// details.
var regressionNetParams = params{
	Params:  &chaincfg.RegressionNetParams,
	rpcPort: "18334",
}

// testNetParams contains parameters specific to the test network (version 3)
// (wire.TestNet).  NOTE: The RPC port is intentionally different than the
// reference implementation - see the mainNetParams comment for details.
var testNetParams = params{
	Params:  &chaincfg.TestNetParams,
	rpcPort: "18334",
}

// simNetParams contains parameters specific to the simulation test network
// (wire.SimNet).
// var simNetParams = params{
// 	Params:  &chaincfg.SimNetParams,
// 	rpcPort: "18556",
// }
