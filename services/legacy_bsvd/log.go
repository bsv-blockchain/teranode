// Copyright (c) 2013-2017 The btcsuite developers
// Copyright (c) 2017 The Decred developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

package legacy

import (
	"github.com/bitcoin-sv/ubsv/ulogger"
)

var (
	adxrLog = ulogger.New("ADXR")
	amgrLog = ulogger.New("AMGR")
	cmgrLog = ulogger.New("CMGR")
	bcdbLog = ulogger.New("BCDB")
	bsvdLog = ulogger.New("BSVD")
	chanLog = ulogger.New("CHAN")
	discLog = ulogger.New("DISC")
	indxLog = ulogger.New("INDX")
	minrLog = ulogger.New("MINR")
	peerLog = ulogger.New("PEER")
	rpcsLog = ulogger.New("RPCS")
	scrpLog = ulogger.New("SCRP")
	srvrLog = ulogger.New("SRVR")
	syncLog = ulogger.New("SYNC")

	txmpLog = ulogger.New("TXMP")
)

// subsystemLoggers maps each subsystem identifier to its associated logger.
var subsystemLoggers = map[string]ulogger.Logger{
	"ADXR": adxrLog,
	"AMGR": amgrLog,
	"CMGR": cmgrLog,
	"BCDB": bcdbLog,
	"BSVD": bsvdLog,
	"CHAN": chanLog,
	"DISC": discLog,
	"INDX": indxLog,
	"MINR": minrLog,
	"PEER": peerLog,
	"RPCS": rpcsLog,
	"SCRP": scrpLog,
	"SRVR": srvrLog,
	"SYNC": syncLog,
	"TXMP": txmpLog,
}

// directionString is a helper function that returns a string that represents
// the direction of a connection (inbound or outbound).
func directionString(inbound bool) string {
	if inbound {
		return "inbound"
	}
	return "outbound"
}
