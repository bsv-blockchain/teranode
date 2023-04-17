package aerospike

import (
	"testing"

	aero "github.com/aerospike/aerospike-client-go"
	"github.com/libsv/go-p2p/chaincfg/chainhash"
)

func TestEmptyChainHash(t *testing.T) {
	var txID *chainhash.Hash

	bins := aero.BinMap{
		"txid": txID[:],
	}

	t.Logf("%x", bins["txid"])

}
