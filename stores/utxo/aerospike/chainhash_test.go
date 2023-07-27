//go:build manual_tests

package aerospike

import (
	"testing"

	aero "github.com/aerospike/aerospike-client-go/v6"
	"github.com/libsv/go-bt/v2/chainhash"
)

func TestEmptyChainHash(t *testing.T) {
	var txID chainhash.Hash

	bins := aero.BinMap{
		"txid": txID[:],
	}

	t.Logf("%x", bins["txid"])

}
