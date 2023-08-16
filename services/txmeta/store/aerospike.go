//go:build aerospike

package store

import (
	"net/url"

	"github.com/bitcoin-sv/ubsv/stores/txmeta"
	"github.com/bitcoin-sv/ubsv/stores/txmeta/aerospike"
)

func init() {
	availableDatabases["aerospike"] = func(url *url.URL) (txmeta.Store, error) {
		return aerospike.New(url)
	}
}
