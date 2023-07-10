//go:build aerospike

package store

import (
	"net/url"

	"github.com/TAAL-GmbH/ubsv/stores/txmeta"
	"github.com/TAAL-GmbH/ubsv/stores/txmeta/aerospike"
)

func init() {
	availableDatabases["aerospike"] = func(url *url.URL) (txmeta.Store, error) {
		return aerospike.New(url)
	}
}
