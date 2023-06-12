//go:build aerospike

package store

import (
	"net/url"

	"github.com/TAAL-GmbH/ubsv/stores/txstatus"
	"github.com/TAAL-GmbH/ubsv/stores/txstatus/aerospike"
)

func init() {
	availableDatabases["aerospike"] = func(url *url.URL) (txstatus.Store, error) {
		return aerospike.New(url)
	}
}
