package store

import (
	"net/url"

	"github.com/bitcoin-sv/ubsv/stores/txmeta"
	"github.com/bitcoin-sv/ubsv/stores/txmeta/memory"
)

func init() {
	availableDatabases["memory"] = func(url *url.URL) (txmeta.Store, error) {
		var s txmeta.Store

		switch url.Path {
		case "/splitbyhash":
			//s = memory.NewSplitByHash(true)
		default:
			s = memory.New()
		}

		return s, nil
	}
}
