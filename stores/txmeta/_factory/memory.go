package _factory

import (
	"net/url"

	"github.com/bitcoin-sv/ubsv/stores/txmeta"
	"github.com/bitcoin-sv/ubsv/stores/txmeta/memory"
	"github.com/bitcoin-sv/ubsv/ulogger"
)

func init() {
	availableDatabases["memory"] = func(logger ulogger.Logger, url *url.URL) (txmeta.Store, error) {
		var s txmeta.Store

		switch url.Path {
		case "/splitbyhash":
			//s = memory.NewSplitByHash(true)
		default:
			s = memory.New(logger)
		}

		return s, nil
	}
}
