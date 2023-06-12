package store

import (
	"net/url"

	"github.com/TAAL-GmbH/ubsv/stores/txstatus"
	"github.com/TAAL-GmbH/ubsv/stores/txstatus/memory"
)

func init() {
	availableDatabases["memory"] = func(url *url.URL) (txstatus.Store, error) {
		var s txstatus.Store

		switch url.Path {
		case "/splitbyhash":
			//s = memory.NewSplitByHash(true)
		default:
			s = memory.New()
		}

		return s, nil
	}
}
