// //go:build memory

package utxo

import (
	"context"
	"net/url"

	"github.com/TAAL-GmbH/ubsv/services/utxo"
	"github.com/TAAL-GmbH/ubsv/services/utxo/store"
	"github.com/TAAL-GmbH/ubsv/services/utxo/utxostore_api"
	"github.com/ordishs/go-utils"
	"github.com/ordishs/gocore"
)

func init() {
	availableDatabases["memory"] = func(url *url.URL) (store.UTXOStore, error) {
		conn, err := utils.GetGRPCClient(context.Background(), url.Host, &utils.ConnectionOptions{
			OpenTracing: gocore.Config().GetBool("use_open_tracing", true),
			MaxRetries:  3,
		})
		if err != nil {
			return nil, err
		}

		apiClient := utxostore_api.NewUtxoStoreAPIClient(conn)
		return utxo.NewClient(apiClient)
	}
}
