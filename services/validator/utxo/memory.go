// //go:build memory

package utxo

import (
	"context"
	"net/url"

	"github.com/bitcoin-sv/ubsv/services/utxo"
	"github.com/bitcoin-sv/ubsv/services/utxo/utxostore_api"
	utxostore "github.com/bitcoin-sv/ubsv/stores/utxo"
	"github.com/bitcoin-sv/ubsv/util"
	"github.com/ordishs/gocore"
)

func init() {
	availableDatabases["memory"] = func(url *url.URL) (utxostore.Interface, error) {
		conn, err := util.GetGRPCClient(context.Background(), url.Host, &util.ConnectionOptions{
			OpenTracing: gocore.Config().GetBool("use_open_tracing", true),
			Prometheus:  gocore.Config().GetBool("use_prometheus_grpc_metrics", true),
			MaxRetries:  3,
		})
		if err != nil {
			return nil, err
		}

		apiClient := utxostore_api.NewUtxoStoreAPIClient(conn)
		return utxo.NewClient(apiClient)
	}
}
