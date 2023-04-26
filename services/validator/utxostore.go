package validator

import (
	"context"
	"fmt"
	"net/url"
	"strconv"

	"github.com/TAAL-GmbH/ubsv/services/utxo"
	store "github.com/TAAL-GmbH/ubsv/services/utxo/store"
	"github.com/TAAL-GmbH/ubsv/services/utxo/store/aerospike"
	"github.com/TAAL-GmbH/ubsv/services/utxo/store/foundationdb"
	"github.com/TAAL-GmbH/ubsv/services/utxo/utxostore_api"
	"github.com/ordishs/go-utils"
	"github.com/ordishs/gocore"
)

func NewUTXOStore(logger utils.Logger, url *url.URL) (store.UTXOStore, error) {
	port, err := strconv.Atoi(url.Port())
	if err != nil {
		return nil, err
	}

	switch url.Scheme {
	case "aerospike":
		logger.Infof("[UTXOStore] connecting to aerospike at %s:%d", url.Hostname(), port)
		if len(url.Path) < 1 {
			return nil, fmt.Errorf("aerospike namespace not found")
		}
		return aerospike.New(url.Hostname(), port, url.Path[1:])

	case "memory":
		logger.Infof("[UTXOStore] connecting to utxostore service at %s:%d", url.Hostname(), port)
		// conn, err := grpc.Dial(url.Host, grpc.WithTransportCredentials(insecure.NewCredentials()))
		// if err != nil {
		// 	return nil, err
		// }

		conn, err := utils.GetGRPCClient(context.Background(), url.Host, &utils.ConnectionOptions{
			OpenTracing: gocore.Config().GetBool("use_open_tracing", true),
		})
		if err != nil {
			return nil, err
		}

		apiClient := utxostore_api.NewUtxoStoreAPIClient(conn)
		return utxo.NewClient(apiClient)

	case "foundationdb":
		password, _ := url.User.Password()
		logger.Infof("[UTXOStore] connecting to foundationdb service at %s:%d", url.Hostname(), port)
		return foundationdb.New(url.Hostname(), port, url.User.String(), password)
	}

	return nil, fmt.Errorf("unknown scheme: %s", url.Scheme)
}
