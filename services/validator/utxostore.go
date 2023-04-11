package validator

import (
	"fmt"
	"net/url"
	"strconv"

	"github.com/TAAL-GmbH/ubsv/services/utxostore/utxostore_api"
	store "github.com/TAAL-GmbH/ubsv/services/validator/utxostore"
	"github.com/TAAL-GmbH/ubsv/services/validator/utxostore/aerospike"
	"github.com/TAAL-GmbH/ubsv/services/validator/utxostore/utxostore"
	"github.com/ordishs/go-utils"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func NewUTXOStore(logger utils.Logger, uri string) (store.UTXOStore, error) {
	parsedUri, err := url.Parse(uri)
	if err != nil {
		return nil, err
	}

	port, err := strconv.Atoi(parsedUri.Port())
	if err != nil {
		return nil, err
	}

	switch parsedUri.Scheme {
	case "aerospike":
		logger.Infof("[UTXOStore] connecting to aerospike at %s:%d", parsedUri.Hostname(), port)
		return aerospike.New(parsedUri.Hostname(), port, parsedUri.Path[1:])

	case "utxostore":
		logger.Infof("[UTXOStore] connecting to utxostore service at %s:%d", parsedUri.Hostname(), port)
		conn, err := grpc.Dial(parsedUri.Hostname()+":"+parsedUri.Port(), grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return nil, err
		}
		apiClient := utxostore_api.NewUtxoStoreAPIClient(conn)
		return utxostore.New(apiClient)
		//case "foundationdb":
		// TODO: Not working yet
		//	password, _ := parsedUri.User.Password()
		// logger.Infof("[UTXOStore] connecting to foundationdb service at %s:%d", parsedUri.Hostname(), port)
		//	return foundationdb.New(parsedUri.Hostname(), port, parsedUri.User.String(), password)
	}

	return nil, fmt.Errorf("unknown scheme: %s", parsedUri.Scheme)
}
