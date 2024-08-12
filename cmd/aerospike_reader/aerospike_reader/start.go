package aerospike_reader

import (
	"context"
	"os"

	"github.com/bitcoin-sv/ubsv/stores/utxo"
	utxofactory "github.com/bitcoin-sv/ubsv/stores/utxo/_factory"
	"github.com/bitcoin-sv/ubsv/ulogger"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/ordishs/gocore"
)

func Start() {
	ctx := context.Background()
	logger := ulogger.NewGoCoreLogger("aerospike_reader")

	utxoStore := getUtxoStore(ctx, logger)

	txid := os.Args[1]

	hash, err := chainhash.NewHashFromStr(txid)
	if err != nil {
		panic(err)
	}

	tx, err := utxoStore.Get(ctx, hash)
	if err != nil {
		panic(err)
	}

	logger.Infof("tx: %#v", tx)
}

func getUtxoStore(ctx context.Context, logger ulogger.Logger) utxo.Store {
	utxoStoreURL, err, found := gocore.Config().GetURL("utxostore")
	if err != nil {
		panic(err)
	}
	if !found {
		panic("no utxostore setting found")
	}
	utxoStore, err := utxofactory.NewStore(ctx, logger, utxoStoreURL, "aerospike_reader", false) // false to not start blockchain listener
	if err != nil {
		panic(err)
	}

	return utxoStore
}
