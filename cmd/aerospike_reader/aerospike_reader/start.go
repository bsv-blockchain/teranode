package aerospike_reader

import (
	"fmt"
	"os"

	aero "github.com/aerospike/aerospike-client-go/v7"
	"github.com/bitcoin-sv/ubsv/ulogger"
	"github.com/bitcoin-sv/ubsv/util"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/ordishs/gocore"
)

func Start() {
	logger := ulogger.NewGoCoreLogger("aerospike_reader")

	storeURL, err, found := gocore.Config().GetURL("utxostore")
	if err != nil {
		logger.Errorf("[Asset_http] GetUTXOsByTXID error: %s", err.Error())
		panic(err)
	}
	if !found {
		logger.Errorf("[Asset_http] GetUTXOsByTXID error: no utxostore setting found")
		panic("no utxostore setting found")
	}

	client, err := util.GetAerospikeClient(logger, storeURL)
	if err != nil {
		logger.Errorf("[Asset_http] GetUTXOsByTXID error: %s", err.Error())
		panic(err.Error())
	}

	namespace := storeURL.Path[1:]
	setName := storeURL.Query().Get("set")
	if setName == "" {
		setName = "txmeta"
	}

	txid := os.Args[1]

	hash, err := chainhash.NewHashFromStr(txid)
	if err != nil {
		panic(err)
	}

	// get transaction meta data
	key, err := aero.NewKey(namespace, setName, hash[:])
	if err != nil {
		logger.Errorf("[Asset_http] GetUTXOsByTXID error creating key: %s", err.Error())
		panic(err)
	}

	response, err := client.Get(nil, key)
	if err != nil {
		panic(err)
	}

	fmt.Printf("Key:        %x\n", response.Key)
	fmt.Printf("Digest:     %x\n", response.Key.Digest())
	fmt.Printf("Namespace:  %s\n", response.Key.Namespace())
	fmt.Printf("SetName:    %s\n", response.Key.SetName())
	fmt.Printf("Node:       %s\n", response.Node.GetName())
	fmt.Printf("Bins:\n")
	for _, bin := range response.Bins {
		fmt.Printf("             %s\n", bin)
	}
	fmt.Printf("Generation: %s\n", response.Generation)
	fmt.Printf("Expiration: %s\n", response.Expiration)

	fmt.Println()

	for k, v := range response.Bins {
		switch k {
		case "tx":
			fallthrough
		case "inputs":
			fallthrough
		case "outputs":
			fallthrough
		case "utxos":
			fmt.Printf("%-11s: %x\n", k, v.([]byte))
		case "parentTxHashes":
			fmt.Printf("%-11s: %x\n", k, v.([]byte))
		default:
			fmt.Printf("%-11s: %+v\n", k, v)
		}
	}

	fmt.Println()
}
