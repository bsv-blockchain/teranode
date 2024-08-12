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

	fmt.Printf("Digest:     %x\n", response.Key.Digest())
	fmt.Printf("Namespace:  %s\n", response.Key.Namespace())
	fmt.Printf("SetName:    %s\n", response.Key.SetName())
	fmt.Printf("Node:       %s\n", response.Node.GetName())
	fmt.Printf("Bins:\n")
	for binName := range response.Bins {
		fmt.Printf("            %v\n", binName)
	}
	fmt.Printf("Generation: %d\n", response.Generation)
	fmt.Printf("Expiration: %d\n", response.Expiration)

	fmt.Println()

	for k, v := range response.Bins {
		switch k {
		case "Generation":
			fallthrough
		case "Expiration":
			fallthrough
		case "inputs":
			fallthrough
		case "outputs":
			fallthrough
		case "utxos":
			// IGNORE

		default:
			if arr, ok := v.([]interface{}); ok {
				fmt.Printf("%-11s:\n", k)

				for i, item := range arr {
					if b, ok := item.([]byte); ok {
						fmt.Printf("  %-11d: %x\n", i, b)
					} else {
						fmt.Printf("  %-11d: %v\n", i, item)
					}
				}
			} else if b, ok := v.([]byte); ok {
				fmt.Printf("%-11s: %x\n", k, b)
			} else {
				fmt.Printf("%-11s: %v\n", k, v)
			}
		}
	}

	fmt.Printf("inputs:\n")
	for i, input := range response.Bins["inputs"].([]interface{}) {
		fmt.Printf("  %-11d: %x\n", i, input.([]byte))
	}

	fmt.Printf("outputs:\n")
	for i, output := range response.Bins["outputs"].([]interface{}) {
		fmt.Printf("  %-11d: %x\n", i, output.([]byte))
	}

	fmt.Printf("utxos:\n")
	for i, utxo := range response.Bins["utxos"].([]interface{}) {
		fmt.Printf("  %-11d: %x\n", i, utxo.([]byte))
	}

	fmt.Println()
}
