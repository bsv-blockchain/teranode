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

	fmt.Printf("Digest      : %x\n", response.Key.Digest())
	fmt.Printf("Namespace   : %s\n", response.Key.Namespace())
	fmt.Printf("SetName     : %s\n", response.Key.SetName())
	fmt.Printf("Node        : %s\n", response.Node.GetName())
	fmt.Printf("Bins        :")
	var indent = false
	for binName := range response.Bins {
		if indent {
			fmt.Printf("            : %s\n", binName)
		} else {
			fmt.Printf(" %s\n", binName)
		}
		indent = true
	}
	fmt.Printf("Generation  : %d\n", response.Generation)
	fmt.Printf("Expiration  : %d\n", response.Expiration)

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
				printArray(k, arr)
			} else if b, ok := v.([]byte); ok {
				fmt.Printf("%-12s: %x\n", k, b)
			} else {
				fmt.Printf("%-12s: %v\n", k, v)
			}
		}
	}

	printArray("inputs", response.Bins["inputs"].([]interface{}))
	printArray("outputs", response.Bins["outputs"].([]interface{}))
	printArray("utxos", response.Bins["utxos"].([]interface{}))

	fmt.Println()
}

func printArray(name string, arr []interface{}) {
	fmt.Printf("%-12s:", name)

	indent := false

	for i, item := range arr {
		if b, ok := item.([]byte); ok {
			if indent {
				fmt.Printf("            : %5d : %x\n", i, b)
			} else {
				fmt.Printf(" %5d : %x\n", i, b)
			}
		} else {
			if indent {
				fmt.Printf("            : %5d : %v\n", i, item)
			} else {
				fmt.Printf(" %5d : %v\n", i, item)
			}
		}
	}
}
