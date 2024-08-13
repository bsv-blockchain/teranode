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
		fmt.Printf("error reading utxostore setting: %s\n", err)
		os.Exit(1)
	}
	if !found {
		fmt.Printf("missing utxostore setting\n")
		os.Exit(1)
	}

	client, err := util.GetAerospikeClient(logger, storeURL)
	if err != nil {
		fmt.Printf("Failed to connect to aerospike: %s\n", err)
		os.Exit(1)
	}

	namespace := storeURL.Path[1:]
	setName := storeURL.Query().Get("set")
	if setName == "" {
		setName = "txmeta"
	}

	fmt.Println()

	if len(os.Args) != 2 {
		fmt.Printf("Usage: %s <txid>\n", os.Args[0])
		os.Exit(1)
	}

	txid := os.Args[1]

	hash, err := chainhash.NewHashFromStr(txid)
	if err != nil {
		fmt.Printf("Invalid txid: %s\n", txid)
		os.Exit(1)
	}

	// get transaction meta data
	key, err := aero.NewKey(namespace, setName, hash[:])
	if err != nil {
		fmt.Printf("Failed to create key: %s\n", err)
		os.Exit(1)
	}

	response, err := client.Get(nil, key)
	if err != nil {
		fmt.Printf("Failed to get record: %s\n", err)
		os.Exit(1)
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

	for i, item := range arr {
		if b, ok := item.([]byte); ok {
			if i == 0 {
				fmt.Printf(" %5d : %x\n", i, b)
			} else {
				fmt.Printf("            : %5d : %x\n", i, b)
			}
		} else {
			if i == 0 {
				fmt.Printf(" %5d : %v\n", i, item)
			} else {
				fmt.Printf("            : %5d : %v\n", i, item)
			}
		}
	}
}
