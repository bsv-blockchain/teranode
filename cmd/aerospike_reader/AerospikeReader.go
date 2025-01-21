package aerospike_reader

import (
	"fmt"
	"os"

	aero "github.com/aerospike/aerospike-client-go/v7"
	"github.com/bitcoin-sv/teranode/errors"
	"github.com/bitcoin-sv/teranode/settings"
	"github.com/bitcoin-sv/teranode/ulogger"
	"github.com/bitcoin-sv/teranode/util"
	"github.com/bitcoin-sv/teranode/util/uaerospike"
	"github.com/libsv/go-bt/v2/chainhash"
)

// AerospikeReader handles the aerospike reader command logic
func AerospikeReader(txidStr string) {
	logger := ulogger.NewGoCoreLogger("aerospike_reader", ulogger.WithLevel("WARN"))
	tSettings := settings.NewSettings()

	fmt.Println()

	hash, err := chainhash.NewHashFromStr(txidStr)
	if err != nil {
		fmt.Printf("Invalid txid: %s\n", txidStr)
		os.Exit(1)
	}

	storeURL := tSettings.UtxoStore.UtxoStore
	if storeURL == nil {
		fmt.Printf("error reading utxostore setting\n")
		os.Exit(1)
	}

	namespace := storeURL.Path[1:]
	setName := storeURL.Query().Get("set")

	fmt.Printf("Reading record from %s.%s\n", namespace, setName)

	keySource := uaerospike.CalculateKeySource(hash, 0)

	key, err := aero.NewKey(namespace, setName, keySource)
	if err != nil {
		fmt.Printf("Failed to create key: %s\n", err)
		os.Exit(1)
	}

	fmt.Printf("Key         : %x\n", keySource)

	client, err := util.GetAerospikeClient(logger, storeURL, tSettings)
	if err != nil {
		fmt.Printf("Failed to connect to aerospike: %s\n", err)
		os.Exit(1)
	}

	record, err := printRecord(client, key)
	if err != nil {
		fmt.Printf("Failed to print record: %s\n", err)
		os.Exit(1)
	}

	nrRecordsIfc, ok := record.Bins["nrRecords"]
	if ok {
		nrRecords, ok := nrRecordsIfc.(int)
		if ok {
			// nolint: gosec
			for i := uint32(1); i < uint32(nrRecords); i++ {
				keySource := uaerospike.CalculateKeySource(hash, i)

				key, err := aero.NewKey(namespace, setName, keySource)
				if err != nil {
					fmt.Printf("Failed to create key: %s\n", err)
					os.Exit(1)
				}

				fmt.Printf("Record %d\n", i)
				fmt.Printf("-------\n")

				if _, err := printRecord(client, key); err != nil {
					fmt.Printf("Failed to print record %d: %s\n", i, err)
					os.Exit(1)
				}
			}
		}
	}
}

func printRecord(client *uaerospike.Client, key *aero.Key) (*aero.Record, error) {
	response, err := client.Get(nil, key)
	if err != nil {
		return nil, errors.NewError("Failed to get record", err)
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

	printArray("inputs", response.Bins["inputs"])
	printArray("outputs", response.Bins["outputs"])
	printArray("utxos", response.Bins["utxos"])

	fmt.Println()

	return response, nil
}

func printArray(name string, ifc interface{}) {
	fmt.Printf("%-12s:", name)

	if ifc == nil {
		fmt.Printf(" <nil>\n")
		return
	}

	arr, ok := ifc.([]interface{})
	if !ok {
		fmt.Printf(" <not array>\n")
		return
	}

	if len(arr) == 0 {
		fmt.Printf(" <empty>\n")
		return
	}

	for i, item := range arr {
		if b, ok := item.([]byte); ok {
			if i == 0 {
				fmt.Printf(" %-5d : %x\n", i, b)
			} else {
				fmt.Printf("            : %-5d : %x\n", i, b)
			}
		} else {
			if i == 0 {
				fmt.Printf(" %-5d : %v\n", i, item)
			} else {
				fmt.Printf("            : %-5d : %v\n", i, item)
			}
		}
	}
}
