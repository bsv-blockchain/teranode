package aerospikereader

import (
	"context"
	"fmt"
	"os"
	"sort"

	aero "github.com/aerospike/aerospike-client-go/v8"
	"github.com/bitcoin-sv/teranode/errors"
	"github.com/bitcoin-sv/teranode/settings"
	"github.com/bitcoin-sv/teranode/stores/blockchain"
	"github.com/bitcoin-sv/teranode/stores/utxo/fields"
	"github.com/bitcoin-sv/teranode/ulogger"
	"github.com/bitcoin-sv/teranode/util"
	"github.com/bitcoin-sv/teranode/util/uaerospike"
	"github.com/libsv/go-bt/v2/chainhash"
)

// AerospikeReader handles the aerospike reader command logic
func AerospikeReader(logger ulogger.Logger, tSettings *settings.Settings, txidStr string) {
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

	// Get blockchain store URL from settings
	blockchainStoreURL := tSettings.BlockChain.StoreURL
	if blockchainStoreURL == nil {
		fmt.Printf("error reading blockchain store setting\n")
		os.Exit(1)
	}

	// Create blockchain blockchainStore
	blockchainStore, err := blockchain.NewStore(logger, blockchainStoreURL, tSettings)
	if err != nil {
		fmt.Printf("Failed to create blockchain store: %s\n", err)
		os.Exit(1)
	}

	record, err := printRecord(client, key, blockchainStore)
	if err != nil {
		fmt.Printf("Failed to print record: %s\n", err)
		os.Exit(1)
	}

	nrRecordsIfc, ok := record.Bins["totalExtraRecs"]
	if ok {
		totalExtraRecs, ok := nrRecordsIfc.(int)
		if ok {
			nrRecordsUint32, err := util.SafeIntToUint32(totalExtraRecs)
			if err != nil {
				fmt.Printf("Failed to convert nrRecords to uint32: %s\n", err)
				os.Exit(1)
			}

			for i := uint32(1); i <= nrRecordsUint32; i++ {
				keySource := uaerospike.CalculateKeySource(hash, i)

				key, err := aero.NewKey(namespace, setName, keySource)
				if err != nil {
					fmt.Printf("Failed to create key: %s\n", err)
					os.Exit(1)
				}

				fmt.Printf("Record %d\n", i)
				fmt.Printf("-------\n")

				if _, err := printRecord(client, key, blockchainStore); err != nil {
					fmt.Printf("Failed to print record %d: %s\n", i, err)
					os.Exit(1)
				}
			}
		}
	}
}

func printRecord(client *uaerospike.Client, key *aero.Key, blockchainStore blockchain.Store) (*aero.Record, error) {
	response, err := client.Get(nil, key)
	if err != nil {
		return nil, errors.NewError("Failed to get record", err)
	}

	fmt.Printf("Digest        : %x\n", response.Key.Digest())
	fmt.Printf("Namespace     : %s\n", response.Key.Namespace())
	fmt.Printf("SetName       : %s\n", response.Key.SetName())
	fmt.Printf("Node          : %s\n", response.Node.GetName())

	fmt.Println()

	fmt.Printf("Bins          :")

	var indent = false

	bins := make([]string, 0, len(response.Bins))

	for binName := range response.Bins {
		bins = append(bins, binName)
	}

	sort.SliceStable(bins, func(i, j int) bool {
		return bins[i] < bins[j]
	})

	for _, binName := range bins {
		if indent {
			fmt.Printf("              : %s\n", binName)
		} else {
			fmt.Printf(" %s\n", binName)
		}

		indent = true
	}

	fmt.Println()

	fmt.Printf("Generation    : %d\n", response.Generation)
	fmt.Printf("Expiration    : %d\n", response.Expiration)

	fmt.Println()

	for _, k := range bins {
		v := response.Bins[k]

		switch k {
		case "Generation":
			fallthrough
		case "Expiration":

		case fields.BlockIDs.String():
			printBlockIDs(response.Bins[fields.BlockIDs.String()], blockchainStore)

		default:
			if arr, ok := v.([]interface{}); ok {
				printArray(k, arr)
			} else if b, ok := v.([]byte); ok {
				fmt.Printf("%-14s: %x\n", k, b)
			} else {
				fmt.Printf("%-14s: %v\n", k, v)
			}
		}
	}

	fmt.Println()

	return response, nil
}

func printArray(name string, ifc interface{}) {
	fmt.Printf("%-14s:", name)

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
				fmt.Printf("              : %-5d : %x\n", i, b)
			}
		} else {
			if i == 0 {
				fmt.Printf(" %-5d : %v\n", i, item)
			} else {
				fmt.Printf("              : %-5d : %v\n", i, item)
			}
		}
	}
}

func printBlockIDs(ifc interface{}, blockchainStore blockchain.Store) {
	fmt.Printf("%-14s:", fields.BlockIDs.String())

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
				fmt.Printf("                  : %-5d : %x\n", i, b)
			}
		} else {
			blockID, ok := item.(int)
			if !ok {
				if i == 0 {
					fmt.Printf(" %-5d : %v\n", i, item)
				} else {
					fmt.Printf("                : %-5d : %v\n", i, item)
				}

				continue
			}

			// Get block by ID
			ctx := context.Background()

			block, err := blockchainStore.GetBlockByID(ctx, uint64(blockID)) //nolint:gosec
			if err != nil {
				if i == 0 {
					fmt.Printf(" %-5d : %v (error getting block)\n", i, blockID)
				} else {
					fmt.Printf("              : %-5d : %v (error getting block)\n", i, blockID)
				}

				continue
			}

			// Print block ID and hash
			if i == 0 {
				fmt.Printf(" %-5d : %v (%v [%d])\n", i, blockID, block.Hash(), block.Height)
			} else {
				fmt.Printf("              : %-5d : %v (%v [%d])\n", i, blockID, block.Hash(), block.Height)
			}
		}
	}
}
