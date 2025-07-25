// Package aerospikereader provides utilities for reading and displaying records from an Aerospike UTXO store.
// It is intended for debugging and inspection of UTXO data and related blockchain information.
//
// Usage:
//
//	This package is typically used as a command-line tool to fetch and print UTXO records
//	and their associated block information from an Aerospike database, using the provided settings.
//
// Functions:
//   - ReadAerospike: Reads and prints a UTXO record and related data for a given transaction ID.
//
// Side effects:
//
//	Functions in this package may print to stdout and exit the process if an error occurs.
package aerospikereader

import (
	"context"
	"fmt"
	"os"
	"sort"
	"strings"

	"math"
	"time"

	aero "github.com/aerospike/aerospike-client-go/v8"
	"github.com/bitcoin-sv/teranode/errors"
	"github.com/bitcoin-sv/teranode/settings"
	"github.com/bitcoin-sv/teranode/stores/blockchain"
	"github.com/bitcoin-sv/teranode/stores/utxo/fields"
	"github.com/bitcoin-sv/teranode/ulogger"
	"github.com/bitcoin-sv/teranode/util"
	"github.com/bitcoin-sv/teranode/util/uaerospike"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	safeconversion "github.com/bsv-blockchain/go-safe-conversion"
)

// ReadAerospike handles the Aerospike reader command logic.
// Parameters:
//
//	logger: ulogger.Logger for logging
//	settings: pointer to settings.Settings
//	txIDString: transaction ID string to read from the Aerospike store
//
// Side effects: prints record info to stdout, exits on error.
func ReadAerospike(logger ulogger.Logger, settings *settings.Settings, txIDString string) { //nolint:gocognit // this can be broken apart in the future
	// Print an empty line for better readability
	fmt.Println()

	// Check if txIDString is valid
	hash, err := chainhash.NewHashFromStr(txIDString)
	if err != nil {
		fmt.Printf("Invalid txid: %s\n", txIDString)
		os.Exit(1)
	}

	// Get UTXO store URL from settings
	storeURL := settings.UtxoStore.UtxoStore
	if storeURL == nil {
		fmt.Printf("Error reading utxostore setting\n")
		os.Exit(1)
	}

	// Parse the store URL for namespace and set name
	namespace := strings.TrimPrefix(storeURL.Path, "/")
	setName := storeURL.Query().Get("set")

	fmt.Printf("Reading record from %s.%s\n", namespace, setName)

	// Create the Aerospike key for the given transaction ID
	keySource := uaerospike.CalculateKeySource(hash, 0)

	var key *aero.Key
	key, err = aero.NewKey(namespace, setName, keySource)
	if err != nil {
		fmt.Printf("Failed to create key: %s\n", err)
		os.Exit(1)
	}

	fmt.Printf("Key         : %x\n", keySource)

	// Create Aerospike client
	var client *uaerospike.Client
	client, err = util.GetAerospikeClient(logger, storeURL, settings)
	if err != nil {
		fmt.Printf("Failed to connect to aerospike: %s\n", err)
		os.Exit(1)
	}

	// Get blockchain store URL from settings
	blockchainStoreURL := settings.BlockChain.StoreURL
	if blockchainStoreURL == nil {
		fmt.Printf("Error reading blockchain store setting\n")
		os.Exit(1)
	}

	// Create blockchain blockchainStore
	var blockchainStore blockchain.Store
	blockchainStore, err = blockchain.NewStore(logger, blockchainStoreURL, settings)
	if err != nil {
		fmt.Printf("Failed to create blockchain store: %s\n", err)
		os.Exit(1)
	}

	// Print the Aerospike record for the given key
	var record *aero.Record
	record, err = printAerospikeRecord(client, key, blockchainStore)
	if err != nil {
		fmt.Printf("Failed to print record: %s\n", err)
		os.Exit(1)
	}

	// Print the record's digest
	nrRecordsIfc, ok := record.Bins["totalExtraRecs"]
	if ok {
		var totalExtraRecords int
		totalExtraRecords, ok = nrRecordsIfc.(int)
		if ok {
			var nrRecordsUint32 uint32
			nrRecordsUint32, err = safeconversion.IntToUint32(totalExtraRecords)
			if err != nil {
				fmt.Printf("Failed to convert nrRecords to uint32: %s\n", err)
				os.Exit(1)
			}

			for i := uint32(1); i <= nrRecordsUint32; i++ {
				// Calculate the key source for the additional records
				keySource = uaerospike.CalculateKeySource(hash, i)

				// Create a new key for the additional record
				key, err = aero.NewKey(namespace, setName, keySource)
				if err != nil {
					fmt.Printf("Failed to create key: %s\n", err)
					os.Exit(1)
				}

				fmt.Printf("Record %d\n", i)
				fmt.Printf("-------\n")

				if _, err = printAerospikeRecord(client, key, blockchainStore); err != nil {
					fmt.Printf("Failed to print record %d: %s\n", i, err)
					os.Exit(1)
				}
			}
		}
	}
}

// formatExpiration formats the Aerospike expiration value into a human-readable string.
// Parameters:
//
//	expiration: The expiration value from the Aerospike record
//
// Returns: A formatted string showing the expiration value and its meaning
func formatExpiration(expiration uint32) string {
	switch expiration {
	case 0:
		return fmt.Sprintf("%d (Never expires)", expiration)
	case math.MaxUint32:
		// TTLDontExpire = -1 when cast to int32
		return fmt.Sprintf("%d (TTLDontExpire)", expiration)
	case math.MaxUint32 - 1:
		// TTLDontUpdate = -2 when cast to int32
		return fmt.Sprintf("%d (TTLDontUpdate)", expiration)
	default:
		// Positive values are Unix timestamps
		expirationTime := time.Unix(int64(expiration), 0).UTC()
		return fmt.Sprintf("%d (%s)", expiration, expirationTime.Format(time.RFC3339))
	}
}

// printAerospikeRecord prints an Aerospike record's details.
// Parameters:
//
//	client: Aerospike client
//	key: Aerospike key
//	blockchainStore: blockchain.Store for block lookups
//
// Returns: the Aerospike record and error if any.
func printAerospikeRecord(client *uaerospike.Client, key *aero.Key,
	blockchainStore blockchain.Store) (*aero.Record, error) {
	// Get the record from Aerospike using the provided key
	response, err := client.Get(nil, key)
	if err != nil {
		return nil, errors.NewError("Failed to get record", err)
	}

	// Print the record details
	fmt.Printf("Digest        : %x\n", response.Key.Digest())
	fmt.Printf("Namespace     : %s\n", response.Key.Namespace())
	fmt.Printf("SetName       : %s\n", response.Key.SetName())
	fmt.Printf("Node          : %s\n", response.Node.GetName())
	fmt.Println()
	fmt.Printf("Bins          :")

	// Loop through the bins in the response and print them
	var indent = false
	bins := make([]string, 0, len(response.Bins))
	for binName := range response.Bins {
		bins = append(bins, binName)
	}

	// Sort the bins
	sort.SliceStable(bins, func(i, j int) bool {
		return bins[i] < bins[j]
	})

	// Print the sorted bin names
	for _, binName := range bins {
		if indent {
			fmt.Printf("              : %s\n", binName)
		} else {
			fmt.Printf(" %s\n", binName)
		}

		indent = true
	}

	// Print the generation and expiration
	fmt.Println()
	fmt.Printf("Generation    : %d\n", response.Generation)
	fmt.Printf("Expiration    : %s\n", formatExpiration(response.Expiration))
	fmt.Println()

	// Loop through and print generation, expiration, and other bins
	for _, k := range bins {
		v := response.Bins[k]

		switch k {
		case "Generation":
			fallthrough
		case "Expiration":
			// These fields are already printed earlier in the function (lines 222-223),
			// so no additional processing is needed here.
		case fields.BlockIDs.String():
			printBlockIDs(response.Bins[fields.BlockIDs.String()], blockchainStore)
		case fields.Utxos.String():
			printUtxos(response.Bins[fields.Utxos.String()])
		case fields.ConflictingChildren.String():
			printConflictingChildren(response.Bins[fields.ConflictingChildren.String()])
		case fields.TxID.String():
			printTxID(response.Bins[fields.TxID.String()])

		default:
			var b []byte
			if arr, ok := v.([]interface{}); ok {
				printArray(k, arr)
			} else if b, ok = v.([]byte); ok {
				fmt.Printf("%-14s: %x\n", k, b)
			} else {
				fmt.Printf("%-14s: %v\n", k, v)
			}
		}
	}

	fmt.Println()

	return response, nil
}

// printArray prints an array bin from an Aerospike record.
// Parameters:
//
//	name: bin name
//	value: interface value (should be []interface{})
func printArray(name string, value interface{}) {
	fmt.Printf("%-14s:", name)

	if value == nil {
		fmt.Printf(" <nil>\n")
		return
	}

	arr, ok := value.([]interface{})
	if !ok {
		fmt.Printf(" <not array>\n")
		return
	}

	if len(arr) == 0 {
		fmt.Printf(" <empty>\n")
		return
	}

	for i, item := range arr {
		if b, found := item.([]byte); found {
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

// printUtxos prints UTXO values from an Aerospike record with special formatting.
// For values > 32 bytes, adds spaces after byte 32 and byte 64.
// Parameters:
//
//	name: bin name
//	value: interface value (should be []interface{})
func printUtxos(value interface{}) {
	fmt.Printf("%-14s:", fields.Utxos.String())

	if value == nil {
		fmt.Printf(" <nil>\n")
		return
	}

	arr, ok := value.([]interface{})
	if !ok {
		fmt.Printf(" <not array>\n")
		return
	}

	if len(arr) == 0 {
		fmt.Printf(" <empty>\n")
		return
	}

	for i, item := range arr {
		if b, found := item.([]byte); found {
			// Format the hex string with spaces after byte 32 and 64
			hexStr := formatUtxoHex(b)
			if i == 0 {
				fmt.Printf(" %-5d : %s\n", i, hexStr)
			} else {
				fmt.Printf("              : %-5d : %s\n", i, hexStr)
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

// formatUtxoHex formats a byte slice as hex with spaces after positions 32 and 64.
// For 32-byte values (hashes), it reverses the bytes from little-endian to big-endian.
func formatUtxoHex(b []byte) string {
	if len(b) == 32 {
		// Reverse 32-byte hash from little-endian to big-endian
		reversed := make([]byte, 32)
		for i := 0; i < 32; i++ {
			reversed[i] = b[31-i]
		}
		return fmt.Sprintf("%x", reversed)
	}

	if len(b) < 32 {
		return fmt.Sprintf("%x", b)
	}

	// For values > 32 bytes, reverse the first 32 bytes (hash part)
	reversed := make([]byte, 32)
	for i := 0; i < 32; i++ {
		reversed[i] = b[31-i]
	}
	result := fmt.Sprintf("%x", reversed)

	if len(b) > 32 && len(b) <= 64 {
		result += " " + fmt.Sprintf("%x", b[32:])
	} else if len(b) > 64 {
		result += " " + fmt.Sprintf("%x", b[32:64]) + " " + fmt.Sprintf("%x", b[64:])
	}

	return result
}

// printTxID prints a transaction ID in big-endian format (reversed from storage).
// Parameters:
//
//	value: interface value (should be []byte)
func printTxID(value interface{}) {
	fmt.Printf("%-14s:", fields.TxID.String())

	if value == nil {
		fmt.Printf(" <nil>\n")
		return
	}

	b, ok := value.([]byte)
	if !ok {
		fmt.Printf(" <not bytes>\n")
		return
	}

	if len(b) != 32 {
		fmt.Printf(" %x (invalid length: %d bytes)\n", b, len(b))
		return
	}

	// Reverse the 32-byte hash from little-endian to big-endian
	reversed := make([]byte, 32)
	for i := 0; i < 32; i++ {
		reversed[i] = b[31-i]
	}
	fmt.Printf(" %x\n", reversed)
}

// printConflictingChildren prints an array of conflicting transaction IDs in big-endian format.
// Parameters:
//
//	value: interface value (should be []interface{} containing []byte)
func printConflictingChildren(value interface{}) {
	fmt.Printf("%-14s:", fields.ConflictingChildren.String())

	if value == nil {
		fmt.Printf(" <nil>\n")
		return
	}

	arr, ok := value.([]interface{})
	if !ok {
		fmt.Printf(" <not array>\n")
		return
	}

	if len(arr) == 0 {
		fmt.Printf(" <empty>\n")
		return
	}

	for i, item := range arr {
		if b, found := item.([]byte); found {
			var hexStr string
			if len(b) == 32 {
				// Reverse 32-byte hash from little-endian to big-endian
				reversed := make([]byte, 32)
				for j := 0; j < 32; j++ {
					reversed[j] = b[31-j]
				}
				hexStr = fmt.Sprintf("%x", reversed)
			} else {
				hexStr = fmt.Sprintf("%x (invalid length: %d bytes)", b, len(b))
			}

			if i == 0 {
				fmt.Printf(" %-5d : %s\n", i, hexStr)
			} else {
				fmt.Printf("              : %-5d : %s\n", i, hexStr)
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

// printBlockIDs prints block IDs and their details from an Aerospike record.
// Parameters:
//
//	value: interface value (should be []interface{})
//	blockchainStore: blockchain.Store for block lookups
func printBlockIDs(value interface{}, blockchainStore blockchain.Store) { //nolint:gocognit // this can be broken apart in the future
	fmt.Printf("%-14s:", fields.BlockIDs.String())

	if value == nil {
		fmt.Printf(" <nil>\n")
		return
	}

	array, ok := value.([]interface{})
	if !ok {
		fmt.Printf(" <not array>\n")
		return
	}

	if len(array) == 0 {
		fmt.Printf(" <empty>\n")
		return
	}

	for i, item := range array {
		if b, found := item.([]byte); found {
			if i == 0 {
				fmt.Printf(" %-5d : %x\n", i, b)
			} else {
				fmt.Printf("                  : %-5d : %x\n", i, b)
			}
		} else {
			var blockID int
			blockID, ok = item.(int)
			if !ok {
				if i == 0 {
					fmt.Printf(" %-5d : %v\n", i, item)
				} else {
					fmt.Printf("                : %-5d : %v\n", i, item)
				}

				continue
			}

			// Get the block by ID
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
