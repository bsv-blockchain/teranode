package main

import (
	"fmt"
	"os"

	aero "github.com/aerospike/aerospike-client-go/v7"
	"github.com/libsv/go-bt/v2/chainhash"
)

func main() {
	client, aeroErr := aero.NewClient("localhost", 3000)
	if aeroErr != nil {
		fmt.Printf("ERROR: %v\n", aeroErr)
		return
	}
	defer client.Close()

	if len(os.Args) < 2 {
		fmt.Printf("ERROR: txid required\n")
		return
	}

	txid, err := chainhash.NewHashFromStr(os.Args[1])
	if err != nil {
		fmt.Printf("ERROR: %v\n", err)
		return
	}

	key, err := aero.NewKey("test", "utxo", txid.CloneBytes())
	if err != nil {
		fmt.Printf("ERROR: %v\n", err)
		return
	}

	rec, err := client.Get(nil, key)
	if err != nil {
		fmt.Printf("ERROR: %v\n", err)
		return
	}

	for binName := range rec.Bins {
		bin := rec.Bins[binName]
		switch bin.(type) {
		case []interface{}:
			b := bin.([]interface{})
			t := "slice"
			fmt.Printf("%-15s [%-6s]: len(%d)\n", binName, t, len(b))
		default:
			t := fmt.Sprintf("%T", bin)
			fmt.Printf("%-15s [%-6s]: %v\n", binName, t, bin)
		}
	}
}
