package main

import (
	"fmt"
	"log"
	"os"

	aero "github.com/aerospike/aerospike-client-go/v7"
	"github.com/libsv/go-bt/v2/chainhash"
)

func main() {
	client, aeroErr := aero.NewClient("localhost", 3000)
	if aeroErr != nil {
		log.Printf("ERROR: %v", aeroErr)
		return
	}
	defer client.Close()

	if len(os.Args) < 2 {
		log.Printf("ERROR: txid required")
		return
	}

	txid, err := chainhash.NewHashFromStr(os.Args[1])
	if err != nil {
		log.Printf("ERROR: %v", err)
		return
	}

	key, err := aero.NewKey("test", "utxo", txid.CloneBytes())
	if err != nil {
		log.Printf("ERROR: %v", err)
		return
	}

	rec, err := client.Get(nil, key)
	if err != nil {
		log.Printf("ERROR: %v", err)
		return
	}

	for binName := range rec.Bins {
		bin := rec.Bins[binName]
		switch bin.(type) {
		case []interface{}:
			b := bin.([]interface{})
			t := "slice"
			log.Printf("%-15s [%-6s]: %T", binName, t, len(b))
		default:
			t := fmt.Sprintf("%T", bin)
			log.Printf("%-15s [%-6s]: %v", binName, t, bin)
		}
	}
}
