package main

import (
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

	for bin := range rec.Bins {
		log.Printf("Bin: %s", bin)
	}

	spendingHeight, ok := rec.Bins["spendingHeight"].(int)
	if !ok {
		log.Printf("ERROR: spendingHeight not found")
		return
	}

	log.Printf("%+v", spendingHeight)
}
