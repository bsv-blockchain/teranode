package main

import (
	"log"

	aero "github.com/aerospike/aerospike-client-go/v7"
	"github.com/bitcoin-sv/ubsv/util"
	"github.com/bitcoin-sv/ubsv/util/usql"
	_ "github.com/lib/pq"
	"github.com/libsv/go-bt/v2"
)

func main() {
	client, aeroErr := aero.NewClient("localhost", 3000)
	if aeroErr != nil {
		log.Printf("ERROR: %v", aeroErr)
		return
	}

	defer client.Close()

	policy := util.GetAerospikeReadPolicy()

	db, err := usql.Open("postgres", "user=ubsv password=ubsv dbname=ubsv sslmode=disable host=localhost port=5432")
	if err != nil {
		log.Printf("ERROR: %v", err)
		return
	}
	defer db.Close()

	q := "SELECT height, coinbase_tx FROM blocks ORDER BY height"

	rows, err := db.Query(q)
	if err != nil {
		log.Printf("ERROR: %v", err)
		return
	}

	for rows.Next() {
		var height int
		var b []byte

		err = rows.Scan(&height, &b)
		if err != nil {
			log.Printf("ERROR: %v", err)
			return
		}

		if height == 0 {
			log.Printf("Height %d skipped", height)
			continue
		}

		coinbaseTx, err := bt.NewTxFromBytes(b)
		if err != nil {
			log.Printf("ERROR: %v", err)
			return
		}

		key, err := aero.NewKey("test", "utxo", coinbaseTx.TxIDChainHash().CloneBytes())
		if err != nil {
			log.Printf("ERROR: %v", err)
			return
		}

		rec, err := client.Get(policy, key, "spendingHeight")
		if err != nil {
			log.Printf("ERROR: %v", err)
			return
		}

		spendingHeight, ok := rec.Bins["spendingHeight"].(int)
		if !ok {
			log.Printf("ERROR: %v", err)
			return
		}

		expectedHeight := height + 100

		if spendingHeight != expectedHeight {
			log.Printf("Height %d: %v - expected %d, actual %d", height, coinbaseTx.TxIDChainHash(), expectedHeight, spendingHeight)

			newBin := aero.NewBin("spendingHeight", aero.NewIntegerValue(expectedHeight))
			err := client.PutBins(nil, key, newBin)
			if err != nil {
				log.Printf("ERROR: %v", err)
				return
			}
		}
	}
}
