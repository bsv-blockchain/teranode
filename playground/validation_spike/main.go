package main

import (
	"context"
	"log"
	"net/http"
	_ "net/http/pprof"
	"time"

	"github.com/TAAL-GmbH/ubsv/services/utxo/store"
	"github.com/TAAL-GmbH/ubsv/services/validator"
	"github.com/libsv/go-bt/v2"
	"github.com/libsv/go-p2p/chaincfg/chainhash"
)

type NullStore struct{}

func (ns *NullStore) DeleteSpends(deleteSpends bool) {
	// No nothing
}

func (ns *NullStore) Get(ctx context.Context, hash *chainhash.Hash) (*store.UTXOResponse, error) {
	// fmt.Printf("Get(%s)\n", hash.String())
	return nil, nil
}

func (ns *NullStore) Store(ctx context.Context, hash *chainhash.Hash) (*store.UTXOResponse, error) {
	// fmt.Printf("Store(%s)\n", hash.String())
	return nil, nil
}

func (ns *NullStore) Spend(ctx context.Context, hash *chainhash.Hash, txID *chainhash.Hash) (*store.UTXOResponse, error) {
	// fmt.Printf("Spend(%s, %s)\n", hash.String(), txID.String())
	return &store.UTXOResponse{
		Status:       0,
		SpendingTxID: txID,
	}, nil
}

func (ns *NullStore) Reset(ctx context.Context, hash *chainhash.Hash) (*store.UTXOResponse, error) {
	// fmt.Printf("Reset(%s)\n", hash.String())
	return nil, nil
}

func main() {
	// tx, err := bt.NewTxFromString("0100000001f3f0d33a5c5afd524043762f8b812999caa5a225e6e20ecdb71a7e0e1c207b43530000006a473044022049e20908f21bdcb901b5c5a9a93b238446606267e19db4e662df1a7c4a5bae08022036960a340515e2cfee79b9c194093f24f253d4243bf9d0baa97352983e2263fa412102a98c1a3be041da2591761fbef4b2ab0f147aef36c308aee66df0b9825218de23ffffffff01000000000000000070006a0963657274696861736822314c6d763150594d70387339594a556e374d3948565473446b64626155386b514e4a403263333934306361313334353331373035326334346630613861636362323162323165633131386465646330396538643764393064323166333935663063613000000000")
	// if err != nil {
	// 	panic(err)
	// }

	// tx.Inputs[0].PreviousTxSatoshis = 16

	// s, err := bscript.NewFromHexString("76a914a8d6bd6648139d95dac35d411c592b05bc0973aa88ac")
	// if err != nil {
	// 	panic(err)
	// }

	// tx.Inputs[0].PreviousTxScript = s

	// fmt.Printf("%x", tx.ExtendedBytes())

	go func() {
		log.Println(http.ListenAndServe("localhost:6060", nil))
	}()

	tx, err := bt.NewTxFromString("010000000000000000ef01f3f0d33a5c5afd524043762f8b812999caa5a225e6e20ecdb71a7e0e1c207b43530000006a473044022049e20908f21bdcb901b5c5a9a93b238446606267e19db4e662df1a7c4a5bae08022036960a340515e2cfee79b9c194093f24f253d4243bf9d0baa97352983e2263fa412102a98c1a3be041da2591761fbef4b2ab0f147aef36c308aee66df0b9825218de23ffffffff10000000000000001976a914a8d6bd6648139d95dac35d411c592b05bc0973aa88ac01000000000000000070006a0963657274696861736822314c6d763150594d70387339594a556e374d3948565473446b64626155386b514e4a403263333934306361313334353331373035326334346630613861636362323162323165633131386465646330396538643764393064323166333935663063613000000000")
	if err != nil {
		panic(err)
	}

	log.Printf("TXID: %x", tx.TxIDBytes())

	ns := &NullStore{}

	v := validator.New(ns)

	start := time.Now()

	for i := 0; i < 1_000_000; i++ {
		if err := v.Validate(context.Background(), tx); err != nil {
			log.Printf("ERROR: %v\n", err)
		}
	}

	log.Printf("Valid in %s\n", time.Since(start))

}
