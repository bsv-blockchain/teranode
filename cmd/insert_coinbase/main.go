package main

import (
	"context"
	utxostore "github.com/bitcoin-sv/ubsv/stores/txmeta/aerospikemap"
	"github.com/bitcoin-sv/ubsv/ulogger"
	"github.com/libsv/go-bt/v2"
	url2 "net/url"
)

func main() {
	logger := ulogger.New("validator")
	url, err := url2.Parse("aerospikemap://editor:password1234@aerospike-0.ubsv.internal:3000/utxo-store?set=utxo&WarmUp=0&ConnectionQueueSize=16&LimitConnectionsToQueueSize=true&MinConnectionsPerNode=16&expiration=21600")
	if err != nil {
		panic(err)
	}

	utxoStore, err := utxostore.New(logger, url)
	if err != nil {
		panic(err)
	}

	tx, err := bt.NewTxFromString("01000000010000000000000000000000000000000000000000000000000000000000000000ffffffff18030910002f6d352d6363312fdcce95f3c057431c486ae662ffffffff0a0065cd1d000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac0065cd1d000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac0065cd1d000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac0065cd1d000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac0065cd1d000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac0065cd1d000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac0065cd1d000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac0065cd1d000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac0065cd1d000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac0065cd1d000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac00000000")

	if _, err = utxoStore.Create(context.Background(), tx, 4105+100); err != nil {
		// error will be handled below
		logger.Errorf("[SubtreeProcessor] error storing coinbase utxos: %v", err)
	}
}
