package main

import (
	"context"
	"os"
	"strconv"

	"github.com/bitcoin-sv/ubsv/stores/utxo/_factory"
	"github.com/bitcoin-sv/ubsv/ulogger"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/ordishs/gocore"
)

func main() {
	ctx := context.Background()
	logger := ulogger.New("utxo_set_tx_mined")

	if len(os.Args) < 3 {
		panic("Usage: utxo_set_tx_mined <txid> <blockID>")
	}

	hashHex := os.Args[1]

	hash, err := chainhash.NewHashFromStr(hashHex)
	if err != nil {
		panic(err)
	}

	// get the block ID from the command line
	blockIDStr := os.Args[2]

	blockID, err := strconv.ParseUint(blockIDStr, 10, 32)
	if err != nil {
		panic(err)
	}

	if blockID == 0 {
		panic("blockID must be greater than 0")
	}

	storeUrl, err, found := gocore.Config().GetURL("utxostore")
	if err != nil {
		panic(err)
	}

	if !found {
		panic("utxo_store not found in config")
	}

	utxoStore, err := _factory.NewStore(ctx, logger, storeUrl, "utxo_set_tx_mined", false)
	if err != nil {
		panic(err)
	}

	logger.Infof("Setting mined state for tx %s: %d", hash.String(), blockID)

	if err = utxoStore.SetMinedMulti(ctx, []*chainhash.Hash{hash}, uint32(blockID)); err != nil {
		panic(err)
	}

	logger.Infof("blockID set successfully")
}
