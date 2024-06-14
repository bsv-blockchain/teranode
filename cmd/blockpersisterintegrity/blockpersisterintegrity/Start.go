package blockpersisterintegrity

import (
	"context"
	"flag"
	"os"
	"strings"

	"github.com/bitcoin-sv/ubsv/model"
	"github.com/bitcoin-sv/ubsv/stores/blob"
	blockchain_store "github.com/bitcoin-sv/ubsv/stores/blockchain"

	// utxostore_factory "github.com/bitcoin-sv/ubsv/stores/utxo/_factory"
	"github.com/bitcoin-sv/ubsv/ulogger"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/ordishs/gocore"
)

type BlockSubtree struct {
	Block   chainhash.Hash
	Subtree chainhash.Hash
	Index   int
}

func Start() {
	path, err := os.Getwd()
	if err != nil {
		panic(err)
	}

	// check whether path contains cmd/chainintegrity or not
	if strings.Contains(path, "cmd/blockpersisterintegrity") {
		if err = os.Chdir("../../"); err != nil {
			panic(err)
		}
	}

	debug := flag.Bool("debug", true, "enable debug logging")
	// logfile := flag.String("logfile", "blockpersisterintegrity.log", "path to logfile")
	flag.Parse()

	debugLevel := "INFO"
	if *debug {
		debugLevel = "DEBUG"
	}
	// var logger = ulogger.New("blockpersisterintegrity", ulogger.WithLevel(debugLevel), ulogger.WithLoggerType("file"), ulogger.WithFilePath(*logfile))
	var logger = ulogger.New("blockpersisterintegrity", ulogger.WithLevel(debugLevel))

	blockchainStoreURL, err, found := gocore.Config().GetURL("blockchain_store")
	if err != nil {
		panic(err.Error())
	}
	if !found {
		panic("no blockchain_store setting found")
	}

	blockchainDB, err := blockchain_store.NewStore(logger, blockchainStoreURL)
	if err != nil {
		panic(err)
	}

	blockStoreUrl, err, found := gocore.Config().GetURL("blockstore")
	if err != nil {
		panic(err)
	}
	if !found {
		panic("blockstore config not found")
	}
	blobStore, err := blob.NewStore(logger, blockStoreUrl)
	if err != nil {
		panic(err)
	}

	// utxoStoreURL, err, found := gocore.Config().GetURL("utxostore")
	// if err != nil {
	// 	panic(err)
	// }
	// if !found {
	// 	panic("utxostore config not found")
	// }
	// utxoStore, err := utxostore_factory.NewStore(context.Background(), logger, utxoStoreURL, "main", false)
	// if err != nil {
	// 	panic(err)
	// }

	// ------------------------------------------------------------------------------------------------
	// start the actual tests
	// ------------------------------------------------------------------------------------------------

	ctx := context.Background()

	// get best block header
	bestBlockHeader, _, err := blockchainDB.GetBestBlockHeader(ctx)
	if err != nil {
		panic(err)
	}

	// get all block headers
	blockHeaders, blockMetas, err := blockchainDB.GetBlockHeaders(ctx, bestBlockHeader.Hash(), 100000)
	if err != nil {
		panic(err)
	}

	logger.Infof("found %d block headers", len(blockHeaders))

	var previousBlockHeader *model.BlockHeader
	for _, blockHeader := range blockHeaders {
		logger.Debugf("checking block header %s", blockHeader)
		if previousBlockHeader != nil {
			if !previousBlockHeader.HashPrevBlock.IsEqual(blockHeader.Hash()) {
				logger.Errorf("block header %s does not match previous block header %s", blockHeader.Hash(), previousBlockHeader.HashPrevBlock)
			}
		}
		previousBlockHeader = blockHeader
	}

	bp := NewBlockProcessor(logger, blobStore)

	// range through the block headers in reverse order, oldest first
	for i := len(blockHeaders) - 1; i >= 0; i-- {
		if err := bp.ProcessBlock(ctx, blockHeaders[i], blockMetas[i]); err != nil {
			logger.Errorf("failed to process block %s: %s", blockHeaders[i].Hash(), err)
		}
	}

	// check all the transactions in tx blaster log
	// logger.Infof("checking transactions from tx blaster log")
	// txLog, err := os.OpenFile("data/txblaster.log", os.O_RDONLY, 0644)
	// if err != nil {
	// 	logger.Errorf("failed to open txblaster.log: %s", err)
	// } else {
	// 	fileScanner := bufio.NewScanner(txLog)
	// 	fileScanner.Split(bufio.ScanLines)

	// 	var txHash *chainhash.Hash
	// 	for fileScanner.Scan() {
	// 		txId := fileScanner.Text()
	// 		txHash, err = chainhash.NewHashFromStr(txId)
	// 		if err != nil {
	// 			logger.Errorf("failed to parse tx id %s: %s", txId, err)
	// 			continue
	// 		}

	// 		_, ok := transactionMap[*txHash]
	// 		if !ok {
	// 			logger.Errorf("transaction %s does not exist in any subtree in any block", txHash)
	// 		}
	// 	}
	// 	_ = txLog.Close()
	// }
}
