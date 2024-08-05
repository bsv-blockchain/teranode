package chainextracts

import (
	"context"
	"flag"
	"time"

	"github.com/bitcoin-sv/ubsv/cmd/filereader/filereader"
	"github.com/bitcoin-sv/ubsv/stores/blob"
	"github.com/bitcoin-sv/ubsv/stores/blob/options"
	blockchain_store "github.com/bitcoin-sv/ubsv/stores/blockchain"
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

	debug := flag.Bool("debug", false, "enable debug logging")
	logfile := flag.String("logfile", "chainextract.log", "path to logfile")
	flag.Parse()

	debugLevel := "INFO"
	if *debug {
		debugLevel = "DEBUG"
	}
	var logger = ulogger.New("chainextracts", ulogger.WithLevel(debugLevel), ulogger.WithLoggerType("file"), ulogger.WithFilePath(*logfile))

	blockchainStoreURL, err, found := gocore.Config().GetURL("blockchain_store.docker.ci.chainintegrity.ubsv1")
	logger.Debugf("blockchainStoreURL: %v", blockchainStoreURL)
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

	subtreeStoreUrl, err, found := gocore.Config().GetURL("subtreestore.docker.ci.chainintegrity.ubsv1")
	if err != nil {
		panic(err)
	}
	if !found {
		panic("subtreestore config not found")
	}
	subtreeStore, err := blob.NewStore(logger, subtreeStoreUrl)
	if err != nil {
		panic(err)
	}

	// ------------------------------------------------------------------------------------------------
	// test stores
	// ------------------------------------------------------------------------------------------------

	ctx := context.Background()
	fromTime := time.Date(2024, 4, 7, 14, 00, 00, 0, time.UTC)
	toTime := time.Date(2024, 4, 7, 15, 00, 00, 0, time.UTC)

	allBlocks, err := blockchainDB.GetBlocksByTime(ctx, fromTime, toTime)
	if err != nil {
		logger.Errorf("error getting blocks: %s", err)
	}

	logger.Infof("found %d blocks", len(allBlocks))

	for _, hashBytes := range allBlocks {
		hash := chainhash.Hash(hashBytes)

		logger.Infof("checking block %s", hash)
		block, height, err := blockchainDB.GetBlock(ctx, (*chainhash.Hash)(hashBytes))
		if err != nil {
			logger.Errorf("failed to get block %s: %s", height, err)
			continue
		}
		logger.Infof("block %s has height %d", hash, height)

		if len(block.Subtrees) == 0 {
			logger.Infof("no subtrees found for block %s", hash)
			continue
		}

		logger.Infof("checking %d subtrees", len(block.Subtrees))

		for _, subtreeHash := range block.Subtrees {
			_, err = subtreeStore.Get(ctx, subtreeHash[:], options.WithFileExtension("subtree"))
			if err != nil {
				logger.Errorf("failed to get subtree %s for block %s: %s", subtreeHash, hash, err)
				logger.Debugf("block dump: %s", block.Header.StringDump())
			}
			logger.Infof("subtree %s exists", subtreeHash)
		}

		if err := filereader.ProcessFile(hash.String()+".block", logger); err != nil {
			logger.Errorf("Error during validation: %v", err)
			return
		} else {
			logger.Infof("Block %s is valid", hash)

		}
	}
}
