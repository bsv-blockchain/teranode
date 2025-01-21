package chainextracts

import (
	"context"
	"flag"
	"time"

	"github.com/bitcoin-sv/teranode/cmd/filereader"
	"github.com/bitcoin-sv/teranode/settings"
	"github.com/bitcoin-sv/teranode/stores/blob"
	"github.com/bitcoin-sv/teranode/stores/blob/options"
	blockchain_store "github.com/bitcoin-sv/teranode/stores/blockchain"
	"github.com/bitcoin-sv/teranode/ulogger"
	"github.com/libsv/go-bt/v2/chainhash"
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
	tSettings := settings.NewSettings()

	debugLevel := "INFO"
	if *debug {
		debugLevel = "DEBUG"
	}

	var logger = ulogger.New("chainextracts", ulogger.WithLevel(debugLevel), ulogger.WithLoggerType("file"), ulogger.WithFilePath(*logfile))

	// this was using a specific hard coded context
	blockchainStoreURL := tSettings.BlockChain.StoreURL // GetURL("blockchain_store.docker.ci.chainintegrity.teranode1")
	if blockchainStoreURL == nil {
		panic("no blockchain_store setting found")
	}

	logger.Debugf("blockchainStoreURL: %v", blockchainStoreURL)

	blockchainDB, err := blockchain_store.NewStore(logger, blockchainStoreURL, tSettings)
	if err != nil {
		panic(err)
	}

	// this was using a specific hard coded context
	subtreeStoreURL := tSettings.SubtreeValidation.SubtreeStore // GetURL("subtreestore.docker.ci.chainintegrity.teranode1")
	if subtreeStoreURL == nil {
		panic("subtreestore config not found")
	}

	subtreeStore, err := blob.NewStore(logger, subtreeStoreURL)
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

		if err := filereader.ProcessFile(context.Background(), hash.String()+".block", logger); err != nil {
			logger.Errorf("Error during validation: %v", err)
			return
		} else {
			logger.Infof("Block %s is valid", hash)
		}
	}
}
