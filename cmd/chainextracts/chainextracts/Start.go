package chainextracts

import (
	"bufio"
	"context"
	"flag"
	"io"
	"time"

	"github.com/bitcoin-sv/ubsv/cmd/validate_block/validateblock"
	"github.com/bitcoin-sv/ubsv/stores/blob"
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
	persistURL, err, ok := gocore.Config().GetURL("blockPersister_persistURL.ci.chainintegrity.ubsv1")
	if err != nil || !ok {
		logger.Fatalf("Error getting blockpersister_store URL: %v", err)
	}
	persistStore, err := blob.NewStore(logger, persistURL)
	if err != nil {
		logger.Errorf("failed to open store at %s: %s", persistURL, err)
		return
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

	for _, hash := range allBlocks {
		logger.Infof("checking block %s", chainhash.Hash(hash))
		block, height, err := blockchainDB.GetBlock(ctx, (*chainhash.Hash)(hash))
		if err != nil {
			logger.Errorf("failed to get block %s: %s", height, err)
			continue
		}
		logger.Infof("block %s has height %d", chainhash.Hash(hash), height)

		if len(block.Subtrees) == 0 {
			logger.Infof("no subtrees found for block %s", chainhash.Hash(hash))
			continue
		}

		logger.Infof("checking %d subtrees", len(block.Subtrees))

		for _, subtreeHash := range block.Subtrees {
			_, err = subtreeStore.Get(ctx, subtreeHash[:])
			if err != nil {
				logger.Errorf("failed to get subtree %s for block %s: %s", subtreeHash, chainhash.Hash(hash), err)
				logger.Debugf("block dump: %s", block.Header.StringDump())
			}
			logger.Infof("subtree %s exists", subtreeHash)
		}

		rc, err := persistStore.GetIoReader(context.Background(), hash)
		if err != nil {
			logger.Errorf("error getting reader from store: %s", err)
			return
		}
		logger.Infof("block %s has been persisted", chainhash.Hash(hash))
		var r io.Reader = rc
		r = bufio.NewReaderSize(r, 1024*1024)

		valid, err := validateblock.ValidateBlock(r, logger)
		if err != nil {
			logger.Errorf("Error during validation: %v", err)
			return
		}

		if valid {
			logger.Infof("Block %s is valid", chainhash.Hash(hash))
		} else {
			logger.Errorf("Block %s is NOT valid", chainhash.Hash(hash))
		}

	}
}
