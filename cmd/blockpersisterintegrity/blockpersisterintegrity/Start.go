package blockpersisterintegrity

import (
	"context"
	"flag"
	"fmt"
	"net/url"
	"os"
	"strings"

	"github.com/bitcoin-sv/ubsv/model"
	"github.com/bitcoin-sv/ubsv/stores/blob"
	blockchain_store "github.com/bitcoin-sv/ubsv/stores/blockchain"

	// utxostore_factory "github.com/bitcoin-sv/ubsv/stores/utxo/_factory"
	"github.com/bitcoin-sv/ubsv/ulogger"
	"github.com/libsv/go-bt/v2/chainhash"
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

	blockchainStoreURLString := flag.String("d", "", "blockchain store URL")
	s3StoreURLString := flag.String("s3", "", "S3 block-store URL")
	// debug := flag.Bool("debug", true, "enable debug logging")
	// logfile := flag.String("logfile", "blockpersisterintegrity.log", "path to logfile")
	flag.Parse()

	if *blockchainStoreURLString == "" {
		usage("blockchain store URL required")
	}

	if *s3StoreURLString == "" {
		usage("S3 block-store URL required")
	}

	// debugLevel := "INFO"
	// if *debug {
	// 	debugLevel = "DEBUG"
	// }
	// var logger = ulogger.New("blockpersisterintegrity", ulogger.WithLevel(debugLevel), ulogger.WithLoggerType("file"), ulogger.WithFilePath(*logfile))
	// var logger = ulogger.New("blockpersisterintegrity", ulogger.WithLevel(debugLevel))
	var logger = VerboseLogger{}

	blockchainStoreURL, err := url.ParseRequestURI(*blockchainStoreURLString)
	if err != nil {
		panic(err.Error())
	}

	blockchainDB, err := blockchain_store.NewStore(ulogger.TestLogger{}, blockchainStoreURL)
	if err != nil {
		panic(err)
	}

	s3StoreURL, err := url.ParseRequestURI(*s3StoreURLString)
	if err != nil {
		panic(err.Error())
	}
	blobStore, err := blob.NewStore(ulogger.TestLogger{}, s3StoreURL)
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

	logger.Infof("found %d block headers\n", len(blockHeaders))

	var previousBlockHeader *model.BlockHeader
	for _, blockHeader := range blockHeaders {
		if previousBlockHeader != nil {
			if !previousBlockHeader.HashPrevBlock.IsEqual(blockHeader.Hash()) {
				logger.Errorf("block header %s does not match previous block header %s\n", blockHeader.Hash(), previousBlockHeader.HashPrevBlock)
			}
		}
		previousBlockHeader = blockHeader
	}

	bp := NewBlockProcessor(logger, blobStore)

	// range through the block headers in reverse order, oldest first
	for i := len(blockHeaders) - 4010; i >= 0; i-- {
		if err := bp.ProcessBlock(ctx, blockHeaders[i], blockMetas[i]); err != nil {
			logger.Errorf("failed to process block %s: %s\n", blockHeaders[i].Hash(), err)
		}
	}

	// check all the transactions in tx blaster log
	// logger.Infof("checking transactions from tx blaster log\n")
	// txLog, err := os.OpenFile("data/txblaster.log", os.O_RDONLY, 0644)
	// if err != nil {
	// 	logger.Errorf("failed to open txblaster.log: %s\n", err)
	// } else {
	// 	fileScanner := bufio.NewScanner(txLog)
	// 	fileScanner.Split(bufio.ScanLines)

	// 	var txHash *chainhash.Hash
	// 	for fileScanner.Scan() {
	// 		txId := fileScanner.Text()
	// 		txHash, err = chainhash.NewHashFromStr(txId)
	// 		if err != nil {
	// 			logger.Errorf("failed to parse tx id %s: %s\n", txId, err)
	// 			continue
	// 		}

	// 		_, ok := transactionMap[*txHash]
	// 		if !ok {
	// 			logger.Errorf("transaction %s does not exist in any subtree in any block\n", txHash)
	// 		}
	// 	}
	// 	_ = txLog.Close()
	// }
}

func usage(msg string) {
	if msg != "" {
		fmt.Printf("Error: %s\n\n", msg)
	}
	fmt.Printf("Usage: blockpersisterintegrity [-verbose] -d <postgres-URL> -s3 <block-store-URL>\n\n")
	os.Exit(1)
}

type VerboseLogger struct{}

func (l VerboseLogger) LogLevel() int {
	return 0
}
func (l VerboseLogger) SetLogLevel(level string) {}

func (l VerboseLogger) New(service string, options ...ulogger.Option) ulogger.Logger {
	return VerboseLogger{}
}
func (l VerboseLogger) Debugf(format string, args ...interface{}) {
	fmt.Printf(format, args...)
}
func (l VerboseLogger) Infof(format string, args ...interface{}) {
	fmt.Printf(format, args...)
}
func (l VerboseLogger) Warnf(format string, args ...interface{}) {
	fmt.Printf(format, args...)
}
func (l VerboseLogger) Errorf(format string, args ...interface{}) {
	fmt.Printf(format, args...)
}
func (l VerboseLogger) Fatalf(format string, args ...interface{}) {
	fmt.Printf(format, args...)
}
