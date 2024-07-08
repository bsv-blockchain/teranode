package s3inventoryintegrity

import (
	"context"
	"encoding/csv"
	"flag"
	"fmt"
	"io"
	"log"
	"net/url"
	"os"
	"slices"
	"strings"
	"time"

	"github.com/bitcoin-sv/ubsv/model"
	"github.com/bitcoin-sv/ubsv/stores/blob"
	"github.com/bitcoin-sv/ubsv/stores/blob/options"
	blockchain_store "github.com/bitcoin-sv/ubsv/stores/blockchain"
	"github.com/bitcoin-sv/ubsv/ulogger"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/ordishs/gocore"
)

type s3bucket struct {
	url   string
	store blob.Store
}

// Function to process each row
func Start() {

	path, err := os.Getwd()
	if err != nil {
		panic(err)
	}

	if strings.Contains(path, "cmd/s3inventoryintegrity") {
		if err = os.Chdir("../../"); err != nil {
			panic(err)
		}
	}

	verbose := flag.Bool("verbose", false, "enable verbose logging")
	quick := flag.Bool("quick", false, "skip checking file exists in S3 storage (very slow)")
	blockchainStoreURLString := flag.String("d", "", "blockchain store URL")
	filename := flag.String("f", "", "CSV filename")
	flag.Parse()

	if *blockchainStoreURLString == "" {
		usage("blockchain store URL required")
	}

	if *filename == "" {
		usage("filename required")
	}

	fmt.Printf("%s\n", *filename)

	var verboseLogger ulogger.Logger
	if *verbose {
		verboseLogger = VerboseLogger{}
	} else {
		// output nothing
		verboseLogger = ulogger.TestLogger{}
	}

	blockchainStoreURL, err := url.ParseRequestURI(*blockchainStoreURLString)
	if err != nil {
		panic(err.Error())
	}

	blockchainDB, err := blockchain_store.NewStore(ulogger.TestLogger{}, blockchainStoreURL)
	if err != nil {
		panic(err)
	}

	// s3buckets := make(map[string]blob.Store)

	s3buckets := []s3bucket{}

	if !*quick {
		for i := 1; i <= 6; i++ {
			url, err, _ := gocore.Config().GetURL(fmt.Sprintf("blockstore_m%d", i))
			if err != nil {
				panic(err.Error())
			}

			store, err := blob.NewStore(ulogger.TestLogger{}, url)
			if err != nil {
				panic(err)
			}
			// s3buckets[url.String()] = store
			s3buckets = append(s3buckets, s3bucket{url.String(), store})

		}
	}

	ctx := context.Background()

	bestBlockHeader, _, err := blockchainDB.GetBestBlockHeader(ctx)
	if err != nil {
		panic(err)
	}

	filenames := loadFilenames(*filename)
	fmt.Printf("%d filenames in CSV\n", len(filenames))

	if blockHeaders, _, err := blockchainDB.GetBlockHeaders(ctx, bestBlockHeader.Hash(), 100000); err != nil {
		panic(err)
	} else {
		fmt.Printf("=================================================================\n")
		fmt.Printf("found %d block headers\n", len(blockHeaders))
		checkFile(ctx, blockHeaders, verboseLogger, filenames, s3buckets, blockchainDB, true)
	}

	if forkedBlockHeaders, _, err := blockchainDB.GetForkedBlockHeaders(ctx, bestBlockHeader.Hash(), 100000); err != nil {
		panic(err)
	} else {
		fmt.Printf("=================================================================\n")
		fmt.Printf("found %d forked block headers\n", len(forkedBlockHeaders))
		checkFile(ctx, forkedBlockHeaders, verboseLogger, filenames, s3buckets, blockchainDB, false)
	}

	fmt.Printf("=================================================================\n")
	if len(filenames) == 0 {
		fmt.Printf("No additional (non blockchain related) files found in CSV\n")
	} else {
		fmt.Printf("%d additional files in CSV that are not part of any block in the blockchain store\n", len(filenames))

		blocks := 0
		utdodiffs := 0
		subtrees := 0
		others := 0
		for filename := range filenames {
			if strings.HasSuffix(filename, ".block") {
				blocks++
			} else if strings.HasSuffix(filename, ".utxodiff") {
				utdodiffs++
			} else if strings.HasSuffix(filename, ".subtree") {
				subtrees++
			} else {
				others++
			}
		}
		fmt.Printf("blocks: %d\n", blocks)
		fmt.Printf("utxodiffs: %d\n", utdodiffs)
		fmt.Printf("subtrees: %d\n", subtrees)
		fmt.Printf("others: %d\n", others)

		// print the others
		for filename := range filenames {
			if !strings.HasSuffix(filename, ".block") && !strings.HasSuffix(filename, ".utxodiff") && !strings.HasSuffix(filename, ".subtree") {
				fmt.Printf("%s\n", filename)
			}
		}

		for filename := range filenames {
			verboseLogger.Infof("%s\n", filename)
		}
	}

}

func checkFile(ctx context.Context, blockHeaders []*model.BlockHeader, verboseLogger ulogger.Logger, filenames map[string]bool, s3buckets []s3bucket, blockchainDB blockchain_store.Store, testChainIntegrity bool) {
	numBlocks := 0
	numDiffs := 0
	numSubtrees := 0
	foundBlocks := 0
	foundDiffs := 0
	foundSubtrees := 0

	var previousHash chainhash.Hash
	hashGenesisBlock, _ := chainhash.NewHashFromStr("000000000019d6689c085ae165831e934ff763ae46a2a6c172b3f1b60a8ce26f")

	slices.Reverse(blockHeaders)
	for _, blockHeader := range blockHeaders {
		if blockHeader.Hash().IsEqual(hashGenesisBlock) {
			continue
		}

		numBlocks++

		if testChainIntegrity && !blockHeader.HashPrevBlock.IsEqual(&previousHash) && !blockHeader.HashPrevBlock.IsEqual(hashGenesisBlock) {
			verboseLogger.Infof("block %s has incorrect previous block hash %s\n", blockHeader.Hash(), blockHeader.HashPrevBlock)
		}
		previousHash = *blockHeader.Hash()

		if filenames[blockHeader.Hash().String()+".block"] {
			foundBlocks++
		} else {
			if existsInAnotherS3Bucket(ctx, s3buckets, *blockHeader.Hash(), "block", time.Unix(int64(blockHeader.Timestamp), 0), verboseLogger) {
				foundBlocks++
			}
		}
		delete(filenames, blockHeader.Hash().String()+".block")

		numDiffs++
		if filenames[blockHeader.Hash().String()+".utxodiff"] {
			foundDiffs++
		} else {
			if existsInAnotherS3Bucket(ctx, s3buckets, *blockHeader.Hash(), "utxodiff", time.Unix(int64(blockHeader.Timestamp), 0), verboseLogger) {
				foundDiffs++
			}
		}
		delete(filenames, blockHeader.Hash().String()+".utxodiff")
		delete(filenames, blockHeader.Hash().String()+".utxoset")

		block, _, err := blockchainDB.GetBlock(ctx, blockHeader.Hash())
		if err != nil {
			fmt.Printf("failed to get block %s: %s\n", blockHeader.Hash(), err)
			continue
		}

		for _, subtreeHash := range block.Subtrees {
			numSubtrees++
			if filenames[subtreeHash.String()+".subtree"] {
				foundSubtrees++
			} else {

				if existsInAnotherS3Bucket(ctx, s3buckets, *subtreeHash, "subtree", time.Unix(int64(blockHeader.Timestamp), 0), verboseLogger) {
					foundSubtrees++
				}
			}
			delete(filenames, subtreeHash.String()+".subtree")
		}
	}

	fmt.Printf("block headers: found %.2f%% (%d of %d) \n", float64(foundBlocks)/float64(numBlocks)*100, foundBlocks, numBlocks)
	fmt.Printf("utxodiffs: found %.2f%% (%d of %d)\n", float64(foundDiffs)/float64(numDiffs)*100, foundDiffs, numDiffs)
	fmt.Printf("subtrees: found %.2f%% (%d of %d)\n", float64(foundSubtrees)/float64(numSubtrees)*100, foundSubtrees, numSubtrees)
}

func existsInAnotherS3Bucket(ctx context.Context, s3buckets []s3bucket, hash chainhash.Hash, extension string, time time.Time, verboseLogger ulogger.Logger) bool {
	found := false
	for i, s3bucket := range s3buckets {
		storeName := s3bucket.url
		store := s3bucket.store
		exists, err := store.Exists(ctx, hash[:], options.WithFileExtension(extension))
		if err != nil {
			fmt.Printf("failed to check if %s.block exists in %s: %s\n", hash, storeName, err)
			continue
		}
		if exists {
			if i != 0 {
				// found in m1, skip
				verboseLogger.Infof("%s.%s not found in CSV file but exists in %s\n", hash, extension, storeName)
				// fmt.Printf("%s.%s not found in CSV file but exists in %s\n", hash, extension, storeName)
				// fmt.Printf("%s.%s %s\n", hash, extension, storeName)
			}
			found = true
			break
		}
	}
	if !found {
		verboseLogger.Infof("%s.%s not found in CSV file %s\n", hash, extension, time)
		// fmt.Printf("%s.%s MISSING\n", hash, extension)
	}
	return found
}

func loadFilenames(filename string) map[string]bool {
	filenames := make(map[string]bool)

	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("Unable to open CSV file: %v", err)
	}
	defer file.Close()

	reader := csv.NewReader(file)

	for {
		line, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Fatalf("Error reading CSV line: %v", err)
		}

		filenames[line[1]] = true
	}

	return filenames
}

func usage(msg string) {
	if msg != "" {
		fmt.Printf("Error: %s\n\n", msg)
	}
	fmt.Printf("Usage: s3inventoryintegrity [-verbose] [-quick] -d <postgres-URL> -f <csv-filename>\n\n")
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
