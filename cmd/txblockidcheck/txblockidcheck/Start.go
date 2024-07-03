package txblockidcheck

import (
	"context"
	"flag"
	"fmt"
	"net/url"
	"os"
	"strings"

	"github.com/bitcoin-sv/ubsv/errors"
	"github.com/bitcoin-sv/ubsv/model"
	"github.com/bitcoin-sv/ubsv/stores/blob"
	"github.com/bitcoin-sv/ubsv/stores/blockchain"
	"github.com/bitcoin-sv/ubsv/stores/utxo"
	utxofactory "github.com/bitcoin-sv/ubsv/stores/utxo/_factory"
	"github.com/bitcoin-sv/ubsv/stores/utxo/meta"
	"github.com/bitcoin-sv/ubsv/ulogger"
	"github.com/libsv/go-bt/v2/chainhash"
)

func Start() {
	ctx := context.Background()
	logger := ulogger.TestLogger{}

	if path, err := os.Getwd(); err != nil {
		panic(err)
	} else {
		// check whether path contains cmd/chainintegrity or not
		if strings.Contains(path, "cmd/txblockidcheck") {
			if err := os.Chdir("../../"); err != nil {
				panic(err)
			}
		}
	}
	utxoStoreString := flag.String("utxostore", "", "utxo store URL")
	blockchainStoreString := flag.String("blockchainstore", "", "utxo store URL")
	subtreeStoreString := flag.String("subtreestore", "", "subtree store URL")
	txHashString := flag.String("txhash", "", "transaction hash")
	flag.Parse()

	if *utxoStoreString == "" {
		usage("utxo store URL required")
	}

	var utxoStore utxo.Store
	utxoStore, err := utxofactory.NewStore(ctx, logger, parseURL(utxoStoreString), "main", false)
	if err != nil {
		usage(err.Error())
	}

	if *blockchainStoreString == "" {
		usage("blockchain store URL required")
	}

	var blockchainStore blockchain.Store
	blockchainStore, err = blockchain.NewStore(logger, parseURL(blockchainStoreString))
	if err != nil {
		usage(err.Error())
	}

	if *subtreeStoreString == "" {
		usage("subtree store URL required")
	}

	var subtreeStore blob.Store
	subtreeStore, err = blob.NewStore(logger, parseURL(subtreeStoreString))
	if err != nil {
		usage(err.Error())
	}

	if *txHashString == "" {
		usage("transaction hash required")
	}

	txHash, err := chainhash.NewHashFromStr(*txHashString)
	if err != nil {
		usage(err.Error())
	}

	txMeta, err := utxoStore.GetMeta(ctx, txHash)
	if errors.Is(err, errors.ErrTxNotFound) {
		fmt.Printf("Tx %s not found in utxoStore\n", txHash)
	} else if err != nil && err != utxo.ErrNotFound {
		usage(err.Error())
	}

	var parentTxs []*meta.Data

	if txMeta != nil {
		fmt.Printf("Tx %s (coinbase=%v) (blockID=%v)\n", txHash, txMeta.IsCoinbase, txMeta.BlockIDs)

		for _, hash := range txMeta.ParentTxHashes {
			txMeta, err := utxoStore.GetMeta(ctx, &hash)
			if errors.Is(err, errors.ErrTxNotFound) {
				fmt.Printf("Parent tx %s not found in utxoStore\n", txHash)
			} else if err != nil {
				fmt.Printf("Parent tx %s get error\n", hash)
			} else {
				parentTxs = append(parentTxs, txMeta)
				fmt.Printf("Parent tx %s (coinbase=%v) (blockID=%v)\n", hash, txMeta.IsCoinbase, txMeta.BlockIDs)
			}
		}
	}

	blockHeader, _, err := blockchainStore.GetBestBlockHeader(ctx)
	if err != nil {
		usage(err.Error())
	}

	// blockchainIDs, err := blockchainStore.GetBlockHeaderIDs(ctx, blockHeader.Hash(), 10000)
	// if err != nil {
	// 	usage(err.Error())
	// }
	// fmt.Printf("Best block IDs: %v\n", blockchainIDs)

	fmt.Printf("\nChecking main chain...\n")
	blockHeaders, blockMetas, err := blockchainStore.GetBlockHeaders(ctx, blockHeader.Hash(), 10000)
	if err != nil {
		usage(err.Error())
	}
	checkBlocks(ctx, blockHeaders, blockMetas, txMeta, parentTxs, blockchainStore, subtreeStore, txHash)

	fmt.Printf("\nChecking forks...\n")
	forkedBlockHeaders, forkedBlockMetas, err := blockchainStore.GetForkedBlockHeaders(ctx, blockHeader.Hash(), 10000)
	if err != nil {
		usage(err.Error())
	}
	checkBlocks(ctx, forkedBlockHeaders, forkedBlockMetas, txMeta, parentTxs, blockchainStore, subtreeStore, txHash)

}

func checkBlocks(ctx context.Context, blockHeaders []*model.BlockHeader, blockMetas []*model.BlockHeaderMeta, txMeta *meta.Data, parentTxs []*meta.Data, blockchainStore blockchain.Store, subtreeStore blob.Store, txHash *chainhash.Hash) {
	foundCount := 0
	foundBlockID := false
	foundParentCount := 0
	foundParentBlockID := false

	for i, blockHeader := range blockHeaders {
		blockMeta := blockMetas[i]

		if txMeta != nil {
			for _, blockID := range txMeta.BlockIDs {
				if blockMeta.ID == blockID {
					foundBlockID = true
					fmt.Printf("Block ID %d found\n", blockID)
					break
				}
			}
		}

		for _, parentTx := range parentTxs {
			for _, blockID := range parentTx.BlockIDs {
				if blockMeta.ID == blockID {
					foundParentBlockID = true
					fmt.Printf("Parent Block ID %d found\n", blockID)
					break
				}
			}
		}

		block, _, err := blockchainStore.GetBlock(ctx, blockHeader.Hash())
		if err != nil {
			usage(err.Error())
		}

		if err := block.GetAndValidateSubtrees(ctx, ulogger.TestLogger{}, subtreeStore); err != nil {
			fmt.Println(err.Error())
			continue
		}

		for _, subtreeSlice := range block.SubtreeSlices {
			for _, node := range subtreeSlice.Nodes {
				if node.Hash.IsEqual(txHash) {
					foundCount++
					fmt.Print("===============\n")
					fmt.Printf("Tx found: %s\n", txHash)
					fmt.Printf("Block Height: %d\n", blockMeta.Height)
					fmt.Printf("Block ID: %d\n", blockMeta.ID)
					fmt.Printf("Block Hash: %s\n", blockHeader.Hash())
					fmt.Printf("Subtree Root Hash: %s\n", subtreeSlice.RootHash())
					fmt.Printf("Subtree Node: %d\n", i)
					fmt.Print("===============\n")
					continue
				}

				if txMeta == nil {
					continue
				}

				for _, parentTxHash := range txMeta.ParentTxHashes {
					if node.Hash.Equal(parentTxHash) {
						foundParentCount++
						fmt.Print("===============\n")
						fmt.Printf("Parent tx found: %s\n", parentTxHash)
						fmt.Printf("Block Height: %d\n", blockMeta.Height)
						fmt.Printf("Block ID: %d\n", blockMeta.ID)
						fmt.Printf("Block Hash: %s\n", blockHeader.Hash())
						fmt.Printf("Subtree Root Hash: %s\n", subtreeSlice.RootHash())
						fmt.Printf("Subtree Node: %d\n", i)
						fmt.Print("===============\n")
						continue
					}
				}
			}
		}
	}

	if txMeta != nil && !foundBlockID {
		fmt.Printf("%v not found\n", txMeta.BlockIDs)
	}
	if foundCount == 0 {
		fmt.Printf("Tx not found\n")
	}
	if txMeta != nil && !foundParentBlockID {
		fmt.Printf("Parent %v not found\n", txMeta.BlockIDs)
	}
	if txMeta != nil && foundParentCount == 0 {
		fmt.Printf("Parent tx not found\n")
	}
}

func usage(msg string) {
	if msg != "" {
		fmt.Printf("Error: %s\n\n", msg)
	}
	fmt.Printf("Usage: txinfo -utxostore <utxo-store-URL> -blockchainstore <blockchain-store-URL> -subtreestore <subtree-store-URL> -txhash <tx-hash>\n\n")
	os.Exit(1)
}

func parseURL(s *string) *url.URL {
	u, err := url.Parse(*s)
	if err != nil {
		usage(err.Error())
	}
	return u
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
