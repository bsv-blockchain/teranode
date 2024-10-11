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
	"github.com/bitcoin-sv/ubsv/stores/blob/options"
	"github.com/bitcoin-sv/ubsv/stores/blockchain"
	utxofactory "github.com/bitcoin-sv/ubsv/stores/utxo/_factory"
	"github.com/bitcoin-sv/ubsv/stores/utxo/meta"
	"github.com/bitcoin-sv/ubsv/ulogger"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/ordishs/gocore"
)

func Start() {
	ctx := context.Background()
	logger := ulogger.TestLogger{}

	if path, err := os.Getwd(); err != nil {
		panic(err)
	} else if strings.Contains(path, "cmd/txblockidcheck") { // check whether path contains cmd/chainintegrity or not
		if err := os.Chdir("../../"); err != nil {
			panic(err)
		}
	}

	utxoStoreFlag := flag.String("utxostore", "", "utxo store URL")
	blockchainStoreFlag := flag.String("blockchainstore", "", "blockchain store URL")
	subtreeStoreFlag := flag.String("subtreestore", "", "subtree store URL")
	txHashString := flag.String("txhash", "", "transaction hash")
	flag.Parse()

	var (
		ok              bool
		utxoStoreString string
	)

	if *utxoStoreFlag != "" {
		utxoStoreString = *utxoStoreFlag
	} else {
		utxoStoreString, ok = gocore.Config().Get("utxostore")
		if !ok {
			usage("utxo store URL required: provide via -utxostore flag or configuration")
		}
	}

	var blockchainStoreString string
	if *blockchainStoreFlag != "" {
		blockchainStoreString = *blockchainStoreFlag
	} else {
		blockchainStoreString, ok = gocore.Config().Get("blockchain_store")
		if !ok {
			usage("blockchain store URL required: provide via -blockchainstore flag or configuration")
		}
	}

	var subtreeStoreString string
	if *subtreeStoreFlag != "" {
		subtreeStoreString = *subtreeStoreFlag
	} else {
		subtreeStoreString, ok = gocore.Config().Get("subtreestore")
		if !ok {
			usage("subtree store URL required: provide via -subtreestore flag or configuration")
		}
	}

	if *txHashString == "" {
		usage("transaction hash required")
	}

	utxoStore, err := utxofactory.NewStore(ctx, logger, parseURL(utxoStoreString), "main", false)
	if err != nil {
		usage(err.Error())
	}

	blockchainStore, err := blockchain.NewStore(logger, parseURL(blockchainStoreString))
	if err != nil {
		usage(err.Error())
	}

	subtreeStore, err := blob.NewStore(logger, parseURL(subtreeStoreString), options.WithHashPrefix(2))
	if err != nil {
		usage(err.Error())
	}

	txHash, err := chainhash.NewHashFromStr(*txHashString)
	if err != nil {
		usage(err.Error())
	}

	txMeta, err := utxoStore.Get(ctx, txHash)
	if errors.Is(err, errors.ErrTxNotFound) {
		fmt.Printf("Tx %s not found in utxoStore\n", txHash)
	} else if err != nil {
		usage(err.Error())
	}

	var parentTxs []*meta.Data

	if txMeta != nil {
		fmt.Printf("Tx %s (coinbase=%v) (blockID=%v)\n", txHash, txMeta.IsCoinbase, txMeta.BlockIDs)

		for _, hash := range txMeta.ParentTxHashes {
			txMeta, err := utxoStore.Get(ctx, &hash)

			switch {
			case errors.Is(err, errors.ErrTxNotFound):
				fmt.Printf("Parent tx %s not found in utxoStore\n", txHash)
			case err != nil:
				fmt.Printf("Parent tx %s get error\n", hash)
			default:
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

					fmt.Printf("Block ID found %d \n", blockID)

					break
				}
			}
		}

		for _, parentTx := range parentTxs {
			for _, blockID := range parentTx.BlockIDs {
				if blockMeta.ID == blockID {
					foundParentBlockID = true

					fmt.Printf("Parent block ID found %d\n", blockID)

					break
				}
			}
		}

		block, _, err := blockchainStore.GetBlock(ctx, blockHeader.Hash())
		if err != nil {
			usage(err.Error())
		}

		if err := block.GetAndValidateSubtrees(ctx, ulogger.TestLogger{}, subtreeStore, nil); err != nil {
			fmt.Println(err.Error())
			continue
		}

		if parentTxs != nil && parentTxs[0].IsCoinbase {
			if block, _, err := blockchainStore.GetBlock(ctx, blockHeader.Hash()); err != nil {
				fmt.Println(err.Error())
			} else {
				// fmt.Printf("%s[%d] coinbase tx %s\n", blockHeader.Hash(), blockMeta.ID, block.CoinbaseTx.TxIDChainHash())
				for _, parentTx := range parentTxs {
					parentTxHash := parentTx.Tx.TxIDChainHash()
					if block.CoinbaseTx.TxIDChainHash().IsEqual(parentTxHash) {
						foundParentCount++

						fmt.Print("===============\n")
						fmt.Printf("Coinbase Parent tx found: %s\n", parentTxHash)
						fmt.Printf("Block Height: %d\n", blockMeta.Height)
						fmt.Printf("Block ID: %d\n", blockMeta.ID)
						fmt.Printf("Block Hash: %s\n", blockHeader.Hash())
						fmt.Print("===============\n")
					}
				}
			}
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
		fmt.Printf("Block ID %v not found\n", txMeta.BlockIDs)
	}

	if foundCount == 0 {
		fmt.Printf("Tx not found\n")
	}

	if txMeta != nil && !foundParentBlockID {
		for _, parentTx := range parentTxs {
			fmt.Printf("Parent block ID %v not found\n", parentTx.BlockIDs)
		}
	}

	if txMeta != nil && foundParentCount == 0 {
		fmt.Printf("Parent tx not found\n")
	}
}

func usage(msg string) {
	if msg != "" {
		fmt.Printf("Error: %s\n\n", msg)
	}

	fmt.Printf("Usage: txinfo [-utxostore <utxo-store-URL>] [-blockchainstore <blockchain-store-URL>] [-subtreestore <subtree-store-URL>] -txhash <tx-hash>\n\n")

	os.Exit(1)
}

func parseURL(s string) *url.URL {
	u, err := url.Parse(s)
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
func (l VerboseLogger) Duplicate(options ...ulogger.Option) ulogger.Logger {
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
