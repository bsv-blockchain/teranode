package main

import (
	"bufio"
	"context"
	"flag"
	"os"

	"github.com/bitcoin-sv/ubsv/model"
	"github.com/bitcoin-sv/ubsv/stores/blob"
	blockchain_store "github.com/bitcoin-sv/ubsv/stores/blockchain"
	"github.com/bitcoin-sv/ubsv/util"
	"github.com/libsv/go-bt/v2"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/ordishs/gocore"
)

var (
	genesisScript = "04ffff001d0104455468652054696d65732030332f4a616e2f32303039204368616e63656c6c6f72206f6e206272696e6b206f66207365636f6e64206261696c6f757420666f722062616e6b73"
)

type BlockSubtree struct {
	Block   chainhash.Hash
	Subtree chainhash.Hash
}

func main() {
	_ = os.Chdir("../../")

	debug := flag.Bool("debug", false, "enable debug logging")
	flag.Parse()

	debugLevel := gocore.NewLogLevelFromString("INFO")
	if *debug {
		debugLevel = gocore.NewLogLevelFromString("DEBUG")
	}
	var logger = gocore.Log("chainintegrity", debugLevel)

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

	subtreeStoreUrl, err, found := gocore.Config().GetURL("subtreestore")
	if err != nil {
		panic(err)
	}
	if !found {
		panic("subtreestore config not found")
	}
	subtreeStore, err := blob.NewStore(subtreeStoreUrl)
	if err != nil {
		panic(err)
	}

	txStoreUrl, err, found := gocore.Config().GetURL("txstore")
	if err != nil {
		panic(err)
	}
	if !found {
		panic("txstore config not found")
	}
	txStore, err := blob.NewStore(txStoreUrl)
	if err != nil {
		panic(err)
	}

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
	blockHeaders, _, err := blockchainDB.GetBlockHeaders(ctx, bestBlockHeader.Hash(), 100000)
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

	logger.Debugf("block headers are valid")

	transactionMap := make(map[chainhash.Hash]BlockSubtree)

	logger.Infof("checking blocks / subtrees")
	var block *model.Block
	var height uint32
	var coinbaseHeight uint32

	// range through the block headers in reverse order, oldest first
	for i := len(blockHeaders) - 1; i >= 0; i-- {
		blockHeader := blockHeaders[i]
		blockFees := uint64(0)

		block, height, err = blockchainDB.GetBlock(ctx, blockHeader.Hash())
		if err != nil {
			logger.Errorf("failed to get block %s: %s", blockHeader.Hash(), err)
			continue
		}

		logger.Debugf("checking block %s", block.Hash())

		if block.CoinbaseTx == nil || !block.CoinbaseTx.IsCoinbase() {
			logger.Errorf("block %s does not have a valid coinbase transaction", block.Hash())
		}

		if block.CoinbaseTx.Inputs[0].UnlockingScript.String() != genesisScript {
			coinbaseHeight, err = util.ExtractCoinbaseHeight(block.CoinbaseTx)
			if err != nil {
				logger.Errorf("failed to extract coinbase height from block coinbase %s: %s", block.Hash(), err)
			}

			if coinbaseHeight != height {
				logger.Errorf("coinbase height %d does not match block height %d", coinbaseHeight, height)
			}

			// add the coinbase to the map
			transactionMap[*block.CoinbaseTx.TxIDChainHash()] = BlockSubtree{Block: *block.Hash()}

			// check subtrees
			var subtreeBytes []byte
			var subtree *util.Subtree
			logger.Debugf("checking subtrees: %d", len(block.Subtrees))
			for _, subtreeHash := range block.Subtrees {
				logger.Debugf("checking subtree %s", subtreeHash)

				subtreeBytes, err = subtreeStore.Get(ctx, subtreeHash[:])
				if err != nil {
					logger.Errorf("failed to get subtree %s for block %s: %s", subtreeHash, block, err)
					logger.Debugf("block dump: %s", block.Header.StringDump())
				}

				subtree, err = util.NewSubtreeFromBytes(subtreeBytes)
				if err != nil {
					logger.Errorf("failed to parse subtree %s for block %s: %s", subtreeHash, block, err)
					continue
				}

				var tx []byte
				var btTx *bt.Tx
				subtreeFees := uint64(0)
				for _, node := range subtree.Nodes {
					if !model.CoinbasePlaceholderHash.IsEqual(node.Hash) {
						previousBlockSubtree, ok := transactionMap[*node.Hash]
						if ok {
							logger.Debugf("current subtree %s in block %s", subtreeHash, block.Hash())
							logger.Errorf("transaction %s already exists in subtree %s in block %s", node.Hash, previousBlockSubtree.Subtree, previousBlockSubtree.Block)
						} else {
							transactionMap[*node.Hash] = BlockSubtree{Block: *block.Hash(), Subtree: *subtreeHash}
						}

						tx, err = txStore.Get(ctx, node.Hash[:])
						if err != nil {
							logger.Errorf("failed to get transaction %s from tx store: %s", node, err)
							continue
						}
						btTx, err = bt.NewTxFromBytes(tx)
						if err != nil {
							logger.Errorf("failed to parse transaction %s from tx store: %s", node, err)
							continue
						}

						// check the topological order of the transactions
						for _, input := range btTx.Inputs {
							// the input tx id (parent tx) should already be in the transaction map
							inputHash := chainhash.Hash(input.PreviousTxID())
							if !inputHash.Equal(chainhash.Hash{}) { // coinbase is parent
								_, ok = transactionMap[inputHash]
								if !ok {
									logger.Errorf("the parent %s does not appear before the transaction %s", inputHash, node.Hash.String())
								}
							}
						}

						// the coinbase fees are calculated differently to check if everything matches up
						if !btTx.IsCoinbase() {
							fees, err := util.GetFees(btTx)
							if err != nil {
								logger.Errorf("failed to get the fees for tx: %s", btTx.String())
								continue
							}
							subtreeFees += fees
						}
					}
				}
				logger.Debugf("subtree %s has %d transactions and %d in fees", subtreeHash, len(subtree.Nodes), subtreeFees)

				if subtreeFees != subtree.Fees {
					logger.Errorf("subtree %s has incorrect fees: %d != %d", subtreeHash, subtreeFees, subtree.Fees)
				}

				blockFees += subtreeFees
			}
		}

		blockReward := block.CoinbaseTx.TotalOutputSatoshis()
		blockSubsidy := util.GetBlockSubsidyForHeight(height)
		if blockFees+blockSubsidy != blockReward {
			logger.Errorf("block %s has incorrect fees: %d != %d", block.Hash(), blockFees, blockReward)
		} else {
			logger.Debugf("block %s has %d in fees, subsidy %d", block.Hash(), blockFees, blockSubsidy)
		}
	}

	// check all the transactions in tx blaster log
	logger.Infof("checking transactions from tx blaster log")
	txLog, err := os.OpenFile("data/txblaster.log", os.O_RDONLY, 0644)
	if err != nil {
		logger.Errorf("failed to open txblaster.log: %s", err)
	} else {
		fileScanner := bufio.NewScanner(txLog)
		fileScanner.Split(bufio.ScanLines)

		var txHash *chainhash.Hash
		for fileScanner.Scan() {
			txId := fileScanner.Text()
			txHash, err = chainhash.NewHashFromStr(txId)
			if err != nil {
				logger.Errorf("failed to parse tx id %s: %s", txId, err)
				continue
			}

			_, ok := transactionMap[*txHash]
			if !ok {
				logger.Errorf("transaction %s does not exist in any subtree in any block", txHash)
			}
		}
		_ = txLog.Close()
	}
}
