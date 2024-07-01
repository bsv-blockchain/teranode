package chainintegrity

import (
	"context"
	"flag"
	"os"
	"strings"

	"github.com/bitcoin-sv/ubsv/model"
	"github.com/bitcoin-sv/ubsv/stores/blob"
	blockchain_store "github.com/bitcoin-sv/ubsv/stores/blockchain"
	utxostore "github.com/bitcoin-sv/ubsv/stores/utxo"
	utxostore_factory "github.com/bitcoin-sv/ubsv/stores/utxo/_factory"
	"github.com/bitcoin-sv/ubsv/ulogger"
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
	Index   int
}

func Start() {
	// turn off all batching in aerospike, in this case it will only slow us down, since we are reading in 1 thread
	gocore.Config().Set("utxostore_batchingEnabled", "false")

	path, err := os.Getwd()
	if err != nil {
		panic(err)
	}

	// check whether path contains cmd/chainintegrity or not
	if strings.Contains(path, "cmd/chainintegrity") {
		if err = os.Chdir("../../"); err != nil {
			panic(err)
		}
	}

	debug := flag.Bool("debug", false, "enable debug logging")
	logfile := flag.String("logfile", "chainintegrity.log", "path to logfile")
	flag.Parse()

	debugLevel := "INFO"
	if *debug {
		debugLevel = "DEBUG"
	}
	var logger = ulogger.New("chainintegrity", ulogger.WithLevel(debugLevel), ulogger.WithLoggerType("file"), ulogger.WithFilePath(*logfile))

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
	subtreeStore, err := blob.NewStore(logger, subtreeStoreUrl)
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
	txStore, err := blob.NewStore(logger, txStoreUrl)
	if err != nil {
		panic(err)
	}

	utxoStoreURL, err, found := gocore.Config().GetURL("utxostore")
	if err != nil {
		panic(err)
	}
	if !found {
		panic("utxostore config not found")
	}
	utxoStore, err := utxostore_factory.NewStore(context.Background(), logger, utxoStoreURL, "main", false)
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
	missingParents := make(map[chainhash.Hash]BlockSubtree)

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
		} else {
			logger.Debugf("block %s has a valid coinbase transaction: %s", block.Hash(), block.CoinbaseTx.TxIDChainHash())
		}

		if block.CoinbaseTx.Inputs[0].UnlockingScript.String() != genesisScript {
			// check that all coinbase utxos were created
			for vout, output := range block.CoinbaseTx.Outputs {
				utxoHash, err := util.UTXOHashFromOutput(block.CoinbaseTx.TxIDChainHash(), output, uint32(vout))
				if err != nil {
					logger.Errorf("failed to get utxo hash for output %d in coinbase %s: %s", vout, block.CoinbaseTx.TxIDChainHash(), err)
					continue
				}
				utxo, err := utxoStore.GetSpend(ctx, &utxostore.Spend{
					TxID: block.CoinbaseTx.TxIDChainHash(),
					Vout: uint32(vout),
					Hash: utxoHash,
				})
				if err != nil {
					logger.Errorf("failed to get utxo %s from utxo store: %s", utxoHash, err)
					continue
				}
				if utxo == nil {
					logger.Errorf("utxo %s does not exist in utxo store", utxoHash)
				} else {
					logger.Debugf("coinbase vout %d utxo %s exists in utxo store with status %s, spending tx %s, locktime %d", vout, utxoHash, utxostore.Status(utxo.Status), utxo.SpendingTxID, utxo.LockTime)
				}
			}

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
				if err != nil || subtree == nil {
					logger.Errorf("failed to parse subtree %s for block %s: %s", subtreeHash, block, err)
					continue
				}

				var tx []byte
				var btTx *bt.Tx
				subtreeFees := uint64(0)

				for nodeIdx, node := range subtree.Nodes {
					nodeIdx := nodeIdx
					node := node
					if !model.CoinbasePlaceholderHash.Equal(node.Hash) {
						logger.Debugf("checking transaction %s", node.Hash)

						// check that the transaction does not already exist in another block
						previousBlockSubtree, ok := transactionMap[node.Hash]
						if ok {
							logger.Debugf("current subtree %s in block %s", subtreeHash, block.Hash())
							logger.Errorf("transaction %s already exists in subtree %s in block %s", node.Hash, previousBlockSubtree.Subtree, previousBlockSubtree.Block)
						} else {
							transactionMap[node.Hash] = BlockSubtree{Block: *block.Hash(), Subtree: *subtreeHash, Index: nodeIdx}
						}

						// check that the transaction exists in the tx store
						tx, err = txStore.Get(ctx, node.Hash[:])
						if err != nil {
							txMeta, err := utxoStore.Get(ctx, &node.Hash)
							if err != nil {
								logger.Errorf("failed to get transaction %s from txmeta store: %s", node.Hash, err)
								continue
							}
							if txMeta.Tx != nil {
								tx = txMeta.Tx.ExtendedBytes()
							} else {
								logger.Errorf("failed to get transaction %s from tx store: %s", node.Hash, err)
								continue
							}
						}

						btTx, err = bt.NewTxFromBytes(tx)
						if err != nil {
							logger.Errorf("failed to parse transaction %s from tx store: %s", node.Hash, err)
							continue
						}

						// check the topological order of the transactions
						for _, input := range btTx.Inputs {
							// the input tx id (parent tx) should already be in the transaction map
							inputHash := chainhash.Hash(input.PreviousTxID())
							if !inputHash.Equal(chainhash.Hash{}) { // coinbase is parent
								_, ok = transactionMap[inputHash]
								if !ok {
									missingParents[inputHash] = BlockSubtree{Block: *block.Hash(), Subtree: *subtreeHash, Index: nodeIdx}
									logger.Errorf("the parent %s does not appear before the transaction %s, in block %s, subtree %s:%d", inputHash, node.Hash.String(), block.Hash(), subtreeHash, nodeIdx)
								} else {
									// check that parent inputs are marked as spent by this tx in the utxo store
									utxoHash, err := util.UTXOHashFromInput(input)
									if err != nil {
										logger.Errorf("failed to get utxo hash for parent tx input %s in transaction %s: %s", input, btTx.TxIDChainHash(), err)
										continue
									}
									utxo, err := utxoStore.GetSpend(ctx, &utxostore.Spend{
										TxID:         input.PreviousTxIDChainHash(),
										SpendingTxID: btTx.TxIDChainHash(),
										Vout:         input.PreviousTxOutIndex,
										Hash:         utxoHash,
									})
									if err != nil {
										logger.Errorf("failed to get parent utxo %s from utxo store: %s", utxoHash, err)
										continue
									}
									if utxo == nil {
										logger.Errorf("parent utxo %s does not exist in utxo store", utxoHash)
									} else if !utxo.SpendingTxID.IsEqual(btTx.TxIDChainHash()) {
										logger.Errorf("parent utxo %s (%s:%d) is not marked as spent by transaction %s instead it is spent by %s", utxoHash, input.PreviousTxIDChainHash(), input.PreviousTxOutIndex, btTx.TxIDChainHash(), utxo.SpendingTxID)
									} else {
										logger.Debugf("transaction %s parent utxo %s exists in utxo store with status %s, spending tx %s, locktime %d", btTx.TxIDChainHash(), utxoHash, utxostore.Status(utxo.Status), utxo.SpendingTxID, utxo.LockTime)
									}
								}
							}
						}

						// check outputs in utxo store
						var utxoHash *chainhash.Hash
						for vout, output := range btTx.Outputs {
							utxoHash, err = util.UTXOHashFromOutput(btTx.TxIDChainHash(), output, uint32(vout))
							if err != nil {
								logger.Errorf("failed to get utxo hash for output %d in transaction %s: %s", vout, btTx.TxIDChainHash(), err)
								continue
							}
							utxo, err := utxoStore.GetSpend(ctx, &utxostore.Spend{
								TxID: btTx.TxIDChainHash(),
								Vout: uint32(vout),
								Hash: utxoHash,
							})
							if err != nil {
								logger.Errorf("failed to get utxo %s from utxo store: %s", utxoHash, err)
								continue
							}
							if utxo == nil {
								logger.Errorf("utxo %s does not exist in utxo store", utxoHash)
							} else {
								logger.Debugf("transaction %s vout %d utxo %s exists in utxo store with status %s, spending tx %s, locktime %d", btTx.TxIDChainHash(), vout, utxoHash, utxostore.Status(utxo.Status), utxo.SpendingTxID, utxo.LockTime)
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

						// check whether this transaction was missing before and write out info if it was
						if blockOfChild, ok := missingParents[node.Hash]; ok {
							logger.Warnf("found missing parent %s in block %s, subtree %s:%d", node.Hash, block.Hash(), subtreeHash, nodeIdx)
							logger.Warnf("-- child was in block %s, subtree %s:%d", blockOfChild.Block, blockOfChild.Subtree, blockOfChild.Index)
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
