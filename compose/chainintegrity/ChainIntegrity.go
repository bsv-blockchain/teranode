package chainintegrity

import (
	"bufio"
	"context"
	"crypto/sha256"
	"fmt"
	"os"
	"strings"

	"github.com/bitcoin-sv/teranode/model"
	"github.com/bitcoin-sv/teranode/settings"
	"github.com/bitcoin-sv/teranode/stores/blob"
	"github.com/bitcoin-sv/teranode/stores/blob/options"
	blockchain_store "github.com/bitcoin-sv/teranode/stores/blockchain"
	utxostore "github.com/bitcoin-sv/teranode/stores/utxo"
	utxostore_factory "github.com/bitcoin-sv/teranode/stores/utxo/_factory"
	"github.com/bitcoin-sv/teranode/ulogger"
	"github.com/bitcoin-sv/teranode/util"
	"github.com/libsv/go-bt/v2"
	"github.com/libsv/go-bt/v2/chainhash"
)

var (
	genesisScript = "04ffff001d0104455468652054696d65732030332f4a616e2f32303039204368616e63656c6c6f72206f6e206272696e6b206f66207365636f6e64206261696c6f757420666f722062616e6b73"
)

type BlockSubtree struct {
	Block   chainhash.Hash
	Subtree chainhash.Hash
	Index   int
}

// Helper to fetch block headers from a node
func fetchBlockHeaders(nodeContext string, debug bool, logfile string) ([]*model.BlockHeader, error) {
	path, err := os.Getwd()
	if err != nil {
		return nil, err
	}

	if strings.Contains(path, "cmd/chainintegrity") {
		if err = os.Chdir("../../"); err != nil {
			return nil, err
		}
	}

	detail := nodeContext

	if idx := strings.LastIndex(nodeContext, "."); idx != -1 {
		detail = nodeContext[idx+1:]
	}

	detailLoggerName := fmt.Sprintf("chainintegrity-fetch-%s", detail)

	detailLogfile := fmt.Sprintf("%s-fetch-%s.log", logfile, detail)

	dbgLevel := "INFO"
	if debug {
		dbgLevel = "DEBUG"
	}

	logger := ulogger.New(detailLoggerName, ulogger.WithLevel(dbgLevel), ulogger.WithLoggerType("file"), ulogger.WithFilePath(detailLogfile))

	tSettings := settings.NewSettings(nodeContext)

	blockchainDB, err := blockchain_store.NewStore(logger, tSettings.BlockChain.StoreURL, tSettings)
	if err != nil {
		return nil, err
	}

	ctx := context.Background()

	bestBlockHeader, _, err := blockchainDB.GetBestBlockHeader(ctx)
	if err != nil {
		return nil, err
	}

	blockHeaders, _, err := blockchainDB.GetBlockHeaders(ctx, bestBlockHeader.Hash(), 100000)
	if err != nil {
		return nil, err
	}

	return blockHeaders, nil
}

// ChainIntegrity handles the chain integrity command logic
//
// Extracted function to check integrity for a single node
//
//nolint:gocognit // cognitive complexity too high
func checkNodeIntegrity(nodeContext string, checkInterval int, alertThreshold int, debug bool, logfile string, blockHeaders []*model.BlockHeader) {
	// turn off all batching in aerospike, in this case it will only slow us down, since we are reading in 1 thread
	tSettings := settings.NewSettings(nodeContext)
	tSettings.UtxoStore.GetBatcherSize = 1

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

	detail := nodeContext
	if idx := strings.LastIndex(nodeContext, "."); idx != -1 {
		detail = nodeContext[idx+1:]
	}

	detailLoggerName := fmt.Sprintf("chainintegrity-%s", detail)
	detailLogfile := fmt.Sprintf("%s-%s.log", logfile, detail)

	dbgLevel := "INFO"
	if debug {
		dbgLevel = "DEBUG"
	}

	logger := ulogger.New(detailLoggerName, ulogger.WithLevel(dbgLevel), ulogger.WithLoggerType("file"), ulogger.WithFilePath(detailLogfile))

	loggerContext := fmt.Sprintf("[%s]", "chainintegrity")

	blockchainDB, err := blockchain_store.NewStore(logger, tSettings.BlockChain.StoreURL, tSettings)
	if err != nil {
		panic(err)
	}

	hashPrefix := 2

	subtreeStore, err := blob.NewStore(logger, tSettings.SubtreeValidation.SubtreeStore, options.WithHashPrefix(hashPrefix))
	if err != nil {
		panic(err)
	}

	txStore, err := blob.NewStore(logger, tSettings.Block.TxStore)
	if err != nil {
		panic(err)
	}

	utxoStore, err := utxostore_factory.NewStore(context.Background(), logger, tSettings, "main", false)
	if err != nil {
		panic(err)
	}

	// ------------------------------------------------------------------------------------------------
	// start the actual tests
	// ------------------------------------------------------------------------------------------------

	ctx := context.Background()

	logger.Infof("[%s] found %d block headers", loggerContext, len(blockHeaders))

	var previousBlockHeader *model.BlockHeader

	for _, blockHeader := range blockHeaders {
		logger.Debugf("[%s] checking block header %s", loggerContext, blockHeader)

		if previousBlockHeader != nil {
			if !previousBlockHeader.HashPrevBlock.IsEqual(blockHeader.Hash()) {
				logger.Errorf("[%s] block header %s does not match previous block header %s", loggerContext, blockHeader.Hash(), previousBlockHeader.HashPrevBlock)
			}
		}

		previousBlockHeader = blockHeader
	}

	logger.Debugf("[%s] block headers are valid", loggerContext)

	transactionMap := make(map[chainhash.Hash]BlockSubtree)

	logger.Infof("[%s] checking blocks / subtrees", loggerContext)

	var block *model.Block

	var height uint32

	var coinbaseHeight uint32

	missingParents := make(map[chainhash.Hash]BlockSubtree)

	// range through the block headers in reverse order, oldest first
	for i := len(blockHeaders) - 1; i >= 0; i-- {
		logger.Debugf("[%s] blockHeaders: accessing index %d (len=%d)", loggerContext, i, len(blockHeaders))

		blockHeader := blockHeaders[i]
		blockFees := uint64(0)

		block, height, err = blockchainDB.GetBlock(ctx, blockHeader.Hash())
		if err != nil {
			logger.Errorf("[%s] failed to get block %s: %s", loggerContext, blockHeader.Hash(), err)
			continue
		}

		logger.Debugf("[%s] checking block %s", loggerContext, block.Hash())

		if block.CoinbaseTx == nil || !block.CoinbaseTx.IsCoinbase() {
			logger.Errorf("[%s] block %s does not have a valid coinbase transaction", loggerContext, block.Hash())
		} else {
			logger.Debugf("[%s] block %s has a valid coinbase transaction: %s", loggerContext, block.Hash(), block.CoinbaseTx.TxIDChainHash())
		}

		if block.CoinbaseTx.Inputs[0].UnlockingScript.String() != genesisScript {
			// check that all coinbase utxos were created
			for vout, output := range block.CoinbaseTx.Outputs {
				logger.Debugf("[%s] block.CoinbaseTx.Outputs: accessing index %d (len=%d)", loggerContext, vout, len(block.CoinbaseTx.Outputs))

				//nolint:gosec
				utxoHash, err := util.UTXOHashFromOutput(block.CoinbaseTx.TxIDChainHash(), output, uint32(vout))
				if err != nil {
					logger.Errorf("[%s] failed to get utxo hash for output %d in coinbase %s: %s", loggerContext, vout, block.CoinbaseTx.TxIDChainHash(), err)
					continue
				}

				utxo, err := utxoStore.GetSpend(ctx, &utxostore.Spend{
					TxID: block.CoinbaseTx.TxIDChainHash(),
					//nolint:gosec
					Vout:     uint32(vout),
					UTXOHash: utxoHash,
				})
				if err != nil {
					logger.Errorf("[%s] failed to get utxo %s from utxo store: %s", loggerContext, utxoHash, err)
					continue
				}

				if utxo == nil {
					logger.Errorf("[%s] utxo %s does not exist in utxo store", loggerContext, utxoHash)
				} else {
					//nolint:gosec
					logger.Debugf("[%s] coinbase vout %d utxo %s exists in utxo store with status %s, spending tx %s, locktime %d", loggerContext, vout, utxoHash, utxostore.Status(utxo.Status), utxo.SpendingTxID, utxo.LockTime)
				}
			}

			coinbaseHeight, err = util.ExtractCoinbaseHeight(block.CoinbaseTx)
			if err != nil {
				logger.Errorf("[%s] failed to extract coinbase height from block coinbase %s: %s", loggerContext, block.Hash(), err)
			}

			if coinbaseHeight != height {
				logger.Errorf("[%s] coinbase height %d does not match block height %d", loggerContext, coinbaseHeight, height)
			}

			// add the coinbase to the map
			transactionMap[*block.CoinbaseTx.TxIDChainHash()] = BlockSubtree{Block: *block.Hash()}

			// check subtrees
			var subtreeBytes []byte

			var subtree *util.Subtree

			logger.Debugf("[%s] checking subtrees: %d", loggerContext, len(block.Subtrees))

			for _, subtreeHash := range block.Subtrees {
				logger.Debugf("[%s] checking subtree %s", loggerContext, subtreeHash)

				subtreeBytes, err = subtreeStore.Get(ctx, subtreeHash[:], options.WithFileExtension("subtree"))
				if err != nil {
					logger.Errorf("[%s] failed to get subtree %s for block %s: %s", loggerContext, subtreeHash, block, err)
					logger.Debugf("[%s] block dump: %s", block.Header.StringDump())
				}

				subtree, err = util.NewSubtreeFromBytes(subtreeBytes)
				if err != nil || subtree == nil {
					logger.Errorf("[%s] failed to parse subtree %s for block %s: %s", loggerContext, subtreeHash, block, err)
					continue
				}

				var tx []byte

				var btTx *bt.Tx

				subtreeFees := uint64(0)

				for nodeIdx, node := range subtree.Nodes {
					nodeIdx := nodeIdx
					node := node

					logger.Debugf("[%s] subtree.Nodes: accessing index %d (len=%d)", loggerContext, nodeIdx, len(subtree.Nodes))

					if !util.CoinbasePlaceholderHash.Equal(node.Hash) {
						logger.Debugf("[%s] checking transaction %s", loggerContext, node.Hash)

						// check that the transaction does not already exist in another block
						previousBlockSubtree, ok := transactionMap[node.Hash]
						if ok {
							logger.Debugf("[%s] current subtree %s in block %s", loggerContext, subtreeHash, block.Hash())
							logger.Errorf("[%s] transaction %s already exists in subtree %s in block %s", loggerContext, node.Hash, previousBlockSubtree.Subtree, previousBlockSubtree.Block)
						} else {
							transactionMap[node.Hash] = BlockSubtree{Block: *block.Hash(), Subtree: *subtreeHash, Index: nodeIdx}
						}

						// check that the transaction exists in the tx store
						tx, err = txStore.Get(ctx, node.Hash[:])
						if err != nil {
							txMeta, err := utxoStore.Get(ctx, &node.Hash)
							if err != nil {
								logger.Errorf("[%s] failed to get transaction %s from txmeta store: %s", loggerContext, node.Hash, err)
								continue
							}

							if txMeta.Tx != nil {
								tx = txMeta.Tx.ExtendedBytes()
							} else {
								logger.Errorf("[%s] failed to get transaction %s from tx store: %s", loggerContext, node.Hash, err)
								continue
							}
						}

						btTx, err = bt.NewTxFromBytes(tx)
						if err != nil {
							logger.Errorf("[%s] failed to parse transaction %s from tx store: %s", loggerContext, node.Hash, err)
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
									logger.Errorf("[%s] the parent %s does not appear before the transaction %s, in block %s, subtree %s:%d", loggerContext, inputHash, node.Hash.String(), block.Hash(), subtreeHash, nodeIdx)
								} else {
									// check that parent inputs are marked as spent by this tx in the utxo store
									utxoHash, err := util.UTXOHashFromInput(input)
									if err != nil {
										logger.Errorf("[%s] failed to get utxo hash for parent tx input %s in transaction %s: %s", loggerContext, input, btTx.TxIDChainHash(), err)
										continue
									}

									utxo, err := utxoStore.GetSpend(ctx, &utxostore.Spend{
										TxID:         input.PreviousTxIDChainHash(),
										SpendingTxID: btTx.TxIDChainHash(),
										Vout:         input.PreviousTxOutIndex,
										UTXOHash:     utxoHash,
									})
									if err != nil {
										logger.Errorf("[%s] failed to get parent utxo %s from utxo store: %s", loggerContext, utxoHash, err)
										continue
									}
									//nolint:gocritic // rewrite to switch
									if utxo == nil {
										logger.Errorf("[%s] parent utxo %s does not exist in utxo store", loggerContext, utxoHash)
									} else if !utxo.SpendingTxID.IsEqual(btTx.TxIDChainHash()) {
										logger.Errorf("[%s] parent utxo %s (%s:%d) is not marked as spent by transaction %s instead it is spent by %s", loggerContext, utxoHash, input.PreviousTxIDChainHash(), input.PreviousTxOutIndex, btTx.TxIDChainHash(), utxo.SpendingTxID)
									} else {
										//nolint:gosec
										logger.Debugf("[%s] transaction %s parent utxo %s exists in utxo store with status %s, spending tx %s, locktime %d", loggerContext, btTx.TxIDChainHash(), utxoHash, utxostore.Status(utxo.Status), utxo.SpendingTxID, utxo.LockTime)
									}
								}
							}
						}

						// check outputs in utxo store
						var utxoHash *chainhash.Hash

						for vout, output := range btTx.Outputs {
							logger.Debugf("[%s] btTx.Outputs: accessing index %d (len=%d)", loggerContext, vout, len(btTx.Outputs))

							//nolint:gosec
							utxoHash, err = util.UTXOHashFromOutput(btTx.TxIDChainHash(), output, uint32(vout))
							if err != nil {
								logger.Errorf("[%s] failed to get utxo hash for output %d in transaction %s: %s", loggerContext, vout, btTx.TxIDChainHash(), err)
								continue
							}

							utxo, err := utxoStore.GetSpend(ctx, &utxostore.Spend{
								TxID: btTx.TxIDChainHash(),
								//nolint:gosec
								Vout:     uint32(vout),
								UTXOHash: utxoHash,
							})
							if err != nil {
								logger.Errorf("[%s] failed to get utxo %s from utxo store: %s", loggerContext, utxoHash, err)
								continue
							}

							if utxo == nil {
								logger.Errorf("[%s] utxo %s does not exist in utxo store", loggerContext, utxoHash)
							} else {
								//nolint:gosec
								logger.Debugf("[%s] transaction %s vout %d utxo %s exists in utxo store with status %s, spending tx %s, locktime %d", loggerContext, btTx.TxIDChainHash(), vout, utxoHash, utxostore.Status(utxo.Status), utxo.SpendingTxID, utxo.LockTime)
							}
						}

						// the coinbase fees are calculated differently to check if everything matches up
						if !btTx.IsCoinbase() {
							fees, err := util.GetFees(btTx)
							if err != nil {
								logger.Errorf("[%s] failed to get the fees for tx: %s", loggerContext, btTx.String())
								continue
							}

							subtreeFees += fees
						}

						// check whether this transaction was missing before and write out info if it was
						if blockOfChild, ok := missingParents[node.Hash]; ok {
							logger.Warnf("[%s] found missing parent %s in block %s, subtree %s:%d", loggerContext, node.Hash, block.Hash(), subtreeHash, nodeIdx)
							logger.Warnf("[%s] -- child was in block %s, subtree %s:%d", loggerContext, blockOfChild.Block, blockOfChild.Subtree, blockOfChild.Index)
						}
					}
				}

				logger.Debugf("[%s] subtree %s has %d transactions and %d in fees", loggerContext, subtreeHash, len(subtree.Nodes), subtreeFees)

				if subtreeFees != subtree.Fees {
					logger.Errorf("[%s] subtree %s has incorrect fees: %d != %d", loggerContext, subtreeHash, subtreeFees, subtree.Fees)
				}

				blockFees += subtreeFees
			}
		}

		blockReward := block.CoinbaseTx.TotalOutputSatoshis()

		blockSubsidy := util.GetBlockSubsidyForHeight(height, tSettings.ChainCfgParams)
		if blockFees+blockSubsidy != blockReward {
			logger.Errorf("[%s] block %s has incorrect fees: %d != %d", loggerContext, block.Hash(), blockFees, blockReward)
		} else {
			logger.Debugf("[%s] block %s has %d in fees, subsidy %d", loggerContext, block.Hash(), blockFees, blockSubsidy)
		}
	}
}

// ChainIntegrity Test against a baseline node
// Gets all block headers from a node and tests for chain integrity in all 3 nodes
// Hashes the filtered log files and tests if the hashes are the same
func ChainIntegrityBaseline(checkInterval int, alertThreshold int, debug bool, logfile string) {
	const errorString = "ERROR"

	nodeContexts := []string{"docker.host.teranode1", "docker.host.teranode2", "docker.host.teranode3"}

	fmt.Println("[Baseline] Fetching block headers from Node1...")

	blockHeaders, err := fetchBlockHeaders(nodeContexts[0], debug, logfile)
	if err != nil {
		panic(fmt.Sprintf("Failed to fetch block headers from Node1: %v", err))
	}

	if len(blockHeaders) <= 10 {
		panic("Not enough block headers to baseline!")
	}

	baselineHeaders := blockHeaders[:len(blockHeaders)-10]
	fmt.Printf("[Baseline] Using %d block headers from Node1\n", len(baselineHeaders))

	for _, nodeContext := range nodeContexts {
		fmt.Printf("\n--- Running Baseline ChainIntegrity for %s ---\n", nodeContext)
		checkNodeIntegrity(nodeContext, checkInterval, alertThreshold, debug, logfile, baselineHeaders)
	}

	// Hash and compare filtered log files
	fmt.Println("\n[Baseline] Filtering and comparing log file hashes...")

	hashes := make(map[string]string)

	for _, nodeContext := range nodeContexts {
		detail := nodeContext
		if idx := strings.LastIndex(nodeContext, "."); idx != -1 {
			detail = nodeContext[idx+1:]
		}

		logFileName := fmt.Sprintf("%s-%s.log", logfile, detail)

		filteredFileName := fmt.Sprintf("%s-%s.filtered.log", logfile, detail)

		in, err := os.Open(logFileName)
		if err != nil {
			fmt.Printf("Error reading log file %s: %v\n", logFileName, err)

			hashes[filteredFileName] = errorString

			continue
		}

		defer in.Close()

		out, err := os.Create(filteredFileName)
		if err != nil {
			fmt.Printf("Error creating filtered log file %s: %v\n", filteredFileName, err)

			hashes[filteredFileName] = errorString

			continue
		}

		defer out.Close()

		scanner := bufio.NewScanner(in)
		for scanner.Scan() {
			line := scanner.Text()
			if idx := strings.Index(line, "[[chainintegrity]]"); idx != -1 {
				_, err := out.WriteString(line[idx:] + "\n")
				if err != nil {
					fmt.Printf("Error writing to filtered log file %s: %v\n", filteredFileName, err)

					hashes[filteredFileName] = errorString

					continue
				}
			}
		}

		if err := scanner.Err(); err != nil {
			fmt.Printf("Error scanning log file %s: %v\n", logFileName, err)

			hashes[filteredFileName] = errorString

			continue
		}

		err = out.Sync()
		if err != nil {
			fmt.Printf("Error syncing filtered log file %s: %v\n", filteredFileName, err)

			hashes[filteredFileName] = errorString

			continue
		}

		filteredData, err := os.ReadFile(filteredFileName)
		if err != nil {
			fmt.Printf("Error reading filtered log file %s: %v\n", filteredFileName, err)

			hashes[filteredFileName] = errorString

			continue
		}

		hash := fmt.Sprintf("%x", sha256.Sum256(filteredData))
		hashes[filteredFileName] = hash
	}

	// Print hashes
	for file, hash := range hashes {
		fmt.Printf("%s: %s\n", file, hash)
	}

	// Check if all hashes are equal
	unique := make(map[string]struct{})
	for _, hash := range hashes {
		unique[hash] = struct{}{}
	}

	if len(unique) == 1 {
		fmt.Println("[Baseline] All nodes are consistent: filtered log file hashes match.")
	} else {
		fmt.Println("[Baseline] Filtered log file hashes differ! Nodes are NOT consistent.")
	}
}

// ChainIntegrity handles the chain integrity command logic (original flavor)
func ChainIntegrity(checkInterval int, alertThreshold int, debug bool, logfile string) {
	nodeContexts := []string{"docker.host.teranode1", "docker.host.teranode2", "docker.host.teranode3"}
	for _, nodeContext := range nodeContexts {
		fmt.Printf("\n--- Running ChainIntegrity for %s ---\n", nodeContext)

		blockHeaders, err := fetchBlockHeaders(nodeContext, debug, logfile)
		if err != nil {
			panic(fmt.Sprintf("Failed to fetch block headers from %s: %v", nodeContext, err))
		}

		checkNodeIntegrity(nodeContext, checkInterval, alertThreshold, debug, logfile, blockHeaders)
	}
}
