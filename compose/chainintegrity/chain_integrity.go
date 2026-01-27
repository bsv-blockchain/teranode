// Package chainintegrity provides tools for verifying the integrity of a local blockchain.
// It includes functions to fetch block headers, check node integrity, and compare log file hashes.
// This package is intended for debugging and inspection purposes in a controlled environment.
package chainintegrity

import (
	"bufio"
	"context"
	"crypto/sha256"
	"fmt"
	"io"
	"net/url"
	"os"
	"strings"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/stores/blob"
	"github.com/bsv-blockchain/teranode/stores/blob/options"
	blockchainstore "github.com/bsv-blockchain/teranode/stores/blockchain"
	utxostore "github.com/bsv-blockchain/teranode/stores/utxo"
	utxostorefactory "github.com/bsv-blockchain/teranode/stores/utxo/factory"
	"github.com/bsv-blockchain/teranode/stores/utxo/meta"
	spendpkg "github.com/bsv-blockchain/teranode/stores/utxo/spend"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util"
)

// NodeConfig holds the configuration for connecting to a teranode instance from the host
type NodeConfig struct {
	Name          string
	BlockchainURL string // PostgreSQL URL for blockchain store
	UtxoStoreURL  string // Aerospike URL for UTXO store
	SubtreeStore  string // File path for subtree store
	TxStore       string // File path for tx store
	ExternalStore string // File path for external store (used by UTXO)
}

// nodeConfigs contains hardcoded configurations for the 3 teranode instances
// These match the docker-compose-3blasters.yml port mappings
var nodeConfigs = []NodeConfig{
	{
		Name:          "teranode1",
		BlockchainURL: "postgres://miner1:miner1@localhost:15432/teranode1",
		UtxoStoreURL:  "aerospike://localhost:3100/test?set=utxo&WarmUp=32&ConnectionQueueSize=32&LimitConnectionsToQueueSize=true&MinConnectionsPerNode=8",
		SubtreeStore:  "file://./data/teranode1/subtreestore",
		TxStore:       "file://./data/teranode1/txstore",
		ExternalStore: "file://./data/teranode1/external",
	},
	{
		Name:          "teranode2",
		BlockchainURL: "postgres://miner2:miner2@localhost:15432/teranode2",
		UtxoStoreURL:  "aerospike://localhost:3200/test?set=utxo&WarmUp=32&ConnectionQueueSize=32&LimitConnectionsToQueueSize=true&MinConnectionsPerNode=8",
		SubtreeStore:  "file://./data/teranode2/subtreestore",
		TxStore:       "file://./data/teranode2/txstore",
		ExternalStore: "file://./data/teranode2/external",
	},
	{
		Name:          "teranode3",
		BlockchainURL: "postgres://miner3:miner3@localhost:15432/teranode3",
		UtxoStoreURL:  "aerospike://localhost:3300/test?set=utxo&WarmUp=32&ConnectionQueueSize=32&LimitConnectionsToQueueSize=true&MinConnectionsPerNode=8",
		SubtreeStore:  "file://./data/teranode3/subtreestore",
		TxStore:       "file://./data/teranode3/txstore",
		ExternalStore: "file://./data/teranode3/external",
	},
}

var (
	genesisScript = "04ffff001d0104455468652054696d65732030332f4a616e2f32303039204368616e63656c6c6f72206f6e" +
		"206272696e6b206f66207365636f6e64206261696c6f757420666f722062616e6b73"
)

// BlockSubtree holds information about a block and its subtree
type BlockSubtree struct {
	Block   chainhash.Hash
	Subtree chainhash.Hash
	Index   int
}

// CheckChainIntegrityBaseline verifies the integrity of the blockchain across multiple nodes.
// It fetches block headers from a baseline node, compares them across other nodes, and checks
// the consistency of filtered log file hashes.
//
// Parameters:
// - checkInterval: Interval for checking the chain integrity.
// - alertThreshold: Threshold for raising alerts.
// - debug: Enables debug mode for detailed logging.
// - logfile: Name of the log file to store results.
//
//nolint:gocognit // cognitive complexity too high
func CheckChainIntegrityBaseline(checkInterval int, alertThreshold int, debug bool, logfile string) {
	const errorString = "ERROR"

	fmt.Println("[Baseline] Fetching block headers from Node1...")

	blockHeaders, err := fetchBlockHeaders(nodeConfigs[0], debug, logfile)
	if err != nil {
		panic(fmt.Sprintf("Failed to fetch block headers from Node1: %v", err))
	}

	const minHeight = 100

	if len(blockHeaders) <= minHeight {
		panic("Not enough block headers to baseline!")
	}

	baselineHeaders := blockHeaders[:]
	fmt.Printf("[Baseline] Using %d block headers from Node1\n", len(baselineHeaders))

	for _, nodeConfig := range nodeConfigs {
		fmt.Printf("\n--- Running Baseline ChainIntegrity for %s ---\n", nodeConfig.Name)
		checkNodeIntegrity(nodeConfig, checkInterval, alertThreshold, debug, logfile, baselineHeaders)
	}

	// Hash and compare filtered log files
	fmt.Println("\n[Baseline] Filtering and comparing log file hashes...")

	hashes := make(map[string]string)

	for _, nodeConfig := range nodeConfigs {
		detail := nodeConfig.Name

		logFileName := fmt.Sprintf("%s-%s.log", logfile, detail)

		filteredFileName := fmt.Sprintf("%s-%s.filtered.log", logfile, detail)

		var in *os.File

		in, err = os.Open(logFileName)
		if err != nil {
			fmt.Printf("Error reading log file %s: %v\n", logFileName, err)

			hashes[filteredFileName] = errorString

			continue
		}

		// TODO - defer should not be in a loop, but we need to close the file after processing
		defer func() {
			_ = in.Close()
		}()

		var out *os.File

		out, err = os.Create(filteredFileName)
		if err != nil {
			fmt.Printf("Error creating filtered log file %s: %v\n", filteredFileName, err)

			hashes[filteredFileName] = errorString

			continue
		}

		// TODO - defer should not be in a loop, but we need to close the file after processing
		defer func() {
			_ = out.Close()
		}()

		scanner := bufio.NewScanner(in)
		for scanner.Scan() {
			line := scanner.Text()
			if idx := strings.Index(line, "[[chainintegrity]]"); idx != -1 {
				_, err = out.WriteString(line[idx:] + "\n")
				if err != nil {
					fmt.Printf("Error writing to filtered log file %s: %v\n", filteredFileName, err)

					hashes[filteredFileName] = errorString

					continue
				}
			}
		}

		if err = scanner.Err(); err != nil {
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

		var filteredData []byte

		filteredData, err = os.ReadFile(filteredFileName)
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

	if len(unique) <= 2 {
		fmt.Println("[Baseline] At least two nodes are consistent: filtered log file hashes match for a majority.")
	} else {
		fmt.Println("[Baseline] All filtered log file hashes differ! No majority consensus among nodes.")
	}
}

// CheckChainIntegrity runs chain integrity checks for each node.
// It fetches block headers and verifies their integrity, transactions, and subtrees.
//
// Parameters:
// - checkInterval: Interval for checking the chain integrity.
// - alertThreshold: Threshold for raising alerts.
// - debug: Enables debug mode for detailed logging.
// - logfile: Name of the log file to store results.
func CheckChainIntegrity(checkInterval int, alertThreshold int, debug bool, logfile string) {
	for _, nodeConfig := range nodeConfigs {
		fmt.Printf("\n--- Running ChainIntegrity for %s ---\n", nodeConfig.Name)

		blockHeaders, err := fetchBlockHeaders(nodeConfig, debug, logfile)
		if err != nil {
			panic(fmt.Sprintf("Failed to fetch block headers from %s: %v", nodeConfig.Name, err))
		}

		checkNodeIntegrity(nodeConfig, checkInterval, alertThreshold, debug, logfile, blockHeaders)
	}
}

// fetchBlockHeaders retrieves block headers from the specified node.
// It uses the blockchain database to fetch the best block header and later headers.
//
// Parameters:
// - nodeConfig: Configuration of the node to fetch headers from.
// - debug: Enables debug mode for detailed logging.
// - logfile: Name of the log file to store results.
//
// Returns:
// - A slice of block headers.
// - An error if the operation fails.
func fetchBlockHeaders(nodeConfig NodeConfig, debug bool, logfile string) ([]*model.BlockHeader, error) {
	path, err := os.Getwd()
	if err != nil {
		return nil, err
	}

	if strings.Contains(path, "cmd/chainintegrity") {
		if err = os.Chdir("../../"); err != nil {
			return nil, err
		}
	}

	detailLoggerName := fmt.Sprintf("chainintegrity-fetch-%s", nodeConfig.Name)
	detailLogfile := fmt.Sprintf("%s-fetch-%s.log", logfile, nodeConfig.Name)

	dbgLevel := "INFO"
	if debug {
		dbgLevel = "DEBUG"
	}

	logger := ulogger.New(
		detailLoggerName,
		ulogger.WithLevel(dbgLevel),
		ulogger.WithLoggerType("file"),
		ulogger.WithFilePath(detailLogfile),
	)

	// Parse the blockchain URL directly from config
	blockchainURL, err := url.Parse(nodeConfig.BlockchainURL)
	if err != nil {
		return nil, errors.NewInvalidArgumentError("failed to parse blockchain URL: %v", err)
	}

	// Create minimal settings for the blockchain store
	appSettings := settings.NewSettings()

	var blockchainDB blockchainstore.Store

	blockchainDB, err = blockchainstore.NewStore(logger, blockchainURL, appSettings)
	if err != nil {
		return nil, err
	}

	ctx := context.Background()

	blockAtTestHeight, err := blockchainDB.GetBlockByHeight(ctx, 120)
	if err != nil {
		return nil, err
	}

	blockHeaders, _, err := blockchainDB.GetBlockHeaders(ctx, blockAtTestHeight.Hash(), 100000)
	if err != nil {
		return nil, err
	}

	return blockHeaders, nil
}

// checkNodeIntegrity verifies the integrity of a node's blockchain.
// It checks block headers, transactions, subtrees, and UTXO data for consistency and correctness.
//
// Parameters:
// - nodeConfig: Configuration of the node to check.
// - checkInterval: Interval for checking the chain integrity.
// - alertThreshold: Threshold for raising alerts.
// - debug: Enables debug mode for detailed logging.
// - logfile: Name of the log file to store results.
// - blockHeaders: Slice of block headers to verify.
//
//nolint:gocognit // cognitive complexity too high
func checkNodeIntegrity(nodeConfig NodeConfig, _ int, _ int, debug bool, logfile string,
	blockHeaders []*model.BlockHeader) {
	path, err := os.Getwd()
	if err != nil {
		panic(err)
	}

	// check whether a path contains cmd/chainintegrity or not
	if strings.Contains(path, "cmd/chainintegrity") {
		if err = os.Chdir("../../"); err != nil {
			panic(err)
		}
	}

	detailLoggerName := fmt.Sprintf("chainintegrity-%s", nodeConfig.Name)
	detailLogfile := fmt.Sprintf("%s-%s.log", logfile, nodeConfig.Name)

	dbgLevel := "INFO"
	if debug {
		dbgLevel = "DEBUG"
	}

	logger := ulogger.New(
		detailLoggerName,
		ulogger.WithLevel(dbgLevel),
		ulogger.WithLoggerType("file"),
		ulogger.WithFilePath(detailLogfile),
	)

	loggerContext := fmt.Sprintf("[%s]", "chainintegrity")

	// Create minimal settings and override the UTXO store URL
	appSettings := settings.NewSettings()
	appSettings.UtxoStore.GetBatcherSize = 1
	appSettings.UtxoStore.UtxoBatchSize = 50 // Must match docker setting (utxostore_utxoBatchSize.docker = 50)

	// Parse URLs from config
	blockchainURL, err := url.Parse(nodeConfig.BlockchainURL)
	if err != nil {
		panic(fmt.Sprintf("failed to parse blockchain URL: %v", err))
	}

	utxoStoreURL, err := url.Parse(nodeConfig.UtxoStoreURL + "&externalStore=" + nodeConfig.ExternalStore)
	if err != nil {
		panic(fmt.Sprintf("failed to parse utxo store URL: %v", err))
	}
	appSettings.UtxoStore.UtxoStore = utxoStoreURL

	subtreeStoreURL, err := url.Parse(nodeConfig.SubtreeStore)
	if err != nil {
		panic(fmt.Sprintf("failed to parse subtree store URL: %v", err))
	}

	txStoreURL, err := url.Parse(nodeConfig.TxStore)
	if err != nil {
		panic(fmt.Sprintf("failed to parse tx store URL: %v", err))
	}

	var blockchainDB blockchainstore.Store

	blockchainDB, err = blockchainstore.NewStore(logger, blockchainURL, appSettings)
	if err != nil {
		panic(err)
	}

	hashPrefix := 2

	var subtreeStore blob.Store

	subtreeStore, err = blob.NewStore(logger, subtreeStoreURL, options.WithHashPrefix(hashPrefix))
	if err != nil {
		panic(err)
	}

	var txStore blob.Store

	txStore, err = blob.NewStore(logger, txStoreURL)
	if err != nil {
		panic(err)
	}

	var utxoStore utxostore.Store

	utxoStore, err = utxostorefactory.NewStore(
		context.Background(), logger, appSettings,
		"main", false,
	)
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
				logger.Debugf("[%s] block.CoinbaseTx.Outputs: accessing index %d (len=%d), coinbase tx id %s", loggerContext, vout, len(block.CoinbaseTx.Outputs), block.CoinbaseTx.TxIDChainHash())

				var utxoHash *chainhash.Hash

				//nolint:gosec
				utxoHash, err = util.UTXOHashFromOutput(block.CoinbaseTx.TxIDChainHash(), output, uint32(vout))
				if err != nil {
					logger.Errorf("[%s] failed to get utxo hash for output %d in coinbase %s: %s", loggerContext, vout, block.CoinbaseTx.TxIDChainHash(), err)
					continue
				}

				var utxo *utxostore.SpendResponse

				utxo, err = utxoStore.GetSpend(ctx, &utxostore.Spend{
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
					if utxo.SpendingData != nil {
						metaData := &meta.Data{}
						if err := utxoStore.GetMeta(ctx, utxo.SpendingData.TxID, metaData); err != nil {
							logger.Errorf("[%s] failed to get utxo %s from utxo store: %s", loggerContext, utxoHash, err)
							continue
						}

						if len(metaData.BlockIDs) == 0 {
							// non-mined spending tx obviously won't appear on the other nodes
							// so for the sake of the integtrity check we pretend it is unspent
							// and assume this is what it will be on the other nodes
							utxo.SpendingData = nil
							utxo.Status = 0
						}
					}

					//nolint:gosec
					logger.Debugf("[%s] coinbase vout %d utxo %s exists in utxo store with status %s, spending data %v, locktime %d", loggerContext, vout, utxoHash, utxostore.Status(utxo.Status), utxo.SpendingData, utxo.LockTime)
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
			var (
				subtreeReader io.ReadCloser
				subtree       *subtreepkg.Subtree
			)

			logger.Debugf("[%s] checking subtrees: %d", loggerContext, len(block.Subtrees))

			for _, subtreeHash := range block.Subtrees {
				logger.Debugf("[%s] checking subtree %s", loggerContext, subtreeHash)

				subtreeReader, err = subtreeStore.GetIoReader(ctx, subtreeHash[:], fileformat.FileTypeSubtree)
				if err != nil {
					logger.Errorf("[%s] failed to get subtree %s for block %s: %v", loggerContext, subtreeHash, block, err)
					logger.Debugf("[%s] block dump: %s", loggerContext, block.Header.StringDump())

					os.Exit(1)
				}

				if subtree, err = subtreepkg.NewSubtreeFromReader(subtreeReader); err != nil || subtree == nil {
					logger.Errorf("[%s] failed to parse subtree %s for block %s: %v", loggerContext, subtreeHash, block, err)

					if subtreeReader != nil {
						_ = subtreeReader.Close()
					}

					continue
				}

				if subtreeReader != nil {
					_ = subtreeReader.Close()
				}

				var (
					tx          []byte
					btTx        *bt.Tx
					subtreeFees = uint64(0)
				)

				for nodeIdx, node := range subtree.Nodes {

					logger.Debugf("[%s] subtree.Nodes: accessing index %d (len=%d), tx id %s", loggerContext, nodeIdx, len(subtree.Nodes), node.Hash)

					if !subtreepkg.CoinbasePlaceholderHash.Equal(node.Hash) {
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
						tx, err = txStore.Get(ctx, node.Hash[:], fileformat.FileTypeTx)
						if err != nil {

							var txMeta *meta.Data

							txMeta, err = utxoStore.Get(ctx, &node.Hash)
							if err != nil {
								logger.Errorf("[%s] failed to get transaction %s from txmeta store: %s", loggerContext, node.Hash, err)
								continue
							}

							if len(txMeta.BlockIDs) == 0 {
								logger.Debugf("[%s] transaction %s is not mined", loggerContext, node.Hash)
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

									var utxoHash *chainhash.Hash

									// check that parent inputs are marked as spent by this tx in the utxo store
									utxoHash, err = util.UTXOHashFromInput(input)
									if err != nil {
										logger.Errorf("[%s] failed to get utxo hash for parent tx input %s in transaction %s: %s", loggerContext, input, btTx.TxIDChainHash(), err)
										continue
									}

									var utxo *utxostore.SpendResponse

									utxo, err = utxoStore.GetSpend(ctx, &utxostore.Spend{
										TxID:         input.PreviousTxIDChainHash(),
										SpendingData: spendpkg.NewSpendingData(btTx.TxIDChainHash(), 0),
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
									} else if utxo.SpendingData == nil {
										logger.Errorf("[%s] parent utxo %s does not have a spending data", loggerContext, utxoHash)
									} else if utxo.SpendingData.TxID == nil {
										logger.Errorf("[%s] parent utxo %s does not have a spending data TxID", loggerContext, utxoHash)
									} else if !utxo.SpendingData.TxID.IsEqual(btTx.TxIDChainHash()) {
										logger.Errorf("[%s] parent utxo %s (%s:%d) is not marked as spent by transaction %s instead it is spent by %s", loggerContext, utxoHash, input.PreviousTxIDChainHash(), input.PreviousTxOutIndex, btTx.TxIDChainHash(), utxo.SpendingData.TxID)
									} else {
										if utxo.SpendingData != nil {
											metaData := &meta.Data{}
											if err := utxoStore.GetMeta(ctx, utxo.SpendingData.TxID, metaData); err != nil {
												logger.Errorf("[%s] failed to get utxo %s from utxo store: %s", loggerContext, utxoHash, err)
												continue
											}

											if len(metaData.BlockIDs) == 0 {
												// non-mined spending tx obviously won't appear on the other nodes
												// so for the sake of the integtrity check we pretend it is unspent
												// and assume this is what it will be on the other nodes
												utxo.SpendingData = nil
												utxo.Status = 0
											}
										}

										//nolint:gosec
										logger.Debugf("[%s] transaction %s parent utxo %s exists in utxo store with status %s, spending data %v, locktime %d", loggerContext, btTx.TxIDChainHash(), utxoHash, utxostore.Status(utxo.Status), utxo.SpendingData, utxo.LockTime)
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

							var utxo *utxostore.SpendResponse

							utxo, err = utxoStore.GetSpend(ctx, &utxostore.Spend{
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
								logger.Debugf("[%s] transaction %s vout %d utxo %s exists in utxo store with status %s, spending data %v, locktime %d", loggerContext, btTx.TxIDChainHash(), vout, utxoHash, utxostore.Status(utxo.Status), utxo.SpendingData, utxo.LockTime)
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

						var blockOfChild BlockSubtree

						// check whether this transaction was missing before and write out info if it was
						if blockOfChild, ok = missingParents[node.Hash]; ok {
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

		blockSubsidy := util.GetBlockSubsidyForHeight(height, appSettings.ChainCfgParams)
		if blockFees+blockSubsidy != blockReward {
			logger.Errorf("[%s] block %s has incorrect fees: %d != %d", loggerContext, block.Hash(), blockFees, blockReward)
		} else {
			logger.Debugf("[%s] block %s has %d in fees, subsidy %d", loggerContext, block.Hash(), blockFees, blockSubsidy)
		}
	}
}
