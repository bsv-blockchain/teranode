package netsync

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/bitcoin-sv/ubsv/model"
	"github.com/bitcoin-sv/ubsv/services/blockchain"
	"github.com/bitcoin-sv/ubsv/services/blockvalidation"
	legacy_blockchain "github.com/bitcoin-sv/ubsv/services/legacy/blockchain"
	"github.com/bitcoin-sv/ubsv/services/legacy/bsvutil"
	"github.com/bitcoin-sv/ubsv/services/validator"
	"github.com/bitcoin-sv/ubsv/stores/txmeta"
	txmetafactory "github.com/bitcoin-sv/ubsv/stores/txmeta/_factory"
	"github.com/bitcoin-sv/ubsv/util"
	"github.com/labstack/echo/v4"
	"github.com/libsv/go-bt/v2"
	"github.com/libsv/go-bt/v2/bscript"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/ordishs/go-utils/expiringmap"
	"github.com/ordishs/gocore"

	"github.com/bitcoin-sv/ubsv/services/legacy/wire"
)

var (
	once sync.Once
	tb   *TeranodeBridge
)

type wrapper struct {
	bytes     []byte
	readCount int
}

type TeranodeBridge struct {
	blockValidationClient *blockvalidation.Client
	blockchainClient      blockchain.ClientI
	txmetaStore           txmeta.Store
	txCache               *expiringmap.ExpiringMap[chainhash.Hash, *wrapper]
	subtreeCache          *expiringmap.ExpiringMap[chainhash.Hash, *wrapper]
	blockCache            *expiringmap.ExpiringMap[chainhash.Hash, *wrapper]
	baseUrl               string
	height                atomic.Uint32
	chain                 *legacy_blockchain.BlockChain
	verifyOnly            bool
}

func NewTeranodeBridge(chain *legacy_blockchain.BlockChain) (*TeranodeBridge, error) {
	verifyOnly := gocore.Config().GetBool("legacy_verifyOnly", true)

	listenAddress, ok := gocore.Config().Get("legacy_httpListenAddress")
	if !ok {
		return nil, errors.New("legacy_httpListenAddress not set")
	}

	baseUrl, err, ok := gocore.Config().GetURL("legacy_httpAddress")
	if err != nil {
		return nil, fmt.Errorf("could not read legacy_httpAddress: %w", err)
	}

	if !ok {
		return nil, errors.New("could not find legacy_httpAddress")
	}

	var blockchainClient blockchain.ClientI
	var blockvalidationClient *blockvalidation.Client
	if !verifyOnly {
		// when verifyOnly is enabled, we want to run legacyServer without any the need for any other ubsv services
		blockchainClient, err = blockchain.NewClient(context.TODO(), log)
		if err != nil {
			return nil, fmt.Errorf("error creating blockchain client: %w", err)
		}
		blockvalidationClient = blockvalidation.NewClient(context.TODO(), log)
	}

	txmetaStoreURL, err, found := gocore.Config().GetURL("txmeta_store")
	if err != nil {
		return nil, fmt.Errorf("could not read txmeta_store: %w", err)
	}
	if !found {
		return nil, fmt.Errorf("could not find txmeta_store: %w", err)
	}

	txmetaStore, err := txmetafactory.New(log, txmetaStoreURL)
	if err != nil {
		panic(err)
	}

	tb := &TeranodeBridge{
		txCache:               expiringmap.New[chainhash.Hash, *wrapper](30 * time.Minute),
		subtreeCache:          expiringmap.New[chainhash.Hash, *wrapper](30 * time.Minute),
		blockCache:            expiringmap.New[chainhash.Hash, *wrapper](30 * time.Minute),
		blockValidationClient: blockvalidationClient,
		blockchainClient:      blockchainClient,
		baseUrl:               baseUrl.String(),
		chain:                 chain,
		txmetaStore:           txmetaStore,
		verifyOnly:            verifyOnly,
	}

	e := echo.New()

	e.GET("/block/:hash", tb.BlockHandler)
	e.GET("/subtree/:hash", tb.SubtreeHandler)
	e.GET("/tx/:hash", tb.TxHandler)
	e.POST("/txs", tb.TxBatchHandler())
	e.GET("/headers/:hash", tb.TempHeaderHandler)

	go func() {
		if err := e.Start(listenAddress); err != nil {
			log.Errorf("error starting echo server: %s", err)
		}
	}()

	log.Infof("Teranode bridge started on %s", listenAddress)

	if !verifyOnly {
		// Get the best block from the chain and re-advertise it to Teranode to ensure it is in sync
		_, meta, err := blockchainClient.GetBestBlockHeader(context.TODO())
		if err != nil {
			log.Errorf("Failed to get best block header: %s", err)
		}

		teranodeHeight := meta.Height
		tb.height.Store(teranodeHeight)

		log.Infof("Teranode bridge starting at height %d", teranodeHeight)

		best := chain.BestSnapshot()
		log.Infof("Legacy chain best block height: %d", best.Height)

		if teranodeHeight < uint32(best.Height) {
			log.Infof("Teranode bridge syncing with legacy...")
		}

		// Start syncing from the last block we have in Teranode + 1
		teranodeHeight++

		sigs := make(chan os.Signal, 1)
		signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

		for teranodeHeight <= uint32(best.Height) {
			select {
			case <-sigs:
				break
			default:
				block, err := chain.BlockByHeight(int32(teranodeHeight))
				if err != nil {
					return nil, fmt.Errorf("Failed to get block by hash %s: %w", best.Hash, err)
				} else {
					if err := tb.HandleBlockConnected(block); err != nil {
						return nil, fmt.Errorf("Failed to handle block connected %s: %s", block.Hash(), err)
					}
				}
				log.Infof("Teranode bridge synced with legacy block %d", teranodeHeight)

				teranodeHeight++
			}
		}

		log.Infof("Teranode bridge synced with legacy")
	}

	return tb, nil
}

func (tb *TeranodeBridge) HandleBlock(block *bsvutil.Block) error {
	if !tb.verifyOnly {
		return nil
	}

	for _, wireTx := range block.Transactions() {
		txHash := *wireTx.Hash()

		// Serialize the tx to standard format
		var txBytes bytes.Buffer
		if err := wireTx.MsgTx().Serialize(&txBytes); err != nil {
			return fmt.Errorf("Could not serialize msgTx: %w", err)
		}

		// Create a bt.Tx from the serialized bytes
		tx, err := bt.NewTxFromBytes(txBytes.Bytes())
		if err != nil {
			return fmt.Errorf("Failed to create bt.Tx: %w", err)
		}

		// Extend the tx with additional information
		if !tx.IsCoinbase() {
			for _, input := range tx.Inputs {
				// Lookup the previous tx in out txCache
				var script *bscript.Script
				var satoshis uint64

				w, ok := tb.txCache.Get(*input.PreviousTxIDChainHash())

				if ok {
					prevTx, err := bt.NewTxFromBytes(w.bytes)
					if err != nil {
						return fmt.Errorf("Failed to create bt.Tx for previous tx (%s): %w", input.PreviousTxIDChainHash(), err)
					}

					script = prevTx.Outputs[input.PreviousTxOutIndex].LockingScript
					satoshis = prevTx.Outputs[input.PreviousTxOutIndex].Satoshis

				} else {
					// This only works before BSVD processes the block.
					// Once processed, the UTXOEntry is spent and removed from the UTXOSet
					previousOutput, err := tb.chain.FetchUtxoEntry(wire.OutPoint{
						Hash:  *input.PreviousTxIDChainHash(),
						Index: input.PreviousTxOutIndex,
					})

					if err != nil {
						return fmt.Errorf("Failed to lookup previous tx (%s:%d): %w", *input.PreviousTxIDChainHash(), input.PreviousTxOutIndex, err)
					}

					if previousOutput == nil {
						return fmt.Errorf("previous output not found for block %s, tx %s, prevTx %s, vout %d", block.Hash(), txHash, input.PreviousTxIDChainHash(), input.PreviousTxOutIndex)
					}

					script = bscript.NewFromBytes(previousOutput.PkScript())
					satoshis = uint64(previousOutput.Amount())
				}

				input.PreviousTxScript = script
				input.PreviousTxSatoshis = satoshis
			}

			if !tx.IsExtended() {
				return fmt.Errorf("tx %s is not extended", txHash)
			}
		}

		tb.txCache.Set(txHash, &wrapper{
			bytes: tx.ExtendedBytes(),
		})

	}

	return nil
}

func (tb *TeranodeBridge) HandleBlockConnected(block *bsvutil.Block) error {
	if tb.verifyOnly {
		go func() {
			if err := tb.VerifyOnly(block); err != nil {
				log.Errorf("Failed to verify block: %s", err)
			}
		}()
		return nil
	}
	return tb.SendToTeranode(block)
}

/* validate every tx and every block using validator.TxValidator.ValidateTransaction() and model.Block.Valid() */
func (tb *TeranodeBridge) VerifyOnly(block *bsvutil.Block) error {
	txs := block.Transactions()

	subtree, err := util.NewIncompleteTreeByLeafCount(len(txs))
	if err != nil {
		return fmt.Errorf("Failed to create subtree: %w", err)
	}

	if err := subtree.AddNode(model.CoinbasePlaceholder, 0, 0); err != nil {
		return fmt.Errorf("Failed to add coinbase placeholder: %w", err)
	}

	txv := validator.TxValidator{}

	for _, wireTx := range block.Transactions() {
		txHash := *wireTx.Hash()

		txBytes, ok := tb.txCache.Get(txHash)
		if !ok {
			return fmt.Errorf("tx %s not found in cache", txHash)
		}

		tx, err := bt.NewTxFromBytes(txBytes.bytes)
		if err != nil {
			return fmt.Errorf("Failed to create bt.Tx: %w", err)
		}

		if !tx.IsCoinbase() {
			if err := subtree.AddNode(txHash, 0, uint64(tx.Size())); err != nil {
				return fmt.Errorf("Failed to add node (%s) to subtree: %w", txHash, err)
			}

			if !tx.IsExtended() {
				return fmt.Errorf("tx %s is not extended", txHash)
			}

			if err := txv.ValidateTransaction(tx, uint32(block.Height())); err != nil {
				return fmt.Errorf("tx %s failed validation: %w", txHash, err)
			}

		}
	}

	var headerBytes bytes.Buffer
	if err := block.MsgBlock().Header.Serialize(&headerBytes); err != nil {
		return fmt.Errorf("Failed to serialize header: %w", err)
	}

	header, err := model.NewBlockHeaderFromBytes(headerBytes.Bytes())
	if err != nil {
		return fmt.Errorf("Failed to create block header from bytes: %w", err)
	}

	var coinbase bytes.Buffer
	if err := txs[0].MsgTx().Serialize(&coinbase); err != nil {
		return fmt.Errorf("Failed to serialize coinbase: %w", err)
	}

	coinbaseTx, err := bt.NewTxFromBytes(coinbase.Bytes())
	if err != nil {
		return fmt.Errorf("Failed to create bt.Tx for coinbase: %w", err)
	}

	blockSize := block.MsgBlock().SerializeSize()

	subtrees := make([]*chainhash.Hash, 0)
	if subtree.Height > 0 {
		subtrees = append(subtrees, subtree.RootHash())
	}

	teranodeBlock, err := model.NewBlock(header, coinbaseTx, subtrees, uint64(len(txs)), uint64(blockSize), 0)
	if err != nil {
		return fmt.Errorf("Failed to create model.NewBlock: %w", err)
	}

	if _, err := teranodeBlock.Valid(context.TODO(), log, nil, nil, nil, nil, nil, nil); err != nil {
		return fmt.Errorf("Failed to validate block: %w", err)
	}

	return nil
}

/* cache every Tx, create a subtree and cache it, create a teranode block and cache it and finally notify BlockValidator */
func (tb *TeranodeBridge) SendToTeranode(block *bsvutil.Block) error {
	teranodeHeight := tb.height.Load()
	legacyHeight := uint32(block.Height())

	if teranodeHeight > 0 && legacyHeight != teranodeHeight+1 {
		log.Infof("HandleBlockConnected received for %s, expected height %d, got %d - IGNORING...", block.Hash(), teranodeHeight+1, legacyHeight)
		return nil
	}

	// log.Warnf("HandleBlockConnected received for %s (height %d)", block.Hash(), legacyHeight)

	var size int64

	txs := block.Transactions()

	subtree, err := util.NewIncompleteTreeByLeafCount(len(txs))
	if err != nil {
		return fmt.Errorf("Failed to create subtree: %w", err)
	}

	if err := subtree.AddNode(model.CoinbasePlaceholder, 0, 0); err != nil {
		return fmt.Errorf("Failed to add coinbase placeholder: %w", err)
	}

	for _, wireTx := range block.Transactions() {
		// Add the txid to the subtree
		txHash := *wireTx.Hash()

		// Serialize the tx
		var txBytes bytes.Buffer
		if err := wireTx.MsgTx().Serialize(&txBytes); err != nil {
			return fmt.Errorf("Could not serialize msgTx: %w", err)
		}

		txSize := uint64(txBytes.Len())
		size += int64(txSize)

		tx, err := bt.NewTxFromBytes(txBytes.Bytes())
		if err != nil {
			return fmt.Errorf("Failed to create bt.Tx: %w", err)
		}

		if !tx.IsCoinbase() {
			if err := subtree.AddNode(txHash, 0, txSize); err != nil {
				return fmt.Errorf("Failed to add node (%s) to subtree: %w", txHash, err)
			}
		}

		// Add the tx to the cache
		tb.txCache.Set(txHash, &wrapper{
			bytes: tx.Bytes(),
		})

	}

	if subtree.Height > 0 {
		// Add the subtree to the cache
		subtreeBytes, err := subtree.SerializeNodes()
		if err != nil {
			return fmt.Errorf("Failed to serialize subtree: %w", err)
		}

		tb.subtreeCache.Set(*subtree.RootHash(), &wrapper{
			bytes: subtreeBytes,
		})
	}

	// 3. Create a block message with (block hash, coinbase tx and slice if 1 subtree)
	var headerBytes bytes.Buffer
	if err := block.MsgBlock().Header.Serialize(&headerBytes); err != nil {
		return fmt.Errorf("Failed to serialize header: %w", err)
	}

	header, err := model.NewBlockHeaderFromBytes(headerBytes.Bytes())
	if err != nil {
		return fmt.Errorf("Failed to create block header from bytes: %w", err)
	}

	var coinbase bytes.Buffer
	if err := txs[0].MsgTx().Serialize(&coinbase); err != nil {
		return fmt.Errorf("Failed to serialize coinbase: %w", err)
	}

	coinbaseTx, err := bt.NewTxFromBytes(coinbase.Bytes())
	if err != nil {
		return fmt.Errorf("Failed to create bt.Tx for coinbase: %w", err)
	}

	blockSize := block.MsgBlock().SerializeSize()

	subtrees := make([]*chainhash.Hash, 0)
	if subtree.Height > 0 {
		subtrees = append(subtrees, subtree.RootHash())
	}

	teranodeBlock, err := model.NewBlock(header, coinbaseTx, subtrees, uint64(len(txs)), uint64(blockSize), legacyHeight)
	if err != nil {
		return fmt.Errorf("Failed to create model.NewBlock: %w", err)
	}

	blockBytes, err := teranodeBlock.Bytes()
	if err != nil {
		return fmt.Errorf("Failed to get block bytes: %w", err)
	}

	tb.blockCache.Set(*block.Hash(), &wrapper{
		bytes: blockBytes,
	})

	if err := tb.blockValidationClient.BlockFound(context.TODO(), block.Hash(), tb.baseUrl, true); err != nil {
		return fmt.Errorf("error broadcasting block from %s: %w", tb.baseUrl, err)
	}

	tb.height.Store(legacyHeight)

	return nil
}

func (tb *TeranodeBridge) BlockHandler(c echo.Context) error {
	hash, err := chainhash.NewHashFromStr(c.Param("hash"))
	if err != nil {
		return c.JSON(http.StatusBadRequest, "invalid hash")
	}

	w, ok := tb.blockCache.Get(*hash)
	if !ok {
		return c.JSON(http.StatusNotFound, "block not found")
	}

	w.readCount++
	if w.readCount > 1 {
		log.Warnf("block %s read %d times", hash, w.readCount)
	}

	return c.Blob(http.StatusOK, "application/octet-stream", w.bytes)
}

func (tb *TeranodeBridge) SubtreeHandler(c echo.Context) error {
	hash, err := chainhash.NewHashFromStr(c.Param("hash"))
	if err != nil {
		return c.JSON(http.StatusBadRequest, "invalid hash")
	}

	w, ok := tb.subtreeCache.Get(*hash)
	if !ok {
		return c.JSON(http.StatusNotFound, "subtree not found")
	}

	w.readCount++
	if w.readCount > 1 {
		log.Warnf("subtree %s read %d times", hash, w.readCount)
	}

	return c.Blob(http.StatusOK, "application/octet-stream", w.bytes)
}

func (tb *TeranodeBridge) TxHandler(c echo.Context) error {
	hash, err := chainhash.NewHashFromStr(c.Param("hash"))
	if err != nil {
		return c.JSON(http.StatusBadRequest, "invalid hash")
	}

	w, ok := tb.txCache.Get(*hash)
	if !ok {
		return c.JSON(http.StatusNotFound, "tx not found")
	}

	w.readCount++
	if w.readCount > 1 {
		log.Warnf("tx %s read %d times", hash, w.readCount)
	}

	return c.Blob(http.StatusOK, "application/octet-stream", w.bytes)
}

func (tb *TeranodeBridge) TempHeaderHandler(c echo.Context) error {
	// TODO: SAO - remove
	// panic("TernodeBridge has started catchup.")

	log.Infof("Teranode bridge Header request received")

	hash, err := chainhash.NewHashFromStr(c.Param("hash"))
	if err != nil {
		return c.JSON(http.StatusBadRequest, "invalid hash")
	}

	blockBytes, ok := tb.blockCache.Get(*hash)
	if !ok {
		log.Errorf("block requested in /headers (%s) was NOT found in blockCache", hash)
		return c.JSON(http.StatusInternalServerError, "/headers not implemented")
	}

	var header *model.BlockHeader

	block, err := model.NewBlockFromBytes(blockBytes.bytes)
	if err != nil {
		return c.JSON(http.StatusInternalServerError, err.Error())
	}

	header = block.Header

	log.Infof("Teranode bridge Header request response sent")

	// switch mode {
	// case BINARY_STREAM:
	return c.Blob(200, echo.MIMEOctetStream, header.Bytes())
	// case HEX:
	// return c.String(200, hex.EncodeToString(header.Bytes()))
	// case JSON:
	// 	headerResponse := &blockHeaderResponse{
	// 		BlockHeader: header,
	// 		Hash:        header.String(),
	// 		Height:      meta.Height,
	// 		TxCount:     meta.TxCount,
	// 		SizeInBytes: meta.SizeInBytes,
	// 		Miner:       meta.Miner,
	// 	}
	// 	return c.JSONPretty(200, headerResponse, "  ")
	// default:
	// 	return echo.NewHTTPError(http.StatusInternalServerError, "Bad read mode")
	// }
}

func (tb *TeranodeBridge) TxBatchHandler() func(c echo.Context) error {
	return func(c echo.Context) error {
		nrTxAdded := 0

		body := c.Request().Body
		defer body.Close()

		c.Response().Header().Set(echo.HeaderContentType, echo.MIMEOctetStream)

		responseBytes := make([]byte, 0, 32*1024*1024) // 32MB initial capacity
		for {
			var hash chainhash.Hash
			_, err := io.ReadFull(body, hash[:])
			if err != nil {
				if errors.Is(err, io.EOF) {
					break
				} else {
					log.Errorf("[GetTransactions][%s] failed to read tx hash from body: %s", hash.String(), err.Error())
					return echo.NewHTTPError(http.StatusInternalServerError, err.Error())
				}
			}

			// log.Infof("ReadTXID *********** %s", hash)

			if hash.IsEqual(model.CoinbasePlaceholderHash) {
				continue
			}

			w, ok := tb.txCache.Get(hash)
			if !ok {
				hashStr := hash.String()
				log.Errorf("[GetTransactions][%s] tx not found in repository", hashStr)
				return echo.NewHTTPError(http.StatusNotFound)
			}

			w.readCount++
			if w.readCount > 1 {
				log.Warnf("txs %s read %d times", hash, w.readCount)
			}

			responseBytes = append(responseBytes, w.bytes...)

			nrTxAdded++
		}

		log.Infof("sending %d txs to client (%d bytes)", nrTxAdded, len(responseBytes))
		return c.Blob(200, echo.MIMEOctetStream, responseBytes)
	}
}
