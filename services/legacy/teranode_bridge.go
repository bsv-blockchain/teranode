package legacy

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sync"
	"time"

	"github.com/bitcoin-sv/ubsv/model"
	"github.com/bitcoin-sv/ubsv/services/blockchain"
	"github.com/bitcoin-sv/ubsv/services/blockvalidation"
	"github.com/bitcoin-sv/ubsv/services/legacy/bsvutil"
	"github.com/bitcoin-sv/ubsv/services/validator"
	"github.com/bitcoin-sv/ubsv/stores/utxo"
	utxoFactory "github.com/bitcoin-sv/ubsv/stores/utxo/_factory"
	"github.com/bitcoin-sv/ubsv/stores/utxo/meta"
	"github.com/bitcoin-sv/ubsv/ulogger"
	"github.com/bitcoin-sv/ubsv/util"
	"github.com/labstack/echo/v4"
	"github.com/libsv/go-bt/v2"
	"github.com/libsv/go-bt/v2/bscript"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/ordishs/go-utils/expiringmap"
	"github.com/ordishs/gocore"
	"golang.org/x/sync/errgroup"
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
	utxoStore             utxo.Store
	txCache               *expiringmap.ExpiringMap[chainhash.Hash, *wrapper]
	subtreeCache          *expiringmap.ExpiringMap[chainhash.Hash, *wrapper]
	blockCache            *expiringmap.ExpiringMap[chainhash.Hash, *wrapper]
	baseUrl               string
	verifyOnly            bool
	verifyOnlyErrorGroup  *errgroup.Group
	logger                ulogger.Logger
	blockHeight           uint32
}

func NewTeranodeBridge(ctx context.Context, logger ulogger.Logger) (*TeranodeBridge, error) {
	verifyOnly := gocore.Config().GetBool("legacy_verifyOnly", true)
	verifyOnly_concurrency, _ := gocore.Config().GetInt("legacy_verifyOnly_concurrency", 1000)

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
		blockchainClient, err = blockchain.NewClient(ctx, logger)
		if err != nil {
			return nil, fmt.Errorf("error creating blockchain client: %w", err)
		}
		blockvalidationClient = blockvalidation.NewClient(ctx, logger)
	}

	storeURL, err, found := gocore.Config().GetURL("utxostore")
	if err != nil {
		return nil, fmt.Errorf("could not read utxostore: %w", err)
	}
	if !found {
		return nil, fmt.Errorf("could not find utxostore: %w", err)
	}

	utxoStore, err := utxoFactory.NewStore(ctx, logger, storeURL, "teranode_bridge", false)
	if err != nil {
		panic(err)
	}

	verifyOnlyErrorGroup := errgroup.Group{}
	verifyOnlyErrorGroup.SetLimit(verifyOnly_concurrency)

	tb := &TeranodeBridge{
		txCache:               expiringmap.New[chainhash.Hash, *wrapper](1 * time.Minute),
		subtreeCache:          expiringmap.New[chainhash.Hash, *wrapper](1 * time.Minute),
		blockCache:            expiringmap.New[chainhash.Hash, *wrapper](1 * time.Minute),
		blockValidationClient: blockvalidationClient,
		blockchainClient:      blockchainClient,
		baseUrl:               baseUrl.String(),
		utxoStore:             utxoStore,
		verifyOnly:            verifyOnly,
		verifyOnlyErrorGroup:  &verifyOnlyErrorGroup,
		logger:                logger,
	}

	e := echo.New()

	e.GET("/block/:hash", tb.BlockHandler)
	e.GET("/subtree/:hash", tb.SubtreeHandler)
	e.GET("/tx/:hash", tb.TxHandler)
	e.POST("/txs", tb.TxBatchHandler())
	e.GET("/headers/:hash", tb.TempHeaderHandler)

	go func() {
		if err := e.Start(listenAddress); err != nil {
			logger.Errorf("error starting echo server: %s", err)
		}
	}()

	logger.Infof("Teranode bridge started on %s", listenAddress)

	// Get the best block from the chain and re-advertise it to Teranode to ensure it is in sync
	_, meta, err := blockchainClient.GetBestBlockHeader(ctx)
	if err != nil {
		logger.Errorf("Failed to get best block header: %s", err)
	}

	tb.blockHeight = meta.Height

	logger.Infof("Teranode bridge starting at height %d", tb.blockHeight)

	return tb, nil
}

func (tb *TeranodeBridge) HandleBlock(ctx context.Context, block *bsvutil.Block) error {
	if tb.verifyOnly {
		go func() {
			if err := tb.VerifyOnly(block); err != nil {
				tb.logger.Errorf("Failed to verify block: %s", err)
			}
		}()

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
				// Lookup the previous tx in our txCache
				var script *bscript.Script
				var satoshis uint64

				// Using tb.chain.GetUtxoEntry() doesn't work for utxos that are spent in the same block
				w, ok := tb.txCache.Get(*input.PreviousTxIDChainHash())

				if ok {
					prevTx, err := bt.NewTxFromBytes(w.bytes)
					if err != nil {
						return fmt.Errorf("Failed to create bt.Tx for previous tx (%s): %w", input.PreviousTxIDChainHash(), err)
					}

					script = prevTx.Outputs[input.PreviousTxOutIndex].LockingScript
					satoshis = prevTx.Outputs[input.PreviousTxOutIndex].Satoshis

				} else {
					// Get the previous output from the UTXO store
					po := &meta.PreviousOutput{
						PreviousTxID: *input.PreviousTxIDChainHash(),
						Vout:         input.PreviousTxOutIndex,
					}

					err := tb.utxoStore.PreviousOutputsDecorate(context.Background(), []*meta.PreviousOutput{po})
					if err != nil {
						return fmt.Errorf("Failed to lookup previous tx (%s:%d): %w", *input.PreviousTxIDChainHash(), input.PreviousTxOutIndex, err)
					}

					if po.LockingScript == nil || len(po.LockingScript) == 0 {
						return fmt.Errorf("Previous output script is empty for %s:%d", *input.PreviousTxIDChainHash(), input.PreviousTxOutIndex)
					}

					script = bscript.NewFromBytes(po.LockingScript)
					satoshis = uint64(po.Satoshis)
				}

				input.PreviousTxScript = script
				input.PreviousTxSatoshis = satoshis
			}
		}

		tb.txCache.Set(txHash, &wrapper{
			bytes: tx.ExtendedBytes(),
		})

	}

	return tb.SendToTeranode(ctx, block)
}

/* validate every tx and every block using validator.TxValidator.ValidateTransaction() and model.Block.Valid() */
func (tb *TeranodeBridge) VerifyOnly(block *bsvutil.Block) error {
	txs := block.Transactions()
	blockHeight := uint32(block.Height())

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

			if !util.IsExtended(tx, blockHeight) {
				return fmt.Errorf("tx %s is not extended", txHash)
			}

			tb.verifyOnlyErrorGroup.Go(func() error {
				if err := txv.ValidateTransaction(tx, blockHeight); err != nil {
					tb.logger.Errorf("[height %d] tx %s failed validation: %v", blockHeight, txHash, err)
				}
				return nil
			})

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

	tb.verifyOnlyErrorGroup.Go(func() error {
		if _, err := teranodeBlock.Valid(context.TODO(), tb.logger, nil, nil, nil, nil, nil, nil); err != nil {
			tb.logger.Errorf("[height %d] Failed to validate block: %v", blockHeight, err)
		}
		return nil
	})

	return nil
}

/* cache every Tx, create a subtree and cache it, create a teranode block and cache it and finally notify BlockValidator */
func (tb *TeranodeBridge) SendToTeranode(ctx context.Context, block *bsvutil.Block) error {
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

	teranodeBlock, err := model.NewBlock(header, coinbaseTx, subtrees, uint64(len(txs)), uint64(blockSize), uint32(block.Height()))
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

	if err := tb.blockValidationClient.BlockFound(ctx, block.Hash(), tb.baseUrl, true); err != nil {
		return fmt.Errorf("error broadcasting block from %s: %w", tb.baseUrl, err)
	}

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
		tb.logger.Warnf("block %s read %d times", hash, w.readCount)
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
		tb.logger.Warnf("subtree %s read %d times", hash, w.readCount)
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
		tb.logger.Warnf("tx %s read %d times", hash, w.readCount)
	}

	return c.Blob(http.StatusOK, "application/octet-stream", w.bytes)
}

func (tb *TeranodeBridge) TempHeaderHandler(c echo.Context) error {
	// TODO: SAO - remove
	// panic("TernodeBridge has started catchup.")

	tb.logger.Infof("Teranode bridge Header request received")

	hash, err := chainhash.NewHashFromStr(c.Param("hash"))
	if err != nil {
		return c.JSON(http.StatusBadRequest, "invalid hash")
	}

	blockBytes, ok := tb.blockCache.Get(*hash)
	if !ok {
		tb.logger.Errorf("block requested in /headers (%s) was NOT found in blockCache", hash)
		return c.JSON(http.StatusInternalServerError, "/headers not implemented")
	}

	var header *model.BlockHeader

	block, err := model.NewBlockFromBytes(blockBytes.bytes)
	if err != nil {
		return c.JSON(http.StatusInternalServerError, err.Error())
	}

	header = block.Header

	tb.logger.Infof("Teranode bridge Header request response sent")

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
					tb.logger.Errorf("[GetTransactions][%s] failed to read tx hash from body: %s", hash.String(), err.Error())
					return echo.NewHTTPError(http.StatusInternalServerError, err.Error())
				}
			}

			if hash.IsEqual(model.CoinbasePlaceholderHash) {
				continue
			}

			w, ok := tb.txCache.Get(hash)
			if !ok {
				hashStr := hash.String()
				tb.logger.Errorf("[GetTransactions][%s] tx not found in repository", hashStr)
				return echo.NewHTTPError(http.StatusNotFound)
			}

			w.readCount++
			if w.readCount > 1 {
				tb.logger.Warnf("txs %s read %d times", hash, w.readCount)
			}

			responseBytes = append(responseBytes, w.bytes...)

			nrTxAdded++
		}

		tb.logger.Infof("sending %d txs to client (%d bytes)", nrTxAdded, len(responseBytes))
		return c.Blob(200, echo.MIMEOctetStream, responseBytes)
	}
}
