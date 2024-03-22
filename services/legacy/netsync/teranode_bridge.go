package netsync

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/bitcoin-sv/ubsv/model"
	"github.com/bitcoin-sv/ubsv/services/blockvalidation"
	"github.com/bitcoin-sv/ubsv/ubsverrors"
	"github.com/bitcoin-sv/ubsv/util"
	"github.com/labstack/echo/v4"
	"github.com/libsv/go-bt/v2"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/ordishs/go-utils/expiringmap"
	"github.com/ordishs/gocore"
	"golang.org/x/sync/errgroup"
)

var (
	once sync.Once
	tb   *TeranodeBridge
)

type TeranodeBridge struct {
	blockValidationClient *blockvalidation.Client
	txCache               *expiringmap.ExpiringMap[chainhash.Hash, []byte]
	subtreeCache          *expiringmap.ExpiringMap[chainhash.Hash, []byte]
	blockCache            *expiringmap.ExpiringMap[chainhash.Hash, []byte]
	baseUrl               string
}

func NewTeranodeBridge(ctx context.Context) *TeranodeBridge {
	listenAddress, ok := gocore.Config().Get("legacy_httpListenAddress")
	if !ok {
		panic("legacy_httpListenAddress not set")
	}

	baseUrl, err, ok := gocore.Config().GetURL("legacy_httpAddress")
	if err != nil {
		panic(err)
	}

	if !ok {
		panic("legacy_httpAddress not set")
	}

	tb := &TeranodeBridge{
		txCache:               expiringmap.New[chainhash.Hash, []byte](20 * time.Minute),
		subtreeCache:          expiringmap.New[chainhash.Hash, []byte](20 * time.Minute),
		blockCache:            expiringmap.New[chainhash.Hash, []byte](20 * time.Minute),
		blockValidationClient: blockvalidation.NewClient(ctx, log),
		baseUrl:               baseUrl.String(),
	}

	e := echo.New()

	e.GET("/block/:hash", tb.BlockHandler)
	e.GET("/subtree/:hash", tb.SubtreeHandler)
	e.GET("/tx/:hash", tb.TxHandler)
	e.POST("/txs", tb.TxBatchHandler())

	go func() {
		if err := e.Start(listenAddress); err != nil {
			log.Errorf("error starting echo server: %s", err)
		}
	}()

	return tb
}

func (tb *TeranodeBridge) HandleBlock(msg *blockMsg) error {
	var size int64

	// stopBlockHash0, _ := chainhash.NewHashFromStr("000000002a22cfee1f2c846adbd12b3e183d4f97683f85dad08a79780a84bd55")
	// if stopBlockHash0.IsEqual(msg.block.Hash()) {
	// 	var i int
	// 	i++
	// 	_ = i
	// }

	// stopBlockHash1, _ := chainhash.NewHashFromStr("00000000d1145790a8694403d4063f323d499e655c83426834d4ce2f8dd4a2ee")
	// if stopBlockHash1.IsEqual(msg.block.Hash()) {
	// 	var i int
	// 	i++
	// 	_ = i
	// }

	txs := msg.block.Transactions()

	subtree, err := util.NewIncompleteTreeByLeafCount(len(txs))
	if err != nil {
		return err
	}

	if err := subtree.AddNode(model.CoinbasePlaceholder, 0, 0); err != nil {
		return err
	}

	for i, tx := range msg.block.Transactions() {
		if i == 0 {
			continue // Skip coinbase tx
		}

		// Add the txid to the subtree
		txHash := *tx.Hash()

		if err := subtree.AddNode(txHash, 0, 0); err != nil {
			return err
		}

		// Serialize the tx
		var txBytes bytes.Buffer
		if err := tx.MsgTx().Serialize(&txBytes); err != nil {
			return err
		}

		size += int64(txBytes.Len())

		// Add the tx to the cache
		tb.txCache.Set(txHash, txBytes.Bytes())
	}

	if subtree.Height > 0 {
		// Add the subtree to the cache
		subtreeBytes, err := subtree.SerializeNodes()
		if err != nil {
			return err
		}

		tb.subtreeCache.Set(*subtree.RootHash(), subtreeBytes)
	}

	// 3. Create a block message with (block hash, coinbase tx and slice if 1 subtree)
	var headerBytes bytes.Buffer
	if err := msg.block.MsgBlock().Header.Serialize(&headerBytes); err != nil {
		return err
	}

	header, err := model.NewBlockHeaderFromBytes(headerBytes.Bytes())
	if err != nil {
		return err
	}

	var coinbase bytes.Buffer
	if err := txs[0].MsgTx().Serialize(&coinbase); err != nil {
		return err
	}

	coinbaseTx, err := bt.NewTxFromBytes(coinbase.Bytes())
	if err != nil {
		return err
	}

	blockSize := msg.block.MsgBlock().SerializeSize()

	subtrees := make([]*chainhash.Hash, 0)
	if subtree.Height > 0 {
		subtrees = append(subtrees, subtree.RootHash())
	}

	block, err := model.NewBlock(header, coinbaseTx, subtrees, uint64(len(txs)), uint64(blockSize))
	if err != nil {
		return err
	}

	blockHash := block.Hash()

	blockBytes, err := block.Bytes()
	if err != nil {
		return err
	}

	tb.blockCache.Set(*blockHash, blockBytes)

	log.Warnf("Block %s received", block)

	if err = tb.blockValidationClient.BlockFound(context.TODO(), blockHash, tb.baseUrl); err != nil {
		log.Errorf("error broadcasting block from %s: %s", tb.baseUrl, err)
	}

	return nil
}

func TeranodeHandler(ctx context.Context) func(msg *blockMsg) error {
	once.Do(func() {
		tb = NewTeranodeBridge(ctx)
	})

	return tb.HandleBlock
}

func (tb *TeranodeBridge) BlockHandler(c echo.Context) error {
	hash, err := chainhash.NewHashFromStr(c.Param("hash"))
	if err != nil {
		return c.JSON(http.StatusBadRequest, "invalid hash")
	}

	// stopBlockHashG, _ := chainhash.NewHashFromStr("000000000019d6689c085ae165831e934ff763ae46a2a6c172b3f1b60a8ce26f")
	// if stopBlockHashG.IsEqual(hash) {
	// 	var i int
	// 	i++
	// 	_ = i
	// }

	// stopBlockHash1, _ := chainhash.NewHashFromStr("00000000839a8e6886ab5951d76f411475428afc90947ee320161bbf18eb6048")
	// if stopBlockHash1.IsEqual(hash) {
	// 	var i int
	// 	i++
	// 	_ = i
	// }

	// stopBlockHash170, _ := chainhash.NewHashFromStr("00000000d1145790a8694403d4063f323d499e655c83426834d4ce2f8dd4a2ee")
	// if stopBlockHash170.IsEqual(hash) {
	// 	var i int
	// 	i++
	// 	_ = i
	// }

	blockBytes, ok := tb.blockCache.Get(*hash)
	if !ok {
		return c.JSON(http.StatusNotFound, "block not found")
	}

	return c.Blob(http.StatusOK, "application/octet-stream", blockBytes)
}

func (tb *TeranodeBridge) SubtreeHandler(c echo.Context) error {
	hash, err := chainhash.NewHashFromStr(c.Param("hash"))
	if err != nil {
		return c.JSON(http.StatusBadRequest, "invalid hash")
	}

	subtreeBytes, ok := tb.subtreeCache.Get(*hash)
	if !ok {
		return c.JSON(http.StatusNotFound, "subtree not found")
	}

	return c.Blob(http.StatusOK, "application/octet-stream", subtreeBytes)
}

func (tb *TeranodeBridge) TxHandler(c echo.Context) error {
	hash, err := chainhash.NewHashFromStr(c.Param("hash"))
	if err != nil {
		return c.JSON(http.StatusBadRequest, "invalid hash")
	}

	tx, ok := tb.txCache.Get(*hash)
	if !ok {
		return c.JSON(http.StatusNotFound, "tx not found")
	}

	return c.Blob(http.StatusOK, "application/octet-stream", tx)
}

func (tb *TeranodeBridge) TxBatchHandler() func(c echo.Context) error {
	return func(c echo.Context) error {
		nrTxAdded := 0

		body := c.Request().Body
		defer body.Close()

		c.Response().Header().Set(echo.HeaderContentType, echo.MIMEOctetStream)

		// Read the body into a 32 byte hashes one by one and stream the tx data back to the client
		g, _ := errgroup.WithContext(c.Request().Context())
		g.SetLimit(1024)

		responseBytes := make([]byte, 0, 32*1024*1024) // 32MB initial capacity
		responseBytesMu := sync.Mutex{}
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

			g.Go(func() error {
				b, ok := tb.txCache.Get(hash)

				if !ok {
					if errors.Is(err, ubsverrors.ErrNotFound) || strings.Contains(err.Error(), "not found") {
						log.Errorf("[GetTransactions][%s] tx not found in repository: %s", hash.String(), err.Error())
						return echo.NewHTTPError(http.StatusNotFound, err.Error())
					} else {
						log.Errorf("[GetTransactions][%s] failed to get tx from repository: %s", hash.String(), err.Error())
						return echo.NewHTTPError(http.StatusInternalServerError, err.Error())
					}
				}

				responseBytesMu.Lock()
				responseBytes = append(responseBytes, b...)
				responseBytesMu.Unlock()

				return nil
			})

			nrTxAdded++
		}

		if err := g.Wait(); err != nil {
			log.Errorf("[GetTransactions] failed to get txs from repository: %s", err.Error())
			return echo.NewHTTPError(http.StatusInternalServerError, err.Error())
		}

		log.Infof("[GetTransactions] sending %d txs to client (%d bytes)", nrTxAdded, len(responseBytes))
		return c.Blob(200, echo.MIMEOctetStream, responseBytes)
	}
}
