package netsync

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
	"github.com/bitcoin-sv/ubsv/services/blockvalidation"
	"github.com/bitcoin-sv/ubsv/services/legacy/blockchain"
	"github.com/bitcoin-sv/ubsv/services/legacy/bsvutil"
	"github.com/bitcoin-sv/ubsv/services/legacy/wire"
	"github.com/bitcoin-sv/ubsv/util"
	"github.com/labstack/echo/v4"
	"github.com/libsv/go-bt/v2"
	"github.com/libsv/go-bt/v2/bscript"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/ordishs/go-utils/expiringmap"
	"github.com/ordishs/gocore"
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
	txCache               *expiringmap.ExpiringMap[chainhash.Hash, *wrapper]
	subtreeCache          *expiringmap.ExpiringMap[chainhash.Hash, *wrapper]
	blockCache            *expiringmap.ExpiringMap[chainhash.Hash, *wrapper]
	baseUrl               string
	lookupFn              func(wire.OutPoint) (*blockchain.UtxoEntry, error)
}

func NewTeranodeBridge(ctx context.Context, lookupFn func(wire.OutPoint) (*blockchain.UtxoEntry, error)) *TeranodeBridge {
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
		txCache:               expiringmap.New[chainhash.Hash, *wrapper](2 * time.Minute),
		subtreeCache:          expiringmap.New[chainhash.Hash, *wrapper](2 * time.Minute),
		blockCache:            expiringmap.New[chainhash.Hash, *wrapper](2 * time.Minute),
		blockValidationClient: blockvalidation.NewClient(ctx, log),
		baseUrl:               baseUrl.String(),
		lookupFn:              lookupFn,
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

	return tb
}

func (tb *TeranodeBridge) HandleBlock(block *bsvutil.Block) error {
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

		// txHashStr := txHash.String()
		// if txHashStr == "0437cd7f8525ceed2324359c2d0ba26006d92d856a9c20fa0241106ee5a597c9" {
		// 	var i int
		// 	i++
		// 	_ = i
		// }

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

			for _, input := range tx.Inputs {
				// Lookup the previous tx in out txCache
				var script *bscript.Script
				var satoshis uint64

				w, ok := tb.txCache.Get(*input.PreviousTxIDChainHash())

				if ok {
					prevTx, err := bt.NewTxFromBytes(w.bytes)
					if err != nil {
						return fmt.Errorf("Failed to create bt.Tx for previous tx (s): %w", *input.PreviousTxIDChainHash(), err)
					}

					script = prevTx.Outputs[input.PreviousTxOutIndex].LockingScript
					satoshis = prevTx.Outputs[input.PreviousTxOutIndex].Satoshis

				} else {
					previousOutput, err := tb.lookupFn(wire.OutPoint{
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

		txBytesExtended := tx.ExtendedBytes()

		// Add the tx to the cache
		tb.txCache.Set(txHash, &wrapper{
			bytes: txBytesExtended,
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

	teranodeBlock, err := model.NewBlock(header, coinbaseTx, subtrees, uint64(len(txs)), uint64(blockSize))
	if err != nil {
		return fmt.Errorf("Failed to create model.NewBlock: %w", err)
	}

	blockHash := teranodeBlock.Hash()

	blockBytes, err := teranodeBlock.Bytes()
	if err != nil {
		return fmt.Errorf("Failed to get block bytes: %w", err)
	}

	tb.blockCache.Set(*blockHash, &wrapper{
		bytes: blockBytes,
	})

	log.Warnf("Block %s received", teranodeBlock)

	if err = tb.blockValidationClient.BlockFound(context.TODO(), blockHash, tb.baseUrl, true); err != nil {
		return fmt.Errorf("error broadcasting block from %s: %w", tb.baseUrl, err)
	}

	return nil
}

func TeranodeHandler(ctx context.Context, lookupFn func(wire.OutPoint) (*blockchain.UtxoEntry, error)) func(msg *bsvutil.Block) error {
	once.Do(func() {
		tb = NewTeranodeBridge(ctx, lookupFn)
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

	stopBlockHash170, _ := chainhash.NewHashFromStr("00000000d1145790a8694403d4063f323d499e655c83426834d4ce2f8dd4a2ee")
	if stopBlockHash170.IsEqual(hash) {
		var i int
		i++
		_ = i
	}

	w, ok := tb.blockCache.Get(*hash)
	if !ok {
		return c.JSON(http.StatusNotFound, "block not found")
	}

	w.readCount++
	log.Warnf("block %s read %d times", hash, w.readCount)

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
	log.Warnf("subtree %s read %d times", hash, w.readCount)

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
	log.Warnf("tx %s read %d times", hash, w.readCount)

	return c.Blob(http.StatusOK, "application/octet-stream", w.bytes)
}

func (tb *TeranodeBridge) TempHeaderHandler(c echo.Context) error {
	hash, err := chainhash.NewHashFromStr(c.Param("hash"))
	if err != nil {
		return c.JSON(http.StatusBadRequest, "invalid hash")
	}

	_, ok := tb.blockCache.Get(*hash)
	if !ok {
		log.Errorf("block requested in /headers (%s) was NOT found in blockCache", hash)
		return c.JSON(http.StatusInternalServerError, "/headers not implemented")
	}

	log.Errorf("block requested in /headers (%s) was found in blockCache", hash)
	return c.JSON(http.StatusInternalServerError, "/headers not implemented")
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

			log.Infof("ReadTXID *********** %s", hash)

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
			log.Warnf("txs %s read %d times SIMON", hash, w.readCount)

			responseBytes = append(responseBytes, w.bytes...)

			nrTxAdded++
		}

		log.Infof("sending %d txs to client (%d bytes)", nrTxAdded, len(responseBytes))
		return c.Blob(200, echo.MIMEOctetStream, responseBytes)
	}
}
