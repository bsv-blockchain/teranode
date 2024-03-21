package netsync

import (
	"bytes"
	"context"
	"net/http"
	"sync"

	"github.com/bitcoin-sv/ubsv/model"
	"github.com/bitcoin-sv/ubsv/services/blockvalidation"
	"github.com/bitcoin-sv/ubsv/services/legacy/wire"
	"github.com/bitcoin-sv/ubsv/util"
	"github.com/labstack/echo/v4"
	"github.com/libsv/go-bt/v2"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/ordishs/gocore"
)

var once sync.Once

type TeranodeBridge struct {
	blockValidationClient *blockvalidation.Client
	txCache               map[chainhash.Hash]*wire.MsgTx
	subtreeCache          map[chainhash.Hash]*util.Subtree
	blockCache            map[chainhash.Hash]*model.Block
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
		txCache:               make(map[chainhash.Hash]*wire.MsgTx),
		subtreeCache:          make(map[chainhash.Hash]*util.Subtree),
		blockCache:            make(map[chainhash.Hash]*model.Block),
		blockValidationClient: blockvalidation.NewClient(ctx, log),
		baseUrl:               baseUrl.String(),
	}

	e := echo.New()

	e.GET("/block/:hash", tb.BlockHandler)
	e.GET("/subtree/:hash", tb.SubtreeHandler)
	e.GET("/tx/:hash", tb.TxHandler)

	go func() {
		if err := e.Start(listenAddress); err != nil {
			log.Errorf("error starting echo server: %s", err)
		}
	}()

	return tb
}

func (tb *TeranodeBridge) HandleBlock(msg *blockMsg) error {
	var size int64

	txs := msg.block.Transactions()

	st, err := util.NewIncompleteTreeByLeafCount(len(txs))
	if err != nil {
		return err
	}

	if err := st.AddNode(model.CoinbasePlaceholder, 0, 0); err != nil {
		return err
	}

	for i, tx := range msg.block.Transactions() {
		if i == 0 {
			continue // Skip coinbase tx
		}

		tb.txCache[*tx.Hash()] = tx.MsgTx()
		size += int64(tx.MsgTx().SerializeSize())

		if err := st.AddNode(*tx.Hash(), 0, 0); err != nil {
			return err
		}
	}

	tb.subtreeCache[*st.RootHash()] = st

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

	block, err := model.NewBlock(header, coinbaseTx, []*chainhash.Hash{st.RootHash()}, uint64(len(txs)), uint64(blockSize))
	if err != nil {
		return err
	}

	blockHash := block.Hash()

	tb.blockCache[*blockHash] = block

	log.Warnf("Block %s received", block)

	if err = tb.blockValidationClient.BlockFound(context.TODO(), blockHash, tb.baseUrl); err != nil {
		log.Errorf("error broadcasting block from %s: %s", tb.baseUrl, err)
	}

	return nil
}

func TeranodeHandler(ctx context.Context) func(msg *blockMsg) error {
	var tb *TeranodeBridge

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

	block, ok := tb.blockCache[*hash]
	if !ok {
		return c.JSON(http.StatusNotFound, "block not found")
	}

	blockBytes, err := block.Bytes()
	if err != nil {
		return c.JSON(http.StatusInternalServerError, "error serializing block")
	}

	return c.Blob(http.StatusOK, "application/octet-stream", blockBytes)
}

func (tb *TeranodeBridge) SubtreeHandler(c echo.Context) error {
	hash, err := chainhash.NewHashFromStr(c.Param("hash"))
	if err != nil {
		return c.JSON(http.StatusBadRequest, "invalid hash")
	}

	subtree, ok := tb.subtreeCache[*hash]
	if !ok {
		return c.JSON(http.StatusNotFound, "subtree not found")
	}

	subtreeBytes, err := subtree.SerializeNodes()
	if err != nil {
		return c.JSON(http.StatusInternalServerError, "error serializing subtree")
	}

	return c.Blob(http.StatusOK, "application/octet-stream", subtreeBytes)
}

func (tb *TeranodeBridge) TxHandler(c echo.Context) error {
	hash, err := chainhash.NewHashFromStr(c.Param("hash"))
	if err != nil {
		return c.JSON(http.StatusBadRequest, "invalid hash")
	}

	tx, ok := tb.txCache[*hash]
	if !ok {
		return c.JSON(http.StatusNotFound, "tx not found")
	}

	var txBytes bytes.Buffer
	if err := tx.Serialize(&txBytes); err != nil {
		return c.JSON(http.StatusInternalServerError, "error serializing tx")
	}

	return c.Blob(http.StatusOK, "application/octet-stream", txBytes.Bytes())
}
