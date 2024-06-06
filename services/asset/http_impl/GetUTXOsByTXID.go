package http_impl

import (
	"net/http"
	"strings"

	"github.com/bitcoin-sv/ubsv/errors"
	"github.com/bitcoin-sv/ubsv/stores/utxo"
	"github.com/bitcoin-sv/ubsv/util"
	"github.com/labstack/echo/v4"
	"github.com/libsv/go-bt/v2"
	"github.com/libsv/go-bt/v2/bscript"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/ordishs/gocore"
	"golang.org/x/sync/errgroup"
)

type UTXOItem struct {
	Txid          *chainhash.Hash `json:"txid"`
	Vout          uint32          `json:"vout"`
	LockingScript *bscript.Script `json:"lockingScript"`
	Satoshis      uint64          `json:"satoshis"`
	UtxoHash      *chainhash.Hash `json:"utxoHash"`
	Status        string          `json:"status"`
	SpendingTxID  *chainhash.Hash `json:"spendingTxId,omitempty"`
	LockTime      uint32          `json:"lockTime,omitempty"`
}

func (h *HTTP) GetUTXOsByTXID(mode ReadMode) func(c echo.Context) error {

	return func(c echo.Context) error {
		start := gocore.CurrentTime()
		defer func() {
			AssetStat.NewStat("GetUTXOsByTXID_http").AddTime(start)
		}()

		h.logger.Debugf("[Asset_http] GetUTXOsByTXID in %s for %s: %s", mode, c.Request().RemoteAddr, c.Param("hash"))
		hash, err := chainhash.NewHashFromStr(c.Param("hash"))
		if err != nil {
			h.logger.Errorf("[Asset_http] GetUTXOsByTXID error creating hash: %s", err.Error())
			return echo.NewHTTPError(http.StatusInternalServerError, err.Error())
		}

		b, err := h.repository.GetTransaction(c.Request().Context(), hash)
		if err != nil {
			h.logger.Errorf("[Asset_http][%s] GetUTXOsByTXID error getting transaction: %s", hash.String(), err.Error())
			if errors.Is(err, errors.ErrNotFound) || strings.Contains(err.Error(), "not found") {
				return echo.NewHTTPError(http.StatusNotFound, err.Error())
			} else {
				return echo.NewHTTPError(http.StatusInternalServerError, err.Error())
			}
		}

		tx, err := bt.NewTxFromBytes(b)
		if err != nil {
			h.logger.Errorf("[Asset_http][%s] GetUTXOsByTXID error creating transaction: %s", hash.String(), err.Error())
			return echo.NewHTTPError(http.StatusInternalServerError, err.Error())
		}

		// Go through all the outputs and get the UTXOHash for each one
		// and then look up the UTXO by that hash.  This is done in parallel
		// to help speed things up.

		g, ctx := errgroup.WithContext(c.Request().Context())

		// Create a channel to receive the results from the goroutines
		// that will be created.
		utxos := make([]*UTXOItem, len(tx.Outputs))

		// Create a goroutine for each output in the transaction.
		for i, output := range tx.Outputs {
			safeI, safeOutput := i, output

			g.Go(func() error {
				// Get the UTXOHash for this output.
				utxoHash, err := util.UTXOHash(hash, uint32(safeI), safeOutput.LockingScript, safeOutput.Satoshis)
				if err != nil {
					return err
				}

				utxoItem := &UTXOItem{
					Txid:          hash,
					Vout:          uint32(safeI),
					LockingScript: safeOutput.LockingScript,
					Satoshis:      safeOutput.Satoshis,
					UtxoHash:      utxoHash,
				}

				// Get the UTXO for this output.
				utxoRes, _ := h.repository.GetUtxo(ctx, &utxo.Spend{
					UTXOHash: utxoHash,
					TxID:     tx.TxIDChainHash(),
					Vout:     uint32(safeI),
				})

				if utxoRes != nil && utxoRes.Status != int(utxo.Status_NOT_FOUND) {
					utxoItem.Status = utxo.Status(utxoRes.Status).String()
					utxoItem.SpendingTxID = utxoRes.SpendingTxID
					utxoItem.LockTime = utxoRes.LockTime
				} else {
					utxoItem.Status = utxo.Status_NOT_FOUND.String()
				}

				// this can be set here, but only directly by index
				utxos[safeI] = utxoItem

				return nil
			})
		}

		if err = g.Wait(); err != nil {
			h.logger.Errorf("[Asset_http][%s] GetUTXOsByTXID error: %s", hash.String(), err.Error())
			return echo.NewHTTPError(http.StatusInternalServerError, err.Error())
		}

		prometheusAssetHttpGetUTXO.WithLabelValues("OK", "200").Inc()

		switch mode {
		case JSON:
			return c.JSONPretty(200, utxos, "  ")
		default:
			h.logger.Errorf("[Asset_http][%s] GetUTXOsByTXID error: Bad read mode", hash.String())
			return echo.NewHTTPError(http.StatusInternalServerError, "Bad read mode")
		}
	}
}
