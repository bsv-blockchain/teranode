package http_impl

import (
	"net/http"
	"strings"

	"github.com/bitcoin-sv/ubsv/util"
	"github.com/labstack/echo/v4"
	"github.com/libsv/go-bt/v2"
	"github.com/libsv/go-bt/v2/bscript"
	"github.com/libsv/go-bt/v2/chainhash"
	"golang.org/x/sync/errgroup"
)

func (h *HTTP) GetUTXOsByTXID(mode ReadMode) func(c echo.Context) error {

	return func(c echo.Context) error {
		h.logger.Debugf("[BlobServer_http] GetUTXOsByTXID in %s for %s: %s", mode, c.Request().RemoteAddr, c.Param("hash"))
		hash, err := chainhash.NewHashFromStr(c.Param("hash"))
		if err != nil {
			return err
		}

		b, err := h.repository.GetTransaction(c.Request().Context(), hash)
		if err != nil {
			if strings.HasSuffix(err.Error(), " not found") {
				return echo.NewHTTPError(http.StatusNotFound, err.Error())
			} else {
				return echo.NewHTTPError(http.StatusInternalServerError, err.Error())
			}
		}

		tx, err := bt.NewTxFromBytes(b)
		if err != nil {
			return err
		}

		// Go through all the outputs and get the UTXOHash for each one
		// and then look up the UTXO by that hash.  This is done in parallel
		// to help speed things up.

		g, ctx := errgroup.WithContext(c.Request().Context())

		type UTXOItem struct {
			Txid          *chainhash.Hash `json:"txid"`
			Vout          uint32          `json:"vout"`
			LockingScript *bscript.Script `json:"lockingScript"`
			Satoshis      uint64          `json:"satoshis"`
			UtxoHash      *chainhash.Hash `json:"utxoHash"`
			Status        int             `json:"status"`
			SpendingTxID  *chainhash.Hash `json:"spendingTxId,omitempty"`
			LockTime      uint32          `json:"lockTime,omitempty"`
		}

		// Create a channel to receive the results from the goroutines
		// that will be created.
		utxoCh := make(chan *UTXOItem, len(tx.Outputs))

		utxos := make([]*UTXOItem, 0, len(tx.Outputs))

		go func() {
			for {
				select {
				case <-ctx.Done():
					return
				case utxo := <-utxoCh:
					utxos = append(utxos, utxo)
				}
			}
		}()

		// Create a goroutine for each output in the transaction.
		for i, output := range tx.Outputs {
			safeI, safeOutput := i, output

			g.Go(func() error {
				select {
				case <-ctx.Done():
					return ctx.Err()
				default:

					// Get the UTXOHash for this output.
					utxoHash, err := util.UTXOHash(hash, uint32(safeI), *safeOutput.LockingScript, safeOutput.Satoshis)
					if err != nil {
						return err
					}

					// Get the UTXO for this output.
					utxoRes, err := h.repository.GetUtxo(ctx, utxoHash)
					if err != nil {
						return err
					}

					// Send the UTXO to the channel.
					utxoCh <- &UTXOItem{
						Txid:          hash,
						Vout:          uint32(safeI),
						LockingScript: safeOutput.LockingScript,
						Satoshis:      safeOutput.Satoshis,
						UtxoHash:      utxoHash,
						Status:        utxoRes.Status,
						SpendingTxID:  utxoRes.SpendingTxID,
						LockTime:      utxoRes.LockTime,
					}
				}

				return nil
			})
		}

		if err := g.Wait(); err != nil {
			return err
		}

		prometheusBlobServerHttpGetUTXO.WithLabelValues("OK", "200").Inc()

		switch mode {
		case JSON:
			return c.JSONPretty(200, utxos, "  ")
		default:
			return echo.NewHTTPError(http.StatusInternalServerError, "Bad read mode")
		}
	}
}
