package http_impl

import (
	"net/http"
	"sort"
	"strings"

	"github.com/bitcoin-sv/ubsv/services/utxo/utxostore_api"
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
		start := gocore.CurrentNanos()
		defer func() {
			blobServerStat.NewStat("GetUTXOsByTXID_http").AddTime(start)
		}()

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

		// Create a channel to receive the results from the goroutines
		// that will be created.
		utxoCh := make(chan *UTXOItem)
		utxos := make([]*UTXOItem, 0, len(tx.Outputs))

		waitCh := make(chan struct{})

		go func() {
			for utxo := range utxoCh {
				utxos = append(utxos, utxo)
			}
			close(waitCh)
		}()

		// Create a goroutine for each output in the transaction.
		for i, output := range tx.Outputs {
			safeI, safeOutput := i, output

			g.Go(func() error {
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

				utxoItem := &UTXOItem{
					Txid:          hash,
					Vout:          uint32(safeI),
					LockingScript: safeOutput.LockingScript,
					Satoshis:      safeOutput.Satoshis,
					UtxoHash:      utxoHash,
				}

				if utxoRes != nil && utxoRes.Status != int(utxostore_api.Status_NOT_FOUND) {
					utxoItem.Status = utxostore_api.Status(utxoRes.Status).String()
					utxoItem.SpendingTxID = utxoRes.SpendingTxID
					utxoItem.LockTime = utxoRes.LockTime
				} else {
					utxoItem.Status = utxostore_api.Status_NOT_FOUND.String()
				}

				// Send the UTXO to the channel.
				utxoCh <- utxoItem

				return nil
			})
		}

		if err = g.Wait(); err != nil {
			return err
		}

		close(utxoCh)

		// Now we have to wait for the answers to be added to the slice.
		<-waitCh

		// Sort the UTXOs by the vout.
		sort.SliceStable(utxos, func(i, j int) bool {
			return utxos[i].Vout < utxos[j].Vout
		})

		prometheusBlobServerHttpGetUTXO.WithLabelValues("OK", "200").Inc()

		switch mode {
		case JSON:
			return c.JSONPretty(200, utxos, "  ")
		default:
			return echo.NewHTTPError(http.StatusInternalServerError, "Bad read mode")
		}
	}
}
