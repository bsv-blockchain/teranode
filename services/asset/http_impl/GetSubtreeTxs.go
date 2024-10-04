package http_impl

import (
	"net/http"
	"strings"
	"time"

	"github.com/bitcoin-sv/ubsv/errors"
	"github.com/bitcoin-sv/ubsv/stores/utxo/meta"
	"github.com/labstack/echo/v4"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/ordishs/gocore"
)

type SubtreeTx struct {
	Index        int    `json:"index"`
	TxID         string `json:"txid"`
	InputsCount  int    `json:"inputsCount"`
	OutputsCount int    `json:"outputsCount"`
	Size         int    `json:"size"`
	Fee          int    `json:"fee"`
}

func (h *HTTP) GetSubtreeTxs(mode ReadMode) func(c echo.Context) error {
	return func(c echo.Context) error {
		var b []byte

		start := gocore.CurrentTime()
		stat := AssetStat.NewStat("GetSubtree_http")

		defer func() {
			stat.AddTime(start)
			duration := time.Since(start)
			sizeInKB := float64(len(b)) / 1024

			h.logger.Infof("[Asset_http] GetSubtree in %s for %s (%.2f kB): %s DONE in %s (%.2f kB/sec)", mode, c.Request().RemoteAddr, c.Param("hash"), sizeInKB, duration, calculateSpeed(duration, sizeInKB))
		}()

		h.logger.Infof("[Asset_http] GetSubtree in %s for %s: %s", mode, c.Request().RemoteAddr, c.Param("hash"))
		hash, err := chainhash.NewHashFromStr(c.Param("hash"))
		if err != nil {
			return echo.NewHTTPError(http.StatusInternalServerError, err.Error())
		}

		prometheusAssetHttpGetSubtree.WithLabelValues("OK", "200").Inc()

		if mode == JSON {
			start2 := gocore.CurrentTime()
			// get subtree is much less efficient than get subtree reader and then only deserializing the nodes
			// this is only needed for the json response
			subtree, err := h.repository.GetSubtree(c.Request().Context(), hash)
			if err != nil {
				if errors.Is(err, errors.ErrNotFound) || strings.Contains(err.Error(), "not found") {
					return echo.NewHTTPError(http.StatusNotFound, err.Error())
				} else {
					return echo.NewHTTPError(http.StatusInternalServerError, err.Error())
				}
			}
			_ = stat.NewStat("Get Subtree from repository").AddTime(start2)

			offset, limit, err := h.getLimitOffset(c)
			if err != nil {
				return echo.NewHTTPError(http.StatusBadRequest, err.Error())
			}

			data := make([]SubtreeTx, 0, limit)

			var txMeta *meta.Data

			for i := offset; i < offset+limit; i++ {
				if i >= subtree.Length() {
					break
				}
				node := subtree.Nodes[i]

				subtreeData := SubtreeTx{
					Index: i,
					TxID:  node.Hash.String(),
				}

				txMeta, err = h.repository.GetTransactionMeta(c.Request().Context(), &node.Hash)
				if err != nil {
					h.logger.Errorf("[GetSubtreeTxs][%s] error getting transaction meta: %s", node.Hash.String(), err.Error())
					continue
				}
				if txMeta.Tx == nil {
					h.logger.Errorf("[GetSubtreeTxs][%s] transaction meta is nil", node.Hash.String())
					continue
				}
				subtreeData.InputsCount = len(txMeta.Tx.Inputs)
				subtreeData.OutputsCount = len(txMeta.Tx.Outputs)
				subtreeData.Size = int(txMeta.SizeInBytes)
				subtreeData.Fee = int(txMeta.Fee)

				data = append(data, subtreeData)
			}

			response := ExtendedResponse{
				Data: data,
				Pagination: Pagination{
					Offset:       offset,
					Limit:        limit,
					TotalRecords: subtree.Length(),
				},
			}

			h.logger.Infof("[GetSubtree][%s] sending to client in json (%d nodes)", hash.String(), subtree.Length())
			return c.JSONPretty(200, response, "  ")
		}

		return echo.NewHTTPError(http.StatusInternalServerError, "Bad read mode")
	}
}
