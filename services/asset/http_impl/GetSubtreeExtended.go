package http_impl

import (
	"github.com/bitcoin-sv/ubsv/stores/txmeta"
	"net/http"
	"strings"
	"time"

	"github.com/labstack/echo/v4"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/ordishs/gocore"
)

type SubtreeTxs struct {
	Index        int    `json:"index"`
	TxID         string `json:"txid"`
	InputsCount  int    `json:"inputsCount"`
	OutputsCount int    `json:"outputsCount"`
	Size         int    `json:"size"`
	Fee          int    `json:"fee"`
}

type SubtreeExtended struct {
	Hash    string       `json:"hash"`
	TxCount int          `json:"txCount"`
	Fees    int          `json:"fees"`
	Size    int          `json:"size"`
	Txs     []SubtreeTxs `json:"txs"`
}

func (h *HTTP) GetSubtreeExtended(mode ReadMode) func(c echo.Context) error {
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
				if strings.HasSuffix(err.Error(), " not found") {
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

			data := SubtreeExtended{
				Hash:    hash.String(),
				TxCount: len(subtree.Nodes),
				Fees:    int(subtree.Fees),
				Size:    int(subtree.SizeInBytes),
				Txs:     make([]SubtreeTxs, 0, limit),
			}

			var txMeta *txmeta.Data
			for i := offset; i < offset+limit; i++ {
				if i >= subtree.Length() {
					break
				}
				node := subtree.Nodes[i]

				subtreeData := SubtreeTxs{
					Index: i,
					TxID:  node.Hash.String(),
				}

				txMeta, _ = h.repository.GetTransactionMeta(c.Request().Context(), &node.Hash)
				if txMeta != nil {
					subtreeData.InputsCount = len(txMeta.Tx.Inputs)
					subtreeData.OutputsCount = len(txMeta.Tx.Outputs)
					subtreeData.Size = int(txMeta.SizeInBytes)
					subtreeData.Fee = int(txMeta.Fee)
				}

				data.Txs = append(data.Txs, subtreeData)
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
