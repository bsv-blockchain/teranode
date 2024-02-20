package http_impl

import (
	"errors"
	"net/http"
	"strings"

	"github.com/bitcoin-sv/ubsv/ubsverrors"

	"github.com/bitcoin-sv/ubsv/util"
	"github.com/labstack/echo/v4"
	"github.com/libsv/go-bt/v2/chainhash"
)

type SubtreeMeta struct {
	TxCount int    `json:"txCount"`
	Hash    string `json:"hash"`
	Index   int    `json:"index"`
	Fee     uint64 `json:"fee"`
	Size    uint64 `json:"size"`
}

func (h *HTTP) GetBlockSubtrees(mode ReadMode) func(c echo.Context) error {
	return func(c echo.Context) error {
		h.logger.Debugf("[Asset_http] GetBlockSubtrees in %s for %s: %s", mode, c.Request().RemoteAddr, c.Param("hash"))
		hash, err := chainhash.NewHashFromStr(c.Param("hash"))
		if err != nil {
			return err
		}

		block, err := h.repository.GetBlockByHash(c.Request().Context(), hash)
		if err != nil {
			if strings.HasSuffix(err.Error(), " not found") {
				return echo.NewHTTPError(http.StatusNotFound, err.Error())
			} else {
				return echo.NewHTTPError(http.StatusInternalServerError, err.Error())
			}
		}

		offset, limit, err := h.getLimitOffset(c)
		if err != nil {
			return echo.NewHTTPError(http.StatusBadRequest, err.Error())
		}

		result := ExtendedResponse{
			Pagination: Pagination{
				Offset:       offset,
				Limit:        limit,
				TotalRecords: len(block.Subtrees),
			},
		}

		// get all the subtrees for the block
		data := make([]SubtreeMeta, 0, limit)
		var subtreeHead *util.Subtree
		var numNodes int
		if len(block.Subtrees) > 0 {
			for i := offset; i < offset+limit; i++ {
				if i >= len(block.Subtrees) {
					break
				}

				subtreeHash := block.Subtrees[i]
				subtreeHead, numNodes, err = h.repository.GetSubtreeHead(c.Request().Context(), subtreeHash)
				if err != nil {
					if errors.Is(err, ubsverrors.ErrNotFound) {
						return echo.NewHTTPError(http.StatusNotFound, err.Error())
					}
					return echo.NewHTTPError(http.StatusInternalServerError, err.Error())
				}

				// do something with the subtree result
				data = append(data, SubtreeMeta{
					Index:   i,
					Hash:    subtreeHash.String(),
					TxCount: numNodes,
					Fee:     subtreeHead.Fees,
					Size:    subtreeHead.SizeInBytes,
				})
			}
		}

		result.Data = data

		prometheusAssetHttpGetBlock.WithLabelValues("OK", "200").Inc()

		if mode == JSON {
			return c.JSONPretty(200, result, "  ")
		}

		return echo.NewHTTPError(http.StatusInternalServerError, "Bad read mode")
	}
}
