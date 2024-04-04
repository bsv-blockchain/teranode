package http_impl

import (
	"encoding/hex"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/bitcoin-sv/ubsv/errors"
	"github.com/bitcoin-sv/ubsv/model"
	"github.com/labstack/echo/v4"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/ordishs/gocore"
)

type BlockExtended struct {
	*model.Block
	NextBlock *chainhash.Hash `json:"nextblock"`
}

func (h *HTTP) GetBlockByHeight(mode ReadMode) func(c echo.Context) error {
	return func(c echo.Context) error {
		start := gocore.CurrentTime()
		defer func() {
			AssetStat.NewStat("GetBlock_http").AddTime(start)
			h.logger.Infof("[Asset_http] GetBlockByHeight in %s for %s: %s DONE in %s", mode, c.Request().RemoteAddr, c.Param("height"), time.Since(start))
		}()

		h.logger.Infof("[Asset_http] GetBlockByHeight in %s for %s: %s", mode, c.Request().RemoteAddr, c.Param("height"))
		height, err := strconv.ParseUint(c.Param("height"), 10, 64)
		if err != nil {
			return echo.NewHTTPError(http.StatusBadRequest, err.Error())
		}

		block, err := h.repository.GetBlockByHeight(c.Request().Context(), uint32(height))
		if err != nil {
			if errors.Is(err, errors.ErrNotFound) || strings.Contains(err.Error(), "not found") {
				return echo.NewHTTPError(http.StatusNotFound, err.Error())
			} else {
				return echo.NewHTTPError(http.StatusInternalServerError, err.Error())
			}
		}
		prometheusAssetHttpGetBlock.WithLabelValues("OK", "200").Inc()

		if mode == JSON {
			// get next hash to include in response
			var nextBlockHash *chainhash.Hash
			nextBlock, _ := h.repository.GetBlockByHeight(c.Request().Context(), uint32(height+1))
			if nextBlock != nil {
				nextBlockHash = nextBlock.Hash()
			}

			blockExtended := BlockExtended{
				Block:     block,
				NextBlock: nextBlockHash,
			}
			return c.JSONPretty(200, blockExtended, "  ")
		}

		b, err := block.Bytes()
		if err != nil {
			return echo.NewHTTPError(http.StatusBadRequest, err.Error())
		}

		switch mode {
		case BINARY_STREAM:
			return c.Blob(200, echo.MIMEOctetStream, b)
		case HEX:
			return c.String(200, hex.EncodeToString(b))
		default:
			return echo.NewHTTPError(http.StatusInternalServerError, "Bad read mode")
		}
	}
}

func (h *HTTP) GetBlockByHash(mode ReadMode) func(c echo.Context) error {
	return func(c echo.Context) error {
		h.logger.Debugf("[Asset_http] GetBlockByHash in %s for %s: %s", mode, c.Request().RemoteAddr, c.Param("hash"))
		hash, err := chainhash.NewHashFromStr(c.Param("hash"))
		if err != nil {
			return err
		}

		block, err := h.repository.GetBlockByHash(c.Request().Context(), hash)
		if err != nil {
			if errors.Is(err, errors.ErrNotFound) || strings.Contains(err.Error(), "not found") {
				return echo.NewHTTPError(http.StatusNotFound, err.Error())
			} else {
				return echo.NewHTTPError(http.StatusInternalServerError, err.Error())
			}
		}

		prometheusAssetHttpGetBlock.WithLabelValues("OK", "200").Inc()

		if mode == JSON {
			height := uint32(0)
			_, blockMeta, _ := h.repository.GetBlockHeader(c.Request().Context(), hash)
			if blockMeta != nil {
				height = blockMeta.Height
			}

			// get next hash to include in response
			var nextBlockHash *chainhash.Hash
			nextBlock, _ := h.repository.GetBlockByHeight(c.Request().Context(), height+1)
			if nextBlock != nil {
				nextBlockHash = nextBlock.Hash()
			}

			blockExtended := BlockExtended{
				Block:     block,
				NextBlock: nextBlockHash,
			}
			return c.JSONPretty(200, blockExtended, "  ")
		}

		b, err := block.Bytes()
		if err != nil {
			return echo.NewHTTPError(http.StatusBadRequest, err.Error())
		}

		switch mode {
		case BINARY_STREAM:
			return c.Blob(200, echo.MIMEOctetStream, b)
		case HEX:
			return c.String(200, hex.EncodeToString(b))
		default:
			return echo.NewHTTPError(http.StatusInternalServerError, "Bad read mode")
		}
	}
}
