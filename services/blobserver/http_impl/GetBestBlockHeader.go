package http_impl

import (
	"fmt"
	"net/http"

	"github.com/bitcoin-sv/ubsv/model"
	"github.com/labstack/echo/v4"
)

type resp struct {
	*model.BlockHeader
	Hash   string `json:"hash"`
	Height uint32 `json:"height"`
}

func (r *resp) MarshalJSON() ([]byte, error) {
	return []byte(fmt.Sprintf(`{"hash":"%s","version":%d,"previousblockhash":"%s","merkleroot":"%s","time":%d,"bits":"%s","nonce":%d,"height":%d}`,
		r.Hash,
		r.Version,
		r.HashPrevBlock.String(),
		r.HashMerkleRoot.String(),
		r.Timestamp,
		r.Bits.String(),
		r.Nonce,
		r.Height,
	)), nil
}

func (r *resp) UnmarshalJSON([]byte) error {
	return nil
}

func (h *HTTP) GetBestBlockHeader(mode ReadMode) func(c echo.Context) error {
	return func(c echo.Context) error {
		h.logger.Debugf("[BlobServer_http] GetBestBlockHeader")

		block, height, err := h.repository.GetBestBlockHeader(c.Request().Context())
		if err != nil {
			return echo.NewHTTPError(http.StatusInternalServerError, err.Error())
		}
		prometheusBlobServerHttpGetBestBlockHeader.WithLabelValues("OK", "200").Inc()

		r := &resp{
			BlockHeader: block,
			Height:      height,
			Hash:        block.String(),
		}

		switch mode {
		case JSON:
			return c.JSONPretty(200, r, "  ")
		case BINARY_STREAM:
			return echo.NewHTTPError(http.StatusInternalServerError, "BINARY is not supported for BestBlockHeader")
		case HEX:
			return echo.NewHTTPError(http.StatusInternalServerError, "HEX is not supported for BestBlockHeader")
		default:
			return echo.NewHTTPError(http.StatusInternalServerError, "Bad read mode")
		}
	}
}
