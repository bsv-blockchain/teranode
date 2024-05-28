package http_impl

import (
	"encoding/binary"
	"github.com/bitcoin-sv/ubsv/errors"
	"github.com/labstack/echo/v4"
	"github.com/libsv/go-bt"
	"github.com/libsv/go-bt/v2/chainhash"
	"golang.org/x/sync/errgroup"
	"io"
	"net/http"
	"strings"
)

func (h *HTTP) GetLegacyBlock() func(c echo.Context) error {
	return func(c echo.Context) error {
		h.logger.Debugf("[Asset_http] GetBlockGetLegacyBlockByHash for %s: %s", c.Request().RemoteAddr, c.Param("hash"))
		hash, err := chainhash.NewHashFromStr(c.Param("hash"))
		if err != nil {
			return err
		}

		block, err := h.repository.GetBlockByHash(c.Request().Context(), hash)
		if err != nil {
			if errors.Is(err, errors.ErrNotFound) || strings.Contains(err.Error(), "not found") {
				prometheusAssetHttpGetBlockLegacy.WithLabelValues("ERROR", http.StatusText(http.StatusNotFound)).Inc()
				return echo.NewHTTPError(http.StatusNotFound, err.Error())
			} else {
				prometheusAssetHttpGetBlockLegacy.WithLabelValues("ERROR", http.StatusText(http.StatusInternalServerError)).Inc()
				return echo.NewHTTPError(http.StatusInternalServerError, err.Error())
			}
		}

		r, w := io.Pipe()

		g, gCtx := errgroup.WithContext(c.Request().Context())
		g.Go(func() error {
			// write bitcoin block magic number
			_, _ = w.Write([]byte{0xf9, 0xbe, 0xb4, 0xd9})

			// write the block size
			sizeInBytes := make([]byte, 4)
			binary.LittleEndian.PutUint32(sizeInBytes, uint32(block.SizeInBytes))
			if _, err = w.Write(sizeInBytes); err != nil {
				_ = w.Close()
				_ = r.CloseWithError(err)
				prometheusAssetHttpGetBlockLegacy.WithLabelValues("ERROR", http.StatusText(http.StatusInternalServerError)).Inc()
				return echo.NewHTTPError(http.StatusInternalServerError, err.Error())
			}

			// write the 80 byte block header
			if _, err = w.Write(block.Header.Bytes()); err != nil {
				_ = w.Close()
				_ = r.CloseWithError(err)
				prometheusAssetHttpGetBlockLegacy.WithLabelValues("ERROR", http.StatusText(http.StatusInternalServerError)).Inc()
				return echo.NewHTTPError(http.StatusInternalServerError, err.Error())
			}

			// write number of transactions
			if _, err = w.Write(bt.VarInt(block.TransactionCount)); err != nil {
				_ = w.Close()
				_ = r.CloseWithError(err)
				prometheusAssetHttpGetBlockLegacy.WithLabelValues("ERROR", http.StatusText(http.StatusInternalServerError)).Inc()
				return echo.NewHTTPError(http.StatusInternalServerError, err.Error())
			}

			for _, subtree := range block.Subtrees {
				// get a subtree reader and write it to the pipe
				subtreeReader, err := h.repository.GetSubtreeDataReader(gCtx, subtree)
				if err != nil {
					_ = w.Close()
					_ = r.CloseWithError(err)
					// stop echo stream and return error
					prometheusAssetHttpGetBlockLegacy.WithLabelValues("ERROR", http.StatusText(http.StatusInternalServerError)).Inc()
					return echo.NewHTTPError(http.StatusInternalServerError, err.Error())
				}

				// drop the subtree tx size
				_, _ = subtreeReader.Read(make([]byte, 4))

				// write the subtree to the pipe
				_, err = io.Copy(w, subtreeReader)
				if err != nil {
					_ = w.Close()
					_ = r.CloseWithError(err)
					// stop echo stream and return error
					prometheusAssetHttpGetBlockLegacy.WithLabelValues("ERROR", http.StatusText(http.StatusInternalServerError)).Inc()
					return echo.NewHTTPError(http.StatusInternalServerError, err.Error())
				}

				// close the subtree reader
				_ = subtreeReader.Close()
			}

			// close the writer after all subtrees have been streamed
			_ = w.Close()
			_ = r.Close()

			return nil
		})

		prometheusAssetHttpGetBlockLegacy.WithLabelValues("OK", "200").Inc()

		return c.Stream(http.StatusOK, echo.MIMEOctetStream, r)
	}
}
