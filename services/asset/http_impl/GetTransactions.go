package http_impl

import (
	"errors"
	"github.com/bitcoin-sv/ubsv/ubsverrors"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/labstack/echo/v4"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/ordishs/gocore"
	"golang.org/x/sync/errgroup"
)

func (h *HTTP) GetTransactions() func(c echo.Context) error {
	return func(c echo.Context) error {
		nrTxAdded := 0

		start := gocore.CurrentTime()
		defer func() {
			AssetStat.NewStat("GetTransactions_http").AddTime(start)
			h.logger.Infof("[Asset_http] GetTransactions for %s: %d DONE in %s", c.Request().RemoteAddr, nrTxAdded, time.Since(start))
		}()

		h.logger.Infof("[Asset_http] GetTransactions for %s: %d", c.Request().RemoteAddr, c.Request().ContentLength)

		body := c.Request().Body
		defer body.Close()

		c.Response().Header().Set(echo.HeaderContentType, echo.MIMEOctetStream)

		// Read the body into a 32 byte hashes one by one and stream the tx data back to the client
		g, gCtx := errgroup.WithContext(c.Request().Context())
		g.SetLimit(1024)

		responseBytes := make([]byte, 0, 32*1024*1024) // 32MB initial capacity
		responseBytesMu := sync.Mutex{}
		for {
			var hash chainhash.Hash
			_, err := io.ReadFull(body, hash[:])
			if err != nil {
				if errors.Is(err, io.EOF) {
					break
				} else {
					h.logger.Errorf("[GetTransactions][%s] failed to read tx hash from body: %s", hash.String(), err.Error())
					return echo.NewHTTPError(http.StatusInternalServerError, err.Error())
				}
			}

			g.Go(func() error {
				b, err := h.repository.GetTransaction(gCtx, &hash)
				if err != nil {
					if errors.Is(err, ubsverrors.ErrNotFound) || strings.Contains(err.Error(), "not found") {
						h.logger.Errorf("[GetTransactions][%s] tx not found in repository: %s", hash.String(), err.Error())
						return echo.NewHTTPError(http.StatusNotFound, err.Error())
					} else {
						h.logger.Errorf("[GetTransactions][%s] failed to get tx from repository: %s", hash.String(), err.Error())
						return echo.NewHTTPError(http.StatusInternalServerError, err.Error())
					}
				}

				responseBytesMu.Lock()
				responseBytes = append(responseBytes, b...)
				responseBytesMu.Unlock()

				return nil
			})

			nrTxAdded++
		}

		if err := g.Wait(); err != nil {
			h.logger.Errorf("[GetTransactions] failed to get txs from repository: %s", err.Error())
			return echo.NewHTTPError(http.StatusInternalServerError, err.Error())
		}

		prometheusAssetHttpGetTransactions.WithLabelValues("OK", "200").Add(float64(nrTxAdded))

		h.logger.Infof("[GetTransactions] sending %d txs to client (%d bytes)", nrTxAdded, len(responseBytes))
		return c.Blob(200, echo.MIMEOctetStream, responseBytes)
	}
}
