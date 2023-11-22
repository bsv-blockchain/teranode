package http_impl

import (
	"io"
	"net/http"
	"strings"
	"sync"

	"github.com/labstack/echo/v4"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/ordishs/gocore"
	"golang.org/x/sync/errgroup"
)

func (h *HTTP) GetTransactions() func(c echo.Context) error {
	return func(c echo.Context) error {
		start := gocore.CurrentTime()
		defer func() {
			AssetStat.NewStat("GetTransaction_http").AddTime(start)
		}()

		body := c.Request().Body
		defer body.Close()

		c.Response().Header().Set(echo.HeaderContentType, echo.MIMEOctetStream)

		// Read the body into a 32 byte hashes one by one and stream the tx data back to the client
		nrTxAdded := 0

		g, gCtx := errgroup.WithContext(c.Request().Context())
		g.SetLimit(128)

		var b []byte
		responseBytes := make([]byte, 0, 2*1024*1024) // 2MB initial capacity
		responseBytesMu := sync.Mutex{}
		for {
			var hash chainhash.Hash
			_, err := io.ReadFull(body, hash[:])
			if err != nil {
				if err.Error() == "EOF" {
					break
				} else {
					return echo.NewHTTPError(http.StatusInternalServerError, err.Error())
				}
			}

			g.Go(func() error {
				b, err = h.repository.GetTransaction(gCtx, &hash)
				if err != nil {
					if strings.HasSuffix(err.Error(), " not found") {
						return echo.NewHTTPError(http.StatusNotFound, err.Error())
					} else {
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
			return err
		}

		prometheusAssetHttpGetTransactions.WithLabelValues("OK", "200").Add(float64(nrTxAdded))

		return c.Blob(200, echo.MIMEOctetStream, responseBytes)
	}
}
