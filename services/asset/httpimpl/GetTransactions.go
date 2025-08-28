package httpimpl

import (
	"io"
	"net/http"
	"strings"
	"sync"

	"github.com/bitcoin-sv/teranode/errors"
	"github.com/bitcoin-sv/teranode/util"
	"github.com/bitcoin-sv/teranode/util/tracing"
	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/labstack/echo/v4"
	"golang.org/x/sync/errgroup"
)

// GetTransactions creates an HTTP handler for retrieving multiple transactions in a single request.
// It accepts a stream of transaction hashes and returns concatenated transaction data.
//
// HTTP Method:
//   - POST
//
// Request:
//
//	Content-Type: application/octet-stream
//	Body: Concatenated 32-byte transaction hashes
//	Note: Each hash must be exactly 32 bytes
//
// Returns:
//   - func(c echo.Context) error: Echo handler function
//
// HTTP Response:
//
//	Status: 200 OK
//	Content-Type: application/octet-stream
//	Body: Concatenated transaction data for all found transactions
//
// Error Responses:
//
//   - 404 Not Found:
//
//   - One or more transactions not found
//     Example: {"message": "not found"}
//
//   - 500 Internal Server Error:
//
//   - Error reading request body
//
//   - Invalid hash format
//
//   - Repository errors
//
// Performance:
//   - Concurrent transaction retrieval (up to 1024 goroutines)
//   - Initial response buffer capacity: 32MB
//   - Thread-safe response construction
//
// Monitoring:
//   - Execution time recorded in "GetTransactions_http" statistic
//   - Prometheus metric "asset_http_get_transactions" tracks number of transactions
//   - Debug logging includes:
//   - Number of transactions processed
//   - Total response size
//   - Processing duration
//
// Example Usage:
//
//	# Request multiple transactions
//	POST /transactions
//	Body: <32-byte-hash1><32-byte-hash2>...
//
// Notes:
//   - Each transaction hash in the request must be exactly 32 bytes
//   - Response contains only found transactions
//   - Transactions are retrieved concurrently for better performance
//   - Response order may not match request order due to concurrent processing
func (h *HTTP) GetTransactions() func(c echo.Context) error {
	return func(c echo.Context) error {
		ctx, _, deferFn := tracing.Tracer("asset").Start(c.Request().Context(), "GetTransactions_http",
			tracing.WithParentStat(AssetStat),
			tracing.WithLogMessage(h.logger, "[Asset_http:GetTransactions] for %s", c.Request().RemoteAddr),
		)

		defer deferFn()

		transactionFromSubtreeData := make(map[chainhash.Hash]*bt.Tx)

		// this if statement is temporary and will be removed when the subtree data is implemented
		if len(c.Param("hash")) > 0 {
			if len(c.Param("hash")) != 64 {
				return echo.NewHTTPError(http.StatusBadRequest, errors.NewInvalidArgumentError("invalid subtree hash length").Error())
			}

			subtreeHash, err := chainhash.NewHashFromStr(c.Param("hash"))
			if err != nil {
				return echo.NewHTTPError(http.StatusBadRequest, errors.NewInvalidArgumentError("invalid subtree hash string", err).Error())
			}

			// check whether the subtree exists
			subtreeExists, err := h.repository.GetSubtreeExists(ctx, subtreeHash)
			if err != nil {
				if errors.Is(err, errors.ErrNotFound) || strings.Contains(err.Error(), "not found") || strings.Contains(err.Error(), "no such file") {
					return echo.NewHTTPError(http.StatusNotFound, errors.NewNotFoundError("subtree %s not found", subtreeHash.String(), err).Error())
				} else {
					return echo.NewHTTPError(http.StatusInternalServerError, errors.NewProcessingError("error getting subtree", subtreeHash.String(), err).Error())
				}
			}

			if !subtreeExists {
				return echo.NewHTTPError(http.StatusNotFound, errors.NewNotFoundError("subtree %s not found", subtreeHash.String()).Error())
			}

			// read the data from the subtreeData file and create a map of transaction hashes to transactions
			transactionFromSubtreeData, err = h.repository.GetSubtreeTransactions(ctx, subtreeHash)
			if err != nil {
				// this should not be an ERROR, but a warning, because it is not critical if the subtree data is not available
				// it just means that the transactions will be fetched from the utxo store
				h.logger.Debugf("[Asset_http:GetTransactions][%s] error getting transactions from subtree data: %s", subtreeHash.String(), err.Error())
			}
		}

		nrTxAdded := 0

		body := c.Request().Body
		defer func() {
			_ = body.Close()
		}()

		c.Response().Header().Set(echo.HeaderContentType, echo.MIMEOctetStream)

		// Read the body into a 32 byte hashes one by one and stream the tx data back to the client
		g, gCtx := errgroup.WithContext(ctx)
		util.SafeSetLimit(g, 1024)

		responseBytes := make([]byte, 0, 32*1024*1024) // 32MB initial capacity
		responseBytesMu := sync.Mutex{}

		for {
			var hash chainhash.Hash

			_, err := io.ReadFull(body, hash[:])
			if err != nil {
				if errors.Is(err, io.EOF) {
					break
				} else {
					return echo.NewHTTPError(http.StatusInternalServerError, errors.NewProcessingError("error reading request body", err).Error())
				}
			}

			if tx, ok := transactionFromSubtreeData[hash]; ok {
				// If the transaction is found in the subtree data, serialize it and append to response
				responseBytesMu.Lock()
				// always write the non-extended normal bytes as a response !
				// our peer node should extend the transactions if needed
				responseBytes = append(responseBytes, tx.Bytes()...)
				responseBytesMu.Unlock()
			} else {
				g.Go(func() error {
					b, err := h.repository.GetTransaction(gCtx, &hash)
					if err != nil {
						if errors.Is(err, errors.ErrNotFound) || strings.Contains(err.Error(), "not found") || strings.Contains(err.Error(), "no such file") {
							return echo.NewHTTPError(http.StatusNotFound, errors.NewNotFoundError("transaction not found", err).Error())
						} else {
							return echo.NewHTTPError(http.StatusInternalServerError, errors.NewProcessingError("error getting transaction", err).Error())
						}
					}

					responseBytesMu.Lock()
					responseBytes = append(responseBytes, b...)
					responseBytesMu.Unlock()

					return nil
				})
			}

			nrTxAdded++
		}

		if err := g.Wait(); err != nil {
			h.logger.Errorf("failed to get txs from repository: %s", err.Error())
			return err
		}

		prometheusAssetHTTPGetTransactions.WithLabelValues("OK", "200").Add(float64(nrTxAdded))

		h.logger.Infof("[Asset_http:GetTransactions] sending %d txs to client (%d bytes)", nrTxAdded, len(responseBytes))

		return c.Blob(200, echo.MIMEOctetStream, responseBytes)
	}
}
