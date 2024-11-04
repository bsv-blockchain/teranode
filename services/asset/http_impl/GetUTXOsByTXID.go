// Package http_impl provides HTTP handlers for blockchain data retrieval and analysis.
package http_impl

import (
	"net/http"
	"strings"

	"github.com/bitcoin-sv/ubsv/errors"
	"github.com/bitcoin-sv/ubsv/stores/utxo"
	"github.com/bitcoin-sv/ubsv/util"
	"github.com/labstack/echo/v4"
	"github.com/libsv/go-bt/v2"
	"github.com/libsv/go-bt/v2/bscript"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/ordishs/gocore"
	"golang.org/x/sync/errgroup"
)

// UTXOItem represents detailed information about an Unspent Transaction Output (UTXO).
// It contains both the output details and its current status in the UTXO set.
type UTXOItem struct {
	// Txid is the hash of the transaction that created this UTXO
	Txid *chainhash.Hash `json:"txid"`

	// Vout is the index of this output in the creating transaction
	Vout uint32 `json:"vout"`

	// LockingScript is the output's script that specifies spending conditions
	LockingScript *bscript.Script `json:"lockingScript"`

	// Satoshis represents the amount of this output in satoshis
	Satoshis uint64 `json:"satoshis"`

	// UtxoHash is a unique identifier for this UTXO, derived from txid, vout,
	// locking script, and amount
	UtxoHash *chainhash.Hash `json:"utxoHash"`

	// Status indicates the current state of the UTXO (e.g., "SPENT", "NOT_FOUND")
	Status string `json:"status"`

	// SpendingTxID is the hash of the transaction that spent this UTXO.
	// Only present if the UTXO has been spent.
	SpendingTxID *chainhash.Hash `json:"spendingTxId,omitempty"`

	// LockTime specifies when this UTXO becomes spendable.
	// Only present if the UTXO has a time lock.
	LockTime uint32 `json:"lockTime,omitempty"`
}

// GetUTXOsByTXID creates an HTTP handler for retrieving all UTXOs associated with a transaction.
// It processes each output concurrently for improved performance.
//
// Parameters:
//   - mode: ReadMode (only JSON mode is supported)
//
// Returns:
//   - func(c echo.Context) error: Echo handler function
//
// URL Parameters:
//   - hash: Transaction ID (hex string)
//
// HTTP Response:
//
//	Status: 200 OK
//	Content-Type: application/json
//	Body: Array of UTXO information:
//	  [
//	    {
//	      "txid": "<string>",              // Transaction ID
//	      "vout": <uint32>,                // Output index
//	      "lockingScript": "<string>",      // Output locking script
//	      "satoshis": <uint64>,            // Output value in satoshis
//	      "utxoHash": "<string>",          // UTXO identifier hash
//	      "status": "<string>",            // UTXO status (e.g., "NOT_FOUND")
//	      "spendingTxId": "<string>",      // Hash of spending transaction (if spent)
//	      "lockTime": <uint32>             // Optional lock time
//	    },
//	    // ... additional UTXOs
//	  ]
//
// Error Responses:
//
//   - 404 Not Found:
//
//   - Transaction not found
//     Example: {"message": "not found"}
//
//   - 500 Internal Server Error:
//
//   - Invalid transaction hash format
//
//   - Transaction retrieval errors
//
//   - Transaction parsing errors
//
//   - UTXO hash calculation errors
//
//   - Invalid read mode (non-JSON)
//
// Monitoring:
//   - Execution time recorded in "GetUTXOsByTXID_http" statistic
//   - Prometheus metric "asset_http_get_utxo" tracks successful responses
//   - Debug logging of request handling
//
// Performance:
//   - Processes all transaction outputs concurrently
//   - Uses errgroup for parallel UTXO lookups
//   - Maintains output order in response
//
// Example Usage:
//
//	# Get all UTXOs for a transaction
//	GET /utxos/txid/<transaction_hash>
//
// Notes:
//   - Only JSON format is supported
//   - Each output's UTXO status is checked independently
//   - Response array matches transaction output order
//   - UTXOs can be in various states (spent, unspent, frozen, etc.)
func (h *HTTP) GetUTXOsByTXID(mode ReadMode) func(c echo.Context) error {

	return func(c echo.Context) error {
		start := gocore.CurrentTime()
		defer func() {
			AssetStat.NewStat("GetUTXOsByTXID_http").AddTime(start)
		}()

		h.logger.Debugf("[Asset_http] GetUTXOsByTXID in %s for %s: %s", mode, c.Request().RemoteAddr, c.Param("hash"))
		hash, err := chainhash.NewHashFromStr(c.Param("hash"))
		if err != nil {
			h.logger.Errorf("[Asset_http] GetUTXOsByTXID error creating hash: %s", err.Error())
			return echo.NewHTTPError(http.StatusInternalServerError, err.Error())
		}

		b, err := h.repository.GetTransaction(c.Request().Context(), hash)
		if err != nil {
			h.logger.Errorf("[Asset_http][%s] GetUTXOsByTXID error getting transaction: %s", hash.String(), err.Error())
			if errors.Is(err, errors.ErrNotFound) || strings.Contains(err.Error(), "not found") {
				return echo.NewHTTPError(http.StatusNotFound, err.Error())
			} else {
				return echo.NewHTTPError(http.StatusInternalServerError, err.Error())
			}
		}

		tx, err := bt.NewTxFromBytes(b)
		if err != nil {
			h.logger.Errorf("[Asset_http][%s] GetUTXOsByTXID error creating transaction: %s", hash.String(), err.Error())
			return echo.NewHTTPError(http.StatusInternalServerError, err.Error())
		}

		// Go through all the outputs and get the UTXOHash for each one
		// and then look up the UTXO by that hash.  This is done in parallel
		// to help speed things up.

		g, ctx := errgroup.WithContext(c.Request().Context())

		// Create a channel to receive the results from the goroutines
		// that will be created.
		utxos := make([]*UTXOItem, len(tx.Outputs))

		// Create a goroutine for each output in the transaction.
		for i, output := range tx.Outputs {
			safeI, safeOutput := i, output

			g.Go(func() error {
				// Get the UTXOHash for this output.
				utxoHash, err := util.UTXOHash(hash, uint32(safeI), safeOutput.LockingScript, safeOutput.Satoshis)
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

				// Get the UTXO for this output.
				utxoRes, _ := h.repository.GetUtxo(ctx, &utxo.Spend{
					UTXOHash: utxoHash,
					TxID:     tx.TxIDChainHash(),
					Vout:     uint32(safeI),
				})

				if utxoRes != nil && utxoRes.Status != int(utxo.Status_NOT_FOUND) {
					utxoItem.Status = utxo.Status(utxoRes.Status).String()
					utxoItem.SpendingTxID = utxoRes.SpendingTxID
					utxoItem.LockTime = utxoRes.LockTime
				} else {
					utxoItem.Status = utxo.Status_NOT_FOUND.String()
				}

				// this can be set here, but only directly by index
				utxos[safeI] = utxoItem

				return nil
			})
		}

		if err = g.Wait(); err != nil {
			h.logger.Errorf("[Asset_http][%s] GetUTXOsByTXID error: %s", hash.String(), err.Error())
			return echo.NewHTTPError(http.StatusInternalServerError, err.Error())
		}

		prometheusAssetHttpGetUTXO.WithLabelValues("OK", "200").Inc()

		switch mode {
		case JSON:
			return c.JSONPretty(200, utxos, "  ")
		default:
			h.logger.Errorf("[Asset_http][%s] GetUTXOsByTXID error: Bad read mode", hash.String())
			return echo.NewHTTPError(http.StatusInternalServerError, "Bad read mode")
		}
	}
}
