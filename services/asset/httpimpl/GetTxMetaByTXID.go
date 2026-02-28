package httpimpl

import (
	"encoding/hex"
	"encoding/json"
	"net/http"

	aero "github.com/aerospike/aerospike-client-go/v8"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/util"
	"github.com/bsv-blockchain/teranode/util/tracing"
	"github.com/labstack/echo/v4"
)

type aerospikeRecord struct {
	Key        string                 `json:"key"`
	Digest     string                 `json:"digest"`
	Namespace  string                 `json:"namespace"`
	SetName    string                 `json:"set"`
	Node       string                 `json:"node"`
	Bins       map[string]interface{} `json:"bins"`
	Generation uint32                 `json:"generation"`
}

// GetTxMetaByTxID creates an HTTP handler for retrieving transaction metadata directly
// from the Aerospike store. Supports multiple response formats.
//
// Parameters:
//   - mode: ReadMode specifying the response format (JSON, BINARY_STREAM, or HEX)
//
// Returns:
//   - func(c echo.Context) error: Echo handler function
//
// URL Parameters:
//   - hash: Transaction hash (hex string)
//
// Configuration Required:
//   - utxostore: URL configuration for Aerospike connection
//   - Default set name: "txmeta" if not specified in URL query
//
// HTTP Response Formats:
//
//  1. JSON (mode = JSON):
//     Status: 200 OK
//     Content-Type: application/json
//     Body: Aerospike record with metadata:
//     {
//     "key": "<string>",                // Aerospike key string
//     "digest": "<string>",             // Key digest (hex)
//     "namespace": "<string>",          // Aerospike namespace
//     "set": "<string>",                // Set name
//     "node": "<string>",               // Aerospike node name
//     "bins": {                         // Record data
//     "tx": "<hex string>",             // Transaction data
//     "parentTxHashes": "<hex string>",
//     // ... other bins
//     },
//     "generation": <uint32>,           // Record generation
//     "deleteAtHeight": <uint32>        // Record deletion height
//     }
//
//  2. HEX (mode = HEX):
//     Status: 200 OK
//     Content-Type: text/plain
//     Body: Hexadecimal string of the Aerospike record string representation
//
//  3. Binary (mode = BINARY_STREAM):
//     Status: 200 OK
//     Content-Type: application/octet-stream
//     Body: Raw bytes of the Aerospike record string representation
//
// Error Responses:
//   - 500 Internal Server Error:
//   - Missing or invalid utxostore configuration
//   - Aerospike connection errors
//   - Invalid transaction hash
//   - Record retrieval errors
//   - JSON marshalling errors
//   - Invalid read mode
//
// Monitoring:
//   - Execution time recorded in "GetUTXOsByTXID_http" statistic
//   - Prometheus metric "asset_http_get_utxo" tracks successful responses
//   - Debug logging of request handling
//
// Example Usage:
//
//	# Get metadata in JSON format
//	GET /tx/meta/<txid>
//
//	# Get metadata in hex format
//	GET /tx/meta/<txid>/hex
//
//	# Get metadata in binary format
//	GET /tx/meta/<txid>/raw
//
// Notes:
//   - Requires Aerospike database connection
//   - Binary data in JSON response is hex-encoded
//   - Direct access to underlying storage system
func (h *HTTP) GetTxMetaByTxID(mode ReadMode) func(c echo.Context) error {
	return func(c echo.Context) error {
		_, _, deferFn := tracing.Tracer("asset").Start(c.Request().Context(), "GetTxMetaByTxID_http",
			tracing.WithParentStat(AssetStat),
		)

		defer deferFn()

		// get
		storeURL := h.settings.UtxoStore.UtxoStore
		if storeURL == nil {
			h.logger.Errorf("[Asset_http] GetUTXOsByTXID error: no utxostore setting found")

			return echo.NewHTTPError(http.StatusInternalServerError, "no utxostore setting found")
		}

		client, err := util.GetAerospikeClient(h.logger, storeURL, h.settings)
		if err != nil {
			h.logger.Errorf("[Asset_http] GetUTXOsByTXID error: %s", err.Error())

			return echo.NewHTTPError(http.StatusInternalServerError, err.Error())
		}

		namespace := storeURL.Path[1:]

		setName := storeURL.Query().Get("set")
		if setName == "" {
			setName = "txmeta"
		}

		h.logger.Debugf("[Asset_http] GetUTXOsByTXID in %s for %s: %s", mode, c.Request().RemoteAddr, c.Param("hash"))

		hash, err := chainhash.NewHashFromStr(c.Param("hash"))
		if err != nil {
			h.logger.Errorf("[Asset_http] GetUTXOsByTXID error creating hash: %s", err.Error())

			return echo.NewHTTPError(http.StatusInternalServerError, err.Error())
		}

		// get transaction meta data
		key, err := aero.NewKey(namespace, setName, hash[:])
		if err != nil {
			h.logger.Errorf("[Asset_http] GetUTXOsByTXID error creating key: %s", err.Error())

			return echo.NewHTTPError(http.StatusInternalServerError, err.Error())
		}

		response, err := client.Get(nil, key)
		if err != nil {
			h.logger.Errorf("[Asset_http] GetUTXOsByTXID error getting transaction meta data: %s", err.Error())

			return echo.NewHTTPError(http.StatusInternalServerError, err.Error())
		}

		prometheusAssetHTTPGetUTXO.WithLabelValues("OK", "200").Inc()

		switch mode {
		case JSON:
			if tx, ok := response.Bins["tx"].([]byte); ok {
				response.Bins["tx"] = hex.EncodeToString(tx)
			}
			record := aerospikeRecord{
				Key:        response.Key.String(),
				Digest:     hex.EncodeToString(response.Key.Digest()),
				Namespace:  response.Key.Namespace(),
				SetName:    response.Key.SetName(),
				Node:       response.Node.GetName(),
				Bins:       response.Bins,
				Generation: response.Generation,
			}

			b, err := json.MarshalIndent(record, "", "  ")
			if err != nil {
				h.logger.Errorf("[Asset_http][%s] GetUTXOsByTXID error: %s", hash.String(), err.Error())
				return echo.NewHTTPError(http.StatusInternalServerError, err.Error())
			}

			return c.String(200, string(b))
		case HEX:
			return c.String(200, hex.EncodeToString([]byte(response.String())))
		case BINARY_STREAM:
			return c.Blob(200, echo.MIMEOctetStream, []byte(response.String()))
		default:
			h.logger.Errorf("[Asset_http][%s] GetUTXOsByTXID error: bad read mode", hash.String())
			return echo.NewHTTPError(http.StatusInternalServerError, "bad read mode")
		}
	}
}
