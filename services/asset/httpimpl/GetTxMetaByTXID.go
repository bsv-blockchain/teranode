package httpimpl

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"time"

	aero "github.com/bsv-blockchain/aerospike-client-go/v8"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/util"
	"github.com/bsv-blockchain/teranode/util/tracing"
	"github.com/labstack/echo/v4"
)

// rawReadPolicyTimeout is the fallback TotalTimeout applied when the configured
// aerospike_readPolicy leaves TotalTimeout at zero and the request carries no context
// deadline. Asset HTTP requests don't get one by default: net/http does not derive a
// context deadline from Server.ReadTimeout/WriteTimeout, and there is no Echo timeout
// middleware on this chain. Without a bound, the shared Aerospike semaphore's
// acquirePermit falls onto its uncancelable blocking-send path and a public caller
// waits forever for a permit. 5s matches the other Asset-handler-level RPC timeouts
// (get_catchup_status.go, get_peers.go, get_service_heights.go).
const rawReadPolicyTimeout = 5 * time.Second

// applyReadPolicyTimeout bounds policy.TotalTimeout by the request's context deadline
// when it has one, or by rawReadPolicyTimeout as a floor when it doesn't and the
// configured policy would otherwise leave TotalTimeout at zero (or negative).
func applyReadPolicyTimeout(ctx context.Context, policy *aero.BasePolicy) {
	if deadline, ok := ctx.Deadline(); ok {
		if remaining := time.Until(deadline); remaining > 0 {
			policy.TotalTimeout = remaining
		}

		return
	}

	if policy.TotalTimeout <= 0 {
		policy.TotalTimeout = rawReadPolicyTimeout
	}
}

// swagger:model aerospikeRecord
type aerospikeRecord struct {
	Key        string                 `json:"key"`
	Digest     string                 `json:"digest"`
	Namespace  string                 `json:"namespace"`
	SetName    string                 `json:"set"`
	Node       string                 `json:"node"`
	Bins       map[string]interface{} `json:"bins"`
	Generation uint32                 `json:"generation"`
}

// newAerospikeRecord builds the JSON view of a store record.
//
// Node is nil whenever the client did not attribute the record to a cluster
// node (a cached or client-side-constructed record), and dereferencing it here
// panicked on an unauthenticated route.
func newAerospikeRecord(response *aero.Record) aerospikeRecord {
	record := aerospikeRecord{
		Bins:       response.Bins,
		Generation: response.Generation,
	}

	if response.Key != nil {
		record.Key = response.Key.String()
		record.Digest = hex.EncodeToString(response.Key.Digest())
		record.Namespace = response.Key.Namespace()
		record.SetName = response.Key.SetName()
	}

	if response.Node != nil {
		record.Node = response.Node.GetName()
	}

	return record
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
//	GET /txmeta_raw/<txid>/json
//
//	# Get metadata in hex format
//	GET /txmeta_raw/<txid>/hex
//
//	# Get metadata in binary format
//	GET /txmeta_raw/<txid>
//
// Notes:
//   - Requires Aerospike database connection
//   - Binary data in JSON response is hex-encoded
//   - Direct access to underlying storage system
func (h *HTTP) GetTxMetaByTxID(mode ReadMode) func(c echo.Context) error {
	return func(c echo.Context) error {
		ctx, _, deferFn := tracing.Tracer("asset").Start(c.Request().Context(), "GetTxMetaByTxID_http",
			tracing.WithParentStat(AssetStat),
		)

		defer deferFn()

		// This route serves the raw store record. An operator who does not need
		// it can take it off the public surface without a redeploy. Answer with
		// echo's own not-found error so a disabled route can't be told apart
		// from an unregistered one.
		if !h.settings.Asset.TxMetaRawEnabled {
			return echo.ErrNotFound
		}

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

		h.logger.Debugf("[Asset_http] GetTxMetaByTxID in %s for %s: %s", mode, c.RealIP(), c.Param("hash"))

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

		// A typed-nil policy leaves TotalTimeout at zero, which sends the shared
		// Aerospike semaphore acquire down its uncancelable blocking-send path:
		// a public caller then waits forever for a permit while internal
		// callers, which do pass a timeout, fail. Use the configured
		// aerospike_readPolicy so this route follows the same policy as every
		// other read, then bound it by the request deadline when the caller
		// supplied one, or by a fallback when it doesn't and the configured
		// policy would otherwise leave TotalTimeout at zero.
		policy := util.GetAerospikeReadPolicy(h.settings)
		applyReadPolicyTimeout(ctx, policy)

		response, err := client.Get(policy, key)
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
			record := newAerospikeRecord(response)

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
