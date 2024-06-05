package http_impl

import (
	"encoding/hex"
	aero "github.com/aerospike/aerospike-client-go/v7"
	"github.com/bitcoin-sv/ubsv/util"
	jsoniter "github.com/json-iterator/go"
	"github.com/labstack/echo/v4"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/ordishs/gocore"
	"net/http"
)

type aerospikeRecord struct {
	Key        string                 `json:"key"`
	Digest     string                 `json:"digest"`
	Namespace  string                 `json:"namespace"`
	SetName    string                 `json:"set"`
	Node       string                 `json:"node"`
	Bins       map[string]interface{} `json:"bins"`
	Generation uint32                 `json:"generation"`
	Expiration uint32                 `json:"expiration"`
}

func (h *HTTP) GetTxMetaByTXID(mode ReadMode) func(c echo.Context) error {

	return func(c echo.Context) error {
		start := gocore.CurrentTime()
		defer func() {
			AssetStat.NewStat("GetUTXOsByTXID_http").AddTime(start)
		}()

		// get
		storeURL, err, found := gocore.Config().GetURL("utxostore")
		if err != nil {
			h.logger.Errorf("[Asset_http] GetUTXOsByTXID error: %s", err.Error())
			return echo.NewHTTPError(http.StatusInternalServerError, err.Error())
		}
		if !found {
			h.logger.Errorf("[Asset_http] GetUTXOsByTXID error: no utxostore setting found")
			return echo.NewHTTPError(http.StatusInternalServerError, "no utxostore setting found")
		}

		client, err := util.GetAerospikeClient(h.logger, storeURL)
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

		prometheusAssetHttpGetUTXO.WithLabelValues("OK", "200").Inc()

		switch mode {
		case JSON:
			response.Bins["tx"] = hex.EncodeToString(response.Bins["tx"].([]byte))
			response.Bins["parentTxHashes"] = hex.EncodeToString(response.Bins["parentTxHashes"].([]byte))
			record := aerospikeRecord{
				Key:        response.Key.String(),
				Digest:     hex.EncodeToString(response.Key.Digest()),
				Namespace:  response.Key.Namespace(),
				SetName:    response.Key.SetName(),
				Node:       response.Node.GetName(),
				Bins:       response.Bins,
				Generation: response.Generation,
				Expiration: response.Expiration,
			}

			// convert to json using jsoniter, since Bins cannot be marshalled by encoding/json
			var json = jsoniter.ConfigCompatibleWithStandardLibrary
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
			h.logger.Errorf("[Asset_http][%s] GetUTXOsByTXID error: Bad read mode", hash.String())
			return echo.NewHTTPError(http.StatusInternalServerError, "Bad read mode")
		}
	}
}
