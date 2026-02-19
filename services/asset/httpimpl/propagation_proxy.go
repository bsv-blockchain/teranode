package httpimpl

import (
	"net/http"
	"net/http/httputil"
	"net/url"

	"github.com/labstack/echo/v4"
)

// ProxyPropagationTx creates an Echo handler that reverse-proxies transaction
// submissions to the propagation service. The request path is rewritten from
// /api/v1/tx to /tx to match the propagation service's HTTP endpoint.
func (h *HTTP) ProxyPropagationTx() echo.HandlerFunc {
	target, err := url.Parse(h.settings.Asset.PropagationProxyAddress)
	if err != nil {
		h.logger.Errorf("[Asset] failed to parse propagation proxy address %q: %v", h.settings.Asset.PropagationProxyAddress, err)
		return func(c echo.Context) error {
			return echo.NewHTTPError(http.StatusBadGateway, "propagation proxy misconfigured")
		}
	}

	proxy := &httputil.ReverseProxy{
		Director: func(req *http.Request) {
			req.URL.Scheme = target.Scheme
			req.URL.Host = target.Host
			req.URL.Path = "/tx"
			req.URL.RawQuery = ""
			req.Host = target.Host
		},
	}

	return func(c echo.Context) error {
		prometheusAssetHTTPProxyPropagationTx.WithLabelValues("ProxyPropagationTx", "request").Inc()
		proxy.ServeHTTP(c.Response(), c.Request())
		return nil
	}
}
