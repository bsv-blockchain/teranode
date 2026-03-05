package httpimpl

import (
	"fmt"
	"net/http"
	"net/http/httputil"
	"net/url"

	"github.com/labstack/echo/v4"
)

// ProxyPropagationTx creates an Echo handler that reverse-proxies transaction
// submissions to the propagation service. The request path is rewritten from
// /api/v1/tx to /tx to match the propagation service's HTTP endpoint.
// The target URL must be pre-validated by the caller.
func (h *HTTP) ProxyPropagationTx(target *url.URL) echo.HandlerFunc {
	proxy := &httputil.ReverseProxy{
		Director: func(req *http.Request) {
			req.URL.Scheme = target.Scheme
			req.URL.Host = target.Host
			req.URL.Path = "/tx"
			req.URL.RawQuery = ""
			req.Host = target.Host
		},
		ErrorHandler: func(w http.ResponseWriter, r *http.Request, err error) {
			h.logger.Errorf("[Asset] propagation proxy error: %v", err)
			prometheusAssetHTTPProxyPropagationTx.WithLabelValues("ERROR", http.StatusText(http.StatusBadGateway)).Inc()
			w.WriteHeader(http.StatusBadGateway)
		},
	}

	return func(c echo.Context) error {
		proxy.ServeHTTP(c.Response(), c.Request())
		status := c.Response().Status
		statusStr := fmt.Sprintf("%d", status)
		if status >= 200 && status < 400 {
			prometheusAssetHTTPProxyPropagationTx.WithLabelValues("OK", statusStr).Inc()
		} else if status >= 400 {
			prometheusAssetHTTPProxyPropagationTx.WithLabelValues("ERROR", http.StatusText(status)).Inc()
		}
		return nil
	}
}
