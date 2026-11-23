package daemon

import (
	"context"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/pkg/urlutil"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/ulogger"
)

// alertSystemPollInterval is how often checkAlertSystem retries the alert system's liveness
// endpoint. It is a variable so tests can shorten it.
var alertSystemPollInterval = 2 * time.Second

// checkAlertSystem makes the alert system mandatory for the blockchain service: it returns an
// error, and the blockchain service must not start, unless the alert service runs in this
// process or answers on alert_healthAddress within alert_startupTimeout.
//
// It probes liveness, not readiness. The alert service's readiness depends on the blockchain
// service, so waiting for it before starting the blockchain service would never finish.
func checkAlertSystem(ctx context.Context, logger ulogger.Logger, tSettings *settings.Settings, alertInProcess bool) error {
	if !tSettings.BlockChain.RequireAlertSystem {
		logger.Warnf("[Alert] blockchain_requireAlertSystem is false, starting the blockchain service without checking the alert system")
		return nil
	}

	if alertInProcess {
		return nil
	}

	address := strings.TrimRight(tSettings.Alert.HealthAddress, "/")
	if address == "" {
		return errors.NewConfigurationError("the alert system is required but does not run in this process and alert_healthAddress is not set: start it here with -alert=1, set alert_healthAddress, or set blockchain_requireAlertSystem=false")
	}

	u, err := url.Parse(address)
	if err != nil {
		return errors.NewConfigurationError("alert_healthAddress %q is not a valid URL", urlutil.RedactString(address), urlutil.ParseErrorReason(err))
	}

	if (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		return errors.NewConfigurationError("alert_healthAddress %q must be an http:// or https:// URL with a host", urlutil.RedactString(address))
	}

	livenessURL := address + "/health/liveness"
	timeout := tSettings.Alert.StartupTimeout
	if timeout <= 0 {
		return errors.NewConfigurationError("alert_startupTimeout must be positive, got %s", timeout)
	}

	logger.Infof("[Alert] waiting up to %s for the alert system at %s", timeout, urlutil.RedactString(livenessURL))

	waitCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	client := &http.Client{Timeout: alertSystemPollInterval}
	ticker := time.NewTicker(alertSystemPollInterval)

	defer ticker.Stop()

	var lastErr error

	for {
		if lastErr = probeAlertLiveness(waitCtx, client, livenessURL); lastErr == nil {
			logger.Infof("[Alert] alert system is live at %s", urlutil.RedactString(livenessURL))
			return nil
		}

		select {
		case <-waitCtx.Done():
			if ctx.Err() != nil {
				return errors.NewContextCanceledError("stopped waiting for the alert system at %s", urlutil.RedactString(livenessURL), ctx.Err())
			}

			return errors.NewServiceUnavailableError("the alert system at %s did not answer within alert_startupTimeout (%s), not starting the blockchain service", urlutil.RedactString(livenessURL), timeout, lastErr)
		case <-ticker.C:
		}
	}
}

// probeAlertLiveness returns nil when livenessURL answers 200.
func probeAlertLiveness(ctx context.Context, client *http.Client, livenessURL string) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, livenessURL, nil)
	if err != nil {
		return errors.NewConfigurationError("invalid alert liveness URL", urlutil.ParseErrorReason(err))
	}

	resp, err := client.Do(req)
	if err != nil {
		// Transport and redirect errors can echo credentials outside URL tokens.
		return errors.NewServiceUnavailableError("alert system liveness request failed")
	}

	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return errors.NewServiceUnavailableError("alert system liveness returned %d", resp.StatusCode)
	}

	return nil
}
