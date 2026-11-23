package daemon

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/stretchr/testify/require"
)

// alertCheckSettings returns settings that require the alert system, with the given health
// address and a short startup timeout, and shortens the poll interval for the test.
func alertCheckSettings(t *testing.T, healthAddress string) *settings.Settings {
	t.Helper()

	previous := alertSystemPollInterval
	alertSystemPollInterval = 10 * time.Millisecond

	t.Cleanup(func() { alertSystemPollInterval = previous })

	tSettings := settings.NewSettings()
	tSettings.BlockChain.RequireAlertSystem = true
	tSettings.Alert.HealthAddress = healthAddress
	tSettings.Alert.StartupTimeout = 2 * time.Second

	return tSettings
}

func TestCheckAlertSystemRedactsCredentials(t *testing.T) {
	t.Setenv("PRETTY_LOGS", "false")
	const password = "health-password-secret"
	for _, scenario := range []string{"live", "timeout", "cancelled", "unreachable", "malformed", "invalid scheme"} {
		t.Run(scenario, func(t *testing.T) {
			var authenticated atomic.Bool
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				user, pass, ok := r.BasicAuth()
				authenticated.Store(ok && user == "health-user" && pass == password)
				if scenario != "live" {
					w.WriteHeader(http.StatusServiceUnavailable)
				}
			}))
			t.Cleanup(srv.Close)
			address := strings.Replace(srv.URL, "http://", "http://health-user:"+password+"@", 1)
			ctx := t.Context()
			switch scenario {
			case "unreachable":
				srv.Close()
			case "malformed":
				address = "http://health-user:" + password + "%zz@localhost"
			case "invalid scheme":
				address = "ftp://health-user:" + password + "@localhost"
			case "cancelled":
				var cancel context.CancelFunc
				ctx, cancel = context.WithCancel(ctx)
				cancel()
			}

			tSettings := alertCheckSettings(t, address)
			tSettings.Alert.StartupTimeout = 30 * time.Millisecond
			var logs bytes.Buffer
			logger := ulogger.New("alert-check-test", ulogger.WithWriter(&logs), ulogger.WithLevel("INFO"))
			err := checkAlertSystem(ctx, logger, tSettings, false)
			if scenario == "live" {
				require.NoError(t, err)
				require.True(t, authenticated.Load(), "the request must retain its credentials")
				require.Contains(t, logs.String(), "alert system is live")
			} else {
				require.Error(t, err)
				require.NotContains(t, err.Error(), password)
			}
			require.NotContains(t, logs.String(), password)
		})
	}
}

func TestProbeAlertLivenessRedactsCredentials(t *testing.T) {
	const password = "redirect-password-secret"
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		// The unescaped question mark makes net/url echo the password as an
		// invalid port in the nested redirect error, outside the URL itself.
		w.Header().Set("Location", "http://health-user:"+password+"?@localhost")
		w.WriteHeader(http.StatusFound)
	}))
	t.Cleanup(srv.Close)

	for _, address := range []string{srv.URL, "http://health-user:" + password + "%zz@localhost"} {
		err := probeAlertLiveness(t.Context(), srv.Client(), address)
		require.Error(t, err)
		require.NotContains(t, err.Error(), password)
	}
}

// livenessServer answers /health/liveness with 503 for the first failures requests, then 200.
func livenessServer(t *testing.T, failures int32) (*httptest.Server, *atomic.Int32) {
	t.Helper()

	var calls atomic.Int32

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/health/liveness" {
			w.WriteHeader(http.StatusNotFound)
			return
		}

		if calls.Add(1) <= failures {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}

		w.WriteHeader(http.StatusOK)
	}))

	t.Cleanup(srv.Close)

	return srv, &calls
}

func TestCheckAlertSystem(t *testing.T) {
	logger := ulogger.TestLogger{}

	t.Run("not required", func(t *testing.T) {
		tSettings := alertCheckSettings(t, "")
		tSettings.BlockChain.RequireAlertSystem = false

		require.NoError(t, checkAlertSystem(t.Context(), logger, tSettings, false))
	})

	t.Run("alert service in this process", func(t *testing.T) {
		tSettings := alertCheckSettings(t, "")

		require.NoError(t, checkAlertSystem(t.Context(), logger, tSettings, true))
	})

	t.Run("no health address", func(t *testing.T) {
		tSettings := alertCheckSettings(t, "")

		err := checkAlertSystem(t.Context(), logger, tSettings, false)
		require.ErrorIs(t, err, errors.ErrConfiguration)
		require.ErrorContains(t, err, "alert_healthAddress is not set")
	})

	t.Run("health address without http scheme", func(t *testing.T) {
		tSettings := alertCheckSettings(t, "alert-system:8000")

		err := checkAlertSystem(t.Context(), logger, tSettings, false)
		require.ErrorIs(t, err, errors.ErrConfiguration)
	})

	t.Run("non-positive startup timeout", func(t *testing.T) {
		srv, calls := livenessServer(t, 0)
		tSettings := alertCheckSettings(t, srv.URL)
		tSettings.Alert.StartupTimeout = 0

		err := checkAlertSystem(t.Context(), logger, tSettings, false)
		require.ErrorIs(t, err, errors.ErrConfiguration)
		require.Zero(t, calls.Load())
	})

	t.Run("alert system live", func(t *testing.T) {
		srv, calls := livenessServer(t, 0)
		tSettings := alertCheckSettings(t, srv.URL+"/")

		require.NoError(t, checkAlertSystem(t.Context(), logger, tSettings, false))
		require.Equal(t, int32(1), calls.Load())
	})

	t.Run("alert system becomes live", func(t *testing.T) {
		srv, calls := livenessServer(t, 3)
		tSettings := alertCheckSettings(t, srv.URL)

		require.NoError(t, checkAlertSystem(t.Context(), logger, tSettings, false))
		require.Equal(t, int32(4), calls.Load())
	})

	t.Run("alert system never live", func(t *testing.T) {
		srv, calls := livenessServer(t, 1<<30)
		tSettings := alertCheckSettings(t, srv.URL)
		tSettings.Alert.StartupTimeout = 100 * time.Millisecond

		err := checkAlertSystem(t.Context(), logger, tSettings, false)
		require.ErrorIs(t, err, errors.ErrServiceUnavailable)
		require.ErrorContains(t, err, "alert_startupTimeout")
		require.Greater(t, calls.Load(), int32(1))
	})

	t.Run("alert system unreachable", func(t *testing.T) {
		srv, _ := livenessServer(t, 0)
		address := srv.URL

		srv.Close()

		tSettings := alertCheckSettings(t, address)
		tSettings.Alert.StartupTimeout = 100 * time.Millisecond

		err := checkAlertSystem(t.Context(), logger, tSettings, false)
		require.ErrorIs(t, err, errors.ErrServiceUnavailable)
	})

	t.Run("parent context cancelled", func(t *testing.T) {
		srv, _ := livenessServer(t, 1<<30)
		tSettings := alertCheckSettings(t, srv.URL)

		ctx, cancel := context.WithCancel(t.Context())
		time.AfterFunc(50*time.Millisecond, cancel)

		err := checkAlertSystem(ctx, logger, tSettings, false)
		require.ErrorIs(t, err, errors.ErrContextCanceled)
	})
}
