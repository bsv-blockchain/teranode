package settings

import (
	"fmt"
	"strings"
	"testing"

	"github.com/bsv-blockchain/go-chaincfg"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/ordishs/gocore"
	"github.com/stretchr/testify/require"
)

// capturingLogger records everything PrintSettings writes, so the assertion is
// about what an operator actually sees rather than about a helper called on the
// way there.
type capturingLogger struct {
	ulogger.TestLogger

	lines []string
}

func (l *capturingLogger) Infof(format string, args ...interface{}) {
	l.lines = append(l.lines, fmt.Sprintf(format, args...))
}

func (l *capturingLogger) Errorf(format string, args ...interface{}) {
	l.lines = append(l.lines, fmt.Sprintf(format, args...))
}

func (l *capturingLogger) output() string {
	return strings.Join(l.lines, "\n")
}

// TestPrintSettingsNeverLogsStoreCredentials covers the gocore configuration
// dump PrintSettings logs, which is the same dump RunDaemon logs at boot.
//
// It is the wiring test for that call site: it drives PrintSettings end to end
// rather than calling the redactor, so removing the redaction from the call
// site fails it.
//
// The dump is two lines above the settings.Redact call PrintSettings already
// makes, and it carries the same credential that call is careful about: gocore
// decrypts each value on the way out and masks only its own "*EHE*" prefix.
func TestPrintSettingsNeverLogsStoreCredentials(t *testing.T) {
	const password = "canary-print-settings-password"

	const storeKey = "blockchain_store_print_settings_canary"

	cfg := gocore.Config()
	cfg.Set(storeKey, "postgres://teranode:"+password+"@db.example:5432/blockchain")

	t.Cleanup(func() { cfg.Unset(storeKey) })

	require.Contains(t, cfg.Stats(), password,
		"gocore's own dump no longer carries the credential, so this test is not exercising a leak")

	logger := &capturingLogger{}

	s := &settings.Settings{Version: "1.0.0", ChainCfgParams: &chaincfg.MainNetParams}
	PrintSettings(logger, s, "1.0.0", "deadbeef")

	out := logger.output()
	require.NotEmpty(t, out, "PrintSettings logged nothing, so this test proves nothing")

	require.NotContains(t, out, password, "the settings dump still carries a store password")
	require.Contains(t, out, "db.example:5432", "the settings row lost its host")
	require.Contains(t, out, "teranode:xxxxx@", "the username should survive redaction")
}
