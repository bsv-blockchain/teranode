package propagation

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"

	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/stretchr/testify/require"
)

// propagationHTTPPassword is the canary. Userinfo in propagation_httpAddresses
// is a working credential: http.Client.Do turns it into a Basic Authorization
// header on every /tx and /txs post.
const propagationHTTPPassword = "canary-propagation-http-password"

// lineLogger records every formatted log line.
type lineLogger struct {
	ulogger.TestLogger

	mu    sync.Mutex
	lines []string
}

func (l *lineLogger) record(format string, args ...interface{}) {
	l.mu.Lock()
	defer l.mu.Unlock()

	l.lines = append(l.lines, fmt.Sprintf(format, args...))
}

func (l *lineLogger) captured() []string {
	l.mu.Lock()
	defer l.mu.Unlock()

	return append([]string(nil), l.lines...)
}

func (l *lineLogger) Debugf(format string, args ...interface{}) { l.record(format, args...) }
func (l *lineLogger) Infof(format string, args ...interface{})  { l.record(format, args...) }
func (l *lineLogger) Warnf(format string, args ...interface{})  { l.record(format, args...) }
func (l *lineLogger) Errorf(format string, args ...interface{}) { l.record(format, args...) }

func (l *lineLogger) New(string, ...ulogger.Option) ulogger.Logger { return l }

func (l *lineLogger) Duplicate(...ulogger.Option) ulogger.Logger { return l }

func (l *lineLogger) WithTraceContext(context.Context) ulogger.Logger { return l }

// TestNewClientNeverLogsPropagationHTTPPassword pins the Info line NewClient
// writes on every construction. It used to print the configured address with
// its userinfo intact.
func TestNewClientNeverLogsPropagationHTTPPassword(t *testing.T) {
	logger := &lineLogger{}

	s := &settings.Settings{
		Propagation: settings.PropagationSettings{
			GRPCAddresses:    []string{"localhost:9090"},
			HTTPAddresses:    []string{"http://teranode:" + propagationHTTPPassword + "@propagation:8833"},
			SendBatchSize:    10,
			SendBatchTimeout: 10,
		},
	}

	client, err := NewClient(context.Background(), logger, s)
	require.NoError(t, err)

	t.Cleanup(func() { _ = client.Close() })

	// The client must still carry the credential, or the posts it makes would
	// stop authenticating.
	require.Equal(t, propagationHTTPPassword, func() string { p, _ := client.propagationHTTPAddr.User.Password(); return p }())

	var addressLine string

	for _, line := range logger.captured() {
		require.NotContains(t, line, propagationHTTPPassword, "a log line carries the propagation HTTP password")

		if strings.HasPrefix(line, "Using propagation HTTP address:") {
			addressLine = line
		}
	}

	require.NotEmpty(t, addressLine, "NewClient no longer logs the propagation HTTP address, so this test checks nothing")
	require.Contains(t, addressLine, "teranode:xxxxx@propagation:8833", "the address line should keep the user and host")
}
