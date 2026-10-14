// Package alert — tests that a freeze reports what it actually matched.
package alert

import (
	"context"
	"fmt"
	"net/url"
	"strings"
	"sync"
	"testing"

	"github.com/bsv-blockchain/go-bn/models"
	"github.com/bsv-blockchain/teranode/stores/utxo/sql"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/require"
)

// captureLogger records Infof output so a test can assert what the operator
// would have seen; Errorf is forwarded to an inner logger that fails the test.
type captureLogger struct {
	mu    sync.Mutex
	info  []string
	inner ulogger.Logger
}

func (c *captureLogger) LogLevel() int                 { return c.inner.LogLevel() }
func (c *captureLogger) SetLogLevel(level string)      { c.inner.SetLogLevel(level) }
func (c *captureLogger) Debugf(string, ...interface{}) {}
func (c *captureLogger) Warnf(string, ...interface{})  {}
func (c *captureLogger) Fatalf(f string, a ...interface{}) {
	c.inner.Fatalf(f, a...)
}
func (c *captureLogger) New(string, ...ulogger.Option) ulogger.Logger    { return c }
func (c *captureLogger) Duplicate(...ulogger.Option) ulogger.Logger      { return c }
func (c *captureLogger) WithTraceContext(context.Context) ulogger.Logger { return c }

func (c *captureLogger) Errorf(format string, args ...interface{}) {
	c.inner.Errorf(format, args...)
}

func (c *captureLogger) Infof(format string, args ...interface{}) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.info = append(c.info, fmt.Sprintf(format, args...))
}

func (c *captureLogger) joined() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return strings.Join(c.info, "\n")
}

// TestAddToConsensusBlacklist_ReportsSuccesses pins the fix for "froze five coins"
// being indistinguishable from "matched nothing": the response body only lists
// failures, so the per-fund success line and the processed/applied summary must
// name the split in the logs an operator reads.
func TestAddToConsensusBlacklist_ReportsSuccesses(t *testing.T) {
	ctx := context.Background()
	tSettings := test.CreateBaseTestSettings(t)

	capture := &captureLogger{inner: ulogger.NewErrorTestLogger(t)}

	utxoStoreURL, err := url.Parse("sqlitememory:///report")
	require.NoError(t, err)
	utxoStore, err := sql.New(ctx, capture, tSettings, utxoStoreURL)
	require.NoError(t, err)

	_ = utxoStore.SetBlockHeight(101)
	_, err = utxoStore.Create(ctx, tx, 101)
	require.NoError(t, err)

	node := NewNodeConfig(capture, nil, utxoStore, nil, nil, nil, tSettings)

	// One real output and one unknown txid: a partial success must be
	// distinguishable from a total miss in what the service reports.
	funds := []models.Fund{
		{TxOut: models.TxOut{TxId: tx.TxIDChainHash().String(), Vout: 1}},
		{TxOut: models.TxOut{TxId: strings.Repeat("0", 64), Vout: 0}},
	}

	response, err := node.AddToConsensusBlacklist(ctx, funds)
	require.NoError(t, err)
	require.Equal(t, 1, len(response.NotProcessed), "the unknown txid must still be the only listed failure")

	seen := capture.joined()
	require.Contains(t, seen, "froze "+tx.TxIDChainHash().String()+":1",
		"successes must name the fund they applied, operator log was:\n%s", seen)
	require.Contains(t, seen, "processed 2 funds: 1 applied, 1 not processed",
		"the summary must state the applied/failed split, operator log was:\n%s", seen)
}
