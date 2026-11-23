// Package file — the start-up notice for a store with no default retention
// (notes section 43, open question 35).
package file

import (
	"context"
	"fmt"
	"net/url"
	"strings"
	"sync"
	"testing"

	"github.com/bsv-blockchain/teranode/stores/blob/options"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/stretchr/testify/require"
)

type infoCapture struct {
	mu   sync.Mutex
	info []string
}

func (c *infoCapture) LogLevel() int                                { return 0 }
func (c *infoCapture) SetLogLevel(string)                           {}
func (c *infoCapture) Debugf(string, ...interface{})                {}
func (c *infoCapture) Warnf(string, ...interface{})                 {}
func (c *infoCapture) Errorf(string, ...interface{})                {}
func (c *infoCapture) Fatalf(string, ...interface{})                {}
func (c *infoCapture) New(string, ...ulogger.Option) ulogger.Logger { return c }
func (c *infoCapture) Duplicate(...ulogger.Option) ulogger.Logger   { return c }
func (c *infoCapture) WithTraceContext(context.Context) ulogger.Logger {
	return c
}

func (c *infoCapture) Infof(format string, args ...interface{}) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.info = append(c.info, fmt.Sprintf(format, args...))
}

func (c *infoCapture) joined() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return strings.Join(c.info, "\n")
}

func newTestURL(t *testing.T) *url.URL {
	t.Helper()
	u, err := url.Parse("file://" + t.TempDir())
	require.NoError(t, err)
	return u
}

func TestNew_LogsWhenNoDefaultRetentionIsInForce(t *testing.T) {
	cap := &infoCapture{}
	_, err := New(cap, newTestURL(t))
	require.NoError(t, err)
	require.Contains(t, cap.joined(), "no default retention is in force")
}

func TestNew_SilentWhenRetentionOrExternalLifecycleIsDeclared(t *testing.T) {
	cap := &infoCapture{}
	_, err := New(cap, newTestURL(t), options.WithDefaultBlockHeightRetention(5))
	require.NoError(t, err)
	require.NotContains(t, cap.joined(), "no default retention is in force")

	cap2 := &infoCapture{}
	u, err := url.Parse("file://" + t.TempDir() + "?disableDAH=true")
	require.NoError(t, err)
	_, err = New(cap2, u)
	require.NoError(t, err)
	require.NotContains(t, cap2.joined(), "no default retention is in force")
}
