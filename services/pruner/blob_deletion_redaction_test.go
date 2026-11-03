package pruner

import (
	"context"
	"crypto/rand"
	"fmt"
	"net/url"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/stores/blob"
	"github.com/bsv-blockchain/teranode/stores/blob/storetypes"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/stretchr/testify/require"
)

// blobStorePassword is the canary. A blob store URL of this shape is supported
// configuration, and Go's HTTP client turns the userinfo into a Basic
// Authorization header, so the password here is a working credential.
const blobStorePassword = "canary-blob-store-password"

// capturingLogger records every formatted log line so a test can assert on
// what the pruner would have written to a pod log.
type capturingLogger struct {
	mu    sync.Mutex
	lines *[]string
}

func newCapturingLogger() *capturingLogger {
	lines := make([]string, 0, 64)
	return &capturingLogger{lines: &lines}
}

func (l *capturingLogger) record(format string, args ...interface{}) {
	l.mu.Lock()
	defer l.mu.Unlock()

	*l.lines = append(*l.lines, fmt.Sprintf(format, args...))
}

func (l *capturingLogger) captured() []string {
	l.mu.Lock()
	defer l.mu.Unlock()

	out := make([]string, len(*l.lines))
	copy(out, *l.lines)

	return out
}

func (l *capturingLogger) LogLevel() int                                   { return 0 }
func (l *capturingLogger) SetLogLevel(string)                              {}
func (l *capturingLogger) Debugf(format string, args ...interface{})       { l.record(format, args...) }
func (l *capturingLogger) Infof(format string, args ...interface{})        { l.record(format, args...) }
func (l *capturingLogger) Warnf(format string, args ...interface{})        { l.record(format, args...) }
func (l *capturingLogger) Errorf(format string, args ...interface{})       { l.record(format, args...) }
func (l *capturingLogger) Fatalf(format string, args ...interface{})       { l.record(format, args...) }
func (l *capturingLogger) New(string, ...ulogger.Option) ulogger.Logger    { return l }
func (l *capturingLogger) Duplicate(...ulogger.Option) ulogger.Logger      { return l }
func (l *capturingLogger) WithTraceContext(context.Context) ulogger.Logger { return l }

// TestBlobDeletionNeverLogsStoreCredentials is the regression test for the
// audit finding that the pruner logged the full blob store URL, userinfo and
// all, handing working credentials to anyone who could read the pod logs.
//
// It drives the two paths that format a store URL: the volume-mount warning
// raised when every blob in a batch is already missing, and the lazy store
// initialisation line. Both must name the store without its password.
func TestBlobDeletionNeverLogsStoreCredentials(t *testing.T) {
	initPrometheusMetrics()

	credentialedStoreURL, err := url.Parse("http://teranode:" + blobStorePassword + "@blobserver:8080/")
	require.NoError(t, err)

	t.Run("volume mount warning", func(t *testing.T) {
		logger := newCapturingLogger()
		ctx := context.Background()

		// A real, empty file store: every scheduled deletion comes back "already
		// missing", which is what triggers the warning that formats the store URL.
		emptyStore, err := blob.NewStore(ulogger.TestLogger{}, &url.URL{
			Scheme: "file",
			Path:   filepath.Join(t.TempDir(), "blobs"),
		})
		require.NoError(t, err)

		mockBlockchain := newMockBlockchainClient()
		observer := &testBlobDeletionObserver{t: t, complete: make(chan blobDeletionEvent, 10)}

		server := &Server{
			ctx:                  ctx,
			logger:               logger,
			blobStores:           map[storetypes.BlobStoreType]blob.Store{storetypes.TXSTORE: emptyStore},
			blockchainClient:     mockBlockchain,
			blobDeletionObserver: observer,
			settings: &settings.Settings{
				Pruner: settings.PrunerSettings{
					BlobDeletionBatchSize:  100,
					BlobDeletionMaxRetries: 3,
				},
			},
		}

		// The settings lookup the warning uses to name the store.
		server.settings.Block.TxStore = credentialedStoreURL

		key := make([]byte, 32)
		_, err = rand.Read(key)
		require.NoError(t, err)

		_, _, err = mockBlockchain.ScheduleBlobDeletion(ctx, key, string(fileformat.FileTypeTesting), storetypes.TXSTORE, 10)
		require.NoError(t, err)

		server.processBlobDeletionsAtHeight(10, chainhash.Hash{})

		_, err = observer.waitFor(5 * time.Second)
		require.NoError(t, err)

		lines := logger.captured()
		requireWarningWasRaised(t, lines, "already missing from disk")
		requireNoCredential(t, lines)
	})

	t.Run("store initialisation", func(t *testing.T) {
		logger := newCapturingLogger()

		server := &Server{
			ctx:        context.Background(),
			logger:     logger,
			blobStores: map[storetypes.BlobStoreType]blob.Store{},
			settings: &settings.Settings{
				Pruner: settings.PrunerSettings{
					BlobDeletionBatchSize:  100,
					BlobDeletionMaxRetries: 3,
				},
			},
		}
		server.settings.Block.TxStore = credentialedStoreURL

		// Lazily builds the store from settings and logs the line under test.
		_, err := server.getBlobStore(storetypes.TXSTORE)
		require.NoError(t, err)

		lines := logger.captured()
		requireWarningWasRaised(t, lines, "initialized")
		requireNoCredential(t, lines)
	})
}

// requireWarningWasRaised proves the test actually drove the code path that
// formats the URL. Without it, a test that logs nothing at all would pass.
func requireWarningWasRaised(t *testing.T, lines []string, needle string) {
	t.Helper()

	for _, line := range lines {
		if strings.Contains(line, needle) {
			return
		}
	}

	t.Fatalf("expected a log line containing %q, got:\n%s", needle, strings.Join(lines, "\n"))
}

func requireNoCredential(t *testing.T, lines []string) {
	t.Helper()

	for _, line := range lines {
		require.NotContains(t, line, blobStorePassword,
			"pruner log line leaks the blob store password: %s", line)
	}

	// The host must survive redaction, or the log line is useless for the
	// misconfiguration it exists to diagnose.
	requireWarningWasRaised(t, lines, "blobserver:8080")
}
