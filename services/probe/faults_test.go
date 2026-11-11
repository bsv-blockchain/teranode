package probe

import (
	"context"
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/require"
)

func countLines(lines []string, sub string) int {
	n := 0

	for _, l := range lines {
		if strings.Contains(l, sub) {
			n++
		}
	}

	return n
}

func awaitSelfStop(t *testing.T, srv *Server, errCh chan error) {
	t.Helper()

	select {
	case err := <-errCh:
		require.NoError(t, err)
	case <-time.After(15 * time.Second):
		require.FailNow(t, "probe did not stop itself")
	}

	for _, liveness := range []bool{true, false} {
		status, msg, err := srv.Health(context.Background(), liveness)
		require.NoError(t, err)
		require.Equal(t, http.StatusOK, status)
		require.Equal(t, probeStoppedHealth, msg)
	}

	require.NoError(t, srv.Stop(context.Background()))
	require.NoError(t, srv.Stop(context.Background()))
}

func TestStoppedHealthTellsOperatorToRestartNode(t *testing.T) {
	require.Contains(t, probeStoppedHealth, "repeated failures")
	require.Contains(t, probeStoppedHealth, "node must be restarted")
	require.NotContains(t, probeStoppedHealth, "restart the probe")
}

func TestCorruptStateFileIsMovedAsideAndProbeStarts(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "probe_state.json")
	garbage := []byte("{this is not json")
	require.NoError(t, os.WriteFile(path, garbage, 0o600))

	logger := &captureLogger{}
	srv := newProbe(t, probeOpts{statePath: path, logger: logger})
	errCh := runProbe(t, srv)

	snap := servedSnapshot(t, srv)
	require.NotNil(t, snap.Alarms)

	matches, err := filepath.Glob(path + ".corrupt-*")
	require.NoError(t, err)
	require.Len(t, matches, 1)
	require.Regexp(t, `\.corrupt-\d{8}T\d{6}Z$`, matches[0])

	moved, err := os.ReadFile(matches[0])
	require.NoError(t, err)
	require.Equal(t, garbage, moved)

	require.Equal(t, 1, countLines(logger.Lines(), filepath.Base(matches[0])))

	status, _ := doRequest(t, http.MethodPost, apiURL(srv, "/api/snooze"), testToken, map[string]any{"id": "cache-hit-rate", "minutes": 5})
	require.Equal(t, http.StatusNoContent, status)

	select {
	case err := <-errCh:
		require.FailNow(t, "probe stopped", "%v", err)
	default:
	}
}

func TestCorruptStateFileRenameFailureStopsProbeNotDaemon(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "probe_state.json")
	require.NoError(t, os.WriteFile(path, []byte("garbage"), 0o600))

	logger := &captureLogger{}
	srv := newProbe(t, probeOpts{statePath: path, logger: logger})
	srv.rename = func(string, string) error { return errors.NewProcessingError("rename refused") }
	require.NoError(t, srv.Init(context.Background()))

	cancel, errCh := startServer(t, srv)
	defer cancel()

	awaitSelfStop(t, srv, errCh)
	require.Empty(t, srv.Addr())

	raw, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, "garbage", string(raw))

	require.Equal(t, 1, countLines(logger.Lines(), "ERROR"))
}

func TestUnwritableStateDirIsBootError(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "ro")
	require.NoError(t, os.MkdirAll(dir, 0o700))
	require.NoError(t, os.Chmod(dir, 0o500))

	t.Cleanup(func() { _ = os.Chmod(dir, 0o700) })

	srv := newProbe(t, probeOpts{statePath: filepath.Join(dir, "probe_state.json")})
	require.NoError(t, srv.Init(context.Background()))
	require.Error(t, srv.Start(context.Background(), make(chan struct{})))
	require.Empty(t, srv.Addr())
}

func panicAt(point string, times int64) func(string) {
	var hits atomic.Int64

	return func(p string) {
		if p != point {
			return
		}

		if times < 0 || hits.Add(1) <= times {
			panic("injected " + point)
		}
	}
}

func TestPanicInFlushIsRecoveredAndCounted(t *testing.T) {
	logger := &captureLogger{}

	srv := newProbe(t, probeOpts{logger: logger})
	srv.flushEvery = time.Nanosecond
	srv.setFaultHook(panicAt(faultFlush, -1))
	require.NoError(t, srv.Init(context.Background()))

	cancel, errCh := startServer(t, srv)
	defer cancel()

	awaitSelfStop(t, srv, errCh)

	lines := logger.Lines()
	require.GreaterOrEqual(t, countLines(lines, "state flush panicked"), maxConsecutivePanics)
	require.Equal(t, 1, countLines(lines, probeStoppedMessage))
}

func TestPanicInFlushOnceDoesNotStopProbe(t *testing.T) {
	logger := &captureLogger{}

	srv := newProbe(t, probeOpts{logger: logger})
	srv.flushEvery = time.Nanosecond
	srv.setFaultHook(panicAt(faultFlush, 2))
	errCh := runProbe(t, srv)

	require.Eventually(t, func() bool { return countLines(logger.Lines(), "state flush panicked") == 2 }, 5*time.Second, 10*time.Millisecond)

	first := servedSnapshot(t, srv)

	require.Eventually(t, func() bool { return servedSnapshot(t, srv).Time.After(first.Time) }, 5*time.Second, 10*time.Millisecond)

	select {
	case err := <-errCh:
		require.FailNow(t, "probe stopped", "%v", err)
	default:
	}
}

func TestPanicInHubRunIsRecovered(t *testing.T) {
	logger := &captureLogger{}

	srv := newProbe(t, probeOpts{logger: logger})
	srv.logClock = stepClock()
	srv.setFaultHook(panicAt(faultHub, 3))
	errCh := runProbe(t, srv)

	conn := authedWS(t, srv)

	require.Eventually(t, func() bool { return countLines(logger.Lines(), "websocket hub panicked") == 3 }, 5*time.Second, 10*time.Millisecond)

	first := readSnapshot(t, conn, 2*time.Second)
	later := readSnapshot(t, conn, 2*time.Second)

	for !later.Time.After(first.Time) {
		later = readSnapshot(t, conn, 2*time.Second)
	}

	select {
	case err := <-errCh:
		require.FailNow(t, "probe stopped", "%v", err)
	default:
	}
}

func TestPanicInClientWriteLoopClosesOnlyThatClient(t *testing.T) {
	logger := &captureLogger{}

	srv := newProbe(t, probeOpts{logger: logger})
	runProbe(t, srv)

	a := authedWS(t, srv)
	b := authedWS(t, srv)

	readSnapshot(t, a, 2*time.Second)
	readSnapshot(t, b, 2*time.Second)
	require.Equal(t, 2, srv.hubClients())

	srv.setFaultHook(panicAt(faultClientWrite, 1))

	require.Eventually(t, func() bool { return srv.hubClients() == 1 }, 5*time.Second, 5*time.Millisecond)
	require.Equal(t, 1, countLines(logger.Lines(), "websocket client writer panicked"))

	alive := 0

	var wg sync.WaitGroup

	var mu sync.Mutex

	for _, conn := range []*websocket.Conn{a, b} {
		wg.Add(1)

		go func() {
			defer wg.Done()

			ok := true

			for range 5 {
				require.NoError(t, conn.SetReadDeadline(time.Now().Add(2*time.Second)))

				if _, _, err := conn.ReadMessage(); err != nil {
					ok = false

					break
				}
			}

			if ok {
				mu.Lock()
				alive++
				mu.Unlock()
			}
		}()
	}

	wg.Wait()
	require.Equal(t, 1, alive)
}

func TestPanicInCloseDuringStopIsRecovered(t *testing.T) {
	logger := &captureLogger{}

	srv := newProbe(t, probeOpts{logger: logger})
	srv.setFaultHook(panicAt(faultClose, -1))
	require.NoError(t, srv.Init(context.Background()))

	cancel, errCh := startServer(t, srv)
	addr := srv.Addr()

	servedSnapshot(t, srv)
	cancel()

	require.NotPanics(t, func() { _ = srv.Stop(context.Background()) })

	select {
	case <-errCh:
	case <-time.After(10 * time.Second):
		require.FailNow(t, "Start did not return")
	}

	require.Equal(t, 1, countLines(logger.Lines(), "state close panicked"))

	ln, err := net.Listen("tcp", addr)
	require.NoError(t, err)
	require.NoError(t, ln.Close())
}

type breakableListener struct {
	net.Listener

	broken atomic.Bool
}

func (l *breakableListener) Accept() (net.Conn, error) {
	conn, err := l.Listener.Accept()
	if l.broken.Load() {
		if conn != nil {
			_ = conn.Close()
		}

		return nil, errors.NewProcessingError("listener failed permanently")
	}

	return conn, err
}

func (l *breakableListener) breakNow() {
	l.broken.Store(true)
	_ = l.Listener.Close()
}

func TestServeFailureAfterStartStopsProbeNotDaemon(t *testing.T) {
	logger := &captureLogger{}

	var bl *breakableListener

	srv := newProbe(t, probeOpts{logger: logger})
	srv.listen = func(network, address string) (net.Listener, error) {
		ln, err := net.Listen(network, address)
		if err != nil {
			return nil, err
		}

		bl = &breakableListener{Listener: ln}

		return bl, nil
	}
	require.NoError(t, srv.Init(context.Background()))

	cancel, errCh := startServer(t, srv)
	defer cancel()

	addr := srv.Addr()
	servedSnapshot(t, srv)

	bl.breakNow()

	awaitSelfStop(t, srv, errCh)
	require.Equal(t, 1, countLines(logger.Lines(), "http server failed"))

	ln, err := net.Listen("tcp", addr)
	require.NoError(t, err)
	require.NoError(t, ln.Close())
}

func unguardedGoStatements(fset *token.FileSet, file *ast.File) []string {
	var bad []string

	ast.Inspect(file, func(n ast.Node) bool {
		stmt, ok := n.(*ast.GoStmt)
		if !ok {
			return true
		}

		if !startsWithGuard(stmt) {
			bad = append(bad, fset.Position(stmt.Pos()).String())
		}

		return true
	})

	return bad
}

func startsWithGuard(stmt *ast.GoStmt) bool {
	lit, ok := stmt.Call.Fun.(*ast.FuncLit)
	if !ok || len(lit.Body.List) == 0 {
		return false
	}

	deferStmt, ok := lit.Body.List[0].(*ast.DeferStmt)
	if !ok {
		return false
	}

	sel, ok := deferStmt.Call.Fun.(*ast.SelectorExpr)
	if !ok {
		return false
	}

	pkg, ok := sel.X.(*ast.Ident)

	return ok && pkg.Name == "guard" && sel.Sel.Name == "Recover"
}

func TestGoroutineGuardCheckerRejectsUnguarded(t *testing.T) {
	src := `package x

func f(h hub) {
	go func() { defer guard.Recover(r, "ok"); work() }()
	go func() { work() }()
	go func() { defer wg.Done(); defer guard.Recover(r, "late") }()
	go h.loop()
	go func() { defer other.Recover(r, "x") }()
}`

	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, "x.go", src, 0)
	require.NoError(t, err)
	require.Len(t, unguardedGoStatements(fset, file), 4)
}

func TestEveryGoroutineIsRecovered(t *testing.T) {
	fset := token.NewFileSet()
	checked := 0

	err := filepath.WalkDir(".", func(path string, d fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}

		if d.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}

		file, err := parser.ParseFile(fset, path, nil, 0)
		require.NoError(t, err)

		checked++

		require.Empty(t, unguardedGoStatements(fset, file), "go statements must start with defer guard.Recover")

		return nil
	})
	require.NoError(t, err)
	require.Greater(t, checked, 10)
}
