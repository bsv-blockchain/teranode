package main

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/hex"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// mainSubprocessArgs, when set, makes the test binary run main() with these
// daemon args instead of the tests. The exit-status tests re-execute the test
// binary this way, so the status they observe is the one main() and RunDaemon
// produce, with no stand-in for either.
const mainSubprocessArgs = "TERANODE_TEST_MAIN_ARGS"

func TestMain(m *testing.M) {
	if args, ok := os.LookupEnv(mainSubprocessArgs); ok {
		os.Args = append([]string{progname}, strings.Fields(args)...)

		main()

		// A process whose main returns exits 0.
		os.Exit(0)
	}

	os.Exit(m.Run())
}

// healthListening matches the line Start logs once the health server has its
// listener, which carries the address actually bound.
var healthListening = regexp.MustCompile(`Health check endpoint listening on (\S+)`)

// mainChild is a running child process executing main().
type mainChild struct {
	cmd     *exec.Cmd
	logPath string
	done    chan struct{} // closed once cmd.Wait has returned
	waitErr error         // cmd.Wait's result, valid after done is closed
}

// output returns everything the child has written so far.
func (c *mainChild) output() string {
	b, _ := os.ReadFile(c.logPath)
	return string(b)
}

// exitCode waits (bounded) for the child to exit and returns its status.
func (c *mainChild) exitCode(t *testing.T, timeout time.Duration) int {
	t.Helper()

	select {
	case <-c.done:
	case <-time.After(timeout):
		t.Fatalf("child did not exit within %v:\n%s", timeout, c.output())
	}

	if c.waitErr == nil {
		return 0
	}

	exitErr, ok := c.waitErr.(*exec.ExitError) //nolint:errorlint // Wait returns *ExitError unwrapped
	require.True(t, ok, "child did not run to an exit status: %v\n%s", c.waitErr, c.output())

	return exitErr.ExitCode()
}

// socketDir returns a fresh directory for the child's gocore control socket.
//
// gocore.Log, which RunDaemon calls, unlinks <socketDIR>/TERANODE.sock and
// binds its own listener there, and socketDIR defaults to the shared
// /tmp/gocore. A child left on the default would take over the control socket
// of any node running as the same user. A Unix socket path is limited to
// about 104 bytes, so a long TMPDIR (as some sandboxes set) falls back to
// /tmp.
func socketDir(t *testing.T) string {
	t.Helper()

	base := os.TempDir()
	if len(base) > 60 {
		base = "/tmp"
	}

	dir, err := os.MkdirTemp(base, "tn-sock-")
	require.NoError(t, err)

	t.Cleanup(func() { _ = os.RemoveAll(dir) })

	return dir
}

// startMainChild runs main() in a child process with only the blockchain
// service enabled. Settings reach it through the environment, which gocore
// reads ahead of settings.conf; env overrides those defaults.
//
// The child's cleanup kills it if it is still running and always reaps it.
// It is registered after the socket directory's, so it runs first and the
// directory outlives the child.
func startMainChild(t *testing.T, env ...string) *mainChild {
	t.Helper()

	sockDir := socketDir(t)

	logFile, err := os.Create(filepath.Join(t.TempDir(), "child.log"))
	require.NoError(t, err)

	defer func() { _ = logFile.Close() }() // the child holds its own descriptor

	cmd := exec.Command(os.Args[0], "-test.run=^$") //nolint:gosec // re-executes this test binary
	cmd.Env = append(os.Environ(),
		mainSubprocessArgs+"=-all=0 -blockchain=1",
		"socketDIR="+sockDir,
		// The health address is discovered from an INFO line, so the child
		// must log at INFO whatever level the parent's environment selects.
		// exec.Cmd uses the last value of a duplicated key.
		"logLevel=INFO",
		"health_check_httpListenAddress=127.0.0.1:0",
		"blockchain_grpcListenAddress=127.0.0.1:0",
		"blockchain_httpListenAddress=127.0.0.1:0",
		"grpc_admin_api_key=exit-status-test-key-0123456789abcdef",
		"profilerAddr=",
		"prometheusEndpoint=",
	)
	cmd.Env = append(cmd.Env, env...)

	// An *os.File goes to the child directly, with no copying goroutine in
	// this process, so reading the log back cannot race with the writes.
	cmd.Stdout = logFile
	cmd.Stderr = logFile

	require.NoError(t, cmd.Start())

	c := &mainChild{cmd: cmd, logPath: logFile.Name(), done: make(chan struct{})}

	// The only Wait. Everything else observes done.
	go func() {
		c.waitErr = cmd.Wait()
		close(c.done)
	}()

	t.Cleanup(func() {
		select {
		case <-c.done:
		default:
			_ = cmd.Process.Kill()
			<-c.done
		}
	})

	return c
}

// TestMainExitsNonZeroWhenTheDaemonFails: a service that cannot start must
// end the process with a non-zero status, or on-failure restart policies
// (Docker, systemd) never fire. Guards both RunDaemon passing Start's error up
// and main turning it into the exit status.
func TestMainExitsNonZeroWhenTheDaemonFails(t *testing.T) {
	// An unsupported store scheme fails the blockchain service in
	// startServices, before blockchain binds anything, after RunDaemon has handed
	// over to Start.
	child := startMainChild(t, "blockchain_store=nosuchscheme:///exit_status")

	require.Equal(t, 1, child.exitCode(t, 2*time.Minute), "child output:\n%s", child.output())
	require.Contains(t, child.output(), "blockchain: unknown scheme", "the child failed for another reason")
}

// TestMainExitsZeroOnRequestedShutdown: a SIGTERM to a healthy node is a
// requested stop, and the process must still exit 0.
func TestMainExitsZeroOnRequestedShutdown(t *testing.T) {
	// The health server binds port 0 and the address is read from the child's
	// log. The blockchain service's readiness check dials its configured
	// addresses, so those need real ports: they are picked free here, and a
	// child that loses one to another process in between is retried. Nothing
	// else is retried.
	const attempts = 3

	for attempt := 1; ; attempt++ {
		grpcAddr := freeLocalAddr(t)

		child := startMainChild(t,
			"blockchain_store=sqlitememory:///exit_status",
			"blockchain_grpcListenAddress="+grpcAddr,
			"blockchain_grpcAddress="+grpcAddr,
			"blockchain_httpListenAddress="+freeLocalAddr(t),
		)

		if !awaitReadiness(t, child, time.Minute) {
			select {
			case <-child.done:
				if attempt < attempts && strings.Contains(child.output(), "address already in use") {
					t.Logf("attempt %d lost a port to another process, retrying", attempt)
					continue
				}
			default:
			}

			t.Fatalf("child never became ready:\n%s", child.output())
		}

		require.NoError(t, child.cmd.Process.Signal(syscall.SIGTERM))
		require.Equal(t, 0, child.exitCode(t, 2*time.Minute), "child output:\n%s", child.output())

		return
	}
}

// awaitReadiness polls the child's health readiness endpoint until it answers
// 200, the child exits, or timeout passes. Every request is bounded, so a
// stalled endpoint cannot hold the test past its deadline.
func awaitReadiness(t *testing.T, child *mainChild, timeout time.Duration) bool {
	t.Helper()

	ctx, cancel := context.WithTimeout(t.Context(), timeout)
	defer cancel()

	client := &http.Client{Timeout: 2 * time.Second}

	for {
		if m := healthListening.FindStringSubmatch(child.output()); m != nil {
			if readinessOK(ctx, client, "http://"+strings.TrimPrefix(m[1], "http://")+"/health/readiness") {
				return true
			}
		}

		select {
		case <-child.done:
			return false
		case <-ctx.Done():
			return false
		case <-time.After(200 * time.Millisecond):
		}
	}
}

// TestMainSplitAlertStartup starts the real alert daemon before blockchain.
// Alert liveness must answer while its blockchain client is still connecting,
// so blockchain can pass its alert gate and alert can initialize its service.
func TestMainSplitAlertStartup(t *testing.T) {
	grpcAddr := freeLocalAddr(t)
	alertAddr := freeLocalAddr(t)
	_, p2pPort, err := net.SplitHostPort(freeLocalAddr(t))
	require.NoError(t, err)
	_, keyBytes, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)

	alert := startMainChild(t,
		mainSubprocessArgs+"=-all=0 -alert=1 -blockchain=0",
		"health_check_httpListenAddress="+alertAddr,
		"blockchain_grpcAddress="+grpcAddr,
		"blockchain_maxRetries=100",
		"blockchain_retrySleep=10",
		"alert_store=sqlitememory:///split_alert",
		"utxostore=sqlitememory:///split_alert_utxo",
		"dataFolder="+t.TempDir(),
		"alert_p2p_private_key="+hex.EncodeToString(keyBytes),
		"ALERT_P2P_PORT="+p2pPort,
		"alert_p2p_allow_private_ips=true",
		"alert_protocol_id=/teranode-test/split-alert/1.0.0",
		"alert_topic_name=teranode-test-split-alert",
	)
	t.Cleanup(func() {
		if t.Failed() {
			t.Logf("alert output:\n%s", alert.output())
		}
	})
	client := &http.Client{Timeout: time.Second}
	ctx := t.Context()
	require.Eventually(t, func() bool {
		return readinessOK(ctx, client, "http://"+alertAddr+"/health/liveness")
	}, 5*time.Second, 20*time.Millisecond)
	require.False(t, readinessOK(ctx, client, "http://"+alertAddr+"/health/readiness"))

	blockchain := startMainChild(t,
		"blockchain_requireAlertSystem=true",
		"alert_healthAddress=http://"+alertAddr,
		"alert_startupTimeout=10s",
		"blockchain_store=sqlitememory:///split_blockchain",
		"blockchain_grpcListenAddress="+grpcAddr,
		"blockchain_grpcAddress="+grpcAddr,
		"blockchain_httpListenAddress="+freeLocalAddr(t),
	)
	require.True(t, awaitReadiness(t, blockchain, 20*time.Second), "blockchain did not become ready:\n%s", blockchain.output())
	require.Eventually(t, func() bool {
		return strings.Contains(alert.output(), "Starting service Alert")
	}, 20*time.Second, 20*time.Millisecond)
	require.True(t, readinessOK(ctx, client, "http://"+alertAddr+"/health/liveness"))

	// Peer discovery is independent of the dependency cycle under test. Stop
	// alert here without waiting for it to join an external alert network.
	require.NoError(t, alert.cmd.Process.Kill())
	<-alert.done
	require.NoError(t, blockchain.cmd.Process.Signal(syscall.SIGTERM))
	require.Equal(t, 0, blockchain.exitCode(t, 20*time.Second), "blockchain output:\n%s", blockchain.output())
}

func readinessOK(ctx context.Context, client *http.Client, url string) bool {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return false
	}

	resp, err := client.Do(req)
	if err != nil {
		return false
	}

	_ = resp.Body.Close()

	return resp.StatusCode == http.StatusOK
}

// freeLocalAddr returns a loopback address that was free a moment ago. It is
// not a reservation: the caller must cope with losing it.
func freeLocalAddr(t *testing.T) string {
	t.Helper()

	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	addr := l.Addr().String()
	require.NoError(t, l.Close())

	return addr
}
