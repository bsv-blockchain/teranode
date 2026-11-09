package main

import (
	"context"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
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

func freeLocalAddr(t *testing.T) string {
	t.Helper()

	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	addr := l.Addr().String()
	require.NoError(t, l.Close())

	return addr
}

// mainSubprocess builds a command that runs main() in a child process with
// only the blockchain service, its store set to storeURL, and every listener on
// a free loopback port. Settings come from the environment, which gocore reads
// ahead of settings.conf.
func mainSubprocess(t *testing.T, ctx context.Context, storeURL, healthAddr string) *exec.Cmd {
	t.Helper()

	grpcAddr := freeLocalAddr(t)

	cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^$") //nolint:gosec // re-executes this test binary
	cmd.Env = append(os.Environ(),
		mainSubprocessArgs+"=-all=0 -blockchain=1",
		"blockchain_store="+storeURL,
		"blockchain_grpcListenAddress="+grpcAddr,
		"blockchain_grpcAddress="+grpcAddr,
		"blockchain_httpListenAddress="+freeLocalAddr(t),
		"health_check_httpListenAddress="+healthAddr,
		"grpc_admin_api_key=exit-status-test-key-0123456789abcdef",
		"profilerAddr=",
		"prometheusEndpoint=",
	)

	return cmd
}

func exitCode(t *testing.T, err error) int {
	t.Helper()

	if err == nil {
		return 0
	}

	exitErr, ok := err.(*exec.ExitError) //nolint:errorlint // exec returns *ExitError unwrapped
	require.True(t, ok, "child did not run to an exit status: %v", err)

	return exitErr.ExitCode()
}

// TestMainExitsNonZeroWhenTheDaemonFails: a service that cannot start must
// end the process with a non-zero status, or on-failure restart policies
// (Docker, systemd) never fire. Guards both RunDaemon passing Start's error up
// and main turning it into the exit status.
func TestMainExitsNonZeroWhenTheDaemonFails(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
	defer cancel()

	// An unsupported store scheme fails the blockchain service in
	// startServices, after RunDaemon has handed over to Start.
	cmd := mainSubprocess(t, ctx, "nosuchscheme:///exit_status", freeLocalAddr(t))

	out, err := cmd.CombinedOutput()
	require.NoError(t, ctx.Err(), "child did not exit in time:\n%s", out)
	require.Equal(t, 1, exitCode(t, err), "child output:\n%s", out)
}

// TestMainExitsZeroOnRequestedShutdown: a SIGTERM to a healthy node is a
// requested stop, and the process must still exit 0.
func TestMainExitsZeroOnRequestedShutdown(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
	defer cancel()

	healthAddr := freeLocalAddr(t)
	cmd := mainSubprocess(t, ctx, "sqlitememory:///exit_status", healthAddr)

	// An *os.File is handed to the child directly, with no copying goroutine
	// in this process, so reading it back below cannot race with the writes.
	logFile, err := os.Create(filepath.Join(t.TempDir(), "child.log"))
	require.NoError(t, err)

	defer func() { _ = logFile.Close() }()

	cmd.Stdout = logFile
	cmd.Stderr = logFile

	childOutput := func() string {
		b, _ := os.ReadFile(logFile.Name())
		return string(b)
	}

	require.NoError(t, cmd.Start())

	ready := false

	for !ready && ctx.Err() == nil {
		resp, err := http.Get("http://" + healthAddr + "/health/readiness") //nolint:noctx // bounded by the poll loop
		if err == nil {
			ready = resp.StatusCode == http.StatusOK
			_ = resp.Body.Close()
		}

		if !ready {
			time.Sleep(200 * time.Millisecond)
		}
	}

	if !ready {
		t.Fatalf("child never became ready:\n%s", childOutput())
	}

	require.NoError(t, cmd.Process.Signal(syscall.SIGTERM))

	err = cmd.Wait()
	require.NoError(t, ctx.Err(), "child did not exit in time:\n%s", childOutput())
	require.Equal(t, 0, exitCode(t, err), "child output:\n%s", childOutput())
}
