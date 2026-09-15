package teranodecli

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"syscall"
	"testing"
	"time"

	"github.com/bsv-blockchain/teranode/pkg/replayrecovery"
	"github.com/ordishs/gocore"
	"github.com/stretchr/testify/require"
)

// Run Start in a subprocess so os.Exit and process-global logger settings match
// the executable, while keeping stdout and stderr independently observable.
func TestRecoveryCLIStdoutContainsNoLogs(t *testing.T) {
	for _, config := range []struct {
		name        string
		jsonLogging string
		env         []string
	}{
		{name: "pretty", jsonLogging: "false"},
		{name: "pretty and dual JSON", jsonLogging: "true"},
		{name: "environment overrides", jsonLogging: "false", env: []string{"PRETTY_LOGS=true", "jsonLogging=true"}},
	} {
		t.Run(config.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
			defer cancel()
			cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestRecoveryCLIOutputHelper$")
			cmd.Env = append(os.Environ(), "RECOVERY_CLI_OUTPUT_HELPER=1", "RECOVERY_JSON_LOGGING="+config.jsonLogging)
			cmd.Env = append(cmd.Env, config.env...)
			var stdout, stderr bytes.Buffer
			cmd.Stdout, cmd.Stderr = &stdout, &stderr
			err := cmd.Run()
			var exit *exec.ExitError
			require.ErrorAs(t, err, &exit)
			require.Equal(t, 1, exit.ExitCode())
			require.Empty(t, stdout.String(), "startup and validation logs must not contaminate the JSON output stream")
			require.Contains(t, stderr.String(), "Using default resolver")
			require.Contains(t, stderr.String(), "work-dir is required")
		})
	}
}

func TestRecoverySignalsCancelGracefully(t *testing.T) {
	for _, sig := range []os.Signal{os.Interrupt, syscall.SIGTERM, syscall.SIGHUP} {
		t.Run(sig.String(), func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
			defer cancel()
			cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestRecoverySignalHelper$")
			cmd.Env = append(os.Environ(), "RECOVERY_SIGNAL_HELPER=1")
			stdout, err := cmd.StdoutPipe()
			require.NoError(t, err)
			var stderr bytes.Buffer
			cmd.Stderr = &stderr
			require.NoError(t, cmd.Start())
			ready := make([]byte, len("ready\n"))
			_, err = io.ReadFull(stdout, ready)
			require.NoError(t, err)
			require.Equal(t, "ready\n", string(ready))
			require.NoError(t, cmd.Process.Signal(sig))
			var report replayrecovery.Summary
			err = json.NewDecoder(stdout).Decode(&report)
			waitErr := cmd.Wait()
			require.NoError(t, waitErr, stderr.String())
			require.NoError(t, err)
			require.Equal(t, "canceled", report.Stage)
			require.False(t, report.Complete)
		})
	}
}

func TestRecoverySignalHelper(t *testing.T) {
	if os.Getenv("RECOVERY_SIGNAL_HELPER") != "1" {
		return
	}
	ctx, stop := recoveryContext()
	defer stop()
	fmt.Println("ready")
	<-ctx.Done()
	if ctx.Err() != context.Canceled {
		os.Exit(2)
	}
	_ = json.NewEncoder(os.Stdout).Encode(replayrecovery.Summary{Stage: "canceled"})
	os.Exit(0)
}

func TestRecoveryCLIOutputHelper(t *testing.T) {
	if os.Getenv("RECOVERY_CLI_OUTPUT_HELPER") != "1" {
		return
	}
	gocore.Config().Set("PRETTY_LOGS", "true")
	gocore.Config().Set("jsonLogging", os.Getenv("RECOVERY_JSON_LOGGING"))
	gocore.Config().Set("logLevel", "INFO")
	Start([]string{"recoverreplayedtransactions"}, "test", "test")
}
