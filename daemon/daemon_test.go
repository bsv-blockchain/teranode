package daemon

import (
	"context"
	"testing"
	"time"

	"github.com/bitcoin-sv/teranode/settings"
	"github.com/bitcoin-sv/teranode/ulogger"
	"github.com/ordishs/gocore"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func init() {
	// Initialize test settings
	gocore.Config().Set("network", "regtest")
	gocore.Config().Set("use_cgo_verifier", "false")
	gocore.Config().Set("use_cgo_signer", "false")
	gocore.Config().Set("use_otel_tracing", "false")
	gocore.Config().Set("use_open_tracing", "false")
	gocore.Config().Set("profilerAddr", "")
	gocore.Config().Set("prometheusEndpoint", "")
}

func TestNew(t *testing.T) {
	d := New()
	require.NotNil(t, d)
	require.NotNil(t, d.doneCh)
}

func TestDaemon_Stop(t *testing.T) {
	d := New()

	// Create a goroutine to check if the done channel is closed
	done := make(chan bool)

	go func() {
		<-d.doneCh
		done <- true
	}()

	// Stop the daemon
	d.Stop()

	// Wait for the done signal or timeout
	select {
	case <-done:
		// Channel was closed successfully
	case <-time.After(time.Second):
		t.Fatal("Timeout waiting for daemon to stop")
	}
}

func TestShouldStart(t *testing.T) {
	tests := []struct {
		name     string
		app      string
		args     []string
		expected bool
	}{
		{
			name:     "empty args",
			app:      "test_app",
			args:     []string{},
			expected: false,
		},
		{
			name:     "app flag present",
			app:      "test_app",
			args:     []string{"-test_app=1"},
			expected: true,
		},
		{
			name:     "app flag present but disabled",
			app:      "test_app",
			args:     []string{"-test_app=0"},
			expected: false,
		},
		{
			name:     "different app flag",
			app:      "test_app",
			args:     []string{"-other_app=1"},
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := shouldStart(tt.app, tt.args)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestDaemon_Start_Basic(t *testing.T) {
	d := New()
	// logger := ulogger.NewVerboseTestLogger(t)
	logger := ulogger.TestLogger{}
	args := []string{} // No special flags
	tSettings := settings.NewSettings()

	// Create a ready channel
	readyCh := make(chan struct{})

	// Start the daemon in a goroutine
	go func() {
		d.Start(logger, args, tSettings, readyCh)
	}()

	select {
	case <-readyCh:
		// Daemon started successfully
	case <-time.After(10 * time.Second):
		t.Fatal("Timeout waiting for daemon to start")
	}

	// Stop the daemon
	d.Stop()
}

func TestDaemon_Start_WithContext(t *testing.T) {
	d := New()
	logger := ulogger.NewVerboseTestLogger(t)
	args := []string{} // No special flags
	tSettings := settings.NewSettings()

	// Create a context that we'll cancel
	ctx, cancel := context.WithCancel(context.Background())

	// Start the daemon in a goroutine
	go func() {
		d.Start(logger, args, tSettings)
	}()

	// Cancel the context after a short delay
	go func() {
		time.Sleep(100 * time.Millisecond)
		cancel()
	}()

	// Wait for context cancellation
	<-ctx.Done()

	// Stop the daemon
	d.Stop()
}
