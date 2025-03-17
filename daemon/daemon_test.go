package daemon

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/bitcoin-sv/teranode/settings"
	testkafka "github.com/bitcoin-sv/teranode/test/util/kafka"
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
	require.NotNil(t, d.stopCh)
}

func TestDaemon_Stop(t *testing.T) {
	d := New()
	done := make(chan struct{})

	go func() {
		<-d.stopCh
		close(done)
	}()

	// Stop the daemon
	require.NoError(t, d.Stop(1*time.Second))

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
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	d := New()

	logger := ulogger.NewErrorTestLogger(t, cancel)

	if !isKafkaRunning() {
		kafkaContainer, err := testkafka.RunTestContainer(ctx)
		require.NoError(t, err)

		t.Cleanup(func() {
			_ = kafkaContainer.CleanUp()
		})

		gocore.Config().Set("KAFKA_PORT", strconv.Itoa(kafkaContainer.KafkaPort))
	}

	args := []string{"-all=0", "-blockchain=1"}
	tSettings := settings.NewSettings()

	// Create a ready channel
	readyCh := make(chan struct{})

	// Start the daemon in a goroutine
	go func() {
		d.Start(logger, args, tSettings, readyCh)
	}()

	select {
	case <-readyCh:
		// Stop the daemon
		require.NoError(t, d.Stop(5*time.Second))

	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for readyCh")
	}
}
