package teraslab_test

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"testing"

	"github.com/bsv-blockchain/teranode/settings"
	teraslabstore "github.com/bsv-blockchain/teranode/stores/utxo/teraslab"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

const (
	teraslabDefaultImage = "ghcr.io/icellan/teraslab:latest"
	teraslabWirePort     = "3300/tcp"
	teraslabHTTPPort     = "9100/tcp"
)

// initTeraSlab creates a TeraSlab testcontainer and returns a configured Store.
func initTeraSlab(t *testing.T, tSettings *settings.Settings, logger ulogger.Logger) (*teraslabstore.Store, context.Context, func()) {
	ctx := context.Background()

	image := teraslabDefaultImage
	if envImage := os.Getenv("TERASLAB_IMAGE"); envImage != "" {
		image = envImage
	}

	req := testcontainers.ContainerRequest{
		Image:           image,
		AlwaysPullImage: true,
		ExposedPorts:    []string{teraslabWirePort, teraslabHTTPPort},
		Entrypoint:      []string{"teraslab-server"}, // override to avoid default --config CMD
		WaitingFor:      wait.ForHTTP("/health/live").WithPort(teraslabHTTPPort),
	}

	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		t.Skipf("Skipping TeraSlab integration tests: container not available (%v)", err)
	}

	t.Cleanup(func() {
		if container != nil {
			_ = container.Terminate(ctx)
		}
	})

	host, err := container.Host(ctx)
	require.NoError(t, err)

	mappedPort, err := container.MappedPort(ctx, teraslabWirePort)
	require.NoError(t, err)

	teraslabURL := fmt.Sprintf("teraslab://%s:%s", host, mappedPort.Port())
	parsedURL, err := url.Parse(teraslabURL)
	require.NoError(t, err)

	store, err := teraslabstore.New(ctx, logger, tSettings, parsedURL)
	require.NoError(t, err)

	// Set initial block height
	err = store.SetBlockHeight(1)
	require.NoError(t, err)

	return store, ctx, func() {}
}

// initTeraSlabWithDefaults creates a TeraSlab test store with default test settings.
func initTeraSlabWithDefaults(t *testing.T) (*teraslabstore.Store, context.Context, func()) {
	logger := ulogger.NewErrorTestLogger(t)
	tSettings := test.CreateBaseTestSettings(t)
	return initTeraSlab(t, tSettings, logger)
}
