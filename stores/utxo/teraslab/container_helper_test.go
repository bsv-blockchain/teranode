//go:build teraslab

package teraslab_test

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
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
	teraslabDefaultImage = "ghcr.io/icellan/teraslab:0.9.1"
	teraslabWirePort     = "3300/tcp"
	teraslabHTTPPort     = "9100/tcp"
)

// initTeraSlab creates a TeraSlab testcontainer and returns a configured Store.
func initTeraSlab(t *testing.T, tSettings *settings.Settings, logger ulogger.Logger) (*teraslabstore.Store, context.Context, func()) {
	ctx := context.Background()

	image := teraslabDefaultImage
	// Force a registry pull for the default published image so CI always tests
	// it. When an explicit TERASLAB_IMAGE is provided (e.g. a locally-built dev
	// image not in any registry), do NOT force a pull — otherwise testcontainers
	// errors trying to fetch a tag that only exists in the local Docker daemon.
	alwaysPull := true
	if envImage := os.Getenv("TERASLAB_IMAGE"); envImage != "" {
		image = envImage
		alwaysPull = false
	}

	// Single-node config so the server binds 0.0.0.0 (reachable from the test
	// host via the mapped port) instead of the loopback default. ServerConfig
	// is #[serde(default)], so a partial TOML suffices; node_id 0 keeps it in
	// single-node mode (no cluster_secret required) and enable_remote_bind
	// permits the non-loopback bind. The image's default CMD is
	// `--config /etc/teraslab/node.toml`, so we mount the config there and do
	// NOT override the entrypoint (the previous bare-entrypoint override used
	// the loopback defaults, which testcontainers could never reach → all
	// integration tests skipped).
	cfg := `node_id = 0
listen_addr = "0.0.0.0:3300"
http_listen_addr = "0.0.0.0:9100"
enable_remote_bind = true
device_paths = ["/data/teraslab.dat"]
device_size = 1073741824
`
	cfgPath := filepath.Join(t.TempDir(), "node.toml")
	require.NoError(t, os.WriteFile(cfgPath, []byte(cfg), 0o644))

	req := testcontainers.ContainerRequest{
		Image:           image,
		AlwaysPullImage: alwaysPull,
		ExposedPorts:    []string{teraslabWirePort, teraslabHTTPPort},
		Files: []testcontainers.ContainerFile{
			{
				HostFilePath:      cfgPath,
				ContainerFilePath: "/etc/teraslab/node.toml",
				FileMode:          0o644,
			},
		},
		WaitingFor: wait.ForHTTP("/health/live").WithPort(teraslabHTTPPort),
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
