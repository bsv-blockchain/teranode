//go:build teraslab

package containers

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

const (
	teraslabDefaultImage = "ghcr.io/icellan/teraslab:0.9.1"
	teraslabWirePort     = "3300/tcp"
	teraslabHTTPPort     = "9100/tcp"
)

// teraslabSingleNodeConfig is a minimal single-node TeraSlab server config.
//
// The server binds 0.0.0.0 (not the loopback default) so the in-process daemon
// running on the test host can reach it via the mapped port; node_id 0 keeps it
// in single-node mode (no cluster secret) and enable_remote_bind permits the
// non-loopback bind. ServerConfig is #[serde(default)] so a partial TOML is
// sufficient. The image's default CMD is `--config /etc/teraslab/node.toml`, so
// we mount the config there and do NOT override the entrypoint.
const teraslabSingleNodeConfig = `node_id = 0
listen_addr = "0.0.0.0:3300"
http_listen_addr = "0.0.0.0:9100"
enable_remote_bind = true
device_paths = ["/data/teraslab.dat"]
device_size = 1073741824
`

// initializeTeraslab starts a TeraSlab server container and returns a
// teraslab:// URL pointing at the mapped wire port.
//
// The image defaults to ghcr.io/icellan/teraslab:0.9.1 and is always pulled so
// CI exercises the published image. Set TERASLAB_IMAGE to use a locally-built
// dev image not present in any registry (in which case the pull is skipped).
func (cm *ContainerManager) initializeTeraslab(ctx context.Context) (*url.URL, error) {
	image := teraslabDefaultImage
	alwaysPull := true
	if envImage := os.Getenv("TERASLAB_IMAGE"); envImage != "" {
		image = envImage
		alwaysPull = false
	}

	req := testcontainers.ContainerRequest{
		Image:           image,
		AlwaysPullImage: alwaysPull,
		ExposedPorts:    []string{teraslabWirePort, teraslabHTTPPort},
		Files: []testcontainers.ContainerFile{
			{
				Reader:            strings.NewReader(teraslabSingleNodeConfig),
				ContainerFilePath: "/etc/teraslab/node.toml",
				FileMode:          0o644,
			},
		},
		WaitingFor: wait.ForHTTP("/health/live").
			WithPort(teraslabHTTPPort).
			WithStartupTimeout(60 * time.Second),
	}

	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		return nil, errors.NewExternalError("failed to start TeraSlab container: %v", err)
	}

	cm.cleanupFunc = func() error {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		return container.Terminate(cleanupCtx)
	}

	// The container is started, so every error path below must terminate it via
	// cleanupFunc before returning, or a failing test run leaks the container.
	host, err := container.Host(ctx)
	if err != nil {
		_ = cm.cleanupFunc()
		return nil, errors.NewExternalError("failed to get TeraSlab host: %v", err)
	}

	mappedPort, err := container.MappedPort(ctx, teraslabWirePort)
	if err != nil {
		_ = cm.cleanupFunc()
		return nil, errors.NewExternalError("failed to get TeraSlab wire port: %v", err)
	}

	cm.containerURL = fmt.Sprintf("teraslab://%s:%s", host, mappedPort.Port())

	parsedURL, err := url.Parse(cm.containerURL)
	if err != nil {
		_ = cm.cleanupFunc()
		return nil, errors.NewExternalError("failed to parse TeraSlab URL: %v", err)
	}

	return parsedURL, nil
}
