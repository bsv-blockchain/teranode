package aerospike

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/bsv-blockchain/teranode/stores/utxo/aerospike"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/bsv-blockchain/teranode/util/uaerospike"
	aerospike2 "github.com/bsv-blockchain/testcontainers-aerospike-go"
	"github.com/testcontainers/testcontainers-go"
)

// init disables the testcontainers Ryuk reaper sidecar on CI only. Ryuk pulls
// a separate image from Docker Hub which is the dominant flake source on CI
// runners (rate-limit timeouts, 504 gateway errors). The container is
// terminated explicitly in the cleanup func returned from
// InitAerospikeContainer, so the reaper provides no additional safety on
// ephemeral CI runners.
//
// On local dev machines Ryuk is left enabled: it cleans up containers when a
// test process exits without graceful teardown (panic, SIGKILL, OOM), which
// is the dominant leak source locally. Users can override either way by
// setting TESTCONTAINERS_RYUK_DISABLED explicitly before invoking tests.
func init() {
	if _, set := os.LookupEnv("TESTCONTAINERS_RYUK_DISABLED"); set {
		return
	}
	if os.Getenv("CI") == "" {
		return
	}
	_ = os.Setenv("TESTCONTAINERS_RYUK_DISABLED", "true")
}

// InitAerospikeContainer starts an Aerospike server for a test: test.AerospikeImage, or the image
// AEROSPIKE_CONTAINER_IMAGE names (the BSV fork, to run the native mod-teranode operations), on the
// platform in AEROSPIKE_CONTAINER_PLATFORM when that is set.
func InitAerospikeContainer() (string, func() error, error) {
	aerospike.InitPrometheusMetrics()

	ctx := context.Background()

	opts := []testcontainers.ContainerCustomizer{aerospike2.WithImage(test.AerospikeImage), aerospike2.WithTTLSupport("test")}
	if image := os.Getenv("AEROSPIKE_CONTAINER_IMAGE"); image != "" {
		opts = append(opts, aerospike2.WithImage(image))
	}

	if platform := os.Getenv("AEROSPIKE_CONTAINER_PLATFORM"); platform != "" {
		opts = append(opts, testcontainers.WithImagePlatform(platform))
	}

	container, err := aerospike2.RunContainer(ctx, opts...)
	if err != nil {
		return "", nil, err
	}

	cleanup := func() error {
		// Create a new context with timeout for cleanup to prevent hanging
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		return container.Terminate(cleanupCtx)
	}

	host, err := container.Host(ctx)
	if err != nil {
		return "", cleanup, err
	}

	port, err := container.ServicePort(ctx)
	if err != nil {
		return "", cleanup, err
	}

	// raw client to be able to do gets and cleanup
	client, aeroErr := uaerospike.NewClient(host, port)
	if aeroErr != nil {
		return "", cleanup, aeroErr
	}

	aerospikeContainerURL := fmt.Sprintf("aerospike://%s:%d/%s?set=%s&expiration=%s&externalStore=file://./data/externalStore", host, port, "test", "test", "10m")

	return aerospikeContainerURL, func() error {
		client.Close()
		return cleanup()
	}, nil
}
