package aerospike

import (
	"context"
	"os"

	"github.com/bsv-blockchain/teranode/util/test"
	aeroTest "github.com/bsv-blockchain/testcontainers-aerospike-go"
	"github.com/testcontainers/testcontainers-go"
)

// runAerospikeTestContainer starts an Aerospike server for a test: test.AerospikeImage, or the
// image in AEROSPIKE_CONTAINER_IMAGE (for example the mod-teranode fork) on the platform in
// AEROSPIKE_CONTAINER_PLATFORM, linux/amd64 when that is unset.
func runAerospikeTestContainer(ctx context.Context, opts ...testcontainers.ContainerCustomizer) (*aeroTest.Container, error) {
	image := os.Getenv("AEROSPIKE_CONTAINER_IMAGE")
	if image == "" {
		return aeroTest.RunContainer(ctx, append(opts, aeroTest.WithImage(test.AerospikeImage))...)
	}

	opts = append(opts, aeroTest.WithImage(image))

	platform := os.Getenv("AEROSPIKE_CONTAINER_PLATFORM")
	if platform == "" {
		platform = "linux/amd64"
	}
	opts = append(opts, testcontainers.WithImagePlatform(platform))

	return aeroTest.RunContainer(ctx, opts...)
}
