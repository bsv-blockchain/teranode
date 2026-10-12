package aerospike

import (
	"context"
	"os"

	aeroTest "github.com/bsv-blockchain/testcontainers-aerospike-go"
	"github.com/testcontainers/testcontainers-go"
)

func runAerospikeTestContainer(ctx context.Context, opts ...testcontainers.ContainerCustomizer) (*aeroTest.Container, error) {
	image := os.Getenv("AEROSPIKE_CONTAINER_IMAGE")
	if image != "" {
		opts = append(opts, aeroTest.WithImage(image))

		platform := os.Getenv("AEROSPIKE_CONTAINER_PLATFORM")
		if platform == "" {
			platform = "linux/amd64"
		}
		opts = append(opts, testcontainers.WithImagePlatform(platform))
	}

	return aeroTest.RunContainer(ctx, opts...)
}
