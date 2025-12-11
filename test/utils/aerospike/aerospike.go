package aerospike

import (
	"context"
	"fmt"
	"time"

	"github.com/bsv-blockchain/teranode/stores/utxo/aerospike"
	"github.com/bsv-blockchain/teranode/util/uaerospike"
	aerospike2 "github.com/bsv-blockchain/testcontainers-aerospike-go"
)

func InitAerospikeContainer() (string, func() error, error) {
	aerospike.InitPrometheusMetrics()

	ctx := context.Background()

	container, err := aerospike2.RunContainer(ctx, aerospike2.WithTTLSupport("test"))
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
