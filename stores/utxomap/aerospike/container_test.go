package aerospike

import (
	"context"
	"fmt"
	"github.com/stretchr/testify/assert"
	"testing"

	aero "github.com/aerospike/aerospike-client-go/v7"
	aeroTest "github.com/ajeetdsouza/testcontainers-aerospike-go"
	"github.com/stretchr/testify/require"
)

func TestAerospikeContainer(t *testing.T) {
	aeroClient := setupAerospike(t)
	// your code here

	assert.NotNil(t, aeroClient)
	fmt.Printf("Aerospike client: %v\n", aeroClient)
}

func setupAerospike(t *testing.T) *aero.Client {
	ctx := context.Background()

	container, err := aeroTest.RunContainer(ctx, aeroTest.WithImage("aerospike:ce-6.4.0.7_2"))
	require.NoError(t, err)
	t.Cleanup(func() {
		err := container.Terminate(ctx)
		require.NoError(t, err)
	})

	host, err := container.Host(ctx)
	require.NoError(t, err)
	port, err := container.ServicePort(ctx)
	require.NoError(t, err)

	client, err := aero.NewClient(host, port)
	require.NoError(t, err)

	return client
}
