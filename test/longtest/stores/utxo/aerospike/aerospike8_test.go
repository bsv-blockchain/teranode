package aerospike

import (
	"context"
	"testing"

	"github.com/aerospike/aerospike-client-go/v8"
	"github.com/bsv-blockchain/teranode/errors"
	aeroTest "github.com/bsv-blockchain/testcontainers-aerospike-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAerospike8TransactionSupport(t *testing.T) {
	t.Skip("Aerospike 8.0 community edition does not support transactions")

	ctx := context.Background()

	container, err := aeroTest.RunContainer(ctx, aeroTest.WithImage("aerospike/aerospike-server:8.0"), aeroTest.WithTTLSupport("test"))
	require.NoError(t, err)

	t.Cleanup(func() {
		err = container.Terminate(ctx)
		require.NoError(t, err)
	})

	host, err := container.Host(ctx)
	require.NoError(t, err)

	port, err := container.ServicePort(ctx)
	require.NoError(t, err)

	client, err := aerospike.NewClient(host, port)
	require.NoError(t, err)

	t.Run("TestAerospikeTransactionAbort", func(t *testing.T) {
		// Begin a transaction
		policy := aerospike.NewWritePolicy(0, 0)

		txn := aerospike.NewTxn()
		policy.Txn = txn

		key1, err := aerospike.NewKey("test", "test", []byte("1"))
		require.NoError(t, err)

		bin1 := aerospike.NewBin("bin1", "value1")

		_, err = client.Get(nil, key1)
		assert.Error(t, err)
		assert.True(t, errors.Is(err, aerospike.ErrKeyNotFound))

		// Perform put operations within the transaction
		err = client.PutBins(policy, key1, bin1)
		require.NoError(t, err)

		// Verify that the record exists
		value, err := client.Get(nil, key1)
		require.NoError(t, err)
		assert.Equal(t, "value1", value.Bins["bin1"])

		// Abort the transaction by not committing
		status, err := client.Abort(txn)
		require.NoError(t, err)
		assert.Equal(t, aerospike.AbortStatusOK, status)

		// Verify that the record does not exist
		_, err = client.Get(nil, key1)
		assert.Error(t, err)
		assert.True(t, errors.Is(err, aerospike.ErrKeyNotFound))
	})
}
