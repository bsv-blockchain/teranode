package rpc

import (
	"context"
	"testing"
	"time"

	"github.com/bitcoin-sv/teranode/services/propagation"
	"github.com/bitcoin-sv/teranode/settings"
	"github.com/bitcoin-sv/teranode/ulogger"
	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/stretchr/testify/assert"
)

func TestDistributor_Options(t *testing.T) {
	t.Run("WithBackoffDuration", func(t *testing.T) {
		backoff := 5 * time.Second
		d := &Distributor{}
		opt := WithBackoffDuration(backoff)
		opt(d)
		assert.Equal(t, backoff, d.backoff)
	})

	t.Run("WithRetryAttempts", func(t *testing.T) {
		attempts := int32(3)
		d := &Distributor{}
		opt := WithRetryAttempts(attempts)
		opt(d)
		assert.Equal(t, attempts, d.attempts)
	})

	t.Run("WithFailureTolerance", func(t *testing.T) {
		tolerance := 25
		d := &Distributor{}
		opt := WithFailureTolerance(tolerance)
		opt(d)
		assert.Equal(t, tolerance, d.failureTolerance)
	})
}

func TestDistributor_SendTransaction_Basic(t *testing.T) {
	logger := ulogger.TestLogger{}

	// Create a test transaction
	tx := bt.NewTx()

	t.Run("No propagation servers", func(t *testing.T) {
		d := &Distributor{
			logger:             logger,
			propagationServers: make(map[string]*propagation.Client),
			attempts:           1,
			failureTolerance:   50,
			settings: &settings.Settings{
				Coinbase: settings.CoinbaseSettings{
					DistributorTimeout: 5 * time.Second,
				},
			},
		}

		responses, err := d.SendTransaction(context.Background(), tx)
		// When there are no servers, it should error with NaN%
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "NaN")
		assert.Empty(t, responses)
	})
}

func TestDistributor_GetPropagationGRPCAddresses(t *testing.T) {
	d := &Distributor{
		propagationServers: map[string]*propagation.Client{
			"server1": nil, // We just need the keys for this test
			"server2": nil,
		},
	}

	addresses := d.GetPropagationGRPCAddresses()
	assert.Len(t, addresses, 2)
	assert.Contains(t, addresses, "server1")
	assert.Contains(t, addresses, "server2")
}

func TestDistributor_TriggerBatcher(t *testing.T) {
	d := &Distributor{
		propagationServers: map[string]*propagation.Client{
			// Empty map - TriggerBatcher iterates over the map but doesn't crash on empty map
		},
	}

	// This should not panic with empty propagation servers
	assert.NotPanics(t, func() {
		d.TriggerBatcher()
	})
}

func TestResponseWrapper(t *testing.T) {
	t.Run("ResponseWrapper struct", func(t *testing.T) {
		rw := &ResponseWrapper{
			Addr:     "test-server",
			Duration: 100 * time.Millisecond,
			Retries:  2,
		}

		assert.Equal(t, "test-server", rw.Addr)
		assert.Equal(t, 100*time.Millisecond, rw.Duration)
		assert.Equal(t, int32(2), rw.Retries)
		assert.NoError(t, rw.Error)
	})
}

func TestConstants(t *testing.T) {
	t.Run("Error message constants exist", func(t *testing.T) {
		assert.NotEmpty(t, errMsgCreateGRPCClient)
		assert.NotEmpty(t, errMsgCreateClient)
		assert.NotEmpty(t, errMsgConnecting)
		assert.NotEmpty(t, errMsgSendTransaction)
		assert.NotEmpty(t, errMsgSendToServers)
		assert.NotEmpty(t, errMsgDistributing)
		assert.NotEmpty(t, errMsgNoServers)
		assert.NotEmpty(t, errMsgAddress)
	})
}

func TestNewDistributorFromAddress_InvalidAddress(t *testing.T) {
	logger := ulogger.TestLogger{}
	tSettings := &settings.Settings{}

	// Test with invalid address - should fail gracefully
	distributor, err := NewDistributorFromAddress(context.Background(), logger, tSettings, "invalid-address", nil)
	assert.Error(t, err)
	assert.Nil(t, distributor)
}

func TestDistributor_Structure(t *testing.T) {
	t.Run("Distributor struct initialization", func(t *testing.T) {
		logger := ulogger.TestLogger{}

		d := &Distributor{
			logger:             logger,
			propagationServers: make(map[string]*propagation.Client),
			attempts:           3,
			backoff:            time.Second,
			failureTolerance:   50,
			waitMsBetweenTxs:   100,
		}

		assert.NotNil(t, d.logger)
		assert.Equal(t, int32(3), d.attempts)
		assert.Equal(t, time.Second, d.backoff)
		assert.Equal(t, 50, d.failureTolerance)
		assert.Equal(t, 100, d.waitMsBetweenTxs)
		assert.NotNil(t, d.propagationServers)
	})
}

func TestNewDistributor_NoAddresses(t *testing.T) {
	logger := ulogger.TestLogger{}
	tSettings := &settings.Settings{
		Propagation: settings.PropagationSettings{
			GRPCAddresses: []string{}, // Empty addresses
		},
	}

	distributor, err := NewDistributor(context.Background(), logger, tSettings)
	assert.Error(t, err)
	assert.Nil(t, distributor)
	assert.Contains(t, err.Error(), errMsgNoServers)
}
