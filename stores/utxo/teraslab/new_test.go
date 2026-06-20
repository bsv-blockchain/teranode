package teraslab_test

import (
	"context"
	"net/url"
	"testing"

	teraslabstore "github.com/bsv-blockchain/teranode/stores/utxo/teraslab"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestNewInvalidPoolSize verifies New() rejects a malformed pool_size query
// parameter before attempting any connection (so no server is required).
func TestNewInvalidPoolSize(t *testing.T) {
	logger := ulogger.NewErrorTestLogger(t)
	tSettings := test.CreateBaseTestSettings(t)

	u, err := url.Parse("teraslab://localhost:3300?pool_size=not-a-number")
	require.NoError(t, err)

	store, err := teraslabstore.New(context.Background(), logger, tSettings, u)
	require.Error(t, err)
	assert.Nil(t, store)
	assert.Contains(t, err.Error(), "pool_size")
}

// TestNewClusterModeUnreachable verifies two things at once, without a server:
//   - the cluster= query parameter is parsed into seed addresses (primary +
//     extras), exercising the cluster branch of New; and
//   - cluster init connects eagerly to its seeds, so unreachable seeds make the
//     client-init step fail and New surfaces that error (returning a nil store).
func TestNewClusterModeUnreachable(t *testing.T) {
	logger := ulogger.NewErrorTestLogger(t)
	tSettings := test.CreateBaseTestSettings(t)

	// Nothing is listening on these ports, so cluster seed discovery must fail.
	u, err := url.Parse("teraslab://127.0.0.1:3300?cluster=127.0.0.1:3301,127.0.0.1:3302&pool_size=4")
	require.NoError(t, err)

	store, err := teraslabstore.New(context.Background(), logger, tSettings, u)
	require.Error(t, err)
	assert.Nil(t, store)
	assert.Contains(t, err.Error(), "client init")
}
