package teraslab_test

import (
	"context"
	"net/url"
	"testing"
	"time"

	"github.com/bsv-blockchain/teranode/errors"
	teraslabstore "github.com/bsv-blockchain/teranode/stores/utxo/teraslab"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
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
	require.Nil(t, store)
	require.Contains(t, err.Error(), "pool_size")
	// Typed so callers can classify the failure, matching the other backends.
	require.ErrorIs(t, err, errors.ErrInvalidArgument)
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

	// Bound the call so the test fails fast and deterministically even if the
	// client falls back to OS-level dial timeouts for an unreachable seed.
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	store, err := teraslabstore.New(ctx, logger, tSettings, u)
	require.Error(t, err)
	require.Nil(t, store)
	require.Contains(t, err.Error(), "client init")
	// Client-init failure is a typed storage-class error, not a bare fmt.Errorf.
	require.ErrorIs(t, err, errors.ErrStorageError)
}
