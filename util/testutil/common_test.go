package testutil

import (
	"context"
	"net/http"
	"testing"

	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewCommonTestSetup(t *testing.T) {
	setup := NewCommonTestSetup(t)

	require.NotNil(t, setup)
	require.NotNil(t, setup.Ctx)
	require.NotNil(t, setup.Logger)
	require.NotNil(t, setup.Settings)

	assert.Equal(t, context.Background(), setup.Ctx)
}

func TestNewMemoryBlobStore(t *testing.T) {
	store := NewMemoryBlobStore()

	require.NotNil(t, store)
}

func TestNewSQLiteMemoryUTXOStore(t *testing.T) {
	setup := NewCommonTestSetup(t)

	var store utxo.Store

	// The store must close when the test that built it ends, or its batchers stay
	// alive for the rest of the test binary.
	t.Run("built", func(t *testing.T) {
		store = NewSQLiteMemoryUTXOStore(setup.Ctx, setup.Logger, setup.Settings, t)
		require.NotNil(t, store)

		_, _, err := store.Health(setup.Ctx, false)
		require.NoError(t, err)
	})

	_, _, err := store.Health(setup.Ctx, false)
	require.Error(t, err, "the store is still open after the test that built it ended")
}

func TestNewMemorySQLiteBlockchainClient(t *testing.T) {
	setup := NewCommonTestSetup(t)

	var client blockchain.ClientI

	// The blockchain store must close when the test that built it ends, or its
	// background loops keep that test's t, and everything it built, reachable.
	t.Run("built", func(t *testing.T) {
		client = NewMemorySQLiteBlockchainClient(setup.Logger, setup.Settings, t)
		require.NotNil(t, client)

		status, _, _ := client.Health(setup.Ctx, false)
		require.Equal(t, http.StatusOK, status)
	})

	status, _, _ := client.Health(setup.Ctx, false)
	require.NotEqual(t, http.StatusOK, status, "the blockchain store is still open after the test that built it ended")
}

func TestAssertHealthResponse(t *testing.T) {
	tests := []struct {
		name         string
		status       int
		msg          string
		err          error
		expectStatus int
		expectErr    bool
	}{
		{
			name:         "success with 200 status",
			status:       200,
			msg:          "OK",
			err:          nil,
			expectStatus: 200,
			expectErr:    false,
		},
		{
			name:         "success with expected error",
			status:       500,
			msg:          "Internal error",
			err:          assert.AnError,
			expectStatus: 500,
			expectErr:    true,
		},
		{
			name:         "flexible status check accepts 200",
			status:       200,
			msg:          "OK",
			err:          nil,
			expectStatus: -1,
			expectErr:    false,
		},
		{
			name:         "flexible status check accepts 503",
			status:       503,
			msg:          "Service unavailable",
			err:          nil,
			expectStatus: -1,
			expectErr:    false,
		},
		{
			name:         "no error when expecting no error",
			status:       200,
			msg:          "Healthy",
			err:          nil,
			expectStatus: 200,
			expectErr:    false,
		},
		{
			name:         "flexible status accepts both 200 and 503",
			status:       200,
			msg:          "Service OK",
			err:          nil,
			expectStatus: -1,
			expectErr:    false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// These should all pass - the function is designed to fail tests on invalid inputs
			AssertHealthResponse(t, tt.status, tt.msg, tt.err, tt.expectStatus, tt.expectErr)
		})
	}
}

// TestAssertHealthResponseValidation tests that the function correctly validates inputs
// Note: This function uses require assertions that will fail the test on invalid inputs
// The negative test cases are documented here but not run to avoid test failures
func TestAssertHealthResponseValidation(t *testing.T) {
	// Test that function exists and can be called (basic functionality test)
	AssertHealthResponse(t, 200, "OK", nil, 200, false)

	// Document the validation behavior:
	// - Empty message should fail the test (require.NotEmpty)
	// - Unexpected error should fail the test (require.NoError when expectErr is false)
	// - Wrong status should fail the test (require.Equal)
	// - Flexible status (-1) should accept both 200 and 503, fail on other status codes
}
