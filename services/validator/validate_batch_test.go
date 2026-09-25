package validator

import (
	"context"
	"net/url"
	"testing"

	"github.com/bsv-blockchain/teranode/stores/utxo/sql"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/bsv-blockchain/teranode/util/tracing"
	"github.com/stretchr/testify/require"
)

// newValidatorForTest creates a minimal Validator suitable for unit tests.
// It uses an in-memory SQLite UTXO store and disables block assembly so
// that no external services need to be running.
func newValidatorForTest(t testing.TB) *Validator {
	t.Helper()
	tracing.SetupMockTracer()

	ctx := context.Background()
	logger := ulogger.TestLogger{}
	tSettings := test.CreateBaseTestSettings(t)
	tSettings.BlockAssembly.Disabled = true

	utxoStoreURL, err := url.Parse("sqlitememory:///test")
	require.NoError(t, err)

	utxoStore, err := sql.New(ctx, logger, tSettings, utxoStoreURL)
	require.NoError(t, err)

	iface, err := New(ctx, logger, tSettings, utxoStore, nil, nil, nil, nil, nil)
	require.NoError(t, err)

	return iface.(*Validator)
}

func TestValidateBatch_Empty(t *testing.T) {
	v := newValidatorForTest(t)
	results, err := v.ValidateBatch(context.Background(), nil, 0)
	require.NoError(t, err)
	require.Len(t, results, 0)
}
