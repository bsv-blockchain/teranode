package teranodecli

import (
	"bytes"
	"context"
	"net/url"
	"testing"

	"github.com/bsv-blockchain/teranode/stores/utxo/nullstore"
	utxosql "github.com/bsv-blockchain/teranode/stores/utxo/sql"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/require"
)

func TestPrepareLaneSchemaOnASQLStore(t *testing.T) {
	u, err := url.Parse("sqlitememory:///lane_schema_cli")
	require.NoError(t, err)

	store, err := utxosql.New(context.Background(), ulogger.TestLogger{}, test.CreateBaseTestSettings(t), u)
	require.NoError(t, err)

	var out bytes.Buffer
	require.NoError(t, prepareLaneSchema(context.Background(), store, &out))
	require.Contains(t, out.String(), "Lane schema installed")
}

func TestPrepareLaneSchemaOnAStoreWithoutOne(t *testing.T) {
	store, err := nullstore.NewNullStore()
	require.NoError(t, err)

	var out bytes.Buffer
	require.NoError(t, prepareLaneSchema(context.Background(), store, &out))
	require.Contains(t, out.String(), "no lane schema")
}

func TestPrepareLaneSchemaRefusesArguments(t *testing.T) {
	require.Error(t, laneSchemaExecute(ulogger.TestLogger{}, test.CreateBaseTestSettings(t), &bytes.Buffer{})([]string{"x"}))
}
