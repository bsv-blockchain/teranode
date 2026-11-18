package teranodecli

import (
	"bytes"
	"context"
	"net/url"
	"os"
	"os/exec"
	"testing"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo/nullstore"
	utxosql "github.com/bsv-blockchain/teranode/stores/utxo/sql"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/require"
)

// The command installs what is missing: a lane column dropped from a SQL store comes back.
func TestPrepareLaneSchemaOnASQLStore(t *testing.T) {
	u, err := url.Parse("sqlitememory:///lane_schema_cli")
	require.NoError(t, err)

	store, err := utxosql.New(context.Background(), ulogger.TestLogger{}, test.CreateBaseTestSettings(t), u)
	require.NoError(t, err)

	_, err = store.RawDB().Exec(`ALTER TABLE outputs DROP COLUMN spending_lane_position`)
	require.NoError(t, err)
	require.Zero(t, outputsColumnCount(t, store, "spending_lane_position"))

	var out bytes.Buffer
	require.NoError(t, prepareLaneSchema(context.Background(), store, &out))
	require.Contains(t, out.String(), "Lane schema installed")
	require.Equal(t, 1, outputsColumnCount(t, store, "spending_lane_position"))
}

func outputsColumnCount(t *testing.T, store *utxosql.Store, column string) int {
	t.Helper()

	var n int
	require.NoError(t, store.RawDB().QueryRow(`SELECT count(*) FROM pragma_table_info('outputs') WHERE name = $1`, column).Scan(&n))

	return n
}

// failingPreparer is a real SQL store whose lane schema install fails, as it does for a role that
// does not own the tables.
type failingPreparer struct {
	*utxosql.Store
}

func (failingPreparer) PrepareLaneSchema(context.Context) error {
	return errors.NewStorageError("permission denied for table outputs")
}

func TestPrepareLaneSchemaReportsAFailedInstall(t *testing.T) {
	u, err := url.Parse("sqlitememory:///lane_schema_cli_fail")
	require.NoError(t, err)

	store, err := utxosql.New(context.Background(), ulogger.TestLogger{}, test.CreateBaseTestSettings(t), u)
	require.NoError(t, err)

	var out bytes.Buffer
	err = prepareLaneSchema(context.Background(), failingPreparer{store}, &out)
	require.Error(t, err)
	require.Contains(t, err.Error(), "permission denied")
	require.NotContains(t, out.String(), "installed")
}

// teranode-cli dispatches prepare-lane-schema to this command: its --help prints the command's
// long help and exits 0, where an unknown command exits 1. Start exits the process, so it runs in
// a child copy of this test binary.
func TestPrepareLaneSchemaIsDispatched(t *testing.T) {
	if os.Getenv("TERANODE_CLI_DISPATCH_CHILD") == "1" {
		Start([]string{"prepare-lane-schema", "--help"}, "test", "test")
		return
	}

	cmd := exec.Command(os.Args[0], "-test.run=^TestPrepareLaneSchemaIsDispatched$") //nolint:gosec // re-runs this test binary
	cmd.Env = append(os.Environ(), "TERANODE_CLI_DISPATCH_CHILD=1")

	out, err := cmd.CombinedOutput()
	require.NoError(t, err, string(out))
	require.Contains(t, string(out), "Usage of prepare-lane-schema")
	require.Contains(t, string(out), "Installs the lane columns")
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
