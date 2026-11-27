package teranodecli

import (
	"context"
	"fmt"
	"io"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/settings"
	utxostore "github.com/bsv-blockchain/teranode/stores/utxo"
	utxofactory "github.com/bsv-blockchain/teranode/stores/utxo/factory"
	"github.com/bsv-blockchain/teranode/ulogger"
)

const laneSchemaLongHelp = `Installs the lane columns of the configured UTXO store: nullable, empty columns for the
lane positions that the lane migration later fills. Run it at utxostore_laneMigration=off, before
the historical conversion, with every teranode process of the node stopped, as a database role that
owns the tables. It takes no arguments and is safe to run again. The lane columns it adds are empty:
it fills no existing row. It opens the store as the node does first, and that startup can create the
store's base schema and its metadata rows on a new database. Start the node's processes afterwards so
each sees the same schema. The blockchain store's lane tables are not this command's: every start of
the blockchain service creates them when its database role may CREATE. A role without CREATE needs
the database owner to create the lane tables and the lane_map_lane_last index first, then grant
SELECT and INSERT on the six map and conversion tables (lane_map, lane_floor, lane_gap and the
three lane_conversion tables), SELECT, INSERT and UPDATE on lane_lease, SELECT, INSERT and DELETE on
lane_handout, and SELECT and INSERT on lane_close and lane_settlement, which lease open, renewal,
release and the stale-lease sweep write. A role without CREATE upgrading to a build that adds a lane
table, such as lane_settlement, cannot start until the owner has created it.`

// laneSchemaExecute opens the configured UTXO store and installs its lane columns.
func laneSchemaExecute(logger ulogger.Logger, tSettings *settings.Settings, out io.Writer) func(args []string) error {
	return func(args []string) error {
		if len(args) != 0 {
			return errors.NewInvalidArgumentError("prepare-lane-schema takes no arguments")
		}

		ctx := context.Background()

		store, err := utxofactory.NewStore(ctx, logger, tSettings, "prepare-lane-schema", false)
		if err != nil {
			return errors.NewStorageError("failed to open the UTXO store", err)
		}

		defer func() { _ = store.Close(ctx) }()

		return prepareLaneSchema(ctx, store, out)
	}
}

// prepareLaneSchema installs store's lane columns, or reports that it has none to install.
func prepareLaneSchema(ctx context.Context, store utxostore.Store, out io.Writer) error {
	preparer, ok := utxostore.As[utxostore.LaneSchemaPreparer](store)
	if !ok {
		_, _ = fmt.Fprintln(out, "This UTXO store has no lane schema to install.")
		return nil
	}

	if err := preparer.PrepareLaneSchema(ctx); err != nil {
		return errors.NewStorageError("failed to install the lane schema", err)
	}

	// Where the store can list its lane columns, confirm every one is there before saying so.
	if checker, ok := utxostore.As[utxostore.LaneSchemaChecker](store); ok {
		installed, err := checker.LaneSchemaInstalled(ctx)
		if err != nil {
			return errors.NewStorageError("failed to check the lane schema", err)
		}

		if !installed {
			return errors.NewStorageError("the lane schema install returned without error but a lane column is still missing")
		}
	}

	_, _ = fmt.Fprintln(out, "Lane schema installed. Start every teranode process of the node again so each sees it.")

	return nil
}
