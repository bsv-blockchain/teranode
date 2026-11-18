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
owns the tables. It writes no row and is safe to run again. Start the node's processes afterwards so
each sees the same schema. The blockchain store's lane tables need no command: every start of the
blockchain service creates them.`

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

	_, _ = fmt.Fprintln(out, "Lane schema installed. Start every teranode process of the node again so each sees it.")

	return nil
}
