package factory

import (
	"context"
	"net/url"

	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/utxoset"
	"github.com/bsv-blockchain/teranode/ulogger"
)

// The utxoset store carries its own URL scheme, and that is TRANSITIONAL.
//
// It is intended to REPLACE the append-only store behind "postgres" outright, not to sit
// alongside it: one indexes the spent set in a `spends` table that only grows, the other
// indexes the live unspent set and deletes on spend, and there is no configuration in
// which running both makes sense. The separate scheme exists so the new store can be
// deployed and measured against a node still running the old one, without ripping the old
// one out mid-flight. Once it is proven, "postgres" repoints here and stores/utxo/postgres
// goes.
//
// It is deliberately a scheme rather than a query parameter on "postgres", because the two
// schemas are incompatible and a typo in a parameter that silently selected the wrong one
// against a live database is not a failure mode worth having.
func init() {
	availableDatabases["utxoset"] = func(ctx context.Context, logger ulogger.Logger, tSettings *settings.Settings, storeURL *url.URL) (utxo.Store, error) {
		return utxoset.New(ctx, logger, tSettings, storeURL)
	}
}
