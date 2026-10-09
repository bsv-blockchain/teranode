package settings

import (
	"testing"
	"time"

	"github.com/ordishs/gocore"
	"github.com/stretchr/testify/require"
)

// TestSpendRollbackTimeout_LoaderReadsKey guards utxostore_spendRollbackTimeout
// against the field-exists-but-loader-never-reads-it bug.
//
// The field shipped with a `key:` tag, a documented 120s default and a longdesc
// telling operators to raise it when utxo_spend_rollback_failed shows truncation —
// but NewSettings() never called getDuration for it, so the field stayed at the Go
// zero value, both call sites took their `<= 0` fallback, and the knob did nothing.
// The struct tag is read by reflection only for export metadata, not to load values,
// so a tag alone proves nothing.
//
// A default-value assertion alone would not catch a regression here either, because
// the fallback happens to equal the documented default. The honest test sets a
// distinctive value and asserts NewSettings() reads it back.
func TestSpendRollbackTimeout_LoaderReadsKey(t *testing.T) {
	const key = "utxostore_spendRollbackTimeout"

	require.Equal(t, 120*time.Second, NewSettings().UtxoStore.SpendRollbackTimeout,
		"documented default must come from the loader, not from a call-site fallback")

	gocore.Config().Set(key, "300s")
	t.Cleanup(func() { gocore.Config().Set(key, "") })

	require.Equal(t, 300*time.Second, NewSettings().UtxoStore.SpendRollbackTimeout,
		"loader must read %s; an operator raising it must actually change the rollback budget", key)
}
