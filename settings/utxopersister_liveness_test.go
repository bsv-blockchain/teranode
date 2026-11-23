package settings

import (
	"testing"
	"time"

	"github.com/ordishs/gocore"
	"github.com/stretchr/testify/require"
)

// TestUTXOPersisterLivenessStallTimeout_LoaderReadsKey guards
// utxoPersister_livenessStallTimeout against a struct tag with no loader line in
// NewSettings. The default is 0, the Go zero value, so only a non-zero override
// proves the loader reads the key.
func TestUTXOPersisterLivenessStallTimeout_LoaderReadsKey(t *testing.T) {
	const key = "utxoPersister_livenessStallTimeout"

	require.Zero(t, NewSettings().Block.UTXOPersisterLivenessStallTimeout,
		"the check must ship off so merging it changes no deployment's behaviour")

	gocore.Config().Set(key, "7m")
	t.Cleanup(func() { gocore.Config().Set(key, "") })

	require.Equal(t, 7*time.Minute, NewSettings().Block.UTXOPersisterLivenessStallTimeout,
		"the loader must read %s, or the setting is dead and the check cannot be enabled", key)
}
