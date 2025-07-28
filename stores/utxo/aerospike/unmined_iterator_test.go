package aerospike

import (
	"testing"

	"github.com/bitcoin-sv/teranode/stores/utxo/fields"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func Test_extractLocked(t *testing.T) {
	it := &unminedTxIterator{}

	locked, err := it.extractLocked(map[string]interface{}{
		fields.Locked.String(): true,
	})
	require.NoError(t, err)
	assert.True(t, locked)

	locked, err = it.extractLocked(map[string]interface{}{
		fields.Locked.String(): false,
	})
	require.NoError(t, err)
	assert.False(t, locked)

	// missing field should default to false
	locked, err = it.extractLocked(map[string]interface{}{})
	require.NoError(t, err)
	assert.False(t, locked)
}
