package aerospike

import (
	"testing"

	aerospike "github.com/bsv-blockchain/aerospike-client-go/v8"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/stretchr/testify/require"
)

// The parent-depth guard asks for BlockHeights on every parent read. A record from
// an older node version or a snapshot restore may lack the bin, and that must read
// as "no heights known" — the guard then fails closed on depth — not fail the whole
// read with ERR_STORAGE, which would wedge block validation on the parent.
func TestProcessBlockHeights_MissingBinReadsAsNoHeights(t *testing.T) {
	heights, err := processBlockHeights(aerospike.BinMap{})

	require.NoError(t, err)
	require.Empty(t, heights)
}

func TestProcessBlockHeights_ReadsStoredHeights(t *testing.T) {
	heights, err := processBlockHeights(aerospike.BinMap{
		fields.BlockHeights.String(): []interface{}{900, 905},
	})

	require.NoError(t, err)
	require.Equal(t, []uint32{900, 905}, heights)
}

func TestProcessBlockHeights_MalformedHeightStillFails(t *testing.T) {
	heights, err := processBlockHeights(aerospike.BinMap{
		fields.BlockHeights.String(): []interface{}{"not-a-height"},
	})

	require.Error(t, err)
	require.Nil(t, heights)
}
