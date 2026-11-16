package settings

import (
	"reflect"
	"testing"

	"github.com/bsv-blockchain/teranode/util/bytesize"
	"github.com/stretchr/testify/require"
)

// TestUTXOPersisterBufferSizeDefault asserts the runtime default and the documented default
// separately. The struct tag is what the settings exporter publishes, so a tag that drifts
// from the value the code actually uses ships documentation that contradicts the node.
func TestUTXOPersisterBufferSizeDefault(t *testing.T) {
	t.Run("runtime value", func(t *testing.T) {
		parsed, err := bytesize.Parse(NewSettings().Block.UTXOPersisterBufferSize)
		require.NoError(t, err)
		require.Equal(t, 256*1024, parsed.Int())
	})

	t.Run("documented default", func(t *testing.T) {
		field, ok := reflect.TypeOf(BlockSettings{}).FieldByName("UTXOPersisterBufferSize")
		require.True(t, ok)
		require.Equal(t, "256KB", field.Tag.Get("default"))
	})
}
