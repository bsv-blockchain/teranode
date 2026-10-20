package fault_test

import (
	"os"
	"testing"

	"github.com/bsv-blockchain/teranode/util/test/fault/internal/faultgen"
	"github.com/stretchr/testify/require"
)

// TestGeneratedDecoratorsAreCurrent fails when a decorated interface changed without the
// decorators being regenerated. The build already fails when a method is added; this also
// catches a changed signature or a removed method, which would otherwise leave a stale method
// on the decorator.
func TestGeneratedDecoratorsAreCurrent(t *testing.T) {
	files, err := faultgen.Generate(".")
	require.NoError(t, err)

	for name, want := range files {
		got, err := os.ReadFile(name)
		require.NoError(t, err, "%s is missing; run go generate ./util/test/fault", name)
		require.Equal(t, string(want), string(got), "%s is stale; run go generate ./util/test/fault", name)
	}
}
