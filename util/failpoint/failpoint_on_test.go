//go:build failpoints

package failpoint

import (
	"os"
	"os/exec"
	"testing"

	"github.com/stretchr/testify/require"
)

// helperEnv selects the failpoint the re-executed test binary injects.
const helperEnv = "FAILPOINT_TEST_HELPER_INJECT"

// TestHelperProcess is run as a subprocess by the tests below. It injects the
// failpoint named in helperEnv and exits 0 if the process survives.
func TestHelperProcess(t *testing.T) {
	name := os.Getenv(helperEnv)
	if name == "" {
		t.Skip("helper process only")
	}

	Inject(name)
	os.Exit(0)
}

func runHelper(t *testing.T, armedEnv, inject string) (int, string) {
	t.Helper()

	cmd := exec.Command(os.Args[0], "-test.run=^TestHelperProcess$") //nolint:gosec // re-executes this test binary
	cmd.Env = append(os.Environ(), EnvVar+"="+armedEnv, helperEnv+"="+inject)

	out, err := cmd.CombinedOutput()
	require.NotNil(t, cmd.ProcessState, "helper did not run: %v", err)

	return cmd.ProcessState.ExitCode(), string(out)
}

func TestInjectArmedExits(t *testing.T) {
	code, out := runHelper(t, "other, "+UTXOSpendBeforeCreate, UTXOSpendBeforeCreate)

	require.Equal(t, 1, code)
	require.Contains(t, out, HitMarker+UTXOSpendBeforeCreate)
}

func TestInjectUnarmedReturns(t *testing.T) {
	code, out := runHelper(t, BlockAssemblyPersistBeforeAddBlock, UTXOSpendBeforeCreate)

	require.Equal(t, 0, code, out)
	require.NotContains(t, out, HitMarker)
}

func TestEnableDisable(t *testing.T) {
	const name = "test/enable-disable"

	Enable(name)
	mu.RLock()
	_, ok := armed[name]
	mu.RUnlock()
	require.True(t, ok)

	Disable(name)
	Inject(name) // must not exit
}
