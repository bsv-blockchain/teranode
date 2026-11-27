//go:build failpoints

package utxo

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/teranode/stores/utxo/meta"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/failpoint"
	"github.com/stretchr/testify/require"
)

// spendSeamHelperEnv selects the SequentialSpendAndCreate call mode the
// re-executed test binary runs with the spend-before-create seam armed.
const spendSeamHelperEnv = "UTXO_SPEND_SEAM_TEST_HELPER_MODE"

// Markers the helper's store writes to stderr, so the parent test can see which
// store calls ran before the seam exited the process.
const (
	spendCalledMarker  = "SPEND CALLED"
	createCalledMarker = "CREATE CALLED"
)

// tracingSequentialStore reports each Spend and Create call on stderr.
type tracingSequentialStore struct {
	fakeSequentialStore
}

func (s *tracingSequentialStore) Spend(ctx context.Context, tx *bt.Tx, blockHeight uint32, ignoreFlags ...IgnoreFlags) ([]*Spend, error) {
	_, _ = fmt.Fprintln(os.Stderr, spendCalledMarker)

	return s.fakeSequentialStore.Spend(ctx, tx, blockHeight, ignoreFlags...)
}

func (s *tracingSequentialStore) Create(ctx context.Context, tx *bt.Tx, blockHeight uint32, opts ...CreateOption) (*meta.Data, error) {
	_, _ = fmt.Fprintln(os.Stderr, createCalledMarker)

	return s.fakeSequentialStore.Create(ctx, tx, blockHeight, opts...)
}

// TestSpendSeamHelperProcess is run as a subprocess by the tests below. It calls
// SequentialSpendAndCreate in the mode named by spendSeamHelperEnv and exits 0
// if the process survives.
func TestSpendSeamHelperProcess(t *testing.T) {
	mode := os.Getenv(spendSeamHelperEnv)
	if mode == "" {
		t.Skip("helper process only")
	}

	var opts []CreateOption

	switch mode {
	case "spend-and-create":
	case "create-only":
		opts = append(opts, WithCreateOnly())
	case "spend-only":
		opts = append(opts, WithSpendOnly())
	default:
		t.Fatalf("unknown mode %q", mode)
	}

	f := &tracingSequentialStore{fakeSequentialStore{spends: []*Spend{{}}, createMeta: &meta.Data{}}}

	_, _, err := SequentialSpendAndCreate(context.Background(), ulogger.TestLogger{}, f, bt.NewTx(), 100, opts...)
	require.NoError(t, err)

	os.Exit(0)
}

func runSpendSeamHelper(t *testing.T, mode string) (int, string) {
	t.Helper()

	cmd := exec.Command(os.Args[0], "-test.run=^TestSpendSeamHelperProcess$") //nolint:gosec // re-executes this test binary
	cmd.Env = append(os.Environ(), failpoint.EnvVar+"="+failpoint.UTXOSpendBeforeCreate, spendSeamHelperEnv+"="+mode)

	out, err := cmd.CombinedOutput()
	require.NotNil(t, cmd.ProcessState, "helper did not run: %v", err)

	return cmd.ProcessState.ExitCode(), string(out)
}

func TestSpendBeforeCreateSeamFiresAfterSpend(t *testing.T) {
	code, out := runSpendSeamHelper(t, "spend-and-create")

	require.Equal(t, 1, code, out)
	require.Contains(t, out, failpoint.HitMarker+failpoint.UTXOSpendBeforeCreate)
	require.Contains(t, out, spendCalledMarker)
	require.NotContains(t, out, createCalledMarker, "the seam must fire before the child record is created")
	require.Less(t, strings.Index(out, spendCalledMarker), strings.Index(out, failpoint.HitMarker), out)
}

func TestSpendBeforeCreateSeamSkipsCreateOnly(t *testing.T) {
	code, out := runSpendSeamHelper(t, "create-only")

	require.Equal(t, 0, code, out)
	require.NotContains(t, out, failpoint.HitMarker)
}

func TestSpendBeforeCreateSeamSkipsSpendOnly(t *testing.T) {
	code, out := runSpendSeamHelper(t, "spend-only")

	require.Equal(t, 0, code, out)
	require.NotContains(t, out, failpoint.HitMarker)
}
