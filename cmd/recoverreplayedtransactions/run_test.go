package recoverreplayedtransactions

import (
	"context"
	"io"
	"testing"
	"time"

	"github.com/bsv-blockchain/teranode/pkg/replayrecovery"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/stretchr/testify/require"
)

func TestRunRejectsUnsafeOptionsBeforeConnecting(t *testing.T) {
	for _, resume := range []bool{false, true} {
		err := Run(context.Background(), ulogger.TestLogger{}, &settings.Settings{}, Options{WorkDir: t.TempDir(), Apply: true, Resume: resume, Timeout: time.Minute, Concurrency: 1}, io.Discard, io.Discard)
		require.ErrorContains(t, err, "maintenance")
	}
}
func TestExitCodesRemainDistinctWhenWrapped(t *testing.T) {
	require.Equal(t, 0, ExitCode(nil))
	require.Equal(t, 1, ExitCode(commandError("ordinary failure")))
	require.Equal(t, 2, ExitCode(commandError("audit: %w", replayrecovery.ErrIncomplete)))
}
func TestOptionsRequireExplicitApplyAndResume(t *testing.T) {
	base := Options{WorkDir: t.TempDir(), Timeout: time.Minute, Concurrency: 1}
	require.NoError(t, base.validate())
	base.Apply = true
	require.Error(t, base.validate())
	base.Maintenance = true
	require.NoError(t, base.validate())
	base.Apply = false
	base.Resume = true
	require.Error(t, base.validate())
	base.Resume = false
	base.WorkDir = ""
	require.Error(t, base.validate())
}

func TestVerificationFailurePreservesCompletedMutationReport(t *testing.T) {
	applied := replayrecovery.Summary{Applied: true, RestartRequired: true, Repaired: 2, Stage: "applied-verification-pending"}
	failed := replayrecovery.Summary{}
	report := retainMutationStatus(failed, applied)
	require.True(t, report.Applied)
	require.True(t, report.RestartRequired)
	require.EqualValues(t, 2, report.Repaired)
	require.False(t, report.Complete)
}
