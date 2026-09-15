package replayrecovery

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestVerifyRejectsPostRepairCorruption(t *testing.T) {
	for _, mode := range []string{"recreated child", "missing parent", "lost marker", "live output"} {
		t.Run(mode, func(t *testing.T) {
			base, source, tx, manifest, journal := recoveryFixture(t)
			before := base.records[tx.TxID()]
			_, err := discoverForTest(t.Context(), base, source, manifest, nil)
			require.NoError(t, err)
			_, err = Apply(t.Context(), base, source, manifest, journal, ApplyOptions{Guard: allowRecovery, Maintenance: true, Tip: source.tip})
			require.NoError(t, err)
			switch mode {
			case "recreated child":
				base.records[tx.TxID()] = before
			case "missing parent":
				delete(base.records, "parent")
			case "lost marker":
				r := base.records["parent"]
				r.Data = []byte("spent:" + tx.TxID())
				base.records["parent"] = r
			case "live output":
				ev := source.evidence[tx.TxID()]
				ev.Classification = Live
				source.evidence[tx.TxID()] = ev
			}
			writes := base.writes
			result, err := Verify(t.Context(), base, source, manifest, journal, allowRecovery)
			require.Error(t, err)
			require.NotEqual(t, "verified", result.Stage)
			require.Equal(t, writes, base.writes, "verification never repairs corruption implicitly")
		})
	}
}
