package replayrecovery

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// These faults model outcomes at the remote mutation boundary. Each assertion
// checks which records survive, not merely that a backend error was forwarded.
type mutationFaultBackend struct {
	*recordBackend
	ambiguousDelete  bool
	ignoredDelete    bool
	wrongMarkerReply bool
}

func (b *mutationFaultBackend) Delete(ctx context.Context, r Record) error {
	if b.ignoredDelete {
		return nil
	}
	if err := b.recordBackend.Delete(ctx, r); err != nil {
		return err
	}
	if b.ambiguousDelete {
		return failure("delete response lost")
	}
	return nil
}

func (b *mutationFaultBackend) Mark(ctx context.Context, r Record, id string) (Record, error) {
	after, err := b.recordBackend.Mark(ctx, r, id)
	if err == nil && b.wrongMarkerReply {
		after.Generation++
	}
	return after, err
}

func TestApplyLostDeleteResponseResumesWithoutAnotherMutation(t *testing.T) {
	base, source, tx, manifest, journal := recoveryFixture(t)
	backend := &mutationFaultBackend{recordBackend: base, ambiguousDelete: true}
	_, err := discoverForTest(t.Context(), backend, source, manifest, nil)
	require.NoError(t, err)
	_, err = Apply(t.Context(), backend, source, manifest, journal, ApplyOptions{Guard: allowRecovery, Maintenance: true, Tip: source.tip})
	require.ErrorContains(t, err, "delete response lost")
	require.NotContains(t, base.records, tx.TxID())
	require.True(t, base.HasMarker(base.records["parent"], tx.TxID()))
	writes := base.writes
	backend.ambiguousDelete = false
	result, err := Apply(t.Context(), backend, source, manifest, journal, ApplyOptions{Guard: allowRecovery, Maintenance: true, Resume: true, Tip: source.tip})
	require.NoError(t, err)
	require.EqualValues(t, 1, result.Repaired)
	require.Equal(t, writes, base.writes, "recorded intent plus authoritative absence must not repeat deletion")
}

func TestApplyRejectsFalseMutationAcknowledgements(t *testing.T) {
	for _, mode := range []string{"ignored delete", "wrong marker generation"} {
		t.Run(mode, func(t *testing.T) {
			base, source, tx, manifest, journal := recoveryFixture(t)
			backend := &mutationFaultBackend{recordBackend: base, ignoredDelete: mode == "ignored delete", wrongMarkerReply: mode == "wrong marker generation"}
			_, err := discoverForTest(t.Context(), backend, source, manifest, nil)
			require.NoError(t, err)
			_, err = Apply(t.Context(), backend, source, manifest, journal, ApplyOptions{Guard: allowRecovery, Maintenance: true, Tip: source.tip})
			require.Error(t, err)
			require.Contains(t, base.records, tx.TxID(), "a successful write response is insufficient without matching readback")
			backend.ignoredDelete = false
			backend.wrongMarkerReply = false
			_, err = Apply(t.Context(), backend, source, manifest, journal, ApplyOptions{Guard: allowRecovery, Maintenance: true, Resume: true, Tip: source.tip})
			require.NoError(t, err)
			require.NotContains(t, base.records, tx.TxID())
		})
	}
}

func TestResumeRefusesUnrelatedParentChange(t *testing.T) {
	base, source, tx, manifest, journal := recoveryFixture(t)
	_, err := discoverForTest(t.Context(), base, source, manifest, nil)
	require.NoError(t, err)
	base.failAfterMark = true
	_, err = Apply(t.Context(), base, source, manifest, journal, ApplyOptions{Guard: allowRecovery, Maintenance: true, Tip: source.tip})
	require.Error(t, err)
	r := base.records["parent"]
	r.Generation++
	base.records["parent"] = r
	base.failAfterMark = false
	writes := base.writes
	_, err = Apply(t.Context(), base, source, manifest, journal, ApplyOptions{Guard: allowRecovery, Maintenance: true, Resume: true, Tip: source.tip})
	require.ErrorContains(t, err, "store record changed outside the recovery journal")
	require.Equal(t, writes, base.writes)
	require.Contains(t, base.records, tx.TxID(), "an existing marker cannot authorize a divergent parent")
}

func TestResumeRejectsRecreatedPageBeforeDeletingMaster(t *testing.T) {
	base, source, tx, manifest, journal := recoveryFixture(t)
	key := tx.TxID() + ":page"
	page := Record{Key: []byte(key), Data: []byte("page"), Generation: 3}
	base.records[key] = Record{Key: []byte(key), Data: []byte("page"), Generation: 3}
	_, err := discoverForTest(t.Context(), base, source, manifest, nil)
	require.NoError(t, err)
	base.failDeleteKey = tx.TxID()
	_, err = Apply(t.Context(), base, source, manifest, journal, ApplyOptions{Guard: allowRecovery, Maintenance: true, Tip: source.tip})
	require.Error(t, err)
	base.records[key] = page
	base.failDeleteKey = ""
	writes := base.writes
	_, err = Apply(t.Context(), base, source, manifest, journal, ApplyOptions{Guard: allowRecovery, Maintenance: true, Resume: true, Tip: source.tip})
	require.ErrorContains(t, err, "store record changed outside the recovery journal")
	require.Equal(t, writes, base.writes)
	require.Contains(t, base.records, key)
}

func TestApplyRevalidatesDiscoveryCoverageAndCanonicalEvidence(t *testing.T) {
	for _, mode := range []string{"new unmined record", "changed dependency", "live output", "changed tip"} {
		t.Run(mode, func(t *testing.T) {
			base, source, tx, manifest, journal := recoveryFixture(t)
			_, err := discoverForTest(t.Context(), base, source, manifest, nil)
			require.NoError(t, err)
			tip := source.tip
			switch mode {
			case "new unmined record":
				id := strings.Repeat("9", 64)
				base.seeds = append(base.seeds, id)
				base.records[id] = Record{Key: []byte(id), Data: []byte("new unmined record"), Generation: 1}
			case "changed dependency":
				record := base.records[tx.TxID()]
				record.Data = []byte("changed persisted input")
				record.Generation++
				base.records[tx.TxID()] = record
			case "live output":
				ev := source.evidence[tx.TxID()]
				ev.Classification = Live
				source.evidence[tx.TxID()] = ev
			case "changed tip":
				source.tip.Hash = strings.Repeat("9", 64)
			}
			_, err = Apply(t.Context(), base, source, manifest, journal, ApplyOptions{Guard: allowRecovery, Maintenance: true, Tip: tip})
			require.Error(t, err)
			require.Zero(t, base.writes)
			require.Contains(t, base.records, tx.TxID())
		})
	}
}
