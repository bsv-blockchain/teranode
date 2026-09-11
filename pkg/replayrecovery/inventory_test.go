package replayrecovery

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

type censusFixture struct {
	records []InventoryRecord
	refs    []SpendReference
}

func (c censusFixture) Inventory(ctx context.Context, visit func(InventoryRecord) error) error {
	for _, r := range c.records {
		if err := visit(r); err != nil {
			return err
		}
	}
	return ctx.Err()
}
func (c censusFixture) SpendReferences(ctx context.Context, visit func(SpendReference) error) error {
	for _, r := range c.refs {
		if err := visit(r); err != nil {
			return err
		}
	}
	return ctx.Err()
}
func allowRecovery(context.Context) error { return nil }

func TestInventoryIncludesMinedParentMissingChildAndDirtyFlags(t *testing.T) {
	parent, child, dirty := strings.Repeat("1", 64), strings.Repeat("2", 64), strings.Repeat("3", 64)
	c := censusFixture{
		records: []InventoryRecord{
			{Record: Record{Key: []byte("parent"), Data: []byte("spent")}, TxID: parent, Master: true},
			{Record: Record{Key: []byte("dirty"), Data: []byte("locked")}, TxID: dirty, Master: true, Candidate: true, Reason: "locked record"},
		},
		refs: []SpendReference{{ParentKey: []byte("parent"), ParentTxID: parent, ChildTxID: child}},
	}
	dir := t.TempDir()
	require.NoError(t, os.Chmod(dir, 0700))
	path := filepath.Join(dir, "manifest.sqlite")
	summary, err := Inventory(t.Context(), c, path, allowRecovery)
	require.NoError(t, err)
	require.Equal(t, int64(2), summary.Scanned)
	db, err := openPrivateDB(path, false, true)
	require.NoError(t, err)
	defer db.Close()
	var count int
	require.NoError(t, db.db.QueryRow("SELECT COUNT(*) FROM entries WHERE id IN (?,?)", child, dirty).Scan(&count))
	require.Equal(t, 2, count)
	require.NoError(t, db.db.QueryRow("SELECT COUNT(*) FROM entries WHERE id=?", parent).Scan(&count))
	require.Zero(t, count, "normally mined parent must not become a delete candidate")
	var data []byte
	require.NoError(t, db.db.QueryRow("SELECT record FROM inventory WHERE key=?", []byte("dirty")).Scan(&data))
	var rec Record
	require.NoError(t, json.Unmarshal(data, &rec))
	require.Equal(t, []byte("locked"), rec.Data)
}

func TestInventoryRecordsMissingPagesAndRefusesAbsentGuard(t *testing.T) {
	id := strings.Repeat("4", 64)
	c := censusFixture{records: []InventoryRecord{{Record: Record{Key: []byte("master")}, TxID: id, Master: true, ExpectedPages: 1}}}
	dir := t.TempDir()
	require.NoError(t, os.Chmod(dir, 0700))
	_, err := Inventory(t.Context(), c, filepath.Join(dir, "unguarded"), nil)
	require.Error(t, err)
	path := filepath.Join(dir, "manifest.sqlite")
	_, err = Inventory(t.Context(), c, path, allowRecovery)
	require.NoError(t, err)
	db, err := openPrivateDB(path, false, true)
	require.NoError(t, err)
	defer db.Close()
	var count int
	require.NoError(t, db.db.QueryRow("SELECT COUNT(*) FROM findings").Scan(&count))
	require.Positive(t, count)
}

// A failed census must never become an authority for a later repair invocation.
func requireUnsealedInventory(t *testing.T, path string) {
	t.Helper()
	db, err := openPrivateDB(path, false, true)
	require.NoError(t, err)
	var count int
	require.NoError(t, db.db.QueryRow("SELECT COUNT(*) FROM inventory_state").Scan(&count))
	require.Zero(t, count)
	require.NoError(t, db.db.QueryRow("SELECT COUNT(*) FROM manifest").Scan(&count))
	require.Zero(t, count)
	require.NoError(t, db.Close())
	backend := &recordBackend{records: map[string]Record{}}
	source := &evidenceSource{tip: Tip{Hash: strings.Repeat("a", 64), Height: 1}}
	_, err = Discover(t.Context(), backend, source, path, allowRecovery, nil)
	require.ErrorContains(t, err, "inventory incomplete")
	_, err = Apply(t.Context(), backend, source, path, filepath.Join(filepath.Dir(path), "journal.sqlite"), ApplyOptions{Guard: allowRecovery, Maintenance: true, Tip: source.tip})
	require.Error(t, err)
	require.Zero(t, backend.writes)
}

func TestInventoryRejectsMalformedRecordsAndReferencesWithoutSealing(t *testing.T) {
	parent, child := strings.Repeat("1", 64), strings.Repeat("2", 64)
	master := InventoryRecord{Record: Record{Key: []byte("parent")}, TxID: parent, Master: true}
	ref := SpendReference{ParentKey: master.Record.Key, ParentTxID: parent, ChildTxID: child}
	cases := []struct {
		name    string
		records []InventoryRecord
		refs    []SpendReference
		message string
	}{
		{"missing-key", []InventoryRecord{{TxID: parent, Master: true}}, nil, "no stable key"},
		{"invalid-owner", []InventoryRecord{{Record: master.Record, TxID: "invalid", Master: true}}, nil, "invalid inventory transaction identity"},
		{"duplicate-key", []InventoryRecord{master, master}, nil, "UNIQUE constraint failed"},
		{"invalid-parent-reference", []InventoryRecord{master}, []SpendReference{{ParentKey: ref.ParentKey, ParentTxID: "invalid", ChildTxID: child}}, "invalid spend reference identity"},
		{"invalid-child-reference", []InventoryRecord{master}, []SpendReference{{ParentKey: ref.ParentKey, ParentTxID: parent, ChildTxID: "invalid"}}, "invalid spend reference identity"},
		{"missing-reference-owner", []InventoryRecord{master}, []SpendReference{{ParentKey: []byte("absent"), ParentTxID: parent, ChildTxID: child}}, "no rows"},
		{"changed-reference-owner", []InventoryRecord{master}, []SpendReference{{ParentKey: ref.ParentKey, ParentTxID: child, ChildTxID: parent}}, "ownership changed"},
		{"duplicate-spend-owner", []InventoryRecord{master}, []SpendReference{ref, ref}, "UNIQUE constraint failed"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			require.NoError(t, os.Chmod(dir, 0700))
			path := filepath.Join(dir, "manifest.sqlite")
			summary, err := Inventory(t.Context(), censusFixture{records: tc.records, refs: tc.refs}, path, allowRecovery)
			require.ErrorContains(t, err, tc.message)
			require.False(t, summary.Complete)
			requireUnsealedInventory(t, path)
		})
	}
}

func TestInventoryGuardInterruptionNeverSealsPartialScan(t *testing.T) {
	parent, child := strings.Repeat("1", 64), strings.Repeat("2", 64)
	fixture := censusFixture{
		records: []InventoryRecord{{Record: Record{Key: []byte("parent")}, TxID: parent, Master: true}},
		refs:    []SpendReference{{ParentKey: []byte("parent"), ParentTxID: parent, ChildTxID: child}},
	}
	for _, phase := range []struct {
		name string
		stop int
	}{{"record", 2}, {"reference", 3}, {"completion", 4}} {
		t.Run(phase.name, func(t *testing.T) {
			dir := t.TempDir()
			require.NoError(t, os.Chmod(dir, 0700))
			path := filepath.Join(dir, "manifest.sqlite")
			calls := 0
			guard := func(context.Context) error {
				calls++
				if calls == phase.stop {
					return failure("persisted state left IDLE")
				}
				return nil
			}
			summary, err := Inventory(t.Context(), fixture, path, guard)
			require.ErrorContains(t, err, "persisted state left IDLE")
			require.Equal(t, phase.stop, calls)
			require.False(t, summary.Complete)
			requireUnsealedInventory(t, path)
		})
	}
	t.Run("already-cancelled", func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		path := filepath.Join(t.TempDir(), "manifest.sqlite")
		guardCalled := false
		_, err := Inventory(ctx, fixture, path, func(context.Context) error { guardCalled = true; return nil })
		require.ErrorIs(t, err, context.Canceled)
		require.False(t, guardCalled)
		_, err = os.Stat(path)
		require.True(t, os.IsNotExist(err), "cancelled operation must not create an inventory")
	})
}
