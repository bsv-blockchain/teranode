package replayrecovery

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/stretchr/testify/require"
)

func TestApplyBoundaryResumesAfterPageDeletion(t *testing.T) {
	backend, source, tx, manifestPath, journalPath := recoveryFixture(t)
	pageKey := tx.TxID() + ":page"
	backend.records[pageKey] = Record{Key: []byte(pageKey), Data: []byte("page before image"), Generation: 4}
	_, err := discoverForTest(t.Context(), backend, source, manifestPath, nil)
	require.NoError(t, err)
	backend.failDeleteKey = tx.TxID()
	_, err = Apply(t.Context(), backend, source, manifestPath, journalPath, ApplyOptions{Guard: allowRecovery, Maintenance: true, Tip: source.tip})
	require.ErrorContains(t, err, "page deletion failed")
	require.Contains(t, backend.records, tx.TxID(), "the master retains page ownership until deletion completes")
	require.NotContains(t, backend.records, pageKey)
	require.True(t, backend.HasMarker(backend.records["parent"], tx.TxID()))
	writes := backend.writes
	parent := backend.records["parent"]
	backend.failDeleteKey = ""
	result, err := Apply(t.Context(), backend, source, manifestPath, journalPath, ApplyOptions{Guard: allowRecovery, Maintenance: true, Resume: true, Tip: source.tip})
	require.NoError(t, err)
	require.EqualValues(t, 1, result.Repaired)
	require.NotContains(t, backend.records, pageKey)
	require.Equal(t, writes+1, backend.writes, "resume deletes only the remaining master")
	require.True(t, backend.Equal(parent, backend.records["parent"]), "resume preserves the already verified marker generation")
}

func TestApplyBoundaryUnknownDescendantBlocksParent(t *testing.T) {
	backend, source, tx, manifestPath, journalPath := recoveryFixture(t)
	descendant := bt.NewTx()
	require.NoError(t, descendant.From(tx.TxID(), 0, tx.Outputs[0].LockingScript.String(), tx.Outputs[0].Satoshis))
	require.NoError(t, descendant.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 250))
	id := descendant.TxID()
	backend.txs[id] = descendant
	backend.records[id] = Record{Key: []byte(id), Data: []byte("unresolved descendant"), Generation: 3}
	backend.seeds = append(backend.seeds, id)
	summary, err := discoverForTest(t.Context(), backend, source, manifestPath, nil)
	require.ErrorIs(t, err, ErrIncomplete)
	require.EqualValues(t, 1, summary.Unknown)
	require.EqualValues(t, 1, summary.Blocked)
	result, err := Apply(t.Context(), backend, source, manifestPath, journalPath, ApplyOptions{Guard: allowRecovery, Maintenance: true, Tip: source.tip})
	require.ErrorIs(t, err, ErrIncomplete)
	require.Zero(t, result.Repaired)
	require.Zero(t, backend.writes, "unresolved descendants must prevent all writes to their component")
	require.Contains(t, backend.records, tx.TxID())
	require.Contains(t, backend.records, id)
	require.Equal(t, "spent:"+tx.TxID(), string(backend.records["parent"].Data))
}

func TestApplyBoundaryResealedInventoryCannotDeleteUnrelatedRecord(t *testing.T) {
	backend, source, tx, manifestPath, journalPath := recoveryFixture(t)
	victim := Record{Key: []byte("unrelated"), Data: []byte("must survive"), Generation: 9}
	backend.records["unrelated"] = victim
	_, err := discoverForTest(t.Context(), backend, source, manifestPath, nil)
	require.NoError(t, err)
	// Recompute the integrity seal deliberately: a valid checksum cannot turn
	// operator-edited record keys into authority to delete unrelated data.
	db, err := openPrivateDB(manifestPath, false, false)
	require.NoError(t, err)
	m := &manifest{lockedDB: db}
	var header, data []byte
	require.NoError(t, m.db.QueryRow("SELECT header FROM manifest").Scan(&header))
	require.NoError(t, json.Unmarshal(header, &m.header))
	require.NoError(t, m.db.QueryRow("SELECT entry FROM entries WHERE id=?", tx.TxID()).Scan(&data))
	var entry Entry
	require.NoError(t, json.Unmarshal(data, &entry))
	entry.Snapshot.Records[0] = victim
	data, err = json.Marshal(entry)
	require.NoError(t, err)
	_, err = m.db.Exec("UPDATE entries SET entry=? WHERE id=?", data, tx.TxID())
	require.NoError(t, err)
	digest, err := m.checksum()
	require.NoError(t, err)
	_, err = m.db.Exec("UPDATE manifest SET digest=?", digest)
	require.NoError(t, err)
	require.NoError(t, m.Close())
	// Prove rejection comes from semantic authorization, not a stale checksum.
	accepted, err := openManifest(manifestPath)
	require.NoError(t, err)
	require.NoError(t, accepted.Close())
	_, err = Apply(t.Context(), backend, source, manifestPath, journalPath, ApplyOptions{Guard: allowRecovery, Maintenance: true, Tip: source.tip})
	require.ErrorContains(t, err, "inventory does not match")
	require.Zero(t, backend.writes)
	require.True(t, backend.Equal(victim, backend.records["unrelated"]))
	require.Contains(t, backend.records, tx.TxID())
}

func TestApplyBoundaryFiniteParentTTLNeverWrites(t *testing.T) {
	backend, source, tx, manifestPath, journalPath := recoveryFixture(t)
	parent := backend.records["parent"]
	parent.ExpiresAt = time.Now().Add(time.Hour).Unix()
	backend.records["parent"] = parent
	audit, err := discoverForTest(t.Context(), backend, source, manifestPath, nil)
	require.ErrorIs(t, err, ErrIncomplete)
	require.EqualValues(t, 1, audit.Unknown)
	require.False(t, audit.Complete)
	_, err = Apply(t.Context(), backend, source, manifestPath, journalPath, ApplyOptions{Guard: allowRecovery, Maintenance: true, Tip: source.tip})
	require.Error(t, err, "finite parent TTL cannot guarantee marker survival across remote mutations")
	require.Zero(t, backend.writes)
	require.True(t, backend.Equal(parent, backend.records["parent"]))
	require.Contains(t, backend.records, tx.TxID())
}

type disappearingParentBackend struct {
	*recordBackend
	masterKey string
	deleted   []string
}

func (b *disappearingParentBackend) Delete(ctx context.Context, record Record) error {
	if err := b.recordBackend.Delete(ctx, record); err != nil {
		return err
	}
	b.deleted = append(b.deleted, string(record.Key))
	if string(record.Key) == b.masterKey {
		delete(b.records, "parent")
	}
	return nil
}

func TestApplyBoundaryParentDisappearsBetweenPages(t *testing.T) {
	base, source, tx, manifestPath, journalPath := recoveryFixture(t)
	pageKey := tx.TxID() + ":page"
	backend := &disappearingParentBackend{recordBackend: base, masterKey: pageKey}
	backend.records[pageKey] = Record{Key: []byte(pageKey), Data: []byte("remaining page"), Generation: 4}
	_, err := discoverForTest(t.Context(), backend, source, manifestPath, nil)
	require.NoError(t, err)
	_, err = Apply(t.Context(), backend, source, manifestPath, journalPath, ApplyOptions{Guard: allowRecovery, Maintenance: true, Tip: source.tip})
	require.Error(t, err, "loss of a required marker must stop the next page deletion")
	require.Equal(t, []string{pageKey}, backend.deleted)
	require.Contains(t, backend.records, tx.TxID())
}
