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

type dependencyCensusBackend struct {
	*recordBackend
	refs []SpendReference
}

func (b *dependencyCensusBackend) SpendReferences(ctx context.Context, visit func(SpendReference) error) error {
	if err := b.recordBackend.SpendReferences(ctx, visit); err != nil {
		return err
	}
	for _, ref := range b.refs {
		if err := visit(ref); err != nil {
			return err
		}
	}
	return ctx.Err()
}

func TestApplyBoundaryHealthySiblingDoesNotBlockReplay(t *testing.T) {
	base, source, replay, manifestPath, journalPath := recoveryFixture(t)
	sibling := bt.NewTx()
	parentID := replay.Inputs[0].PreviousTxIDChainHash().String()
	require.NoError(t, sibling.From(parentID, 1, "51", 1000))
	require.NoError(t, sibling.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 500))
	siblingRecord := Record{Key: []byte(sibling.TxID()), Data: []byte("normally mined sibling"), Generation: 3}
	base.records[sibling.TxID()] = siblingRecord
	base.txs[sibling.TxID()] = sibling
	backend := &dependencyCensusBackend{recordBackend: base, refs: []SpendReference{{ParentKey: []byte("parent"), ParentTxID: parentID, Vout: 1, ChildTxID: sibling.TxID()}}}
	summary, err := discoverForTest(t.Context(), backend, source, manifestPath, nil)
	require.NoError(t, err, "a healthy sibling must not make their historical parent an affected entry")
	require.True(t, summary.Complete)
	require.EqualValues(t, 1, summary.FullySpent)
	require.Zero(t, summary.Blocked)
	result, err := Apply(t.Context(), backend, source, manifestPath, journalPath, ApplyOptions{Guard: allowRecovery, Maintenance: true, Tip: source.tip})
	require.NoError(t, err)
	require.EqualValues(t, 1, result.Repaired)
	require.NotContains(t, backend.records, replay.TxID())
	require.Equal(t, siblingRecord, backend.records[sibling.TxID()])
	require.True(t, backend.HasMarker(backend.records["parent"], replay.TxID()))
}

func TestApplyBoundaryUnknownDescendantThroughConfirmedEntryBlocksReplay(t *testing.T) {
	base, source, replay, manifestPath, journalPath := recoveryFixture(t)
	descendant := func(parent *bt.Tx) *bt.Tx {
		tx := bt.NewTx()
		require.NoError(t, tx.From(parent.TxID(), 0, parent.Outputs[0].LockingScript.String(), parent.Outputs[0].Satoshis))
		require.NoError(t, tx.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", parent.Outputs[0].Satoshis-100))
		base.txs[tx.TxID()] = tx
		base.records[tx.TxID()] = Record{Key: []byte(tx.TxID()), Data: []byte("mined metadata"), Generation: 3}
		return tx
	}
	confirmed := descendant(replay)
	unresolved := descendant(confirmed)
	source.evidence[confirmed.TxID()] = Evidence{TxID: confirmed.TxID(), RawTx: confirmed.String(), BlockHash: source.tip.Hash, BlockHeight: source.tip.Height, Classification: Live}
	backend := &dependencyCensusBackend{recordBackend: base, refs: []SpendReference{
		{ParentKey: []byte(replay.TxID()), ParentTxID: replay.TxID(), ChildTxID: confirmed.TxID()},
		{ParentKey: []byte(confirmed.TxID()), ParentTxID: confirmed.TxID(), ChildTxID: unresolved.TxID()},
	}}
	summary, err := discoverForTest(t.Context(), backend, source, manifestPath, nil)
	require.ErrorIs(t, err, ErrIncomplete)
	require.EqualValues(t, 1, summary.Kept, "canonical intermediary remains confirmed")
	require.EqualValues(t, 1, summary.Blocked, "unclassified descendant still protects its affected ancestors")
	result, err := Apply(t.Context(), backend, source, manifestPath, journalPath, ApplyOptions{Guard: allowRecovery, Maintenance: true, Tip: source.tip})
	require.ErrorIs(t, err, ErrIncomplete)
	require.Zero(t, result.Repaired)
	require.Zero(t, backend.writes)
	require.Contains(t, backend.records, replay.TxID())
	require.Contains(t, backend.records, confirmed.TxID())
	require.Contains(t, backend.records, unresolved.TxID())
}

func TestMinedDescendantEntryWithoutInclusionBlocksRepair(t *testing.T) {
	base, source, replay, manifestPath, journalPath := recoveryFixture(t)
	child := bt.NewTx()
	require.NoError(t, child.From(replay.TxID(), 0, replay.Outputs[0].LockingScript.String(), replay.Outputs[0].Satoshis))
	require.NoError(t, child.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 250))
	base.txs[child.TxID()] = child
	base.records[child.TxID()] = Record{Key: []byte(child.TxID()), Data: []byte("normally mined"), Generation: 3}
	source.evidence[child.TxID()] = Evidence{TxID: child.TxID(), RawTx: child.String(), BlockHeight: 100, Classification: Live}
	backend := &dependencyCensusBackend{recordBackend: base, refs: []SpendReference{{ParentKey: []byte(replay.TxID()), ParentTxID: replay.TxID(), ChildTxID: child.TxID()}}}
	summary, err := discoverForTest(t.Context(), backend, source, manifestPath, nil)
	require.ErrorIs(t, err, ErrIncomplete)
	require.EqualValues(t, 1, summary.Unknown)
	require.Zero(t, summary.Kept)
	require.EqualValues(t, 1, summary.Blocked)
	m, err := openManifest(manifestPath)
	require.NoError(t, err)
	var data []byte
	require.NoError(t, m.db.QueryRow("SELECT entry FROM entries WHERE id=?", child.TxID()).Scan(&data))
	var entry Entry
	require.NoError(t, json.Unmarshal(data, &entry))
	require.Equal(t, "dependent mined record lacks canonical inclusion", entry.Evidence.Reason)
	require.NoError(t, m.Close())
	result, err := Apply(t.Context(), backend, source, manifestPath, journalPath, ApplyOptions{Guard: allowRecovery, Maintenance: true, Tip: source.tip})
	require.ErrorIs(t, err, ErrIncomplete)
	require.Zero(t, result.Repaired)
	require.Zero(t, backend.writes)
	require.Contains(t, base.records, replay.TxID())
	require.Contains(t, base.records, child.TxID())
}
