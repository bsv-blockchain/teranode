package replayrecovery

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/teranode/errors"
)

type ApplyOptions struct {
	Guard       Guard
	Maintenance bool
	Resume      bool
	// Tip must be read independently from the target node's canonical blockchain
	// store while writers are stopped and matched against sealed evidence.
	Tip Tip
}

func requireTip(ctx context.Context, source Source, want Tip) error {
	tip, err := source.Tip(ctx)
	if err != nil {
		return err
	}
	if tip != want {
		return failure("trusted chain tip changed; stop and revalidate before resuming")
	}
	return nil
}

func (j *journal) expected(before Record) (*Record, error) {
	b, err := j.get(fmt.Sprintf("latest/%x", before.Key))
	if err != nil {
		return nil, err
	}
	if b == nil {
		return &before, nil
	}
	var r *Record
	if err = json.Unmarshal(b, &r); err != nil {
		return nil, err
	}
	return r, nil
}

func (j *journal) step(id string) (*mutation, error) {
	b, err := j.get("step/" + id)
	if err != nil || b == nil {
		return nil, err
	}
	var m mutation
	err = json.Unmarshal(b, &m)
	return &m, err
}

func (j *journal) prepare(id, kind, child string, before Record) (*mutation, error) {
	old, err := j.step(id)
	if err != nil || old != nil {
		return old, err
	}
	r, err := j.expected(before)
	if err != nil {
		return nil, err
	}
	if r == nil {
		return nil, failure("required record was already deleted by this recovery")
	}
	m := &mutation{Kind: kind, Child: child, Before: *r}
	b, err := json.Marshal(m)
	if err != nil {
		return nil, err
	}
	// FULL-synchronous SQLite commit happens before returning to the caller,
	// therefore before a remote write. A failed commit cannot authorize a write.
	if err = j.put("step/"+id, b); err != nil {
		return nil, err
	}
	return m, nil
}

func (j *journal) mark(ctx context.Context, backend Backend, p Parent, guard Guard) error {
	if err := checkGuard(ctx, guard); err != nil {
		return err
	}
	// Expiry cannot be made atomic with a different record's deletion. Preserve
	// TTLs by refusing expiring replay guards rather than extending their lifetime.
	if p.Record.ExpiresAt != 0 {
		return failure("required parent has finite expiry; automatic repair is unsafe")
	}
	id := fmt.Sprintf("mark/%s/%x", p.Child, p.Record.Key)
	m, err := j.prepare(id, "mark", p.Child, p.Record)
	if err != nil {
		return err
	}
	current, err := backend.Read(ctx, m.Before.Key)
	if err != nil {
		return err
	}
	if current == nil {
		return failure("required parent is absent")
	}
	if m.Done {
		expected, e := j.expected(m.Before)
		if e != nil {
			return e
		}
		if expected == nil || !backend.Equal(*expected, *current) || !backend.HasMarker(*current, p.Child) {
			return failure("verified parent marker or record changed")
		}
		return nil
	}
	if backend.Equal(m.Before, *current) {
		if backend.HasMarker(*current, p.Child) {
			m.After = current
			return j.finish(id, *m)
		}
		if err = checkGuard(ctx, guard); err != nil {
			return err
		}
		after, e := backend.Mark(ctx, m.Before, p.Child)
		if e != nil {
			return e
		}
		if !backend.HasMarker(after, p.Child) || !backend.MatchesMarked(m.Before, after, p.Child) {
			return failure("marker result did not preserve required parent state")
		}
		m.After = &after
	} else if backend.MatchesMarked(m.Before, *current, p.Child) {
		// The previous write landed but its outcome was not journaled. Recognize
		// only the exact expected mutation, never any record merely having a marker.
		m.After = current
	} else {
		return failure("parent generation or before-state changed")
	}
	if err = j.finish(id, *m); err != nil {
		return err
	}
	return checkGuard(ctx, guard)
}

func (j *journal) remove(ctx context.Context, backend Backend, before Record, guard Guard) error {
	if err := checkGuard(ctx, guard); err != nil {
		return err
	}
	id := fmt.Sprintf("delete/%x", before.Key)
	prior, err := j.step(id)
	if err != nil {
		return err
	}
	current, err := backend.Read(ctx, before.Key)
	if err != nil {
		return err
	}
	if prior != nil && prior.Done {
		if current != nil {
			return failure("deleted record reappeared")
		}
		return nil
	}
	if current == nil {
		if prior == nil {
			return failure("record missing without a journaled delete intent")
		}
		return j.finish(id, *prior)
	}
	m, err := j.prepare(id, "delete", "", before)
	if err != nil {
		return err
	}
	if !backend.Equal(m.Before, *current) {
		return failure("child generation or before-state changed")
	}
	if err = checkGuard(ctx, guard); err != nil {
		return err
	}
	if err = backend.Delete(ctx, m.Before); err != nil {
		return err
	}
	current, err = backend.Read(ctx, before.Key)
	if err != nil {
		return err
	}
	if current != nil {
		return failure("child deletion readback failed")
	}
	if err = j.finish(id, *m); err != nil {
		return err
	}
	return checkGuard(ctx, guard)
}

func initializeWork(ctx context.Context, m *manifest, j *journal) error {
	done, err := j.get("work-ready")
	if err != nil || done != nil {
		return err
	}
	if _, err = j.db.Exec("CREATE TABLE IF NOT EXISTS work(id TEXT PRIMARY KEY,entry BLOB NOT NULL,done INTEGER NOT NULL DEFAULT 0); CREATE TABLE IF NOT EXISTS edges(child TEXT NOT NULL,parent TEXT NOT NULL,PRIMARY KEY(child,parent)); CREATE INDEX IF NOT EXISTS edge_parent ON edges(parent)"); err != nil {
		return err
	}
	if err = m.each(ctx, true, func(e Entry) error {
		b, err := json.Marshal(e)
		if err != nil {
			return err
		}
		_, err = j.db.ExecContext(ctx, "INSERT OR IGNORE INTO work(id,entry) VALUES (?,?)", e.Evidence.TxID, b)
		return err
	}); err != nil {
		return err
	}
	rows, err := m.db.QueryContext(ctx, "SELECT child,parent FROM edges")
	if err != nil {
		return err
	}
	for rows.Next() {
		var child, parent string
		if err = rows.Scan(&child, &parent); err != nil {
			_ = rows.Close()
			return err
		}
		if _, err = j.db.ExecContext(ctx, "INSERT OR IGNORE INTO edges(child,parent) VALUES (?,?)", child, parent); err != nil {
			_ = rows.Close()
			return err
		}
	}
	err = rows.Err()
	closeErr := rows.Close()
	if err != nil {
		return err
	}
	if closeErr != nil {
		return closeErr
	}
	return j.put("work-ready", []byte("true"))
}

func sameSnapshot(backend Backend, a, b Snapshot) bool {
	if a.TxID != b.TxID || len(a.Records) != len(b.Records) || len(a.Parents) != len(b.Parents) {
		return false
	}
	records := make(map[string]Record, len(a.Records))
	for _, r := range a.Records {
		records[string(r.Key)] = r
	}
	if len(records) != len(a.Records) {
		return false
	}
	for _, r := range b.Records {
		previous, ok := records[string(r.Key)]
		if !ok || !backend.Equal(previous, r) {
			return false
		}
	}
	parents := make(map[string]Parent, len(a.Parents))
	for _, p := range a.Parents {
		parents[string(p.Record.Key)+p.Child] = p
	}
	if len(parents) != len(a.Parents) {
		return false
	}
	for _, p := range b.Parents {
		previous, ok := parents[string(p.Record.Key)+p.Child]
		if !ok || !backend.Equal(previous.Record, p.Record) {
			return false
		}
	}
	return true
}

// Apply never starts node services. All shared-store writers must remain stopped
// for its duration, including an interrupted apply until deliberate resumption.
func Apply(ctx context.Context, backend Backend, source Source, manifestPath, journalPath string, opts ApplyOptions) (result Summary, err error) {
	if err = checkGuard(ctx, opts.Guard); err != nil {
		return result, err
	}
	if !opts.Maintenance {
		return result, failure("apply requires acknowledgement that all shared-store writers are stopped")
	}
	m, err := openManifest(manifestPath)
	if err != nil {
		return result, err
	}
	defer func() { err = combineErrors(err, m.Close()) }()
	if m.header.Identity != backend.Identity() {
		return result, failure("manifest belongs to a different store")
	}
	if !validHash(opts.Tip.Hash) || opts.Tip != m.header.Tip {
		return result, failure("local tip differs from audit; a new discovery or deliberate resume is required")
	}
	if err = requireTip(ctx, source, opts.Tip); err != nil {
		return result, err
	}
	result, err = m.summary()
	if err != nil {
		return result, err
	}
	result.Stage = "applying"
	j, err := openJournal(journalPath, opts.Resume, m.digest, backend.Identity())
	if err != nil {
		return result, err
	}
	defer func() {
		var intents int
		reportErr := j.db.QueryRow("SELECT COUNT(*) FROM state WHERE key LIKE 'step/%'").Scan(&intents)
		result.RestartRequired = intents > 0
		err = combineErrors(combineErrors(err, reportErr), j.Close())
	}()
	validated, err := j.get("validated")
	if err != nil {
		return result, err
	}
	if validated == nil {
		// Discoveries made before maintenance cannot silently overwrite changes
		// that occurred between discovery and apply. Preflight every before-image
		// before writing even a replay marker.
		err = m.each(ctx, true, func(e Entry) error {
			if e.Snapshot.TxID != e.Evidence.TxID || (e.Action != "mark-absent" && len(e.Snapshot.Records) == 0) || len(e.Snapshot.Parents) == 0 {
				return failure("incomplete repair snapshot")
			}
			// The manifest is editable operator input, not an authority to choose
			// arbitrary record keys. Bind its complete inventory to the real
			// transaction again before any journal authorizes mutations.
			tx, parseErr := bt.NewTxFromString(e.Evidence.RawTx)
			if parseErr != nil || tx.TxID() != e.Evidence.TxID {
				return failure("invalid manifest transaction bytes")
			}
			var actual Snapshot
			var snapshotErr error
			switch e.Action {
			case "delete-recreated":
				actual, snapshotErr = backend.Snapshot(ctx, tx)
			case "mark-absent":
				native, ok := backend.(AbsentBackend)
				if !ok {
					return failure("backend cannot verify absent child")
				}
				if len(e.Snapshot.Records) != 0 {
					return failure("marker-only action includes delete records")
				}
				actual, snapshotErr = native.SnapshotAbsent(ctx, tx)
			default:
				return failure("unsupported repair action")
			}
			if snapshotErr != nil {
				return snapshotErr
			}
			if !sameSnapshot(backend, e.Snapshot, actual) {
				return failure("manifest inventory does not match the transaction's records")
			}
			records := append([]Record(nil), e.Snapshot.Records...)
			for _, p := range e.Snapshot.Parents {
				if p.Record.ExpiresAt != 0 {
					return failure("required parent has finite expiry; automatic repair is unsafe")
				}
				records = append(records, p.Record)
			}
			for _, r := range records {
				current, err := backend.Read(ctx, r.Key)
				if err != nil {
					return err
				}
				if current == nil || !backend.Equal(r, *current) {
					return failure("record changed since discovery for %s", e.Evidence.TxID)
				}
			}
			return nil
		})
		if err != nil {
			return result, err
		}
		if err = j.put("validated", []byte("true")); err != nil {
			return result, err
		}
	}
	if err = inventoryMatches(ctx, m, j, backend, opts.Guard); err != nil {
		return result, err
	}
	if err = initializeWork(ctx, m, j); err != nil {
		return result, err
	}
	for {
		if err = ctx.Err(); err != nil {
			return result, err
		}
		var data []byte
		err = j.db.QueryRowContext(ctx, `SELECT w.entry FROM work w WHERE w.done=0 AND NOT EXISTS(SELECT 1 FROM edges e JOIN work child ON child.id=e.child AND child.done=0 WHERE e.parent=w.id) ORDER BY w.id LIMIT 1`).Scan(&data)
		if errors.Is(err, sql.ErrNoRows) {
			break
		}
		if err != nil {
			return result, err
		}
		var e Entry
		if err = json.Unmarshal(data, &e); err != nil {
			return result, err
		}
		ev, checkErr := source.Check(ctx, e.Evidence.TxID, opts.Tip)
		if checkErr != nil {
			return result, checkErr
		}
		if ev.TxID != e.Evidence.TxID || ev.RawTx != e.Evidence.RawTx || ev.Classification != FullySpent || ev.Tip != opts.Tip {
			return result, failure("canonical evidence no longer authorizes repair of %s", e.Evidence.TxID)
		}
		if err = requireTip(ctx, source, opts.Tip); err != nil {
			return result, err
		}
		for _, p := range e.Snapshot.Parents {
			if err = j.mark(ctx, backend, p, opts.Guard); err != nil {
				return result, failure("mark parent for %s: %w", e.Evidence.TxID, err)
			}
		}
		// Keep the master until all pages are gone so an interrupted job can
		// still derive native page ownership during its resume census.
		for i := len(e.Snapshot.Records) - 1; i >= 0; i-- {
			r := e.Snapshot.Records[i]
			if err = requireTip(ctx, source, opts.Tip); err != nil {
				return result, err
			}
			// Every destructive boundary requires all replay guards to survive.
			for _, p := range e.Snapshot.Parents {
				if err = j.mark(ctx, backend, p, opts.Guard); err != nil {
					return result, err
				}
			}
			if err = j.remove(ctx, backend, r, opts.Guard); err != nil {
				return result, failure("delete record for %s: %w", e.Evidence.TxID, err)
			}
		}
		if _, err = j.db.ExecContext(ctx, "UPDATE work SET done=1 WHERE id=?", e.Evidence.TxID); err != nil {
			return result, err
		}
	}
	var pending int64
	if err = j.db.QueryRow("SELECT COUNT(*) FROM work WHERE done=0").Scan(&pending); err != nil {
		return result, err
	}
	if pending > 0 {
		return result, failure("cyclic or incomplete affected dependency graph")
	}
	if err = j.db.QueryRow("SELECT COUNT(*) FROM work WHERE done=1").Scan(&result.Repaired); err != nil {
		return result, err
	}
	if err = requireTip(ctx, source, opts.Tip); err != nil {
		return result, err
	}
	tipJSON, _ := json.Marshal(opts.Tip)
	if err = j.put("applied", tipJSON); err != nil {
		return result, err
	}
	result.Stage = "applied-verification-pending"
	result.Applied = true
	if !result.Complete {
		return result, ErrIncomplete
	}
	return result, nil
}
