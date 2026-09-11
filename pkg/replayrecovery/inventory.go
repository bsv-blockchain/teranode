package replayrecovery

import (
	"context"
	"database/sql"
	"encoding/hex"
	"encoding/json"

	"github.com/bsv-blockchain/teranode/errors"
)

func checkGuard(ctx context.Context, guard Guard) error {
	if guard == nil {
		return failure("persisted IDLE guard is required")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	return guard(ctx)
}

// Inventory takes a complete, disk-backed census before evidence or mutations.
// An interrupted scan is never marked complete and cannot authorize apply.
func Inventory(ctx context.Context, backend CensusBackend, path string, guard Guard) (result Summary, err error) {
	if err = checkGuard(ctx, guard); err != nil {
		return result, err
	}
	db, err := openPrivateDB(path, true, false)
	if err != nil {
		return result, err
	}
	defer func() { err = combineErrors(err, db.Close()) }()
	_, err = db.db.ExecContext(ctx, `CREATE TABLE inventory (key BLOB PRIMARY KEY,txid TEXT NOT NULL,master INTEGER NOT NULL,page INTEGER NOT NULL,expected_pages INTEGER NOT NULL,record BLOB NOT NULL,candidate INTEGER NOT NULL,reason TEXT NOT NULL);
 CREATE INDEX inventory_txid ON inventory(txid);
 CREATE TABLE spend_refs (parent_key BLOB NOT NULL,vout INTEGER NOT NULL,parent_txid TEXT NOT NULL,child_txid TEXT NOT NULL,vin INTEGER NOT NULL,marked INTEGER NOT NULL,PRIMARY KEY(parent_key,vout));
 CREATE INDEX spend_refs_child ON spend_refs(child_txid);
 CREATE TABLE findings(key BLOB NOT NULL,reason TEXT NOT NULL,PRIMARY KEY(key,reason));
 CREATE TABLE inventory_state(complete INTEGER NOT NULL);
 CREATE TABLE entries(id TEXT PRIMARY KEY,entry BLOB NOT NULL DEFAULT '',classification TEXT NOT NULL DEFAULT '',blocked INTEGER NOT NULL DEFAULT 0);
 CREATE TABLE edges(child TEXT NOT NULL,parent TEXT NOT NULL,PRIMARY KEY(child,parent));
 CREATE INDEX edge_parent ON edges(parent);
 CREATE TABLE targets(id TEXT PRIMARY KEY);
 CREATE TABLE manifest(header BLOB NOT NULL,digest TEXT NOT NULL);`)
	if err != nil {
		return result, err
	}
	result.Stage = "inventory"
	err = backend.Inventory(ctx, func(rec InventoryRecord) error {
		if err := checkGuard(ctx, guard); err != nil {
			return err
		}
		if len(rec.Record.Key) == 0 {
			return failure("inventory record has no stable key")
		}
		if rec.TxID != "" && !validHash(rec.TxID) {
			return failure("invalid inventory transaction identity")
		}
		if rec.TxID == "" && rec.Reason == "" {
			rec.Reason = "record ownership unknown"
		}
		data, e := json.Marshal(rec.Record)
		if e != nil {
			return e
		}
		_, e = db.db.ExecContext(ctx, "INSERT INTO inventory VALUES(?,?,?,?,?,?,?,?)", rec.Record.Key, rec.TxID, rec.Master, rec.Page, rec.ExpectedPages, data, rec.Candidate, rec.Reason)
		if e != nil {
			return e
		}
		if rec.Reason != "" {
			if _, e = db.db.ExecContext(ctx, "INSERT OR IGNORE INTO findings VALUES(?,?)", rec.Record.Key, rec.Reason); e != nil {
				return e
			}
		}
		result.Scanned++
		return nil
	})
	if err != nil {
		return result, err
	}
	err = backend.SpendReferences(ctx, func(ref SpendReference) error {
		if err := checkGuard(ctx, guard); err != nil {
			return err
		}
		if !validHash(ref.ParentTxID) || !validHash(ref.ChildTxID) {
			return failure("invalid spend reference identity")
		}
		var owner string
		if e := db.db.QueryRowContext(ctx, "SELECT txid FROM inventory WHERE key=?", ref.ParentKey).Scan(&owner); e != nil {
			return e
		}
		if owner != ref.ParentTxID {
			return failure("spend reference ownership changed during inventory")
		}
		_, e := db.db.ExecContext(ctx, "INSERT INTO spend_refs VALUES(?,?,?,?,?,?)", ref.ParentKey, ref.Vout, ref.ParentTxID, ref.ChildTxID, ref.Vin, ref.Marked)
		return e
	})
	if err != nil {
		return result, err
	}
	// Only candidate masters and absent children lacking a marker seed repairs.
	// Page inconsistencies are findings; an unknown owner can never be discarded.
	_, err = db.db.ExecContext(ctx, `INSERT OR IGNORE INTO findings SELECT m.key,'missing, duplicate or inconsistent pagination' FROM inventory m WHERE m.master=1 AND (
   (SELECT COUNT(*) FROM inventory p WHERE p.txid=m.txid AND p.master=0)!=m.expected_pages OR
   EXISTS(SELECT 1 FROM inventory p WHERE p.txid=m.txid AND p.master=0 AND (p.page<1 OR p.page>m.expected_pages)) OR
   (SELECT COUNT(DISTINCT p.page) FROM inventory p WHERE p.txid=m.txid AND p.master=0)!=m.expected_pages);
 INSERT OR IGNORE INTO findings SELECT p.key,'pagination master absent or ambiguous' FROM inventory p WHERE p.master=0 AND (SELECT COUNT(*) FROM inventory m WHERE m.txid=p.txid AND m.master=1)!=1;
 INSERT OR IGNORE INTO findings SELECT m.key,'duplicate transaction master' FROM inventory m WHERE m.master=1 AND (SELECT COUNT(*) FROM inventory n WHERE n.txid=m.txid AND n.master=1)!=1;
 INSERT OR IGNORE INTO entries(id) SELECT txid FROM inventory WHERE txid!='' AND (candidate=1 OR key IN (SELECT key FROM findings));
 INSERT OR IGNORE INTO entries(id) SELECT child_txid FROM spend_refs r WHERE marked=0 AND NOT EXISTS(SELECT 1 FROM inventory i WHERE i.txid=r.child_txid AND i.master=1);
 INSERT OR IGNORE INTO edges SELECT child_txid,parent_txid FROM spend_refs;
 INSERT OR IGNORE INTO entries(id) SELECT e.child FROM edges e JOIN inventory i ON i.txid=e.child AND i.master=1 WHERE e.parent IN (SELECT id FROM entries);
 INSERT OR IGNORE INTO targets SELECT id FROM entries;
 INSERT OR IGNORE INTO targets SELECT parent FROM edges WHERE child IN (SELECT id FROM entries);`)
	if err != nil {
		return result, err
	}
	if err = checkGuard(ctx, guard); err != nil {
		return result, err
	}
	_, err = db.db.ExecContext(ctx, "INSERT INTO inventory_state VALUES(1)")
	return result, err
}

// inventoryMatches compares the entire census against original or journaled
// before/after images. Any unjournaled mutation, addition or loss aborts apply.
func inventoryMatches(ctx context.Context, m *manifest, j *journal, backend Backend, guard Guard) error {
	census, ok := backend.(CensusBackend)
	if !ok {
		return failure("backend cannot enumerate the full store")
	}
	if _, err := m.db.ExecContext(ctx, "CREATE TEMP TABLE IF NOT EXISTS seen(key BLOB PRIMARY KEY); DELETE FROM seen"); err != nil {
		return err
	}
	err := census.Inventory(ctx, func(rec InventoryRecord) error {
		if err := checkGuard(ctx, guard); err != nil {
			return err
		}
		var data []byte
		if err := m.db.QueryRowContext(ctx, "SELECT record FROM inventory WHERE key=?", rec.Record.Key).Scan(&data); err != nil {
			return failure("store inventory changed: %w", err)
		}
		var before Record
		if err := json.Unmarshal(data, &before); err != nil {
			return err
		}
		expected := &before
		if j != nil {
			var err error
			expected, err = j.expected(before)
			if err != nil {
				return err
			}
		}
		if expected == nil || !backend.Equal(*expected, rec.Record) {
			// An interrupted intent can have landed without its outcome. Accept only
			// an exact native marker transition; apply will journal reconciliation.
			if j == nil {
				return failure("store record changed since inventory")
			}
			rows, err := j.db.QueryContext(ctx, "SELECT value FROM state WHERE key LIKE 'step/mark/%'")
			if err != nil {
				return err
			}
			matched := false
			for rows.Next() {
				var raw []byte
				if err = rows.Scan(&raw); err != nil {
					break
				}
				var step mutation
				if err = json.Unmarshal(raw, &step); err != nil {
					break
				}
				if !step.Done && backend.MatchesMarked(step.Before, rec.Record, step.Child) {
					matched = true
				}
			}
			rowErr := rows.Err()
			closeErr := rows.Close()
			if err != nil {
				return err
			}
			if rowErr != nil {
				return rowErr
			}
			if closeErr != nil {
				return closeErr
			}
			if !matched {
				return failure("store record changed outside the recovery journal")
			}
		}
		_, err := m.db.ExecContext(ctx, "INSERT INTO seen VALUES(?)", rec.Record.Key)
		return err
	})
	if err != nil {
		return err
	}
	last := []byte{}
	for {
		var key, data []byte
		err = m.db.QueryRowContext(ctx, "SELECT key,record FROM inventory WHERE key>? AND key NOT IN (SELECT key FROM seen) ORDER BY key LIMIT 1", last).Scan(&key, &data)
		if errors.Is(err, sql.ErrNoRows) {
			break
		}
		if err != nil {
			return err
		}
		if j == nil {
			return failure("store record disappeared since inventory")
		}
		var before Record
		if err = json.Unmarshal(data, &before); err != nil {
			return err
		}
		step, err := j.step("delete/" + hex.EncodeToString(key))
		if err != nil {
			return err
		}
		if step == nil {
			return failure("record disappeared without this job's exact delete intent")
		}
		last = key
	}
	return checkGuard(ctx, guard)
}
