package utxoset

import (
	"context"
	"fmt"
	"strings"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/jackc/pgx/v5"
)

// partitionSpec describes one range partition the block path may need to add to a
// partitioned parent while the node runs.
type partitionSpec struct {
	parent string   // the partitioned table
	child  string   // the partition's name
	key    string   // the column the parent is ranged on
	lo, hi uint32   // the partition's range, lo inclusive and hi exclusive
	with   string   // storage parameters for the child, without the WITH keyword; may be empty
	after  []string // statements run on the standalone child before it is attached
}

// partitionStateSQL reports whether the named table exists and, if so, whether it is an
// attached partition of the parent and whether a detach of it is still pending.
//
// It joins pg_inherits on the parent, so a standalone table that merely shares the name is
// reported as existing but not a partition, which is the state a crash between DETACH and
// DROP leaves behind.
const partitionStateSQL = `
SELECT c.relispartition,
       COALESCE(i.inhdetachpending, false)
  FROM pg_class c
  LEFT JOIN pg_inherits i
         ON i.inhrelid = c.oid AND i.inhparent = $1::regclass
 WHERE c.oid = to_regclass($2)`

// ensureAttachedPartition adds spec's partition to its parent without ever taking the
// parent's strongest lock, and does nothing if the partition is already attached.
//
// CREATE TABLE ... PARTITION OF takes ACCESS EXCLUSIVE on the parent. That waits for every
// reader of the parent to finish, and every new reader then waits behind it. On the block
// path that is a stall of however long the longest concurrent read of the parent takes:
// MEASURED on mainnet, 939 to 1,004 ms per window boundary while the pruner's stamp held a
// multi-second read on tx_mined.
//
// ATTACH PARTITION takes SHARE UPDATE EXCLUSIVE instead, which neither readers nor ordinary
// writers conflict with. So the child is built as a standalone table shaped like the parent,
// given a CHECK constraint matching its range so the attach can skip scanning it, attached,
// and relieved of the helper constraint. The parent's indexes are recreated on the child by
// INCLUDING INDEXES.
//
// All of it is one transaction, so a crash leaves either nothing or an attached partition.
//
// A table of the child's name that exists but is not an attached partition is refused. Only
// the pruner leaves that state, between detaching a window and dropping it, and its rows are
// a window the pruner has already retired. Adopting them would resurrect a dropped window.
//
// Callers hold their own per-table mutex, so within a process two ensures for the same
// window never run at once. Across processes a race ends in a duplicate-table error from
// CREATE, which the caller sees and which the next call clears, because the cache is set
// only on success.
func (s *Store) ensureAttachedPartition(ctx context.Context, spec partitionSpec) error {
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return errors.NewStorageError("[utxoset] begin partition attach of %s", spec.child, err)
	}

	defer func() { _ = tx.Rollback(ctx) }()

	var (
		isPartition   bool
		detachPending bool
	)

	err = tx.QueryRow(ctx, partitionStateSQL, spec.parent, spec.child).Scan(&isPartition, &detachPending)

	switch {
	case err == nil && isPartition && !detachPending:
		return nil

	case err == nil:
		return errors.NewProcessingError("[utxoset] refusing to adopt %s: it exists but is not an attached partition of %s (detach pending: %v)",
			spec.child, spec.parent, detachPending)

	case !errors.Is(err, pgx.ErrNoRows):
		return errors.NewStorageError("[utxoset] read partition state of %s", spec.child, err)
	}

	with := ""
	if spec.with != "" {
		with = " WITH (" + spec.with + ")"
	}

	rangeConstraint := spec.child + "_range"

	statements := make([]string, 0, len(spec.after)+4)
	statements = append(statements,
		fmt.Sprintf(`CREATE TABLE %s (LIKE %s INCLUDING DEFAULTS INCLUDING CONSTRAINTS INCLUDING INDEXES)%s`,
			spec.child, spec.parent, with))
	statements = append(statements, spec.after...)
	statements = append(statements,
		fmt.Sprintf(`ALTER TABLE %s ADD CONSTRAINT %s CHECK (%s >= %d AND %s < %d)`,
			spec.child, rangeConstraint, spec.key, spec.lo, spec.key, spec.hi),
		fmt.Sprintf(`ALTER TABLE %s ATTACH PARTITION %s FOR VALUES FROM (%d) TO (%d)`,
			spec.parent, spec.child, spec.lo, spec.hi),
		fmt.Sprintf(`ALTER TABLE %s DROP CONSTRAINT %s`, spec.child, rangeConstraint),
	)

	if _, err := tx.Exec(ctx, strings.Join(statements, ";\n")); err != nil {
		return errors.NewStorageError("[utxoset] create and attach partition %s of %s", spec.child, spec.parent, err)
	}

	if err := tx.Commit(ctx); err != nil {
		return errors.NewStorageError("[utxoset] commit partition attach of %s", spec.child, err)
	}

	return nil
}
