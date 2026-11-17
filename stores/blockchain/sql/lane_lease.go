package sql

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/blockchain/options"
	"github.com/bsv-blockchain/teranode/util"
	"github.com/bsv-blockchain/teranode/util/lane"
)

// grantableLanes is the number of lanes a lease can take: 0 to 253. Lane 254 is the coinbase lane
// and 255 the backfill lane, and neither is ever leased.
const grantableLanes = 254

// Close kinds recorded in lane_close.
const (
	closeClean  = "clean"
	closeLapsed = "lapsed"
)

// The lease, in the blockchain database.
//
// A lease is one process's claim on one lane. lane_lease holds one row per lane, updated in place:
// the lane's current lease or its last one. lane_handout gets one insert-only row per grant, and
// lane_close one insert-only row per ended lease. Each grant in a lane starts one above the lane's
// newest hand-out, whoever held it, so two leases on one lane can never hand out the same
// position, even when one outlives its close.
//
// Times are on the database clock, so a sweep run from any blockchain pod judges every lease by
// the same clock. Closing is one conditional UPDATE from open to closed, and only the caller whose
// UPDATE changed the row writes the close row, so a release and a sweep racing write one.
//
// Open and a renewal that asks for a grant take the hand-out lock, which serialises every grant.
// On SQLite every lease call takes it, because one process owns the file and concurrent writers on
// a shared cache fail with a table lock instead of waiting.

// OpenLaneLease closes stale leases, then takes a free lane with a first grant of GrantMin
// positions. A lane with no lease row and no hand-out comes first, lowest number first; after that
// the lane closed longest ago, where a lane that has hand-outs but no lease row counts as closed at
// its newest hand-out. With no lane free it returns a service error, and the caller retries.
func (s *SQL) OpenLaneLease(ctx context.Context, req options.LaneLeaseOpen) (lane.LeaseID, lane.Grant, error) {
	if req.Holder == "" {
		return lane.LeaseID{}, lane.Grant{}, errors.NewInvalidArgumentError("lane lease holder must not be empty")
	}

	if req.GrantMin == 0 {
		return lane.LeaseID{}, lane.Grant{}, errors.NewInvalidArgumentError("lane grant size must be at least 1")
	}

	if req.CloseAfter <= 0 {
		return lane.LeaseID{}, lane.Grant{}, errors.NewInvalidArgumentError("lane lease close time must be positive")
	}

	id, err := lane.NewLeaseID()
	if err != nil {
		return lane.LeaseID{}, lane.Grant{}, errors.NewProcessingError("could not make a lane lease id", err)
	}

	var grant lane.Grant

	err = s.leaseTx(ctx, true, func(tx *sql.Tx) error {
		if _, err := s.closeStale(ctx, tx, req.CloseAfter); err != nil {
			return err
		}

		chosen, err := s.chooseLeaseLane(ctx, tx)
		if err != nil {
			return err
		}

		grant, err = s.recordGrant(ctx, tx, id, chosen, req.GrantMin, req.Holder, req.Height, lane.Settles{})
		if err != nil {
			return err
		}

		now := s.dbNow()

		if _, err = tx.ExecContext(ctx, `
			INSERT INTO lane_lease (lane, lease_id, holder, opened_at, renewed_at, last_granted, last_reported_taken, settles_first, settles_last_taken, closed_at)
			VALUES ($1, $2, $3, `+now+`, `+now+`, $4, NULL, NULL, NULL, NULL)
			ON CONFLICT (lane) DO UPDATE SET
				lease_id = EXCLUDED.lease_id, holder = EXCLUDED.holder, opened_at = EXCLUDED.opened_at,
				renewed_at = EXCLUDED.renewed_at, last_granted = EXCLUDED.last_granted,
				last_reported_taken = NULL, settles_first = NULL, settles_last_taken = NULL, closed_at = NULL`,
			int(chosen), s.leaseParam(id), req.Holder, lane.ToSigned(grant.Last),
		); err != nil {
			return errors.NewStorageError("could not record the lane lease", err)
		}

		return nil
	})
	if err != nil {
		return lane.LeaseID{}, lane.Grant{}, leaseError("could not open a lane lease", err)
	}

	return id, grant, nil
}

// RenewLaneLease records a renewal: the highest position taken and the settles pair, at the
// database's now. With WantGrant it also records the next grant, sized at Rate × GrantDuration and
// at least GrantMin. A lease that is closed, or is not the lane's current one, gets closed = true
// and nothing is written.
func (s *SQL) RenewLaneLease(ctx context.Context, req options.LaneLeaseRenew) (grant lane.Grant, granted, closed bool, err error) {
	if req.WantGrant && req.GrantMin == 0 {
		return lane.Grant{}, false, false, errors.NewInvalidArgumentError("lane grant size must be at least 1")
	}

	err = s.leaseTx(ctx, req.WantGrant, func(tx *sql.Tx) error {
		res, err := tx.ExecContext(ctx, `
			UPDATE lane_lease SET renewed_at = `+s.dbNow()+`, last_reported_taken = $1, settles_first = $2, settles_last_taken = $3
			WHERE lane = $4 AND lease_id = $5 AND closed_at IS NULL`,
			nullPosition(req.Taken), settlesFirst(req.Settles), settlesLastTaken(req.Settles), int(req.Lane), s.leaseParam(req.Lease),
		)
		if err != nil {
			return errors.NewStorageError("could not renew the lane lease", err)
		}

		if n, err := res.RowsAffected(); err != nil {
			return errors.NewStorageError("could not renew the lane lease", err)
		} else if n == 0 {
			closed = true
			return nil
		}

		if !req.WantGrant {
			return nil
		}

		grant, err = s.recordGrant(ctx, tx, req.Lease, req.Lane, grantSize(req.Rate, req.GrantDuration, req.GrantMin), "", req.Height, req.Settles)
		if err != nil {
			return err
		}

		if _, err = tx.ExecContext(ctx, `UPDATE lane_lease SET last_granted = $1 WHERE lane = $2`, lane.ToSigned(grant.Last), int(req.Lane)); err != nil {
			return errors.NewStorageError("could not record the lane grant on the lease", err)
		}

		granted = true

		return nil
	})
	if err != nil {
		return lane.Grant{}, false, false, leaseError("could not renew a lane lease", err)
	}

	return grant, granted, closed, nil
}

// ReleaseLaneLease closes a lease cleanly and records the last position its holder took. It
// reports alreadyClosed when the lease was closed before, and then writes nothing.
func (s *SQL) ReleaseLaneLease(ctx context.Context, req options.LaneLeaseRelease) (alreadyClosed bool, err error) {
	err = s.leaseTx(ctx, false, func(tx *sql.Tx) error {
		now := s.dbNow()

		res, err := tx.ExecContext(ctx, `UPDATE lane_lease SET closed_at = `+now+` WHERE lane = $1 AND lease_id = $2 AND closed_at IS NULL`,
			int(req.Lane), s.leaseParam(req.Lease))
		if err != nil {
			return errors.NewStorageError("could not close the lane lease", err)
		}

		if n, err := res.RowsAffected(); err != nil {
			return errors.NewStorageError("could not close the lane lease", err)
		} else if n == 0 {
			alreadyClosed = true
			return nil
		}

		if _, err = tx.ExecContext(ctx, `
			INSERT INTO lane_close (lease_id, lane, kind, last, last_reported_taken, settles_first, settles_last_taken, closed_at)
			VALUES ($1, $2, $3, $4, NULL, $5, $6, `+now+`)`,
			s.leaseParam(req.Lease), int(req.Lane), closeClean, lane.ToSigned(req.LastTaken), settlesFirst(req.Settles), settlesLastTaken(req.Settles),
		); err != nil {
			return errors.NewStorageError("could not record the lane lease close", err)
		}

		return nil
	})
	if err != nil {
		return false, leaseError("could not release a lane lease", err)
	}

	return alreadyClosed, nil
}

// CloseStaleLaneLeases closes every open lease whose last renewal is closeAfter or more old on the
// database clock, and records each as lapsed. It returns how many it closed. Several blockchain
// pods may sweep at once: the conditional close makes each lease close once.
func (s *SQL) CloseStaleLaneLeases(ctx context.Context, closeAfter time.Duration) (int, error) {
	var closed int

	err := s.leaseTx(ctx, false, func(tx *sql.Tx) error {
		var err error

		closed, err = s.closeStale(ctx, tx, closeAfter)

		return err
	})
	if err != nil {
		return 0, leaseError("could not close stale lane leases", err)
	}

	return closed, nil
}

// RenewOpenLaneLeases treats every open lease as renewed now. The blockchain service runs it at
// start, so its own downtime never counts against the leases it was away for.
func (s *SQL) RenewOpenLaneLeases(ctx context.Context) (int, error) {
	var renewed int64

	err := s.leaseTx(ctx, false, func(tx *sql.Tx) error {
		res, err := tx.ExecContext(ctx, `UPDATE lane_lease SET renewed_at = `+s.dbNow()+` WHERE closed_at IS NULL`)
		if err != nil {
			return errors.NewStorageError("could not renew the open lane leases", err)
		}

		renewed, err = res.RowsAffected()

		return err
	})
	if err != nil {
		return 0, leaseError("could not renew the open lane leases", err)
	}

	return int(renewed), nil
}

// leaseTx runs fn in a transaction. With grants true it holds the hand-out lock. On SQLite it
// always holds it; see the comment at the top of this file.
func (s *SQL) leaseTx(ctx context.Context, grants bool, fn func(tx *sql.Tx) error) error {
	if s.engine != util.Postgres {
		laneHandoutMu.Lock()
		defer laneHandoutMu.Unlock()
	}

	return s.db.RetryTx(ctx, &sql.TxOptions{}, func(tx *sql.Tx) error {
		if grants && s.engine == util.Postgres {
			if _, err := tx.ExecContext(ctx, `SELECT pg_advisory_xact_lock($1)`, laneHandoutLockID); err != nil {
				return errors.NewStorageError("could not take the lane hand-out lock", err)
			}
		}

		return fn(tx)
	})
}

// closeStale is the sweep's statement. The staleness test sits in the UPDATE itself, so a renewal
// that commits while the sweep waits for the row is seen, and that lease stays open.
func (s *SQL) closeStale(ctx context.Context, tx *sql.Tx, closeAfter time.Duration) (int, error) {
	now := s.dbNow()

	var (
		rows *sql.Rows
		err  error
	)

	if s.engine == util.Postgres {
		rows, err = tx.QueryContext(ctx, `
			UPDATE lane_lease SET closed_at = `+now+`
			WHERE closed_at IS NULL AND renewed_at <= `+now+` - $1::bigint * interval '1 microsecond'
			RETURNING lease_id, lane, last_granted, last_reported_taken, settles_first, settles_last_taken`,
			closeAfter.Microseconds())
	} else {
		rows, err = tx.QueryContext(ctx, `
			UPDATE lane_lease SET closed_at = `+now+`
			WHERE closed_at IS NULL AND renewed_at <= strftime('%Y-%m-%d %H:%M:%f', 'now', $1)
			RETURNING lease_id, lane, last_granted, last_reported_taken, settles_first, settles_last_taken`,
			sqliteOffset(-closeAfter))
	}

	if err != nil {
		return 0, errors.NewStorageError("could not close stale lane leases", err)
	}

	type lapsed struct {
		id                                  any
		lane                                int
		lastGranted                         int64
		reported, settlesFirst, settlesLast sql.NullInt64
	}

	var closed []lapsed

	for rows.Next() {
		var (
			l  lapsed
			id []byte
		)

		if err = rows.Scan(&id, &l.lane, &l.lastGranted, &l.reported, &l.settlesFirst, &l.settlesLast); err != nil {
			_ = rows.Close()
			return 0, errors.NewStorageError("could not read a stale lane lease", err)
		}

		if s.engine == util.Postgres {
			l.id = string(id)
		} else {
			l.id = id
		}

		closed = append(closed, l)
	}

	if err = rows.Close(); err != nil {
		return 0, errors.NewStorageError("could not read the stale lane leases", err)
	}

	if err = rows.Err(); err != nil {
		return 0, errors.NewStorageError("could not read the stale lane leases", err)
	}

	for _, l := range closed {
		if _, err = tx.ExecContext(ctx, `
			INSERT INTO lane_close (lease_id, lane, kind, last, last_reported_taken, settles_first, settles_last_taken, closed_at)
			VALUES ($1, $2, $3, $4, $5, $6, $7, `+now+`)`,
			l.id, l.lane, closeLapsed, l.lastGranted, l.reported, l.settlesFirst, l.settlesLast,
		); err != nil {
			return 0, errors.NewStorageError("could not record a lapsed lane lease", err)
		}
	}

	return len(closed), nil
}

// chooseLeaseLane applies the lane choice rule. It runs under the hand-out lock.
func (s *SQL) chooseLeaseLane(ctx context.Context, tx *sql.Tx) (uint8, error) {
	type leaseRow struct {
		closedAt    sql.NullTime
		lastGranted int64
	}

	leases := make(map[int]leaseRow, grantableLanes)

	rows, err := tx.QueryContext(ctx, `SELECT lane, closed_at, last_granted FROM lane_lease`)
	if err != nil {
		return 0, errors.NewStorageError("could not read the lane leases", err)
	}

	for rows.Next() {
		var (
			l int
			r leaseRow
		)

		if err = rows.Scan(&l, &r.closedAt, &r.lastGranted); err != nil {
			_ = rows.Close()
			return 0, errors.NewStorageError("could not read a lane lease", err)
		}

		leases[l] = r
	}

	if err = rows.Close(); err != nil {
		return 0, errors.NewStorageError("could not read the lane leases", err)
	}

	if err = rows.Err(); err != nil {
		return 0, errors.NewStorageError("could not read the lane leases", err)
	}

	var (
		best      = -1
		bestSince time.Time
	)

	for l := 0; l < grantableLanes; l++ {
		var since time.Time

		if r, ok := leases[l]; ok {
			if !r.closedAt.Valid || lane.FromSigned(r.lastGranted) >= lane.MaxPosition {
				continue
			}

			since = r.closedAt.Time
		} else {
			st, err := readLaneState(ctx, tx, uint8(l)) //nolint:gosec // l is below 254
			if err != nil {
				return 0, err
			}

			if !st.used {
				// A lane never used comes first, lowest number first.
				return uint8(l), nil //nolint:gosec // l is below 254
			}

			if st.exhausted {
				continue
			}

			since = st.handedAt
		}

		if best < 0 || since.Before(bestSince) {
			best, bestSince = l, since
		}
	}

	if best < 0 {
		return 0, errors.NewServiceError("no free lane: all %d lanes hold an open lease or are exhausted", grantableLanes)
	}

	return uint8(best), nil //nolint:gosec // best is below 254
}

// recordGrant records the lane's next grant: size positions starting one above the lane's newest
// hand-out. It runs under the hand-out lock.
func (s *SQL) recordGrant(ctx context.Context, tx *sql.Tx, id lane.LeaseID, l uint8, size uint64, holder string, height uint32, settles lane.Settles) (lane.Grant, error) {
	st, err := readLaneState(ctx, tx, l)
	if err != nil {
		return lane.Grant{}, err
	}

	first := lane.FirstPosition
	if st.used {
		if st.exhausted {
			return lane.Grant{}, errors.NewServiceError("lane %d has no positions left", l)
		}

		first = st.last + 1
	}

	last := lane.MaxPosition
	if size-1 < lane.MaxPosition-first {
		last = first + size - 1
	}

	if holder == "" {
		if err = tx.QueryRowContext(ctx, `SELECT holder FROM lane_lease WHERE lane = $1`, int(l)).Scan(&holder); err != nil {
			return lane.Grant{}, errors.NewStorageError("could not read the lane lease holder", err)
		}
	}

	if _, err = tx.ExecContext(ctx, `
		INSERT INTO lane_handout (lane, first, last, holder, height, handed_at, lease_id, settles_first, settles_last_taken)
		VALUES ($1, $2, $3, $4, $5, `+s.dbNow()+`, $6, $7, $8)`,
		int(l), lane.ToSigned(first), lane.ToSigned(last), holder, int64(height), s.leaseParam(id), settlesFirst(settles), settlesLastTaken(settles),
	); err != nil {
		return lane.Grant{}, errors.NewStorageError("could not record the lane grant", err)
	}

	return lane.Grant{Lane: l, First: first, Last: last}, nil
}

// grantSize is two minutes of the reported rate, by default, and never less than the minimum.
func grantSize(rate uint64, d time.Duration, minimum uint64) uint64 {
	secs := uint64(d / time.Second)
	if secs == 0 || rate == 0 {
		return minimum
	}

	if rate > lane.MaxPosition/secs {
		return lane.MaxPosition
	}

	return max(minimum, rate*secs)
}

// dbNow is the database clock as an SQL expression. Postgres uses clock_timestamp, not now, so a
// grant recorded after waiting for the hand-out lock is stamped after the grant it waited for.
func (s *SQL) dbNow() string {
	if s.engine == util.Postgres {
		return `clock_timestamp()`
	}

	return `strftime('%Y-%m-%d %H:%M:%f', 'now')`
}

// leaseParam is a lease ID in the form the database keeps: a UUID string on Postgres, 16 bytes on SQLite.
func (s *SQL) leaseParam(id lane.LeaseID) any {
	if s.engine == util.Postgres {
		return id.String()
	}

	return id[:]
}

// sqliteOffset is an SQLite date modifier that moves a time by d, to the millisecond.
func sqliteOffset(d time.Duration) string {
	return fmt.Sprintf("%+.3f seconds", d.Seconds())
}

func nullPosition(p uint64) sql.NullInt64 {
	return sql.NullInt64{Int64: lane.ToSigned(p), Valid: p != 0}
}

func settlesFirst(st lane.Settles) sql.NullInt64 {
	return nullPosition(st.First)
}

func settlesLastTaken(st lane.Settles) sql.NullInt64 {
	return sql.NullInt64{Int64: lane.ToSigned(st.LastTaken), Valid: st.First != 0}
}

func leaseError(msg string, err error) error {
	if errors.Is(err, errors.ErrStorageError) || errors.Is(err, errors.ErrServiceError) || errors.Is(err, errors.ErrInvalidArgument) {
		return err
	}

	return errors.NewStorageError(msg, err)
}
