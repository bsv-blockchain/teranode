package sql

import (
	"context"
	"database/sql"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/stores/blockchain/options"
	"github.com/bsv-blockchain/teranode/util"
	"github.com/bsv-blockchain/teranode/util/lane"
)

// laneHandoutLockID is the Postgres advisory lock that serialises lane
// hand-outs across blockchain service pods. It must stay unique among the
// advisory lock ids used on this database and stable across releases.
const laneHandoutLockID int64 = 7_265_726_102

// laneHandoutMu serialises hand-outs on SQLite, where one process owns the file.
var laneHandoutMu sync.Mutex

// openLeaseAttemptHook, when set, runs at the end of each OpenLaneLease attempt, inside its
// transaction. It is nil in production. Tests set it to make an attempt commit and then report a
// retriable error, as a lost COMMIT reply would.
var openLeaseAttemptHook func(tx *sql.Tx) error

// laneState is the latest hand-out in one lane.
type laneState struct {
	used      bool
	last      uint64
	handedAt  time.Time
	exhausted bool
}

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
// the lane's current lease or its last one, with its times and no positions. lane_handout gets one
// insert-only row per grant, and lane_close one insert-only row per ended lease. Each grant in a
// lane starts one above the lane's newest hand-out, whoever held it, so two leases on one lane can
// never hand out the same position, even when one outlives its close. The service holds nothing
// between calls: a holder reports a grant's final number (its settles pair) on the request after it
// leaves the grant, and that request's row records it.
//
// Not every hand-out is settled. A grant nobody used gets no settles pair: the spare held at a
// release, a grant the holder refused, and a grant whose reply was lost. A lost reply happens when
// the service commits the grant but the holder's call times out or the connection drops: the holder
// retries with the same settles pair and gets the next grant, so two hand-out rows carry that pair
// and the first is never installed and never settled. Positions stay unique, because the retry
// starts above it. A reader of settles pairs must take a hand-out no pair names as possibly unused.
//
// Times are on the database clock, so a sweep run from any blockchain pod judges every lease by
// the same clock. Closing is one conditional UPDATE from open to closed, and only the caller whose
// UPDATE changed the row writes the close row, so a release and a sweep racing write one.
//
// Open and a renewal that asks for a grant take the hand-out lock, which serialises every grant.
// On SQLite every lease call takes it, because one process owns the file and concurrent writers on
// a shared cache fail with a table lock instead of waiting.

// OpenLaneLease closes stale leases, then takes a free lane with a first grant targeting GrantMin
// positions, capped by settings.MaxLaneGrant and the positions remaining in the lane. A lane with
// no lease row and no hand-out comes first, lowest number first; after that the lane closed longest
// ago, where a lane that has hand-outs but no lease row counts as closed at its newest hand-out.
// With no lane free it returns a service error, and the caller retries.
// lapsed is how many stale leases it closed first, for the same metric the sweep feeds.
func (s *SQL) OpenLaneLease(ctx context.Context, req options.LaneLeaseOpen) (lane.LeaseID, lane.Grant, int, error) {
	if req.Holder == "" {
		return lane.LeaseID{}, lane.Grant{}, 0, errors.NewInvalidArgumentError("lane lease holder must not be empty")
	}

	if req.GrantMin == 0 {
		return lane.LeaseID{}, lane.Grant{}, 0, errors.NewInvalidArgumentError("lane grant size must be at least 1")
	}

	if req.CloseAfter <= 0 {
		return lane.LeaseID{}, lane.Grant{}, 0, errors.NewInvalidArgumentError("lane lease close time must be positive")
	}

	var (
		id     lane.LeaseID
		grant  lane.Grant
		lapsed int
	)

	err := s.leaseTx(ctx, true, func(tx *sql.Tx) error {
		var err error

		// RetryTx may run this more than once; only the run that commits counts. Each run makes its
		// own lease id: an attempt can commit and still report a retriable error (a lost COMMIT
		// reply), and a retry that reused the id would leave two lane_lease rows under one id. Their
		// lapsed closes would then collide on lane_close's primary key and fail every sweep and open.
		if id, err = lane.NewLeaseID(); err != nil {
			return errors.NewProcessingError("could not make a lane lease id", err)
		}

		if lapsed, err = s.closeStale(ctx, tx, req.CloseAfter); err != nil {
			return err
		}

		chosen, err := s.chooseLeaseLane(ctx, tx)
		if err != nil {
			return err
		}

		grant, err = s.recordGrant(ctx, tx, id, chosen, min(req.GrantMin, settings.MaxLaneGrant), req.Holder, req.Height, lane.Settles{})
		if err != nil {
			return err
		}

		if _, err = s.laneExec(ctx, tx, openLeaseSQL,
			int(chosen), s.leaseParam(id), req.Holder,
		); err != nil {
			return errors.NewStorageError("could not record the lane lease", err)
		}

		if openLeaseAttemptHook != nil {
			return openLeaseAttemptHook(tx)
		}

		return nil
	})
	if err != nil {
		return lane.LeaseID{}, lane.Grant{}, 0, leaseError("could not open a lane lease", err)
	}

	return id, grant, lapsed, nil
}

// RenewLaneLease records a renewal at the database's now. With WantGrant it also records the next
// grant, targeting Rate × GrantDuration and at least GrantMin, capped by settings.MaxLaneGrant and
// the positions remaining in the lane, with the request's settles pair on its hand-out row. A lease
// that is closed, or is not the lane's current one, gets closed = true and nothing is written.
func (s *SQL) RenewLaneLease(ctx context.Context, req options.LaneLeaseRenew) (grant lane.Grant, granted, closed bool, err error) {
	if req.WantGrant && req.GrantMin == 0 {
		return lane.Grant{}, false, false, errors.NewInvalidArgumentError("lane grant size must be at least 1")
	}

	err = s.leaseTx(ctx, req.WantGrant, func(tx *sql.Tx) error {
		res, err := s.laneExec(ctx, tx, renewLeaseSQL,
			int(req.Lane), s.leaseParam(req.Lease),
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
		res, err := s.laneExec(ctx, tx, releaseLeaseSQL,
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

		if _, err = s.laneExec(ctx, tx, cleanCloseSQL,
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
// that commits while the sweep waits for the row is seen, and that lease stays open. A lapsed close
// records the lease's newest hand-out as its last position, and no settles pair: the holder never
// reported one.
func (s *SQL) closeStale(ctx context.Context, tx *sql.Tx, closeAfter time.Duration) (int, error) {
	var (
		rows *sql.Rows
		err  error
	)

	if s.engine == util.Postgres {
		rows, err = tx.QueryContext(ctx, closeStaleSQL.postgres, closeAfter.Microseconds())
	} else {
		rows, err = tx.QueryContext(ctx, closeStaleSQL.sqlite, sqliteOffset(-closeAfter))
	}

	if err != nil {
		return 0, errors.NewStorageError("could not close stale lane leases", err)
	}

	type lapsed struct {
		id   any
		lane int
	}

	var closed []lapsed

	for rows.Next() {
		var (
			l  lapsed
			id []byte
		)

		if err = rows.Scan(&id, &l.lane); err != nil {
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
		if _, err = s.laneExec(ctx, tx, lapsedCloseSQL,
			l.id, l.lane, closeLapsed,
		); err != nil {
			return 0, errors.NewStorageError("could not record a lapsed lane lease", err)
		}
	}

	return len(closed), nil
}

// chooseLeaseLane applies the lane choice rule. It runs under the hand-out lock.
func (s *SQL) chooseLeaseLane(ctx context.Context, tx *sql.Tx) (uint8, error) {
	leases := make(map[int]sql.NullTime, grantableLanes)

	rows, err := tx.QueryContext(ctx, `SELECT lane, closed_at FROM lane_lease`)
	if err != nil {
		return 0, errors.NewStorageError("could not read the lane leases", err)
	}

	for rows.Next() {
		var (
			l        int
			closedAt sql.NullTime
		)

		if err = rows.Scan(&l, &closedAt); err != nil {
			_ = rows.Close()
			return 0, errors.NewStorageError("could not read a lane lease", err)
		}

		leases[l] = closedAt
	}

	if err = rows.Close(); err != nil {
		return 0, errors.NewStorageError("could not read the lane leases", err)
	}

	if err = rows.Err(); err != nil {
		return 0, errors.NewStorageError("could not read the lane leases", err)
	}

	type candidate struct {
		lane   int
		since  time.Time
		leased bool
	}

	var candidates []candidate

	for l := 0; l < grantableLanes; l++ {
		closedAt, leased := leases[l]
		if leased {
			if closedAt.Valid {
				candidates = append(candidates, candidate{lane: l, since: closedAt.Time, leased: true})
			}

			continue // an open lease holds it
		}

		// A lane with no lease row may still have hand-outs from before the lease.
		st, err := readLaneState(ctx, tx, uint8(l)) //nolint:gosec // l is below 254
		if err != nil {
			return 0, err
		}

		if !st.used {
			// A lane never used comes first, lowest number first.
			return uint8(l), nil //nolint:gosec // l is below 254
		}

		if !st.exhausted {
			candidates = append(candidates, candidate{lane: l, since: st.handedAt})
		}
	}

	sort.SliceStable(candidates, func(a, b int) bool { return candidates[a].since.Before(candidates[b].since) })

	// A closed lane is checked for positions left only once it is the one to choose, so an open
	// reads one hand-out per lane only while lanes without a lease row remain.
	for _, c := range candidates {
		if c.leased {
			st, err := readLaneState(ctx, tx, uint8(c.lane)) //nolint:gosec // c.lane is below 254
			if err != nil {
				return 0, err
			}

			if st.exhausted {
				continue
			}
		}

		return uint8(c.lane), nil //nolint:gosec // c.lane is below 254
	}

	return 0, errors.NewServiceError("no free lane: all %d lanes hold an open lease or are exhausted", grantableLanes)
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

	if _, err = s.laneExec(ctx, tx, recordGrantSQL,
		int(l), lane.ToSigned(first), lane.ToSigned(last), holder, int64(height), s.leaseParam(id), settlesFirst(settles), settlesLastTaken(settles),
	); err != nil {
		return lane.Grant{}, errors.NewStorageError("could not record the lane grant", err)
	}

	return lane.Grant{Lane: l, First: first, Last: last}, nil
}

// grantSize targets two minutes of the reported rate, by default, and at least the minimum, capped
// by settings.MaxLaneGrant. It sizes on whole seconds of d, so a positive d under one second gives
// the capped minimum. recordGrant also caps the result at the positions remaining in the lane.
func grantSize(rate uint64, d time.Duration, minimum uint64) uint64 {
	minimum = min(minimum, settings.MaxLaneGrant)

	secs := uint64(d / time.Second)
	if secs == 0 || rate == 0 {
		return minimum
	}

	if rate > settings.MaxLaneGrant/secs {
		return settings.MaxLaneGrant
	}

	return max(minimum, rate*secs)
}

// laneExec runs a lease statement in the form for this database's engine. It passes the constant
// itself to ExecContext, not a string a function returned, so no query is built at run time.
func (s *SQL) laneExec(ctx context.Context, tx *sql.Tx, q engineSQL, args ...any) (sql.Result, error) {
	if s.engine == util.Postgres {
		return tx.ExecContext(ctx, q.postgres, args...)
	}

	return tx.ExecContext(ctx, q.sqlite, args...)
}

// engineSQL is one lease statement written out for each engine. They differ only in how they read
// the database clock: clock_timestamp() on Postgres, strftime on SQLite. Postgres uses
// clock_timestamp, not now, so a grant recorded after waiting for the hand-out lock is stamped after
// the grant it waited for. Each is a constant, so no
// statement is built from pieces at run time.
type engineSQL struct {
	postgres string
	sqlite   string
}

var (
	openLeaseSQL = engineSQL{
		postgres: `
			INSERT INTO lane_lease (lane, lease_id, holder, opened_at, renewed_at, closed_at)
			VALUES ($1, $2, $3, clock_timestamp(), clock_timestamp(), NULL)
			ON CONFLICT (lane) DO UPDATE SET
				lease_id = EXCLUDED.lease_id, holder = EXCLUDED.holder, opened_at = EXCLUDED.opened_at,
				renewed_at = EXCLUDED.renewed_at, closed_at = NULL`,
		sqlite: `
			INSERT INTO lane_lease (lane, lease_id, holder, opened_at, renewed_at, closed_at)
			VALUES ($1, $2, $3, strftime('%Y-%m-%d %H:%M:%f', 'now'), strftime('%Y-%m-%d %H:%M:%f', 'now'), NULL)
			ON CONFLICT (lane) DO UPDATE SET
				lease_id = EXCLUDED.lease_id, holder = EXCLUDED.holder, opened_at = EXCLUDED.opened_at,
				renewed_at = EXCLUDED.renewed_at, closed_at = NULL`,
	}
	renewLeaseSQL = engineSQL{
		postgres: `UPDATE lane_lease SET renewed_at = clock_timestamp() WHERE lane = $1 AND lease_id = $2 AND closed_at IS NULL`,
		sqlite:   `UPDATE lane_lease SET renewed_at = strftime('%Y-%m-%d %H:%M:%f', 'now') WHERE lane = $1 AND lease_id = $2 AND closed_at IS NULL`,
	}
	releaseLeaseSQL = engineSQL{
		postgres: `UPDATE lane_lease SET closed_at = clock_timestamp() WHERE lane = $1 AND lease_id = $2 AND closed_at IS NULL`,
		sqlite:   `UPDATE lane_lease SET closed_at = strftime('%Y-%m-%d %H:%M:%f', 'now') WHERE lane = $1 AND lease_id = $2 AND closed_at IS NULL`,
	}
	cleanCloseSQL = engineSQL{
		postgres: `
			INSERT INTO lane_close (lease_id, lane, kind, last, settles_first, settles_last_taken, closed_at)
			VALUES ($1, $2, $3, $4, $5, $6, clock_timestamp())`,
		sqlite: `
			INSERT INTO lane_close (lease_id, lane, kind, last, settles_first, settles_last_taken, closed_at)
			VALUES ($1, $2, $3, $4, $5, $6, strftime('%Y-%m-%d %H:%M:%f', 'now'))`,
	}
	// closeStaleSQL takes the close time as $1: microseconds on Postgres, an SQLite date modifier
	// on SQLite.
	closeStaleSQL = engineSQL{
		postgres: `
			UPDATE lane_lease SET closed_at = clock_timestamp()
			WHERE closed_at IS NULL AND renewed_at <= clock_timestamp() - $1::bigint * interval '1 microsecond'
			RETURNING lease_id, lane`,
		sqlite: `
			UPDATE lane_lease SET closed_at = strftime('%Y-%m-%d %H:%M:%f', 'now')
			WHERE closed_at IS NULL AND renewed_at <= strftime('%Y-%m-%d %H:%M:%f', 'now', $1)
			RETURNING lease_id, lane`,
	}
	lapsedCloseSQL = engineSQL{
		postgres: `
			INSERT INTO lane_close (lease_id, lane, kind, last, settles_first, settles_last_taken, closed_at)
			SELECT $1, $2, $3, h.last, NULL, NULL, clock_timestamp()
			FROM lane_handout h WHERE h.lane = $2 AND h.lease_id = $1
			ORDER BY h.first DESC LIMIT 1`,
		sqlite: `
			INSERT INTO lane_close (lease_id, lane, kind, last, settles_first, settles_last_taken, closed_at)
			SELECT $1, $2, $3, h.last, NULL, NULL, strftime('%Y-%m-%d %H:%M:%f', 'now')
			FROM lane_handout h WHERE h.lane = $2 AND h.lease_id = $1
			ORDER BY h.first DESC LIMIT 1`,
	}
	recordGrantSQL = engineSQL{
		postgres: `
			INSERT INTO lane_handout (lane, first, last, holder, height, handed_at, lease_id, settles_first, settles_last_taken)
			VALUES ($1, $2, $3, $4, $5, clock_timestamp(), $6, $7, $8)`,
		sqlite: `
			INSERT INTO lane_handout (lane, first, last, holder, height, handed_at, lease_id, settles_first, settles_last_taken)
			VALUES ($1, $2, $3, $4, $5, strftime('%Y-%m-%d %H:%M:%f', 'now'), $6, $7, $8)`,
	}
)

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

// readLaneState reads the latest hand-out in a lane through the primary key.
func readLaneState(ctx context.Context, tx *sql.Tx, l uint8) (laneState, error) {
	var (
		last     int64
		handedAt time.Time
	)

	err := tx.QueryRowContext(ctx,
		`SELECT last, handed_at FROM lane_handout WHERE lane = $1 ORDER BY first DESC LIMIT 1`,
		int(l),
	).Scan(&last, &handedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return laneState{}, nil
	}

	if err != nil {
		return laneState{}, errors.NewStorageError("could not read the latest lane hand-out", err)
	}

	lastPos := lane.FromSigned(last)

	return laneState{used: true, last: lastPos, handedAt: handedAt, exhausted: lastPos >= lane.MaxPosition}, nil
}
