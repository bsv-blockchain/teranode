package sql

import (
	"context"
	"database/sql"
	"sync"
	"time"

	"github.com/bsv-blockchain/teranode/errors"
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

// laneState is the latest hand-out in one lane.
type laneState struct {
	used      bool
	last      uint64
	holder    string
	handedAt  time.Time
	exhausted bool
}

// ReserveLaneRange records and returns the next range of positions in a lane.
//
// A lane's range always starts above the highest position ever handed out in it.
// The requested lane is granted when it was never used, when its latest
// hand-out went to the same holder, or when it has been idle for FreeAfter.
// Otherwise a free lane is assigned: never-used lanes first, lowest number
// first, then the lane idle longest (lowest number on a tie). Exhausted lanes
// are never handed out. The table is insert-only.
func (s *SQL) ReserveLaneRange(ctx context.Context, req options.LaneRangeRequest) (lane.Range, error) {
	if req.Size == 0 {
		return lane.Range{}, errors.NewInvalidArgumentError("lane range size must be at least 1")
	}

	if req.Holder == "" {
		return lane.Range{}, errors.NewInvalidArgumentError("lane range holder must not be empty")
	}

	if s.engine != util.Postgres {
		laneHandoutMu.Lock()
		defer laneHandoutMu.Unlock()
	}

	var result lane.Range

	err := s.db.RetryTx(ctx, &sql.TxOptions{}, func(tx *sql.Tx) error {
		if s.engine == util.Postgres {
			if _, err := tx.ExecContext(ctx, `SELECT pg_advisory_xact_lock($1)`, laneHandoutLockID); err != nil {
				return errors.NewStorageError("could not take the lane hand-out lock", err)
			}
		}

		chosen, err := chooseLane(ctx, tx, req)
		if err != nil {
			return err
		}

		state, err := readLaneState(ctx, tx, chosen)
		if err != nil {
			return err
		}

		first := lane.FirstPosition
		if state.used {
			first = state.last + 1
		}

		last := lane.MaxPosition
		if req.Size-1 < lane.MaxPosition-first {
			last = first + req.Size - 1
		}

		if _, err = tx.ExecContext(ctx,
			`INSERT INTO lane_handout (lane, first, last, holder, height, handed_at) VALUES ($1, $2, $3, $4, $5, $6)`,
			int(chosen), lane.ToSigned(first), lane.ToSigned(last), req.Holder, int64(req.Height), req.Now.UTC(),
		); err != nil {
			return errors.NewStorageError("could not record the lane hand-out", err)
		}

		result = lane.Range{Lane: chosen, First: first, Last: last}

		return nil
	})
	if err != nil {
		if errors.Is(err, errors.ErrStorageError) || errors.Is(err, errors.ErrServiceError) {
			return lane.Range{}, err
		}

		return lane.Range{}, errors.NewStorageError("could not reserve a lane range", err)
	}

	return result, nil
}

// chooseLane applies the lane choice rule inside the hand-out transaction.
func chooseLane(ctx context.Context, tx *sql.Tx, req options.LaneRangeRequest) (uint8, error) {
	if req.HasLane {
		st, err := readLaneState(ctx, tx, req.WantLane)
		if err != nil {
			return 0, err
		}

		if !st.exhausted && (!st.used || st.holder == req.Holder || !req.Now.Before(st.handedAt.Add(req.FreeAfter))) {
			return req.WantLane, nil
		}
	}

	var (
		best      = -1
		bestSince time.Time
	)

	for l := 0; l <= lane.MaxLane; l++ {
		st, err := readLaneState(ctx, tx, uint8(l)) //nolint:gosec // l is at most 255
		if err != nil {
			return 0, err
		}

		if st.exhausted {
			continue
		}

		if !st.used {
			// A never-used lane beats any used one; the lowest number wins.
			return uint8(l), nil //nolint:gosec // l is at most 255
		}

		if req.Now.Before(st.handedAt.Add(req.FreeAfter)) {
			continue
		}

		if best < 0 || st.handedAt.Before(bestSince) {
			best, bestSince = l, st.handedAt
		}
	}

	if best < 0 {
		return 0, errors.NewServiceError("no free lane: all %d lanes had a hand-out in the last %s or are exhausted", lane.MaxLane+1, req.FreeAfter)
	}

	return uint8(best), nil //nolint:gosec // best is at most 255
}

// readLaneState reads the latest hand-out in a lane through the primary key.
func readLaneState(ctx context.Context, tx *sql.Tx, l uint8) (laneState, error) {
	var (
		last     int64
		holder   string
		handedAt time.Time
	)

	err := tx.QueryRowContext(ctx,
		`SELECT last, holder, handed_at FROM lane_handout WHERE lane = $1 ORDER BY first DESC LIMIT 1`,
		int(l),
	).Scan(&last, &holder, &handedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return laneState{}, nil
	}

	if err != nil {
		return laneState{}, errors.NewStorageError("could not read the latest lane hand-out", err)
	}

	lastPos := lane.FromSigned(last)

	return laneState{used: true, last: lastPos, holder: holder, handedAt: handedAt, exhausted: lastPos >= lane.MaxPosition}, nil
}
