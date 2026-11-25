package sql

import (
	"context"
	"math"
	"time"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/util/lane"
)

// capGrantPage is how many hand-out rows one step of a lane's walk reads.
const capGrantPage = 256

// laneCapGrantSQL reads a lane's hand-out rows below a position, newest first, through the
// primary key (lane, first). lane_handout has no index on height, so the walk goes down the key
// and stops at the first row below the height: it reads the grants handed out at or above the
// height, and at most one page of capGrantPage rows past them.
const laneCapGrantSQL = `SELECT first, height, handed_at FROM lane_handout WHERE lane = $1 AND first < $2 ORDER BY first DESC LIMIT $3`

// LaneCapGrants returns, for each lane with one, the first grant handed out at or above height,
// by lane.
func (s *SQL) LaneCapGrants(ctx context.Context, height uint32) ([]lane.CapGrant, error) {
	var out []lane.CapGrant

	for l := 0; l <= lane.MaxLane; l++ {
		g, ok, err := s.laneCapGrant(ctx, uint8(l), height)
		if err != nil {
			return nil, err
		}

		if ok {
			out = append(out, g)
		}
	}

	return out, nil
}

// laneCapGrant walks lane l's grants from the newest down and returns the oldest one at or above
// height, or false when the newest is below it or the lane has none.
func (s *SQL) laneCapGrant(ctx context.Context, l uint8, height uint32) (lane.CapGrant, bool, error) {
	var (
		g     lane.CapGrant
		found bool
	)

	before := int64(math.MaxInt64)

	for {
		rows, err := s.db.QueryContext(ctx, laneCapGrantSQL, int(l), before, capGrantPage)
		if err != nil {
			return g, false, errors.NewStorageError("read lane %d hand-outs", l, err)
		}

		n, stop := 0, false

		for rows.Next() {
			var (
				first, h int64
				handedAt time.Time
			)

			if err = rows.Scan(&first, &h, &handedAt); err != nil {
				_ = rows.Close()

				return g, false, errors.NewStorageError("scan lane %d hand-out", l, err)
			}

			n++
			before = first

			if h < int64(height) {
				stop = true

				break
			}

			if first <= 0 || first > int64(lane.MaxPosition) || h > math.MaxUint32 {
				_ = rows.Close()

				return g, false, errors.NewProcessingError("lane %d hand-out at %d holds out-of-range values", l, first)
			}

			g, found = lane.CapGrant{Lane: l, First: uint64(first), Height: uint32(h), HandedAt: handedAt}, true
		}

		closeErr := rows.Close()

		if err = rows.Err(); err != nil {
			return g, false, errors.NewStorageError("read lane %d hand-outs", l, err)
		}

		if closeErr != nil {
			return g, false, errors.NewStorageError("read lane %d hand-outs", l, closeErr)
		}

		if stop || n < capGrantPage {
			return g, found, nil
		}
	}
}
