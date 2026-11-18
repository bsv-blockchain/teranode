package utxo

import (
	"context"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/util/lane"
)

// LanePositionSource hands out lane positions (util/lane), one per record. Next waits a bounded time
// for a position, then fails with a service-unavailable error, and the create fails with it: no
// record goes out without a position. *lane.Allocator implements it.
type LanePositionSource interface {
	Next(ctx context.Context) (uint64, error)
}

// LanePositionTaker is a store that gives each record it creates a position from a source. A store
// takes the position before it spends or opens a database transaction, so a wait for one holds
// nothing, and a create that cannot get one fails having written nothing. A store that plans a write
// again after a rollback keeps the positions it took. A position taken for a write that is then
// refused, such as a transaction that exists already, is burned, which is safe: no record carries it.
// There is no wrapper around the store, because a wrapper has to scan and extend the options on
// every create, and it hides what the wrapped store can do.
//
// Find it with As, since the store may sit inside a logging wrapper. Set the source before the
// first create, because it is not guarded against a concurrent create.
type LanePositionTaker interface {
	SetLanePositionSource(source LanePositionSource)

	// TakeLanePosition returns the position for the record that o describes; see LanePositions.Take.
	TakeLanePosition(ctx context.Context, o *CreateOptions) (uint64, error)
}

// LanePositions is the part of a store that implements LanePositionTaker. With no source set, which
// is every store with lane positions off, a create pays one nil check.
type LanePositions struct {
	source LanePositionSource
}

// SetLanePositionSource implements LanePositionTaker.
func (l *LanePositions) SetLanePositionSource(source LanePositionSource) {
	l.source = source
}

// Take returns the position for the record that o describes: the caller's own if it named one,
// zero with no source, otherwise the source's next. With a source set, an invalid explicit or
// returned position fails; it is never replaced by a fresh allocation or written as zero.
func (l *LanePositions) Take(ctx context.Context, o *CreateOptions) (uint64, error) {
	if l.source == nil {
		return o.LanePosition, nil
	}

	position := o.LanePosition
	if position == lane.None {
		var err error
		position, err = l.source.Next(ctx)
		if err != nil {
			return lane.None, err
		}
	}

	if !lane.Valid(position) {
		return lane.None, errors.NewProcessingError("lane positions enabled: invalid lane position %d", position)
	}

	return position, nil
}

// TakeLanePosition implements LanePositionTaker.
func (l *LanePositions) TakeLanePosition(ctx context.Context, o *CreateOptions) (uint64, error) {
	return l.Take(ctx, o)
}

// TakeLanePositions takes n positions, one per transaction of a list, or returns nil with no source.
func (l *LanePositions) TakeLanePositions(ctx context.Context, n int) ([]uint64, error) {
	if l.source == nil {
		return nil, nil
	}

	positions := make([]uint64, n)

	for i := range positions {
		p, err := l.Take(ctx, &CreateOptions{})
		if err != nil {
			return nil, err
		}

		positions[i] = p
	}

	return positions, nil
}

// LanePositionHolder is a store whose lane position source can be started before the first create
// and asked whether it can give positions. A validator starts it when it is built, so it holds a
// lease before it takes traffic, and is not ready without one.
type LanePositionHolder interface {
	// StartLanePositions starts the source, if it has a Start. It does not wait for a lease.
	StartLanePositions(ctx context.Context) error

	// LanePositionsReady returns nil with no source, or when the source has nothing to report, or
	// when the source is ready.
	LanePositionsReady() error
}

// StartLanePositions implements LanePositionHolder.
func (l *LanePositions) StartLanePositions(ctx context.Context) error {
	if starter, ok := l.source.(interface{ Start(context.Context) error }); ok {
		return starter.Start(ctx)
	}

	return nil
}

// LanePositionsReady implements LanePositionHolder.
func (l *LanePositions) LanePositionsReady() error {
	if ready, ok := l.source.(interface{ Ready() error }); ok {
		return ready.Ready()
	}

	return nil
}

// WithTakenLanePosition returns opts with the position appended, without writing into the caller's
// backing array. A position of zero leaves opts as they are.
func WithTakenLanePosition(opts []CreateOption, position uint64) []CreateOption {
	if position == 0 {
		return opts
	}

	return append(opts[:len(opts):len(opts)], WithLanePosition(position))
}

// LaneSchemaPreparer is a store whose database needs lane columns installed before the historical
// conversion numbers anything. PrepareLaneSchema installs them, nullable and empty, at any lane
// migration step, off included; it writes no row and is safe to run again. Restart every process
// of the node afterwards so each one sees the same schema. Find it with As. Aerospike has no
// schema to install: its lane bins are written by the records that carry them.
type LaneSchemaPreparer interface {
	PrepareLaneSchema(ctx context.Context) error
}
