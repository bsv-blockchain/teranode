// Package lanestamp wraps a UTXO store so that every record it creates gets a
// lane position (util/lane) from the process's allocator.
//
// The daemon wraps its one UTXO store with it when utxostore_lanePositions is
// on, so every service in the process that creates records (the validator,
// the validator inside propagation, subtree validation, block validation and
// legacy sync through it, block assembly's coinbase and reorg re-creates)
// writes lane positions from one allocator, which is one lane per process,
// with no change at any call site. With the flag off the store is not
// wrapped, so nothing changes.
package lanestamp

import (
	"context"
	"sync"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/meta"
	"github.com/bsv-blockchain/teranode/ulogger"
)

// startWait bounds how long the background start waits for the first range
// before it logs that none came. The allocator keeps trying after that.
const startWait = 30 * time.Second

// Positions hands out lane positions. *lane.Allocator implements it.
type Positions interface {
	Start(ctx context.Context) error
	Next() (uint64, bool)
	Fill(dst []uint64) int
	Close()
}

// Store is a utxo.Store that adds a lane position to every create.
//
// The allocator starts on the first create, not when the store is built. Most
// services that hold the UTXO store never create a record (asset, RPC, alert,
// block persister, pruner), and starting at build time would give each of
// those processes a lane it never uses. The cost is that the first creates of
// a process go out without a lane position while the first range is fetched.
type Store struct {
	utxo.Store
	positions Positions
	logger    ulogger.Logger
	startOnce sync.Once
}

// New wraps store so every record it creates takes the next lane position.
func New(store utxo.Store, positions Positions, logger ulogger.Logger) *Store {
	if logger == nil {
		logger = ulogger.TestLogger{}
	}

	return &Store{Store: store, positions: positions, logger: logger}
}

func (s *Store) start() {
	s.startOnce.Do(func() {
		go func() {
			ctx, cancel := context.WithTimeout(context.Background(), startWait)
			defer cancel()

			if err := s.positions.Start(ctx); err != nil {
				s.logger.Warnf("[lanestamp] new records go out without a lane position until a range arrives: %v", err)
			}
		}()
	})
}

// SpendAndCreate gives the record the next lane position unless the call
// creates nothing. With no range in hand the record is created without one.
func (s *Store) SpendAndCreate(ctx context.Context, tx *bt.Tx, blockHeight uint32, opts ...utxo.CreateOption) (*meta.Data, []*utxo.Spend, error) {
	s.start()

	var o utxo.CreateOptions
	for _, opt := range opts {
		opt(&o)
	}

	if !o.SpendOnly {
		if v, ok := s.positions.Next(); ok {
			opts = append(opts[:len(opts):len(opts)], utxo.WithLanePosition(v))
		}
	}

	return s.Store.SpendAndCreate(ctx, tx, blockHeight, opts...)
}

// SpendAndCreateMulti gives each transaction of the list the next lane
// position, in list order. Positions the allocator cannot fill are left as
// lane.None, and those records are created without one.
func (s *Store) SpendAndCreateMulti(ctx context.Context, txs []*bt.Tx, blockHeight uint32, opts ...utxo.CreateOption) ([]utxo.SpendAndCreateMultiResult, error) {
	s.start()

	if len(txs) > 0 {
		positions := make([]uint64, len(txs))
		if s.positions.Fill(positions) > 0 {
			opts = append(opts[:len(opts):len(opts)], utxo.WithLanePositions(positions))
		}
	}

	return s.Store.SpendAndCreateMulti(ctx, txs, blockHeight, opts...)
}

// Close stops the allocator's background fetch, then closes the store.
func (s *Store) Close(ctx context.Context) error {
	s.positions.Close()

	return s.Store.Close(ctx)
}
