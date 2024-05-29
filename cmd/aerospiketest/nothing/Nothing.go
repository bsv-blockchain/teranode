package nothing

import (
	"context"
	"sync"
	"time"

	"github.com/bitcoin-sv/ubsv/errors"
	"github.com/bitcoin-sv/ubsv/ulogger"
	"github.com/libsv/go-bt/v2"
)

type Nothing struct {
	logger ulogger.Logger
}

func New(logger ulogger.Logger) *Nothing {
	return &Nothing{
		logger: logger,
	}
}

func (s *Nothing) Storer(ctx context.Context, id int, txCount int, wg *sync.WaitGroup, spenderCh chan *bt.Tx, counterCh chan int) {
	wg.Add(1)

	go func() {
		defer wg.Done()

		var counter int
		var totalTime time.Duration

		defer func() {
			counterCh <- counter
		}()

		defer func() {
			if counter > 0 {
				s.logger.Infof("Storer %d: %d txs in %s", id, counter, totalTime)
			}
		}()

		for i := 0; i < txCount; i++ {
			elapsed, err := writeToChannel(ctx, spenderCh, &bt.Tx{})
			if err != nil {
				s.logger.Errorf("Storer %d: %v", id, err)
				return
			}
			counter++
			totalTime += elapsed
		}
	}()
}

func (s *Nothing) Spender(ctx context.Context, wg *sync.WaitGroup, spenderCh chan *bt.Tx, deleterCh chan *bt.Tx, counterCh chan int) {
	wg.Add(1)

	go func() {
		defer wg.Done()

		var counter int
		var totalReadTime time.Duration
		var totalWriteTime time.Duration

		defer func() {
			counterCh <- counter
		}()

		defer func() {
			if counter > 0 {
				s.logger.Infof("Spender read: %d txs in %s", counter, totalReadTime)
				s.logger.Infof("Spender write: %d txs in %s", counter, totalWriteTime)
			}
		}()

		for {
			hash, elapsedRead, err := readFromChannel(ctx, spenderCh)
			if err != nil {
				if errors.Is(err, ErrChannelClosed) {
					s.logger.Infof("Spender: channel closed")
					return
				}

				s.logger.Errorf("Spender: %v", err)
				return
			} else {
				totalReadTime += elapsedRead

				counter++

				elapsedWrite, err := writeToChannel(ctx, deleterCh, hash)
				if err != nil {
					s.logger.Errorf("Spender: %v", err)
					return
				}
				totalWriteTime += elapsedWrite
			}
		}
	}()
}

func (s *Nothing) Deleter(_ context.Context, wg *sync.WaitGroup, deleteCh chan *bt.Tx, counterCh chan int) {
	wg.Add(1)

	go func() {
		defer wg.Done()

		var counter int
		var totalTime time.Duration

		defer func() {
			counterCh <- counter
		}()

		defer func() {
			if counter > 0 {
				s.logger.Infof("Deleter: %d txs in %s", counter, totalTime)
			}
		}()

		for {
			_, elapsed, err := readFromChannel(context.Background(), deleteCh)
			if err != nil {
				if errors.Is(err, ErrChannelClosed) {
					s.logger.Infof("Deleter: channel closed")
					return
				}
				s.logger.Warnf("Deleter: error: %v", err)
				continue
			}

			totalTime += elapsed
			counter++
		}
	}()
}

var ErrTimeout = errors.New(errors.ERR_PROCESSING, "timeout")
var ErrChannelClosed = errors.New(errors.ERR_PROCESSING, "channel closed")

func writeToChannel[T any](ctx context.Context, ch chan *T, hash *T) (time.Duration, error) {
	start := time.Now()

	select {
	case ch <- hash:
		return time.Since(start), nil
	case <-ctx.Done():
		return 0, ctx.Err()
	case <-time.After(3 * time.Second):
		return 0, ErrTimeout
	}
}

func readFromChannel[T any](ctx context.Context, ch chan *T) (*T, time.Duration, error) {
	start := time.Now()

	select {
	case hash, ok := <-ch:
		if !ok {
			return nil, 0, ErrChannelClosed
		}
		return hash, time.Since(start), nil
	case <-ctx.Done():
		return nil, 0, ctx.Err()
	case <-time.After(3 * time.Second):
		return nil, 0, ErrTimeout
	}
}
