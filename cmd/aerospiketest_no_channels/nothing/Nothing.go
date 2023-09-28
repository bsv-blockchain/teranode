package nothing

import (
	"context"
	"sync"
	"time"

	"github.com/ordishs/go-utils"
)

type Nothing struct {
	logger utils.Logger
}

func New(logger utils.Logger) *Nothing {
	return &Nothing{
		logger: logger,
	}
}

func (s *Nothing) Work(ctx context.Context, id int, txCount int, wg *sync.WaitGroup, counterCh chan int) {
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
			counter++
		}
	}()
}
