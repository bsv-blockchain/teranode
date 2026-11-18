package collect

import (
	"context"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/pkg/urlutil"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/services/probe/guard"
	"github.com/bsv-blockchain/teranode/services/probe/snapshot"
	"github.com/bsv-blockchain/teranode/ulogger"
)

const (
	chainCallTimeout    = 3 * time.Second
	chainMaxWaiting     = 20
	chainPersisterLimit = 25
	chainBacklogEvery   = 10 * time.Second
	noteBacklogStale    = "backlog unavailable"
)

type Chain struct {
	bc     blockchain.ClientI
	logger ulogger.Logger
	now    func() time.Time

	mu         sync.Mutex
	haveLast   bool
	lastAt     time.Time
	goodAt     time.Time
	failing    bool
	lastWait   []uint32
	lastCount  snapshot.Reading
	lastBehind snapshot.Reading
}

func NewChain(bc blockchain.ClientI, logger ulogger.Logger) *Chain {
	return &Chain{bc: bc, logger: logger, now: time.Now}
}

func (c *Chain) Name() string {
	return "chain"
}

func (c *Chain) Collect(ctx context.Context) (snapshot.Patch, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	fsm, err := c.fsmState(ctx)
	if err != nil {
		return nil, err
	}

	tip, err := c.tipHeight(ctx)
	if err != nil {
		return nil, err
	}

	waiting, count, behind := c.backlog(ctx)

	return func(s *snapshot.Snapshot) {
		s.FSM = fsm
		s.Tip = tip
		s.Chain.BlocksWaiting = waiting
		s.Chain.BlocksWaitingCount = count
		s.Chain.PersisterBehind = behind
	}, nil
}

func (c *Chain) backlog(ctx context.Context) ([]uint32, snapshot.Reading, snapshot.Reading) {
	c.mu.Lock()
	defer c.mu.Unlock()

	now := c.now()
	if c.haveLast && now.Sub(c.lastAt) < chainBacklogEvery {
		return c.backlogReadings(now)
	}

	c.haveLast = true
	c.lastAt = now

	notPersisted, err := c.notPersisted(ctx)
	if err != nil {
		if !c.failing {
			c.logger.Warnf("[Probe] chain backlog has no data: %s", urlutil.RedactText(strings.Join(strings.Fields(err.Error()), " ")))
		}

		c.failing = true

		return c.backlogReadings(now)
	}

	if c.failing {
		c.logger.Infof("[Probe] chain backlog is back")
	}

	c.failing = false
	c.goodAt = now
	c.setBacklog(notPersisted)

	return c.backlogReadings(now)
}

func (c *Chain) backlogReadings(now time.Time) ([]uint32, snapshot.Reading, snapshot.Reading) {
	waiting := append([]uint32{}, c.lastWait...)
	count, behind := c.lastCount, c.lastBehind

	if c.failing {
		var age float64
		if !c.goodAt.IsZero() {
			age = now.Sub(c.goodAt).Seconds()
		}

		count, behind = staleReading(count, age), staleReading(behind, age)
	}

	return waiting, count, behind
}

func staleReading(r snapshot.Reading, age float64) snapshot.Reading {
	r.Status = snapshot.StatusNoData
	r.Note = noteBacklogStale
	r.AgeSec = age

	return r
}

func (c *Chain) setBacklog(notPersisted []*model.Block) {
	behind := snapshot.Reading{Value: float64(len(notPersisted)), Status: snapshot.StatusOK}
	if len(notPersisted) >= chainPersisterLimit {
		behind.Value = chainPersisterLimit
		behind.Note = "25 or more"
	}

	count := snapshot.Reading{Value: float64(countHeights(notPersisted)), Status: snapshot.StatusOK}
	if len(notPersisted) >= chainPersisterLimit {
		count.Note = behind.Note
	}

	c.lastWait = waitingHeights(notPersisted)
	c.lastCount = count
	c.lastBehind = behind
}

func (c *Chain) Events(ctx context.Context, rec guard.Recorder) <-chan struct{} {
	out := make(chan struct{}, 1)

	notifications, err := c.bc.Subscribe(ctx, blockchain.SubscriberProbe)
	if err != nil {
		go func() {
			defer guard.Recover(rec, "chain events")

			<-ctx.Done()
			close(out)
		}()

		return out
	}

	go func() {
		defer guard.Recover(rec, "chain events")
		defer close(out)

		for {
			select {
			case <-ctx.Done():
				return
			case n, ok := <-notifications:
				if !ok {
					return
				}

				if n == nil || (n.Type != model.NotificationType_FSMState && n.Type != model.NotificationType_Block) {
					continue
				}

				select {
				case out <- struct{}{}:
				default:
				}
			}
		}
	}()

	return out
}

type fsmFetcher interface {
	FetchFSMCurrentState(ctx context.Context) (*blockchain.FSMStateType, error)
}

func (c *Chain) fsmState(ctx context.Context) (string, error) {
	ctx, cancel := context.WithTimeout(ctx, chainCallTimeout)
	defer cancel()

	read := c.bc.GetFSMCurrentState
	if f, ok := c.bc.(fsmFetcher); ok {
		read = f.FetchFSMCurrentState
	}

	state, err := read(ctx)
	if err != nil {
		return "", err
	}

	return state.String(), nil
}

func (c *Chain) tipHeight(ctx context.Context) (uint32, error) {
	ctx, cancel := context.WithTimeout(ctx, chainCallTimeout)
	defer cancel()

	_, meta, err := c.bc.GetBestBlockHeader(ctx)
	if err != nil {
		return 0, err
	}

	return meta.Height, nil
}

func (c *Chain) notPersisted(ctx context.Context) ([]*model.Block, error) {
	ctx, cancel := context.WithTimeout(ctx, chainCallTimeout)
	defer cancel()

	return c.bc.GetBlocksNotPersisted(ctx, chainPersisterLimit)
}

func waitingHeights(groups ...[]*model.Block) []uint32 {
	seen := make(map[uint32]struct{})
	heights := make([]uint32, 0, chainMaxWaiting)

	for _, blocks := range groups {
		for _, b := range blocks {
			if b == nil {
				continue
			}

			if _, ok := seen[b.Height]; ok {
				continue
			}

			seen[b.Height] = struct{}{}
			heights = append(heights, b.Height)
		}
	}

	sort.Slice(heights, func(i, j int) bool { return heights[i] < heights[j] })

	if len(heights) > chainMaxWaiting {
		heights = heights[:chainMaxWaiting]
	}

	return heights
}

func countHeights(groups ...[]*model.Block) int {
	seen := make(map[uint32]struct{})

	for _, blocks := range groups {
		for _, b := range blocks {
			if b != nil {
				seen[b.Height] = struct{}{}
			}
		}
	}

	return len(seen)
}
