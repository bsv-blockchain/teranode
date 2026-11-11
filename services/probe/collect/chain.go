package collect

import (
	"context"
	"sort"
	"sync"
	"time"

	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/services/probe/guard"
	"github.com/bsv-blockchain/teranode/services/probe/snapshot"
)

const (
	chainCallTimeout    = 3 * time.Second
	chainMaxWaiting     = 20
	chainPersisterLimit = 25
	chainBacklogEvery   = 10 * time.Second
)

type Chain struct {
	bc  blockchain.ClientI
	now func() time.Time

	mu         sync.Mutex
	haveLast   bool
	lastAt     time.Time
	lastWait   []uint32
	lastCount  snapshot.Reading
	lastBehind snapshot.Reading
}

func NewChain(bc blockchain.ClientI) *Chain {
	return &Chain{bc: bc, now: time.Now}
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
		return append([]uint32{}, c.lastWait...), c.lastCount, c.lastBehind
	}

	c.haveLast = true
	c.lastAt = now

	notPersisted, err := c.notPersisted(ctx)
	if err == nil {
		c.setBacklog(notPersisted)

		return append([]uint32{}, c.lastWait...), c.lastCount, c.lastBehind
	}

	c.lastWait = []uint32{}
	c.lastCount = snapshot.Reading{Status: snapshot.StatusNoData, Note: "no data"}
	c.lastBehind = snapshot.Reading{Status: snapshot.StatusNoData, Note: "no data"}

	return []uint32{}, c.lastCount, c.lastBehind
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

func (c *Chain) fsmState(ctx context.Context) (string, error) {
	ctx, cancel := context.WithTimeout(ctx, chainCallTimeout)
	defer cancel()

	state, err := c.bc.GetFSMCurrentState(ctx)
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
