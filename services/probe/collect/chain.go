package collect

import (
	"context"
	"sort"
	"time"

	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/services/p2p"
	"github.com/bsv-blockchain/teranode/services/probe/snapshot"
)

const (
	chainCallTimeout    = 3 * time.Second
	chainMaxWaiting     = 20
	chainPersisterLimit = 1000
)

type Chain struct {
	bc   blockchain.ClientI
	p2pc p2p.ClientI
}

func NewChain(bc blockchain.ClientI, p2pc p2p.ClientI) *Chain {
	return &Chain{bc: bc, p2pc: p2pc}
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

	peerTip, err := c.peerTip(ctx)
	if err != nil {
		return nil, err
	}

	notPersisted, err := c.notPersisted(ctx)
	if err != nil {
		return nil, err
	}

	minedNotSet, err := c.minedNotSet(ctx)
	if err != nil {
		return nil, err
	}

	waiting := waitingHeights(notPersisted, minedNotSet)

	behind := snapshot.Reading{Value: float64(len(notPersisted)), Status: snapshot.StatusOK}
	if len(notPersisted) > 0 {
		behind.Status = snapshot.StatusWarn
	}

	return func(s *snapshot.Snapshot) {
		s.FSM = fsm
		s.Tip = tip
		s.PeerTip = peerTip
		s.Chain.BlocksWaiting = waiting
		s.Chain.PersisterBehind = behind
	}, nil
}

func (c *Chain) Events(ctx context.Context) <-chan struct{} {
	out := make(chan struct{}, 1)

	notifications, err := c.bc.Subscribe(ctx, blockchain.SubscriberProbe)
	if err != nil {
		go func() {
			<-ctx.Done()
			close(out)
		}()

		return out
	}

	go func() {
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

func (c *Chain) peerTip(ctx context.Context) (uint32, error) {
	ctx, cancel := context.WithTimeout(ctx, chainCallTimeout)
	defer cancel()

	peers, err := c.p2pc.GetPeers(ctx)
	if err != nil {
		return 0, err
	}

	var tip uint32

	for _, p := range peers {
		if p == nil {
			continue
		}

		if h := p.DisplayHeight(); h > tip {
			tip = h
		}

		if p.Height > tip {
			tip = p.Height
		}
	}

	return tip, nil
}

func (c *Chain) notPersisted(ctx context.Context) ([]*model.Block, error) {
	ctx, cancel := context.WithTimeout(ctx, chainCallTimeout)
	defer cancel()

	return c.bc.GetBlocksNotPersisted(ctx, chainPersisterLimit)
}

func (c *Chain) minedNotSet(ctx context.Context) ([]*model.Block, error) {
	ctx, cancel := context.WithTimeout(ctx, chainCallTimeout)
	defer cancel()

	return c.bc.GetBlocksMinedNotSet(ctx)
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
