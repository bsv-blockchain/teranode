package recoverreplayedtransactions

import (
	"context"
	"time"

	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/pkg/replayrecovery"
)

type idleChain interface {
	GetState(context.Context, string) ([]byte, error)
	GetBestBlockHeader(context.Context) (*model.BlockHeader, *model.BlockHeaderMeta, error)
}

type idleGuard struct {
	chain idleChain
	tip   replayrecovery.Tip
}

func persistedIdle(ctx context.Context, chain idleChain) error {
	state, err := chain.GetState(ctx, "fsm_state")
	if err != nil {
		return commandError("read persisted FSM state: %w", err)
	}
	if string(state) != "IDLE" {
		return commandError("recovery requires persisted IDLE; found %q", state)
	}
	return nil
}

func newIdleGuard(ctx context.Context, chain idleChain) (*idleGuard, error) {
	if chain == nil {
		return nil, commandError("blockchain reader is required")
	}
	if err := persistedIdle(ctx, chain); err != nil {
		return nil, err
	}
	header, meta, err := chain.GetBestBlockHeader(ctx)
	if err != nil {
		return nil, err
	}
	if header == nil || meta == nil {
		return nil, commandError("local canonical tip unavailable")
	}
	g := &idleGuard{chain: chain, tip: replayrecovery.Tip{Hash: header.Hash().String(), Height: meta.Height}}
	return g, g.Check(ctx)
}

func (g *idleGuard) Check(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := persistedIdle(ctx, g.chain); err != nil {
		return err
	}
	header, meta, err := g.chain.GetBestBlockHeader(ctx)
	if err != nil {
		return err
	}
	if header == nil || meta == nil || header.Hash().String() != g.tip.Hash || meta.Height != g.tip.Height {
		return commandError("canonical tip changed during recovery")
	}
	return persistedIdle(ctx, g.chain)
}

// watch cancels ongoing work on a failed state read. This detects changes; the
// operator must still stop all writers because RPC checks are not a write lock.
func (g *idleGuard) watch(ctx context.Context, cancel context.CancelCauseFunc) <-chan struct{} {
	done := make(chan struct{})
	go func() {
		defer close(done)
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if err := g.Check(ctx); err != nil {
					cancel(err)
					return
				}
			}
		}
	}()
	return done
}
