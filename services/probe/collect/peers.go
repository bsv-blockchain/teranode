package collect

import (
	"context"

	"github.com/bsv-blockchain/teranode/services/p2p"
	"github.com/bsv-blockchain/teranode/services/probe/snapshot"
)

const notePeersNone = "no peers"

type Peers struct {
	p2pc p2p.ClientI
}

func NewPeers(p2pc p2p.ClientI) *Peers {
	return &Peers{p2pc: p2pc}
}

func (p *Peers) Name() string {
	return "p2p"
}

func (p *Peers) Collect(ctx context.Context) (snapshot.Patch, error) {
	ctx, cancel := context.WithTimeout(ctx, chainCallTimeout)
	defer cancel()

	peers, err := p.p2pc.GetPeers(ctx)
	if err != nil {
		return nil, err
	}

	var tip uint32

	for _, peer := range peers {
		if peer == nil {
			continue
		}

		tip = max(tip, peer.DisplayHeight(), peer.Height)
	}

	height := snapshot.Reading{Value: float64(tip), Status: snapshot.StatusOK}
	if len(peers) == 0 {
		height.Note = notePeersNone
	}

	return func(s *snapshot.Snapshot) {
		s.PeerTip = tip
		s.Chain.PeerHeight = height
	}, nil
}
