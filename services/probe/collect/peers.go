package collect

import (
	"context"
	"slices"

	"github.com/bsv-blockchain/teranode/services/p2p"
	"github.com/bsv-blockchain/teranode/services/probe/snapshot"
)

const (
	notePeersNone     = "no peers"
	notePeersNoHeight = "no usable peer height"
)

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

	tip := medianHeight(peers)

	height := snapshot.Reading{Value: float64(tip), Status: snapshot.StatusOK}
	switch {
	case len(peers) == 0:
		height.Note = notePeersNone
	case tip == 0:
		height.Note = notePeersNoHeight
	}

	return func(s *snapshot.Snapshot) {
		s.PeerTip = tip
		s.Chain.PeerHeight = height
	}, nil
}

func medianHeight(peers []*p2p.PeerInfo) uint32 {
	heights := make([]uint32, 0, len(peers))

	for _, peer := range peers {
		if peer == nil || peer.IsBanned || peer.Height == 0 {
			continue
		}

		heights = append(heights, peer.Height)
	}

	if len(heights) == 0 {
		return 0
	}

	slices.Sort(heights)

	return heights[(len(heights)-1)/2]
}
