package legacy

import (
	"github.com/bitcoin-sv/ubsv/services/legacy/peer"
	"github.com/bitcoin-sv/ubsv/services/legacy/wire"
)

func (pm *PeerManager) onInv() func(p *peer.Peer, msg *wire.MsgInv) {
	return func(p *peer.Peer, msg *wire.MsgInv) {
		// TODO
	}
}
