package legacy

import (
	"github.com/bitcoin-sv/ubsv/services/legacy/peer"
	"github.com/bitcoin-sv/ubsv/services/legacy/wire"
)

// onInv is invoked when a peer receives an inv bitcoin message.
func (pm *PeerManager) onInv() func(p *peer.Peer, msg *wire.MsgInv) {
	return func(p *peer.Peer, msg *wire.MsgInv) {
		// TODO
	}
}
