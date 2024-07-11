package legacy

import (
	"github.com/bitcoin-sv/ubsv/services/legacy/peer"
	"github.com/bitcoin-sv/ubsv/services/legacy/wire"
)

// onReject is invoked when a peer receives a reject bitcoin message.
func (pm *PeerManager) onReject() func(p *peer.Peer, msg *wire.MsgReject) {
	return func(p *peer.Peer, msg *wire.MsgReject) {

	}
}
