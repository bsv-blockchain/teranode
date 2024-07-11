package legacy

import (
	"github.com/bitcoin-sv/ubsv/services/legacy/peer"
	"github.com/bitcoin-sv/ubsv/services/legacy/wire"
)

// onGetAddr is invoked when a peer receives a getaddr bitcoin message.
func (pm *PeerManager) onGetAddr() func(p *peer.Peer, msg *wire.MsgGetAddr) {
	return func(p *peer.Peer, msg *wire.MsgGetAddr) {
	}
}
