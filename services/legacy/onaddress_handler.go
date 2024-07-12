package legacy

import (
	"github.com/bitcoin-sv/ubsv/services/legacy/peer"
	"github.com/bitcoin-sv/ubsv/services/legacy/wire"
)

// onAddr is invoked when a peer receives an addr bitcoin message.
func (pm *PeerManager) onAddr() func(p *peer.Peer, msg *wire.MsgAddr) {
	return func(p *peer.Peer, msg *wire.MsgAddr) {

	}
}
