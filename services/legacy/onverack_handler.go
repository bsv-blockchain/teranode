package legacy

import (
	"github.com/bitcoin-sv/ubsv/services/legacy/peer"
	"github.com/bitcoin-sv/ubsv/services/legacy/wire"
)

// onVerAck is invoked when a peer receives a verack bitcoin message.
func (pm *PeerManager) onVerAck() func(p *peer.Peer, msg *wire.MsgVerAck) {
	return func(p *peer.Peer, msg *wire.MsgVerAck) {

	}
}
