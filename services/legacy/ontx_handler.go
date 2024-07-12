package legacy

import (
	"github.com/bitcoin-sv/ubsv/services/legacy/peer"
	"github.com/bitcoin-sv/ubsv/services/legacy/wire"
)

// onTx is invoked when a peer receives a tx bitcoin message.
func (pm *PeerManager) onTx() func(p *peer.Peer, msg *wire.MsgTx) {
	return func(p *peer.Peer, msg *wire.MsgTx) {
		// TODO

		// announce the transaction to all connected peers
		// something like pm.AnnounceTx(msg)
	}
}
