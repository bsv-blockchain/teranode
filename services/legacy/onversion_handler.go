package legacy

import (
	"github.com/bitcoin-sv/ubsv/services/legacy/peer"
	"github.com/bitcoin-sv/ubsv/services/legacy/wire"
)

// onVersion is invoked when a peer receives a version bitcoin message.
// The caller may return a reject message in which case the message will
// be sent to the peer and the peer will be disconnected.
func (pm *PeerManager) onVersion() func(p *peer.Peer, msg *wire.MsgVersion) *wire.MsgReject {
	return func(p *peer.Peer, msg *wire.MsgVersion) *wire.MsgReject {
		// TODO: check the version
		pm.logger.Debugf("onVersion msg %+v", msg)
		return nil
	}
}
