package legacy

import (
	"context"
	"github.com/bitcoin-sv/ubsv/services/legacy/chaincfg"
	"github.com/bitcoin-sv/ubsv/services/legacy/peer"
	"github.com/bitcoin-sv/ubsv/services/legacy/wire"
	"github.com/bitcoin-sv/ubsv/ulogger"
	"net"
)

type Peer struct {
	logger ulogger.Logger
	addr   string
	peer   *peer.Peer
}

func NewPeer(pm *PeerManager, addr string) (*Peer, error) {
	p, err := peer.NewOutboundPeer(&peer.Config{
		UserAgentName:    "teranode-legacy-p2p",
		UserAgentVersion: "0.0.1",
		ChainParams:      &chaincfg.MainNetParams, // TODO make configurable
		Listeners: peer.MessageListeners{
			OnHeaders:   pm.onHeaders(),
			OnBlock:     pm.onBlock(context.TODO()),
			OnTx:        pm.onTx(),
			OnInv:       pm.onInv(),
			OnCFCheckpt: nil, // not implementing because of ....
		},
	}, addr)

	// Establish a connection to the peer
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		pm.logger.Fatalf("Failed to connect to peer: %v", err)
	}
	p.AssociateConnection(conn)

	return &Peer{
		logger: pm.logger,
		addr:   addr,
		peer:   p,
	}, nil
}

func (p *Peer) NewOutboundPeer() {
	// Create a new Bitcoin peer
	// addresses, _ := gocore.Config().GetMulti("legacy_connect_peers", "|", []string{"
}

func (p *Peer) QueueMessage(msg *wire.MsgGetHeaders, doneChan chan<- struct{}) {
	p.peer.QueueMessage(msg, doneChan)
}

func (p *Peer) WaitForDisconnect() {
	p.peer.WaitForDisconnect()
}
