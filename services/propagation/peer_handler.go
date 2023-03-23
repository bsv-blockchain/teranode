package propagation

import (
	"github.com/libsv/go-p2p"
	"github.com/libsv/go-p2p/wire"
	"github.com/ordishs/go-utils"
	"github.com/ordishs/gocore"
)

type PeerHandler struct {
	logger utils.Logger
}

func NewPeerHandler() p2p.PeerHandlerI {
	return &PeerHandler{
		logger: gocore.Log("p2p"),
	}
}

func (ph *PeerHandler) HandleTransactionGet(msg *wire.InvVect, peer p2p.PeerI) ([]byte, error) {
	ph.logger.Infof("received transaction get for %s", msg.Hash.String())
	return nil, nil
}

func (ph *PeerHandler) HandleTransactionSent(msg *wire.MsgTx, peer p2p.PeerI) error {
	return nil
}

func (ph *PeerHandler) HandleTransactionAnnouncement(msg *wire.InvVect, peer p2p.PeerI) error {
	ph.logger.Infof("received transaction inv: %s", msg.Hash.String())
	return nil
}

func (ph *PeerHandler) HandleTransactionRejection(rejMsg *wire.MsgReject, peer p2p.PeerI) error {
	return nil
}

func (ph *PeerHandler) HandleTransaction(msg *wire.MsgTx, peer p2p.PeerI) error {
	return nil
}

func (ph *PeerHandler) HandleBlockAnnouncement(msg *wire.InvVect, peer p2p.PeerI) error {
	ph.logger.Infof("received block inv: %v", msg.Hash.String())
	return nil
}

func (ph *PeerHandler) HandleBlock(msg *p2p.BlockMessage, peer p2p.PeerI) error {
	return nil
}
