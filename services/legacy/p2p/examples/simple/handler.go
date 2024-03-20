package main

import (
	"fmt"

	"log/slog"

	"github.com/bitcoin-sv/ubsv/services/legacy/p2p"
	"github.com/bitcoin-sv/ubsv/services/legacy/wire"
)

// SimplePeerHandler is a simple implementation of the PeerHandler interface.
// This is how you can customize the behaviour of the peer.
type SimplePeerHandler struct {
	logger *slog.Logger
}

func (s *SimplePeerHandler) HandleTransactionGet(msg *wire.InvVect, peer p2p.PeerI) ([]byte, error) {
	s.logger.Info("Peer requested transaction", slog.String("hash", msg.Hash.String()), slog.String("peer", peer.String()))
	// You should implement a store and return the transaction bytes here.
	return nil, fmt.Errorf("transaction not found")
}

func (s *SimplePeerHandler) HandleTransactionSent(msg *wire.MsgTx, peer p2p.PeerI) error {
	s.logger.Info("Sent transaction to peer", slog.String("hash", msg.TxHash().String()), slog.String("peer", peer.String()))
	// This is called when a transaction is sent to a peer. You could save this to your store here.
	return nil
}

func (s *SimplePeerHandler) HandleTransactionAnnouncement(msg *wire.InvVect, peer p2p.PeerI) error {
	s.logger.Info("Peer announced transaction", slog.String("hash", msg.Hash.String()), slog.String("peer", peer.String()))
	// This is called when a transaction is announced by a peer. Handle this as you wish.
	return nil
}

func (s *SimplePeerHandler) HandleTransactionRejection(rejMsg *wire.MsgReject, peer p2p.PeerI) error {
	s.logger.Info("Peer rejected transaction", slog.String("hash", rejMsg.Hash.String()), slog.String("peer", peer.String()))
	// This is called when a transaction is rejected by a peer. Handle this as you wish.
	return nil
}

func (s *SimplePeerHandler) HandleTransaction(msg *wire.MsgTx, peer p2p.PeerI) error {
	s.logger.Info("Received transaction from peer", slog.String("hash", msg.TxHash().String()), slog.String("peer", peer.String()))
	// This is called when a transaction is received from a peer. Handle this as you wish.
	return nil
}

func (s *SimplePeerHandler) HandleBlockAnnouncement(msg *wire.InvVect, peer p2p.PeerI) error {
	s.logger.Info("Peer announced block", slog.String("hash", msg.Hash.String()), slog.String("peer", peer.String()))
	// This is called when a block is announced by a peer. Handle this as you wish.
	return nil
}

func (s *SimplePeerHandler) HandleBlock(msg wire.Message, peer p2p.PeerI) error {
	blockMsg, ok := msg.(*p2p.BlockMessage)
	if !ok {
		return fmt.Errorf("failed to cast message to block message")
	}

	s.logger.Info("Received block from peer", slog.String("hash", blockMsg.Header.BlockHash().String()), slog.String("peer", peer.String()))
	// This is called when a block is received from a peer. Handle this as you wish.
	// note: the block message is a custom BlockMessage and not a wire.MsgBlock
	return nil
}
