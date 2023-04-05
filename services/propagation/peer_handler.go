package propagation

import (
	"bytes"
	"fmt"
	"time"

	"github.com/libsv/go-bt/v2"
	"github.com/libsv/go-p2p"
	"github.com/libsv/go-p2p/blockchain"
	"github.com/libsv/go-p2p/chaincfg/chainhash"
	"github.com/libsv/go-p2p/wire"
	"github.com/ordishs/go-utils"
	"github.com/ordishs/go-utils/batcher"
	"github.com/ordishs/gocore"
)

type PeerHandler struct {
	logger                utils.Logger
	getTransactionBatcher *batcher.Batcher[chainhash.Hash]
}

func NewPeerHandler() p2p.PeerHandlerI {
	ph := &PeerHandler{
		logger: gocore.Log("p2p"),
	}

	return ph
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

func (ph *PeerHandler) HandleBlockAnnouncement(invMsg *wire.InvVect, peer p2p.PeerI) error {
	ph.logger.Infof("received block inv: %v", invMsg.Hash.String())

	msg := wire.NewMsgGetData()

	if err := msg.AddInvVect(wire.NewInvVect(wire.InvTypeBlock, &invMsg.Hash)); err != nil {
		ph.logger.Errorf("ProcessBlock: could not create InvVect: %v", err)
		return err
	}

	if err := peer.WriteMsg(msg); err != nil {
		ph.logger.Errorf("ProcessBlock: failed to write message to peer: %v", err)
		return err
	}

	ph.logger.Infof("ProcessBlock: %s", invMsg.Hash.String())

	return nil
}

func (ph *PeerHandler) HandleBlock(wireMsg wire.Message, peer p2p.PeerI) error {
	start := time.Now()

	msg, ok := wireMsg.(*wire.MsgBlock)
	if !ok {
		return fmt.Errorf("could not convert wire.Message to BlockMessage")
	}

	blockHash := msg.Header.BlockHash()

	// previousBlockHash := msg.Header.PrevBlock

	merkleRoot := msg.Header.MerkleRoot

	transactionHashes := make([][]byte, len(msg.Transactions))
	for i, tx := range msg.Transactions {
		var buff bytes.Buffer
		_ = tx.Serialize(&buff)
		btTx, err := bt.NewTxFromBytes(buff.Bytes())
		if err != nil {
			return err
		}
		// bt returns the tx id bytes in reverse order :-/
		transactionHashes[i] = bt.ReverseBytes(btTx.TxIDBytes())
	}

	calculatedMerkleRoot := blockchain.BuildMerkleTreeStore(transactionHashes)
	if !bytes.Equal(calculatedMerkleRoot[len(calculatedMerkleRoot)-1], merkleRoot[:]) {
		return fmt.Errorf("merkle root mismatch for block %s", blockHash.String())
	}

	ph.logger.Infof("Processed block %s, %d transactions in %0.2f seconds", blockHash.String(), len(msg.Transactions), time.Since(start).Seconds())

	return nil
}
