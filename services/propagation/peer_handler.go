package propagation

import (
	"bytes"
	"context"
	"fmt"
	"time"

	"github.com/TAAL-GmbH/ubsv/services/propagation/store"
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
	txStore               store.TransactionStore
	blockStore            store.TransactionStore
	getTransactionBatcher *batcher.Batcher[chainhash.Hash]
}

func NewPeerHandler(txStore store.TransactionStore, blockStore store.TransactionStore) p2p.PeerHandlerI {
	ph := &PeerHandler{
		logger:     gocore.Log("p2p"),
		txStore:    txStore,
		blockStore: blockStore,
	}

	return ph
}

func (ph *PeerHandler) HandleTransactionGet(msg *wire.InvVect, peer p2p.PeerI) ([]byte, error) {
	ph.logger.Infof("received transaction get for %s", msg.Hash.String())

	return ph.txStore.Get(context.Background(), msg.Hash[:])
}

func (ph *PeerHandler) HandleTransactionSent(_ *wire.MsgTx, _ p2p.PeerI) error {
	// do nothing with this for now
	return nil
}

func (ph *PeerHandler) HandleTransactionAnnouncement(msg *wire.InvVect, peer p2p.PeerI) error {
	ph.logger.Infof("received transaction inv: %s", msg.Hash.String())

	// check whether we already have this transaction
	// if we do, we don't need to request it from the peer
	tx, _ := ph.txStore.Get(context.Background(), msg.Hash[:])
	if tx != nil {
		return nil
	}

	// request transaction from peer
	peer.RequestTransaction(&msg.Hash)

	return nil
}

func (ph *PeerHandler) HandleTransactionRejection(rejMsg *wire.MsgReject, peer p2p.PeerI) error {
	ph.logger.Infof("received transaction rejection: %s", rejMsg.Hash.String())
	return nil
}

func (ph *PeerHandler) HandleTransaction(msg *wire.MsgTx, _ p2p.PeerI) error {
	// TODO validate transaction
	// send REJECT message to peer if invalid tx

	var buf bytes.Buffer
	if err := msg.Serialize(&buf); err != nil {
		return err
	}

	txHash := msg.TxHash()
	if err := ph.txStore.Set(context.Background(), txHash[:], buf.Bytes()); err != nil {
		return err
	}

	// TODO broadcast transaction to other peers

	// TODO add transaction to the block assembly service

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

	var buf bytes.Buffer
	if err := msg.Serialize(&buf); err != nil {
		return err
	}

	// TODO announce block to other peers

	return ph.blockStore.Set(context.Background(), blockHash[:], buf.Bytes())
}
