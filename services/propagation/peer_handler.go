package propagation

import (
	"bytes"
	"context"
	"fmt"
	"time"

	"github.com/TAAL-GmbH/ubsv/services/propagation/store"
	"github.com/TAAL-GmbH/ubsv/services/validator"
	"github.com/TAAL-GmbH/ubsv/tracing"
	"github.com/libsv/go-bt/v2"
	"github.com/libsv/go-p2p"
	"github.com/libsv/go-p2p/blockchain"
	"github.com/libsv/go-p2p/chaincfg/chainhash"
	"github.com/libsv/go-p2p/wire"
	"github.com/ordishs/go-utils"
	"github.com/ordishs/gocore"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	prometheusPeerAnnouncedTransactions prometheus.Counter
	prometheusPeerGetTransactions       prometheus.Counter
	prometheusPeerSentTransactions      prometheus.Counter
	prometheusPeerReceivedTransactions  prometheus.Counter
	prometheusPeerInvalidTransactions   prometheus.Counter
	prometheusPeerAnnouncedBlock        prometheus.Counter
	prometheusPeerHandleBlock           prometheus.Counter
	prometheusPeerTransactionDuration   prometheus.Histogram
	prometheusPeerTransactionSize       prometheus.Histogram
	prometheusPeerBlockDuration         prometheus.Histogram
	prometheusPeerBlockSize             prometheus.Histogram
)

func init() {
	prometheusPeerAnnouncedTransactions = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "peer_processed_transactions",
			Help: "Number of transactions announced to the peer handler",
		},
	)
	prometheusPeerGetTransactions = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "peer_get_transactions",
			Help: "Number of transactions get request to the peer handler",
		},
	)
	prometheusPeerSentTransactions = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "peer_sent_transactions",
			Help: "Number of transactions sent by the peer handler",
		},
	)
	prometheusPeerReceivedTransactions = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "peer_received_transactions",
			Help: "Number of transactions received by the peer handler",
		},
	)
	prometheusPeerInvalidTransactions = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "peer_invalid_transactions",
			Help: "Number of transactions found invalid by the peer handler",
		},
	)
	prometheusPeerAnnouncedBlock = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "peer_announced_block",
			Help: "Number of blocks announced by the peer handler",
		},
	)
	prometheusPeerHandleBlock = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "peer_handle_block",
			Help: "Number of blocks handled by the peer handler",
		},
	)
	prometheusPeerTransactionDuration = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Name: "peer_transactions_duration",
			Help: "Duration of transaction processing",
		},
	)
	prometheusPeerTransactionSize = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Name: "peer_transactions_size",
			Help: "Size of transactions processed",
		},
	)
	prometheusPeerBlockDuration = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Name: "peer_block_duration",
			Help: "Duration of block processing",
		},
	)
	prometheusPeerBlockSize = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Name: "peer_block_size",
			Help: "Size of block processed",
		},
	)
}

type PeerHandler struct {
	logger       utils.Logger
	txStore      store.TransactionStore
	blockStore   store.TransactionStore
	validator    validator.Interface
	blockBacklog map[chainhash.Hash]*chainhash.Hash
	// getTransactionBatcher *batcher.Batcher[chainhash.Hash]
}

func NewPeerHandler(txStore store.TransactionStore, blockStore store.TransactionStore, v validator.Interface) p2p.PeerHandlerI {
	ph := &PeerHandler{
		logger:       gocore.Log("p2p"),
		txStore:      txStore,
		blockStore:   blockStore,
		validator:    v,
		blockBacklog: make(map[chainhash.Hash]*chainhash.Hash),
	}

	return ph
}

func (ph *PeerHandler) HandleTransactionGet(msg *wire.InvVect, peer p2p.PeerI) ([]byte, error) {
	ph.logger.Infof("received transaction get for %s", msg.Hash.String())

	prometheusPeerGetTransactions.Inc()

	return ph.txStore.Get(context.Background(), msg.Hash[:])
}

func (ph *PeerHandler) HandleTransactionSent(_ *wire.MsgTx, _ p2p.PeerI) error {
	// do nothing with this for now

	prometheusPeerSentTransactions.Inc()

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

	prometheusPeerAnnouncedTransactions.Inc()

	// request transaction from peer
	peer.RequestTransaction(&msg.Hash)

	return nil
}

func (ph *PeerHandler) HandleTransactionRejection(rejMsg *wire.MsgReject, peer p2p.PeerI) error {
	ph.logger.Infof("received transaction rejection: %s", rejMsg.Hash.String())
	return nil
}

func (ph *PeerHandler) HandleTransaction(msg *wire.MsgTx, peer p2p.PeerI) error {
	timeStart := time.Now()
	traceSpan := tracing.Start(context.Background(), "PeerHandler:HandleTransaction")
	defer traceSpan.Finish()

	ph.logger.Infof("received transaction: %s", msg.TxHash().String())
	var buf bytes.Buffer
	if err := msg.Serialize(&buf); err != nil {
		prometheusPeerInvalidTransactions.Inc()
		return err
	}

	txHash := msg.TxHash()
	if err := ph.txStore.Set(context.Background(), txHash[:], buf.Bytes()); err != nil {
		return err
	}

	txBytes := buf.Bytes()
	btTx, err := bt.NewTxFromBytes(txBytes)
	if err != nil {
		prometheusPeerInvalidTransactions.Inc()
		return err
	}

	// Do not allow propagation of coinbase transactions
	if btTx.IsCoinbase() {
		prometheusPeerInvalidTransactions.Inc()
		return fmt.Errorf("received coinbase transaction: %s", msg.TxHash().String())
	}

	err = ph.extendTransaction(traceSpan.Ctx, btTx)
	if err != nil {
		prometheusPeerInvalidTransactions.Inc()
		return err
	}

	prometheusPeerTransactionSize.Observe(float64(len(txBytes)))
	prometheusPeerReceivedTransactions.Inc()

	if err = ph.validator.Validate(traceSpan.Ctx, btTx); err != nil {
		// send REJECT message to peer if invalid tx
		ph.logger.Errorf("received invalid transaction: %s", err.Error())
		_ = peer.WriteMsg(wire.NewMsgReject(wire.CmdReject, wire.RejectInvalid, err.Error()))
		return err
	}

	// TODO broadcast transaction to other peers

	// TODO add transaction to the block assembly service

	prometheusPeerTransactionDuration.Observe(float64(time.Since(timeStart).Microseconds()))

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

	prometheusPeerAnnouncedBlock.Inc()
	ph.logger.Infof("ProcessBlock: %s", invMsg.Hash.String())

	return nil
}

func (ph *PeerHandler) HandleBlock(wireMsg wire.Message, peer p2p.PeerI) error {
	start := time.Now()
	traceSpan := tracing.Start(context.Background(), "PeerHandler:HandleBlock")
	defer traceSpan.Finish()

	msg, ok := wireMsg.(*wire.MsgBlock)
	if !ok {
		return fmt.Errorf("could not convert wire.Message to BlockMessage")
	}

	blockHash := msg.Header.BlockHash()

	if !blockHash.IsEqual(&chainhash.Hash{}) { // genesis block
		previousBlockHash := msg.Header.PrevBlock
		if !previousBlockHash.IsEqual(&chainhash.Hash{}) { // genesis block
			_, err := ph.blockStore.Get(traceSpan.Ctx, previousBlockHash[:])
			if err != nil {
				_, ok := ph.blockBacklog[previousBlockHash]
				if !ok {
					// get previous block from peer
					ph.logger.Infof("received block %s, but previous block %s is not known, requesting it from peer", blockHash.String(), previousBlockHash.String())
					ph.blockBacklog[previousBlockHash] = &blockHash
					peer.RequestBlock(&previousBlockHash)
				}
				return nil
			}
		}
	}

	merkleRoot := msg.Header.MerkleRoot

	processTxsSpan := tracing.Start(traceSpan.Ctx, "PeerHandler:HandleBlock:Transactions")
	transactionHashes := make([][]byte, len(msg.Transactions))
	for i, tx := range msg.Transactions {
		var buff bytes.Buffer
		_ = tx.Serialize(&buff)
		btTx, err := bt.NewTxFromBytes(buff.Bytes())
		if err != nil {
			return fmt.Errorf("could not convert transaction to bt.Tx: %w", err)
		}

		// extend the transaction with input data
		if !btTx.IsCoinbase() {
			err = ph.extendTransaction(processTxsSpan.Ctx, btTx)
			if err != nil {
				return fmt.Errorf("could not extend transaction %s: %w", btTx.TxID(), err)
			}
		}

		// Validate the transaction
		if err = ph.validator.Validate(processTxsSpan.Ctx, btTx); err != nil {
			// send REJECT message to peer if invalid tx
			ph.logger.Errorf("received invalid transaction: %s", err.Error())
			_ = peer.WriteMsg(wire.NewMsgReject(wire.CmdReject, wire.RejectInvalid, err.Error()))
			return err
		}

		hash := tx.TxHash()
		txExists, _ := ph.txStore.Get(processTxsSpan.Ctx, hash[:])
		if txExists == nil {
			if err = ph.txStore.Set(processTxsSpan.Ctx, hash[:], buff.Bytes()); err != nil {
				return fmt.Errorf("could not store transaction %s: %w", hash.String(), err)
			}
		}

		// bt returns the tx id bytes in reverse order :-/
		transactionHashes[i] = bt.ReverseBytes(btTx.TxIDBytes())
	}
	processTxsSpan.Finish()

	merkleSpan := tracing.Start(traceSpan.Ctx, "PeerHandler:HandleBlock:MerkleRoot")
	calculatedMerkleRoot := blockchain.BuildMerkleTreeStore(transactionHashes)
	if !bytes.Equal(calculatedMerkleRoot[len(calculatedMerkleRoot)-1], merkleRoot[:]) {
		return fmt.Errorf("merkle root mismatch for block %s", blockHash.String())
	}
	merkleSpan.Finish()

	ph.logger.Infof("Processed block %s, %d transactions in %0.2f seconds", blockHash.String(), len(msg.Transactions), time.Since(start).Seconds())

	var buf bytes.Buffer
	if err := msg.Serialize(&buf); err != nil {
		return fmt.Errorf("could not serialize block: %w", err)
	}

	// TODO announce block to other peers

	blockBytes := buf.Bytes()

	prometheusPeerHandleBlock.Inc()
	prometheusPeerBlockSize.Observe(float64(len(blockBytes)))
	prometheusPeerBlockDuration.Observe(float64(time.Since(start).Microseconds()))

	err := ph.blockStore.Set(traceSpan.Ctx, blockHash[:], blockBytes)
	if err != nil {
		return fmt.Errorf("could not store block %s: %w", blockHash.String(), err)
	}

	// do we need to request the next block after processing this one?
	nextBlockHash, ok := ph.blockBacklog[blockHash]
	if ok {
		ph.logger.Infof("requesting next block %s", nextBlockHash.String())
		peer.RequestBlock(nextBlockHash)
		delete(ph.blockBacklog, blockHash)
	}

	return nil
}

func (ph *PeerHandler) extendTransaction(ctx context.Context, transaction *bt.Tx) (err error) {
	return ExtendTransaction(ctx, transaction, ph.txStore)
}
