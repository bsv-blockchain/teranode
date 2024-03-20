package p2p

import (
	"fmt"

	"github.com/bitcoin-sv/ubsv/services/legacy/wire"
	"github.com/libsv/go-bt/v2/chainhash"
)

var (
	ErrPeerNetworkMismatch = fmt.Errorf("peer network mismatch")
)

type PeerManagerI interface {
	AnnounceTransaction(txHash *chainhash.Hash, peers []PeerI) []PeerI
	RequestTransaction(txHash *chainhash.Hash) PeerI
	AnnounceBlock(blockHash *chainhash.Hash, peers []PeerI) []PeerI
	RequestBlock(blockHash *chainhash.Hash) PeerI
	AddPeer(peer PeerI) error
	GetPeers() []PeerI
}

type PeerI interface {
	Connected() bool
	WriteMsg(msg wire.Message) error
	String() string
	AnnounceTransaction(txHash *chainhash.Hash)
	RequestTransaction(txHash *chainhash.Hash)
	AnnounceBlock(blockHash *chainhash.Hash)
	RequestBlock(blockHash *chainhash.Hash)
	Network() wire.BitcoinNet
	IsHealthy() bool
}

type PeerHandlerI interface {
	HandleTransactionGet(msg *wire.InvVect, peer PeerI) ([]byte, error)
	HandleTransactionSent(msg *wire.MsgTx, peer PeerI) error
	HandleTransactionAnnouncement(msg *wire.InvVect, peer PeerI) error
	HandleTransactionRejection(rejMsg *wire.MsgReject, peer PeerI) error
	HandleTransaction(msg *wire.MsgTx, peer PeerI) error
	HandleBlockAnnouncement(msg *wire.InvVect, peer PeerI) error
	HandleBlock(msg wire.Message, peer PeerI) error
}
