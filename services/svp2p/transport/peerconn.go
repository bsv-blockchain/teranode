package transport

import (
	"context"
	"net"

	"github.com/bsv-blockchain/go-wire"
)

// PeerConn is the surface protocol.Peer consumes. *Conn is the single-stream form;
// *Association the multi-stream form. Fixed to the call sites in protocol/peer.go
// and protocol/getdata.go — add nothing here for a state machine's convenience.
type PeerConn interface {
	Start(ctx context.Context)
	Inbound() <-chan wire.Message
	InboundBlocks() <-chan *BlockStream
	InboundTxns() <-chan *TxnStream
	Send(msg wire.Message) error
	SendPriority(msg wire.Message) error
	SendBlock(ctx context.Context, req BlockSendRequest) error
	SetProtocolVersion(v uint32)
	Close() error
	Err() error
	Done() <-chan struct{}
	RemoteAddr() net.Addr
	BytesSent() uint64
	BytesReceived() uint64
}
