package main

import (
	"fmt"

	"github.com/bitcoin-sv/ubsv/services/legacy/peer"
	"github.com/bitcoin-sv/ubsv/services/legacy/wire"
)

func OnVersion(p *peer.Peer, msg *wire.MsgVersion) *wire.MsgReject {
	fmt.Printf("received version: %v\n", msg)
	return nil
}

func OnMemPool(p *peer.Peer, msg *wire.MsgMemPool) {
	fmt.Println("received mempool")
}

func OnTx(p *peer.Peer, msg *wire.MsgTx) {
	fmt.Printf("Received tx message: %v\n", msg.TxHash().String())
}

func OnBlock(p *peer.Peer, msg *wire.MsgBlock, buf []byte) {
	fmt.Printf("Received block message: %v\n", msg.BlockHash().String())
}

func OnInv(p *peer.Peer, msg *wire.MsgInv) {
	fmt.Printf("Received inv message: %+v\n", msg)
	fmt.Printf("message command: %+v\n", msg.Command())

	for i, vect := range msg.InvList {
		fmt.Printf("%d. vect: type: %s\nhash: %s\n", i, vect.Type, vect.Hash.String())
	}
	fmt.Printf("\n")
}

func OnHeaders(p *peer.Peer, msg *wire.MsgHeaders) {
	fmt.Println("received headers")
}

func OnGetData(p *peer.Peer, msg *wire.MsgGetData) {
	fmt.Println("received getData")
}

func OnGetBlocks(p *peer.Peer, msg *wire.MsgGetBlocks) {
	fmt.Println("received getBlocks")
}

func OnGetHeaders(p *peer.Peer, msg *wire.MsgGetHeaders) {
	fmt.Println("received getHeaders")
}

func OnFeeFilter(p *peer.Peer, msg *wire.MsgFeeFilter) {
	fmt.Println("received feeFilter")
}

func OnGetAddr(p *peer.Peer, msg *wire.MsgGetAddr) {
	fmt.Println("received getAddr")
}
func OnAddr(p *peer.Peer, msg *wire.MsgAddr) {
	fmt.Printf("received addr: %+v\n\n", msg)
	for i, vect := range msg.AddrList {
		fmt.Printf("%d. address: %+v\n", i, vect)
	}
	fmt.Printf("\n")

}

// OnRead is invoked when a peer receives a bitcoin message.  It
// consists of the number of bytes read, the message, and whether or not
// an error in the read occurred.  Typically, callers will opt to use
// the callbacks for the specific message types, however this can be
// useful for circumstances such as keeping track of server-wide byte
// counts or working with custom message types for which the peer does
// not directly provide a callback.
func OnRead(p *peer.Peer, bytesRead int, msg wire.Message, err error) {
	fmt.Printf("onRead: bytesRead: %d\nmsg: %+v\n", bytesRead, msg)
	if err != nil {
		fmt.Printf("err: %v\n", err)
		if err.Error() == "ReadMessage: unhandled command [protoconf]" {
			// We don't understand this command, log and ignore it instead of disconnecting
			fmt.Println("Received unhandled command [protoconf], ignoring")
			return
		}
	}
	fmt.Println("")
}

// OnWrite is invoked when we write a bitcoin message to a peer.  It
// consists of the number of bytes written, the message, and whether or
// not an error in the write occurred.  This can be useful for
// circumstances such as keeping track of server-wide byte counts.
func OnWrite(p *peer.Peer, bytesWritten int, msg wire.Message, err error) {
	fmt.Printf("onWrite: bytesWritten: %d\nmsg: %+v\n", bytesWritten, msg)
	if err != nil {
		fmt.Printf("err: %v\n", err)
	}
	fmt.Println("")
}
func OnReject(p *peer.Peer, msg *wire.MsgReject) {
	fmt.Println("received reject")
}
func OnNotFound(p *peer.Peer, msg *wire.MsgNotFound) {
	fmt.Println("received objectNotFound")
}

func OnVerAck(p *peer.Peer, msg *wire.MsgVerAck) {
	bsvPeerConnected <- true
	verack <- struct{}{}
}

func OnPing(p *peer.Peer, msg *wire.MsgPing) {
	fmt.Printf("Received ping: %d\n", msg.Nonce)
	pongMsg := wire.NewMsgPong(msg.Nonce)
	p.QueueMessage(pongMsg, nil)
	fmt.Printf("Sent pong: %d\n", msg.Nonce)
}

func OnPong(p *peer.Peer, msg *wire.MsgPong) {
	fmt.Printf("Received pong: %d\n", msg.Nonce)
}
