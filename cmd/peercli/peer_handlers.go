package main

import (
	"encoding/json"
	"fmt"
	"os"
	"time"

	"github.com/bitcoin-sv/teranode/services/legacy/peer"
	"github.com/bsv-blockchain/go-wire"
)

// OnVersion handles the "version" message received from a peer.
// Parameters:
//   - p: The peer that sent the message.
//   - msg: The version message received.
//
// Returns:
//   - A MsgReject if the version is not acceptable, otherwise nil.
func OnVersion(_ *peer.Peer, msg *wire.MsgVersion) *wire.MsgReject {
	fmt.Printf("Received version: %v\n", msg)
	return nil
}

// OnMemPool handles the "mempool" message received from a peer.
// Parameters:
//   - p: The peer that sent the message.
//   - msg: The mempool message received.
func OnMemPool(_ *peer.Peer, _ *wire.MsgMemPool) {
	fmt.Println("Received mempool")
}

// OnTx handles the "tx" message received from a peer.
// Parameters:
//   - p: The peer that sent the message.
//   - msg: The transaction message received.
func OnTx(_ *peer.Peer, msg *wire.MsgTx) {
	fmt.Printf("Received tx message: %v\n", msg.TxHash().String())
}

// OnBlock handles the "block" message received from a peer.
// Parameters:
//   - p: The peer that sent the message.
//   - msg: The block message received.
//   - buf: The raw block data.
func OnBlock(_ *peer.Peer, msg *wire.MsgBlock, _ []byte) {
	fmt.Printf("Received block message: %v\n", msg.BlockHash().String())
}

// OnInv handles the "inv" message received from a peer.
// Parameters:
//   - p: The peer that sent the message.
//   - msg: The inventory message received.
func OnInv(_ *peer.Peer, msg *wire.MsgInv) {
	fmt.Printf("Received inv message: %+v\n", msg)
	fmt.Printf("Message command: %+v\n", msg.Command())

	for i, vect := range msg.InvList {
		fmt.Printf("%d. vect: type: %s\nhash: %s\n", i, vect.Type, vect.Hash.String())
	}

	fmt.Printf("\n")
}

// OnHeaders handles the "headers" message received from a peer.
// Parameters:
//   - p: The peer that sent the message.
//   - msg: The header message received.
func OnHeaders(_ *peer.Peer, _ *wire.MsgHeaders) {
	fmt.Println("Received headers")
}

// OnGetData handles the "getdata" message received from a peer.
// Parameters:
//   - p: The peer that sent the message.
//   - msg: The getdata message received.
func OnGetData(_ *peer.Peer, _ *wire.MsgGetData) {
	fmt.Println("Received getData")
}

// OnGetBlocks handles the "getblocks" message received from a peer.
// Parameters:
//   - p: The peer that sent the message.
//   - msg: The getblocks message received.
func OnGetBlocks(_ *peer.Peer, _ *wire.MsgGetBlocks) {
	fmt.Println("Received getBlocks")
}

// OnGetHeaders handles the "getheaders" message received from a peer.
// Parameters:
//   - p: The peer that sent the message.
//   - msg: The getheaders message received.
func OnGetHeaders(_ *peer.Peer, _ *wire.MsgGetHeaders) {
	fmt.Println("Received getHeaders")
}

// OnFeeFilter handles the "feefilter" message received from a peer.
// Parameters:
//   - p: The peer that sent the message.
//   - msg: The feefilter message received.
func OnFeeFilter(_ *peer.Peer, _ *wire.MsgFeeFilter) {
	fmt.Println("Received feeFilter")
}

// OnGetAddr handles the "getaddr" message received from a peer.
// Parameters:
//   - p: The peer that sent the message.
//   - msg: The getaddr message received.
func OnGetAddr(_ *peer.Peer, _ *wire.MsgGetAddr) {
	fmt.Println("Received getAddr")
}

// OnAddr handles the "addr" message received from a peer.
// Parameters:
//   - p: The peer that sent the message.
//   - msg: The addr message received.
func OnAddr(_ *peer.Peer, msg *wire.MsgAddr) {
	addrList := make([]map[string]interface{}, len(msg.AddrList))

	for i, addr := range msg.AddrList {
		addrList[i] = map[string]interface{}{
			"timestamp": addr.Timestamp.Format(time.RFC3339),
			"services":  addr.Services,
			"ip":        addr.IP.String(),
			"port":      addr.Port,
		}
	}

	result := map[string]interface{}{
		"addrList": addrList,
	}

	jsonData, err := json.Marshal(result)

	if err != nil {
		fmt.Println("{}") // Print empty JSON on error

		return
	}

	fmt.Println(string(jsonData))
}

// OnRead is invoked when a peer receives a Bitcoin message.
// Parameters:
//   - p: The peer that received the message.
//   - bytesRead: The number of bytes read.
//   - msg: The message received.
//   - err: Any error that occurred during the read.
func OnRead(_ *peer.Peer, bytesRead int, msg wire.Message, err error) {
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

// OnWrite is invoked when a Bitcoin message is written to a peer.
// Parameters:
//   - p: The peer to which the message was written.
//   - bytesWritten: The number of bytes written.
//   - msg: The message written.
//   - err: Any error that occurred during the writing.
func OnWrite(_ *peer.Peer, bytesWritten int, msg wire.Message, err error) {
	_, _ = fmt.Fprintf(os.Stdout, "onWrite: bytesWritten: %d\nmsg: %+v\n", bytesWritten, msg)

	if err != nil {
		_, _ = fmt.Fprintf(os.Stdout, "err: %v\n", err)
	}

	_, _ = fmt.Fprintf(os.Stdout, "\n")
}

// OnReject handles the "reject" message received from a peer.
// Parameters:
//   - p: The peer that sent the message.
//   - msg: The reject message received.
func OnReject(_ *peer.Peer, _ *wire.MsgReject) {
	_, _ = fmt.Fprintf(os.Stdout, "Received reject\n")
}

// OnNotFound handles the "notfound" message received from a peer.
// Parameters:
//   - p: The peer that sent the message.
//   - msg: The notfound message received.
func OnNotFound(_ *peer.Peer, _ *wire.MsgNotFound) {
	_, _ = fmt.Fprintf(os.Stdout, "Received objectNotFound\n")
}

// OnVerAck handles the "verack" message received from a peer.
// Parameters:
//   - p: The peer that sent the message.
//   - msg: The verack message received.
func OnVerAck(_ *peer.Peer, _ *wire.MsgVerAck) {
	bsvPeerConnected <- true
	verack <- struct{}{}
}

// OnPing handles the "ping" message received from a peer.
// Parameters:
//   - p: The peer that sent the message.
//   - msg: The ping message received.
func OnPing(p *peer.Peer, msg *wire.MsgPing) {
	fmt.Printf("Received ping: %d\n", msg.Nonce)
	pongMsg := wire.NewMsgPong(msg.Nonce)
	p.QueueMessage(pongMsg, nil)
	fmt.Printf("Sent pong: %d\n", msg.Nonce)
}

// OnPong handles the "pong" message received from a peer.
// Parameters:
//   - p: The peer that sent the message.
//   - msg: The pong message received.
func OnPong(_ *peer.Peer, msg *wire.MsgPong) {
	_, _ = fmt.Fprintf(os.Stdout, "Received pong: %d\n", msg.Nonce)
}
