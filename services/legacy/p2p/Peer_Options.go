package p2p

import (
	"net"
	"time"
)

type PeerOptions func(p *Peer)

func WithDialer(dial func(network, address string) (net.Conn, error)) PeerOptions {
	return func(p *Peer) {
		p.dial = dial
	}
}

func WithBatchDelay(batchDelay time.Duration) PeerOptions {
	return func(p *Peer) {
		p.batchDelay = batchDelay
	}
}

func WithIncomingConnection(conn net.Conn) PeerOptions {
	return func(p *Peer) {
		p.incomingConn = conn
	}
}

func WithMaximumMessageSize(maximumMessageSize int64) PeerOptions {
	return func(p *Peer) {
		p.maximumMessageSize = maximumMessageSize
	}
}
