// Copyright (c) 2015-2016 The btcsuite developers
// Copyright (c) 2016-2018 The Decred developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

package peer_test

import (
	"context"
	"errors"
	"io"
	"net"
	"net/url"
	"strconv"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-chaincfg"
	"github.com/bsv-blockchain/go-wire"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/services/legacy"
	"github.com/bsv-blockchain/teranode/services/legacy/peer"
	"github.com/bsv-blockchain/teranode/services/legacy/peer_api"
	"github.com/bsv-blockchain/teranode/stores/blob/memory"
	blockchainstore "github.com/bsv-blockchain/teranode/stores/blockchain"
	"github.com/bsv-blockchain/teranode/stores/utxo/sql"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/btcsuite/go-socks/socks"
	"github.com/stretchr/testify/require"
)

// fixedExcessiveBlockSize should not be the default -we want to ensure it will work in all cases
const fixedExcessiveBlockSize uint64 = 42111000

func init() {
	wire.SetLimits(fixedExcessiveBlockSize)
}

// conn mocks a network connection by implementing the net.Conn interface.  It
// is used to test peer connection without actually opening a network
// connection.
type conn struct {
	io.Reader
	io.Writer
	io.Closer

	// local network, address for the connection.
	lnet, laddr string

	// remote network, address for the connection.
	rnet, raddr string

	// mocks socks proxy if true
	proxy bool
}

// LocalAddr returns the local address for the connection.
func (c conn) LocalAddr() net.Addr {
	return &addr{c.lnet, c.laddr}
}

// Remote returns the remote address for the connection.
func (c conn) RemoteAddr() net.Addr {
	if !c.proxy {
		return &addr{c.rnet, c.raddr}
	}

	host, strPort, _ := net.SplitHostPort(c.raddr)
	port, _ := strconv.Atoi(strPort)

	return &socks.ProxiedAddr{
		Net:  c.rnet,
		Host: host,
		Port: port,
	}
}

// Close handles closing the connection.
func (c conn) Close() error {
	if c.Closer == nil {
		return nil
	}

	return c.Closer.Close()
}

func (c conn) SetDeadline(t time.Time) error      { return nil }
func (c conn) SetReadDeadline(t time.Time) error  { return nil }
func (c conn) SetWriteDeadline(t time.Time) error { return nil }

// addr mocks a network address
type addr struct {
	net, address string
}

func (m addr) Network() string { return m.net }
func (m addr) String() string  { return m.address }

// pipe turns two mock connections into a full-duplex connection similar to
// net.Pipe to allow pipe's with (fake) addresses.
func pipe(c1, c2 *conn) (*conn, *conn) {
	r1, w1 := io.Pipe()
	r2, w2 := io.Pipe()

	c1.Writer = w1
	c1.Closer = w1
	c2.Reader = r1
	c1.Reader = r2
	c2.Writer = w2
	c2.Closer = w2

	return c1, c2
}

// peerStats holds the expected peer stats used for testing peer.
type peerStats struct {
	wantUserAgent       string
	wantServices        wire.ServiceFlag
	wantProtocolVersion uint32
	wantConnected       bool
	wantVersionKnown    bool
	wantVerAckReceived  bool
	wantLastBlock       int32
	wantStartingHeight  int32
	wantLastPingTime    time.Time
	wantLastPingNonce   uint64
	wantLastPingMicros  int64
	wantTimeOffset      int64
	wantBytesSent       uint64
	wantBytesReceived   uint64
}

// testPeer tests the given peer's flags and stats
func testPeer(t *testing.T, p *peer.Peer, s peerStats) {
	if p.UserAgent() != s.wantUserAgent {
		t.Errorf("testPeer: wrong UserAgent - got %v, want %v", p.UserAgent(), s.wantUserAgent)
		return
	}

	if p.Services() != s.wantServices {
		t.Errorf("testPeer: wrong Services - got %v, want %v", p.Services(), s.wantServices)
		return
	}

	if !p.LastPingTime().Equal(s.wantLastPingTime) {
		t.Errorf("testPeer: wrong LastPingTime - got %v, want %v", p.LastPingTime(), s.wantLastPingTime)
		return
	}

	if p.LastPingNonce() != s.wantLastPingNonce {
		t.Errorf("testPeer: wrong LastPingNonce - got %v, want %v", p.LastPingNonce(), s.wantLastPingNonce)
		return
	}

	if p.LastPingMicros() != s.wantLastPingMicros {
		t.Errorf("testPeer: wrong LastPingMicros - got %v, want %v", p.LastPingMicros(), s.wantLastPingMicros)
		return
	}

	if p.VerAckReceived() != s.wantVerAckReceived {
		t.Errorf("testPeer: wrong VerAckReceived - got %v, want %v", p.VerAckReceived(), s.wantVerAckReceived)
		return
	}

	if p.VersionKnown() != s.wantVersionKnown {
		t.Errorf("testPeer: wrong VersionKnown - got %v, want %v", p.VersionKnown(), s.wantVersionKnown)
		return
	}

	if p.ProtocolVersion() != s.wantProtocolVersion {
		t.Errorf("testPeer: wrong ProtocolVersion - got %v, want %v", p.ProtocolVersion(), s.wantProtocolVersion)
		return
	}

	if p.LastBlock() != s.wantLastBlock {
		t.Errorf("testPeer: wrong LastBlock - got %v, want %v", p.LastBlock(), s.wantLastBlock)
		return
	}

	// Allow for a deviation of 1s, as the second may tick when the message is
	// in transit and the protocol doesn't support any further precision.
	if p.TimeOffset() != s.wantTimeOffset && p.TimeOffset() != s.wantTimeOffset-1 {
		t.Errorf("testPeer: wrong TimeOffset - got %v, want %v or %v", p.TimeOffset(),
			s.wantTimeOffset, s.wantTimeOffset-1)
		return
	}

	if p.BytesSent() != s.wantBytesSent {
		t.Errorf("testPeer: wrong BytesSent - got %v, want %v", p.BytesSent(), s.wantBytesSent)
		return
	}

	if p.BytesReceived() != s.wantBytesReceived {
		t.Errorf("testPeer: wrong BytesReceived - got %v, want %v", p.BytesReceived(), s.wantBytesReceived)
		return
	}

	if p.StartingHeight() != s.wantStartingHeight {
		t.Errorf("testPeer: wrong StartingHeight - got %v, want %v", p.StartingHeight(), s.wantStartingHeight)
		return
	}

	if p.Connected() != s.wantConnected {
		t.Errorf("testPeer: wrong Connected - got %v, want %v", p.Connected(), s.wantConnected)
		return
	}

	// These tests are inherently racy. Do range tests on time
	// based attributes.
	lastSend := p.LastSend()
	lastRecv := p.LastRecv()

	stats := p.StatsSnapshot()
	if p.ID() != stats.ID {
		t.Errorf("testPeer: wrong ID - got %v, want %v", p.ID(), stats.ID)
		return
	}

	if p.Addr() != stats.Addr {
		t.Errorf("testPeer: wrong Addr - got %v, want %v", p.Addr(), stats.Addr)
		return
	}

	if !stats.LastSend.Equal(p.LastSend()) && (stats.LastSend.Before(lastSend) || stats.LastSend.After(p.LastSend())) {
		t.Errorf("testPeer: wrong LastSend - got %v, want %v", p.LastSend(), stats.LastSend)
		return
	}

	if !stats.LastRecv.Equal(p.LastRecv()) && (stats.LastRecv.Before(lastRecv) || stats.LastRecv.After(p.LastRecv())) {
		t.Errorf("testPeer: wrong LastRecv - got %v, want %v", p.LastRecv(), stats.LastRecv)
		return
	}
}

// TestPeerConnection tests connection between inbound and outbound peers.
func TestPeerConnection(t *testing.T) {
	tSettings := test.CreateBaseTestSettings(t)
	verack := make(chan struct{})
	peer1Cfg := &peer.Config{
		Listeners: peer.MessageListeners{
			OnVerAck: func(p *peer.Peer, msg *wire.MsgVerAck) {
				verack <- struct{}{}
			},
			OnWrite: func(p *peer.Peer, bytesWritten int, msg wire.Message,
				err error) {
				if _, ok := msg.(*wire.MsgVerAck); ok {
					verack <- struct{}{}
				}
			},
		},
		UserAgentName:          "peer",
		UserAgentVersion:       "1.0",
		UserAgentComments:      []string{"comment"},
		ChainParams:            &chaincfg.MainNetParams,
		ProtocolVersion:        wire.RejectVersion, // Configure with older version
		Services:               0,
		TrickleInterval:        time.Second * 10,
		TstAllowSelfConnection: true,
	}
	peer2Cfg := &peer.Config{
		Listeners:              peer1Cfg.Listeners,
		UserAgentName:          "peer",
		UserAgentVersion:       "1.0",
		UserAgentComments:      []string{"comment"},
		ChainParams:            &chaincfg.MainNetParams,
		Services:               wire.SFNodeNetwork,
		TrickleInterval:        time.Second * 10,
		TstAllowSelfConnection: true,
	}

	wantStats1 := peerStats{
		wantUserAgent:       "peer:1.0(comment)/",
		wantServices:        0,
		wantProtocolVersion: wire.RejectVersion,
		wantConnected:       true,
		wantVersionKnown:    true,
		wantVerAckReceived:  true,
		wantLastPingTime:    time.Time{},
		wantLastPingNonce:   uint64(0),
		wantLastPingMicros:  int64(0),
		wantTimeOffset:      int64(0),
		wantBytesSent:       152, // 128 version + 24 verack
		wantBytesReceived:   152,
	}
	wantStats2 := peerStats{
		wantUserAgent:       "peer:1.0(comment)/",
		wantServices:        wire.SFNodeNetwork,
		wantProtocolVersion: wire.RejectVersion,
		wantConnected:       true,
		wantVersionKnown:    true,
		wantVerAckReceived:  true,
		wantLastPingTime:    time.Time{},
		wantLastPingNonce:   uint64(0),
		wantLastPingMicros:  int64(0),
		wantTimeOffset:      int64(0),
		wantBytesSent:       152, // 128 version + 24 verack
		wantBytesReceived:   152,
	}

	tests := []struct {
		name  string
		setup func() (*peer.Peer, *peer.Peer, error)
	}{
		{
			"basic handshake",
			func() (*peer.Peer, *peer.Peer, error) {
				inConn, outConn := pipe(
					&conn{raddr: "10.0.0.1:8333"},
					&conn{raddr: "10.0.0.2:8333"},
				)
				inPeer := peer.NewInboundPeer(ulogger.TestLogger{}, tSettings, peer1Cfg)
				inPeer.AssociateConnection(inConn)

				outPeer, err := peer.NewOutboundPeer(ulogger.TestLogger{}, tSettings, peer2Cfg, "10.0.0.2:8333")
				if err != nil {
					return nil, nil, err
				}
				outPeer.AssociateConnection(outConn)

				for i := 0; i < 4; i++ {
					select {
					case <-verack:
					case <-time.After(time.Second):
						return nil, nil, errors.New("verack timeout")
					}
				}
				return inPeer, outPeer, nil
			},
		},
		{
			"socks proxy",
			func() (*peer.Peer, *peer.Peer, error) {
				inConn, outConn := pipe(
					&conn{raddr: "10.0.0.1:8333", proxy: true},
					&conn{raddr: "10.0.0.2:8333"},
				)
				inPeer := peer.NewInboundPeer(ulogger.TestLogger{}, tSettings, peer1Cfg)
				inPeer.AssociateConnection(inConn)

				outPeer, err := peer.NewOutboundPeer(ulogger.TestLogger{}, tSettings, peer2Cfg, "10.0.0.2:8333")
				if err != nil {
					return nil, nil, err
				}
				outPeer.AssociateConnection(outConn)

				for i := 0; i < 4; i++ {
					select {
					case <-verack:
					case <-time.After(time.Second):
						return nil, nil, errors.New("verack timeout")
					}
				}
				return inPeer, outPeer, nil
			},
		},
	}
	t.Logf("Running %d tests", len(tests))

	for i, test := range tests {
		inPeer, outPeer, err := test.setup()
		if err != nil {
			t.Errorf("TestPeerConnection setup #%d: unexpected err %v", i, err)
			return
		}

		testPeer(t, inPeer, wantStats2)
		testPeer(t, outPeer, wantStats1)

		inPeer.DisconnectWithInfo("disconnect")
		outPeer.DisconnectWithInfo("disconnect")
		inPeer.WaitForDisconnect()
		outPeer.WaitForDisconnect()
	}
}

// TestPeerListeners tests that the peer listeners are called as expected.
func TestPeerListeners(t *testing.T) {
	verack := make(chan struct{}, 1)
	tSettings := test.CreateBaseTestSettings(t)
	ok := make(chan wire.Message, 20)
	peerCfg := &peer.Config{
		Listeners: peer.MessageListeners{
			OnGetAddr: func(p *peer.Peer, msg *wire.MsgGetAddr) {
				ok <- msg
			},
			OnAddr: func(p *peer.Peer, msg *wire.MsgAddr) {
				ok <- msg
			},
			OnPing: func(p *peer.Peer, msg *wire.MsgPing) {
				ok <- msg
			},
			OnPong: func(p *peer.Peer, msg *wire.MsgPong) {
				ok <- msg
			},
			OnMemPool: func(p *peer.Peer, msg *wire.MsgMemPool) {
				ok <- msg
			},
			OnTx: func(p *peer.Peer, msg *wire.MsgTx) {
				ok <- msg
			},
			OnBlock: func(p *peer.Peer, msg *wire.MsgBlock, buf []byte, payloadSize int64) {
				ok <- msg
			},
			OnInv: func(p *peer.Peer, msg *wire.MsgInv) {
				ok <- msg
			},
			OnHeaders: func(p *peer.Peer, msg *wire.MsgHeaders) {
				ok <- msg
			},
			OnNotFound: func(p *peer.Peer, msg *wire.MsgNotFound) {
				ok <- msg
			},
			OnGetData: func(p *peer.Peer, msg *wire.MsgGetData) {
				ok <- msg
			},
			OnGetBlocks: func(p *peer.Peer, msg *wire.MsgGetBlocks) {
				ok <- msg
			},
			OnGetHeaders: func(p *peer.Peer, msg *wire.MsgGetHeaders) {
				ok <- msg
			},
			OnGetCFilters: func(p *peer.Peer, msg *wire.MsgGetCFilters) {
				ok <- msg
			},
			OnGetCFHeaders: func(p *peer.Peer, msg *wire.MsgGetCFHeaders) {
				ok <- msg
			},
			OnGetCFCheckpt: func(p *peer.Peer, msg *wire.MsgGetCFCheckpt) {
				ok <- msg
			},
			OnCFilter: func(p *peer.Peer, msg *wire.MsgCFilter) {
				ok <- msg
			},
			OnCFHeaders: func(p *peer.Peer, msg *wire.MsgCFHeaders) {
				ok <- msg
			},
			OnFeeFilter: func(p *peer.Peer, msg *wire.MsgFeeFilter) {
				ok <- msg
			},
			OnFilterAdd: func(p *peer.Peer, msg *wire.MsgFilterAdd) {
				ok <- msg
			},
			OnFilterClear: func(p *peer.Peer, msg *wire.MsgFilterClear) {
				ok <- msg
			},
			OnFilterLoad: func(p *peer.Peer, msg *wire.MsgFilterLoad) {
				ok <- msg
			},
			OnMerkleBlock: func(p *peer.Peer, msg *wire.MsgMerkleBlock) {
				ok <- msg
			},
			OnVersion: func(p *peer.Peer, msg *wire.MsgVersion) *wire.MsgReject {
				ok <- msg
				return nil
			},
			OnVerAck: func(p *peer.Peer, msg *wire.MsgVerAck) {
				verack <- struct{}{}
			},
			OnReject: func(p *peer.Peer, msg *wire.MsgReject) {
				ok <- msg
			},
			OnSendHeaders: func(p *peer.Peer, msg *wire.MsgSendHeaders) {
				ok <- msg
			},
		},
		UserAgentName:          "peer",
		UserAgentVersion:       "1.0",
		UserAgentComments:      []string{"comment"},
		ChainParams:            &chaincfg.MainNetParams,
		Services:               wire.SFNodeBloom,
		TrickleInterval:        time.Second * 10,
		TstAllowSelfConnection: true,
	}
	inConn, outConn := pipe(
		&conn{raddr: "10.0.0.1:8333"},
		&conn{raddr: "10.0.0.2:8333"},
	)
	inPeer := peer.NewInboundPeer(ulogger.TestLogger{}, tSettings, peerCfg)
	inPeer.AssociateConnection(inConn)

	peerCfg.Listeners = peer.MessageListeners{
		OnVerAck: func(p *peer.Peer, msg *wire.MsgVerAck) {
			verack <- struct{}{}
		},
	}

	outPeer, err := peer.NewOutboundPeer(ulogger.TestLogger{}, tSettings, peerCfg, "10.0.0.1:8333")
	if err != nil {
		t.Errorf("NewOutboundPeer: unexpected err %v\n", err)
		return
	}

	outPeer.AssociateConnection(outConn)

	for i := 0; i < 2; i++ {
		select {
		case <-verack:
		case <-time.After(time.Second * 1):
			t.Errorf("TestPeerListeners: verack timeout\n")
			return
		}
	}

	tests := []struct {
		listener string
		msg      wire.Message
	}{
		{
			"OnGetAddr",
			wire.NewMsgGetAddr(),
		},
		{
			"OnAddr",
			wire.NewMsgAddr(),
		},
		{
			"OnPing",
			wire.NewMsgPing(42),
		},
		{
			"OnPong",
			wire.NewMsgPong(42),
		},
		{
			"OnMemPool",
			wire.NewMsgMemPool(),
		},
		{
			"OnTx",
			wire.NewMsgTx(wire.TxVersion),
		},
		{
			"OnBlock",
			wire.NewMsgBlock(wire.NewBlockHeader(1,
				&chainhash.Hash{}, &chainhash.Hash{}, 1, 1)),
		},
		{
			"OnInv",
			wire.NewMsgInv(),
		},
		{
			"OnHeaders",
			wire.NewMsgHeaders(),
		},
		{
			"OnNotFound",
			wire.NewMsgNotFound(),
		},
		{
			"OnGetData",
			wire.NewMsgGetData(),
		},
		{
			"OnGetBlocks",
			wire.NewMsgGetBlocks(&chainhash.Hash{}),
		},
		{
			"OnGetHeaders",
			wire.NewMsgGetHeaders(),
		},
		{
			"OnGetCFilters",
			wire.NewMsgGetCFilters(wire.GCSFilterRegular, 0, &chainhash.Hash{}),
		},
		// {
		//	"OnGetCFHeaders",
		//	wire.NewMsgGetCFHeaders(wire.GCSFilterRegular, 0, &chainhash.Hash{}),
		// },
		// {
		//	"OnGetCFCheckpt",
		//	wire.NewMsgGetCFCheckpt(wire.GCSFilterRegular, &chainhash.Hash{}),
		// },
		// {
		//	"OnCFilter",
		//	wire.NewMsgCFilter(wire.GCSFilterRegular, &chainhash.Hash{},
		//		[]byte("payload")),
		// },
		// {
		//	"OnCFHeaders",
		//	wire.NewMsgCFHeaders(),
		// },
		{
			"OnFeeFilter",
			wire.NewMsgFeeFilter(15000),
		},
		// {
		//	"OnFilterAdd",
		//	wire.NewMsgFilterAdd([]byte{0x01}),
		// },
		// {
		//	"OnFilterClear",
		//	wire.NewMsgFilterClear(),
		// },
		// {
		//	"OnFilterLoad",
		//	wire.NewMsgFilterLoad([]byte{0x01}, 10, 0, wire.BloomUpdateNone),
		// },
		{
			"OnMerkleBlock",
			wire.NewMsgMerkleBlock(wire.NewBlockHeader(1,
				&chainhash.Hash{}, &chainhash.Hash{}, 1, 1)),
		},
		// only one version message is allowed
		// only one verack message is allowed
		{
			"OnReject",
			wire.NewMsgReject("block", wire.RejectDuplicate, "dupe block"),
		},
		{
			"OnSendHeaders",
			wire.NewMsgSendHeaders(),
		},
	}
	t.Logf("Running %d tests", len(tests))

	for _, test := range tests {
		// Queue the test message
		outPeer.QueueMessage(test.msg, nil)
		select {
		case <-ok:
		case <-time.After(time.Second * 1):
			t.Errorf("TestPeerListeners: %s timeout", test.listener)
			return
		}
	}

	inPeer.DisconnectWithInfo("")
	outPeer.DisconnectWithInfo("")
}

// TestOutboundPeer tests that the outbound peer works as expected.
func TestOutboundPeer(t *testing.T) {
	peerCfg := &peer.Config{
		NewestBlock: func() (*chainhash.Hash, int32, error) {
			return nil, 0, errors.New("newest block not found")
		},
		UserAgentName:          "peer",
		UserAgentVersion:       "1.0",
		UserAgentComments:      []string{"comment"},
		ChainParams:            &chaincfg.MainNetParams,
		Services:               0,
		TrickleInterval:        time.Second * 10,
		TstAllowSelfConnection: true,
	}
	tSettings := test.CreateBaseTestSettings(t)

	r, w := io.Pipe()
	c := &conn{raddr: "10.0.0.1:8333", Writer: w, Reader: r}

	p, err := peer.NewOutboundPeer(ulogger.TestLogger{}, tSettings, peerCfg, "10.0.0.1:8333")
	if err != nil {
		t.Errorf("NewOutboundPeer: unexpected err - %v\n", err)
		return
	}

	// Test trying to connect twice.
	p.AssociateConnection(c)
	p.AssociateConnection(c)

	disconnected := make(chan struct{})
	go func() {
		p.WaitForDisconnect()
		disconnected <- struct{}{}
	}()

	select {
	case <-disconnected:
		close(disconnected)
	case <-time.After(time.Second):
		t.Fatal("Peer did not automatically disconnect.")
	}

	if p.Connected() {
		t.Fatalf("Should not be connected as NewestBlock produces error.")
	}

	// Test Queue Inv
	fakeBlockHash := &chainhash.Hash{0: 0x00, 1: 0x01}
	fakeInv := wire.NewInvVect(wire.InvTypeBlock, fakeBlockHash)

	// Should be noops as the peer could not connect.
	p.QueueInventory(fakeInv)
	p.AddKnownInventory(fakeInv)
	p.QueueInventory(fakeInv)

	fakeMsg := wire.NewMsgVerAck()
	p.QueueMessage(fakeMsg, nil)

	done := make(chan struct{})
	p.QueueMessage(fakeMsg, done)
	<-done
	p.DisconnectWithInfo("")

	// Test NewestBlock
	var newestBlock = func() (*chainhash.Hash, int32, error) {
		hashStr := "14a0810ac680a3eb3f82edc878cea25ec41d6b790744e5daeef"
		hash, err := chainhash.NewHashFromStr(hashStr)
		if err != nil {
			return nil, 0, err
		}
		return hash, 234439, nil
	}

	peerCfg.NewestBlock = newestBlock
	r1, w1 := io.Pipe()
	c1 := &conn{raddr: "10.0.0.1:8333", Writer: w1, Reader: r1}

	p1, err := peer.NewOutboundPeer(ulogger.TestLogger{}, tSettings, peerCfg, "10.0.0.1:8333")
	if err != nil {
		t.Errorf("NewOutboundPeer: unexpected err - %v\n", err)
		return
	}

	p1.AssociateConnection(c1)

	// Test update latest block
	latestBlockHash, err := chainhash.NewHashFromStr("1a63f9cdff1752e6375c8c76e543a71d239e1a2e5c6db1aa679")
	if err != nil {
		t.Errorf("NewHashFromStr: unexpected err %v\n", err)
		return
	}

	p1.UpdateLastAnnouncedBlock(latestBlockHash)
	p1.UpdateLastBlockHeight(234440)

	if p1.LastAnnouncedBlock() != latestBlockHash {
		t.Errorf("LastAnnouncedBlock: wrong block - got %v, want %v",
			p1.LastAnnouncedBlock(), latestBlockHash)
		return
	}

	// Test Queue Inv after connection
	p1.QueueInventory(fakeInv)
	p1.DisconnectWithInfo("")

	// Test regression
	peerCfg.ChainParams = &chaincfg.RegressionNetParams
	peerCfg.Services = wire.SFNodeBloom
	r2, w2 := io.Pipe()
	c2 := &conn{raddr: "10.0.0.1:8333", Writer: w2, Reader: r2}

	p2, err := peer.NewOutboundPeer(ulogger.TestLogger{}, tSettings, peerCfg, "10.0.0.1:8333")
	if err != nil {
		t.Errorf("NewOutboundPeer: unexpected err - %v\n", err)
		return
	}

	p2.AssociateConnection(c2)

	// Test PushXXX
	var addrs []*wire.NetAddress

	for i := 0; i < 5; i++ {
		na := wire.NetAddress{}
		addrs = append(addrs, &na)
	}

	if _, err := p2.PushAddrMsg(addrs); err != nil {
		t.Errorf("PushAddrMsg: unexpected err %v\n", err)
		return
	}

	if err := p2.PushGetBlocksMsg(nil, &chainhash.Hash{}); err != nil {
		t.Errorf("PushGetBlocksMsg: unexpected err %v\n", err)
		return
	}

	if err := p2.PushGetHeadersMsg(nil, &chainhash.Hash{}); err != nil {
		t.Errorf("PushGetHeadersMsg: unexpected err %v\n", err)
		return
	}

	p2.PushRejectMsg("block", wire.RejectMalformed, "malformed", nil, false)
	p2.PushRejectMsg("block", wire.RejectInvalid, "invalid", nil, false)

	// Test Queue Messages
	p2.QueueMessage(wire.NewMsgGetAddr(), nil)
	p2.QueueMessage(wire.NewMsgPing(1), nil)
	p2.QueueMessage(wire.NewMsgMemPool(), nil)
	p2.QueueMessage(wire.NewMsgGetData(), nil)
	p2.QueueMessage(wire.NewMsgGetHeaders(), nil)
	p2.QueueMessage(wire.NewMsgFeeFilter(20000), nil)

	p2.DisconnectWithInfo("")
}

// Tests that the node disconnects from peers with an unsupported protocol
// version.
func TestUnsupportedVersionPeer(t *testing.T) {
	peerCfg := &peer.Config{
		UserAgentName:          "peer",
		UserAgentVersion:       "1.0",
		UserAgentComments:      []string{"comment"},
		ChainParams:            &chaincfg.MainNetParams,
		Services:               0,
		TrickleInterval:        time.Second * 10,
		TstAllowSelfConnection: true,
	}
	tSettings := test.CreateBaseTestSettings(t)

	localNA := wire.NewNetAddressIPPort(
		net.ParseIP("10.0.0.1"),
		uint16(8333),
		wire.SFNodeNetwork,
	)
	remoteNA := wire.NewNetAddressIPPort(
		net.ParseIP("10.0.0.2"),
		uint16(8333),
		wire.SFNodeNetwork,
	)
	localConn, remoteConn := pipe(
		&conn{laddr: "10.0.0.1:8333", raddr: "10.0.0.2:8333"},
		&conn{laddr: "10.0.0.2:8333", raddr: "10.0.0.1:8333"},
	)

	p, err := peer.NewOutboundPeer(ulogger.TestLogger{}, tSettings, peerCfg, "10.0.0.1:8333")
	if err != nil {
		t.Fatalf("NewOutboundPeer: unexpected err - %v\n", err)
	}

	p.AssociateConnection(localConn)

	// Read outbound messages to peer into a channel
	outboundMessages := make(chan wire.Message)

	go func() {
		for {
			_, msg, _, err := wire.ReadMessageN(
				remoteConn,
				p.ProtocolVersion(),
				peerCfg.ChainParams.Net,
			)
			if err == io.EOF {
				close(outboundMessages)
				return
			}

			if err != nil {
				t.Errorf("Error reading message from local node: %v\n", err)
				return
			}

			outboundMessages <- msg
		}
	}()

	// Read version message sent to remote peer.
	select {
	case msg := <-outboundMessages:
		if _, ok := msg.(*wire.MsgVersion); !ok {
			t.Fatalf("Expected version message, got [%s]", msg.Command())
		}
	case <-time.After(time.Second):
		t.Fatal("Peer did not send version message")
	}

	// Remote peer writes version message advertising invalid protocol version 1.
	// Per Bitcoin protocol, VERACK is a reply to the remote VERSION, so the
	// outbound peer must wait for this message before sending VERACK.
	invalidVersionMsg := wire.NewMsgVersion(remoteNA, localNA, 0, 0)
	invalidVersionMsg.ProtocolVersion = 1

	_, err = wire.WriteMessageN(
		remoteConn.Writer,
		invalidVersionMsg,
		uint32(invalidVersionMsg.ProtocolVersion),
		peerCfg.ChainParams.Net,
	)
	if err != nil {
		t.Fatalf("wire.WriteMessageN: unexpected err - %v\n", err)
	}

	// Expect peer to disconnect automatically
	disconnected := make(chan struct{})
	go func() {
		p.WaitForDisconnect()
		disconnected <- struct{}{}
	}()

	select {
	case <-disconnected:
		close(disconnected)
	case <-time.After(time.Second):
		t.Fatal("Peer did not automatically disconnect")
	}

	// Expect no further outbound messages from peer
	select {
	case msg, chanOpen := <-outboundMessages:
		if chanOpen {
			t.Fatalf("Expected no further messages, received [%s]", msg.Command())
		}
	case <-time.After(time.Second):
		t.Fatal("Timeout waiting for remote reader to close")
	}
}

// TestOutboundHandshakeOrder verifies that an outbound peer strictly follows
// the Bitcoin wire protocol handshake order: VERSION -> (receive remote
// VERSION) -> VERACK. VERACK must not be sent before the remote VERSION has
// been received. Standard BSV nodes close the connection with a broken pipe
// error when VERACK arrives before they have sent their own VERSION.
func TestOutboundHandshakeOrder(t *testing.T) {
	peerCfg := &peer.Config{
		UserAgentName:          "peer",
		UserAgentVersion:       "1.0",
		ChainParams:            &chaincfg.MainNetParams,
		Services:               0,
		TrickleInterval:        time.Second * 10,
		TstAllowSelfConnection: true,
	}
	tSettings := test.CreateBaseTestSettings(t)

	localNA := wire.NewNetAddressIPPort(
		net.ParseIP("10.0.0.1"),
		uint16(8333),
		wire.SFNodeNetwork,
	)
	remoteNA := wire.NewNetAddressIPPort(
		net.ParseIP("10.0.0.2"),
		uint16(8333),
		wire.SFNodeNetwork,
	)
	localConn, remoteConn := pipe(
		&conn{laddr: "10.0.0.1:8333", raddr: "10.0.0.2:8333"},
		&conn{laddr: "10.0.0.2:8333", raddr: "10.0.0.1:8333"},
	)

	p, err := peer.NewOutboundPeer(ulogger.TestLogger{}, tSettings, peerCfg, "10.0.0.1:8333")
	if err != nil {
		t.Fatalf("NewOutboundPeer: unexpected err - %v\n", err)
	}

	p.AssociateConnection(localConn)

	outboundMessages := make(chan wire.Message, 4)
	go func() {
		for {
			_, msg, _, readErr := wire.ReadMessageN(
				remoteConn,
				p.ProtocolVersion(),
				peerCfg.ChainParams.Net,
			)
			if readErr != nil {
				close(outboundMessages)
				return
			}
			outboundMessages <- msg
		}
	}()

	// The first outbound message must be VERSION.
	select {
	case msg := <-outboundMessages:
		if _, ok := msg.(*wire.MsgVersion); !ok {
			t.Fatalf("Expected VERSION as first message, got [%s]", msg.Command())
		}
	case <-time.After(time.Second):
		t.Fatal("Peer did not send VERSION")
	}

	// No further outbound messages must arrive before the remote sends its
	// VERSION. If we receive anything (e.g. a premature VERACK), the outbound
	// handshake is broken.
	select {
	case msg := <-outboundMessages:
		t.Fatalf("Peer sent [%s] before remote VERSION was delivered; expected VERACK only after remote VERSION", msg.Command())
	case <-time.After(100 * time.Millisecond):
		// Expected: peer is blocked reading our VERSION.
	}

	// Send the remote VERSION.
	remoteVer := wire.NewMsgVersion(remoteNA, localNA, 0, 0)
	remoteVer.ProtocolVersion = int32(peerCfg.ProtocolVersion)
	if remoteVer.ProtocolVersion == 0 {
		remoteVer.ProtocolVersion = int32(wire.ProtocolVersion)
	}
	if _, err = wire.WriteMessageN(
		remoteConn.Writer,
		remoteVer,
		uint32(remoteVer.ProtocolVersion),
		peerCfg.ChainParams.Net,
	); err != nil {
		t.Fatalf("wire.WriteMessageN: unexpected err - %v\n", err)
	}

	// Only now may VERACK be sent.
	select {
	case msg := <-outboundMessages:
		if _, ok := msg.(*wire.MsgVerAck); !ok {
			t.Fatalf("Expected VERACK after remote VERSION, got [%s]", msg.Command())
		}
	case <-time.After(time.Second):
		t.Fatal("Peer did not send VERACK after remote VERSION")
	}

	p.DisconnectWithInfo("test complete")
}

// TestDuplicateVersionMsg ensures that receiving a version message after one
// has already been received results in the peer being disconnected.
func TestDuplicateVersionMsg(t *testing.T) {
	// Create a pair of peers that are connected to each other using a fake
	// connection.
	verack := make(chan struct{})
	peerCfg := &peer.Config{
		Listeners: peer.MessageListeners{
			OnVerAck: func(p *peer.Peer, msg *wire.MsgVerAck) {
				verack <- struct{}{}
			},
		},
		UserAgentName:          "peer",
		UserAgentVersion:       "1.0",
		ChainParams:            &chaincfg.MainNetParams,
		Services:               0,
		TstAllowSelfConnection: true,
	}
	inConn, outConn := pipe(
		&conn{laddr: "10.0.0.1:9108", raddr: "10.0.0.2:9108"},
		&conn{laddr: "10.0.0.2:9108", raddr: "10.0.0.1:9108"},
	)
	tSettings := test.CreateBaseTestSettings(t)

	outPeer, err := peer.NewOutboundPeer(ulogger.TestLogger{}, tSettings, peerCfg, inConn.laddr)
	if err != nil {
		t.Fatalf("NewOutboundPeer: unexpected err: %v\n", err)
	}

	outPeer.AssociateConnection(outConn)

	inPeer := peer.NewInboundPeer(ulogger.TestLogger{}, tSettings, peerCfg)
	inPeer.AssociateConnection(inConn)

	// Wait for the veracks from the initial protocol version negotiation.
	for i := 0; i < 2; i++ {
		select {
		case <-verack:
		case <-time.After(time.Second):
			t.Fatal("verack timeout")
		}
	}
	// Queue a duplicate version message from the outbound peer and wait until
	// it is sent.
	done := make(chan struct{})
	outPeer.QueueMessage(&wire.MsgVersion{}, done)
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("send duplicate version timeout")
	}
	// Ensure the peer that is the recipient of the duplicate version closes the
	// connection.
	disconnected := make(chan struct{}, 1)
	go func() {
		inPeer.WaitForDisconnect()
		disconnected <- struct{}{}
	}()
	select {
	case <-disconnected:
	case <-time.After(time.Second):
		t.Fatal("peer did not disconnect")
	}
}

// TestBanPeer tests banning peers.
func TestBanPeer(t *testing.T) {
	t.Skip("skipping ban peer test")

	tSettings := test.CreateBaseTestSettings(t)
	verack := make(chan struct{})
	peer1Cfg := &peer.Config{
		Listeners: peer.MessageListeners{
			OnVerAck: func(p *peer.Peer, msg *wire.MsgVerAck) {
				verack <- struct{}{}
			},
			OnWrite: func(p *peer.Peer, bytesWritten int, msg wire.Message,
				err error) {
				if _, ok := msg.(*wire.MsgVerAck); ok {
					verack <- struct{}{}
				}
			},
		},
		UserAgentName:          "peer",
		UserAgentVersion:       "1.0",
		UserAgentComments:      []string{"comment"},
		ChainParams:            &chaincfg.MainNetParams,
		ProtocolVersion:        wire.RejectVersion, // Configure with older version
		Services:               0,
		TrickleInterval:        time.Second * 10,
		TstAllowSelfConnection: true,
	}
	peer2Cfg := &peer.Config{
		Listeners:              peer1Cfg.Listeners,
		UserAgentName:          "peer",
		UserAgentVersion:       "1.0",
		UserAgentComments:      []string{"comment"},
		ChainParams:            &chaincfg.MainNetParams,
		Services:               wire.SFNodeNetwork,
		TrickleInterval:        time.Second * 10,
		TstAllowSelfConnection: true,
	}

	wantStats1 := peerStats{
		wantUserAgent:       "peer:1.0(comment)/",
		wantServices:        0,
		wantProtocolVersion: wire.RejectVersion,
		wantConnected:       true,
		wantVersionKnown:    true,
		wantVerAckReceived:  true,
		wantLastPingTime:    time.Time{},
		wantLastPingNonce:   uint64(0),
		wantLastPingMicros:  int64(0),
		wantTimeOffset:      int64(0),
		wantBytesSent:       152, // 128 version + 24 verack
		wantBytesReceived:   152,
	}
	wantStats2 := peerStats{
		wantUserAgent:       "peer:1.0(comment)/",
		wantServices:        wire.SFNodeNetwork,
		wantProtocolVersion: wire.RejectVersion,
		wantConnected:       true,
		wantVersionKnown:    true,
		wantVerAckReceived:  true,
		wantLastPingTime:    time.Time{},
		wantLastPingNonce:   uint64(0),
		wantLastPingMicros:  int64(0),
		wantTimeOffset:      int64(0),
		wantBytesSent:       152, // 128 version + 24 verack
		wantBytesReceived:   152,
	}

	tests := []struct {
		name  string
		setup func() (*peer.Peer, *peer.Peer, error)
	}{
		{
			"basic handshake",
			func() (*peer.Peer, *peer.Peer, error) {
				inConn, outConn := pipe(
					&conn{raddr: "10.0.0.1:8333"},
					&conn{raddr: "10.0.0.2:8333"},
				)
				inPeer := peer.NewInboundPeer(ulogger.TestLogger{}, tSettings, peer1Cfg)
				inPeer.AssociateConnection(inConn)

				outPeer, err := peer.NewOutboundPeer(ulogger.TestLogger{}, tSettings, peer2Cfg, "10.0.0.2:8333")
				if err != nil {
					return nil, nil, err
				}
				outPeer.AssociateConnection(outConn)

				for i := 0; i < 4; i++ {
					select {
					case <-verack:
					case <-time.After(time.Second):
						return nil, nil, errors.New("verack timeout")
					}
				}
				return inPeer, outPeer, nil
			},
		},
		{
			"socks proxy",
			func() (*peer.Peer, *peer.Peer, error) {
				inConn, outConn := pipe(
					&conn{raddr: "10.0.0.1:8333", proxy: true},
					&conn{raddr: "10.0.0.2:8333"},
				)
				inPeer := peer.NewInboundPeer(ulogger.TestLogger{}, tSettings, peer1Cfg)
				inPeer.AssociateConnection(inConn)

				outPeer, err := peer.NewOutboundPeer(ulogger.TestLogger{}, tSettings, peer2Cfg, "10.0.0.2:8333")
				if err != nil {
					return nil, nil, err
				}
				outPeer.AssociateConnection(outConn)

				for i := 0; i < 4; i++ {
					select {
					case <-verack:
					case <-time.After(time.Second):
						return nil, nil, errors.New("verack timeout")
					}
				}
				return inPeer, outPeer, nil
			},
		},
	}
	t.Logf("Running %d tests", len(tests))

	for i, test := range tests {
		inPeer, outPeer, err := test.setup()
		if err != nil {
			t.Errorf("TestPeerConnection setup #%d: unexpected err %v", i, err)
			return
		}

		testPeer(t, inPeer, wantStats2)
		testPeer(t, outPeer, wantStats1)

		ctx := context.Background()

		s, err := NewTestServer(t)
		require.NoError(t, err)

		err = s.Init(ctx)
		require.NoError(t, err)

		ready := make(chan struct{})
		errc := make(chan error)

		go func() {
			defer func() {
				if r := recover(); r != nil {
					errc <- errors.New("server startup panicked")
				}
			}()

			err = s.Start(ctx, ready)
			if err != nil {
				panic(err)
			}
		}()

		select {
		case <-ready:
			// Server started successfully
		case err := <-errc:
			// Handle the error
			t.Fatalf("server startup failed: %v", err)
		}

		time.Sleep(1 * time.Second)

		r, err := s.BanPeer(ctx, &peer_api.BanPeerRequest{Addr: outPeer.Addr()})
		require.NoError(t, err)
		require.True(t, r.Ok)

		inPeer.DisconnectWithInfo("")
		outPeer.DisconnectWithInfo("")
		inPeer.WaitForDisconnect()
		outPeer.WaitForDisconnect()
	}
}

// flakyWriter wraps an io.Writer and starts failing all writes once armed,
// used to simulate a write error on an otherwise healthy connection.
type flakyWriter struct {
	w    io.Writer
	fail atomic.Bool
}

func (f *flakyWriter) Write(p []byte) (int, error) {
	if f.fail.Load() {
		return 0, errors.New("simulated write error")
	}

	return f.w.Write(p)
}

func (f *flakyWriter) setUnderlying(w io.Writer) {
	f.w = w
}

// opErrorWriter wraps an io.Writer and, once armed, fails all writes with a
// *net.OpError wrapping syscall.EPIPE - a real broken-pipe error of the kind
// shouldLogWriteError is meant to suppress from the Warnf path, unlike
// flakyWriter's plain errors.New error.
type opErrorWriter struct {
	w    io.Writer
	fail atomic.Bool
}

func (f *opErrorWriter) Write(p []byte) (int, error) {
	if f.fail.Load() {
		return 0, &net.OpError{Op: "write", Net: "tcp", Err: syscall.EPIPE}
	}

	return f.w.Write(p)
}

func (f *opErrorWriter) setUnderlying(w io.Writer) {
	f.w = w
}

// logCounts is a minimal ulogger.Logger spy that counts calls per level,
// embedding ulogger.TestLogger for the rest of the interface.
type logCounts struct {
	ulogger.TestLogger
	debug atomic.Int32
	info  atomic.Int32
	warn  atomic.Int32
}

func (l *logCounts) Debugf(format string, args ...interface{}) { l.debug.Add(1) }
func (l *logCounts) Infof(format string, args ...interface{})  { l.info.Add(1) }
func (l *logCounts) Warnf(format string, args ...interface{})  { l.warn.Add(1) }

// connectedPeerPairWithFlakyWriter wires an in-memory peer pair whose outbound
// side writes through a flakyWriter, waits for the handshake to complete and
// arms the write failure. The returned cleanup function tears the inbound peer
// down.
func connectedPeerPairWithFlakyWriter(t *testing.T) (outPeer *peer.Peer, cleanup func()) {
	t.Helper()

	fw := &flakyWriter{}

	outPeer, cleanup = connectedPeerPairWithOutWriter(t, fw, ulogger.TestLogger{})

	// Arm the write failure only after the handshake has completed so the
	// negotiation itself is unaffected.
	fw.fail.Store(true)

	return outPeer, cleanup
}

// connectedPeerPairWithOutWriter wires an in-memory peer pair whose outbound
// side writes through w (with w.w set to the underlying pipe writer), using
// outLogger as the outbound peer's logger. Returns once the handshake has
// completed. The returned cleanup function tears the inbound peer down.
func connectedPeerPairWithOutWriter(t *testing.T, w interface {
	io.Writer
	setUnderlying(io.Writer)
}, outLogger ulogger.Logger,
) (outPeer *peer.Peer, cleanup func()) {
	t.Helper()

	tSettings := test.CreateBaseTestSettings(t)
	verack := make(chan struct{}, 4)
	cfg := &peer.Config{
		Listeners: peer.MessageListeners{
			OnVerAck: func(p *peer.Peer, msg *wire.MsgVerAck) {
				verack <- struct{}{}
			},
			OnWrite: func(p *peer.Peer, bytesWritten int, msg wire.Message, err error) {
				if _, ok := msg.(*wire.MsgVerAck); ok {
					verack <- struct{}{}
				}
			},
		},
		UserAgentName:          "peer",
		UserAgentVersion:       "1.0",
		UserAgentComments:      []string{"comment"},
		ChainParams:            &chaincfg.MainNetParams,
		Services:               0,
		TrickleInterval:        time.Second * 10,
		TstAllowSelfConnection: true,
	}

	inR, outW := io.Pipe()
	outR, inW := io.Pipe()

	w.setUnderlying(outW)

	inConn := &conn{raddr: "10.0.0.1:8333", Reader: inR, Writer: inW, Closer: inW}
	outConn := &conn{raddr: "10.0.0.2:8333", Reader: outR, Writer: w, Closer: outW}

	inPeer := peer.NewInboundPeer(ulogger.TestLogger{}, tSettings, cfg)
	inPeer.AssociateConnection(inConn)

	outPeer, err := peer.NewOutboundPeer(outLogger, tSettings, cfg, "10.0.0.2:8333")
	require.NoError(t, err)
	outPeer.AssociateConnection(outConn)

	for i := 0; i < 4; i++ {
		select {
		case <-verack:
		case <-time.After(2 * time.Second):
			t.Fatal("verack timeout")
		}
	}

	require.True(t, outPeer.Connected())

	return outPeer, func() {
		inPeer.DisconnectWithInfo("test cleanup")
		inPeer.WaitForDisconnect()
	}
}

// TestOutHandlerDisconnectsOnWriteError verifies that outHandler disconnects
// the peer when writeMessage fails, rather than leaving the peer marked as
// connected with a permanently stalled send queue.
func TestOutHandlerDisconnectsOnWriteError(t *testing.T) {
	outPeer, cleanup := connectedPeerPairWithFlakyWriter(t)
	defer cleanup()

	outPeer.QueueMessage(wire.NewMsgPing(1), nil)

	disconnected := make(chan struct{})
	go func() {
		outPeer.WaitForDisconnect()
		close(disconnected)
	}()

	select {
	case <-disconnected:
	case <-time.After(3 * time.Second):
		t.Fatal("peer did not disconnect after a write error")
	}

	require.False(t, outPeer.Connected())
}

// TestOutHandlerDisconnectsBeforeSignallingDoneChan verifies outHandler
// disconnects before it signals the message's done channel, matching upstream
// btcd. A caller that queued a message with an unbuffered done channel and is
// not yet receiving on it must not be able to wedge outHandler before the peer
// is torn down.
func TestOutHandlerDisconnectsBeforeSignallingDoneChan(t *testing.T) {
	outPeer, cleanup := connectedPeerPairWithFlakyWriter(t)
	defer cleanup()

	// Deliberately unbuffered and not received from until the disconnect has
	// been observed below.
	doneChan := make(chan struct{})
	outPeer.QueueMessage(wire.NewMsgPing(1), doneChan)

	disconnected := make(chan struct{})
	go func() {
		outPeer.WaitForDisconnect()
		close(disconnected)
	}()

	select {
	case <-disconnected:
	case <-time.After(3 * time.Second):
		t.Fatal("peer did not disconnect after a write error before signalling doneChan")
	}

	require.False(t, outPeer.Connected())

	// Release outHandler now that the disconnect has been observed.
	select {
	case <-doneChan:
	case <-time.After(3 * time.Second):
		t.Fatal("outHandler never signalled doneChan")
	}
}

// TestDisconnectLogsOnceForRepeatedCalls verifies the disconnect reason is
// logged by the call that actually disconnects the peer and not by the
// subsequent no-op calls. Repeated write errors - one per message still queued
// on a dead connection - must not each add a log line.
func TestDisconnectLogsOnceForRepeatedCalls(t *testing.T) {
	tSettings := test.CreateBaseTestSettings(t)
	p := peer.NewInboundPeer(ulogger.TestLogger{}, tSettings, &peer.Config{
		ChainParams: &chaincfg.MainNetParams,
	})

	logged := 0
	logFunc := func(format string, args ...interface{}) {
		logged++
	}

	p.DisconnectWithLogFunc("write error: first", logFunc)
	p.DisconnectWithLogFunc("write error: second", logFunc)
	p.DisconnectWithLogFunc("write error: third", logFunc)

	require.Equal(t, 1, logged, "only the call that actually disconnects the peer should log at the caller's requested level")
}

// TestDisconnectDemotesLosingCallsToDebug verifies that disconnect reasons
// which lose the race to be first (e.g. a ban or protocol-violation reason
// racing an unrelated "peer stalled" disconnect) are not silently dropped -
// they are still logged, just at Debug rather than the caller's requested
// level, so they stay attributable without adding noise at the default level.
func TestDisconnectDemotesLosingCallsToDebug(t *testing.T) {
	tSettings := test.CreateBaseTestSettings(t)
	spy := &logCounts{}
	p := peer.NewInboundPeer(spy, tSettings, &peer.Config{
		ChainParams: &chaincfg.MainNetParams,
	})

	p.DisconnectWithInfo("peer stalled")
	p.DisconnectWithWarning("ban score exceeded")

	require.Equal(t, int32(1), spy.info.Load(), "the winning call logs at its requested level")
	require.Equal(t, int32(0), spy.warn.Load(), "the losing call must not log at its requested level")
	require.Equal(t, int32(1), spy.debug.Load(), "the losing call's reason must still be logged, at Debug")
}

// TestOutHandlerLogsWriteErrorDisconnect verifies that the outHandler write
// error disconnect is always visible at Info level - even for a suppressed
// *net.OpError like a broken pipe, where shouldLogWriteError gates only the
// additional Warnf about the raw error, not the disconnect event itself.
func TestOutHandlerLogsWriteErrorDisconnect(t *testing.T) {
	t.Run("suppressed net.OpError still logs the disconnect at Info", func(t *testing.T) {
		ow := &opErrorWriter{}
		spy := &logCounts{}

		outPeer, cleanup := connectedPeerPairWithOutWriter(t, ow, spy)
		defer cleanup()

		ow.fail.Store(true)
		outPeer.QueueMessage(wire.NewMsgPing(1), nil)
		outPeer.WaitForDisconnect()

		require.Equal(t, int32(1), spy.info.Load(), "disconnect event must log at Info even for a suppressed error")
		require.Equal(t, int32(0), spy.warn.Load(), "a broken-pipe net.OpError must not also log a Warnf")
	})

	t.Run("a plain write error still logs a Warnf in addition to the Info disconnect", func(t *testing.T) {
		fw := &flakyWriter{}
		spy := &logCounts{}

		outPeer, cleanup := connectedPeerPairWithOutWriter(t, fw, spy)
		defer cleanup()

		fw.fail.Store(true)
		outPeer.QueueMessage(wire.NewMsgPing(1), nil)
		outPeer.WaitForDisconnect()

		require.Equal(t, int32(1), spy.info.Load(), "disconnect event must log at Info")
		require.Equal(t, int32(1), spy.warn.Load(), "a non-suppressed write error must also log a Warnf")
	})
}

func NewTestServer(t *testing.T) (*legacy.Server, error) {
	logger := ulogger.NewVerboseTestLogger(t)
	tSettings := test.CreateBaseTestSettings(t)
	tSettings.Legacy.ListenAddresses = []string{"127.0.0.1:9876"}
	// tSettings.Legacy.ConnectPeers = []string{"78.110.160.26:8333", "13.231.149.50:8333", "54.249.171.1:8333", "3.68.157.171:37890"}
	tSettings.Legacy.ConnectPeers = []string{"10.0.0.1:8333", "10.0.0.2:8333"}

	blockchainStoreURL, _ := url.Parse("sqlitememory://")

	blockchainStore, err := blockchainstore.NewStore(logger, blockchainStoreURL, tSettings)
	if err != nil {
		return nil, err
	}

	blockchainClient, err := blockchain.NewLocalClient(logger, tSettings, blockchainStore, nil, nil)
	if err != nil {
		return nil, err
	}

	memStore := memory.New()

	ctx := context.Background()

	utxoStoreURL, err := url.Parse("sqlitememory:///test")
	require.NoError(t, err)

	utxoStore, err := sql.New(ctx, logger, tSettings, utxoStoreURL)
	require.NoError(t, err)

	return legacy.New(logger, tSettings, legacy.Dependencies{
		BlockchainClient: blockchainClient,
		SubtreeStore:     memStore,
		TempStore:        memStore,
		UtxoStore:        utxoStore,
	}), nil
}
