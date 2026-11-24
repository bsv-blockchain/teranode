package transport

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-wire"
	"github.com/stretchr/testify/require"
)

func testConfig() Config {
	return Config{
		Net:             wire.MainNet,
		ProtocolVersion: wire.ProtocolVersion,
		SendBudgetBytes: 1 << 20,
		RecvQueueLen:    32,
		WriteTimeout:    5 * time.Second,
	}
}

func TestConnRoundTrip(t *testing.T) {
	a, b := net.Pipe()
	ca := New(a, testConfig())
	cb := New(b, testConfig())
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ca.Start(ctx)
	cb.Start(ctx)

	require.NoError(t, ca.Send(wire.NewMsgPing(42)))

	select {
	case msg := <-cb.Inbound():
		ping, ok := msg.(*wire.MsgPing)
		require.True(t, ok)
		require.Equal(t, uint64(42), ping.Nonce)
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for ping")
	}
}

func TestConnCloseUnblocksInbound(t *testing.T) {
	a, b := net.Pipe()
	ca := New(a, testConfig())
	cb := New(b, testConfig())
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ca.Start(ctx)
	cb.Start(ctx)

	require.NoError(t, ca.Close())

	select {
	case _, open := <-cb.Inbound():
		require.False(t, open)
	case <-time.After(5 * time.Second):
		t.Fatal("inbound channel did not close")
	}
	<-cb.Done()
	require.Error(t, cb.Err())
}

func TestConnSendQueueFull(t *testing.T) {
	a, _ := net.Pipe() // no reader on the far side: writer will block
	ca := New(a, Config{
		Net: wire.MainNet, ProtocolVersion: wire.ProtocolVersion,
		SendBudgetBytes: 64, RecvQueueLen: 1, WriteTimeout: 100 * time.Millisecond,
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ca.Start(ctx)

	var sawFull bool

	for i := 0; i < 100; i++ {
		if err := ca.Send(wire.NewMsgPing(uint64(i))); err != nil {
			require.ErrorIs(t, err, ErrSendQueueFull)

			sawFull = true

			break
		}
	}

	require.True(t, sawFull, "send queue never reported full")
}
