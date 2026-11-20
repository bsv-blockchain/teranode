package transport

import (
	"bytes"
	"context"
	"io"
	"net"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-wire"
	"github.com/stretchr/testify/require"
)

// pipeConn builds one Conn of the given stream type over a net.Pipe and hands
// back the far end. Both ends are closed when the test finishes, so a test
// that leaves a writer parked on the pipe still unwinds.
func pipeConn(t *testing.T, streamType wire.StreamType, pver uint32) (*Conn, net.Conn) {
	t.Helper()

	local, remote := net.Pipe()

	cfg := testConfig()
	cfg.StreamType = streamType
	cfg.ProtocolVersion = pver

	c := New(local, cfg)

	t.Cleanup(func() {
		_ = c.Close()
		_ = remote.Close()
	})

	return c, remote
}

// farSideDeadline bounds every far-side read. Without it a message routed to
// the wrong stream leaves the reader parked on the pipe for ever, so a routing
// regression would hang the suite instead of failing it.
const farSideDeadline = 5 * time.Second

// readCommand reads one whole frame off the far side with go-wire's own reader
// and returns its command. It is the far-side assertion for routing: a message
// that reached the other stream never arrives here.
func readCommand(t *testing.T, r net.Conn) string {
	t.Helper()

	require.NoError(t, r.SetReadDeadline(time.Now().Add(farSideDeadline)))

	_, msg, _, err := wire.ReadMessageWithEncodingN(r, wire.ProtocolVersion, wire.MainNet, wire.BaseEncoding)
	require.NoError(t, err)

	return msg.Command()
}

// readHeaderCommand reads the 24 byte basic message header only and returns the
// command field. SendBlock streams its payload, so the body is drained
// separately by the caller.
func readHeaderCommand(t *testing.T, r net.Conn) string {
	t.Helper()

	require.NoError(t, r.SetReadDeadline(time.Now().Add(farSideDeadline)))

	var hdr [wire.MessageHeaderSize]byte

	_, err := io.ReadFull(r, hdr[:])
	require.NoError(t, err)

	return string(bytes.TrimRight(hdr[4:16], "\x00"))
}

func TestAssociationIsAPeerConn(t *testing.T) {
	var _ PeerConn = (*Association)(nil)
}

// association.cpp:137-160 MoveStream refuses to overwrite an existing stream in
// the target association, so one stream per type is the invariant.
func TestAssociation_AttachRefusesDuplicateType(t *testing.T) {
	general, _ := pipeConn(t, wire.StreamTypeGeneral, wire.ProtocolVersion)

	a := NewAssociation(general, []byte{0xaa, 0xbb})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	a.Start(ctx)

	defer func() { _ = a.Close() }()

	require.Equal(t, []byte{0xaa, 0xbb}, a.ID())
	require.Equal(t, wire.DefaultStreamPolicy, a.Policy().Name())
	require.True(t, a.HasStream(wire.StreamTypeGeneral))
	require.False(t, a.HasStream(wire.StreamTypeData1))

	data1a, _ := pipeConn(t, wire.StreamTypeData1, wire.ProtocolVersion)
	require.NoError(t, a.Attach(data1a))
	require.True(t, a.HasStream(wire.StreamTypeData1))
	require.ElementsMatch(t, []wire.StreamType{wire.StreamTypeGeneral, wire.StreamTypeData1}, a.Streams())

	data1b, _ := pipeConn(t, wire.StreamTypeData1, wire.ProtocolVersion)
	err := a.Attach(data1b)
	require.ErrorIs(t, err, ErrStreamExists)

	select {
	case <-data1b.Done():
	case <-time.After(5 * time.Second):
		t.Fatal("the refused stream was not closed")
	}

	require.Len(t, a.Streams(), 2)

	general2, _ := pipeConn(t, wire.StreamTypeGeneral, wire.ProtocolVersion)
	require.ErrorIs(t, a.Attach(general2), ErrStreamExists)
}

// stream_policy.cpp:187-195 BlockPriority routes block, ping and pong to DATA1
// when it is present, and association.cpp:205-210 falls back to GENERAL when
// the requested type is absent.
func TestAssociation_RoutesByPolicy(t *testing.T) {
	general, generalRemote := pipeConn(t, wire.StreamTypeGeneral, wire.ProtocolVersion)

	a := NewAssociation(general, []byte{0x01})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	a.Start(ctx)

	defer func() { _ = a.Close() }()

	blockPriority, ok := PolicyForName(wire.BlockPriorityStreamPolicy)
	require.True(t, ok)

	a.SetPolicy(blockPriority)
	require.Equal(t, wire.BlockPriorityStreamPolicy, a.Policy().Name())

	require.NoError(t, a.Send(wire.NewMsgPing(1)))
	require.Equal(t, wire.CmdPing, readCommand(t, generalRemote))

	data1, data1Remote := pipeConn(t, wire.StreamTypeData1, wire.ProtocolVersion)
	require.NoError(t, a.Attach(data1))

	require.NoError(t, a.Send(wire.NewMsgPing(2)))
	require.Equal(t, wire.CmdPing, readCommand(t, data1Remote))

	require.NoError(t, a.Send(wire.NewMsgInv()))
	require.Equal(t, wire.CmdInv, readCommand(t, generalRemote))

	require.NoError(t, a.SendPriority(wire.NewMsgPong(3)))
	require.Equal(t, wire.CmdPong, readCommand(t, generalRemote))

	payload := blockPayload(t, testMsgBlock(t, 1))

	sendErr := make(chan error, 1)

	go func() {
		calls := 0
		sendErr <- a.SendBlock(ctx, BlockSendRequest{
			Length: uint64(len(payload)),
			Open:   openerFor(payload, &calls),
		})
	}()

	require.Equal(t, wire.CmdBlock, readHeaderCommand(t, data1Remote))

	require.NoError(t, data1Remote.SetReadDeadline(time.Now().Add(farSideDeadline)))

	body := make([]byte, len(payload))
	_, err := io.ReadFull(data1Remote, body)
	require.NoError(t, err)
	require.Equal(t, payload, body)

	select {
	case err := <-sendErr:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("SendBlock did not return")
	}
}

// Both streams' inbound messages reach the one Inbound channel.
func TestAssociation_MergesInboundFromEveryStream(t *testing.T) {
	general, generalRemote := pipeConn(t, wire.StreamTypeGeneral, wire.ProtocolVersion)

	a := NewAssociation(general, []byte{0x02})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	a.Start(ctx)

	defer func() { _ = a.Close() }()

	data1, data1Remote := pipeConn(t, wire.StreamTypeData1, wire.ProtocolVersion)
	require.NoError(t, a.Attach(data1))

	writeErr := make(chan error, 2)

	go func() {
		_, err := wire.WriteMessageWithEncodingN(data1Remote, wire.NewMsgPong(77), wire.ProtocolVersion, wire.MainNet, wire.BaseEncoding)
		writeErr <- err
	}()

	go func() {
		_, err := wire.WriteMessageWithEncodingN(generalRemote, wire.NewMsgInv(), wire.ProtocolVersion, wire.MainNet, wire.BaseEncoding)
		writeErr <- err
	}()

	seen := map[string]bool{}

	for len(seen) < 2 {
		select {
		case msg, open := <-a.Inbound():
			require.True(t, open, "the merged inbound channel closed early")
			seen[msg.Command()] = true
		case <-time.After(5 * time.Second):
			t.Fatalf("timed out with only %v merged", seen)
		}
	}

	require.True(t, seen[wire.CmdPong])
	require.True(t, seen[wire.CmdInv])

	for i := 0; i < 2; i++ {
		require.NoError(t, <-writeErr)
	}
}

// PR 1286 invariant, association.cpp:93-109 Shutdown: one failing stream shuts
// down every stream in the association, exactly once.
func TestAssociation_TeardownFromEitherStream(t *testing.T) {
	for _, victim := range []wire.StreamType{wire.StreamTypeGeneral, wire.StreamTypeData1} {
		general, generalRemote := pipeConn(t, wire.StreamTypeGeneral, wire.ProtocolVersion)

		a := NewAssociation(general, []byte{0x03})

		ctx, cancel := context.WithCancel(context.Background())

		a.Start(ctx)

		data1, data1Remote := pipeConn(t, wire.StreamTypeData1, wire.ProtocolVersion)
		require.NoError(t, a.Attach(data1))

		if victim == wire.StreamTypeGeneral {
			require.NoError(t, generalRemote.Close())
		} else {
			require.NoError(t, data1Remote.Close())
		}

		select {
		case <-a.Done():
		case <-time.After(5 * time.Second):
			t.Fatalf("victim %v: the association did not tear down", victim)
		}

		select {
		case <-general.Done():
		case <-time.After(5 * time.Second):
			t.Fatalf("victim %v: the GENERAL stream stayed open", victim)
		}

		select {
		case <-data1.Done():
		case <-time.After(5 * time.Second):
			t.Fatalf("victim %v: the DATA1 stream stayed open", victim)
		}

		require.Error(t, a.Err(), "victim %v", victim)

		select {
		case _, open := <-a.Inbound():
			require.False(t, open, "victim %v: the merged inbound channel stayed open", victim)
		case <-time.After(5 * time.Second):
			t.Fatalf("victim %v: the merged inbound channel did not close", victim)
		}

		select {
		case _, open := <-a.InboundBlocks():
			require.False(t, open, "victim %v: the merged block channel stayed open", victim)
		case <-time.After(5 * time.Second):
			t.Fatalf("victim %v: the merged block channel did not close", victim)
		}

		select {
		case _, open := <-a.InboundTxns():
			require.False(t, open, "victim %v: the merged txns channel stayed open", victim)
		case <-time.After(5 * time.Second):
			t.Fatalf("victim %v: the merged txns channel did not close", victim)
		}

		late, _ := pipeConn(t, wire.StreamTypeData1, wire.ProtocolVersion)
		require.ErrorIs(t, a.Attach(late), ErrAssociationClosed, "victim %v", victim)

		select {
		case <-late.Done():
		case <-time.After(5 * time.Second):
			t.Fatalf("victim %v: the late stream was not closed", victim)
		}

		require.NoError(t, a.Close(), "victim %v", victim)
		require.NoError(t, a.Close(), "victim %v", victim)

		cancel()
	}
}

func TestAssociation_SumsByteCounters(t *testing.T) {
	general, generalRemote := pipeConn(t, wire.StreamTypeGeneral, wire.ProtocolVersion)

	a := NewAssociation(general, []byte{0x04})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	a.Start(ctx)

	defer func() { _ = a.Close() }()

	blockPriority, ok := PolicyForName(wire.BlockPriorityStreamPolicy)
	require.True(t, ok)

	a.SetPolicy(blockPriority)

	data1, data1Remote := pipeConn(t, wire.StreamTypeData1, wire.ProtocolVersion)
	require.NoError(t, a.Attach(data1))

	require.NoError(t, a.Send(wire.NewMsgPing(5)))
	require.Equal(t, wire.CmdPing, readCommand(t, data1Remote))

	require.NoError(t, a.Send(wire.NewMsgInv()))
	require.Equal(t, wire.CmdInv, readCommand(t, generalRemote))

	require.Eventually(t, func() bool {
		return general.BytesSent() > 0 && data1.BytesSent() > 0
	}, 5*time.Second, 10*time.Millisecond)

	_, err := wire.WriteMessageWithEncodingN(generalRemote, wire.NewMsgPong(6), wire.ProtocolVersion, wire.MainNet, wire.BaseEncoding)
	require.NoError(t, err)

	select {
	case msg, open := <-a.Inbound():
		require.True(t, open)
		require.Equal(t, wire.CmdPong, msg.Command())
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for the pong")
	}

	require.Equal(t, general.BytesSent()+data1.BytesSent(), a.BytesSent())
	require.Equal(t, general.BytesReceived()+data1.BytesReceived(), a.BytesReceived())
	require.Positive(t, a.BytesSent())
	require.Positive(t, a.BytesReceived())
	require.Equal(t, general.RemoteAddr(), a.RemoteAddr())
}

func TestAssociation_SetProtocolVersionReachesEveryStream(t *testing.T) {
	general, _ := pipeConn(t, wire.StreamTypeGeneral, wire.ProtocolVersion)

	a := NewAssociation(general, []byte{0x05})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	a.Start(ctx)

	defer func() { _ = a.Close() }()

	a.SetProtocolVersion(ExtendedPayloadVersion)

	data1, _ := pipeConn(t, wire.StreamTypeData1, wire.ProtocolVersion)
	require.NoError(t, a.Attach(data1))

	require.Equal(t, uint32(ExtendedPayloadVersion), general.pver.Load())
	require.Equal(t, uint32(ExtendedPayloadVersion), data1.pver.Load())

	a.SetProtocolVersion(70015)

	require.Equal(t, uint32(70015), general.pver.Load())
	require.Equal(t, uint32(70015), data1.pver.Load())
}

func TestAssociation_StreamTypeDefaultsToGeneral(t *testing.T) {
	local, remote := net.Pipe()

	defer func() { _ = remote.Close() }()

	c := New(local, testConfig())

	defer func() { _ = c.Close() }()

	require.Equal(t, wire.StreamTypeGeneral, c.StreamType())
}

// A local close must release a read loop parked on a full inbound channel.
// That select waits on the Conn's quit channel, never on the socket, so
// closing the socket alone would leave the reader parked and Done would never
// close — while the association reported a clean teardown.
func TestAssociation_CloseReleasesAReaderParkedOnAFullInbound(t *testing.T) {
	local, remote := net.Pipe()

	defer func() { _ = remote.Close() }()

	cfg := testConfig()
	cfg.RecvQueueLen = 1

	general := New(local, cfg)

	a := NewAssociation(general, []byte{0x06})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	a.Start(ctx)

	// Nothing ever reads a.Inbound here. The far side keeps writing until the
	// pipe goes away, which backs up the merged channel, then the Conn's own
	// inbound channel, and finally parks the read loop mid-send.
	go func() {
		for {
			_, err := wire.WriteMessageWithEncodingN(remote, wire.NewMsgPing(9), wire.ProtocolVersion, wire.MainNet, wire.BaseEncoding)
			if err != nil {
				return
			}
		}
	}()

	require.Eventually(t, func() bool {
		return len(a.inbound) == cap(a.inbound) && len(general.inbound) == cap(general.inbound)
	}, 5*time.Second, 10*time.Millisecond, "the inbound channels never filled, so no reader was parked")

	require.NoError(t, a.Close())

	select {
	case <-general.Done():
	case <-time.After(5 * time.Second):
		t.Fatal("the parked read loop was never released")
	}

	select {
	case <-a.Done():
	case <-time.After(5 * time.Second):
		t.Fatal("the association did not tear down")
	}

	require.ErrorIs(t, general.Err(), ErrConnClosed)
	require.Error(t, a.Err())
}

// A stream attached before Start must be started BY Start, on the caller's
// context, so that cancellation reaches it. Starting it at Attach time would
// bind it to a context the caller never supplied.
func TestAssociation_AttachBeforeStartRunsOnTheCallersContext(t *testing.T) {
	general, _ := pipeConn(t, wire.StreamTypeGeneral, wire.ProtocolVersion)

	a := NewAssociation(general, []byte{0x07})

	data1, data1Remote := pipeConn(t, wire.StreamTypeData1, wire.ProtocolVersion)
	require.NoError(t, a.Attach(data1))
	require.True(t, a.HasStream(wire.StreamTypeData1))

	// net.Pipe is unbuffered, so this write completes only once something
	// reads the other end. Nothing may read it until Start runs.
	writeErr := make(chan error, 1)

	go func() {
		_, err := wire.WriteMessageWithEncodingN(data1Remote, wire.NewMsgPong(11), wire.ProtocolVersion, wire.MainNet, wire.BaseEncoding)
		writeErr <- err
	}()

	select {
	case <-writeErr:
		t.Fatal("the pre-attached stream was reading before Start")
	case <-time.After(250 * time.Millisecond):
	}

	ctx, cancel := context.WithCancel(context.Background())

	a.Start(ctx)

	select {
	case err := <-writeErr:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("Start did not start the pre-attached stream")
	}

	select {
	case msg, open := <-a.Inbound():
		require.True(t, open)
		require.Equal(t, wire.CmdPong, msg.Command())
	case <-time.After(5 * time.Second):
		t.Fatal("the pre-attached stream's message never reached the merged channel")
	}

	cancel()

	for name, ch := range map[string]<-chan struct{}{
		"association": a.Done(),
		"general":     general.Done(),
		"data1":       data1.Done(),
	} {
		select {
		case <-ch:
		case <-time.After(5 * time.Second):
			t.Fatalf("cancelling the context did not close %s", name)
		}
	}

	require.Error(t, a.Err())
}
