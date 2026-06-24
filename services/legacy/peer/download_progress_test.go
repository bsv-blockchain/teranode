package peer

import (
	"io"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-wire"
	"github.com/stretchr/testify/require"
)

// TestActivityConnCountsReadBytes verifies the conn wrapper advances the
// byte-granular readBytes counter and refreshes lastRecv on each read — the
// progress signal that lets an in-flight large block register as activity
// before the message completes.
func TestActivityConnCountsReadBytes(t *testing.T) {
	p := &Peer{}

	client, server := net.Pipe()
	defer client.Close()
	defer server.Close()

	ac := &activityConn{Conn: server, peer: p}

	go func() { _, _ = client.Write([]byte("hello world")) }()

	buf := make([]byte, len("hello world"))
	n, err := io.ReadFull(ac, buf)
	require.NoError(t, err)
	require.Equal(t, len("hello world"), n)

	require.Equal(t, uint64(len("hello world")), p.ReadBytes())
	require.WithinDuration(t, time.Now(), p.LastRecv(), 2*time.Second)
}

// TestAssociationReadBytesSumsStreams verifies association-wide read totals sum
// across all stream peers, and that a non-multistream peer falls back to its
// own count.
func TestAssociationReadBytesSumsStreams(t *testing.T) {
	general := &Peer{}
	atomic.StoreUint64(&general.readBytes, 100)

	data1 := &Peer{}
	atomic.StoreUint64(&data1.readBytes, 900)

	assoc := NewAssociation([]byte{0x01}, general)
	require.True(t, assoc.AddStream(wire.StreamTypeData1, data1))

	require.Equal(t, uint64(1000), assoc.ReadBytes())

	general.SetAssociation(assoc)
	require.Equal(t, uint64(1000), general.AssociationReadBytes())

	lone := &Peer{}
	atomic.StoreUint64(&lone.readBytes, 42)
	require.Equal(t, uint64(42), lone.AssociationReadBytes())
}
