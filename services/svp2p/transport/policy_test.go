package transport

import (
	"io"
	"testing"

	"github.com/bsv-blockchain/go-wire"
	"github.com/stretchr/testify/require"
)

// TestStreamPolicy_Routing pins the BlockPriority router against SVNode's
// send-side rule: DATA1 for exactly IsHighPriorityMsg (stream_policy.cpp:25-31,
// ping/pong plus IsBlockMsg at :11-22), GENERAL for everything else.
//
// The commands are named as raw strings on purpose. The point of the test is
// the WIRE name SVNode compares against (protocol.cpp:24-44), so spelling them
// out here fails if a constant in the router is ever redefined, which sharing
// the constants would hide.
func TestStreamPolicy_Routing(t *testing.T) {
	bp, ok := PolicyForName(wire.BlockPriorityStreamPolicy)
	require.True(t, ok)
	require.True(t, bp.RequiresData1())

	// IsHighPriorityMsg: ping, pong, and the whole of IsBlockMsg.
	for _, cmd := range []string{
		"ping", "pong",
		"block", "cmpctblock", "blocktxn", "getblocktxn",
		"headers", "getheaders", "hdrsen", "gethdrsen",
	} {
		require.Equal(t, wire.StreamTypeData1, bp.StreamFor(&svp2ptestRawMsg{cmd: cmd}),
			"%s is in SVNode's IsHighPriorityMsg set and must take DATA1", cmd)
	}

	// Everything else. getdata and inv are the ones that matter in practice:
	// they are block-RELATED but not in IsBlockMsg, and SVNode leaves them on
	// GENERAL.
	for _, cmd := range []string{
		"inv", "getdata", "notfound", "getblocks", "tx", "addr", "getaddr",
		"version", "verack", "protoconf", "reject", "sendheaders", "mempool",
	} {
		require.Equal(t, wire.StreamTypeGeneral, bp.StreamFor(&svp2ptestRawMsg{cmd: cmd}),
			"%s is outside SVNode's IsHighPriorityMsg set and must take GENERAL", cmd)
	}

	// The typed messages this service actually builds, through the same rule.
	require.Equal(t, wire.StreamTypeData1, bp.StreamFor(wire.NewMsgPing(1)))
	require.Equal(t, wire.StreamTypeData1, bp.StreamFor(wire.NewMsgPong(1)))
	require.Equal(t, wire.StreamTypeData1, bp.StreamFor(&wire.MsgBlock{}))
	require.Equal(t, wire.StreamTypeData1, bp.StreamFor(wire.NewMsgHeaders()),
		"SVNode's IsBlockMsg includes headers (stream_policy.cpp:17)")
	require.Equal(t, wire.StreamTypeData1, bp.StreamFor(wire.NewMsgGetHeaders()),
		"SVNode's IsBlockMsg includes getheaders (stream_policy.cpp:18)")
	require.Equal(t, wire.StreamTypeGeneral, bp.StreamFor(wire.NewMsgInv()))
	require.Equal(t, wire.StreamTypeGeneral, bp.StreamFor(wire.NewMsgGetData()))

	def, ok := PolicyForName("Default")
	require.True(t, ok)
	require.False(t, def.RequiresData1())

	// Default keeps everything on GENERAL, including the high-priority set.
	for _, cmd := range []string{"block", "ping", "headers", "inv"} {
		require.Equal(t, wire.StreamTypeGeneral, def.StreamFor(&svp2ptestRawMsg{cmd: cmd}))
	}

	_, ok = PolicyForName("Bogus")
	require.False(t, ok)
}

// svp2ptestRawMsg is a wire.Message that carries nothing but a command name.
// StreamFor reads only Command(), so this lets the test name a command go-wire
// has no type for (cmpctblock, hdrsen, and the rest of IsBlockMsg).
type svp2ptestRawMsg struct{ cmd string }

func (m *svp2ptestRawMsg) Command() string { return m.cmd }

func (m *svp2ptestRawMsg) MaxPayloadLength(uint32) uint64 { return 0 }

func (m *svp2ptestRawMsg) Bsvdecode(io.Reader, uint32, wire.MessageEncoding) error { return nil }

func (m *svp2ptestRawMsg) BsvEncode(io.Writer, uint32, wire.MessageEncoding) error { return nil }

func TestPreferredPolicy(t *testing.T) {
	require.Equal(t, "BlockPriority", PreferredPolicy(OurPolicyPriority, []string{"Default", "BlockPriority"}).Name())
	require.Equal(t, "Default", PreferredPolicy(OurPolicyPriority, []string{"Default"}).Name())
	require.Equal(t, "Default", PreferredPolicy(OurPolicyPriority, nil).Name())
	require.Equal(t, "Default", PreferredPolicy([]string{"Default"}, []string{"BlockPriority", "Default"}).Name(), "our list decides the order")
}
