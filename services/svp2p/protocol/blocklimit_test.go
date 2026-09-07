package protocol

import (
	"testing"
	"time"

	"github.com/bsv-blockchain/go-wire"
	"github.com/bsv-blockchain/teranode/services/svp2p/svp2ptest"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/stretchr/testify/require"
)

// settings/policy_settings.go excessiveblocksize: the node's own block
// acceptance ceiling, which is what the legacy wire must carry.
func TestBlockPayloadLimit(t *testing.T) {
	tests := []struct {
		name      string
		tSettings *settings.Settings
		want      uint64
	}{
		{name: "the configured excessive block size", tSettings: &settings.Settings{Policy: &settings.PolicySettings{ExcessiveBlockSize: 6_000_000_000}}, want: 6_000_000_000},
		{name: "the settings default", tSettings: &settings.Settings{Policy: &settings.PolicySettings{ExcessiveBlockSize: 4294967296}}, want: 4294967296},
		{name: "zero falls back to the default", tSettings: &settings.Settings{Policy: &settings.PolicySettings{}}, want: defaultExcessiveBlockSize},
		{name: "a negative value falls back to the default", tSettings: &settings.Settings{Policy: &settings.PolicySettings{ExcessiveBlockSize: -1}}, want: defaultExcessiveBlockSize},
		{name: "no policy at all falls back to the default", tSettings: &settings.Settings{}, want: defaultExcessiveBlockSize},
		{name: "no settings at all falls back to the default", tSettings: nil, want: defaultExcessiveBlockSize},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, BlockPayloadLimit(tt.tSettings))
		})
	}
}

// The cap must reach the socket, not just the process-wide go-wire default:
// every stream the manager builds carries the node's excessiveblocksize.
func TestInbound_StreamsCarryTheConfiguredBlockPayloadCap(t *testing.T) {
	const excessive = 6_000_000_000

	m := startedManagerWith(t, func(s *settings.Settings) {
		s.Policy = &settings.PolicySettings{ExcessiveBlockSize: excessive}
	}, nil)

	id := testAssociationID(3)
	_ = establishAssociation(t, m, id)

	raw := dialRaw(t, nodeAddr(t, m, "127.0.0.1"))
	writeMsg(t, raw, &wire.MsgCreateStream{
		AssociationID:    id,
		StreamType:       wire.StreamTypeData1,
		StreamPolicyName: wire.BlockPriorityStreamPolicy,
	})

	_, ok := readMsg(t, raw, 5*time.Second).(*wire.MsgStreamAck)
	require.True(t, ok, "the node must answer createstream with streamack")

	a := m.associationByID(id)
	require.NotNil(t, a)

	for _, streamType := range []wire.StreamType{wire.StreamTypeGeneral, wire.StreamTypeData1} {
		limit, held := a.MaxBlockPayload(streamType)
		require.True(t, held, "the association must hold stream %d", streamType)
		require.Equal(t, uint64(excessive), limit, "stream %d must carry the configured cap", streamType)
	}
}

// The outbound DATA1 connection is opened on a separate socket with its own
// transport config (streams.go openNewStreamConnection), so it needs the cap
// as much as the general stream does.
func TestOutbound_Data1CarriesTheConfiguredBlockPayloadCap(t *testing.T) {
	const excessive = 6_000_000_000

	peer := scriptedListener(t, svp2ptest.Script{})

	m := outboundManager(t, peer.Addr, func(s *settings.Settings) {
		s.Policy = &settings.PolicySettings{ExcessiveBlockSize: excessive}
	})

	require.Eventually(t, func() bool { return peer.Connections() == 2 }, 10*time.Second, 50*time.Millisecond,
		"the node must dial the peer a second time for DATA1")

	cs, ok := firstIn(t, peer.Transcript, wire.CmdCreateStream).Msg.(*wire.MsgCreateStream)
	require.True(t, ok)

	require.Eventually(t, func() bool {
		a := m.associationByID(cs.AssociationID)

		return a != nil && a.HasStream(wire.StreamTypeData1)
	}, 5*time.Second, 20*time.Millisecond)

	a := m.associationByID(cs.AssociationID)
	require.NotNil(t, a)

	for _, streamType := range []wire.StreamType{wire.StreamTypeGeneral, wire.StreamTypeData1} {
		limit, held := a.MaxBlockPayload(streamType)
		require.True(t, held, "the association must hold stream %d", streamType)
		require.Equal(t, uint64(excessive), limit, "stream %d must carry the configured cap", streamType)
	}
}
