package peer

import (
	"testing"

	"github.com/bsv-blockchain/go-wire"
	"github.com/stretchr/testify/require"
)

func TestDefaultStreamPolicyRoutesAllToGeneral(t *testing.T) {
	policy := PolicyForName(wire.DefaultStreamPolicy)

	tests := []wire.Message{
		&wire.MsgBlock{},
		&wire.MsgTx{},
		&wire.MsgPing{},
		&wire.MsgPong{},
		&wire.MsgHeaders{},
		&wire.MsgInv{},
	}

	for _, msg := range tests {
		st := policy.StreamForMessage(msg)
		require.Equal(t, wire.StreamTypeGeneral, st,
			"DefaultStreamPolicy should route %s to GENERAL", msg.Command())
	}
}

func TestBlockPriorityPolicyRoutesCorrectly(t *testing.T) {
	policy := PolicyForName(wire.BlockPriorityStreamPolicy)

	// Messages that should go to DATA1.
	data1Messages := []wire.Message{
		&wire.MsgBlock{},
		&wire.MsgHeaders{},
		&wire.MsgPing{},
		&wire.MsgPong{},
	}

	for _, msg := range data1Messages {
		st := policy.StreamForMessage(msg)
		require.Equal(t, wire.StreamTypeData1, st,
			"BlockPriorityPolicy should route %s to DATA1", msg.Command())
	}

	// Messages that should go to GENERAL.
	generalMessages := []wire.Message{
		&wire.MsgTx{},
		&wire.MsgInv{},
		&wire.MsgGetData{},
		&wire.MsgAddr{},
		&wire.MsgVerAck{},
	}

	for _, msg := range generalMessages {
		st := policy.StreamForMessage(msg)
		require.Equal(t, wire.StreamTypeGeneral, st,
			"BlockPriorityPolicy should route %s to GENERAL", msg.Command())
	}
}

func TestPolicyForNameUnknownReturnsDefault(t *testing.T) {
	policy := PolicyForName("SomeFuturePolicy")

	st := policy.StreamForMessage(&wire.MsgBlock{})
	require.Equal(t, wire.StreamTypeGeneral, st,
		"unknown policy should fall back to default (GENERAL)")
}
