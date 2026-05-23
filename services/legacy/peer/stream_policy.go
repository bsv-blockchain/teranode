package peer

import (
	"github.com/bsv-blockchain/go-wire"
)

// StreamPolicy determines which stream type a message should be sent on
// within a multistream association.
type StreamPolicy interface {
	StreamForMessage(msg wire.Message) wire.StreamType
}

// defaultStreamPolicy routes all messages to the GENERAL stream.
type defaultStreamPolicy struct{}

func (p *defaultStreamPolicy) StreamForMessage(_ wire.Message) wire.StreamType {
	return wire.StreamTypeGeneral
}

// blockPriorityPolicy routes block-related and ping/pong messages to DATA1,
// and everything else to GENERAL. This matches the SV node's
// BlockPriorityStreamPolicy behavior.
type blockPriorityPolicy struct{}

func (p *blockPriorityPolicy) StreamForMessage(msg wire.Message) wire.StreamType {
	switch msg.Command() {
	case wire.CmdBlock, wire.CmdHeaders, wire.CmdPing, wire.CmdPong:
		return wire.StreamTypeData1
	default:
		return wire.StreamTypeGeneral
	}
}

// PolicyForName returns a StreamPolicy implementation for the given policy name.
// Returns a default policy if the name is not recognized.
func PolicyForName(name string) StreamPolicy {
	switch name {
	case wire.BlockPriorityStreamPolicy:
		return &blockPriorityPolicy{}
	default:
		return &defaultStreamPolicy{}
	}
}
