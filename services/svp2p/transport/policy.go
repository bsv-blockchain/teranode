package transport

import (
	"github.com/bsv-blockchain/go-wire"
)

// StreamPolicy defines how messages are routed to streams
type StreamPolicy interface {
	Name() string
	StreamFor(msg wire.Message) wire.StreamType
	RequiresData1() bool
}

// OurPolicyPriority is our prioritised list of preferred stream policies, the
// list CNode::GetPreferredStreamPolicyName walks (net.cpp:945-963)
var OurPolicyPriority = []string{wire.BlockPriorityStreamPolicy, wire.DefaultStreamPolicy}

// defaultStreamPolicy routes all messages to the GENERAL stream (stream_policy.cpp:98-103)
type defaultStreamPolicy struct{}

func (p *defaultStreamPolicy) Name() string {
	return wire.DefaultStreamPolicy
}

func (p *defaultStreamPolicy) StreamFor(msg wire.Message) wire.StreamType {
	return wire.StreamTypeGeneral
}

func (p *defaultStreamPolicy) RequiresData1() bool {
	return false
}

// SVNode command names go-wire has no constant for. They are part of
// IsBlockMsg's set (stream_policy.cpp:11-22), so the router below has to name
// them even though this service never sends one today: an association's
// routing rule must match SVNode's for every command, not only for the
// commands we happen to emit. Strings are protocol.cpp:24-44.
const (
	cmdCompactBlock       = "cmpctblock"  // protocol.cpp:42
	cmdGetBlockTxn        = "getblocktxn" // protocol.cpp:43
	cmdBlockTxn           = "blocktxn"    // protocol.cpp:44
	cmdHeadersEnriched    = "hdrsen"      // protocol.cpp:27
	cmdGetHeadersEnriched = "gethdrsen"   // protocol.cpp:24
)

// blockPriorityStreamPolicy routes SVNode's high-priority set to DATA1 and
// everything else to GENERAL.
type blockPriorityStreamPolicy struct{}

func (p *blockPriorityStreamPolicy) Name() string {
	return wire.BlockPriorityStreamPolicy
}

// StreamFor is the port of SVNode's SEND router,
// BlockPriorityStreamPolicy::PushMessage (stream_policy.cpp:161-184): when the
// caller has not named a stream, the message goes to DATA1 if
// IsHighPriorityMsg holds (stream_policy.cpp:25-31) and to GENERAL otherwise.
// IsHighPriorityMsg is ping, pong, or IsBlockMsg (stream_policy.cpp:11-22),
// and IsBlockMsg is a WIDE set — block, cmpctblock, blocktxn, getblocktxn,
// headers, getheaders, hdrsen, gethdrsen, plus any message whose payload type
// is BLOCK.
//
// BlockPriorityStreamPolicy::GetStreamTypeForMessage (stream_policy.cpp:187-195),
// the three-value BLOCK/PING/OTHER function this port was first written
// against, is NOT the router. Its only caller is
// Association::GetAverageBandwidth (association.cpp:222), which asks which
// stream carries a CLASS of message so it can read that stream's bandwidth
// meter — the statistic behind net_processing.cpp:107 and :5697. Porting it as
// the router left headers and getheaders on GENERAL where SVNode puts them on
// DATA1 (parity-watchlist row 14).
//
// The payload-type arm of IsBlockMsg has no counterpart here: it exists for
// SVNode's CSerializedNetMsg::PayloadType::BLOCK, a sender-side tag on a
// pre-serialised body, and this transport frames a block through SendBlock
// rather than through a tagged generic message.
func (p *blockPriorityStreamPolicy) StreamFor(msg wire.Message) wire.StreamType {
	switch msg.Command() {
	case wire.CmdPing, wire.CmdPong,
		wire.CmdBlock, cmdCompactBlock, cmdBlockTxn, cmdGetBlockTxn,
		wire.CmdHeaders, wire.CmdGetHeaders, cmdHeadersEnriched, cmdGetHeadersEnriched:
		return wire.StreamTypeData1
	default:
		return wire.StreamTypeGeneral
	}
}

func (p *blockPriorityStreamPolicy) RequiresData1() bool {
	return true
}

// PolicyForName returns a StreamPolicy for the given name, or false if not found
func PolicyForName(name string) (StreamPolicy, bool) {
	switch name {
	case wire.DefaultStreamPolicy:
		return &defaultStreamPolicy{}, true
	case wire.BlockPriorityStreamPolicy:
		return &blockPriorityStreamPolicy{}, true
	default:
		return nil, false
	}
}

// PreferredPolicy returns the first policy from ours that is also in theirs
// (net.cpp:945-963 CNode::GetPreferredStreamPolicyName, over the common set
// CNode::SetSupportedStreamPolicies built at net.cpp:904-923)
// Falls back to Default if no common policies
func PreferredPolicy(ours []string, theirs []string) StreamPolicy {
	theirsMap := make(map[string]bool)
	for _, p := range theirs {
		theirsMap[p] = true
	}

	for _, p := range ours {
		if theirsMap[p] {
			policy, _ := PolicyForName(p)
			return policy
		}
	}

	// No common policy; fall back to Default (svp2p deviation: SVNode throws)
	policy, _ := PolicyForName(wire.DefaultStreamPolicy)
	return policy
}
