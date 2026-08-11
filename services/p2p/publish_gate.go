package p2p

import (
	"context"
	"time"

	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/services/blockchain/blockchain_api"
	"github.com/bsv-blockchain/teranode/settings"
)

// topicKind identifies an outbound pubsub message class independent of the
// chain-specific prefix in the wire topic name.
type topicKind string

const (
	topicKindBlock      topicKind = "block"
	topicKindSubtree    topicKind = "subtree"
	topicKindRejectedTx topicKind = "rejected_tx"
	topicKindNodeStatus topicKind = "node_status"
)

// outboundTopicsAllowed declares, per blockchain FSM state, which outbound
// pubsub message classes the node may emit. Every publish goes through
// publishToNetwork, which drops (and counts) anything not listed here, so a
// new code path cannot silently leak announcements while the node is
// catching up. node_status stays allowed in every state so peers can track
// our height.
var outboundTopicsAllowed = map[blockchain_api.FSMStateType]map[topicKind]bool{
	// IDLE means either a deliberate Stop (docs: the node must not
	// participate in the network at all) or "state unknown" - the blockchain
	// client stores IDLE as its safety fallback when the FSM fetch or the
	// heartbeat fails, and reports it without an error. Either way, publish
	// nothing but node_status.
	blockchain_api.FSMStateType_IDLE: {
		topicKindNodeStatus: true,
	},
	blockchain_api.FSMStateType_CATCHINGBLOCKS: {
		topicKindNodeStatus: true,
	},
	blockchain_api.FSMStateType_RUNNING: {
		topicKindBlock:      true,
		topicKindSubtree:    true,
		topicKindRejectedTx: true,
		topicKindNodeStatus: true,
	},
}

// nodeStatusOnly is the fallback for FSM states with no entry in
// outboundTopicsAllowed (e.g. an enum value added by a newer blockchain
// service): let peers keep tracking our height, leak nothing else.
var nodeStatusOnly = map[topicKind]bool{topicKindNodeStatus: true}

// allowedTopicsForState returns the outbound allow-list for the given FSM
// state, falling back to node_status-only for unrecognized states.
func allowedTopicsForState(state blockchain_api.FSMStateType) map[topicKind]bool {
	if allowed, ok := outboundTopicsAllowed[state]; ok {
		return allowed
	}

	return nodeStatusOnly
}

// topicKindForName maps a wire topic name (with chain prefix) back to its
// message class. Unknown names fail closed in publishToNetwork.
func (s *Server) topicKindForName(topicName string) (topicKind, bool) {
	switch topicName {
	case s.blockTopicName:
		return topicKindBlock, true
	case s.subtreeTopicName:
		return topicKindSubtree, true
	case s.rejectedTxTopicName:
		return topicKindRejectedTx, true
	case s.nodeStatusTopicName:
		return topicKindNodeStatus, true
	default:
		return "", false
	}
}

// currentFSMState returns the blockchain FSM state. The blockchain client
// keeps a push-updated local copy of the state, so this is normally a memory
// read rather than a gRPC round-trip. A nil blockchain client reports
// RUNNING so setups without a blockchain stay permissive, matching the
// previous behavior of isBlockchainSyncingOrCatchingUp. The returned state
// is meaningless when err is non-nil.
func (s *Server) currentFSMState(ctx context.Context) (blockchain_api.FSMStateType, error) {
	if s.blockchainClient == nil {
		return blockchain_api.FSMStateType_RUNNING, nil
	}

	fsmCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	state, err := s.blockchainClient.GetFSMCurrentState(fsmCtx)
	if err != nil {
		return blockchain_api.FSMStateType_IDLE, err
	}

	return *state, nil
}

// canSendToNetwork reports whether messages of the given class may be
// published in the current FSM state, counting suppressed classes in
// prometheus. State-fetch failures fail open so a briefly unreachable
// blockchain service does not silence the node.
func (s *Server) canSendToNetwork(ctx context.Context, kind topicKind) bool {
	state, err := s.currentFSMState(ctx)
	if err != nil {
		s.logger.Warnf("[canSendToNetwork] allowing %s publish, error getting blockchain FSM state: %v", kind, err)
		return true
	}

	if !allowedTopicsForState(state)[kind] {
		initPrometheusMetrics()
		s.logger.Debugf("[canSendToNetwork] %s not allowed in FSM state %s", kind, state.String())
		prometheusP2PPublishBlocked.WithLabelValues(string(kind), state.String(), "precheck").Inc()

		return false
	}

	return true
}

// shouldSkipNotification applies the outbound allow-list before any work is
// done for a blockchain notification. PeerFailure is never skipped: it
// drives peer switching during catchup rather than outbound gossip.
func (s *Server) shouldSkipNotification(ctx context.Context, notificationType model.NotificationType) bool {
	switch notificationType {
	case model.NotificationType_Block:
		return !s.canSendToNetwork(ctx, topicKindBlock)
	case model.NotificationType_Subtree:
		return !s.canSendToNetwork(ctx, topicKindSubtree)
	case model.NotificationType_PeerFailure:
		return false
	default:
		// Types with no outbound announcement follow block gossip: processed
		// when block announcements are allowed, skipped otherwise. This
		// preserves the previous blanket "skip everything but PeerFailure
		// while catching up" behavior without polluting the blocked-publish
		// counter.
		state, err := s.currentFSMState(ctx)
		if err != nil {
			return false
		}

		return !allowedTopicsForState(state)[topicKindBlock]
	}
}

// publishToNetwork is the single choke point in front of P2PClient.Publish:
// it drops any message whose class is not allowed by the configured listen
// mode or in the current FSM state (or whose topic is unknown - fail
// closed), logging the drop and counting it in prometheus so a leaking code
// path is visible instead of silent.
func (s *Server) publishToNetwork(ctx context.Context, topicName string, msgBytes []byte) error {
	initPrometheusMetrics()

	kind, ok := s.topicKindForName(topicName)
	if !ok {
		s.logger.Errorf("[publishToNetwork] dropping publish to unknown topic %s: not in the outbound allow-list", topicName)
		prometheusP2PPublishBlocked.WithLabelValues("unknown", "unknown", "chokepoint").Inc()

		return nil
	}

	// Listen-mode policy, enforced centrally in addition to the handlers:
	// silent nodes publish nothing, listen-only nodes publish only
	// node_status (so they stay discoverable without relaying gossip).
	mode := s.settings.P2P.ListenMode
	if mode == settings.ListenModeSilent || (mode == settings.ListenModeListenOnly && kind != topicKindNodeStatus) {
		s.logger.Debugf("[publishToNetwork] dropping %s publish in listen mode %s", kind, mode)
		return nil
	}

	// This fail-open branch covers only a direct RPC error; the common
	// degraded-client case never reaches it, because blockchain.Client
	// stores IDLE as its safety fallback and reports it with a nil error,
	// landing in the (node_status-only) IDLE bucket below instead.
	state, err := s.currentFSMState(ctx)
	if err != nil {
		s.logger.Warnf("[publishToNetwork] allowing %s publish, error getting blockchain FSM state: %v", kind, err)
		return s.P2PClient.Publish(ctx, topicName, msgBytes)
	}

	if !allowedTopicsForState(state)[kind] {
		s.logger.Errorf("[publishToNetwork] dropping %s publish: not allowed in FSM state %s", kind, state.String())
		prometheusP2PPublishBlocked.WithLabelValues(string(kind), state.String(), "chokepoint").Inc()

		return nil
	}

	return s.P2PClient.Publish(ctx, topicName, msgBytes)
}
